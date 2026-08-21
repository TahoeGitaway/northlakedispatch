"""
routes/briefing.py — AI-powered daily operations briefing.

Pulls today's saved routes from the DB, plus arrivals and departures
from the Breezeway API (30+ day stays classified as "Lease"), then
asks Claude to write a plain-English summary.

Results are cached in memory for 15 minutes per date so repeated page
loads don't burn API quota.
"""

import calendar as cal_mod
import json
import os
import re
import time
from datetime import date as date_cls, datetime, timedelta
from zoneinfo import ZoneInfo

import anthropic
import requests
from flask import Blueprint, jsonify, request
from flask_login import login_required, current_user

from db import get_db, get_cursor
from routes.auth import admin_required
from routes.bw_ratelimit import LOCAL_THROTTLE_STATUS as _LOCAL_THROTTLE
from routes.bw_api_log import bw_get

briefing_bp = Blueprint("briefing", __name__)

_PACIFIC = ZoneInfo("America/Los_Angeles")

def _fmt_pacific(ts: float) -> str:
    """Format a unix timestamp as Pacific date + time, e.g. 'May 3, 2:34 PM PT'."""
    dt = datetime.fromtimestamp(ts, tz=_PACIFIC)
    time_str = dt.strftime("%I:%M %p PT").lstrip("0")
    date_str = dt.strftime("%b ") + str(dt.day)
    return f"{date_str}, {time_str}"

ANTHROPIC_API_KEY       = os.environ.get("ANTHROPIC_API_KEY", "")
BREEZEWAY_CLIENT_ID     = os.environ.get("BREEZEWAY_CLIENT_ID", "")
BREEZEWAY_CLIENT_SECRET = os.environ.get("BREEZEWAY_CLIENT_SECRET", "")

CACHE_TTL          = 15 * 60   # 15 minutes for briefing
CALENDAR_CACHE_TTL = 30 * 60   # 30 minutes for calendar activity

# ── In-memory caches ──────────────────────────────────────────────
_briefing_cache:      dict  = {}   # {cache_key: (timestamp, payload)}
_calendar_cache:      dict  = {}   # {(year, month): (timestamp, activity_dict)}
_day_summary_cache:   dict  = {}   # {date_str: (timestamp, payload)}
_owner_cleaned_cache: dict  = {}   # {date_str: (ts, payload, ttl)} — owner-cleaned arrival flags
_OWNER_CLEAN_TTL         = 600     # complete scan
_OWNER_CLEAN_PARTIAL_TTL = 45      # incomplete scan: absorbs clicking between days
                                   # without pinning missing flags for long
_OWNER_CLEAN_JOB_WORKERS = 8       # scheduled scan: gentler than a one-off day-click
_prop_status_cache:   dict  = {}   # {property_id: (timestamp, payload)}
_PROP_STATUS_TTL            = 20 * 60   # 20 minutes per property
_bw_token:            dict  = {"value": None, "expires_at": 0}
_property_cache:      dict  = {}   # {property_id: name}
_property_cache_ts:   float = 0


# ── Breezeway auth ────────────────────────────────────────────────

_bw_token_last_error: str = ""   # why the last token fetch failed, for the UI
_bw_auth_retry_at: float = 0.0   # epoch; don't call auth again before this


def _parse_retry_after(payload: dict) -> float:
    """Breezeway's auth 429 carries the reset time in the BODY, not a header:
        {"details": {"retry_after": "2026-07-30T21:41:58"}}
    Return it as an epoch, or 0 if absent/unparseable. Naive timestamps are read
    as UTC, which matches the observed values."""
    try:
        raw = ((payload or {}).get("details") or {}).get("retry_after")
        if not raw:
            return 0.0
        from datetime import datetime as _dt, timezone as _tz
        ts = _dt.fromisoformat(str(raw).replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=_tz.utc)
        # Clamp: a malformed far-future value must not lock the app out for hours.
        return min(ts.timestamp(), time.time() + 3600)
    except Exception:
        return 0.0


def _get_bw_token_last_error() -> str:
    """Why the most recent token fetch failed. Every failure used to collapse to
    None and surface as "Could not authenticate with Breezeway", which hides the
    difference between missing credentials, a rejected secret, a timeout, and the
    auth endpoint itself being rate-limited."""
    return _bw_token_last_error


def _get_breezeway_token() -> str | None:
    """Return a valid Breezeway JWT, fetching a new one only when stale."""
    global _bw_token_last_error, _bw_auth_retry_at
    if not BREEZEWAY_CLIENT_ID or not BREEZEWAY_CLIENT_SECRET:
        _bw_token_last_error = ("BREEZEWAY_CLIENT_ID / BREEZEWAY_CLIENT_SECRET are "
                                "not set on the server")
        return None
    now = time.time()
    if _bw_token["value"] and now < _bw_token["expires_at"] - 60:
        return _bw_token["value"]
    # Auth is rate-limited SEPARATELY from the data endpoints, and a failure leaves
    # no cached token — so every later request retried auth, each one earning another
    # 429 and pushing the reset further out. That is self-sustaining: it does not
    # recover no matter how long you wait. Honour the reset time Breezeway gives us
    # and simply don't call until then.
    if now < _bw_auth_retry_at:
        wait = int(_bw_auth_retry_at - now)
        _bw_token_last_error = (f"auth is rate-limited; Breezeway asked us to wait "
                                f"until {time.strftime('%H:%M:%S', time.localtime(_bw_auth_retry_at))} "
                                f"({wait}s away). Not retrying before then.")
        return None
    try:
        resp = requests.post(
            "https://api.breezeway.io/public/auth/v1/",
            json={"client_id": BREEZEWAY_CLIENT_ID, "client_secret": BREEZEWAY_CLIENT_SECRET},
            timeout=10,
        )
        try:
            data = resp.json()
        except Exception:
            data = {}
        token = data.get("access_token")
        if token:
            _bw_token["value"]      = token
            _bw_token["expires_at"] = now + 23 * 3600
            _bw_token_last_error    = ""
            return token
        # 200 without a token, or any non-200 — keep the status and body. A 429
        # here means the AUTH endpoint is throttled, which is a very different
        # problem from a bad secret and used to look identical.
        body = ""
        try:
            body = str(data) if data else (resp.text or "")[:300]
        except Exception:
            pass
        if resp.status_code == 429:
            # The reset time lives in the body. Respect it: retrying sooner is what
            # kept the limit permanently exhausted.
            retry_at = _parse_retry_after(data)
            _bw_auth_retry_at = retry_at or (time.time() + 60)
            _bw_token_last_error = (
                f"auth rate-limited (HTTP 429). Breezeway asks us to wait until "
                f"{time.strftime('%H:%M:%S', time.localtime(_bw_auth_retry_at))}. "
                f"Raw: {body}")[:400]
            return None
        _bw_token_last_error = f"auth returned HTTP {resp.status_code}: {body}"[:400]
        return None
    except requests.exceptions.Timeout:
        _bw_token_last_error = "auth request timed out after 10 s"
        return None
    except Exception as ex:
        _bw_token_last_error = f"{type(ex).__name__}: {ex}"[:400]
        return None


# ── Breezeway data fetchers ───────────────────────────────────────

_bw_resv_last_error: str = ""   # last error from _fetch_bw_reservations, for degradation signaling

# How long to wait on ONE page of reservations. Deliberately longer than the 15 s
# the per-property task sweep uses, because the two are not the same situation:
#
#   * The task sweep fans 442 calls across 16 workers under a wall-clock budget.
#     There, a longer timeout means each worker holds its slot longer and FEWER
#     properties get attempted — which is why 4dfbb29 put it back to 15 s.
#   * Reservations paginate SEQUENTIALLY, one call at a time, no pool and no gate
#     slot. Nothing is starved by waiting longer here.
#
# And the reservation endpoint is genuinely slow rather than refusing: the API log
# shows pages returning 200 at 13355 / 14457 / 14888 ms with one ReadTimeout at
# 15203 ms. Those are answers that arrived, discarded ~100 ms short. 25 s clears
# the observed spread with room, and still returns long before the proxy gives up.
BW_RESV_READ_TIMEOUT_S = 25


def _get_bw_resv_last_error() -> str:
    """Return (and represents) the last error _fetch_bw_reservations swallowed, if any."""
    return _bw_resv_last_error


def _fetch_bw_reservations_status(token: str, params: dict) -> tuple:
    """Paginate reservations, returning (results, error).

    `error` is "" when the whole set was read, and a human-readable reason when it
    was not — in which case `results` holds only the pages that did arrive.

    Prefer this over _fetch_bw_reservations anywhere a MISSING reservation changes
    what the user is told. Absence is not evidence here: a house with no reservation
    in the list and a house whose page never loaded look identical, so a caller that
    can't see the difference reports a real check-in as no check-in at all.
    """
    all_results = []
    page, limit = 1, 100
    try:
        while True:
            resp = bw_get(
                "https://api.breezeway.io/public/inventory/v1/reservation",
                headers={"Authorization": f"JWT {token}"},
                params={**params, "limit": limit, "page": page},
                timeout=BW_RESV_READ_TIMEOUT_S,
            )
            # A refused or errored page is NOT an empty page. Its body still parses
            # as JSON, just without "results" — which then reads as "that was the
            # last page" and returns a truncated set that looks complete. This is
            # the same trap _fetch_bw_reservations_checked was written to avoid.
            if not resp.ok:
                return all_results, (f"Breezeway returned HTTP {resp.status_code} on "
                                     f"page {page} of the reservation list")
            data = resp.json()
            page_results = (data.get("results", data.get("data", [])) or []) \
                           if isinstance(data, dict) else (data or [])
            all_results.extend(page_results)
            if len(page_results) < limit:
                break
            page += 1
    except Exception as ex:
        detail = str(ex) or ex.__class__.__name__
        try:
            print(f"[briefing] reservations partial/failed after page {page}: {ex}")
        except Exception:
            pass
        return all_results, (f"Breezeway did not answer page {page} of the reservation "
                             f"list within {BW_RESV_READ_TIMEOUT_S} s ({detail})")
    return all_results, ""


def _fetch_bw_reservations(token: str, params: dict) -> list:
    """Paginate through all Breezeway reservations matching params.

    Note: a mid-pagination failure returns the pages fetched so far. Callers that
    need to know the result may be partial should check _get_bw_resv_last_error()
    immediately after — it is set to a non-empty string when this call errored.
    New callers should prefer _fetch_bw_reservations_status, which hands back the
    same reason directly instead of leaving it on a module global.
    """
    global _bw_resv_last_error
    all_results, err = _fetch_bw_reservations_status(token, params)
    _bw_resv_last_error = err
    return all_results


def _fetch_bw_reservations_checked(token: str, params: dict,
                                   retries: int = 2, backoff: float = 1.5) -> tuple:
    """Like _fetch_bw_reservations, but hardened for callers that must NOT draw
    conclusions from a half-loaded page set (e.g. the PRI vacancy check, which
    infers "no upcoming booking" from absence — a truncated fetch fabricates
    false vacancies).

    Differences:
      - A page that times out / errors is retried up to `retries` times with a
        linear backoff before the fetch is abandoned.
      - Returns (results, complete). `complete` is False iff a page ultimately
        failed after all retries; in that case `results` holds only the pages
        fetched so far and the caller should refuse to treat it as the whole set.
    """
    all_results = []
    page, limit = 1, 100
    while True:
        page_results = None
        last_exc = None
        for attempt in range(retries + 1):
            try:
                resp = bw_get(
                    "https://api.breezeway.io/public/inventory/v1/reservation",
                    headers={"Authorization": f"JWT {token}"},
                    params={**params, "limit": limit, "page": page},
                    timeout=15,
                )
                # A throttled or errored page is NOT an empty page. Without this,
                # a 429 whose body happens to parse as JSON yields no "results",
                # which then reads as "that was the last page" and returns a
                # truncated set marked COMPLETE — defeating the one thing this
                # function exists to guarantee. Raising routes it through the
                # retry/backoff below and, if it never recovers, reports the
                # fetch as incomplete.
                if not resp.ok:
                    raise RuntimeError(f"HTTP {resp.status_code}")
                data = resp.json()
                page_results = (data.get("results", data.get("data", [])) or []) \
                               if isinstance(data, dict) else (data or [])
                break  # this page succeeded
            except Exception as ex:
                last_exc = ex
                if attempt < retries:
                    time.sleep(backoff * (attempt + 1))
        if page_results is None:
            # every attempt for this page failed — data is INCOMPLETE, say so
            try:
                print(f"[briefing] reservations INCOMPLETE at page {page} "
                      f"({len(all_results)} rows so far): {last_exc}")
            except Exception:
                pass
            return all_results, False
        all_results.extend(page_results)
        if len(page_results) < limit:
            break
        page += 1
    return all_results, True


# How long to wait on ONE per-property task read. Kept at 15 s, and deliberately
# separate from BW_RESV_READ_TIMEOUT_S above: these fan out across a thread pool
# under a wall-clock budget, so a longer wait means each worker holds its slot
# longer and FEWER properties get attempted. The API log settles which way that
# trade goes — 74 real timeouts all-time against 22,433 refusals — so waiting
# longer here buys almost nothing and costs coverage. Reservations are the
# opposite case and get their own, longer constant.
BW_TASK_READ_TIMEOUT_S = 15


def fetch_property_tasks_range(token: str, pid: str, ref_id: str,
                               start, end) -> tuple:
    """One property's tasks over a date range. Returns (tasks, ok, status).

    ok=False means it genuinely could not be loaded and the caller must NOT treat
    the empty list as "no tasks". `status` is the last failing HTTP status, or
    None for a timeout, so callers can name the real cause.

    Preserves the id-fallback every copy of this had: Breezeway's task API takes
    the external reference id while other calls take Breezeway's own, and a 200
    carrying no results may just mean we asked in the wrong id space — so an empty
    200 falls through to the next key rather than being accepted as the answer.
    """
    date_range = f"{start.isoformat()},{end.isoformat()}"
    # home_id before property_id. Breezeway aliases property_id onto
    # reference_property_id, so a raw Breezeway pid there can only ever 422 — it was
    # costing every house a guaranteed-failed request before the one that works.
    # Kept last rather than deleted: cheap insurance if home_id ever fails too.
    id_pairs = (([("reference_property_id", ref_id)] if ref_id else [])
                + [("home_id", pid), ("property_id", pid)])

    saw_empty_200 = False
    last_status = None
    for key, val in id_pairs:
        for attempt in range(3):
            try:
                r = bw_get(
                    "https://api.breezeway.io/public/inventory/v1/task/",
                    headers={"Authorization": f"JWT {token}"},
                    params={"scheduled_date": date_range, key: val, "limit": 100},
                    timeout=BW_TASK_READ_TIMEOUT_S,
                )
                last_status = r.status_code
                if r.status_code == 200:
                    body = r.json()
                    results = body.get("results",
                                       body.get("data", body if isinstance(body, list) else []))
                    if results:
                        return results, True, 200
                    saw_empty_200 = True
                    break                       # this id space answered; try the next
                if r.status_code == 429 or r.status_code >= 500:
                    time.sleep(0.3 * (attempt + 1))
                    continue                    # transient — worth another go
                break                           # 4xx that won't change; try the next id key
            except Exception:
                last_status = None              # timeout / transport — retryable
                time.sleep(0.3 * (attempt + 1))

    # Every id space answered 200 with nothing: a real "no tasks here".
    if saw_empty_200:
        return [], True, 200
    return [], False, last_status


def fetch_tasks_for_pids(token: str, pids: list, start, end,
                         max_workers: int = 16, budget_s: float = 45.0) -> tuple:
    """Sweep many properties. Returns (tasks, failed_count, failure_statuses).

    failure_statuses is the {"429": n, "timeout": n, "unreached": n} tally the UI
    needs in order to say WHY, via static/bw-failure.js — the same shape the map
    import already returns.

    Deliberately does NOT share a contextvars.Context across the workers. Doing
    that is what broke every sweep in the app on 2026-08-11: a Context cannot be
    entered by two threads at once, so the first property won the race and the
    other 441 raised before their request was ever made (60bf848). Caller
    labelling is not worth that, so it is simply absent here.

    Not a `with` block, for the same reason the import isn't: exiting one calls
    shutdown(wait=True), which blocks on every already-running request and undoes
    the budget. Abandoning them is safe — they are GETs whose results we have
    already given up on.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    ref_cache = _get_live_ref_cache()
    all_tasks, seen_ids = [], set()
    failed, statuses = 0, {}
    deadline = time.monotonic() + budget_s

    ex = ThreadPoolExecutor(max_workers=max_workers)
    try:
        # _ref_for, not ref_cache.get: both callers of this sweeper (Bear Fence and
        # Bear Fence Delete) build their pid list with str(), and the cache is
        # int-keyed — so every house missed its ref id and took the slow path.
        futures = {
            ex.submit(fetch_property_tasks_range,
                      token, pid, _ref_for(ref_cache, pid), start, end): pid
            for pid in pids
        }
        try:
            for future in as_completed(futures,
                                       timeout=max(0.0, deadline - time.monotonic())):
                try:
                    tasks, ok, status = future.result()
                except Exception:
                    failed += 1
                    statuses["timeout"] = statuses.get("timeout", 0) + 1
                    continue
                if not ok:
                    failed += 1
                    k = "timeout" if status is None else str(status)
                    statuses[k] = statuses.get(k, 0) + 1
                    continue
                for t in tasks:
                    tid = t.get("id")
                    if tid is None or tid not in seen_ids:
                        if tid is not None:
                            seen_ids.add(tid)
                        all_tasks.append(t)
        except Exception:
            pass        # budget reached — the leftovers are counted below

        # Properties the budget cut off were never asked, so they cannot appear in
        # the API log. Filing them as timeouts is what made a panel claim hundreds
        # of properties "did not respond within 15 s" for requests never sent.
        for future, pid in futures.items():
            if not future.done():
                future.cancel()
                failed += 1
                statuses["unreached"] = statuses.get("unreached", 0) + 1
    finally:
        ex.shutdown(wait=False, cancel_futures=True)

    return all_tasks, failed, statuses


def _fetch_bw_endpoint(token: str, path: str, params: dict) -> tuple:
    """Generic paginated GET for any Breezeway endpoint.
    Returns (results_list, error_string, http_status).
    Tries the path exactly as given — caller decides what to do with 404/403.

    Every Breezeway call in the app funnels through here, so this is where the
    process-wide rate gate lives (routes/bw_ratelimit.py). Pacing here means all
    seven per-property fan-outs share one budget instead of 25 thread pools each
    retrying into the same wall.
    """
    from routes.bw_ratelimit import gate, LOCAL_THROTTLE_STATUS
    from routes import bw_api_log

    all_results = []
    page, limit = 1, 100
    last_status = None
    _t0 = time.time()          # so the except branches can time a failed call too
    try:
        while True:
            # Hold until the gate allows a send. If it can't grant one quickly,
            # report a synthetic 429 rather than blocking: this app already loses
            # long requests to a gateway timeout, and callers now surface a
            # throttled property honestly instead of silently showing no tasks.
            if not gate.acquire():
                return [], ("Held back by this app's rate limiter — not sent to "
                            "Breezeway"), LOCAL_THROTTLE_STATUS

            _t0 = time.time()
            resp = requests.get(
                f"https://api.breezeway.io{path}",
                headers={"Authorization": f"JWT {token}"},
                params={**params, "limit": limit, "page": page},
                timeout=15,
            )
            last_status = resp.status_code
            gate.on_response(last_status)
            # One line, one direction. routes/bw_api_log.py owns everything else —
            # including working out which house the call was for, from the params.
            bw_api_log.record(path, last_status, resp.ok,
                              elapsed_ms=int((time.time() - _t0) * 1000),
                              params={**params, "limit": limit, "page": page})
            if not resp.ok:
                try:
                    detail = resp.json()
                except Exception:
                    detail = resp.text[:300]
                return [], f"HTTP {resp.status_code}: {detail}", last_status
            data = resp.json()
            page_results = (data.get("results", data.get("data", data.get("tasks", []))) or []) \
                           if isinstance(data, dict) else (data or [])
            all_results.extend(page_results)
            if len(page_results) < limit:
                break
            page += 1
    except requests.exceptions.Timeout:
        bw_api_log.record(path, None, False, detail="timed out after 15 s",
                          elapsed_ms=int((time.time() - _t0) * 1000),
                          params={**params, "limit": limit, "page": page})
        return [], "Request timed out — Breezeway API did not respond within 15 s", last_status
    except Exception as ex:
        bw_api_log.record(path, None, False, detail=f"{type(ex).__name__}: {ex}",
                          elapsed_ms=int((time.time() - _t0) * 1000),
                          params={**params, "limit": limit, "page": page})
        return [], str(ex), last_status
    return all_results, "", last_status


def _fetch_bw_tasks(token: str, base_params: dict, date_param_sets: list = None) -> tuple:
    """Fetch Breezeway tasks for a date/property filter.
    Tries known task endpoint paths, then multiple date-param conventions.
    Returns (results, error_message).
    """
    # Working path FIRST. /admin/bw-probe confirmed the other three return 404 on
    # this Breezeway plan, and this function is called per-property by spi.py — so
    # with the old ordering every call burned two guaranteed-404 requests before
    # reaching the one that works, tripling the request count against an API that
    # is already rate-limiting us. Kept as fallbacks in case the plan changes, but
    # they now cost nothing on the happy path.
    candidate_paths = [
        "/public/inventory/v1/task",
        "/public/work/v1/task",
        "/public/work/v2/task",
        "/public/v1/task",
    ]
    # Date filter conventions to try in order
    if date_param_sets is None:
        date_param_sets = [{}]  # caller already merged date params into base_params

    last_err = "No task endpoint responded — task API may not be enabled on this Breezeway plan."

    for path in candidate_paths:
        # Quick probe: try base_params first
        results, err, status = _fetch_bw_endpoint(token, path, base_params)

        if status == 404:
            continue  # wrong path entirely, try next
        if status == 403:
            return [], ("Task data requires elevated API access on your Breezeway plan. "
                        "Contact Breezeway support to request task API access.")
        if status in (429, _LOCAL_THROTTLE):
            # Being throttled says nothing about which PATH is right, so walking the
            # remaining candidates cannot help — it just spends more of a budget we
            # have already exhausted. Same for a locally-shed request, which was
            # never sent at all. Stop and report it honestly.
            return [], (err or f"HTTP {status}: rate limited")
        if status and status not in (200, 422):
            last_err = err or f"HTTP {status}"
            continue

        if status == 200:
            return results, ""  # success with base params

        # 422 means this path exists but our params are wrong — try date_param_sets
        if status == 422:
            first_422_body = err  # preserve the raw Breezeway error from the probe call
            non_date = {k: v for k, v in base_params.items()
                        if not any(x in k for x in ("date", "start", "end"))}
            last_422_body = first_422_body
            for dp in date_param_sets:
                merged = {**non_date, **dp}
                res2, err2, st2 = _fetch_bw_endpoint(token, path, merged)
                if st2 == 200:
                    return res2, ""
                if st2 == 403:
                    return [], ("Task data requires elevated API access on your Breezeway plan.")
                if st2 == 422:
                    last_422_body = err2 or last_422_body
                last_err = err2 or f"HTTP {st2} on {path} with {dp}"
            # Fell through all param sets — surface the raw Breezeway error body
            last_err = (
                f"422 on {path} — all date-param formats rejected. "
                f"Last Breezeway error body: {last_422_body}"
            )
            break  # stop path-hunting — we found the real path, params are just wrong

        if err:
            last_err = err

    return [], last_err


_property_cache_error: str  = ""   # last error from property fetch, for diagnostics
_property_addr_cache:  dict = {}   # {property_id: address_string}
_property_ref_cache:   dict = {}   # {property_id: reference_property_id string} for task API


def _load_property_cache() -> str:
    """Fetch all Breezeway properties into _property_cache. Returns error string or ''."""
    global _property_cache, _property_addr_cache, _property_cache_ts, _property_cache_error
    global _property_ref_cache
    token = _get_breezeway_token()
    if not token:
        _property_cache_error = "No Breezeway token"
        return _property_cache_error
    try:
        page, limit = 1, 200
        fetched      = {}
        fetched_addr = {}
        fetched_ref  = {}
        while True:
            resp = bw_get(
                "https://api.breezeway.io/public/inventory/v1/property",
                headers={"Authorization": f"JWT {token}"},
                params={"limit": limit, "page": page, "status": "active"},
                timeout=15,
            )
            if not resp.ok:
                _property_cache_error = f"HTTP {resp.status_code}: {resp.text[:200]}"
                return _property_cache_error
            data = resp.json()
            items = (data.get("results", data.get("data", data if isinstance(data, list) else [])) or [])
            for p in items:
                pid  = p.get("id")
                raw_name = (p.get("name") or p.get("property_name") or
                            p.get("title") or p.get("display_name") or pid)
                name = raw_name if isinstance(raw_name, str) else str(pid)
                # Capture external reference ID for the task API (reference_property_id)
                ref_id = (p.get("reference_property_id") or p.get("reference_id") or
                          p.get("external_id") or p.get("external_property_id") or "")
                # Try several common address field names Breezeway might use
                addr = (p.get("address") or p.get("full_address") or
                        p.get("street_address") or p.get("location") or "")
                if isinstance(addr, dict):
                    # Some APIs return address as a nested object
                    parts = [
                        addr.get("street") or addr.get("line1") or "",
                        addr.get("city") or "",
                        addr.get("state") or "",
                    ]
                    addr = ", ".join(x for x in parts if x)
                if pid:
                    fetched[pid]      = name
                    fetched_addr[pid] = str(addr).strip()
                    if ref_id:
                        fetched_ref[pid] = str(ref_id)
            if len(items) < limit:
                break
            page += 1
        _property_cache      = fetched
        _property_addr_cache = fetched_addr
        _property_ref_cache  = fetched_ref
        _property_cache_ts   = time.time()
        _property_cache_error = ""
        return ""
    except Exception as e:
        _property_cache_error = f"{type(e).__name__}: {e}"
        return _property_cache_error


def _ensure_property_cache():
    if not _property_cache or time.time() - _property_cache_ts > 3600:
        _load_property_cache()


def _get_live_property_cache() -> dict:
    """Return the current _property_cache, refreshing if stale.
    Use this instead of importing _property_cache directly — a direct import
    captures the reference at import time and misses subsequent reassignments."""
    _ensure_property_cache()
    return _property_cache


def _get_live_ref_cache() -> dict:
    """Return the current _property_ref_cache, refreshing if stale."""
    _ensure_property_cache()
    return _property_ref_cache


def _ref_for(ref_cache, pid):
    """reference_property_id for a property id, whichever type either side is.

    The cache is keyed by whatever Breezeway returned from the property endpoint —
    `p.get("id")`, an int — while the scanners carry pids around as strings,
    because that is what reservation JSON and the database hand them. So
    `{858109: "..."}.get("858109")` missed on every house, every time, and the
    single-request path was simply unreachable: each property fell through to the
    id-form fallback and cost two calls instead of one, one of which Breezeway
    could only ever answer 422.

    Nothing looked broken, which is why it lasted — the fallback returns the right
    tasks. It just spent half the shared 180/min budget to do it.

    Lives here, next to the cache it reads, rather than in the module that first
    needed it. Originally written in carpet_scan.py, where it fixed exactly one
    caller while eight others went on missing.
    """
    if pid in ref_cache:
        return ref_cache[pid]
    s = str(pid)
    if s in ref_cache:
        return ref_cache[s]
    if s.isdigit() and int(s) in ref_cache:
        return ref_cache[int(s)]
    return ""


def _get_property_name(property_id) -> str:
    """Return a property's display name by Breezeway property_id, cached 1 hour."""
    if not property_id:
        return "Unknown Property"
    _ensure_property_cache()
    return _property_cache.get(property_id, f"Property {property_id}")


def _get_property_address(property_id) -> str:
    """Return a property's address by Breezeway property_id, cached 1 hour."""
    if not property_id:
        return ""
    _ensure_property_cache()
    return _property_addr_cache.get(property_id, "")


def _fetch_breezeway_checkins_status(date_str: str) -> tuple:
    """The day's check-in reservations as (results, error).

    This is the arrival signal: every "is this house a check-in today?" flag in the
    app is derived from it. When it comes back short, EVERY house silently reads as
    a non-arrival — a route full of check-ins renders as ordinary stops with nothing
    on screen to say the question was never answered. Callers must surface `error`
    rather than treat an empty list as "no arrivals today".

    The no-token path matters as much as the timeout: it returns before any HTTP
    request, so it leaves no row in the API log and is invisible to anyone reading
    that table trying to work out where the arrivals went.
    """
    token = _get_breezeway_token()
    if not token:
        return [], (_get_bw_token_last_error()
                    or "Could not authenticate with Breezeway")
    return _fetch_bw_reservations_status(token, {
        "checkin_date_ge": date_str, "checkin_date_le": date_str,
    })


def _fetch_breezeway_checkins(date_str: str) -> list:
    return _fetch_breezeway_checkins_status(date_str)[0]


def _fetch_breezeway_checkouts(date_str: str) -> list:
    token = _get_breezeway_token()
    if not token:
        return []
    return _fetch_bw_reservations(token, {
        "checkout_date_ge": date_str, "checkout_date_le": date_str,
    })


_BLOCK_TYPES = {"block", "maintenance", "hold", "owner_block", "management_block"}

def _extract_str(val) -> str:
    """Safely pull a lowercase string out of whatever Breezeway sends.
    type_stay / type_reservation are dicts like {"code": "owner", "name": "Owner Stay"}.
    Prefer 'code' — it is the machine-readable standardised value.
    Tags are {"id": int, "name": str} with no code field, so name is used as fallback.
    """
    if not val:
        return ""
    if isinstance(val, dict):
        return (val.get("code") or val.get("name") or
                val.get("label") or val.get("type") or "").lower().strip()
    return str(val).lower().strip()


def _tag_is_pci(tag) -> bool:
    """True when a reservation tag marks PCI. Matches 'PCI' as a standalone token
    (so 'PCI', 'PCI Priority', '(PCI)' all hit) — mirrors the app's task-title PCI
    convention so a word merely containing the letters can't false-positive."""
    s = _extract_str(tag)
    norm = "".join(c if c.isalnum() else " " for c in s)
    return "pci" in norm.split()


def _classify_reservation(r: dict) -> str:
    """Returns 'lease', 'owner', 'block', or 'guest'.

    Priority order:
      1. type_reservation.code == hold/block → block (overrides everything)
      2. type_stay.code == owner → owner
      3. Tag "Owner Next" → owner (manual marker for BW-miscategorised owner bookings)
      4. Duration >= 30 days → lease (applies to all non-owner, non-block guest stays)
      5. type_stay.code == lease → lease
      6. guest
    """
    ts = _extract_str(r.get("type_stay"))
    tr = _extract_str(r.get("type_reservation"))
    tag_names = [_extract_str(t) for t in (r.get("tags") or [])]

    # Holds/blocks take priority — even over Owner Next tag
    if tr in _BLOCK_TYPES or ts in _BLOCK_TYPES:
        return "block"

    # Owner stays
    if ts == "owner":
        return "owner"
    if "owner next" in tag_names:
        return "owner"

    # Duration check runs for ALL remaining reservations — a paying guest
    # stay of 30+ nights is a lease regardless of how Breezeway labels it
    checkin  = r.get("checkin_date")  or ""
    checkout = r.get("checkout_date") or ""
    if checkin and checkout:
        try:
            days = (date_cls.fromisoformat(checkout[:10]) -
                    date_cls.fromisoformat(checkin[:10])).days
            if days >= 30:
                return "lease"
        except Exception:
            pass

    if ts == "lease":
        return "lease"

    return "guest"


# Kept alongside _classify_reservation so both the Group Batcher and the map
# sidebar agree on "who's most present" when several stays span one day.
_OCC_PRIORITY = {"guest": 0, "lease": 1, "owner": 2, "block": 3}


def compute_occupancy_by_date(token: str, date_str: str) -> dict:
    """Who/what is STRICTLY mid-stay in each house on date_str (checkin < D < checkout)
    — present that night, not arriving or departing that day.

    Returns {str(property_id): {"kind": guest|lease|owner|block, "until": checkout ISO}}.

    Mirrors the occupancy block in routes/group_assign.py so the map sidebar shows
    the SAME fact as the Group Batcher. If several stays span the day, keep the most
    "present" one by _OCC_PRIORITY (guest < lease < owner < block)."""
    occupancy: dict = {}
    try:
        day_d = date_cls.fromisoformat(date_str)
    except (ValueError, TypeError):
        return occupancy
    for r in _fetch_bw_reservations(token, {"checkin_date_le": date_str,
                                            "checkout_date_ge": date_str}):
        kind = _classify_reservation(r)
        opid = r.get("property_id") or r.get("home_id")
        if opid is None:
            continue
        ci = (r.get("checkin_date") or "")[:10]
        co = (r.get("checkout_date") or "")[:10]
        try:
            if not (date_cls.fromisoformat(ci) < day_d < date_cls.fromisoformat(co)):
                continue
        except (ValueError, TypeError):
            continue
        key  = str(opid)
        prev = occupancy.get(key)
        if prev is None or _OCC_PRIORITY.get(kind, 9) < _OCC_PRIORITY.get(prev["kind"], 9):
            occupancy[key] = {"kind": kind, "until": co}
    return occupancy


def _fmt_time(hhmm: str) -> str:
    """Convert 'HH:MM:SS' or 'HH:MM' to '3:00 PM'."""
    try:
        parts = hhmm.split(":")
        h, m  = int(parts[0]), int(parts[1])
        return f"{h % 12 or 12}:{m:02d} {'AM' if h < 12 else 'PM'}"
    except Exception:
        return hhmm


def _guest_name(r: dict) -> str:
    guests = r.get("guests") or []
    if guests:
        g = guests[0]
        return f"{g.get('first_name','')} {g.get('last_name','')}".strip()
    return ""


# ── DB helpers ────────────────────────────────────────────────────

def _fetch_briefing_notes(date_str: str) -> str:
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("SELECT note_text FROM briefing_notes WHERE note_date = %s", (date_str,))
    row = cur.fetchone()
    cur.close(); conn.rollback(); conn.close()
    return (row["note_text"] or "").strip() if row else ""


def _fetch_todays_routes(date_str: str, team_id=None) -> list:
    conn = get_db()
    cur  = get_cursor(conn)
    if team_id:
        cur.execute(
            """SELECT r.id, r.name, r.assigned_to, r.stops_json, r.notes, u.name AS created_by_name
               FROM saved_routes r JOIN users u ON r.created_by = u.id
               WHERE r.route_date = %s AND r.team_id = %s AND COALESCE(r.archived, 0) = 0
               ORDER BY r.updated_at DESC""",
            (date_str, team_id),
        )
    else:
        cur.execute(
            """SELECT r.id, r.name, r.assigned_to, r.stops_json, r.notes, u.name AS created_by_name
               FROM saved_routes r JOIN users u ON r.created_by = u.id
               WHERE r.route_date = %s AND COALESCE(r.archived, 0) = 0
               ORDER BY r.updated_at DESC""",
            (date_str,),
        )
    rows = cur.fetchall()
    cur.close(); conn.rollback(); conn.close()
    return rows


# ── Claude briefing ───────────────────────────────────────────────

def _summarise_routes(routes: list) -> list:
    out = []
    for r in routes:
        stops    = [s for s in json.loads(r["stops_json"] or "[]") if not s.get("isLunch")]
        priority = sum(1 for s in stops if s.get("priority_checkin"))
        checkin  = sum(1 for s in stops if s.get("arrival") and not s.get("priority_checkin"))
        out.append({
            "id":          r["id"],
            "name":        r["name"],
            "assigned_to": r["assigned_to"] or "",
            "stops":       len(stops),
            "priority":    priority,
            "checkins":    checkin,
            "notes":       (r.get("notes") or "").strip(),
        })
    return out


def _build_prompt(date_str: str, routes: list, checkins: list,
                  checkouts: list, notes: str = "") -> str:
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    day_name = date_obj.strftime("%A, %B ") + str(date_obj.day)
    lines    = [f"Today is {day_name}.\n"]

    # Dispatcher notes — split into statements (facts) and instructions (commands)
    if notes:
        note_lines   = [l.strip() for l in notes.splitlines() if l.strip()]
        statements   = []
        instructions = []
        for nl in note_lines:
            # Heuristic: ends with '?' or starts with an imperative verb pattern → instruction
            lower = nl.lower()
            imperative_starts = ("describe", "include", "mention", "list", "show", "summarize",
                                 "note", "highlight", "report", "focus", "tell", "say", "do not",
                                 "never", "always", "make sure", "ensure")
            if nl.endswith("?") or any(lower.startswith(v) for v in imperative_starts):
                instructions.append(nl)
            else:
                statements.append(nl)
        if statements:
            lines.append("FACTS TO INCLUDE:\n" + "\n".join(f"- {s}" for s in statements))
        if instructions:
            lines.append("INSTRUCTIONS (follow these exactly):\n" + "\n".join(f"- {i}" for i in instructions))

    # Routes
    if routes:
        route_lines = []
        for r in routes:
            stops    = [s for s in json.loads(r["stops_json"] or "[]") if not s.get("isLunch")]
            n        = len(stops)
            priority = sum(1 for s in stops if s.get("priority_checkin"))
            checkin  = sum(1 for s in stops if s.get("arrival") and not s.get("priority_checkin"))
            line     = f'- "{r["name"]}"'
            if r["assigned_to"]:
                line += f' (assigned to {r["assigned_to"]})'
            line += f": {n} stop{'s' if n != 1 else ''}"
            if priority:
                line += f", {priority} priority check-in{'s' if priority != 1 else ''} (must finish by noon)"
            if checkin:
                line += f", {checkin} regular check-in{'s' if checkin != 1 else ''}"
            if (r.get("notes") or "").strip():
                line += f'. Notes: {r["notes"].strip()}'
            route_lines.append(line)
        lines.append(f"Dispatch routes ({len(routes)} total):\n" + "\n".join(route_lines))
    else:
        lines.append("No dispatch routes are saved for today.")

    # Breezeway arrivals — exclude blocks
    checkins  = [r for r in checkins  if _classify_reservation(r) != "block"]
    checkouts = [r for r in checkouts if _classify_reservation(r) != "block"]

    if checkins:
        counts = {"guest": 0, "owner": 0, "lease": 0, "block": 0}
        arr_lines = []
        for r in checkins:
            kind     = _classify_reservation(r)
            counts[kind] = counts.get(kind, 0) + 1
            prop     = _get_property_name(r.get("property_id"))
            t        = r.get("checkin_time", "")
            out_date = r.get("checkout_date", "")
            prefix   = {"lease": "[LEASE] ", "owner": "[OWNER] "}.get(kind, "")
            entry    = f"- {prefix}{prop}"
            if t:
                entry += f" — check-in at {_fmt_time(t)}"
            if out_date:
                entry += f" (checkout {out_date})"
            arr_lines.append(entry)

        total_arr = len(checkins)
        summary = []
        if counts["guest"]:  summary.append(f"{counts['guest']} guest arrival{'s' if counts['guest']!=1 else ''}")
        if counts["owner"]:  summary.append(f"{counts['owner']} owner stay{'s' if counts['owner']!=1 else ''}")
        if counts["lease"]:  summary.append(f"{counts['lease']} lease arrival{'s' if counts['lease']!=1 else ''}")
        head = f"TOTAL ARRIVALS TODAY: {total_arr}" + (f" ({', '.join(summary)})" if summary else "")
        lines.append(head + ":\n" + "\n".join(arr_lines))
    else:
        lines.append("TOTAL ARRIVALS TODAY: 0 — no arrivals scheduled.")

    if not checkins and not _get_breezeway_token():
        lines[-1] = "(Breezeway data not available — credentials not configured.)"

    return "\n\n".join(l for l in lines if l)


def _generate_briefing(date_str: str, routes: list, checkins: list,
                        checkouts: list, notes: str = "") -> tuple[str | None, str | None]:
    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        return None, "ANTHROPIC_API_KEY is not set."

    try:
        prompt = _build_prompt(date_str, routes, checkins, checkouts, notes)
        client = anthropic.Anthropic(api_key=key)
        msg    = client.messages.create(
            model      = "claude-opus-4-8",   # smartest model — briefing is tiny so cost is negligible
            max_tokens = 240,
            system     = (
                "You are a terse operations briefer for a vacation rental cleaning company "
                "in Lake Tahoe. Write ONE sentence using ONLY the data provided. Add a "
                "SECOND sentence ONLY when dispatcher notes (FACTS TO INCLUDE or INSTRUCTIONS) "
                "are present — that second sentence conveys the notes. If there are no notes, "
                "write only the one sentence.\n\n"
                "First sentence: ALWAYS lead with the TOTAL number of arrivals (check-ins) "
                "for the day from 'TOTAL ARRIVALS TODAY' (e.g. 'Thursday has 9 arrivals'), "
                "then state the single most operationally important fact — a priority "
                "check-in deadline, a lease or owner arrival, or anything that affects "
                "timing. You MAY also break the arrivals down per list (by assignee).\n"
                "Second sentence (only when notes are present): follow the INSTRUCTIONS "
                "exactly and weave in the FACTS TO INCLUDE.\n\n"
                "Rules:\n"
                "- One sentence, or two only when dispatcher notes are present. No bullet lists, no paragraphs.\n"
                "- ALWAYS state the day's TOTAL arrival count. You may also give per-list counts.\n"
                "- NEVER mention departures or checkouts — not relevant to operations.\n"
                "- NEVER characterize the workload. Do not use: heavy, busy, light, big, "
                "significant, demanding, packed, full, or any similar word.\n"
                "- Do not name individual properties — those are listed below. You MAY name "
                "each list (by its assignee) to give that list's arrival count.\n"
                "- Use the actual day name (e.g. 'Thursday') — never 'today'.\n"
                "- No greeting. Start with the fact."
            ),
            messages   = [{"role": "user", "content": prompt}],
        )
        return msg.content[0].text, None
    except Exception as e:
        import flask
        flask.current_app.logger.error(f"Briefing generation failed: {type(e).__name__}: {e}")
        return None, f"{type(e).__name__}: {e}"


# ── Endpoints ─────────────────────────────────────────────────────

@briefing_bp.route("/briefing/notes", methods=["GET"])
@login_required
def get_briefing_notes():
    date_str = request.args.get("date") or datetime.utcnow().strftime("%Y-%m-%d")
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        "SELECT note_text, staff_list, staff_updated_at FROM briefing_notes WHERE note_date = %s",
        (date_str,)
    )
    row = cur.fetchone()
    cur.close(); conn.close()

    staff_entries = []
    if row:
        raw = (row["staff_list"] or "").strip()
        if raw:
            try:
                parsed = json.loads(raw)
                staff_entries = parsed if isinstance(parsed, list) \
                    else [{"text": raw, "saved_at": row["staff_updated_at"] or ""}]
            except Exception:
                staff_entries = [{"text": raw, "saved_at": (row["staff_updated_at"] or "")}]

    # Staff plan is admin-only; notes stay visible to everyone.
    if not getattr(current_user, "is_admin", False):
        staff_entries = []

    return jsonify({
        "note_text":     (row["note_text"] or "").strip() if row else "",
        "staff_entries": staff_entries,
        "date":          date_str,
    })


@briefing_bp.route("/briefing/notes", methods=["POST"])
@login_required
def save_briefing_notes():
    data      = request.get_json(force=True)
    date_str  = (data.get("date") or datetime.utcnow().strftime("%Y-%m-%d")).strip()
    note_text = (data.get("note_text") or "").strip()
    now       = datetime.utcnow().isoformat()

    # Staff plan is admin-only; notes remain open to everyone.
    if "staff_list" in data and not getattr(current_user, "is_admin", False):
        return jsonify({"error": "Staff plan is admin-only."}), 403

    conn = get_db()
    cur  = get_cursor(conn)

    if "staff_list" in data:
        new_text = (data.get("staff_list") or "").strip()
        if new_text:
            cur.execute("SELECT staff_list FROM briefing_notes WHERE note_date = %s", (date_str,))
            existing_row = cur.fetchone()
            existing_raw = (existing_row["staff_list"] or "").strip() if existing_row else ""
            try:
                existing = json.loads(existing_raw) if existing_raw else []
                if not isinstance(existing, list):
                    existing = [{"text": existing_raw, "saved_at": now}] if existing_raw else []
            except Exception:
                existing = [{"text": existing_raw, "saved_at": now}] if existing_raw else []
            entries    = [{"text": new_text, "saved_at": now}] + existing
        else:
            entries = []
        staff_json = json.dumps(entries)
        cur.execute(
            """INSERT INTO briefing_notes (note_date, note_text, staff_list, staff_updated_at, updated_by, updated_at)
               VALUES (%s, %s, %s, %s, %s, %s)
               ON CONFLICT (note_date) DO UPDATE
               SET staff_list       = EXCLUDED.staff_list,
                   staff_updated_at = EXCLUDED.staff_updated_at,
                   updated_by       = EXCLUDED.updated_by,
                   updated_at       = EXCLUDED.updated_at""",
            (date_str, "", staff_json, now, current_user.id, now)
        )
    else:
        cur.execute(
            """INSERT INTO briefing_notes (note_date, note_text, updated_by, updated_at)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT (note_date) DO UPDATE
               SET note_text  = EXCLUDED.note_text,
                   updated_by = EXCLUDED.updated_by,
                   updated_at = EXCLUDED.updated_at""",
            (date_str, note_text, current_user.id, now)
        )

    conn.commit()
    cur.close(); conn.close()
    _briefing_cache.pop(date_str, None)
    return jsonify({"success": True})


@briefing_bp.route("/briefing")
@login_required
@admin_required
def daily_briefing():
    date_str      = request.args.get("date") or datetime.utcnow().strftime("%Y-%m-%d")
    team_id       = request.args.get("team_id") or None
    force_refresh = request.args.get("refresh") == "1"
    peek_only     = request.args.get("peek") == "1"   # return saved blurb only, no generation
    now           = time.time()

    try:
        cache_key = f"{date_str}:{team_id or ''}"

        # 1. In-memory cache (fast path)
        if not force_refresh and cache_key in _briefing_cache:
            ts, payload = _briefing_cache[cache_key]
            if now - ts < CACHE_TTL:
                return jsonify({**payload, "cached": True, "cached_at": _fmt_pacific(ts)})

        # 2. DB-persisted blurb (survives server restarts)
        if not force_refresh:
            conn = get_db()
            cur  = get_cursor(conn)
            cur.execute(
                "SELECT blurb, blurb_generated_at FROM briefing_notes "
                "WHERE note_date = %s AND blurb IS NOT NULL AND blurb != ''",
                (date_str,)
            )
            row = cur.fetchone()
            cur.close(); conn.close()
            if row and row["blurb"]:
                routes  = _fetch_todays_routes(date_str, team_id=team_id)
                payload = {"blurb": row["blurb"], "routes": _summarise_routes(routes)}
                _briefing_cache[cache_key] = (now, payload)
                return jsonify({**payload, "cached": True,
                                "cached_at": row["blurb_generated_at"] or ""})

        # 3. Peek mode — only return what's saved, never generate
        if peek_only:
            return jsonify({"blurb": None, "peek": True})

        # 4. Generate fresh
        routes    = _fetch_todays_routes(date_str, team_id=team_id)
        checkins  = _fetch_breezeway_checkins(date_str)
        checkouts = _fetch_breezeway_checkouts(date_str)
        notes     = _fetch_briefing_notes(date_str)
        blurb, err_msg = _generate_briefing(date_str, routes, checkins, checkouts, notes)

        if blurb:
            generated_at = _fmt_pacific(now)
            # Auto-save to DB so it persists across server restarts
            try:
                conn = get_db()
                cur  = get_cursor(conn)
                cur.execute(
                    """INSERT INTO briefing_notes (note_date, note_text, blurb, blurb_generated_at, updated_at)
                       VALUES (%s, '', %s, %s, %s)
                       ON CONFLICT (note_date) DO UPDATE
                       SET blurb = EXCLUDED.blurb,
                           blurb_generated_at = EXCLUDED.blurb_generated_at""",
                    (date_str, blurb, generated_at, datetime.utcnow().isoformat())
                )
                conn.commit()
                cur.close(); conn.close()
            except Exception:
                pass
            payload = {"blurb": blurb, "routes": _summarise_routes(routes)}
            _briefing_cache[cache_key] = (now, payload)
            return jsonify({**payload, "cached": False, "cached_at": generated_at})

        return jsonify({"blurb": None, "error": err_msg or "Unknown error generating briefing."})

    except Exception as e:
        import flask
        flask.current_app.logger.error(f"daily_briefing unhandled: {type(e).__name__}: {e}")
        return jsonify({"blurb": None, "error": f"Server error: {type(e).__name__}: {e}"}), 500


@briefing_bp.route("/briefing/day-summary")
@login_required
def day_summary():
    """Return arrivals and departures grouped by type for a given date.

    Priority: 1) saved DB snapshot  2) in-memory cache  3) live Breezeway fetch
    Pass ?refresh=1 to force a live re-fetch (overwrites neither DB nor cache automatically).

    A saved snapshot carries its owner-clean flags in `owner_cleaned`, so the page
    renders them with the list instead of firing a second, far more expensive request.
    """
    date_str = request.args.get("date") or datetime.utcnow().strftime("%Y-%m-%d")
    force    = request.args.get("refresh") == "1"

    # 1) Check DB snapshot first (unless force-refresh requested)
    if not force:
        try:
            conn = get_db()
            cur  = get_cursor(conn)
            cur.execute(
                "SELECT arrivals, departures, saved_at, owner_cleaned, owner_cleaned_at "
                "FROM saved_day_summaries WHERE route_date = %s",
                (date_str,)
            )
            row = cur.fetchone()
            cur.close(); conn.rollback(); conn.close()
            if row:
                payload = {
                    "date":       date_str,
                    "arrivals":   json.loads(row["arrivals"]),
                    "departures": json.loads(row["departures"]),
                    "cached_at":  row["saved_at"],
                    "source":     "saved",
                }
                if row["owner_cleaned"]:
                    payload["owner_cleaned"] = {
                        **json.loads(row["owner_cleaned"]),
                        "cached_at": row["owner_cleaned_at"],
                    }
                return jsonify(payload)
        except Exception:
            pass

    # 2) In-memory cache (10-min TTL — never serve a stale snapshot indefinitely)
    cached = _day_summary_cache.get(date_str)
    if cached and not force and (time.time() - cached[0]) < 600:
        ts, payload = cached
        return jsonify({**payload, "cached_at": _fmt_pacific(ts), "source": "live"})

    # 3) Live Breezeway fetch
    token = _get_breezeway_token()
    if not token:
        return jsonify({"arrivals": {}, "departures": {}, "cached_at": None, "source": "live"})

    checkins  = _fetch_bw_reservations(token, {
        "checkin_date_ge": date_str, "checkin_date_le": date_str,
    })
    checkouts = _fetch_bw_reservations(token, {
        "checkout_date_ge": date_str, "checkout_date_le": date_str,
    })

    arrivals, departures = _shape_day_summary(checkins, checkouts)

    payload = {"date": date_str, "arrivals": arrivals, "departures": departures}
    ts      = time.time()
    # Only cache NON-empty results. An empty fetch is usually a transient Breezeway
    # hiccup; caching it (previously with no TTL) is what made arrivals/departures
    # stick on "None" on every auto-load until a manual Refresh.
    if any(arrivals.values()) or any(departures.values()):
        _day_summary_cache[date_str] = (ts, payload)

    return jsonify({**payload, "cached_at": _fmt_pacific(ts), "source": "live"})


def _shape_day_summary(checkins: list, checkouts: list) -> tuple:
    """Turn raw reservations into the arrivals/departures shape the page renders.

    Split out of day_summary() so the overnight refresh job can build the same
    payload without going through the HTTP layer."""
    arrivals   = {"guest": [], "owner": [], "lease": []}
    departures = {"guest": [], "owner": [], "lease": []}

    for r in checkins:
        kind = _classify_reservation(r)
        if kind == "block":
            continue
        prop = _get_property_name(r.get("property_id"))
        t    = (r.get("checkin_time") or "")[:5]
        # Star arrivals whose reservation carries a VIP tag (same detection the
        # VIP tracker's scan uses). Only arrivals get the flag — departures don't.
        # A PCI tag gets a purple P — these are all same-day check-ins, so the
        # "PCI = same-day only" rule is satisfied by construction. Flags are
        # independent: an arrival can be VIP and PCI (and owner-cleaned) at once.
        tags   = r.get("tags") or []
        is_vip = any("vip" in _extract_str(tag) for tag in tags)
        is_pci = any(_tag_is_pci(tag) for tag in tags)
        arrivals.setdefault(kind, []).append({
            "name": prop, "time": t, "vip": is_vip, "pci": is_pci,
            "property_id": r.get("property_id"),
        })

    for r in checkouts:
        kind = _classify_reservation(r)
        if kind == "block":
            continue
        prop = _get_property_name(r.get("property_id"))
        t    = (r.get("checkout_time") or "")[:5]
        departures.setdefault(kind, []).append({"name": prop, "time": t})

    return arrivals, departures


# Dates the snapshot job could not write. Kept so the retry job knows what still
# needs doing — a date left unwritten falls through to a LIVE Breezeway fetch on
# every day-click for the rest of the day, which is the expensive behaviour this
# job exists to remove. Giving up after one failed attempt would leave exactly that.
_day_summary_pending: set = set()


def _dates_with_snapshots(dates: list) -> set:
    """Which of `dates` already have a stored snapshot. One cheap SQL query, no API
    calls — safe to run often."""
    from db import get_db, get_cursor
    if not dates:
        return set()
    try:
        conn = get_db(); cur = get_cursor(conn)
        try:
            cur.execute(
                "SELECT route_date FROM saved_day_summaries WHERE route_date = ANY(%s)",
                (list(dates),))
            return {r["route_date"] for r in cur.fetchall()}
        finally:
            cur.close(); conn.rollback(); conn.close()
    except Exception:
        return set()


def _write_day_summary(date_str: str, arrivals: dict, departures: dict) -> None:
    from db import get_db, get_cursor
    conn = get_db(); cur = get_cursor(conn)
    try:
        cur.execute("""
            INSERT INTO saved_day_summaries
                   (route_date, arrivals, departures, saved_by, saved_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (route_date) DO UPDATE
              SET arrivals   = EXCLUDED.arrivals,
                  departures = EXCLUDED.departures,
                  saved_by   = EXCLUDED.saved_by,
                  saved_at   = EXCLUDED.saved_at
        """, (date_str, json.dumps(arrivals), json.dumps(departures),
              None, _fmt_pacific(time.time())))
        conn.commit()
    finally:
        cur.close(); conn.close()
    _day_summary_cache.pop(date_str, None)   # next read comes from the DB


def _dates_with_owner_cleaned(dates: list) -> set:
    """Which of `dates` already have a stored owner-clean scan. A date can have its
    arrivals stored but not its scan — the fan-out is far more likely to be throttled
    than the two reservation calls — so this is asked separately from
    _dates_with_snapshots, letting the retry job redo only the missing half."""
    from db import get_db, get_cursor
    if not dates:
        return set()
    try:
        conn = get_db(); cur = get_cursor(conn)
        try:
            cur.execute(
                "SELECT route_date FROM saved_day_summaries "
                "WHERE route_date = ANY(%s) AND owner_cleaned IS NOT NULL",
                (list(dates),))
            return {r["route_date"] for r in cur.fetchall()}
        finally:
            cur.close(); conn.rollback(); conn.close()
    except Exception:
        return set()


def _read_owner_cleaned(date_str: str) -> dict:
    """The stored owner-clean scan for a date, or {} when there isn't one."""
    from db import get_db, get_cursor
    try:
        conn = get_db(); cur = get_cursor(conn)
        try:
            cur.execute("SELECT owner_cleaned, owner_cleaned_at FROM saved_day_summaries "
                        "WHERE route_date = %s", (date_str,))
            row = cur.fetchone()
        finally:
            cur.close(); conn.rollback(); conn.close()
        if not row or not row["owner_cleaned"]:
            return {}
        return {**json.loads(row["owner_cleaned"]), "cached_at": row["owner_cleaned_at"]}
    except Exception:
        return {}


def _write_owner_cleaned(date_str: str, payload: dict) -> None:
    """Store a COMPLETE owner-clean scan next to the date's arrivals.

    UPDATE only, never INSERT: a row here is what makes day_summary() serve a date
    from the DB instead of Breezeway, so creating one with the empty arrivals default
    would pin "None" for the whole day. No arrivals row yet means the snapshot job
    hasn't gotten to this date — it will write both together when it does."""
    from db import get_db, get_cursor
    conn = get_db(); cur = get_cursor(conn)
    try:
        cur.execute("""
            UPDATE saved_day_summaries
               SET owner_cleaned = %s, owner_cleaned_at = %s
             WHERE route_date = %s
        """, (json.dumps(payload), _fmt_pacific(time.time()), date_str))
        conn.commit()
    finally:
        cur.close(); conn.close()


def refresh_day_summaries(days: int = 8, only_missing: bool = False,
                          attempts: int = 2) -> dict:
    """Build and store arrivals/departures snapshots for today and the next `days`.

    The Saved Routes page prefers a stored snapshot and skips Breezeway entirely
    when one exists, so filling this table is what keeps day-clicks off the API.
    A date left unwritten does the opposite: it hits Breezeway live on every click
    for the rest of the day.

    only_missing=True does just the dates with no snapshot yet (plus anything a
    previous run failed on), so the retry job costs one SQL query and zero API
    calls once everything is covered.

    Never stores a snapshot whose fetch errored or came back partial — persisting
    an empty result would pin "None" for the whole day, the same bug made
    permanent. Those dates go to _day_summary_pending for the retry job.

    Never raises: a scheduled job that throws kills the scheduler thread."""
    out = {"saved": [], "skipped": [], "errors": [], "pending": []}
    today   = date_cls.today()
    targets = [(today + timedelta(days=i)).isoformat() for i in range(days)]

    have_summary, have_oc = set(), set()
    if only_missing:
        have_summary = _dates_with_snapshots(targets)
        have_oc      = _dates_with_owner_cleaned(targets)
        targets = [d for d in targets
                   if d not in have_summary or d not in have_oc
                   or d in _day_summary_pending]
        if not targets:
            return out                      # everything covered — no API calls at all

    token = _get_breezeway_token()
    if not token:
        # Don't clear pending: without a token nothing was attempted, so the next
        # run must still try these.
        _day_summary_pending.update(targets)
        out["errors"].append(f"no Breezeway token: {_get_bw_token_last_error()}")
        out["pending"] = sorted(_day_summary_pending)
        return out

    for d in targets:
        # A date can be half-covered: the arrivals stored fine this morning but the
        # owner-clean fan-out got throttled. Don't re-fetch reservations we already
        # have — go straight to the scan below.
        stored   = only_missing and d in have_summary and d not in _day_summary_pending
        wrote    = stored
        checkins = None

        if not wrote:
            for attempt in range(max(1, attempts)):
                try:
                    checkins  = _fetch_bw_reservations(token, {
                        "checkin_date_ge": d, "checkin_date_le": d})
                    err_in    = _get_bw_resv_last_error()
                    checkouts = _fetch_bw_reservations(token, {
                        "checkout_date_ge": d, "checkout_date_le": d})
                    err_out   = _get_bw_resv_last_error()

                    if err_in or err_out:
                        # Throttled or partial. One more try after a pause; beyond that
                        # the date stays pending for a later scheduled catch-up rather
                        # than hammering the same query now — Breezeway's limit resets
                        # on the order of a minute, so retrying harder here won't help.
                        checkins = None
                        if attempt < attempts - 1:
                            time.sleep(10.0)
                            continue
                        out["skipped"].append(f"{d}: fetch incomplete ({err_in or err_out})")
                        break

                    arrivals, departures = _shape_day_summary(checkins, checkouts)
                    _write_day_summary(d, arrivals, departures)
                    wrote = True
                    out["saved"].append(
                        f"{d}: {sum(len(v) for v in arrivals.values())} arrivals, "
                        f"{sum(len(v) for v in departures.values())} departures")
                    break
                except Exception as ex:
                    checkins = None
                    if attempt < attempts - 1:
                        time.sleep(10.0)
                        continue
                    out["errors"].append(f"{d}: {type(ex).__name__}: {ex}")

            if wrote:
                _day_summary_pending.discard(d)
            else:
                _day_summary_pending.add(d)  # the retry job will keep at it

        # The owner-clean flags ride along with the list they annotate, so opening
        # the page costs nothing. Same rule as the snapshot itself: store only a
        # COMPLETE scan — a partial one would pin missing flags for the day, and
        # leaving it unstored is what keeps the date in the retry job's sights.
        # Needs the arrivals row to exist (_write_owner_cleaned only UPDATEs), hence
        # `wrote`. Fewer workers than the interactive path: this runs eight dates
        # back to back against a shared rate limit, where a day-click runs one.
        #
        # `not stored` covers the case where a retry just REPLACED the arrivals with
        # freshly fetched ones: whatever flags are on that row describe the old list
        # (today's row is written a day ahead, so they can be a day stale), and they
        # have to be rebuilt with it — not skipped because the column is non-NULL.
        if wrote and (not stored or d not in have_oc):
            try:
                oc = _scan_owner_cleaned(token, d, date_cls.fromisoformat(d),
                                         checkins=checkins,
                                         max_workers=_OWNER_CLEAN_JOB_WORKERS)
                if oc.get("failed_properties"):
                    out["skipped"].append(
                        f"{d}: owner-clean scan incomplete — "
                        f"{oc['failed_properties']} of {oc['scanned']} houses "
                        f"({oc.get('failure_statuses') or 'no status'})")
                else:
                    _write_owner_cleaned(d, oc)
                    _owner_cleaned_cache.pop(d, None)   # next read comes from the DB
                    out["saved"].append(
                        f"{d}: {len(oc['flagged'])} owner-cleaned of {oc['scanned']} arrivals")
            except Exception as ex:
                out["errors"].append(f"{d}: owner-clean scan {type(ex).__name__}: {ex}")

    out["pending"] = sorted(_day_summary_pending)
    return out


@briefing_bp.route("/briefing/save-day-summary", methods=["POST"])
@login_required
def save_day_summary():
    """Persist the current day's arrivals/departures snapshot to the DB."""
    data       = request.get_json(force=True)
    date_str   = (data.get("date") or "").strip()
    arrivals   = data.get("arrivals")
    departures = data.get("departures")

    if not date_str or arrivals is None or departures is None:
        return jsonify({"success": False, "error": "date, arrivals, and departures required"}), 400

    saved_at = _fmt_pacific(time.time())
    try:
        conn = get_db()
        cur  = get_cursor(conn)
        cur.execute("""
            INSERT INTO saved_day_summaries (route_date, arrivals, departures, saved_by, saved_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (route_date) DO UPDATE
              SET arrivals   = EXCLUDED.arrivals,
                  departures = EXCLUDED.departures,
                  saved_by   = EXCLUDED.saved_by,
                  saved_at   = EXCLUDED.saved_at
        """, (date_str, json.dumps(arrivals), json.dumps(departures),
              current_user.id, saved_at))
        conn.commit()
        cur.close(); conn.close()
        # Bust in-memory cache so next load comes from DB
        _day_summary_cache.pop(date_str, None)
        return jsonify({"success": True, "saved_at": saved_at})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ── Owner-cleaned arrival check ───────────────────────────────────
#
# She manages the inspectors. When the LAST clean at an arriving house was the
# owner handling it themselves (a Breezeway task literally titled "Owner Cleaned"),
# she wants to schedule that house's Walk Thru earlier in the day — an owner clean
# needs a closer look than a staff turnover. This scan finds those houses so the
# arrivals list can flag them.

def _is_owner_cleaned_title(title: str) -> bool:
    """True when a title contains the owner-handled clean as a PHRASE, anywhere.
    'Owner Cleaned 3/15', 'Walk Thru - Owner Cleaned' and plain 'Owner Clean' all
    match, so a dated or prefixed title no longer slips through unflagged.

    The two words must be ADJACENT, and that adjacency is the whole safeguard: it
    is what still rejects the standard 'Owner Departure Clean' turnover task, which
    is NOT owner-handled and must never raise the flag. A loose 'owner' + 'clean'
    substring test would wrongly catch it.

    Punctuation is a separator, so 'Walk Thru - Owner Cleaned', '(Owner Cleaned)'
    and 'Owner-Cleaned' all match. Same normalisation the PCI title rule uses."""
    t = " " + re.sub(r"[^a-z0-9]+", " ", (title or "").lower()).strip() + " "
    return (" owner clean " in t
            or " owner cleaned " in t
            or " owner cleaning " in t)


def _task_dept_str(task: dict) -> str:
    """Raw department label for a task (for diagnostics/logging)."""
    dept = task.get("type_department")
    if isinstance(dept, dict):
        dept = dept.get("code") or dept.get("name") or ""
    return str(dept)


def _is_cleaning_dept(task: dict) -> bool:
    """True when a Breezeway task belongs to the cleaning/housekeeping department.
    Same normalization group_assign / assignee_monitor use."""
    dl = _task_dept_str(task).strip().lower()
    return "clean" in dl or "housekeep" in dl


def _robust_property_tasks_window(token, ref_id, start_str, end_str):
    """Fetch ONE property's tasks over a date window with retry/backoff, mirroring
    occupancy_check's single-day fetcher so a momentary Breezeway throttle
    (429 / 5xx) doesn't silently drop the property.
    Returns (tasks, ok, status); ok=False means it genuinely couldn't be loaded,
    and `status` is the HTTP status of the final failed attempt (None = no
    response/timeout) so the UI can name the cause instead of assuming a throttle."""
    status = None
    for attempt in range(3):
        r, _, status = _fetch_bw_endpoint(
            token, "/public/inventory/v1/task",
            {"reference_property_id": ref_id, "scheduled_date": f"{start_str},{end_str}"})
        if status == 200:
            return (r or [], True, status)
        if status is None or status == 429 or status >= 500:
            time.sleep(0.3 * (attempt + 1))
            continue
        r2, _, st2 = _fetch_bw_endpoint(
            token, "/public/inventory/v1/task",
            {"reference_property_id": ref_id, "start_date": start_str, "end_date": end_str})
        return (r2 or [], True, st2) if st2 == 200 else ([], False, st2)
    return ([], False, status)


def _last_clean_is_owner(tasks: list, arrival_date, trace=None) -> bool:
    """Given a property's tasks and its arrival date, decide whether the LAST clean
    on or before arrival was an 'Owner Cleaned' task.

    Look-back is tiered so a recent turnover is preferred over a stale one: try the
    3 days up to arrival, then 7, then the full 14 — the first tier that contains
    any cleaning task wins. Within that tier the LATEST scheduled date is 'the last
    clean'. Flag only when that date's cleaning tasks are owner-cleaned AND carry no
    competing staff clean (a same-day staff clean supersedes the owner one).

    If `trace` (a dict) is passed, it's filled with the diagnostic breakdown that
    drove the decision — used by the endpoint's debug=1 mode, no behavior change."""
    # Keep only cleaning-department tasks scheduled on/before the arrival day,
    # tagged with their parsed date.
    dated = []
    for t in tasks:
        if not _is_cleaning_dept(t):
            continue
        ds = (t.get("scheduled_date") or "")[:10]
        try:
            d = date_cls.fromisoformat(ds)
        except Exception:
            continue
        if d > arrival_date:
            continue
        dated.append((d, t))
    if trace is not None:
        trace["cleaning_tasks"] = sorted(
            ({"date": d.isoformat(), "title": _get_task_title(t),
              "dept": _task_dept_str(t)} for d, t in dated),
            key=lambda x: x["date"])
    if not dated:
        if trace is not None:
            trace["decision"] = "no cleaning tasks in 14-day window"
        return False

    for window in (3, 7, 14):
        cutoff = arrival_date - timedelta(days=window)
        tier = [(d, t) for (d, t) in dated if d >= cutoff]
        if not tier:
            continue
        latest = max(d for d, _ in tier)
        latest_tasks = [t for d, t in tier if d == latest]
        owner = any(_is_owner_cleaned_title(_get_task_title(t)) for t in latest_tasks)
        other = any(not _is_owner_cleaned_title(_get_task_title(t)) for t in latest_tasks)
        if trace is not None:
            trace["window_used"]  = window
            trace["latest_clean"] = latest.isoformat()
            trace["latest_tasks"] = [
                {"title": _get_task_title(t), "dept": _task_dept_str(t),
                 "is_owner_cleaned": _is_owner_cleaned_title(_get_task_title(t))}
                for t in latest_tasks]
            trace["owner"] = owner
            trace["other"] = other
            trace["flagged"] = owner and not other
        return owner and not other
    return False


def _get_task_title(t: dict) -> str:
    """Pull a task's display title, same field order the rest of the app uses."""
    title = (t.get("name") or t.get("task_name") or t.get("task_type") or t.get("type") or "")
    if isinstance(title, dict):
        title = title.get("value") or title.get("name") or ""
    return str(title)


def _scan_owner_cleaned(token, date_str: str, arrival_date, checkins: list = None,
                        debug: bool = False, max_workers: int = 32) -> dict:
    """Run the owner-clean fan-out for one date and return the payload.

    Split out of the endpoint so the morning snapshot job can build the same result
    without going through HTTP. This is the expensive half of the arrivals panel —
    one Breezeway task call per arriving house — which is exactly why it belongs in
    the overnight run rather than on every page open.

    Pass `checkins` when the caller already fetched the date's reservations so this
    doesn't fetch them a second time.
    Returns {date, flagged: [{property_id, name}], scanned, failed_properties,
             failure_statuses}.
    """
    from concurrent.futures import ThreadPoolExecutor

    _ensure_property_cache()
    if checkins is None:
        checkins = _fetch_bw_reservations(token, {
            "checkin_date_ge": date_str, "checkin_date_le": date_str,
        })

    # property_id -> display name, for each non-block arrival
    arriving = {}
    for r in checkins:
        if _classify_reservation(r) == "block":
            continue
        pid = r.get("property_id")
        if pid:
            arriving[pid] = _get_property_name(pid)

    if not arriving:
        # No arrivals is a complete answer, not a partial one.
        return {"date": date_str, "flagged": [], "scanned": 0,
                "failed_properties": 0, "failure_statuses": {}}

    ref_cache = _get_live_ref_cache()
    start_str = (arrival_date - timedelta(days=14)).isoformat()

    def _job(pid):
        ref = _ref_for(ref_cache, pid) or str(pid)
        tasks, ok, status = _robust_property_tasks_window(token, ref, start_str, date_str)
        if not ok:
            # None == load failed, distinct from False (loaded, not flagged).
            # Carry the status so the caller can tally WHY it failed.
            return (pid, None, None, status)
        tr = {} if debug else None
        return (pid, _last_clean_is_owner(tasks, arrival_date, trace=tr), tr, status)

    flagged, failed, debug_details = [], 0, []
    failure_statuses: dict = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for pid, result, tr, status in ex.map(_job, list(arriving.keys())):
            if result is None:
                failed += 1
                k = "timeout" if status is None else str(status)
                failure_statuses[k] = failure_statuses.get(k, 0) + 1
            elif result:
                flagged.append({"property_id": pid, "name": arriving[pid]})
            # Debug: surface only houses that HAD a cleaning task suppressed by a
            # same-day non-owner task (owner present but not flagged) — the suspects.
            if debug and tr is not None and tr.get("owner") and not tr.get("flagged"):
                debug_details.append({"property_id": pid, "name": arriving[pid], **tr})

    payload = {
        "date":              date_str,
        "flagged":           flagged,
        "scanned":           len(arriving),
        "failed_properties": failed,
        "failure_statuses":  failure_statuses,
    }
    if debug:
        payload["debug_suppressed"] = debug_details
    return payload


@briefing_bp.route("/briefing/owner-cleaned-check")
@login_required
def owner_cleaned_check():
    """Owner-clean flags for one date's arrivals.

    Priority: 1) the scan stored by the morning snapshot job  2) in-memory cache
    3) a live fan-out. The stored scan is the normal path and costs no API calls;
    the live fan-out is the fallback for a date the job hasn't covered yet, and
    what ?refresh=1 forces.
    """
    date_str = request.args.get("date") or datetime.utcnow().strftime("%Y-%m-%d")
    debug    = request.args.get("debug") == "1"
    force    = request.args.get("refresh") == "1" or debug

    # 1) Stored scan — built at 5:30am alongside the arrivals list.
    if not force:
        stored = _read_owner_cleaned(date_str)
        if stored:
            return jsonify({**stored, "source": "saved"})

    # 2) Entries carry their own TTL — a complete scan is worth holding for 10 minutes,
    # an incomplete one only briefly (see the write site below).
    cached = _owner_cleaned_cache.get(date_str)
    if cached and not force:
        _ttl = cached[2] if len(cached) > 2 else _OWNER_CLEAN_TTL
        if (time.time() - cached[0]) < _ttl:
            return jsonify({**cached[1], "cached_at": _fmt_pacific(cached[0]),
                            "source": "live"})

    try:
        arrival_date = date_cls.fromisoformat(date_str)
    except Exception:
        return jsonify({"error": f"Bad date: {date_str}"}), 400

    token = _get_breezeway_token()
    if not token:
        return jsonify({"date": date_str, "flagged": [], "scanned": 0,
                        "failed_properties": 0, "error": "Breezeway not configured."})

    # 3) Live fan-out.
    payload = _scan_owner_cleaned(token, date_str, arrival_date, debug=debug)
    failed  = payload.get("failed_properties", 0)
    # A clean scan is worth holding for 10 minutes. An incomplete one used to be
    # discarded entirely — correct in spirit (don't pin missing flags) but it meant
    # that while Breezeway is throttling, when this scan ALWAYS has failures, every
    # day-click re-ran the full ~47-call fan-out. Clicking through five days cost
    # ~250 requests against the budget that was causing the failures.
    # Hold a partial result just long enough to absorb clicking around, not long
    # enough to hide a gap from someone who comes back to check.
    # Never cache a debug run (it forces fresh and carries extra payload).
    if not debug:
        _owner_cleaned_cache[date_str] = (
            time.time(), payload,
            _OWNER_CLEAN_TTL if not failed else _OWNER_CLEAN_PARTIAL_TTL)
    # A complete live scan is worth persisting too — a date the job missed then
    # stops costing every later visitor the same fan-out.
    if not debug and not failed:
        _write_owner_cleaned(date_str, payload)
    return jsonify({**payload, "cached_at": _fmt_pacific(time.time()), "source": "live"})


@briefing_bp.route("/briefing/property-status")
@login_required
def property_status():
    """Return current occupancy status + upcoming bookings for one property (by name).
    Results cached 20 minutes per property — zero cost on repeat clicks.
    """
    prop_name = (request.args.get("name") or "").strip()
    if not prop_name:
        return jsonify({"error": "name required"}), 400

    token = _get_breezeway_token()
    if not token:
        return jsonify({"error": "Breezeway not configured"}), 500

    # Reverse-lookup: name → property_id using the existing property cache
    _ensure_property_cache()
    pid = next((k for k, v in _property_cache.items()
                if v.lower() == prop_name.lower()), None)
    if not pid:
        return jsonify({"error": f"Property not found in Breezeway: {prop_name}"}), 404

    # Serve from cache if fresh
    cached = _prop_status_cache.get(pid)
    if cached and time.time() - cached[0] < _PROP_STATUS_TTL:
        return jsonify(cached[1])

    today     = date_cls.today()
    today_str = today.isoformat()
    end_str   = (today + timedelta(days=90)).isoformat()

    raw = _fetch_bw_reservations(token, {
        "checkin_date_ge":  today_str,
        "checkin_date_le":  end_str,
    }) + _fetch_bw_reservations(token, {
        "checkout_date_ge": today_str,
        "checkout_date_le": end_str,
    })

    # Deduplicate by reservation id and filter to this property
    seen = set()
    prop_res = []
    for r in raw:
        rid = r.get("id")
        if r.get("property_id") == pid and rid not in seen:
            seen.add(rid)
            prop_res.append(r)
    prop_res.sort(key=lambda r: (r.get("checkin_date") or ""))

    # Determine current status
    status      = "vacant"
    status_kind = None
    checkout_today = None
    checkin_today  = None

    for r in prop_res:
        ci = (r.get("checkin_date")  or "")[:10]
        co = (r.get("checkout_date") or "")[:10]
        kind = _classify_reservation(r)
        if ci <= today_str <= co:
            status      = "occupied"
            status_kind = kind
        if co == today_str:
            checkout_today = kind
        if ci == today_str:
            checkin_today = kind

    # Build upcoming list (next 5 bookings starting from today or later)
    upcoming = []
    for r in prop_res:
        ci = (r.get("checkin_date")  or "")[:10]
        co = (r.get("checkout_date") or "")[:10]
        if co < today_str:
            continue
        upcoming.append({
            "type":     _classify_reservation(r),
            "checkin":  ci,
            "checkout": co,
        })
        if len(upcoming) >= 5:
            break

    # Days until next booking (if currently vacant)
    days_until_next = None
    if status == "vacant" and upcoming:
        try:
            days_until_next = (date_cls.fromisoformat(upcoming[0]["checkin"]) - today).days
        except Exception:
            pass

    payload = {
        "property":       prop_name,
        "status":         status,        # "occupied" | "vacant"
        "status_kind":    status_kind,   # "guest" | "owner" | "lease" | None
        "checkout_today": checkout_today,
        "checkin_today":  checkin_today,
        "days_until_next": days_until_next,
        "upcoming":       upcoming,
    }
    _prop_status_cache[pid] = (time.time(), payload)
    return jsonify(payload)


@briefing_bp.route("/briefing/debug-reservations")
@login_required
@admin_required
def debug_reservations():
    """Return raw Breezeway reservation fields for a date — for diagnosing classification and discovering field names."""
    try:
        date_str  = request.args.get("date") or datetime.utcnow().strftime("%Y-%m-%d")
        checkins  = _fetch_breezeway_checkins(date_str)
        checkouts = _fetch_breezeway_checkouts(date_str)

        def safe(v):
            """Convert any value to something jsonify can handle."""
            try:
                json.dumps(v)
                return v
            except (TypeError, ValueError):
                return str(v)

        def summarise(rs):
            out = []
            for r in rs:
                # Dump every top-level key with a safe-serialized value
                raw_safe = {k: safe(v) for k, v in r.items()}
                out.append({
                    "classified_as":   _classify_reservation(r),
                    "property_id":     r.get("property_id"),
                    "property_name":   _get_property_name(r.get("property_id")),
                    "type_stay":       r.get("type_stay"),
                    "type_reservation":r.get("type_reservation"),
                    "tags":            r.get("tags"),
                    "checkin_date":    r.get("checkin_date"),
                    "checkout_date":   r.get("checkout_date"),
                    "checkin_time":    r.get("checkin_time"),
                    "checkout_time":   r.get("checkout_time"),
                    "guest_name":      _guest_name(r),
                    "_all_keys":       list(r.keys()),
                    "_raw":            raw_safe,
                })
            return out

        return jsonify({
            "date":      date_str,
            "checkins":  summarise(checkins),
            "checkouts": summarise(checkouts),
        })
    except Exception as e:
        return jsonify({"error": str(e), "error_type": type(e).__name__}), 500


@briefing_bp.route("/briefing/debug-properties")
@login_required
@admin_required
def debug_properties():
    """Show Breezeway property cache state and raw fields from one property."""
    token = _get_breezeway_token()
    raw_sample = None
    if token:
        try:
            resp = bw_get(
                "https://api.breezeway.io/public/inventory/v1/property",
                headers={"Authorization": f"JWT {token}"},
                params={"limit": 1, "page": 1},
                timeout=15,
            )
            data  = resp.json()
            items = data.get("results", data.get("data", data if isinstance(data, list) else []))
            if items:
                raw_sample = items[0]
        except Exception as e:
            raw_sample = {"error": str(e)}

    err = _load_property_cache()
    return jsonify({
        "property_count":    len(_property_cache),
        "cache_error":       err or None,
        "sample_names":      dict(list(_property_cache.items())[:5]),
        "sample_addresses":  dict(list(_property_addr_cache.items())[:5]),
        "raw_fields_sample": raw_sample,
    })


@briefing_bp.route("/briefing/calendar-activity")
@login_required
def calendar_activity():
    """Return arrival/departure/lease counts per date for a given month."""
    try:
        year  = int(request.args.get("year",  datetime.utcnow().year))
        month = int(request.args.get("month", datetime.utcnow().month))
    except ValueError:
        return jsonify({}), 400

    now       = time.time()
    cache_key = (year, month)
    if cache_key in _calendar_cache:
        ts, data = _calendar_cache[cache_key]
        if now - ts < CALENDAR_CACHE_TTL:
            return jsonify(data)

    token = _get_breezeway_token()
    if not token:
        return jsonify({})

    last_day = cal_mod.monthrange(year, month)[1]
    first_ds = f"{year}-{month:02d}-01"
    last_ds  = f"{year}-{month:02d}-{last_day:02d}"

    activity: dict = {}

    def ensure(ds):
        if ds not in activity:
            activity[ds] = {"arrivals": 0, "departures": 0, "leases": 0}

    # Single API call: all reservations that overlap this month.
    # checkin_date_le=last_ds  → checked in before month end
    # checkout_date_ge=first_ds → checked out after month start
    # Together they select every stay with any overlap with this month.
    for r in _fetch_bw_reservations(token, {
        "checkin_date_le":  last_ds,
        "checkout_date_ge": first_ds,
    }):
        checkin_ds  = r.get("checkin_date",  "") or ""
        checkout_ds = r.get("checkout_date", "") or ""
        kind = _classify_reservation(r)

        if kind == "block":
            continue  # blocks are internal holds, not real guest/owner activity

        if first_ds <= checkin_ds <= last_ds:
            ensure(checkin_ds)
            activity[checkin_ds]["arrivals"] += 1
            if kind == "lease":
                activity[checkin_ds]["leases"] += 1

        if first_ds <= checkout_ds <= last_ds:
            ensure(checkout_ds)
            activity[checkout_ds]["departures"] += 1

    # Only cache non-empty results; an empty response likely means the API
    # call failed or timed out, and we want the next navigation to retry.
    if activity:
        _calendar_cache[cache_key] = (now, activity)
    return jsonify(activity)


@briefing_bp.route("/briefing/reservation-chart")
@login_required
def reservation_chart():
    """Per-day arrival counts by reservation type (guest/owner/lease/block) for a
    date range. Powers the bar chart on the saved routes page."""
    today = date_cls.today()
    try:
        start = date_cls.fromisoformat(request.args["start_date"]) if request.args.get("start_date") else today
    except Exception:
        start = today
    try:
        end = date_cls.fromisoformat(request.args["end_date"]) if request.args.get("end_date") else start + timedelta(days=6)
    except Exception:
        end = start + timedelta(days=6)
    if end < start:
        start, end = end, start
    if (end - start).days > 60:          # bound the API work
        end = start + timedelta(days=60)

    token = _get_breezeway_token()
    if not token:
        return jsonify({"error": "Breezeway not configured."}), 500

    raw = _fetch_bw_reservations(token, {
        "checkin_date_ge": start.isoformat(),
        "checkin_date_le": end.isoformat(),
    })

    dates, d = [], start
    while d <= end:
        dates.append(d.isoformat())
        d += timedelta(days=1)
    counts = {ds: {"guest": 0, "owner": 0, "lease": 0, "block": 0} for ds in dates}

    for r in raw:
        ci = (r.get("checkin_date") or "")[:10]
        if ci not in counts:
            continue
        kind = _classify_reservation(r)
        if kind in counts[ci]:
            counts[ci][kind] += 1

    return jsonify({
        "start": start.isoformat(),
        "end":   end.isoformat(),
        "dates": dates,
        "guest": [counts[ds]["guest"] for ds in dates],
        "owner": [counts[ds]["owner"] for ds in dates],
        "lease": [counts[ds]["lease"] for ds in dates],
        "block": [counts[ds]["block"] for ds in dates],
    })
