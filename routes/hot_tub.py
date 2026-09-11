"""
routes/hot_tub.py — Hot Tub service overdue scanner.

Applies to properties tagged "Hot Tub - TG Service" in Breezeway, OR
properties with a current 30+ night guest lease that also carry a plain
"Hot Tub" tag.
Looks back 45 days for tasks whose title contains "hot tub" AND
("arrival" OR "biweekly" OR "mid stay" OR …). Alerts on any property where the last
service was more than 14 days ago (or never found in the window).

Also flags "too close" services: any two hot tub services on the same
property scheduled within 6 days of each other (likely an accidental
double-booking), looking 45 days back and 45 days forward.

Endpoints:
  GET  /admin/hot-tub        — page
  POST /admin/hot-tub/scan   — scan and return results (JSON)
"""

import re
import requests
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required

from routes.auth import admin_required

hot_tub_bp = Blueprint("hot_tub", __name__)

BW_BASE = "https://api.breezeway.io"

# Scan-result cache: the all-properties tag sweep can run past the hosting proxy's
# timeout (→ "upstream error"); caching means the backend finishes and a retry
# returns instantly. The scan takes no params, so one entry is enough.
import time as _time
from routes.bw_api_log import bw_get
# "state" holds the raw sweep results, not just the finished payload — the tag
# classification, the tasks, and WHICH properties failed at each stage. That is
# what makes "re-read just the missing N" possible: holding only the payload left
# nothing to merge into and nothing to narrow to, so the only remedy was
# re-sweeping the whole portfolio, which is what earns the next round of 429s.
_scan_cache = {"ts": 0.0, "data": None, "state": None}
_SCAN_TTL = 300
# How long the failed-pid lists stay usable for a narrow retry. After this a retry
# has nothing to narrow to and must fall back to a full sweep, which the scan
# route refuses to do silently.
_RETRY_WINDOW = 900         # 15 minutes

# A hot tub task counts as a SERVICE when the title names the tub and the kind of
# visit. Both spellings of the mid-stay visit are accepted — Breezeway titles carry
# it abbreviated ("Hot Tub Service - Mid Str") as well as written out — because a
# service that does not match here is invisible to the whole scan: it cannot satisfy
# an overdue check and cannot be flagged as a double-booking.
HOT_TUB_PATTERN = re.compile(
    r"(?=.*\bhot[\s\-]?tub\b)"
    r"(?=.*\b(arrival|biweekly|bi[\s\-]?weekly|lease|mid[\s\-]?st(ay|r)|d\s*&\s*s)\b)",
    re.IGNORECASE,
)
HOT_TUB_TAG_NAME = "hot tub - tg service"
# A house also qualifies for the scan if it has a current 30+ night guest lease
# AND carries this plain "Hot Tub" tag (matched exactly, NOT as a substring —
# otherwise "Hot Tub - TG Service" would also satisfy it).
HOT_TUB_PLAIN_TAG_NAME = "hot tub"

# Two services scheduled this many days apart (or fewer) are flagged as a
# possible accidental double-booking. Normal cadence is biweekly (~14 days).
TOO_CLOSE_DAYS = 6

# A pair is only flagged if at least one side is a biweekly or lease service —
# those run on a fixed cadence and should never land this close to another
# service. (Arrival / D&S services can legitimately cluster around a stay.)
BIWEEKLY_OR_LEASE_PATTERN = re.compile(
    r"\b(biweekly|bi[\s\-]?weekly|lease)\b",
    re.IGNORECASE,
)

# The double-booking check only inspects services within this many days on
# either side of today (a 2-week window centred on the scan date).
TOO_CLOSE_WINDOW_DAYS = 7


def _get_token():
    from routes.briefing import _get_breezeway_token
    return _get_breezeway_token()


def _get_property_name(pid):
    from routes.briefing import _get_live_property_cache, _ensure_property_cache
    _ensure_property_cache()
    cache = _get_live_property_cache()
    return (cache.get(str(pid)) or
            cache.get(int(pid) if str(pid).isdigit() else pid) or
            str(pid))


def _fetch_hot_tub_tag_id(token: str) -> tuple:
    """Find the Hot Tub - TG Service tag ID. Returns (tag_id, ok, status).

    ok=False means the tag LIST could not be read, which is a different problem
    from the tag genuinely not existing — the first is a throttle or an outage
    and is worth retrying, the second means the tag was renamed in Breezeway.
    Both used to return None and produce the same "check the tag name matches
    exactly" error, sending people to look for a naming problem that wasn't there.
    """
    from routes.bw_ratelimit import gate, LOCAL_THROTTLE_STATUS

    if not gate.acquire():
        return None, False, LOCAL_THROTTLE_STATUS
    try:
        r = bw_get(
            f"{BW_BASE}/public/inventory/v1/property/tags",
            headers={"Authorization": f"JWT {token}"},
            timeout=15,
        )
        gate.on_response(r.status_code)
        if r.status_code != 200:
            return None, False, r.status_code
        body = r.json()
        tags = body if isinstance(body, list) else body.get("results", body.get("data", []))
        for tag in tags:
            name = (tag.get("name") or tag.get("label") or "").lower().strip()
            if name == HOT_TUB_TAG_NAME:
                return tag.get("id"), True, 200
    except Exception:
        return None, False, None
    return None, True, 200          # read fine; the tag simply isn't there


def _fetch_property_tags(token: str, pid: str) -> tuple:
    """Tags for one property. Returns (tags, ok, status).

    ok=False means the lookup FAILED and the empty list must NOT be read as
    "this property has no hot tub tag". This is the most damaging silent failure
    in the tool: a throttled tag read dropped the house out of `tagged_pids`
    entirely, so it vanished from the scan — not flagged, not counted, absent.
    A house quietly falling off 14-day SLA tracking is exactly the outcome this
    page exists to prevent.
    """
    from routes.bw_ratelimit import gate, LOCAL_THROTTLE_STATUS

    last_status = None
    for path in [
        f"/public/inventory/v1/property/{pid}/tags",
        f"/public/inventory/v1/property/{pid}",
    ]:
        # Pace it. This module has never gone through the gate, so two 16-worker
        # sweeps over the whole portfolio ignored the shared budget entirely.
        if not gate.acquire():
            return [], False, LOCAL_THROTTLE_STATUS
        try:
            r = bw_get(
                f"{BW_BASE}{path}",
                headers={"Authorization": f"JWT {token}"},
                timeout=15,
            )
            gate.on_response(r.status_code)
            last_status = r.status_code
            # A 429 is about RATE, not about which path we asked with, so the
            # fallback path is guaranteed to be refused too. Trying it doubles
            # the cost of exactly the properties already being throttled.
            if r.status_code == 429 or r.status_code >= 500:
                return [], False, r.status_code
            if r.status_code == 200:
                body = r.json()
                # /tags endpoint returns a list directly
                if isinstance(body, list):
                    return body, True, 200
                # property detail endpoint — tags may be nested
                tags = body.get("tags") or body.get("property_tags") or []
                if tags:
                    return tags, True, 200
                # A 200 with no tags on the detail endpoint is ambiguous, so fall
                # through to the next path; if that also comes back empty the
                # house genuinely has no tags.
        except Exception:
            return [], False, None
    # Both paths answered without error and neither had tags: a real "no tags".
    if last_status == 200:
        return [], True, 200
    return [], False, last_status


def _fetch_current_lease_pids(token: str, today: date) -> tuple:
    """Property IDs with a guest lease (30+ nights) active today.

    A reservation is "current" if checkin <= today <= checkout. Lease vs
    owner/block classification reuses lease_prep._is_lease so the rules stay
    in one place.

    Returns (pids, complete, status). complete=False means pagination stopped
    early and the set is SHORT by an unknown amount — houses that qualify only
    via a current lease plus a plain "Hot Tub" tag would then be missing from
    the scan with nothing to say so. Every `break` below used to be
    indistinguishable from a clean end of pagination.
    """
    from routes.lease_prep import _is_lease
    from routes.bw_ratelimit import gate, LOCAL_THROTTLE_STATUS

    pids: set[str] = set()
    page = 1
    while True:
        if not gate.acquire():
            return pids, False, LOCAL_THROTTLE_STATUS
        try:
            r = bw_get(
                f"{BW_BASE}/public/inventory/v1/reservation",
                headers={"Authorization": f"JWT {token}"},
                params={"checkin_date_le": today.isoformat(),
                        "checkout_date_ge": today.isoformat(),
                        "limit": 100, "page": page},
                timeout=20,
            )
            gate.on_response(r.status_code)
            if r.status_code != 200:
                return pids, False, r.status_code
            body    = r.json()
            results = body.get("results", body.get("data", body if isinstance(body, list) else []))
            if not results:
                break                      # genuine end of pagination
            for res in results:
                if _is_lease(res):
                    pid = str(res.get("property_id") or res.get("home_id") or "")
                    if pid:
                        pids.add(pid)
            if len(results) < 100:
                break                      # last (partial) page — complete
            page += 1
        except Exception:
            return pids, False, None
    return pids, True, 200


def _fetch_tasks_for_property(token: str, pid: str, ref_id: str,
                              start: date, end: date) -> tuple:
    """One property's tasks over the service window. Returns (tasks, ok, status).

    ok=False means the lookup FAILED. This one fails in the opposite direction
    to the tag read: an empty list here left the house with no past services, so
    days_since came out None and the house was reported OVERDUE. A throttle
    therefore manufactured a false SLA breach rather than hiding a real one, and
    the two are indistinguishable on screen. The caller now marks these houses
    unreadable instead of overdue.
    """
    from routes.bw_ratelimit import gate, LOCAL_THROTTLE_STATUS

    date_range = f"{start.isoformat()},{end.isoformat()}"
    id_pairs = []
    if ref_id:
        id_pairs.append(("reference_property_id", ref_id))
    # home_id first. Breezeway aliases property_id onto reference_property_id,
    # so a raw Breezeway pid there can only ever 422 — it was costing every
    # house a guaranteed-failed request before the one that works. Kept last
    # rather than deleted: cheap insurance if home_id ever fails too.
    id_pairs += [("home_id", pid), ("property_id", pid)]

    saw_empty_200 = False
    last_status = None
    for key, val in id_pairs:
        if not gate.acquire():
            return [], False, LOCAL_THROTTLE_STATUS
        try:
            r = bw_get(
                f"{BW_BASE}/public/inventory/v1/task/",
                headers={"Authorization": f"JWT {token}"},
                params={"scheduled_date": date_range, key: val, "limit": 100},
                timeout=15,
            )
            gate.on_response(r.status_code)
            last_status = r.status_code
            if r.status_code == 429 or r.status_code >= 500:
                return [], False, r.status_code
            if r.status_code == 200:
                body = r.json()
                results = body.get("results", body.get("data", body if isinstance(body, list) else []))
                if results:
                    return results, True, 200
                saw_empty_200 = True
        except Exception:
            return [], False, None
    if saw_empty_200:
        return [], True, 200
    return [], False, last_status


def _sweep_tags(token: str, pids: list, tag_id) -> tuple:
    """Classify each property's tags. Returns
    (tg_pids, plain_pids, failed_pids, statuses).

    failed_pids is WHICH properties could not be classified. They are not
    "untagged" — they are UNKNOWN, and the difference matters: an unknown house
    may well have a hot tub whose service history nobody is watching. Keeping
    the list is also what lets a retry ask about only those.
    """
    tg_pids: set = set()
    plain_pids: set = set()
    failed_pids: list = []
    statuses: dict = {}

    def classify(pid):
        tags, ok, status = _fetch_property_tags(token, pid)
        if not ok:
            return pid, False, False, False, status
        has_tg = has_plain = False
        for t in tags:
            if isinstance(t, dict):
                tid  = t.get("id") or t.get("tag_id")
                name = (t.get("name") or t.get("label") or "").lower().strip()
            else:
                tid, name = None, str(t).lower().strip()
            if tid == tag_id or name == HOT_TUB_TAG_NAME:
                has_tg = True
            if name == HOT_TUB_PLAIN_TAG_NAME:
                has_plain = True
        return pid, has_tg, has_plain, True, status

    with ThreadPoolExecutor(max_workers=16) as ex:
        futures = {ex.submit(classify, pid): pid for pid in pids}
        for future in as_completed(futures):
            try:
                pid, has_tg, has_plain, ok, status = future.result()
            except Exception:
                failed_pids.append(futures[future])
                statuses["timeout"] = statuses.get("timeout", 0) + 1
                continue
            if not ok:
                failed_pids.append(pid)
                key = "timeout" if status is None else str(status)
                statuses[key] = statuses.get(key, 0) + 1
                continue
            if has_tg:
                tg_pids.add(pid)
            if has_plain:
                plain_pids.add(pid)
    return tg_pids, plain_pids, failed_pids, statuses


def _sweep_tasks(token: str, pids: list, ref_cache, start: date, end: date) -> tuple:
    """Fetch each property's tasks. Returns (tasks_by_pid, failed_pids, statuses).

    A property in failed_pids has NO usable service history, which is not the
    same as having no services — reporting it as overdue would invent an SLA
    breach out of a rate limit.
    """
    from routes.briefing import _ref_for
    tasks_by_pid: dict = {}
    failed_pids: list = []
    statuses: dict = {}

    def fetch(pid):
        tasks, ok, status = _fetch_tasks_for_property(
            token, pid, _ref_for(ref_cache, pid), start, end)
        return pid, tasks, ok, status

    with ThreadPoolExecutor(max_workers=16) as ex:
        futures = {ex.submit(fetch, pid): pid for pid in pids}
        for future in as_completed(futures):
            try:
                pid, tasks, ok, status = future.result()
            except Exception:
                failed_pids.append(futures[future])
                statuses["timeout"] = statuses.get("timeout", 0) + 1
                continue
            if not ok:
                failed_pids.append(pid)
                key = "timeout" if status is None else str(status)
                statuses[key] = statuses.get(key, 0) + 1
                continue
            tasks_by_pid[pid] = tasks
    return tasks_by_pid, failed_pids, statuses


def _merge_statuses(*dicts) -> dict:
    """Combine failure tallies from several sweeps into one {code: n} map, so the
    page can describe every cause in a single sentence."""
    out: dict = {}
    for d in dicts:
        for k, v in (d or {}).items():
            out[k] = out.get(k, 0) + v
    return out


@hot_tub_bp.route("/admin/hot-tub")
@login_required
@admin_required
def hot_tub_page():
    return render_template("hot_tub.html")


@hot_tub_bp.route("/admin/hot-tub/scan", methods=["POST"])
@login_required
@admin_required
def hot_tub_scan():
    token = _get_token()
    if not token:
        return jsonify({"error": "Breezeway not configured."}), 500

    body  = request.get_json(silent=True) or {}
    force = bool(body.get("force"))
    # "Re-read just the missing N" — re-ask about ONLY what failed last time and
    # merge it into what already loaded. A plain rescan re-classifies all ~442
    # properties, which is the expensive thing the user is trying to avoid and
    # the thing most likely to earn another round of 429s.
    retry_failed = bool(body.get("retry_failed"))

    # Serve a fresh cached result instantly (also rescues a prior proxy timeout).
    if not force and not retry_failed \
            and _scan_cache["data"] is not None and _time.time() - _scan_cache["ts"] < _SCAN_TTL:
        return jsonify(_scan_cache["data"])

    from routes.briefing import (_get_live_property_cache, _get_live_ref_cache,
                                 _ensure_property_cache)
    _ensure_property_cache()
    prop_cache = _get_live_property_cache()
    ref_cache  = _get_live_ref_cache()

    today     = date.today()
    lookback  = today - timedelta(days=45)
    lookahead = today + timedelta(days=45)

    state = _scan_cache.get("state")
    fresh = bool(state) and _time.time() - _scan_cache["ts"] < _RETRY_WINDOW
    if retry_failed and not (fresh and (state["tag_failed"] or state["task_failed"])):
        # Never let "just the missing ones" silently become a full re-sweep — that
        # is the expensive call this exists to avoid. Say so instead.
        return jsonify({"error": "The list of which properties failed has expired. "
                                 "Scan again to do a full re-check."})

    if retry_failed:
        # ── Narrow retry ────────────────────────────────────────────────
        tag_id      = state["tag_id"]
        all_pids    = state["all_pids"]
        tg_pids     = set(state["tg_pids"])
        plain_pids  = set(state["plain_pids"])
        lease_pids  = set(state["lease_pids"])
        tasks_by_pid = dict(state["tasks_by_pid"])
        lease_complete, lease_status = state["lease_complete"], state["lease_status"]

        # Re-classify only the properties whose tags we couldn't read.
        tag_failed, tag_statuses = list(state["tag_failed"]), {}
        if tag_failed:
            new_tg, new_plain, tag_failed, tag_statuses = _sweep_tags(token, tag_failed, tag_id)
            tg_pids   |= new_tg
            plain_pids |= new_plain

        tagged_pids = sorted(tg_pids | (lease_pids & plain_pids))
        # Anything newly discovered to be a hot tub house still needs its tasks,
        # alongside the houses whose task read failed last time.
        need_tasks = [p for p in tagged_pids if p not in tasks_by_pid]
        retry_task_pids = list(dict.fromkeys(list(state["task_failed"]) + need_tasks))

        task_failed, task_statuses = [], {}
        if retry_task_pids:
            new_tasks, task_failed, task_statuses = _sweep_tasks(
                token, retry_task_pids, ref_cache, lookback, lookahead)
            tasks_by_pid.update(new_tasks)
    else:
        # ── Full scan ───────────────────────────────────────────────────
        # Step 1: find the Hot Tub - TG Service tag ID.
        tag_id, tag_ok, tag_status = _fetch_hot_tub_tag_id(token)
        if not tag_ok:
            # Could not READ the tag list — a throttle or an outage, not a naming
            # problem. Saying "check the tag name" here sent people looking for a
            # fault that wasn't there.
            return jsonify({"error": "Couldn't read the property tag list from Breezeway "
                                     f"({'no response' if tag_status is None else f'HTTP {tag_status}'}). "
                                     "This is usually rate limiting — try again in a minute."}), 503
        if tag_id is None:
            return jsonify({"error": "Could not find 'Hot Tub - TG Service' tag in Breezeway. "
                                     "Check the tag name matches exactly."}), 500

        # Step 2: classify every property's tags — does it carry the
        # "Hot Tub - TG Service" tag, and/or a plain "Hot Tub" tag?
        all_pids = list(prop_cache.keys())
        tg_pids, plain_pids, tag_failed, tag_statuses = _sweep_tags(token, all_pids, tag_id)

        # A house qualifies via EITHER the TG Service tag, OR a current 30+ night
        # guest lease combined with a plain "Hot Tub" tag.
        lease_pids, lease_complete, lease_status = _fetch_current_lease_pids(token, today)
        tagged_pids = sorted(tg_pids | (lease_pids & plain_pids))

        # Step 3: fetch tasks — 45 days back AND forward, for last + next service.
        tasks_by_pid, task_failed, task_statuses = (
            _sweep_tasks(token, tagged_pids, ref_cache, lookback, lookahead)
            if tagged_pids else ({}, [], {}))

    # An empty result is only trustworthy if the sweep that produced it was clean.
    # Reporting "no hot tub properties" after failing to classify half the
    # portfolio is the exact silent under-report this work exists to remove.
    if not tagged_pids and not tag_failed:
        return jsonify({"results": [], "too_close": [], "tag_id": tag_id,
                        "warning": "No properties found with 'Hot Tub - TG Service' "
                                   "tag, nor a current lease + 'Hot Tub' tag."})

    def _assignee_name(t: dict) -> str:
        for a in (t.get("assignments") or []):
            if isinstance(a, dict):
                n = (a.get("name") or a.get("full_name") or
                     f"{a.get('first_name','').strip()} {a.get('last_name','').strip()}".strip())
                if n:
                    return n
        return ""

    # Step 4: find last service (past) and next upcoming service (future) per property
    unreadable = set(task_failed)
    results   = []
    too_close = []   # possible accidental double-bookings (services <= 6 days apart)
    for pid in tagged_pids:
        prop_name = _get_property_name(pid)
        tasks     = tasks_by_pid.get(pid, [])

        past_services   = []
        future_services = []

        for t in tasks:
            title = (t.get("title") or t.get("name") or "")
            if isinstance(title, dict):
                title = title.get("value") or title.get("name") or ""
            if not HOT_TUB_PATTERN.search(title):
                continue
            sched = t.get("scheduled_date") or ""
            try:
                d = date.fromisoformat(sched[:10])
            except (ValueError, TypeError):
                continue
            entry = {
                "date":     d,
                "title":    title,
                "time":     t.get("scheduled_time") or "",
                "assignee": _assignee_name(t),
            }
            if d <= today:
                past_services.append(entry)
            else:
                future_services.append(entry)

        if past_services:
            past_services.sort(key=lambda x: x["date"], reverse=True)
            last = past_services[0]
            last_date       = last["date"]
            last_task_title = last["title"]
            days_since      = (today - last_date).days
        else:
            last_date       = None
            last_task_title = None
            days_since      = None

        # Too-close check: within a 2-week window centred on today, flag any two
        # consecutive services <= TOO_CLOSE_DAYS apart where at least one is a
        # biweekly or lease service (a likely accidental double-booking). Sort by
        # date+time so consecutive pairs are truly adjacent.
        tc_start = today - timedelta(days=TOO_CLOSE_WINDOW_DAYS)
        tc_end   = today + timedelta(days=TOO_CLOSE_WINDOW_DAYS)
        window_services = sorted(
            (s for s in (past_services + future_services) if tc_start <= s["date"] <= tc_end),
            key=lambda x: (x["date"], x["time"] or ""),
        )
        for prev_svc, next_svc in zip(window_services, window_services[1:]):
            gap = (next_svc["date"] - prev_svc["date"]).days
            if gap > TOO_CLOSE_DAYS:
                continue
            if not (BIWEEKLY_OR_LEASE_PATTERN.search(prev_svc["title"])
                    or BIWEEKLY_OR_LEASE_PATTERN.search(next_svc["title"])):
                continue
            too_close.append({
                "property": prop_name,
                "property_id": pid,
                "gap_days": gap,
                "first": {
                    "title":    prev_svc["title"],
                    "date":     prev_svc["date"].isoformat(),
                    "time":     prev_svc["time"],
                    "assignee": prev_svc["assignee"],
                },
                "second": {
                    "title":    next_svc["title"],
                    "date":     next_svc["date"].isoformat(),
                    "time":     next_svc["time"],
                    "assignee": next_svc["assignee"],
                },
            })

        # A house whose tasks could not be read has NO service history, which is
        # not the same as having no services. Calling that overdue manufactures an
        # SLA breach out of a rate limit — the opposite error to the silent drop,
        # and just as wrong. Mark it unreadable and let the page say so.
        cant_read = pid in unreadable
        overdue   = False if cant_read else (days_since is None or days_since > 18)

        results.append({
            "property":        prop_name,
            "property_id":     pid,
            "last_task":       last_task_title,
            "last_date":       last_date.isoformat() if last_date else None,
            "days_since":      days_since,
            "overdue":         overdue,
            "unreadable":      cant_read,
        })

    # Unreadable first (they need a retry, not a service), then overdue, then by
    # days_since descending.
    results.sort(key=lambda x: (not x["unreadable"], not x["overdue"],
                                -(x["days_since"] or 9999)))
    # Tightest gaps first, then by the earlier service date.
    too_close.sort(key=lambda x: (x["gap_days"], x["first"]["date"]))

    # Report what could NOT be read alongside what could. The two failures are
    # kept apart because they mean different things and the page words them
    # differently: a property whose TAGS failed might be a hot tub house nobody
    # is now tracking, while a property whose TASKS failed is known to be one but
    # has no readable service history.
    failed_pids = list(dict.fromkeys(list(tag_failed) + list(task_failed)))
    payload = {
        "results":            results,
        "too_close":          too_close,
        "tag_id":             tag_id,
        "failed_properties":  len(failed_pids),
        "failure_statuses":   _merge_statuses(tag_statuses, task_statuses),
        "scanned_properties": len(all_pids),
        # Split out so the page can name the consequence of each.
        "tag_failed_properties":  len(tag_failed),
        "task_failed_properties": len(task_failed),
    }
    # A short lease list has no per-property remedy — the houses it lost never
    # reached the qualifying set, so a narrow retry cannot ask about them.
    if not lease_complete:
        payload["leases_incomplete"] = True
        payload["leases_status"]     = lease_status

    _scan_cache["data"] = payload          # cache before returning (survives proxy timeout)
    _scan_cache["ts"]   = _time.time()
    # Hold the raw sweep state, not just the payload — this is what a narrow
    # retry merges into and narrows to.
    _scan_cache["state"] = {
        "tag_id":       tag_id,
        "all_pids":     all_pids,
        "tg_pids":      tg_pids,
        "plain_pids":   plain_pids,
        "lease_pids":   lease_pids,
        "tasks_by_pid": tasks_by_pid,
        "tag_failed":   tag_failed,
        "task_failed":  task_failed,
        "lease_complete": lease_complete,
        "lease_status":   lease_status,
    }
    return jsonify(payload)
