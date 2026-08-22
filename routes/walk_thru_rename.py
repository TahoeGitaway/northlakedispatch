"""
routes/walk_thru_rename.py — Walk Thru task rename tool.

Scans Breezeway for Walk Thru tasks (today → +30 days) that don't already
have a date in the name, finds the next reservation arrival on or after each
task's scheduled date for that property, and proposes a rename like
"Walk Thru for 6/15". Admin reviews and approves before anything is changed.

Endpoints:
  GET  /admin/walk-thru-rename        — page
  POST /admin/walk-thru-rename/scan   — scan and return proposals (JSON)
  POST /admin/walk-thru-rename/apply  — PATCH approved renames (JSON)
"""

import re
import requests
from datetime import date, timedelta

from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required

from routes.auth import admin_required

walk_thru_bp = Blueprint("walk_thru", __name__)

BW_BASE = "https://api.breezeway.io"

# Per date-range scan cache. The per-property sweep can run past the hosting
# proxy's timeout (→ "upstream error"); caching means the backend finishes and a
# retry returns instantly. Cleared after any rename.
import time as _time
from routes.bw_api_log import bw_get, bw_patch
_scan_cache: dict = {}      # (start_iso, end_iso) -> (timestamp, result_dict)
_SCAN_TTL = 90
# How long the held partial stays retryable. Same idea as the import's
# _BW_RETRY_WINDOW: the cache keeps the tasks that DID load plus the pids that
# failed, so "load just the missing N" can ask about only those and merge the
# answers in, instead of re-sweeping every property from scratch.
_RETRY_WINDOW = 900

WALK_THRU_PATTERNS = re.compile(
    r"\b(walk[\s\-]?thru|walk[\s\-]?through|lease[\s\-]?walk|move[\s\-]?in[\s\-]?inspection|"
    r"arrival[\s\-]?task|guest[\s\-]?arrival|managed[\s\-]?services?[\s\-]?arrival)\b",
    re.IGNORECASE,
)
# A date ANYWHERE in the title means this task is already dated. Not anchored to
# the end: real titles carry the date mid-string with a note after it —
# "Walk Thru for 8/22 *Owner Cleaned", "Walk Thru for 8/28*10:30 CO",
# "Lease Walk Thru for 9/1 *see notes". Anchoring to `$` missed every one of those
# and proposed appending a SECOND date, which is how you get
# "Walk Thru for 8/22 *Owner Cleaned for 8/22" written into Breezeway.
#
# Bounded and range-checked rather than anchored, which is what separates a date
# from the digit runs the old unanchored version tripped on: month 1-12 and day
# 1-31 reject "24/7", and \b rejects "100/200" (its 3-digit runs cannot match
# \d{1,2} between boundaries).
#
# A bare fraction — "1/2 bath", "3/4 beds" — is still read as a date, and that is
# the deliberate choice. The two ways to be wrong are not equal: treating a dated
# task as undated appends a duplicate date to a live Breezeway task, while
# treating an undated one as dated just means renaming it by hand. Err toward the
# one that writes nothing.
ALREADY_DATED = re.compile(r"\b(?:1[0-2]|0?[1-9])/(?:3[01]|[12][0-9]|0?[1-9])\b")
BB_PREFIX     = re.compile(r"^b/b\s+", re.IGNORECASE)

# How far past the end of the span to look for the arrival a task is preparing for.
# A walk thru runs 1-3 days AHEAD of its arrival, so fetching reservations only to
# end + 1 day meant every scan lost its own tail: a Walk Thru on the last day, for
# an arrival two days later, found no arrival and was dropped in silence. Seven
# covers the real range with margin for an outlier.
#
# Deliberately not pri_rename's LOOKAHEAD_DAYS = 180. That tool chases owner/hold
# arrivals, which can be months out; a guest arrival a walk thru prepares for is
# days away, and a wide window here would just buy pages of reservations nobody
# needs.
ARRIVAL_LOOKAHEAD_DAYS = 7


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


def _build_proposed_title(title: str, arrival: date) -> str:
    return f"{title} for {arrival.month}/{arrival.day}"


def _fetch_tasks_for_property(token: str, pid: str, ref_id: str,
                              start: date, end: date) -> tuple:
    """One property's tasks. Returns (tasks, ok, status).

    ok=False means the lookup FAILED and the empty list must not be read as "this
    property has no Walk Thru task". Previously every failure — a 429, a timeout,
    a rejected token — was swallowed by `except Exception: pass` and returned as
    [], identical to a property that genuinely had nothing. Under throttling this
    tool therefore reported fewer tasks than exist and looked complete: a real
    Walk Thru task went missing from a scan with nothing on screen to say a third
    of the portfolio had never loaded.

    `status` is the last failing HTTP status, or None for a timeout/transport
    error, so the UI can name the cause.

    The request count per property is UNCHANGED (at most one call per id key, no
    retries). Breezeway is already refusing about a third of what this app sends;
    adding retries here would multiply the load on the exact resource that is
    scarce. Recovering a throttled property is the scan-again path's job.
    """
    date_range = f"{start.isoformat()},{end.isoformat()}"
    id_pairs = []
    if ref_id:
        id_pairs.append(("reference_property_id", ref_id))
    # home_id first. Breezeway aliases property_id onto reference_property_id,
    # so a raw Breezeway pid there can only ever 422 — it was costing every
    # house a guaranteed-failed request before the one that works. Kept last
    # rather than deleted: cheap insurance if home_id ever fails too.
    id_pairs += [("home_id", pid), ("property_id", pid)]

    from routes.bw_ratelimit import gate, LOCAL_THROTTLE_STATUS

    saw_empty_200 = False
    last_status = None
    for key, val in id_pairs:
        # Pace it. This module has never gone through the gate, so a 16-worker sweep
        # here ignored the shared budget entirely and competed with every other
        # Breezeway caller in the process.
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
            # Trying the next id space after a 429 is pointless and actively harmful:
            # the refusal is about RATE, not about which id we asked with, so the two
            # remaining attempts are guaranteed to be refused too. That tripled the
            # cost of exactly the properties already being throttled — 33 failures
            # became ~99 requests — which is what made "load just the missing N"
            # re-fail on contact.
            if r.status_code == 429 or r.status_code >= 500:
                return [], False, r.status_code
            if r.status_code == 200:
                body = r.json()
                results = body.get("results", body.get("data", body if isinstance(body, list) else []))
                if results:
                    return results, True, 200
                # A 200 with nothing may just mean we asked in the wrong id space
                # (the task API takes the external reference, other calls take
                # Breezeway's own), so keep trying the remaining keys — but
                # remember that something did answer.
                saw_empty_200 = True
        except Exception:
            # Also stop here. Each attempt carries a 15s timeout, so walking the
            # remaining id spaces after one has already hung costs up to 45s on a
            # single property — and a hanging API is not more likely to answer the
            # same question asked a different way.
            return [], False, None      # timeout / transport, not an HTTP status

    # Every id space answered and none had tasks: a real "nothing here".
    if saw_empty_200:
        return [], True, 200
    return [], False, last_status


def _fetch_tasks_for_pids(token: str, pids: list[str], start: date,
                          end: date) -> tuple:
    """Sweep many properties. Returns (tasks, failed_pids, failure_statuses).

    failed_pids is WHICH properties could not be read, not just how many — that is
    what lets a retry ask about only those instead of re-sweeping everything.
    failure_statuses is the {"429": n, "timeout": n} tally static/bw-failure.js
    turns into a sentence, the same shape the map import already returns.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from routes.briefing import _get_live_ref_cache, _ref_for
    ref_cache = _get_live_ref_cache()

    all_tasks = []
    seen_ids: set = set()
    failed_pids: list = []
    statuses: dict = {}

    with ThreadPoolExecutor(max_workers=16) as ex:
        # _ref_for, not ref_cache.get: these pids are strings and the cache is
        # int-keyed, so a plain .get missed every house and sent the whole sweep
        # down the two-request fallback.
        futures = {ex.submit(_fetch_tasks_for_property, token, pid, _ref_for(ref_cache, pid), start, end): pid
                   for pid in pids}
        for future in as_completed(futures):
            pid = futures[future]
            try:
                tasks, ok, status = future.result()
            except Exception:
                # The helper catches its own errors, so this is a worker that died
                # some other way. Record it rather than let it vanish.
                failed_pids.append(pid)
                statuses["timeout"] = statuses.get("timeout", 0) + 1
                continue
            if not ok:
                failed_pids.append(pid)
                key = "timeout" if status is None else str(status)
                statuses[key] = statuses.get(key, 0) + 1
                continue
            for t in tasks:
                tid = t.get("id")
                if tid is None or tid not in seen_ids:
                    if tid is not None:
                        seen_ids.add(tid)
                    # Stamp the pid we ASKED about. The caller matches tasks to a
                    # property's arrivals, and it used to re-derive that from the
                    # task payload — which only works if the payload's property_id
                    # is in the same id space as the reservation's. It is not
                    # always: a task fetched by reference_property_id can come back
                    # carrying the external reference there, and the match then
                    # finds no arrivals and silently drops every task. The pid is
                    # not in doubt here, so don't rediscover it.
                    t["_swept_pid"] = str(pid)
                    all_tasks.append(t)
    return all_tasks, failed_pids, statuses


def _fetch_reservations_range(token: str, start: date, end: date) -> tuple:
    """Every arrival in the window. Returns (reservations, complete, status).

    complete=False means pagination stopped early — a throttle, a bad status or a
    timeout — and the list is SHORT by an unknown amount.

    That distinction is the whole point. This list is what produces arrival_pids,
    and the task sweep only asks about properties in it. So a truncated page here
    does not show up as a failed property later; those houses are simply never
    asked about, and the scan reports `failed_properties: 0` over a list that
    quietly lost them. Every exit below used to be a bare `break` returning
    whatever had accumulated, indistinguishable from "that was the last page"."""
    from routes.bw_ratelimit import gate, LOCAL_THROTTLE_STATUS

    all_results = []
    page = 1
    while True:
        try:
            if not gate.acquire():
                # This app declined to send, not Breezeway refusing — say which.
                return all_results, False, LOCAL_THROTTLE_STATUS
            r = bw_get(
                f"{BW_BASE}/public/inventory/v1/reservation",
                headers={"Authorization": f"JWT {token}"},
                params={
                    "checkin_date_ge": start.isoformat(),
                    "checkin_date_le": end.isoformat(),
                    "limit": 100,
                    "page": page,
                },
                timeout=20,
            )
            gate.on_response(r.status_code)
            if r.status_code != 200:
                return all_results, False, r.status_code
            body = r.json()
            results = body.get("results", body.get("data", []))
            if not results:
                break                      # genuine end of the list
            all_results.extend(results)
            if len(results) < 100:
                break                      # short page = genuine last page
            page += 1
        except Exception:
            return all_results, False, None    # timeout / transport
    return all_results, True, 200


def _patch_task_name(token: str, task_id, new_name: str,
                     meta: dict = None) -> tuple[bool, str]:
    """meta carries the pre-write name/property so the audit log records what the
    rename replaced."""
    from routes.bw_audit import log_bw_write
    meta = meta or {}
    headers = {"Authorization": f"JWT {token}", "Content-Type": "application/json"}
    url = f"{BW_BASE}/public/inventory/v1/task/{task_id}"
    try:
        r = bw_patch(url, headers=headers, json={"name": new_name}, timeout=15)
        ok = r.status_code in (200, 201)
        try:
            returned_name = r.json().get("name") or "(not in response)"
            msg = f"status={r.status_code} name='{returned_name}'"
        except Exception:
            msg = f"status={r.status_code} body={r.text[:200]}"
        log_bw_write("walk_thru_rename", "name", task_id=task_id,
                     task_name=meta.get("old_name"), property_name=meta.get("property"),
                     task_date=meta.get("date"), old_value=meta.get("old_name"),
                     new_value=new_name, ok=ok, detail=msg)
        return ok, msg
    except Exception as e:
        log_bw_write("walk_thru_rename", "name", task_id=task_id,
                     task_name=meta.get("old_name"), property_name=meta.get("property"),
                     task_date=meta.get("date"), old_value=meta.get("old_name"),
                     new_value=new_name, ok=False, detail=f"{type(e).__name__}: {e}")
        return False, str(e)


@walk_thru_bp.route("/admin/walk-thru-rename")
@login_required
@admin_required
def walk_thru_page():
    return render_template("walk_thru_rename.html")


@walk_thru_bp.route("/admin/walk-thru-rename/scan", methods=["POST"])
@login_required
@admin_required
def walk_thru_scan():
    token = _get_token()
    if not token:
        return jsonify({"error": "Breezeway not configured."}), 500

    today = date.today()
    body  = request.get_json(silent=True) or {}
    try:
        start = date.fromisoformat(body["start"]) if "start" in body else today
        end   = date.fromisoformat(body["end"])   if "end"   in body else today + timedelta(days=7)
    except ValueError:
        start, end = today, today + timedelta(days=7)

    ck     = (start.isoformat(), end.isoformat())
    force  = bool(body.get("force"))
    # "Load just the missing N" — ask about ONLY the properties that failed last
    # time and merge them into what already loaded. A plain rescan re-reads every
    # property, which is the expensive thing the user is trying to avoid.
    retry_failed = bool(body.get("retry_failed"))
    cached = _scan_cache.get(ck)

    if cached and not force and not retry_failed and _time.time() - cached[0] < _SCAN_TTL:
        return jsonify(cached[1])

    held_tasks, retry_pids, reso_by_prop = [], None, None
    if retry_failed and cached and _time.time() - cached[0] < _RETRY_WINDOW:
        # cached = (ts, result, tasks, failed_pids, reso_by_prop, arrival_pids)
        held_tasks  = list(cached[2])
        retry_pids  = list(cached[3])
        reso_by_prop = cached[4]
        arrival_pids = cached[5]
    if retry_failed and not retry_pids:
        # Never let "just the missing ones" silently become a full re-sweep — that
        # is the expensive call this exists to avoid. Say so instead.
        return jsonify({"error": "The list of which properties failed has expired. "
                                 "Scan again to do a full re-check."})

    # True unless THIS run fetched the reservations and came up short. A cached
    # reso_by_prop is complete by construction — an incomplete one is never cached.
    reso_complete, reso_status = True, 200
    if reso_by_prop is None:
        reservations, reso_complete, reso_status = _fetch_reservations_range(
            token, start, end + timedelta(days=ARRIVAL_LOOKAHEAD_DAYS))

        reso_by_prop = {}
        for r in reservations:
            pid     = str(r.get("property_id") or r.get("home_id") or "")
            checkin = r.get("checkin_date") or ""
            if pid and checkin:
                try:
                    d = date.fromisoformat(checkin[:10])
                    reso_by_prop.setdefault(pid, []).append(d)
                except ValueError:
                    pass
        for pid in reso_by_prop:
            reso_by_prop[pid].sort()
        arrival_pids = list(reso_by_prop.keys())

    # A retry sweeps only the failed pids; a normal scan sweeps them all.
    sweep_pids = retry_pids if retry_pids else arrival_pids
    new_tasks, failed_pids, failure_statuses = (
        _fetch_tasks_for_pids(token, sweep_pids, start, end) if sweep_pids
        else ([], [], {}))

    # Merge, de-duplicating by task id so a retry cannot double-add.
    tasks, _seen = list(held_tasks), {t.get("id") for t in held_tasks if t.get("id") is not None}
    for t in new_tasks:
        tid = t.get("id")
        if tid is None or tid not in _seen:
            if tid is not None:
                _seen.add(tid)
            tasks.append(t)
    failed_props = len(failed_pids)

    # Where the tasks went. "No Walk Thrus to rename" is a real answer and also
    # what a silent bug looks like, and the two were indistinguishable — so count
    # each filter and hand the tally back with the result.
    funnel = {"fetched": len(tasks), "not_walk_thru": 0, "already_dated": 0,
              "back_to_back": 0, "bad_date": 0, "no_arrival_match": 0}

    # What the id fields on a real task actually hold, taken from tasks the scan
    # already fetched — no probe, no extra call. Three commits have now ASSERTED
    # that a task fetched by reference_property_id comes back carrying the external
    # reference in property_id, and not one of them observed it. One scan settles
    # it: if `swept` matches `home_id`/`property_id` here, the assertion is wrong
    # and the comments repeating it should be corrected.
    funnel["id_sample"] = [
        {"swept":     t.get("_swept_pid"),
         "property_id": t.get("property_id"),
         "home_id":     t.get("home_id"),
         "reference_property_id": t.get("reference_property_id")}
        for t in tasks[:3]
    ]

    proposals = []
    for t in tasks:
        title = (t.get("title") or t.get("name") or "")
        if isinstance(title, dict):
            title = title.get("value") or title.get("name") or ""

        if not WALK_THRU_PATTERNS.search(title):
            funnel["not_walk_thru"] += 1
            continue
        # b/b BEFORE already-dated. Back-to-back titles usually carry a date, so
        # testing already_dated first filed every one of them in the wrong bucket:
        # back_to_back read zero and already_dated was inflated by exactly the
        # tasks it was not describing — a miscount in the very tally added to make
        # silent drops visible.
        if BB_PREFIX.match(title):
            funnel["back_to_back"] += 1
            continue
        if ALREADY_DATED.search(title):
            funnel["already_dated"] += 1
            continue

        task_id = t.get("id") or t.get("task_id")
        # The pid this task was FETCHED for, which is the same id space
        # reso_by_prop is keyed in. The payload fields are the fallback, and
        # home_id leads them: dispatch.py reads them in that order for the same
        # reason, and taking property_id first is what broke the arrival match.
        pid = (t.get("_swept_pid")
               or str(t.get("home_id") or t.get("property_id") or ""))
        sched   = t.get("scheduled_date") or ""
        try:
            task_date = date.fromisoformat(sched[:10])
        except (ValueError, TypeError):
            funnel["bad_date"] += 1
            continue

        arrivals = reso_by_prop.get(pid, [])
        arrival  = next((d for d in arrivals if d >= task_date), None)
        if not arrival:
            funnel["no_arrival_match"] += 1
            continue

        proposals.append({
            "task_id":        task_id,
            "property":       _get_property_name(pid),
            "property_id":    pid,   # for the Breezeway calendar deep-link in the UI
            "current_title":  title,
            "task_date":      sched[:10],
            "arrival_date":   arrival.isoformat(),
            "proposed_title": _build_proposed_title(title, arrival),
        })

    proposals.sort(key=lambda x: x["task_date"])
    # Report what could NOT be read alongside what could. Without these the page
    # can only ever say "here are your proposals" — never "and 57 properties never
    # loaded", which is how a real task went missing without a trace.
    result = {"proposals": proposals,
              "failed_properties":  failed_props,
              "failure_statuses":   failure_statuses,
              "scanned_properties": len(arrival_pids),
              # Why the proposal list is the length it is. An empty result with
              # 300 tasks fetched and 300 dropped at one filter is a bug; an empty
              # result with 300 fetched and 300 already dated is the tool working.
              # Reporting only the proposals made those identical on screen.
              "funnel": funnel}
    # A short reservation list is a DIFFERENT failure from a property whose tasks
    # wouldn't load, and it has no per-property remedy: the houses it lost are
    # unknown, so "load just the missing N" cannot ask about them. Report it
    # separately so the page can say the list itself is incomplete.
    if not reso_complete:
        result["reservations_incomplete"] = True
        result["reservations_status"]     = reso_status
    # Hold the TASKS and the FAILED PIDS, not just the finished payload — that is
    # what makes "load just the missing N" possible. Holding only the payload (as
    # this used to) left nothing to merge into and nothing to narrow to, so the
    # only option was re-reading every property.
    #
    # Never cache a run built on a short reservation list. Caching it would pin the
    # truncated arrival_pids for the next 90 seconds, so scanning again — the one
    # move that could recover the missing houses — would replay the same gap
    # instantly and look like confirmation.
    if reso_complete:
        _scan_cache[ck] = (_time.time(), result, tasks, failed_pids,
                           reso_by_prop, arrival_pids)
    return jsonify(result)


@walk_thru_bp.route("/admin/walk-thru-rename/apply", methods=["POST"])
@login_required
@admin_required
def walk_thru_apply():
    token = _get_token()
    if not token:
        return jsonify({"error": "Breezeway not configured."}), 500

    items = request.json.get("items", [])
    results = []
    for item in items:
        ok, msg = _patch_task_name(token, item["task_id"], item["proposed_title"],
                                   meta={"old_name": item.get("current_title") or item.get("task_title", ""),
                                         "property": item.get("property", ""),
                                         "date": item.get("date") or item.get("scheduled_date", "")})
        results.append({
            "task_id":        item["task_id"],
            "property":       item.get("property", ""),
            "proposed_title": item["proposed_title"],
            "success":        ok,
            "detail":         msg,
        })

    _scan_cache.clear()   # names changed — next scan should be fresh
    return jsonify({"results": results})
