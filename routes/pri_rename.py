"""
routes/pri_rename.py — Post Rental Inspection (PRI) rename tool.

DELIBERATELY separate from the Walk Thru rename and the PRI Check — its own
blueprint and template so these features can never break one another.

Scans Breezeway for existing "Post Rental Inspection" tasks in a date range and,
for each task's property, finds the NEXT homeowner / hold / block arrival on or
after the task's scheduled date. Proposes renaming the task to
"Post Rental Inspection for M/D" (re-dating even ones that already carry a date).
Admin reviews and approves before anything changes.

Endpoints:
  GET  /admin/pri-rename        — page (a tab in the PRI workflow area)
  POST /admin/pri-rename/scan   — scan and return proposals (JSON)
  POST /admin/pri-rename/apply  — PATCH approved renames (JSON)
"""

import re
import requests
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required

from routes.auth import admin_required

pri_rename_bp = Blueprint("pri_rename", __name__)

BW_BASE = "https://api.breezeway.io"

# Per date-range scan cache — survives a proxy timeout so a retry is instant.
import time as _time
from routes.bw_api_log import bw_get, bw_patch
# (start_iso, end_iso) -> (ts, result, tasks, failed_pids, owner_arrivals, pids)
# The TASKS and the FAILED PIDS are held, not just the finished payload — that is
# what makes "re-read just the missing N" possible. Holding only the payload left
# nothing to merge into and nothing to narrow to, so the only available remedy
# was re-reading every property.
_scan_cache: dict = {}
_SCAN_TTL = 90
# How long the failed-pid list stays usable for a narrow retry. After this a
# retry has nothing to narrow to and must fall back to a full sweep, which the
# scan route refuses to do silently.
_RETRY_WINDOW = 900         # 15 minutes

# How far ahead to look for the next owner/hold/block arrival — it can be well
# beyond the inspection's own date.
LOOKAHEAD_DAYS = 180

PRI_PATTERN   = re.compile(r"post[\s\-]?rental[\s\-]?inspection", re.IGNORECASE)
# Trailing date suffix to strip before re-dating: " for 6/22", " *6/22", " 6/22".
TRAILING_DATE = re.compile(r"\s*(?:for\s+|\*\s*)?\d{1,2}/\d{1,2}\s*$", re.IGNORECASE)
# Back-to-back marker: a "b/b " prefix on the title. These are usually left alone, so
# the UI tucks them into a separate collapsed section — see is_bb on each proposal.
BB_PREFIX     = re.compile(r"^\s*b\s*/\s*b\b", re.IGNORECASE)


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


def _strip_trailing_date(title: str) -> str:
    """Remove an existing 'for M/D' / '*M/D' / 'M/D' from the end of a title."""
    prev, out = None, title.strip()
    while out != prev:
        prev = out
        out = TRAILING_DATE.sub("", out).strip()
    return out


def _build_proposed_title(title: str, arrival: date) -> str:
    return f"{_strip_trailing_date(title)} for {arrival.month}/{arrival.day}"


def _fetch_tasks_for_property(token, pid, ref_id, start, end) -> tuple:
    """One property's tasks. Returns (tasks, ok, status).

    ok=False means the lookup FAILED, and the empty list must not be read as
    "this property has no PRI task". Every failure here — a 429, a timeout, a
    rejected token — used to be swallowed by `except Exception: pass` and
    returned as [], identical to a house that genuinely had nothing scheduled.
    Under throttling this tool therefore proposed fewer renames than it should
    and looked complete: a real inspection went un-renamed with nothing on
    screen to say a third of the properties had never loaded.

    `status` is the last failing HTTP status, or None for a timeout/transport
    error, so the page can name the cause instead of guessing.
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
        # Pace it. This module has never gone through the gate, so its 16-worker
        # sweep ignored the shared budget entirely and competed with every other
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
            # Trying the next id space after a 429 is pointless and actively
            # harmful: the refusal is about RATE, not about which id we asked
            # with, so the remaining attempts are guaranteed to be refused too.
            # That tripled the cost of exactly the properties already being
            # throttled, which is what makes a retry re-fail on contact.
            if r.status_code == 429 or r.status_code >= 500:
                return [], False, r.status_code
            if r.status_code == 200:
                body = r.json()
                results = body.get("results", body.get("data", body if isinstance(body, list) else []))
                if results:
                    return results, True, 200
                # A 200 with nothing may just mean we asked in the wrong id
                # space, so try the remaining keys — but remember something
                # did answer, so an all-empty sweep is a real "nothing here".
                saw_empty_200 = True
        except Exception:
            # Also stop here. Each attempt carries a 15s timeout, so walking the
            # remaining id spaces after one has already hung costs up to 45s on a
            # single property — and a hanging API is not more likely to answer
            # the same question asked a different way.
            return [], False, None      # timeout / transport, not an HTTP status

    if saw_empty_200:
        return [], True, 200
    return [], False, last_status


def _fetch_tasks_for_pids(token, pids, start, end) -> tuple:
    """Sweep many properties. Returns (tasks, failed_pids, failure_statuses).

    failed_pids is WHICH properties could not be read, not just how many — that
    is what lets a retry ask about only those instead of re-sweeping everything.
    A full re-sweep of the owner-arrival set is precisely what provokes the 429s
    it is trying to recover from, so without this the retry recreates the
    failure and never converges.

    failure_statuses is the {"429": n, "timeout": n} tally static/bw-failure.js
    turns into a sentence — the same shape every other scan in the app returns.
    """
    from routes.briefing import _get_live_ref_cache, _ref_for
    ref_cache = _get_live_ref_cache()
    all_tasks, seen = [], set()
    failed_pids: list = []
    statuses: dict = {}
    with ThreadPoolExecutor(max_workers=16) as ex:
        # _ref_for, not ref_cache.get — str pids against an int-keyed cache.
        futures = {ex.submit(_fetch_tasks_for_property, token, pid, _ref_for(ref_cache, pid), start, end): pid
                   for pid in pids}
        for fut in as_completed(futures):
            pid = futures[fut]
            try:
                tasks, ok, status = fut.result()
            except Exception:
                # The helper catches its own errors, so this is a worker that
                # died some other way. Record it rather than let it vanish.
                failed_pids.append(pid)
                statuses["timeout"] = statuses.get("timeout", 0) + 1
                continue
            if not ok:
                failed_pids.append(pid)
                key = "timeout" if status is None else str(status)
                statuses[key] = statuses.get(key, 0) + 1
                continue
            for t in (tasks or []):
                tid = t.get("id")
                if tid is None or tid not in seen:
                    if tid is not None:
                        seen.add(tid)
                    # Stamp the pid we ASKED for. The caller matches each task to
                    # that property's owner arrivals, in a map keyed by the
                    # reservation's property_id — and it used to re-derive the pid
                    # from the task payload instead. That only holds while every
                    # task is fetched by home_id, which stopped being true when the
                    # reference-id lookup started working. The pid is not in doubt
                    # here; don't rediscover it. Same fix as walk_thru_rename.
                    t["_swept_pid"] = str(pid)
                    all_tasks.append(t)
    return all_tasks, failed_pids, statuses


def _fetch_reservations_range(token, start, end) -> tuple:
    """Every owner/hold/block arrival in the window. Returns
    (reservations, complete, status).

    complete=False means pagination stopped early — a throttle, a bad status or
    a timeout — and the list is SHORT by an unknown amount. That is a different
    failure from a property whose tasks wouldn't load, and it has no
    per-property remedy: the houses it lost never make it into the pid list, so
    "re-read just the failed ones" cannot ask about them. The caller has to say
    the arrival list itself is incomplete.

    Every `break` below used to be indistinguishable from a clean end of
    pagination, so a throttled first page produced an empty arrival map and the
    scan cheerfully reported nothing to rename.
    """
    from routes.bw_ratelimit import gate, LOCAL_THROTTLE_STATUS

    all_results, page = [], 1
    while True:
        if not gate.acquire():
            return all_results, False, LOCAL_THROTTLE_STATUS
        try:
            r = bw_get(
                f"{BW_BASE}/public/inventory/v1/reservation",
                headers={"Authorization": f"JWT {token}"},
                params={"checkin_date_ge": start.isoformat(),
                        "checkin_date_le": end.isoformat(),
                        "limit": 100, "page": page},
                timeout=20,
            )
            gate.on_response(r.status_code)
            if r.status_code != 200:
                return all_results, False, r.status_code
            body = r.json()
            results = body.get("results", body.get("data", []))
            if not results:
                break                      # genuine end of pagination
            all_results.extend(results)
            if len(results) < 100:
                break                      # last (partial) page — complete
            page += 1
        except Exception:
            return all_results, False, None
    return all_results, True, 200


def _patch_task_name(token, task_id, new_name, meta: dict = None):
    """meta carries the pre-write name/property so the audit log records
    what the rename replaced."""
    from routes.bw_audit import log_bw_write
    meta = meta or {}
    headers = {"Authorization": f"JWT {token}", "Content-Type": "application/json"}
    url = f"{BW_BASE}/public/inventory/v1/task/{task_id}"
    try:
        r = bw_patch(url, headers=headers, json={"name": new_name}, timeout=15)
        ok = r.status_code in (200, 201)
        try:
            returned = r.json().get("name") or "(not in response)"
            msg = f"status={r.status_code} name='{returned}'"
        except Exception:
            msg = f"status={r.status_code} body={r.text[:200]}"
        log_bw_write("pri_rename", "name", task_id=task_id,
                     task_name=meta.get("old_name"), property_name=meta.get("property"),
                     task_date=meta.get("date"), old_value=meta.get("old_name"),
                     new_value=new_name, ok=ok, detail=msg)
        return ok, msg
    except Exception as e:
        log_bw_write("pri_rename", "name", task_id=task_id,
                     task_name=meta.get("old_name"), property_name=meta.get("property"),
                     task_date=meta.get("date"), old_value=meta.get("old_name"),
                     new_value=new_name, ok=False, detail=f"{type(e).__name__}: {e}")
        return False, str(e)


@pri_rename_bp.route("/admin/pri-rename")
@login_required
@admin_required
def pri_rename_page():
    return render_template("pri_rename.html")


@pri_rename_bp.route("/admin/pri-rename/scan", methods=["POST"])
@login_required
@admin_required
def pri_rename_scan():
    from routes.briefing import _classify_reservation
    token = _get_token()
    if not token:
        return jsonify({"error": "Breezeway not configured."}), 500

    today = date.today()
    body  = request.get_json(silent=True) or {}
    try:
        start = date.fromisoformat(body["start"]) if "start" in body else today
        end   = date.fromisoformat(body["end"])   if "end"   in body else today + timedelta(days=30)
    except ValueError:
        start, end = today, today + timedelta(days=30)

    ck     = (start.isoformat(), end.isoformat())
    force  = bool(body.get("force"))
    # "Re-read just the missing N" — ask about ONLY the properties that failed
    # last time and merge them into what already loaded. A plain rescan re-reads
    # every property, which is the expensive thing the user is trying to avoid
    # and the thing most likely to earn another round of 429s.
    retry_failed = bool(body.get("retry_failed"))
    cached = _scan_cache.get(ck)
    if cached and not force and not retry_failed and _time.time() - cached[0] < _SCAN_TTL:
        return jsonify(cached[1])

    held_tasks, retry_pids, owner_arrivals, pids = [], None, None, None
    if retry_failed and cached and _time.time() - cached[0] < _RETRY_WINDOW:
        # cached = (ts, result, tasks, failed_pids, owner_arrivals, pids)
        held_tasks     = list(cached[2])
        retry_pids     = list(cached[3])
        owner_arrivals = cached[4]
        pids           = cached[5]
    if retry_failed and not retry_pids:
        # Never let "just the missing ones" silently become a full re-sweep —
        # that is the expensive call this exists to avoid. Say so instead.
        return jsonify({"error": "The list of which properties failed has expired. "
                                 "Scan again to do a full re-check."})

    # True unless THIS run fetched the reservations and came up short. A cached
    # arrival map is complete by construction — an incomplete one is never cached.
    reso_complete, reso_status = True, 200
    if owner_arrivals is None:
        # Homeowner / hold / block arrivals across a wide forward window (the next
        # such arrival can be well past the inspection date). Holds fold into "block".
        reservations, reso_complete, reso_status = _fetch_reservations_range(
            token, start, end + timedelta(days=LOOKAHEAD_DAYS))
        owner_arrivals = {}    # pid -> sorted [date]
        for r in reservations:
            if _classify_reservation(r) not in ("owner", "block"):
                continue
            pid     = str(r.get("property_id") or r.get("home_id") or "")
            checkin = r.get("checkin_date") or ""
            if pid and checkin:
                try:
                    owner_arrivals.setdefault(pid, []).append(date.fromisoformat(checkin[:10]))
                except ValueError:
                    pass
        for pid in owner_arrivals:
            owner_arrivals[pid].sort()
        # Only scan properties that actually have an upcoming owner/hold/block arrival.
        pids = list(owner_arrivals.keys())

    # A retry sweeps ONLY what failed; a normal scan sweeps them all.
    sweep_pids = retry_pids if retry_pids else pids
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

    # Where the tasks went. Without this, "no PRIs to rename" and "every task was
    # silently discarded" are the same sentence on screen — which is exactly how
    # this module's arrival match could break without anyone noticing.
    funnel = {"fetched": len(tasks), "not_pri": 0, "bad_date": 0,
              "no_arrival_match": 0, "already_correct": 0}

    proposals = []
    for t in tasks:
        title = (t.get("title") or t.get("name") or "")
        if isinstance(title, dict):
            title = title.get("value") or title.get("name") or ""
        if not PRI_PATTERN.search(title):
            funnel["not_pri"] += 1
            continue

        task_id = t.get("id") or t.get("task_id")
        # The pid this task was FETCHED for, in the same id space the arrival
        # map is keyed in. Payload fields are the fallback, home_id first — the
        # precedence dispatch.py uses; property_id first is what broke the match.
        pid     = (t.get("_swept_pid")
                   or str(t.get("home_id") or t.get("property_id") or ""))
        sched   = t.get("scheduled_date") or ""
        try:
            task_date = date.fromisoformat(sched[:10])
        except (ValueError, TypeError):
            funnel["bad_date"] += 1
            continue

        arrival = next((d for d in owner_arrivals.get(pid, []) if d >= task_date), None)
        if not arrival:
            funnel["no_arrival_match"] += 1
            continue

        proposed = _build_proposed_title(title, arrival)
        if proposed == title.strip():
            funnel["already_correct"] += 1
            continue   # already correctly dated — nothing to change

        proposals.append({
            "task_id":        task_id,
            "pid":            pid,
            "property":       _get_property_name(pid),
            "current_title":  title,
            "task_date":      sched[:10],
            "arrival_date":   arrival.isoformat(),
            "proposed_title": proposed,
            "is_bb":          bool(BB_PREFIX.match(title.strip())),
        })

    proposals.sort(key=lambda x: x["task_date"])
    # funnel travels with the result so an empty list can explain itself, and the
    # failure fields travel with it so an empty list cannot be MISTAKEN for one.
    # Without these the page can only ever say "here are your proposals" — never
    # "and 57 properties never loaded", which is how a real inspection went
    # un-renamed without a trace.
    result = {"proposals":          proposals,
              "funnel":             funnel,
              "failed_properties":  len(failed_pids),
              "failure_statuses":   failure_statuses,
              "scanned_properties": len(pids)}
    # A short reservation list is a DIFFERENT failure and has no per-property
    # remedy — the houses it lost never reached the pid list, so a narrow retry
    # cannot ask about them. Report it separately so the page can say the
    # arrival list itself is incomplete.
    if not reso_complete:
        result["reservations_incomplete"] = True
        result["reservations_status"]     = reso_status
    # Never cache a run built on a short arrival list: caching it would pin the
    # truncated pid set for the next 90 seconds, so scanning again — the one move
    # that could recover the missing houses — would replay the same gap instantly
    # and look like confirmation.
    if reso_complete:
        _scan_cache[ck] = (_time.time(), result, tasks, failed_pids,
                           owner_arrivals, pids)
    return jsonify(result)


@pri_rename_bp.route("/admin/pri-rename/apply", methods=["POST"])
@login_required
@admin_required
def pri_rename_apply():
    token = _get_token()
    if not token:
        return jsonify({"error": "Breezeway not configured."}), 500

    items   = request.json.get("items", [])
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
