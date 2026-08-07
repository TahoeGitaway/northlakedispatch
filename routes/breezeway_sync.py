"""
routes/breezeway_sync.py — Breezeway task time sync.

Standalone blueprint. Zero shared code with dispatch.py.
One endpoint: POST /api/bw-sync-times
Reads an optimized schedule, finds existing Breezeway tasks for each stop
on the route date, and PATCHes their start_time to match the route ETA.
Never creates tasks. Only updates tasks that already exist.
"""

import time

import requests
from flask import Blueprint, request, jsonify
from flask_login import login_required
from routes.auth import admin_required
from routes.bw_api_log import bw_get

bw_sync_bp = Blueprint("bw_sync", __name__)

BW_BASE = "https://api.breezeway.io"


def _minutes_to_hhmm(minutes: int) -> str:
    # Quantize the synced start time to the nearest 5 minutes (so :00, :05, :10 … :55).
    minutes = round(minutes / 5) * 5
    h = (minutes // 60) % 24
    m = minutes % 60
    return f"{h:02d}:{m:02d}:00"


def _minutes_to_datetime(minutes: int, date_str: str) -> str:
    """Return ISO datetime string for Breezeway: 'YYYY-MM-DDTHH:MM:00'."""
    h = (minutes // 60) % 24
    m = minutes % 60
    return f"{date_str}T{h:02d}:{m:02d}:00"


def _get_token() -> str | None:
    from routes.briefing import _get_breezeway_token
    return _get_breezeway_token()


def _find_bw_property_id(local_name: str, prop_cache: dict) -> int | None:
    """
    Reverse-match a local DB property name to a Breezeway property id.
    prop_cache: {bw_id: bw_name}
    Uses the same fuzzy logic as _match_local_property but in reverse.
    """
    from difflib import get_close_matches
    key = local_name.lower().strip()
    bw_lower = {pid: name.lower().strip() for pid, name in prop_cache.items()}

    # Exact
    for pid, bw_name in bw_lower.items():
        if key == bw_name:
            return pid

    # Substring
    for pid, bw_name in bw_lower.items():
        if key in bw_name or bw_name in key:
            return pid

    # Keywords
    kwords = set(key.split())
    for pid, bw_name in bw_lower.items():
        bw_words = set(bw_name.split())
        if kwords and kwords.issubset(bw_words):
            return pid

    # Fuzzy
    names = list(bw_lower.values())
    hits = get_close_matches(key, names, n=1, cutoff=0.6)
    if hits:
        for pid, bw_name in bw_lower.items():
            if bw_name == hits[0]:
                return pid

    return None


def _task_matches_assignee(task: dict, assignee_lower: str) -> bool:
    """Return True if any assignment on this task contains the given name."""
    for a in (task.get("assignments") or []):
        candidates = [
            a.get("name", ""),
            a.get("full_name", ""),
            f"{a.get('first_name','').strip()} {a.get('last_name','').strip()}".strip(),
        ]
        if any(assignee_lower in c.lower() for c in candidates if c):
            return True
    return False


def _fetch_tasks_for_property(token: str, ref_id: str, date_str: str,
                              deadline: float = 0.0) -> tuple:
    """Fetch existing Breezeway tasks for one property on one date.

    Returns (tasks, ok, detail). ok=False means the lookup FAILED — which is not
    the same as a property having no tasks, though this used to return [] for
    both. The caller reported that as "no tasks found for this property on that
    date", so a throttled lookup silently skipped the stop and the route's times
    were never written, with nothing in the UI to say so.

    Retries throttles and transient server errors, and paces through the shared
    rate gate so this path can't blow the budget the read-side scans depend on."""
    from routes.bw_ratelimit import gate

    # The caller iterates stops SEQUENTIALLY, so every second spent here is paid
    # once per stop. Retries plus a gate that can block for seconds each turned an
    # 8-stop sync into minutes of apparent hanging. Bound the whole thing: two
    # attempts, alternate param shapes only on the first pass, and a hard deadline
    # shared across the sync so it always returns something.
    last_detail = "no response"
    # ONE query shape, and it's the documented one. The old fallbacks used
    # start_date/end_date and date, none of which exist in Breezeway's task API
    # (the real filters are scheduled_date, created_at, finished_at, updated_at).
    # An API that ignores unknown parameters answers those with the property's
    # ENTIRE task list, every date — and this endpoint then PATCHes each one to the
    # route's time. That is how a sync retimed a housekeeper's task on a different
    # day. Never guess at filters on a write path.
    shapes = [
        {"reference_property_id": ref_id, "scheduled_date": f"{date_str},{date_str}"},
    ]
    for attempt in range(2):
        if deadline and time.monotonic() > deadline:
            return ([], False, "ran out of time before this property could be checked")
        # Only hunt alternate param shapes on the first pass — if shape 1 worked
        # before, a retry is about throttling, not about the query being wrong.
        for params in (shapes if attempt == 0 else shapes[:1]):
            if deadline and time.monotonic() > deadline:
                return ([], False, "ran out of time before this property could be checked")
            if not gate.acquire():
                last_detail = "held back by this app's rate limiter"
                continue
            try:
                r = bw_get(
                    f"{BW_BASE}/public/inventory/v1/task",
                    headers={"Authorization": f"JWT {token}"},
                    params={**params, "limit": 50},
                    timeout=15,
                )
                gate.on_response(r.status_code)
                if r.status_code == 200:
                    data = r.json()
                    results = data.get("results", data.get("data", data if isinstance(data, list) else []))
                    return (results or [], True, "")
                last_detail = f"HTTP {r.status_code}: {(r.text or '')[:200]}"
                # A throttle or server error is worth another attempt; a 4xx that
                # isn't 429 means this param shape is wrong, so try the next one.
                if r.status_code == 429 or r.status_code >= 500:
                    break
            except requests.exceptions.Timeout:
                last_detail = "timed out after 15 s"
                break
            except Exception as ex:
                last_detail = f"{type(ex).__name__}: {ex}"[:200]
        if attempt == 0:          # no point sleeping after the final attempt
            time.sleep(0.4)
    return ([], False, last_detail)


def _patch_task_time(token: str, task_id: int, start_time_hhmm: str, date_str: str,
                     task: dict = None, property_name: str = "") -> tuple[bool, str]:
    """PATCH a task's start time.

    `task` is the Breezeway task as it was BEFORE the write. It is used only to
    record the previous time in the audit log — the one piece of information that
    makes an unintended change reversible, and which was gone when a sync retimed
    tasks that were never in scope.
    """
    headers = {"Authorization": f"JWT {token}", "Content-Type": "application/json"}
    url     = f"{BW_BASE}/public/inventory/v1/task/{task_id}"

    from routes.bw_ratelimit import gate
    from routes.bw_audit import log_bw_write

    task = task or {}
    _before   = str(task.get("scheduled_time") or "")[:8]
    _name     = task.get("name") or ""
    _taskdate = str(task.get("scheduled_date") or date_str)[:10]
    # Tags as the task carried them at write time. Breezeway puts them on either
    # key, and entries are {"name": ...} or bare strings.
    _tags = []
    for _k in ("task_tags", "tags"):
        for _t in (task.get(_k) or []):
            _n = (_t.get("name") if isinstance(_t, dict) else str(_t) or "").strip()
            if _n and _n not in _tags:
                _tags.append(_n)

    def _audit(ok, detail):
        log_bw_write("sync_times", "scheduled_time", task_id=task_id,
                     task_name=_name, property_name=property_name,
                     task_date=_taskdate, old_value=_before,
                     new_value=start_time_hhmm, ok=ok, detail=detail,
                     tags=_tags)

    for payload in [
        {"scheduled_time": start_time_hhmm},
    ]:
        try:
            # Writes share the same rate limit as reads — pace them too, or a sync
            # can exhaust the budget the scans depend on.
            if not gate.acquire():
                _audit(False, "held back by this app's rate limiter — not sent")
                return False, "held back by this app's rate limiter — not sent"
            r = requests.patch(url, headers=headers, json=payload, timeout=15)
            gate.on_response(r.status_code)
            try:
                body = r.json()
                got_start  = body.get("scheduled_start") or body.get("start_time") or "?"
                body_keys  = list(body.keys())[:10]
                msg = f"status={r.status_code} payload={list(payload.keys())[0]}={list(payload.values())[0]} echoed={got_start} keys={body_keys}"
            except Exception:
                msg = f"status={r.status_code} payload={payload} raw={r.text[:200]}"
            if r.status_code in (200, 201):
                _audit(True, msg)
                return True, msg
            # non-2xx: report and stop trying
            _audit(False, msg)
            return False, msg
        except Exception as e:
            _audit(False, f"{type(e).__name__}: {e}")
            return False, str(e)
    _audit(False, "all payload variants failed")
    return False, "all payload variants failed"


@bw_sync_bp.route("/api/bw-sync-times", methods=["POST"])
@login_required
@admin_required
def bw_sync_times():
    """
    Body: {"date": "YYYY-MM-DD", "stops": [{"name": str, "eta_minutes": int}, ...]}

    For each stop:
      1. Reverse-match local name -> Breezeway property id
      2. Fetch existing tasks for that property on that date
      3. PATCH start_time on each task found
      4. If no task found, report as skipped (never creates tasks)
    """
    from routes.briefing import _ensure_property_cache, _get_live_property_cache, _get_live_ref_cache

    body          = request.get_json() or {}
    date_str      = (body.get("date") or "").strip()
    stops         = body.get("stops") or []
    assignee_raw  = (body.get("assignee") or "").strip()
    assignee_lower = assignee_raw.lower() if assignee_raw else ""

    if not date_str:
        return jsonify({"error": "date is required"}), 400
    if not stops:
        return jsonify({"error": "no stops provided"}), 400
    # HARD REFUSAL. Without a name the per-assignee filter below was skipped, so the
    # sync rewrote EVERY task at each property that day — other people's included.
    # It happened: a route synced with an empty assignee retimed a second cleaner's
    # tasks and a housekeeper's on an unrelated day. There is no valid reason to
    # write times for "whoever happens to be at this property", so refuse outright
    # rather than treating a missing name as "no filter".
    if not assignee_raw:
        return jsonify({
            "error": "No employee is set on this route, so there's no way to tell "
                     "whose tasks to update. Set the employee before syncing — "
                     "syncing without one would change other people's task times.",
            "diagnostics": {"stage": "assignee_required", "date": date_str,
                            "stops": len(stops)},
        }), 400

    token = _get_token()
    if not token:
        return jsonify({"error": "Could not authenticate with Breezeway"}), 503

    _ensure_property_cache()
    prop_cache = _get_live_property_cache()   # {bw_id: bw_name}
    ref_cache  = _get_live_ref_cache()         # {bw_id: reference_property_id}

    if not prop_cache:
        return jsonify({"error": "Breezeway property cache empty — try again in a moment"}), 502

    results = []
    # Whole-sync budget. Stops are processed one after another, so without a shared
    # ceiling a throttled run just keeps going and the user watches a spinner with
    # no idea whether anything is happening. Better to return a partial result that
    # says exactly which stops weren't reached. Sits under the platform's gateway
    # timeout so the response is actually delivered rather than cut off.
    _sync_deadline = time.monotonic() + 40.0

    for stop in stops:
        name        = (stop.get("name") or "").strip()
        eta_minutes = stop.get("eta_minutes")

        if not name or eta_minutes is None:
            continue

        # Out of time — record the remaining stops honestly instead of pressing on
        # past the gateway timeout and returning nothing at all.
        if time.monotonic() > _sync_deadline:
            results.append({"name": name, "status": "failed",
                            "time": _minutes_to_hhmm(int(eta_minutes)),
                            "reason": "sync ran out of time before reaching this stop — "
                                      "run it again to finish the rest"})
            continue

        start_time = _minutes_to_hhmm(int(eta_minutes))

        # Step 1: find Breezeway property id for this local name
        bw_pid = _find_bw_property_id(name, prop_cache)
        if bw_pid is None:
            # NOT a benign skip: this stop's name doesn't match any Breezeway
            # property, so its time can never be written until the name is fixed.
            # Reported as "skipped" it counted as success and the page navigated
            # away, hiding a data problem only a human can resolve.
            results.append({"name": name, "status": "unmatched",
                            "time": start_time,
                            "reason": "no Breezeway property matches this name — "
                                      "check the spelling against Breezeway"})
            continue

        ref_id = ref_cache.get(bw_pid) or str(bw_pid)

        # Step 2: find existing tasks for this property on this date
        tasks, lookup_ok, lookup_detail = _fetch_tasks_for_property(
            token, ref_id, date_str, deadline=_sync_deadline)
        if not lookup_ok:
            # Could NOT read this property's tasks — its time was not written. This
            # is a failure, not a skip: reporting it as "no tasks found" is what
            # made a throttled sync look like a successful no-op.
            results.append({"name": name, "status": "failed", "time": start_time,
                            "reason": f"couldn't load this property's tasks — {lookup_detail}",
                            "detail": lookup_detail})
            continue
        if not tasks:
            results.append({"name": name, "status": "skipped",
                            "reason": "no tasks found for this property on that date"})
            continue

        # Step 2a: STRICT date guard. Never trust the query to have filtered — every
        # other module that reads tasks re-checks scheduled_date, and this one, the
        # only module that WRITES, did not. A task on any other day is dropped here
        # no matter what the API returned.
        off_day = [t for t in tasks if (t.get("scheduled_date") or "")[:10] != date_str]
        if off_day:
            tasks = [t for t in tasks if (t.get("scheduled_date") or "")[:10] == date_str]
        # Step 2b: ONLY this person's tasks. assignee_lower is guaranteed non-empty
        # by the guard at the top of the endpoint — an unfiltered sync would rewrite
        # every task at the property, including other people's.
        tasks = [t for t in tasks if _task_matches_assignee(t, assignee_lower)]
        if not tasks:
            results.append({"name": name, "status": "skipped",
                            "reason": f"no tasks assigned to '{assignee_raw}' on that date"})
            continue

        # Step 3: PATCH each task (usually just one per property per day)
        first_task_keys = list(tasks[0].keys()) if tasks else []
        first_linked_reso = tasks[0].get("linked_reservation") if tasks else None
        task_results = []
        for task in tasks:
            task_id   = task.get("id")
            task_name = (task.get("name") or "task")[:40]
            # Pass the pre-write task so the log keeps its ORIGINAL time — without
            # that, an unintended change can be seen but not undone.
            ok, msg   = _patch_task_time(token, task_id, start_time, date_str,
                                         task=task, property_name=name)
            task_results.append({"task_id": task_id, "task_name": task_name,
                                 "ok": ok, "msg": msg})

        all_ok = all(t["ok"] for t in task_results)
        results.append({
            "name":       name,
            "status":     "updated" if all_ok else ("partial" if any(t["ok"] for t in task_results) else "failed"),
            "time":       start_time,
            "tasks":      task_results,
            "task_keys":  first_task_keys,
            "linked_reso": first_linked_reso,
        })

    updated   = sum(1 for r in results if r["status"] == "updated")
    skipped   = sum(1 for r in results if r["status"] == "skipped")
    failed    = sum(1 for r in results if r["status"] == "failed")
    partial   = sum(1 for r in results if r["status"] == "partial")
    unmatched = sum(1 for r in results if r["status"] == "unmatched")

    # Anything that did NOT get its time written and NEEDS a human. A throttled
    # lookup can be retried; a name that matches no Breezeway property cannot —
    # someone has to fix it. Both leave the stop without a time, so both belong
    # here. "no tasks that day" is genuinely benign and stays out.
    not_applied = [
        {"name": r["name"], "status": r["status"], "reason": r.get("reason") or ""}
        for r in results if r["status"] in ("failed", "partial", "unmatched")
    ]

    # NOTHING was written and every stop came back "this property has no task for
    # that person on that day" — that is what syncing to the WRONG DATE looks like.
    # It was reported as a clean run of harmless skips, so a route planned for one
    # day could be synced to another with no warning at all. Say it plainly.
    wrong_day = bool(results) and updated == 0 and partial == 0 and skipped == len(results)
    return jsonify({
        "results": results,
        "summary": {"updated": updated, "skipped": skipped,
                    "failed": failed, "partial": partial,
                    "unmatched": unmatched},
        "not_applied": not_applied,
        "wrong_day_suspected": wrong_day,
        "wrong_day_message": (
            f"No times were written. None of the {len(results)} stops has a task for "
            f"{assignee_raw or 'this person'} on {date_str} — check the date is right "
            f"before syncing again." if wrong_day else ""),
        # A run that wrote nothing must never count as applied, or the page navigates
        # away as though it worked.
        "all_applied": (not not_applied) and not wrong_day,
        "diagnostics": {
            "date": date_str,
            "stops_submitted": len(stops),
            "rate_gate": (lambda: __import__("routes.bw_ratelimit",
                                             fromlist=["gate"]).gate.snapshot())(),
        },
    })
