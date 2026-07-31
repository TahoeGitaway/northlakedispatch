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
    shapes = [
        {"reference_property_id": ref_id, "scheduled_date": f"{date_str},{date_str}"},
        {"reference_property_id": ref_id, "start_date": date_str, "end_date": date_str},
        {"reference_property_id": ref_id, "date": date_str},
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
                r = requests.get(
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


def _patch_task_time(token: str, task_id: int, start_time_hhmm: str, date_str: str) -> tuple[bool, str]:
    """PATCH a task's start time. Tries scheduled_start (full datetime) first, then start_time."""
    headers = {"Authorization": f"JWT {token}", "Content-Type": "application/json"}
    url     = f"{BW_BASE}/public/inventory/v1/task/{task_id}"
    dt      = f"{date_str}T{start_time_hhmm}"  # e.g. "2026-05-17T11:29:00"

    from routes.bw_ratelimit import gate

    for payload in [
        {"scheduled_time": start_time_hhmm},
    ]:
        try:
            # Writes share the same rate limit as reads — pace them too, or a sync
            # can exhaust the budget the scans depend on.
            if not gate.acquire():
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
                return True, msg
            # non-2xx: report and stop trying
            return False, msg
        except Exception as e:
            return False, str(e)
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
            results.append({"name": name, "status": "skipped",
                            "reason": "no matching Breezeway property"})
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

        # Step 2b: if an assignee was specified, only touch their tasks
        if assignee_lower:
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
            ok, msg   = _patch_task_time(token, task_id, start_time, date_str)
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

    updated = sum(1 for r in results if r["status"] == "updated")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    failed  = sum(1 for r in results if r["status"] == "failed")
    partial = sum(1 for r in results if r["status"] == "partial")

    # Anything that did NOT get its time written, so the UI can be loud about it.
    # A sync that silently applied nothing is the failure mode being fixed here.
    not_applied = [
        {"name": r["name"], "status": r["status"], "reason": r.get("reason") or ""}
        for r in results if r["status"] in ("failed", "partial")
    ]
    return jsonify({
        "results": results,
        "summary": {"updated": updated, "skipped": skipped,
                    "failed": failed, "partial": partial},
        "not_applied": not_applied,
        "all_applied": not not_applied,
        "diagnostics": {
            "date": date_str,
            "stops_submitted": len(stops),
            "rate_gate": (lambda: __import__("routes.bw_ratelimit",
                                             fromlist=["gate"]).gate.snapshot())(),
        },
    })
