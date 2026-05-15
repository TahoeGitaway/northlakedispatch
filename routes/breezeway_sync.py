"""
routes/breezeway_sync.py — Breezeway task time sync.

Standalone blueprint. Zero shared code with dispatch.py.
One endpoint: POST /api/bw-sync-times
Reads an optimized schedule, finds existing Breezeway tasks for each stop
on the route date, and PATCHes their start_time to match the route ETA.
Never creates tasks. Only updates tasks that already exist.
"""

import requests
from flask import Blueprint, request, jsonify
from flask_login import login_required

bw_sync_bp = Blueprint("bw_sync", __name__)

BW_BASE = "https://api.breezeway.io"


def _minutes_to_hhmm(minutes: int) -> str:
    h = (minutes // 60) % 24
    m = minutes % 60
    return f"{h:02d}:{m:02d}:00"


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


def _fetch_tasks_for_property(token: str, ref_id: str, date_str: str) -> list:
    """Fetch existing Breezeway tasks for one property on one date."""
    for params in [
        {"reference_property_id": ref_id, "scheduled_date": f"{date_str},{date_str}"},
        {"reference_property_id": ref_id, "start_date": date_str, "end_date": date_str},
        {"reference_property_id": ref_id, "date": date_str},
    ]:
        try:
            r = requests.get(
                f"{BW_BASE}/public/inventory/v1/task",
                headers={"Authorization": f"JWT {token}"},
                params={**params, "limit": 50},
                timeout=15,
            )
            if r.status_code == 200:
                data = r.json()
                results = data.get("results", data.get("data", data if isinstance(data, list) else []))
                return results or []
        except Exception:
            pass
    return []


def _patch_task_time(token: str, task_id: int, start_time: str) -> tuple[bool, str]:
    """PATCH a single task's start_time. Returns (success, message)."""
    try:
        r = requests.patch(
            f"{BW_BASE}/public/inventory/v1/task/{task_id}",
            headers={"Authorization": f"JWT {token}", "Content-Type": "application/json"},
            json={"start_time": start_time},
            timeout=15,
        )
        if r.status_code in (200, 201):
            return True, f"updated to {start_time}"
        return False, f"API returned {r.status_code}: {r.text[:120]}"
    except Exception as e:
        return False, str(e)


@bw_sync_bp.route("/api/bw-sync-times", methods=["POST"])
@login_required
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

    for stop in stops:
        name        = (stop.get("name") or "").strip()
        eta_minutes = stop.get("eta_minutes")

        if not name or eta_minutes is None:
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
        tasks = _fetch_tasks_for_property(token, ref_id, date_str)
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
        task_results = []
        for task in tasks:
            task_id   = task.get("id")
            task_name = (task.get("name") or "task")[:40]
            ok, msg   = _patch_task_time(token, task_id, start_time)
            task_results.append({"task_id": task_id, "task_name": task_name,
                                 "ok": ok, "msg": msg})

        all_ok = all(t["ok"] for t in task_results)
        results.append({
            "name":    name,
            "status":  "updated" if all_ok else ("partial" if any(t["ok"] for t in task_results) else "failed"),
            "time":    start_time,
            "tasks":   task_results,
        })

    updated = sum(1 for r in results if r["status"] == "updated")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    failed  = sum(1 for r in results if r["status"] == "failed")

    return jsonify({
        "results": results,
        "summary": {"updated": updated, "skipped": skipped, "failed": failed},
    })
