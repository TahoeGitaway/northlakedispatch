"""
routes/walk_thru_rename.py — Walk Thru task rename tool.

Scans Breezeway for Walk Thru tasks (today → +30 days) that don't already
have a date in the name, finds the next reservation arrival on or after each
task's scheduled date for that property, and proposes a rename like
"Walk Thru for 6/15". Admin reviews and approves before anything is changed.

Two endpoints:
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

WALK_THRU_PATTERNS = re.compile(
    r"\b(walk[\s\-]?thru|walk[\s\-]?through|lease[\s\-]?walk|move[\s\-]?in[\s\-]?inspection|arrival[\s\-]?task|guest[\s\-]?arrival)\b",
    re.IGNORECASE,
)
ALREADY_DATED = re.compile(r"(\bfor\s+)?\d{1,2}/\d{1,2}", re.IGNORECASE)
BB_PREFIX     = re.compile(r"^b/b\s+", re.IGNORECASE)


def _get_token():
    from routes.briefing import _get_breezeway_token
    return _get_breezeway_token()


def _get_property_name(pid):
    from routes.briefing import _get_live_property_cache, _ensure_property_cache
    _ensure_property_cache()
    cache = _get_live_property_cache()
    # Try string key, then integer key
    return (cache.get(str(pid)) or
            cache.get(int(pid) if str(pid).isdigit() else pid) or
            str(pid))


def _build_proposed_title(title: str, arrival: date) -> str:
    date_str = f"{arrival.month}/{arrival.day}"
    if BB_PREFIX.match(title):
        return f"{title} {date_str}"
    return f"{title} for {date_str}"


def _fetch_tasks_for_property(token: str, pid: str, ref_id: str, start: date, end: date) -> list:
    """Fetch Breezeway tasks for one property over a date range."""
    date_range = f"{start.isoformat()},{end.isoformat()}"
    id_pairs = []
    if ref_id:
        id_pairs.append(("reference_property_id", ref_id))
    id_pairs += [("property_id", pid), ("home_id", pid)]
    for key, val in id_pairs:
        try:
            r = requests.get(
                f"{BW_BASE}/public/inventory/v1/task/",
                headers={"Authorization": f"JWT {token}"},
                params={"scheduled_date": date_range, key: val, "limit": 100},
                timeout=15,
            )
            if r.status_code == 200:
                body = r.json()
                results = body.get("results", body.get("data", body if isinstance(body, list) else []))
                if results:
                    return results
        except Exception:
            pass
    return []


def _fetch_tasks_for_pids(token: str, pids: list[str], start: date, end: date) -> list:
    """Fetch tasks only for the given property IDs — much faster than all properties."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from routes.briefing import _get_live_ref_cache
    ref_cache = _get_live_ref_cache()

    all_tasks = []
    seen_ids: set = set()

    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(_fetch_tasks_for_property, token, pid, ref_cache.get(pid, ""), start, end): pid
                   for pid in pids}
        for future in as_completed(futures):
            for t in (future.result() or []):
                tid = t.get("id")
                if tid is None or tid not in seen_ids:
                    if tid is not None:
                        seen_ids.add(tid)
                    all_tasks.append(t)
    return all_tasks


def _fetch_reservations_range(token: str, start: date, end: date) -> list:
    """Fetch all Breezeway reservations with checkin in a date range."""
    all_results = []
    page = 1
    while True:
        try:
            r = requests.get(
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
            if r.status_code != 200:
                break
            body = r.json()
            results = body.get("results", body.get("data", []))
            if not results:
                break
            all_results.extend(results)
            if len(results) < 100:
                break
            page += 1
        except Exception:
            break
    return all_results


def _patch_task_title(token: str, task_id, new_title: str) -> tuple[bool, str]:
    """PATCH a Breezeway task's title. Tries 'title' then 'name'."""
    headers = {"Authorization": f"JWT {token}", "Content-Type": "application/json"}
    url = f"{BW_BASE}/public/inventory/v1/task/{task_id}"
    try:
        r = requests.patch(url, headers=headers, json={"name": new_title}, timeout=15)
        ok = r.status_code in (200, 201)
        try:
            body = r.json()
            returned_name = body.get("name") or body.get("title") or "(not in response)"
            msg = f"status={r.status_code} returned name='{returned_name}'"
        except Exception:
            msg = f"status={r.status_code} body={r.text[:200]}"
        return ok, msg
    except Exception as e:
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

    # Step 1: fetch reservations — one call, get all arrivals in window
    reservations = _fetch_reservations_range(token, start, end + timedelta(days=1))

    # Index by property_id → sorted checkin dates; collect only pids with arrivals
    reso_by_prop: dict[str, list[date]] = {}
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

    # Step 2: only fetch tasks for properties that actually have upcoming arrivals
    arrival_pids = list(reso_by_prop.keys())
    tasks = _fetch_tasks_for_pids(token, arrival_pids, start, end) if arrival_pids else []

    proposals = []
    for t in tasks:
        title = (t.get("title") or t.get("name") or "")
        if isinstance(title, dict):
            title = title.get("value") or title.get("name") or ""

        if not WALK_THRU_PATTERNS.search(title):
            continue
        if ALREADY_DATED.search(title):
            continue

        task_id  = t.get("id") or t.get("task_id")
        pid      = str(t.get("property_id") or t.get("home_id") or "")
        sched    = t.get("scheduled_date") or ""
        try:
            task_date = date.fromisoformat(sched[:10])
        except (ValueError, TypeError):
            continue

        arrivals = reso_by_prop.get(pid, [])
        arrival  = next((d for d in arrivals if d >= task_date), None)
        if not arrival:
            continue

        prop_name    = _get_property_name(pid)
        proposed     = _build_proposed_title(title, arrival)
        proposals.append({
            "task_id":       task_id,
            "property":      prop_name,
            "current_title": title,
            "task_date":     sched[:10],
            "arrival_date":  arrival.isoformat(),
            "proposed_title": proposed,
        })

    proposals.sort(key=lambda x: x["task_date"])

    # Debug info — always returned so we can diagnose empty results
    walk_thru_tasks = []
    for t in tasks:
        title = (t.get("title") or t.get("name") or "")
        if isinstance(title, dict):
            title = title.get("value") or title.get("name") or ""
        if WALK_THRU_PATTERNS.search(title):
            pid = str(t.get("property_id") or t.get("home_id") or "")
            sched = t.get("scheduled_date") or ""
            walk_thru_tasks.append({
                "title": title,
                "already_dated": bool(ALREADY_DATED.search(title)),
                "pid": pid,
                "sched": sched,
                "has_arrival": pid in reso_by_prop,
            })

    debug = {
        "total_tasks_fetched": len(tasks),
        "total_reservations_fetched": len(reservations),
        "reservation_pids": list(reso_by_prop.keys())[:10],
        "walk_thru_matches": walk_thru_tasks,
        "sample_task_keys": list(tasks[0].keys()) if tasks else [],
        "sample_task": {k: tasks[0].get(k) for k in ["title", "name", "property_id", "home_id", "scheduled_date"]} if tasks else {},
    }

    return jsonify({"proposals": proposals, "debug": debug})


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
        ok, msg = _patch_task_title(token, item["task_id"], item["proposed_title"])
        results.append({
            "task_id":       item["task_id"],
            "property":      item.get("property", ""),
            "proposed_title": item["proposed_title"],
            "success":       ok,
            "detail":        msg,
        })

    return jsonify({"results": results})
