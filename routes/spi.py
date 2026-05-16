"""
routes/spi.py — Spring Property Inspection (SPI) tracker.

Fetches all Breezeway tasks whose title contains "spring property inspection"
(case-insensitive) scheduled in 2026 and reports completion status per property.

Endpoints:
  GET  /admin/spring-inspections      — SPI tracker page (admin only)
  GET  /api/spi-status                — JSON data (admin only)
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

from flask import Blueprint, jsonify, render_template, request
from flask_login import login_required

from routes.auth import admin_required

spi_bp = Blueprint("spi", __name__)

# Date windows that cover all of 2026 in ≤30-day chunks
_2026_WINDOWS = []
_d = date(2026, 1, 1)
_end = date(2026, 12, 31)
while _d <= _end:
    _w = min(_d + timedelta(days=29), _end)
    _2026_WINDOWS.append((_d.isoformat(), _w.isoformat()))
    _d = _w + timedelta(days=1)


def _fetch_tasks_for_property(token, pid, ref_id, start, end):
    """Fetch tasks for one property over one date window. Returns list or []."""
    from routes.briefing import _fetch_bw_endpoint

    task_paths = [
        "/public/work/v1/task",
        "/public/inventory/v1/task",
        "/public/work/v2/task",
    ]
    base = {"scheduled_date_ge": start, "scheduled_date_le": end}
    id_pairs = (
        [("reference_property_id", ref_id)] if ref_id else []
    ) + [("property_id", pid), ("home_id", pid)]

    for path in task_paths:
        for key, val in id_pairs:
            results, _err, sc = _fetch_bw_endpoint(token, path, {**base, key: val})
            if sc == 200:
                return results or []
            if sc == 403:
                return []
    return []


def _title_str(task):
    t = task.get("title") or task.get("name") or ""
    if isinstance(t, dict):
        t = t.get("value") or t.get("name") or ""
    return str(t).strip()


def _completion_date(task):
    """Return the best available completion date string, or None."""
    for field in ("completed_at", "completed_date", "completion_date", "updated_at"):
        val = task.get(field)
        if val:
            # Trim to date part if it's a datetime string
            return str(val)[:10]
    return None


def fetch_spi_data():
    """
    Scan all properties for 2026 tasks whose title contains
    'spring property inspection'.  Returns (list_of_results, error_str).

    Each result dict:
        property, title, status_raw, is_complete, completed_date, scheduled_date
    """
    from routes.briefing import (
        _get_breezeway_token,
        _get_live_property_cache,
        _get_live_ref_cache,
        _ensure_property_cache,
    )

    token = _get_breezeway_token()
    if not token:
        return None, "Breezeway not configured."

    _ensure_property_cache()
    prop_cache = _get_live_property_cache()   # {pid: name}
    ref_cache  = _get_live_ref_cache()        # {pid: ref_id}

    if not prop_cache:
        return None, "No properties found in Breezeway."

    # Build work units: (pid, name, ref_id, start, end) for every property × window
    work = [
        (pid, name, ref_cache.get(pid, ""), win_start, win_end)
        for pid, name in prop_cache.items()
        for win_start, win_end in _2026_WINDOWS
    ]

    seen_task_ids = set()
    spi_tasks = []   # raw task dicts augmented with property name

    def fetch_one(pid, name, ref_id, win_start, win_end):
        tasks = _fetch_tasks_for_property(token, pid, ref_id, win_start, win_end)
        matches = []
        for t in tasks:
            title = _title_str(t)
            if "spring property inspection" in title.lower():
                t["_prop_name"] = name
                matches.append(t)
        return matches

    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = [ex.submit(fetch_one, *args) for args in work]
        for fut in as_completed(futures):
            for task in fut.result():
                tid = task.get("id")
                if tid is None or tid not in seen_task_ids:
                    if tid is not None:
                        seen_task_ids.add(tid)
                    spi_tasks.append(task)

    results = []
    for task in spi_tasks:
        status_raw = str(task.get("status") or "").lower().strip()
        is_complete = status_raw in ("complete", "completed", "done", "finished")

        # Pull every name from assignments — Breezeway includes current + may include history
        assignee_names = []
        seen_names = set()
        for src_key in ("assignments", "assignment_history", "history_assignments", "assignees"):
            for a in (task.get(src_key) or []):
                if not isinstance(a, dict):
                    continue
                name = (
                    a.get("name") or
                    f"{a.get('first_name', '')} {a.get('last_name', '')}".strip() or
                    a.get("email") or ""
                ).strip()
                if name and name not in seen_names:
                    seen_names.add(name)
                    assignee_names.append(name)

        results.append({
            "property":       task["_prop_name"],
            "title":          _title_str(task),
            "status_raw":     status_raw,
            "is_complete":    is_complete,
            "completed_date": _completion_date(task) if is_complete else None,
            "scheduled_date": (task.get("scheduled_date") or "")[:10] or None,
            "assignees":      assignee_names,
        })

    # Sort: incomplete first (so they're the focus), then alphabetical within each group
    results.sort(key=lambda r: (r["is_complete"], r["property"].lower()))
    return results, None


@spi_bp.route("/admin/spring-inspections")
@login_required
@admin_required
def spi_page():
    return render_template("admin_spi.html")


@spi_bp.route("/api/spi-status")
@login_required
@admin_required
def api_spi_status():
    results, err = fetch_spi_data()
    if err:
        return jsonify({"error": err}), 500
    total      = len(results)
    complete   = sum(1 for r in results if r["is_complete"])
    incomplete = total - complete
    return jsonify({
        "tasks":      results,
        "total":      total,
        "complete":   complete,
        "incomplete": incomplete,
    })
