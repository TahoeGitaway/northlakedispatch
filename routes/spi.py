"""
routes/spi.py — Spring Property Inspection (SPI) tracker.

Fetches all Breezeway tasks whose title contains "spring property inspection"
(case-insensitive) scheduled in 2026 and reports completion status per property.

Endpoints:
  GET  /admin/spring-inspections      — SPI tracker page (admin only)
  GET  /api/spi-status                — JSON data (admin only)
"""

import time
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

# Simple cache: (timestamp, results)
_spi_cache: dict = {"ts": 0, "data": None}  # bust by hitting Refresh or redeploying
_SPI_CACHE_TTL = 3600  # 1 hour


def _title_str(task):
    t = task.get("title") or task.get("name") or ""
    if isinstance(t, dict):
        t = t.get("value") or t.get("name") or ""
    return str(t).strip()


def _completion_date(task):
    for field in ("completed_at", "completed_date", "completion_date", "updated_at"):
        val = task.get(field)
        if val:
            return str(val)[:10]
    return None


def _fetch_window_global(token, start, end):
    """Try to fetch ALL tasks in a date window without a property filter.
    Returns list on success, None if the API requires a property_id.
    """
    from routes.briefing import _fetch_bw_tasks

    results, err = _fetch_bw_tasks(token, {"scheduled_date": f"{start},{end}"})
    if "elevated API access" in (err or ""):
        return None
    return results  # empty list is valid; None means hard failure


def _fetch_tasks_for_property(token, pid, ref_id, start, end):
    """Fetch tasks for one property over one date window. Returns list or []."""
    from routes.briefing import _fetch_bw_tasks

    date_range = f"{start},{end}"
    id_pairs = (
        [("reference_property_id", ref_id)] if ref_id else []
    ) + [("property_id", pid), ("home_id", pid)]

    for key, val in id_pairs:
        results, err = _fetch_bw_tasks(token, {
            "scheduled_date": date_range,
            key: val,
        })
        if results:
            return results
        if "elevated API access" in (err or ""):
            return []
    return []


def _is_spi(title: str) -> bool:
    return "spring property inspection" in title.lower()


def _build_results(raw_tasks: list) -> list:
    """Convert raw task dicts (with _prop_name) into result records."""
    results = []
    for task in raw_tasks:
        status_raw = str(task.get("status") or "").lower().strip()
        is_complete = status_raw in ("complete", "completed", "done", "finished")

        assignee_names = []
        seen_names: set = set()
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

    results.sort(key=lambda r: (r["is_complete"], r["property"].lower()))
    return results


def fetch_spi_data(force_refresh=False):
    """
    Scan all properties for 2026 tasks whose title contains
    'spring property inspection'.  Returns (list_of_results, error_str).
    Results are cached for 1 hour.
    """
    global _spi_cache

    now = time.time()
    if not force_refresh and _spi_cache["data"] is not None and now - _spi_cache["ts"] < _SPI_CACHE_TTL:
        return _spi_cache["data"], None

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
    prop_cache = _get_live_property_cache()
    ref_cache  = _get_live_ref_cache()

    if not prop_cache:
        return None, "No properties found in Breezeway."

    seen_task_ids: set = set()
    spi_tasks: list = []

    # ── Strategy 1: global queries (13 calls covering all of 2026) ──
    global_worked = False
    global_raw: list = []
    for win_start, win_end in _2026_WINDOWS:
        chunk = _fetch_window_global(token, win_start, win_end)
        if chunk is None:
            global_raw = []
            break
        if chunk:
            global_worked = True  # at least one window returned real data
        global_raw.extend(chunk)

    if global_worked:
        # Attach property name from the task's own property reference field
        pid_to_name = prop_cache  # {pid: name}
        for task in global_raw:
            title = _title_str(task)
            if not _is_spi(title):
                continue
            # Find property name from task fields
            prop_name = ""
            for field in ("property_id", "home_id", "reference_property_id"):
                val = task.get(field)
                if val and str(val) in pid_to_name:
                    prop_name = pid_to_name[str(val)]
                    break
            if not prop_name:
                prop_name = task.get("property_name") or task.get("home_name") or "Unknown"
            task["_prop_name"] = prop_name
            tid = task.get("id")
            if tid is None or tid not in seen_task_ids:
                if tid is not None:
                    seen_task_ids.add(tid)
                spi_tasks.append(task)
    else:
        # ── Strategy 2: per-property fallback ──
        work = [
            (pid, name, ref_cache.get(pid, ""), win_start, win_end)
            for pid, name in prop_cache.items()
            for win_start, win_end in _2026_WINDOWS
        ]

        def fetch_one(pid, name, ref_id, win_start, win_end):
            tasks = _fetch_tasks_for_property(token, pid, ref_id, win_start, win_end)
            matches = []
            for t in tasks:
                if _is_spi(_title_str(t)):
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

    results = _build_results(spi_tasks)
    _spi_cache["ts"]   = now
    _spi_cache["data"] = results
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
    force = request.args.get("refresh") == "1"
    results, err = fetch_spi_data(force_refresh=force)
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
