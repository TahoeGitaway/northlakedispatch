"""
routes/bear_fence_delete.py — Bear Fence *delete* tool.

Pick a single day. The tool pulls every "Disarm Bear Fence" task scheduled on
that day and shows, per house: who it's assigned to, what time, what date, plus
the rest of that house's tasks that day for context. Admin checks the ones to
delete, types a confirmation, and only then are they removed from Breezeway.

HARD CONSTRAINT (enforced server-side, structurally):
  A task is deleted ONLY if it is selected AND its LIVE name — re-read from
  Breezeway at delete time — is exactly "Disarm Bear Fence". The client cannot
  cause any other task to be deleted; the server refuses anything else.

Deletion is permanent and cannot be undone.

Endpoints:
  GET  /admin/bear-fence-delete         — page
  POST /admin/bear-fence-delete/scan    — pull Disarm Bear Fence tasks for a day (JSON)
  POST /admin/bear-fence-delete/delete  — delete the selected tasks (JSON)
"""

import re
import time as _time
import requests
from datetime import date, timedelta

from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required

from routes.auth import admin_required
from routes.group_assign import _assignee_names
from routes.bear_fence import (
    BW_BASE,
    _get_token,
    _get_property_name,
    _fetch_tasks_for_pids,
    _fetch_reservations_range,
    _fetch_task_by_id,
)

bear_fence_delete_bp = Blueprint("bear_fence_delete", __name__)

# The candidate window around the chosen day. A "Disarm Bear Fence" task is an
# arrival task, so its property has a reservation checking in on (or right
# around) that day. ±1 day catches early/late disarms without over-scanning.
CANDIDATE_WINDOW_DAYS = 1

# Per-day scan cache. The per-property task sweep can run past the hosting
# proxy's timeout (→ "upstream error"); caching means the backend finishes and a
# retry returns instantly. Cleared after any delete.
_scan_cache: dict = {}      # day_iso -> (timestamp, result_dict)
_SCAN_TTL = 90


def _task_title(t: dict) -> str:
    ti = t.get("name") or t.get("title") or ""
    if isinstance(ti, dict):
        ti = ti.get("value") or ti.get("name") or ""
    return ti or ""


def _norm_name(name: str) -> str:
    """Collapse whitespace/hyphens, strip, lowercase — for a tolerant but exact
    name comparison (case and spacing don't matter, wording does)."""
    return re.sub(r"[\s\-]+", " ", name or "").strip().lower()


def _is_bear_fence_exact(name: str) -> bool:
    """The hard gate: the task's name must BE 'Disarm Bear Fence' — nothing
    more (no trailing date, no extra words). This is what guarantees we never
    delete a Walk Thru, Hot Tub, cleaning, or any other task."""
    return _norm_name(name) == "disarm bear fence"


def _task_time(t: dict) -> str:
    return str(t.get("scheduled_time") or "")[:5]


def _task_date(t: dict) -> str:
    return str(t.get("scheduled_date") or "")[:10]


def _task_dept(t: dict) -> str:
    dept = t.get("type_department")
    if isinstance(dept, dict):
        dept = dept.get("name") or dept.get("code")
    return dept or ""


def _task_status(t: dict) -> str:
    for key in ("type_task_status", "status", "state"):
        v = t.get(key)
        if isinstance(v, str) and v:
            return v
        if isinstance(v, dict):
            s = v.get("name") or v.get("value") or v.get("label") or v.get("code")
            if s:
                return str(s)
    return ""


def _pid_of(t: dict) -> str:
    return str(t.get("property_id") or t.get("home_id") or "")


def _summarize(t: dict) -> dict:
    return {
        "name":       _task_title(t),
        "time":       _task_time(t),
        "assignees":  _assignee_names(t),
        "status":     _task_status(t),
        "department": _task_dept(t),
    }


def _delete_task(token: str, task_id) -> tuple[bool, str]:
    headers = {"Authorization": f"JWT {token}"}
    url = f"{BW_BASE}/public/inventory/v1/task/{task_id}"
    try:
        r = requests.delete(url, headers=headers, timeout=15)
        ok = r.status_code in (200, 202, 204)
        detail = f"status={r.status_code}"
        if not ok:
            detail += f" body={r.text[:200]}"
        return ok, detail
    except Exception as e:
        return False, str(e)


@bear_fence_delete_bp.route("/admin/bear-fence-delete")
@login_required
@admin_required
def bear_fence_delete_page():
    return render_template("bear_fence_delete.html")


@bear_fence_delete_bp.route("/admin/bear-fence-delete/scan", methods=["POST"])
@login_required
@admin_required
def bear_fence_delete_scan():
    token = _get_token()
    if not token:
        return jsonify({"error": "Breezeway not configured."}), 500

    body = request.get_json(silent=True) or {}
    try:
        day = date.fromisoformat(body["day"]) if body.get("day") else date.today()
    except ValueError:
        day = date.today()

    ck     = day.isoformat()
    force  = bool(body.get("force"))
    cached = _scan_cache.get(ck)
    if cached and not force and _time.time() - cached[0] < _SCAN_TTL:
        return jsonify(cached[1])

    # Candidate properties = those with an arrival on/around the chosen day.
    w_start = day - timedelta(days=CANDIDATE_WINDOW_DAYS)
    w_end   = day + timedelta(days=CANDIDATE_WINDOW_DAYS)
    reservations = _fetch_reservations_range(token, w_start, w_end)
    pids = list({
        str(r.get("property_id") or r.get("home_id") or "")
        for r in reservations
        if r.get("checkin_date")
    } - {""})

    tasks = _fetch_tasks_for_pids(token, pids, day, day) if pids else []

    # Keep only tasks actually scheduled on the chosen day, grouped by property.
    day_iso = day.isoformat()
    by_pid: dict[str, list[dict]] = {}
    for t in tasks:
        if _task_date(t) != day_iso:
            continue
        by_pid.setdefault(_pid_of(t), []).append(t)

    items = []
    for pid, prop_tasks in by_pid.items():
        prop_name = _get_property_name(pid)
        for t in prop_tasks:
            if not _is_bear_fence_exact(_task_title(t)):
                continue
            tid = t.get("id")
            others = [
                _summarize(o) for o in prop_tasks
                if o.get("id") != tid
            ]
            others.sort(key=lambda x: x["time"] or "~")
            items.append({
                "task_id":     tid,
                "property":    prop_name,
                "property_id": pid,
                "name":        _task_title(t),
                "date":        _task_date(t),
                "time":        _task_time(t),
                "assignees":   _assignee_names(t),
                "status":      _task_status(t),
                "others":      others,
            })

    items.sort(key=lambda x: (x["property"], x["time"] or "~"))
    result = {"day": day_iso, "items": items}
    _scan_cache[ck] = (_time.time(), result)
    return jsonify(result)


@bear_fence_delete_bp.route("/admin/bear-fence-delete/delete", methods=["POST"])
@login_required
@admin_required
def bear_fence_delete_apply():
    token = _get_token()
    if not token:
        return jsonify({"error": "Breezeway not configured."}), 500

    body    = request.get_json(silent=True) or {}
    if str(body.get("confirm", "")).strip().upper() != "DELETE":
        return jsonify({"error": "Deletion not confirmed. Type DELETE to confirm."}), 400

    items   = body.get("items", [])
    results = []
    for item in items:
        task_id       = item.get("task_id")
        prop          = item.get("property", "")
        expected_date = str(item.get("date") or "")[:10]

        # Re-read the LIVE task. Everything below gates on this fresh record, not
        # on anything the client sent — that's the structural guarantee.
        live = _fetch_task_by_id(token, task_id)
        if live is None:
            results.append({
                "task_id": task_id, "property": prop, "name": item.get("name", ""),
                "success": False,
                "detail": "⚠ skipped — couldn't re-read the task to verify it. Rescan and try again.",
            })
            continue

        live_name = _task_title(live)

        # HARD CONSTRAINT: only ever delete a task whose live name is exactly
        # "Disarm Bear Fence". Anything else is refused, loudly.
        if not _is_bear_fence_exact(live_name):
            results.append({
                "task_id": task_id, "property": prop, "name": live_name,
                "success": False,
                "detail": f"🛑 REFUSED — this task is named “{live_name or '(no name)'}”, "
                          f"not “Disarm Bear Fence”. Not deleted.",
            })
            continue

        # Staleness guard: if the task moved to a different day since the scan,
        # skip it — the admin reviewed it on a day it's no longer on.
        live_date = _task_date(live)
        if expected_date and live_date and live_date != expected_date:
            results.append({
                "task_id": task_id, "property": prop, "name": live_name,
                "success": False,
                "detail": f"⚠ skipped — task moved to {live_date} (you reviewed {expected_date}). "
                          f"Rescan to see the real dates.",
            })
            continue

        ok, msg = _delete_task(token, task_id)
        results.append({
            "task_id": task_id, "property": prop, "name": live_name,
            "success": ok,
            "detail":  ("deleted · " + msg) if ok else ("delete FAILED · " + msg),
        })

    _scan_cache.clear()   # tasks removed — next scan should be fresh
    return jsonify({"results": results})
