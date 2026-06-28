"""
routes/assignee_monitor.py — Off-list assignee monitor.

A passive safety scan: for ONE selected day, sweep every Breezeway task and flag
any task assigned to someone who is NOT on the group-batcher allow-list — no matter
who/what made the assignment (the Task API itself, another tool, a person). This
catches the original "Derek bug", where Breezeway assigned a task to someone who
can never validly be assigned, even though it didn't come through our batcher.

Scope: cleaning/housekeeping and vendor departments are EXCLUDED (they use rosters
other than the maintenance allow-list). The excluded set is an editable keyword
list, seeded with cleaning/housekeeping; add vendor departments from the page using
the department breakdown it shows.

Endpoints (admin only):
  GET  /admin/assignee-monitor                  — page
  POST /admin/assignee-monitor/scan             — scan one day (JSON)
  GET  /admin/assignee-monitor/ignored-depts    — current ignore keywords
  POST /admin/assignee-monitor/ignored-depts/add
  POST /admin/assignee-monitor/ignored-depts/remove
"""

import time
from datetime import date, datetime
from concurrent.futures import ThreadPoolExecutor

from flask import Blueprint, render_template, jsonify, request
from flask_login import login_required, current_user

from routes.auth import admin_required

assignee_monitor_bp = Blueprint("assignee_monitor", __name__)

# Cache the heavy per-property task sweep briefly so a proxy timeout retry is
# instant and ignore-list / allow-list edits re-filter without re-sweeping.
_sweep_cache: dict = {}   # date_str -> (ts, all_tasks)
_SWEEP_TTL         = 60


def _ignored_keywords() -> list:
    from db import get_db, get_cursor
    conn = get_db(); cur = get_cursor(conn)
    try:
        cur.execute("SELECT keyword FROM assignee_monitor_ignored_depts ORDER BY keyword")
        return [r["keyword"] for r in cur.fetchall()]
    finally:
        cur.close(); conn.rollback(); conn.close()


def _dept_of(task: dict) -> str:
    dept = task.get("type_department")
    if isinstance(dept, dict):
        dept = dept.get("code") or dept.get("name") or ""
    return str(dept).strip().lower()


def _sweep_tasks(token, date_str: str):
    """Fetch every active property's tasks for the date. Returns (all_tasks, failed,
    scanned). Mirrors the batcher's retry/backoff so a throttled property is reported,
    never silently dropped."""
    from routes.briefing import (_fetch_bw_endpoint, _ensure_property_cache,
                                 _get_live_property_cache, _get_live_ref_cache)

    cached = _sweep_cache.get(date_str)
    if cached and time.time() - cached[0] < _SWEEP_TTL:
        all_tasks, failed, scanned = cached[1]
        return all_tasks, failed, scanned

    _ensure_property_cache()
    prop_cache = _get_live_property_cache()
    ref_cache  = _get_live_ref_cache()
    if not prop_cache:
        return None, 0, 0  # signal: cache empty

    pid_candidates = {}
    for bw_pid in prop_cache:
        ref_id = ref_cache.get(bw_pid)
        pid_candidates.setdefault(ref_id if ref_id else str(bw_pid), bw_pid)

    def _tasks_for_ref(ref_id):
        for attempt in range(3):
            r, _, status = _fetch_bw_endpoint(
                token, "/public/inventory/v1/task",
                {"reference_property_id": ref_id, "scheduled_date": f"{date_str},{date_str}"})
            if status == 200:
                return (r or [], True)
            if status is None or status == 429 or status >= 500:
                time.sleep(0.3 * (attempt + 1))
                continue
            r2, _, st2 = _fetch_bw_endpoint(
                token, "/public/inventory/v1/task",
                {"reference_property_id": ref_id, "start_date": date_str, "end_date": date_str})
            return (r2 or [], True) if st2 == 200 else ([], False)
        return ([], False)

    all_tasks, failed = [], 0
    with ThreadPoolExecutor(max_workers=16) as ex:
        for tasks, ok in ex.map(_tasks_for_ref, list(pid_candidates.keys())):
            all_tasks.extend(tasks)
            if not ok:
                failed += 1

    scanned = len(pid_candidates)
    _sweep_cache[date_str] = (time.time(), (all_tasks, failed, scanned))
    return all_tasks, failed, scanned


@assignee_monitor_bp.route("/admin/assignee-monitor")
@login_required
@admin_required
def assignee_monitor_page():
    return render_template("assignee_monitor.html")


@assignee_monitor_bp.route("/admin/assignee-monitor/scan", methods=["POST"])
@login_required
@admin_required
def assignee_monitor_scan():
    from routes.briefing import _get_breezeway_token, _get_property_name
    from routes.dispatch import _bw_task_title
    from routes.group_assign import _assignee_names, _is_candidate, _candidate_keys

    token = _get_breezeway_token()
    if not token:
        return jsonify({"error": "Breezeway not configured."}), 500

    payload  = request.get_json(silent=True) or {}
    date_str = (payload.get("date") or date.today().isoformat())[:10]

    all_tasks, failed, scanned = _sweep_tasks(token, date_str)
    if all_tasks is None:
        return jsonify({"error": "Breezeway property cache is empty — try again in a moment."}), 502

    ignored   = [k.lower() for k in _ignored_keywords()]
    cand_keys = _candidate_keys()

    violations  = []
    dept_counts = {}        # department -> {checked, excluded} so she can tune the ignore list
    seen        = set()
    checked     = 0
    for t in all_tasks:
        tid = t.get("id")
        if tid in seen:
            continue
        seen.add(tid)
        if (t.get("scheduled_date") or "")[:10] != date_str:
            continue  # strict date guard — BW sometimes returns off-date tasks

        dl = _dept_of(t)
        label = dl or "(none)"
        bucket = dept_counts.setdefault(label, {"checked": 0, "excluded": 0})
        if any(kw in dl for kw in ignored):
            bucket["excluded"] += 1
            continue
        bucket["checked"] += 1
        checked += 1

        names   = _assignee_names(t)
        offlist = [n for n in names if not _is_candidate(n, cand_keys)]
        if not offlist:
            continue
        home_id = t.get("home_id") or t.get("property_id")
        violations.append({
            "task_id":    tid,
            "task":       _bw_task_title(t),
            "property":   _get_property_name(home_id),
            "date":       date_str,
            "time":       (str(t.get("scheduled_time") or "")[:5]) or None,
            "department": dl or "—",
            "assignees":  names,
            "offlist":    offlist,
        })

    violations.sort(key=lambda v: ((v["property"] or "").lower(), (v["task"] or "").lower()))
    return jsonify({
        "date":           date_str,
        "violations":     violations,
        "checked_tasks":  checked,
        "total_tasks":    len(seen),
        "dept_counts":    dept_counts,
        "ignored":        _ignored_keywords(),
        "failed_properties":  failed,
        "scanned_properties": scanned,
    })


# ── Ignored-department keyword management ─────────────────────────

@assignee_monitor_bp.route("/admin/assignee-monitor/ignored-depts", methods=["GET"])
@login_required
@admin_required
def ignored_depts_get():
    return jsonify({"ignored": _ignored_keywords()})


@assignee_monitor_bp.route("/admin/assignee-monitor/ignored-depts/add", methods=["POST"])
@login_required
@admin_required
def ignored_depts_add():
    from db import get_db, get_cursor
    kw = ((request.get_json(silent=True) or {}).get("keyword") or "").strip().lower()
    if not kw:
        return jsonify({"error": "Enter a department keyword."}), 400
    conn = get_db(); cur = get_cursor(conn)
    try:
        cur.execute(
            "INSERT INTO assignee_monitor_ignored_depts (keyword, created_at, created_by) "
            "VALUES (%s, %s, %s) ON CONFLICT (keyword) DO NOTHING",
            (kw, datetime.utcnow().isoformat(), current_user.id),
        )
        conn.commit()
    finally:
        cur.close(); conn.close()
    return jsonify({"ok": True, "ignored": _ignored_keywords()})


@assignee_monitor_bp.route("/admin/assignee-monitor/ignored-depts/remove", methods=["POST"])
@login_required
@admin_required
def ignored_depts_remove():
    from db import get_db, get_cursor
    kw = ((request.get_json(silent=True) or {}).get("keyword") or "").strip().lower()
    if not kw:
        return jsonify({"error": "Keyword required."}), 400
    conn = get_db(); cur = get_cursor(conn)
    try:
        cur.execute("DELETE FROM assignee_monitor_ignored_depts WHERE keyword = %s", (kw,))
        conn.commit()
    finally:
        cur.close(); conn.close()
    return jsonify({"ok": True, "ignored": _ignored_keywords()})
