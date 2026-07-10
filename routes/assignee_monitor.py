"""
routes/assignee_monitor.py — Off-list assignee monitor.

A passive safety scan: for ONE selected day, sweep every Breezeway task and flag
any task assigned to someone the user has NOT ticked as "known" — no matter
who/what made the assignment (the Task API itself, another tool, a person). This
catches the original "off-list bug", where Breezeway assigned a task to someone who
can never validly be assigned, even though it didn't come through our batcher.

Model (rebuilt 2026-07-10): the page shows the full Breezeway roster as checkboxes.
Ticking a person marks them "known/expected" so their tasks never flag; anything
assigned to an UNticked person flags. Matching is EXACT on the Breezeway person id
(task assignment `assignee_id` vs. the ticked-person id set) — no name/fuzzy logic.
The ticked set lives in `assignee_monitor_ignored_people` and is fully independent
of the batcher's assignment_candidates allow-list (which guards task writes).

Scope: cleaning/housekeeping and vendor departments are still EXCLUDED at the
department level (they use rosters other than maintenance). That excluded keyword
set is unchanged — editable from the page's department breakdown.

Endpoints (admin only):
  GET  /admin/assignee-monitor                  — page
  POST /admin/assignee-monitor/scan             — scan one day (JSON)
  GET  /admin/assignee-monitor/people           — full roster + ticked state
  POST /admin/assignee-monitor/people/set        — tick/untick one person
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


def _ignored_people_map() -> dict:
    """{person_id(int): name} of people ticked as 'known' — their tasks never flag."""
    from db import get_db, get_cursor
    conn = get_db(); cur = get_cursor(conn)
    try:
        cur.execute("SELECT person_id, name FROM assignee_monitor_ignored_people")
        return {int(r["person_id"]): r["name"] for r in cur.fetchall()}
    finally:
        cur.close(); conn.rollback(); conn.close()


# Full-roster cache, kept SEPARATE from the batcher's people cache so we never
# leak hard-blocked names into the batcher's filtered roster (or vice-versa).
_people_all_cache: dict = {"ts": 0.0, "data": []}


def _fetch_all_people(token: str) -> list:
    """Full active Breezeway roster [{id, name}], INCLUDING anyone the batcher
    hard-blocks — the whole point of this monitor is to show them so their tasks
    flag while unticked. Mirrors group_assign._fetch_people but without the
    block filter, and uses its own cache."""
    import requests
    from routes.group_assign import BW_BASE, _results
    if _people_all_cache["data"] and time.time() - _people_all_cache["ts"] < 3600:
        return _people_all_cache["data"]
    people, page, ok = [], 1, False
    while page <= 10:
        try:
            r = requests.get(f"{BW_BASE}/public/inventory/v1/people",
                             headers={"Authorization": f"JWT {token}"},
                             params={"status": "active", "limit": 200, "page": page},
                             timeout=20)
        except Exception:
            break
        if not r.ok:
            break
        ok = True
        items = _results(r.json())
        for p in items:
            if not isinstance(p, dict):
                continue
            pid  = p.get("id")
            name = (p.get("name") or
                    f"{p.get('first_name','').strip()} {p.get('last_name','').strip()}".strip() or
                    p.get("email") or str(pid))
            if pid is not None:
                people.append({"id": pid, "name": name})
        if len(items) < 200:
            break
        page += 1
    people.sort(key=lambda x: x["name"].lower())
    if ok:   # never cache a transient failure as an empty roster
        _people_all_cache["ts"] = time.time()
        _people_all_cache["data"] = people
    return people


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

    token = _get_breezeway_token()
    if not token:
        return jsonify({"error": "Breezeway not configured."}), 500

    payload  = request.get_json(silent=True) or {}
    date_str = (payload.get("date") or date.today().isoformat())[:10]

    all_tasks, failed, scanned = _sweep_tasks(token, date_str)
    if all_tasks is None:
        return jsonify({"error": "Breezeway property cache is empty — try again in a moment."}), 502

    ignored     = [k.lower() for k in _ignored_keywords()]
    ignored_ids = set(_ignored_people_map())   # person ids ticked as "known"

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

        # Each assignment carries assignee_id (the Breezeway person id) + a name.
        # A person is "known" only if their EXACT id is ticked. A missing id can't
        # be matched to the ignore list, so it flags — surfacing it rather than
        # silently passing an unidentifiable assignee.
        assigns = []
        for a in (t.get("assignments") or []):
            nm = (a.get("name") or
                  f"{a.get('first_name','').strip()} {a.get('last_name','').strip()}".strip())
            try:
                pid = int(a.get("assignee_id"))
            except (TypeError, ValueError):
                pid = None   # unidentifiable assignee → can't be "known" → flags
            assigns.append((pid, nm))
        names   = [nm for _, nm in assigns if nm]
        offlist = [(nm or f"person #{pid}") for pid, nm in assigns
                   if pid not in ignored_ids]
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
        "known_count":    len(ignored_ids),
        "failed_properties":  failed,
        "scanned_properties": scanned,
    })


# ── Known-people ("tick to ignore") management ────────────────────

@assignee_monitor_bp.route("/admin/assignee-monitor/people", methods=["GET"])
@login_required
@admin_required
def assignee_monitor_people():
    """Full roster with a per-person `ignored` flag (True = ticked as known)."""
    from routes.briefing import _get_breezeway_token
    token = _get_breezeway_token()
    if not token:
        return jsonify({"error": "Breezeway not configured."}), 500

    roster    = _fetch_all_people(token)
    ticked    = _ignored_people_map()          # {id: name}
    known_ids = set(ticked)

    people = [{"id": p["id"], "name": p["name"], "ignored": p["id"] in known_ids}
              for p in roster]
    # Surface ticked people who have left the active roster, so they can still be
    # unticked instead of lingering invisibly in the DB (and silently ignored).
    roster_ids = {p["id"] for p in roster}
    for pid in known_ids - roster_ids:
        people.append({"id": pid, "name": ticked.get(pid) or f"person #{pid}",
                       "ignored": True, "inactive": True})
    people.sort(key=lambda x: (x["name"] or "").lower())
    return jsonify({"people": people, "known_count": len(known_ids)})


@assignee_monitor_bp.route("/admin/assignee-monitor/people/set", methods=["POST"])
@login_required
@admin_required
def assignee_monitor_people_set():
    from db import get_db, get_cursor
    body = request.get_json(silent=True) or {}
    try:
        pid = int(body.get("person_id"))
    except (TypeError, ValueError):
        return jsonify({"error": "A valid person_id is required."}), 400
    ignored = bool(body.get("ignored"))
    name    = (body.get("name") or "").strip() or None

    conn = get_db(); cur = get_cursor(conn)
    try:
        if ignored:
            cur.execute(
                "INSERT INTO assignee_monitor_ignored_people (person_id, name, created_at, created_by) "
                "VALUES (%s, %s, %s, %s) "
                "ON CONFLICT (person_id) DO UPDATE SET name = EXCLUDED.name",
                (pid, name, datetime.utcnow().isoformat(), current_user.id))
        else:
            cur.execute("DELETE FROM assignee_monitor_ignored_people WHERE person_id = %s", (pid,))
        conn.commit()
    finally:
        cur.close(); conn.close()
    return jsonify({"ok": True, "known_count": len(_ignored_people_map())})


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
