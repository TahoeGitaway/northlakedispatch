"""
routes/bw_audit.py — record every write this app makes to Breezeway.

Why this exists
---------------
Breezeway's task history / audit / activity / events endpoints all return 404
(see the probe in dispatch.py), so nothing upstream records who changed what.
This app has fourteen separate code paths that PATCH or PUT Breezeway data, and
until now none of them left a trace.

That became concrete when a time sync rewrote a second cleaner's task times on
the route's date, and a housekeeper's on a different date entirely. There was no
way to see what had been changed, and no record of the original values to put
back.

Every attempt is logged, not just successes: a failed write against someone
else's task is exactly the thing you want to find, and a write that errored
halfway is exactly when you need to know what was touched.

Design notes
------------
`old_value` is the point. It is the only record of what a task looked like before
the app changed it, which is what turns this from a diary into something you can
repair from.

log_bw_write() must NEVER raise. A logging failure has to be invisible to the
operation being logged — nobody should lose a task assignment because an audit
insert hit a closed connection.
"""

from datetime import datetime, timezone

from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required

from routes.auth import admin_required

bw_audit_bp = Blueprint("bw_audit", __name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _who() -> tuple:
    """(user_id, user_name) for the request in flight, or (None, None) for a
    scheduled job with no logged-in user."""
    try:
        from flask_login import current_user
        if getattr(current_user, "is_authenticated", False):
            return (getattr(current_user, "id", None),
                    getattr(current_user, "name", None) or getattr(current_user, "email", None))
    except Exception:
        pass
    return (None, None)


def _s(v, limit: int = 500) -> str:
    """Render any value as short text. Lists of assignee names become 'A, B'."""
    if v is None:
        return ""
    if isinstance(v, (list, tuple)):
        v = ", ".join(str(x) for x in v if x is not None)
    elif isinstance(v, dict):
        v = str(v)
    return str(v)[:limit]


def log_bw_write(feature: str, field: str, *, task_id=None, task_name=None,
                 property_name=None, task_date=None, old_value=None,
                 new_value=None, ok: bool = False, detail: str = "") -> None:
    """Record one attempted write. Never raises."""
    try:
        from db import get_db, get_cursor
        uid, uname = _who()
        conn = get_db(); cur = get_cursor(conn)
        try:
            cur.execute("""
                INSERT INTO bw_write_log
                       (ts, user_id, user_name, feature, task_id, task_name,
                        property, task_date, field, old_value, new_value, ok, detail)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (_now_iso(), uid, _s(uname, 120), _s(feature, 40),
                  _s(task_id, 40), _s(task_name, 200), _s(property_name, 200),
                  _s(task_date, 20), _s(field, 40),
                  _s(old_value), _s(new_value), bool(ok), _s(detail, 400)))
            conn.commit()
        finally:
            cur.close(); conn.close()
    except Exception:
        # Deliberately silent. Losing a log line is acceptable; breaking the write
        # it was recording is not.
        pass


def recent_writes(limit: int = 200, feature: str = "", task_date: str = "",
                  only_failed: bool = False) -> list:
    """Most recent writes, newest first. Read-only; safe to call from a page."""
    from db import get_db, get_cursor
    where, args = [], []
    if feature:
        where.append("feature = %s"); args.append(feature)
    if task_date:
        where.append("task_date = %s"); args.append(task_date)
    if only_failed:
        where.append("ok = FALSE")
    sql = "SELECT * FROM bw_write_log"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id DESC LIMIT %s"
    args.append(max(1, min(1000, limit)))

    conn = get_db(); cur = get_cursor(conn)
    try:
        cur.execute(sql, tuple(args))
        return [dict(r) for r in cur.fetchall()]
    finally:
        cur.close(); conn.rollback(); conn.close()


@bw_audit_bp.route("/admin/task-changes")
@login_required
@admin_required
def task_changes_page():
    """Every change this app has made to Breezeway, newest first.

    Filters are deliberately the ones you reach for after something looks wrong:
    the day the TASK was on, which feature did it, and failures only."""
    rows = recent_writes(
        limit=int(request.args.get("limit") or 300),
        feature=(request.args.get("feature") or "").strip(),
        task_date=(request.args.get("task_date") or "").strip()[:10],
        only_failed=request.args.get("failed") == "1",
    )
    return render_template("task_changes.html", rows=rows,
                           f_feature=request.args.get("feature") or "",
                           f_date=request.args.get("task_date") or "",
                           f_failed=request.args.get("failed") == "1")


@bw_audit_bp.route("/admin/task-changes.json")
@login_required
@admin_required
def task_changes_json():
    return jsonify({"rows": recent_writes(
        limit=int(request.args.get("limit") or 300),
        feature=(request.args.get("feature") or "").strip(),
        task_date=(request.args.get("task_date") or "").strip()[:10],
        only_failed=request.args.get("failed") == "1",
    )})
