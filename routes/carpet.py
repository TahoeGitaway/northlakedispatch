"""
routes/carpet.py — carpet cleaning log routes.
"""

from datetime import datetime

from flask import (Blueprint, render_template, request, jsonify)
from flask_login import login_required, current_user

from db import get_db, get_cursor, CARPET_CLEANERS

carpet_bp = Blueprint("carpet", __name__)


@carpet_bp.route("/carpet-log")
@login_required
def carpet_log():
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("""
        SELECT cl.id, cl.log_date, cl.cleaner_name, cl.cleaner_name_2,
               cl.property_name, cl.notes, cl.rescheduled, cl.created_at,
               u.name AS logged_by_name
        FROM carpet_log cl
        JOIN users u ON cl.logged_by = u.id
        ORDER BY cl.log_date DESC, cl.cleaner_name ASC
    """)
    entries = cur.fetchall()

    cur.execute("""
        SELECT "Property Name" FROM properties
        WHERE "Property Name" IS NOT NULL
        ORDER BY "Property Name" ASC
    """)
    properties = [r["Property Name"] for r in cur.fetchall()]

    cur.close(); conn.close()
    today = datetime.utcnow().strftime("%Y-%m-%d")
    return render_template("carpet_log.html",
        entries=entries,
        cleaners=CARPET_CLEANERS,
        properties=properties,
        now_date=today
    )


@carpet_bp.route("/carpet-log/add", methods=["POST"])
@login_required
def carpet_log_add():
    data           = request.json or {}
    log_date       = (data.get("log_date") or "").strip()
    cleaner_name   = (data.get("cleaner_name") or "").strip()
    cleaner_name_2 = (data.get("cleaner_name_2") or "").strip() or None
    property_name  = (data.get("property_name") or "").strip() or None
    notes          = (data.get("notes") or "").strip()

    if not log_date or not cleaner_name:
        return jsonify({"error": "Date and cleaner name are required"}), 400
    if cleaner_name not in CARPET_CLEANERS:
        return jsonify({"error": "Invalid cleaner name"}), 400
    if cleaner_name_2 and cleaner_name_2 not in CARPET_CLEANERS:
        return jsonify({"error": "Invalid second cleaner name"}), 400
    if cleaner_name_2 == cleaner_name:
        cleaner_name_2 = None

    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("""
        INSERT INTO carpet_log
            (log_date, cleaner_name, cleaner_name_2, property_name, notes, rescheduled, logged_by, created_at)
        VALUES (%s, %s, %s, %s, %s, 0, %s, %s) RETURNING id
    """, (log_date, cleaner_name, cleaner_name_2, property_name, notes or None,
          current_user.id, datetime.utcnow().isoformat()))
    new_id = cur.fetchone()["id"]
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"success": True, "id": new_id})


@carpet_bp.route("/carpet-log/<int:entry_id>/update", methods=["POST"])
@login_required
def carpet_log_update(entry_id):
    data           = request.json or {}
    log_date       = (data.get("log_date") or "").strip()
    cleaner_name   = (data.get("cleaner_name") or "").strip()
    cleaner_name_2 = (data.get("cleaner_name_2") or "").strip() or None
    property_name  = (data.get("property_name") or "").strip() or None
    notes          = (data.get("notes") or "").strip()
    rescheduled    = int(bool(data.get("rescheduled", False)))

    if not log_date or not cleaner_name:
        return jsonify({"error": "Date and cleaner name are required"}), 400
    if cleaner_name not in CARPET_CLEANERS:
        return jsonify({"error": "Invalid cleaner name"}), 400
    if cleaner_name_2 and cleaner_name_2 not in CARPET_CLEANERS:
        return jsonify({"error": "Invalid second cleaner name"}), 400
    if cleaner_name_2 == cleaner_name:
        cleaner_name_2 = None

    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("SELECT logged_by FROM carpet_log WHERE id = %s", (entry_id,))
    row = cur.fetchone()

    if not row:
        cur.close(); conn.close()
        return jsonify({"error": "Entry not found"}), 404
    if not current_user.is_admin and row["logged_by"] != current_user.id:
        cur.close(); conn.close()
        return jsonify({"error": "Not authorized"}), 403

    cur.execute("""
        UPDATE carpet_log
        SET log_date=%s, cleaner_name=%s, cleaner_name_2=%s,
            property_name=%s, notes=%s, rescheduled=%s
        WHERE id=%s
    """, (log_date, cleaner_name, cleaner_name_2,
          property_name, notes or None, rescheduled, entry_id))
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"success": True, "log_date": log_date})


@carpet_bp.route("/carpet-log/<int:entry_id>/reschedule", methods=["POST"])
@login_required
def carpet_log_reschedule(entry_id):
    """Mark a clean as rescheduled — removes cleaner assignment, keeps property."""
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("SELECT logged_by FROM carpet_log WHERE id = %s", (entry_id,))
    row = cur.fetchone()

    if not row:
        cur.close(); conn.close()
        return jsonify({"error": "Entry not found"}), 404
    if not current_user.is_admin and row["logged_by"] != current_user.id:
        cur.close(); conn.close()
        return jsonify({"error": "Not authorized"}), 403

    cur.execute("""
        UPDATE carpet_log
        SET rescheduled=1, cleaner_name='Unassigned', cleaner_name_2=NULL
        WHERE id=%s
    """, (entry_id,))
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"success": True})


@carpet_bp.route("/carpet-log/<int:entry_id>/delete", methods=["POST"])
@login_required
def carpet_log_delete(entry_id):
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("SELECT logged_by FROM carpet_log WHERE id = %s", (entry_id,))
    row = cur.fetchone()

    if not row:
        cur.close(); conn.close()
        return jsonify({"error": "Entry not found"}), 404
    if not current_user.is_admin and row["logged_by"] != current_user.id:
        cur.close(); conn.close()
        return jsonify({"error": "Not authorized"}), 403

    cur.execute("DELETE FROM carpet_log WHERE id = %s", (entry_id,))
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"success": True})