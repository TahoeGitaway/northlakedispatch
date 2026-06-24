"""
routes/vip.py — VIP reservation tracker (TEMPORARY, ~one month).

A standalone checklist + per-reservation notes for the special VIP arrivals.
State persists in the vip_tracker table (shared across the team) until the page
is deleted. To remove the whole feature later: delete this file + templates/vip.html,
drop its blueprint from app.py, and (optionally) DROP TABLE vip_tracker.

Endpoints:
  GET  /vip        — page
  GET  /vip/state  — saved {item_key: {done, notes, updated_at}}
  POST /vip/save   — upsert one reservation's {done, notes}
"""

from datetime import datetime

from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required, current_user

from db import get_db, get_cursor

vip_bp = Blueprint("vip", __name__)


@vip_bp.route("/vip")
@login_required
def vip_page():
    return render_template("vip.html")


@vip_bp.route("/vip/state")
@login_required
def vip_state():
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("SELECT item_key, done, notes, updated_at FROM vip_tracker")
    rows = cur.fetchall()
    cur.close(); conn.close()
    state = {
        r["item_key"]: {
            "done":       bool(r["done"]),
            "notes":      r["notes"] or "",
            "updated_at": r["updated_at"],
        }
        for r in rows
    }
    return jsonify({"state": state})


@vip_bp.route("/vip/save", methods=["POST"])
@login_required
def vip_save():
    body = request.get_json(silent=True) or {}
    key  = (body.get("item_key") or "").strip()
    if not key:
        return jsonify({"error": "item_key required"}), 400
    done  = 1 if body.get("done") else 0
    notes = str(body.get("notes") or "")
    now   = datetime.utcnow().isoformat()
    uid   = getattr(current_user, "id", None)

    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        """INSERT INTO vip_tracker (item_key, done, notes, updated_at, updated_by)
           VALUES (%s, %s, %s, %s, %s)
           ON CONFLICT (item_key) DO UPDATE SET
               done = EXCLUDED.done, notes = EXCLUDED.notes,
               updated_at = EXCLUDED.updated_at, updated_by = EXCLUDED.updated_by""",
        (key, done, notes, now, uid),
    )
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"ok": True, "updated_at": now})
