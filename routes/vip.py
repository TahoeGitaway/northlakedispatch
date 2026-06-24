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

from datetime import datetime, date as _date, timedelta as _td

from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required, current_user

from db import get_db, get_cursor

vip_bp = Blueprint("vip", __name__)

# These reservations are all 2026 (the tracker's window).
_VIP_YEAR = 2026


def _match_room_to_pid(room, prop_cache):
    """Match a reservation room name to a Breezeway property id (cache: {pid: name})."""
    import difflib
    key = (room or "").lower().strip()
    if not key:
        return None
    by_name = {(name or "").lower().strip(): pid for pid, name in prop_cache.items()}
    if key in by_name:
        return by_name[key]
    pk = " " + key + " "                       # word-aligned containment
    for nm, pid in by_name.items():
        pnm = " " + nm + " "
        if pk in pnm or pnm in pk:
            return pid
    hits = difflib.get_close_matches(key, list(by_name.keys()), n=1, cutoff=0.8)
    return by_name[hits[0]] if hits else None


def _task_status(t):
    raw = t.get("type_task_status")
    if isinstance(raw, dict):
        raw = raw.get("name") or raw.get("code") or ""
    raw = str(raw or t.get("status") or "").lower()
    if raw in ("complete", "completed", "done", "finished", "approved"):
        return "✓ Complete"
    if raw in ("in_progress", "in progress", "started"):
        return "🔄 In progress"
    return "⏳ Pending"


def _assignees(t):
    out = []
    for a in (t.get("assignments") or []):
        n = (a.get("name") or a.get("full_name") or
             f"{a.get('first_name','').strip()} {a.get('last_name','').strip()}".strip())
        if n:
            out.append(n)
    return out


def _guest_name(r):
    g = (r.get("guest_name") or r.get("primary_guest") or r.get("name") or "")
    if isinstance(g, dict):
        return str(g.get("name") or
                   f"{g.get('first_name','').strip()} {g.get('last_name','').strip()}".strip())
    return str(g)


@vip_bp.route("/vip/house-tasks")
@login_required
def vip_house_tasks():
    """Tasks at one house for the 7 days leading up to a VIP check-in."""
    from routes.briefing import (_get_breezeway_token, _ensure_property_cache,
                                 _get_live_property_cache, _get_live_ref_cache,
                                 _fetch_bw_endpoint)
    from routes.dispatch import _bw_task_title

    room = (request.args.get("room") or "").strip()
    ci   = (request.args.get("ci") or "").strip()
    if not room or not ci:
        return jsonify({"error": "room and ci required"}), 400
    try:
        mm, dd = ci.split("/")[:2]
        ci_date = _date(_VIP_YEAR, int(mm), int(dd))
    except Exception:
        return jsonify({"error": "bad ci date"}), 400
    start = ci_date - _td(days=7)

    token = _get_breezeway_token()
    if not token:
        return jsonify({"error": "Breezeway not configured."}), 503
    _ensure_property_cache()
    prop_cache = _get_live_property_cache()
    ref_cache  = _get_live_ref_cache()

    bw_pid = _match_room_to_pid(room, prop_cache)
    if not bw_pid:
        return jsonify({"matched": False, "room": room,
                        "start": start.isoformat(), "ci": ci_date.isoformat()})

    ref_id = ref_cache.get(bw_pid) or str(bw_pid)
    drange = f"{start.isoformat()},{ci_date.isoformat()}"
    results, _err, _st = _fetch_bw_endpoint(
        token, "/public/inventory/v1/task",
        {"reference_property_id": ref_id, "scheduled_date": drange})

    tasks = []
    for t in (results or []):
        td = (t.get("scheduled_date") or "")[:10]
        if not (start.isoformat() <= td <= ci_date.isoformat()):
            continue
        dept = t.get("type_department")
        if isinstance(dept, dict):
            dept = dept.get("name") or dept.get("code")
        tasks.append({
            "name":       _bw_task_title(t),
            "date":       td,
            "time":       (str(t.get("scheduled_time") or "")[:5]) or None,
            "status":     _task_status(t),
            "assignees":  _assignees(t),
            "department": dept,
        })
    tasks.sort(key=lambda x: (x["date"], x["time"] or "~"))

    # Reservations overlapping the week-before window — who is at the house, and what type.
    from routes.briefing import _classify_reservation
    res_results, _re2, _rs2 = _fetch_bw_endpoint(
        token, "/public/inventory/v1/reservation",
        {"reference_property_id": ref_id,
         "checkout_date_ge": start.isoformat(),
         "checkin_date_le":  ci_date.isoformat()})
    reservations = []
    for r in (res_results or []):
        rpid = str(r.get("property_id") or r.get("home_id") or "")
        if rpid and rpid != str(bw_pid):
            continue                                   # endpoint may ignore the property filter
        cin  = (r.get("checkin_date") or "")[:10]
        cout = (r.get("checkout_date") or "")[:10]
        if not cin or not cout or cout < start.isoformat() or cin > ci_date.isoformat():
            continue
        reservations.append({
            "type":     _classify_reservation(r),
            "checkin":  cin,
            "checkout": cout,
            "guest":    _guest_name(r),
        })
    reservations.sort(key=lambda x: x["checkin"])

    return jsonify({"matched": True, "matched_name": prop_cache.get(bw_pid),
                    "start": start.isoformat(), "ci": ci_date.isoformat(),
                    "reservations": reservations,
                    "tasks": tasks})


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
    """Save just the 'inspected' checkbox (notes live in vip_comments now)."""
    body = request.get_json(silent=True) or {}
    key  = (body.get("item_key") or "").strip()
    if not key:
        return jsonify({"error": "item_key required"}), 400
    done = 1 if body.get("done") else 0
    now  = datetime.utcnow().isoformat()
    uid  = getattr(current_user, "id", None)

    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        """INSERT INTO vip_tracker (item_key, done, updated_at, updated_by)
           VALUES (%s, %s, %s, %s)
           ON CONFLICT (item_key) DO UPDATE SET
               done = EXCLUDED.done,
               updated_at = EXCLUDED.updated_at, updated_by = EXCLUDED.updated_by""",
        (key, done, now, uid),
    )
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"ok": True, "updated_at": now})


@vip_bp.route("/vip/comments")
@login_required
def vip_comments():
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("SELECT id, item_key, author, author_id, body, created_at FROM vip_comments ORDER BY created_at ASC")
    rows = cur.fetchall()
    # Legacy single-field notes (from before threaded comments) — surface them so
    # nothing that was ever written is lost. (Read-only; no id.)
    cur.execute("SELECT item_key, notes, updated_at FROM vip_tracker WHERE COALESCE(notes, '') <> ''")
    legacy = cur.fetchall()
    cur.close(); conn.close()

    out = {}
    for lr in legacy:                       # show earlier notes first
        out.setdefault(lr["item_key"], []).append({
            "id": None, "author": "Earlier note", "author_id": None,
            "body": lr["notes"], "created_at": lr["updated_at"],
        })
    for r in rows:
        out.setdefault(r["item_key"], []).append({
            "id": r["id"], "author": r["author"] or "?", "author_id": r["author_id"],
            "body": r["body"], "created_at": r["created_at"],
        })
    return jsonify({"comments": out})


@vip_bp.route("/vip/comment", methods=["POST"])
@login_required
def vip_comment():
    body = request.get_json(silent=True) or {}
    key  = (body.get("item_key") or "").strip()
    text = str(body.get("body") or "").strip()
    if not key or not text:
        return jsonify({"error": "item_key and body required"}), 400
    now    = datetime.utcnow().isoformat()
    author = (getattr(current_user, "name", None) or getattr(current_user, "email", None) or "Someone")
    uid    = getattr(current_user, "id", None)

    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        "INSERT INTO vip_comments (item_key, author_id, author, body, created_at) "
        "VALUES (%s, %s, %s, %s, %s) RETURNING id",
        (key, uid, author, text, now),
    )
    new_id = cur.fetchone()["id"]
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"ok": True, "comment": {
        "id": new_id, "author": author, "author_id": uid, "body": text, "created_at": now}})


@vip_bp.route("/vip/comment/edit", methods=["POST"])
@login_required
def vip_comment_edit():
    """Edit one of YOUR OWN comments (author-scoped — can't touch others')."""
    body = request.get_json(silent=True) or {}
    cid  = body.get("id")
    text = str(body.get("body") or "").strip()
    if not cid or not text:
        return jsonify({"error": "id and body required"}), 400
    uid = getattr(current_user, "id", None)

    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        "UPDATE vip_comments SET body = %s WHERE id = %s AND author_id = %s",
        (text, cid, uid),
    )
    changed = cur.rowcount
    conn.commit()
    cur.close(); conn.close()
    if not changed:
        return jsonify({"error": "Not found, or not your note."}), 403
    return jsonify({"ok": True, "body": text})


@vip_bp.route("/vip/comment/delete", methods=["POST"])
@login_required
def vip_comment_delete():
    """Delete one of YOUR OWN comments (author-scoped)."""
    body = request.get_json(silent=True) or {}
    cid  = body.get("id")
    if not cid:
        return jsonify({"error": "id required"}), 400
    uid = getattr(current_user, "id", None)

    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        "DELETE FROM vip_comments WHERE id = %s AND author_id = %s",
        (cid, uid),
    )
    changed = cur.rowcount
    conn.commit()
    cur.close(); conn.close()
    if not changed:
        return jsonify({"error": "Not found, or not your note."}), 403
    return jsonify({"ok": True})
