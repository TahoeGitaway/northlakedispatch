"""
routes/briefing.py — AI-powered daily operations briefing.

Pulls today's saved routes from the DB and check-ins from the Breezeway API,
then asks Claude to write a plain-English summary paragraph.

Results are cached in memory for 15 minutes per date so repeated page loads
don't burn API quota.
"""

import json
import os
import time
from datetime import datetime

import anthropic
import requests
from flask import Blueprint, jsonify, request
from flask_login import login_required, current_user

from db import get_db, get_cursor

briefing_bp = Blueprint("briefing", __name__)

ANTHROPIC_API_KEY       = os.environ.get("ANTHROPIC_API_KEY", "")
BREEZEWAY_CLIENT_ID     = os.environ.get("BREEZEWAY_CLIENT_ID", "")
BREEZEWAY_CLIENT_SECRET = os.environ.get("BREEZEWAY_CLIENT_SECRET", "")

CACHE_TTL = 15 * 60  # 15 minutes

# ── In-memory caches ──────────────────────────────────────────────
_briefing_cache: dict = {}          # {date_str: (timestamp, text)}
_bw_token:       dict = {"value": None, "expires_at": 0}


# ── Breezeway helpers ─────────────────────────────────────────────

def _get_breezeway_token() -> str | None:
    """Return a valid Breezeway JWT, fetching a new one only when stale."""
    if not BREEZEWAY_CLIENT_ID or not BREEZEWAY_CLIENT_SECRET:
        return None
    now = time.time()
    if _bw_token["value"] and now < _bw_token["expires_at"] - 60:
        return _bw_token["value"]
    try:
        resp = requests.post(
            "https://api.breezeway.io/public/auth/v1/",
            json={"client_id": BREEZEWAY_CLIENT_ID, "client_secret": BREEZEWAY_CLIENT_SECRET},
            timeout=10,
        )
        data = resp.json()
        token = data.get("access_token")
        if token:
            _bw_token["value"]      = token
            _bw_token["expires_at"] = now + 23 * 3600  # tokens live 24 h; refresh after 23 h
        return token
    except Exception:
        return None


def _fetch_breezeway_checkins(date_str: str) -> list:
    """Return today's Breezeway reservations, paginating through all pages."""
    token = _get_breezeway_token()
    if not token:
        return []
    all_results = []
    page = 1
    limit = 100
    try:
        while True:
            resp = requests.get(
                "https://api.breezeway.io/public/inventory/v1/reservation",
                headers={"Authorization": f"JWT {token}"},
                params={"checkin_date_ge": date_str, "checkin_date_le": date_str,
                        "limit": limit, "page": page},
                timeout=10,
            )
            data = resp.json()
            if isinstance(data, list):
                page_results = data
            else:
                page_results = data.get("results", data.get("data", [])) or []
            all_results.extend(page_results)
            if len(page_results) < limit:
                break  # last page
            page += 1
        return all_results
    except Exception:
        return all_results  # return whatever we managed to fetch


def _classify_checkin(reservation: dict) -> str:
    """Return 'owner stay' or 'guest arrival' based on Breezeway tags."""
    tags = [str(t).lower() for t in (reservation.get("tags") or [])]
    if any("owner" in t for t in tags):
        return "owner stay"
    return "guest arrival"


def _fmt_time(hhmm: str) -> str:
    """Convert 'HH:MM:SS' or 'HH:MM' to '3:00 PM'."""
    try:
        parts = hhmm.split(":")
        h, m  = int(parts[0]), int(parts[1])
        ampm  = "AM" if h < 12 else "PM"
        return f"{h % 12 or 12}:{m:02d} {ampm}"
    except Exception:
        return hhmm


# ── DB helpers ────────────────────────────────────────────────────

def _fetch_briefing_notes(date_str: str) -> str:
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("SELECT note_text FROM briefing_notes WHERE note_date = %s", (date_str,))
    row = cur.fetchone()
    cur.close(); conn.close()
    return (row["note_text"] or "").strip() if row else ""


def _fetch_todays_routes(date_str: str, team_id=None) -> list:
    conn = get_db()
    cur  = get_cursor(conn)
    if team_id:
        cur.execute(
            """SELECT r.id, r.name, r.assigned_to, r.stops_json, r.notes, u.name AS created_by_name
               FROM saved_routes r
               JOIN users u ON r.created_by = u.id
               WHERE r.route_date = %s AND r.team_id = %s
               ORDER BY r.updated_at DESC""",
            (date_str, team_id),
        )
    else:
        cur.execute(
            """SELECT r.id, r.name, r.assigned_to, r.stops_json, r.notes, u.name AS created_by_name
               FROM saved_routes r
               JOIN users u ON r.created_by = u.id
               WHERE r.route_date = %s
               ORDER BY r.updated_at DESC""",
            (date_str,),
        )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


# ── Claude briefing generator ─────────────────────────────────────

def _summarise_routes(routes: list) -> list:
    """Return a list of dicts the frontend uses to render the route list."""
    out = []
    for r in routes:
        stops    = [s for s in json.loads(r["stops_json"] or "[]") if not s.get("isLunch")]
        priority = sum(1 for s in stops if s.get("priority_checkin"))
        checkin  = sum(1 for s in stops if s.get("arrival") and not s.get("priority_checkin"))
        out.append({
            "id":          r["id"],
            "name":        r["name"],
            "assigned_to": r["assigned_to"] or "",
            "stops":       len(stops),
            "priority":    priority,
            "checkins":    checkin,
            "notes":       (r.get("notes") or "").strip(),
        })
    return out


def _build_prompt(date_str: str, routes: list, checkins: list, notes: str = "") -> str:
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    day_name = date_obj.strftime("%A, %B ") + str(date_obj.day)
    lines    = [f"Today is {day_name}.\n"]

    # Routes (plain text for the prompt — no markdown links needed)
    if routes:
        route_lines = []
        for r in routes:
            stops    = [s for s in json.loads(r["stops_json"] or "[]") if not s.get("isLunch")]
            n        = len(stops)
            priority = sum(1 for s in stops if s.get("priority_checkin"))
            checkin  = sum(1 for s in stops if s.get("arrival") and not s.get("priority_checkin"))

            line = f'- "{r["name"]}"'
            if r["assigned_to"]:
                line += f' (assigned to {r["assigned_to"]})'
            line += f": {n} stop{'s' if n != 1 else ''}"
            if priority:
                line += f", {priority} priority check-in{'s' if priority != 1 else ''} (must finish by noon)"
            if checkin:
                line += f", {checkin} regular check-in{'s' if checkin != 1 else ''}"
            if (r.get("notes") or "").strip():
                line += f'. Notes: {r["notes"].strip()}'
            route_lines.append(line)

        lines.append(f"Dispatch routes ({len(routes)} total):\n" + "\n".join(route_lines))
    else:
        lines.append("No dispatch routes are saved for today.")

    # Breezeway check-ins
    if checkins:
        bw_lines   = []
        owner_ct   = 0
        guest_ct   = 0
        for c in checkins:
            kind = _classify_checkin(c)
            if kind == "owner stay":
                owner_ct += 1
            else:
                guest_ct += 1

            guests = c.get("guests") or []
            guest_name = ""
            if guests:
                g = guests[0]
                guest_name = f"{g.get('first_name', '')} {g.get('last_name', '')}".strip()

            entry = f"- {kind}"
            if guest_name:
                entry += f": {guest_name}"
            checkin_time = c.get("checkin_time", "")
            if checkin_time:
                entry += f" checking in at {_fmt_time(checkin_time)}"
            checkout = c.get("checkout_date", "")
            if checkout:
                entry += f" (out {checkout})"
            bw_lines.append(entry)

        summary = []
        if guest_ct:
            summary.append(f"{guest_ct} guest arrival{'s' if guest_ct != 1 else ''}")
        if owner_ct:
            summary.append(f"{owner_ct} owner stay{'s' if owner_ct != 1 else ''}")
        lines.append(
            f"Breezeway check-ins today ({', '.join(summary)}):\n" + "\n".join(bw_lines)
        )
    elif _get_breezeway_token():
        lines.append("Breezeway shows no check-ins scheduled for today.")
    else:
        lines.append("(Breezeway data not available — credentials not yet configured.)")

    # Dispatcher notes
    if notes:
        lines.append(f"Additional notes from the dispatcher:\n{notes}")

    return "\n\n".join(lines)


def _generate_briefing(date_str: str, routes: list, checkins: list, notes: str = "") -> tuple[str | None, str | None]:
    """Returns (text, error_reason). One of the two will always be None."""
    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        return None, "ANTHROPIC_API_KEY is not set in the server environment."

    prompt = _build_prompt(date_str, routes, checkins, notes)
    try:
        client = anthropic.Anthropic(api_key=key)
        msg    = client.messages.create(
            model      = "claude-haiku-4-5-20251001",
            max_tokens = 150,
            system     = (
                "You are a concise operations briefer for a vacation rental cleaning company "
                "in Lake Tahoe. Write 1-2 sentences: mention how many routes are going out and "
                "name the technicians assigned. If Breezeway check-in data is available, "
                "briefly note the number of guest arrivals or owner stays. "
                "Use the actual day name (e.g. 'Thursday') — never use the word 'today'. "
                "Do NOT describe individual routes, repeat route notes, or count stops. "
                "Be direct. Do not start with a greeting."
            ),
            messages   = [{"role": "user", "content": prompt}],
        )
        return msg.content[0].text, None
    except Exception as e:
        import flask
        flask.current_app.logger.error(f"Briefing generation failed: {type(e).__name__}: {e}")
        return None, f"{type(e).__name__}: {e}"


# ── Endpoints ─────────────────────────────────────────────────────

@briefing_bp.route("/briefing/notes", methods=["GET"])
@login_required
def get_briefing_notes():
    date_str = request.args.get("date") or datetime.utcnow().strftime("%Y-%m-%d")
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("SELECT note_text, staff_list, staff_updated_at FROM briefing_notes WHERE note_date = %s", (date_str,))
    row = cur.fetchone()
    cur.close(); conn.close()

    # Parse staff_list as JSON history array; fall back for legacy plain-text values
    staff_entries = []
    if row:
        raw = (row["staff_list"] or "").strip()
        if raw:
            try:
                parsed = json.loads(raw)
                staff_entries = parsed if isinstance(parsed, list) else [{"text": raw, "saved_at": row["staff_updated_at"] or ""}]
            except Exception:
                staff_entries = [{"text": raw, "saved_at": (row["staff_updated_at"] or "")}]

    return jsonify({
        "note_text":    (row["note_text"] or "").strip() if row else "",
        "staff_entries": staff_entries,
        "date": date_str,
    })


@briefing_bp.route("/briefing/notes", methods=["POST"])
@login_required
def save_briefing_notes():
    data      = request.get_json(force=True)
    date_str  = (data.get("date") or datetime.utcnow().strftime("%Y-%m-%d")).strip()
    note_text = (data.get("note_text") or "").strip()
    now       = datetime.utcnow().isoformat()

    conn = get_db()
    cur  = get_cursor(conn)

    if "staff_list" in data:
        new_text = (data.get("staff_list") or "").strip()
        if new_text:
            # Fetch existing entries to prepend to history
            cur.execute("SELECT staff_list FROM briefing_notes WHERE note_date = %s", (date_str,))
            existing_row = cur.fetchone()
            existing_raw = (existing_row["staff_list"] or "").strip() if existing_row else ""
            try:
                existing = json.loads(existing_raw) if existing_raw else []
                if not isinstance(existing, list):
                    existing = [{"text": existing_raw, "saved_at": now}] if existing_raw else []
            except Exception:
                existing = [{"text": existing_raw, "saved_at": now}] if existing_raw else []
            entries = [{"text": new_text, "saved_at": now}] + existing
        else:
            entries = []
        staff_json = json.dumps(entries)
        cur.execute(
            """INSERT INTO briefing_notes (note_date, note_text, staff_list, staff_updated_at, updated_by, updated_at)
               VALUES (%s, %s, %s, %s, %s, %s)
               ON CONFLICT (note_date) DO UPDATE
               SET staff_list       = EXCLUDED.staff_list,
                   staff_updated_at = EXCLUDED.staff_updated_at,
                   updated_by       = EXCLUDED.updated_by,
                   updated_at       = EXCLUDED.updated_at""",
            (date_str, "", staff_json, now, current_user.id, now)
        )
    else:
        cur.execute(
            """INSERT INTO briefing_notes (note_date, note_text, updated_by, updated_at)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT (note_date) DO UPDATE
               SET note_text  = EXCLUDED.note_text,
                   updated_by = EXCLUDED.updated_by,
                   updated_at = EXCLUDED.updated_at""",
            (date_str, note_text, current_user.id, now)
        )

    conn.commit()
    cur.close(); conn.close()

    # Bust the briefing cache for this date so the next generation picks up the new notes
    _briefing_cache.pop(date_str, None)

    return jsonify({"success": True})


@briefing_bp.route("/briefing")
@login_required
def daily_briefing():
    date_str      = request.args.get("date") or datetime.utcnow().strftime("%Y-%m-%d")
    team_id       = request.args.get("team_id") or None
    force_refresh = request.args.get("refresh") == "1"
    now           = time.time()

    cache_key = f"{date_str}:{team_id or ''}"
    if not force_refresh and cache_key in _briefing_cache:
        ts, payload = _briefing_cache[cache_key]
        if now - ts < CACHE_TTL:
            return jsonify({**payload, "cached": True})

    routes        = _fetch_todays_routes(date_str, team_id=team_id)
    checkins      = _fetch_breezeway_checkins(date_str)
    notes         = _fetch_briefing_notes(date_str)
    blurb, err_msg = _generate_briefing(date_str, routes, checkins, notes)

    if blurb:
        payload = {"blurb": blurb, "routes": _summarise_routes(routes)}
        _briefing_cache[cache_key] = (now, payload)
        return jsonify({**payload, "cached": False})

    return jsonify({"blurb": None, "error": err_msg or "Unknown error generating briefing."})
