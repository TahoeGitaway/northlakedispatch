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
from flask_login import login_required

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
    """Return today's Breezeway reservations, or [] if unavailable."""
    token = _get_breezeway_token()
    if not token:
        return []
    try:
        resp = requests.get(
            "https://api.breezeway.io/public/inventory/v1/reservation",
            headers={"Authorization": f"JWT {token}"},
            params={"checkin_date_ge": date_str, "checkin_date_le": date_str, "limit": 100},
            timeout=10,
        )
        data = resp.json()
        # API may return a list directly or wrap it
        if isinstance(data, list):
            return data
        return data.get("results", data.get("data", [])) or []
    except Exception:
        return []


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


# ── DB helper ─────────────────────────────────────────────────────

def _fetch_todays_routes(date_str: str) -> list:
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        """SELECT r.name, r.assigned_to, r.stops_json, u.name AS created_by_name
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

def _build_prompt(date_str: str, routes: list, checkins: list) -> str:
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    day_name = date_obj.strftime("%A, %B ") + str(date_obj.day)
    lines    = [f"Today is {day_name}.\n"]

    # Routes
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

    return "\n\n".join(lines)


def _generate_briefing(date_str: str, routes: list, checkins: list) -> tuple[str | None, str | None]:
    """Returns (text, error_reason). One of the two will always be None."""
    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        return None, "ANTHROPIC_API_KEY is not set in the server environment."

    prompt = _build_prompt(date_str, routes, checkins)
    try:
        client = anthropic.Anthropic(api_key=key)
        msg    = client.messages.create(
            model      = "claude-haiku-4-5-20251001",
            max_tokens = 300,
            system     = (
                "You are a concise operations briefer for a vacation rental cleaning company "
                "in Lake Tahoe. Given today's dispatch routes and Breezeway check-in data, "
                "write a single short paragraph (3-5 sentences) summarizing the day. "
                "Refer to routes by their actual names. Note priority check-ins and their deadlines. "
                "Describe Breezeway check-ins by type (owner stays vs guest arrivals) and timing. "
                "Be direct and useful. Do not start with a greeting."
            ),
            messages   = [{"role": "user", "content": prompt}],
        )
        return msg.content[0].text, None
    except Exception as e:
        import flask
        flask.current_app.logger.error(f"Briefing generation failed: {type(e).__name__}: {e}")
        return None, f"{type(e).__name__}: {e}"


# ── Endpoint ──────────────────────────────────────────────────────

@briefing_bp.route("/briefing")
@login_required
def daily_briefing():
    date_str      = datetime.utcnow().strftime("%Y-%m-%d")
    force_refresh = request.args.get("refresh") == "1"
    now           = time.time()

    if not force_refresh and date_str in _briefing_cache:
        ts, text = _briefing_cache[date_str]
        if now - ts < CACHE_TTL:
            return jsonify({"text": text, "cached": True})

    routes        = _fetch_todays_routes(date_str)
    checkins      = _fetch_breezeway_checkins(date_str)
    text, err_msg = _generate_briefing(date_str, routes, checkins)

    if text:
        _briefing_cache[date_str] = (now, text)
        return jsonify({"text": text, "cached": False})

    return jsonify({"text": None, "error": err_msg or "Unknown error generating briefing."})
