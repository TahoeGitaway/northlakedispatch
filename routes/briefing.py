"""
routes/briefing.py — AI-powered daily operations briefing.

Pulls today's saved routes from the DB, plus arrivals and departures
from the Breezeway API (30+ day stays classified as "Lease"), then
asks Claude to write a plain-English summary.

Results are cached in memory for 15 minutes per date so repeated page
loads don't burn API quota.
"""

import calendar as cal_mod
import json
import os
import time
from datetime import date as date_cls, datetime

import anthropic
import requests
from flask import Blueprint, jsonify, request
from flask_login import login_required, current_user

from db import get_db, get_cursor

briefing_bp = Blueprint("briefing", __name__)

ANTHROPIC_API_KEY       = os.environ.get("ANTHROPIC_API_KEY", "")
BREEZEWAY_CLIENT_ID     = os.environ.get("BREEZEWAY_CLIENT_ID", "")
BREEZEWAY_CLIENT_SECRET = os.environ.get("BREEZEWAY_CLIENT_SECRET", "")

CACHE_TTL          = 15 * 60   # 15 minutes for briefing
CALENDAR_CACHE_TTL = 30 * 60   # 30 minutes for calendar activity

# ── In-memory caches ──────────────────────────────────────────────
_briefing_cache: dict  = {}   # {cache_key: (timestamp, payload)}
_calendar_cache: dict  = {}   # {(year, month): (timestamp, activity_dict)}
_bw_token:       dict  = {"value": None, "expires_at": 0}


# ── Breezeway auth ────────────────────────────────────────────────

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
        data  = resp.json()
        token = data.get("access_token")
        if token:
            _bw_token["value"]      = token
            _bw_token["expires_at"] = now + 23 * 3600
        return token
    except Exception:
        return None


# ── Breezeway data fetchers ───────────────────────────────────────

def _fetch_bw_reservations(token: str, params: dict) -> list:
    """Paginate through all Breezeway reservations matching params."""
    all_results = []
    page, limit = 1, 100
    try:
        while True:
            resp = requests.get(
                "https://api.breezeway.io/public/inventory/v1/reservation",
                headers={"Authorization": f"JWT {token}"},
                params={**params, "limit": limit, "page": page},
                timeout=15,
            )
            data = resp.json()
            page_results = (data.get("results", data.get("data", [])) or []) \
                           if isinstance(data, dict) else (data or [])
            all_results.extend(page_results)
            if len(page_results) < limit:
                break
            page += 1
    except Exception:
        pass
    return all_results


def _fetch_breezeway_checkins(date_str: str) -> list:
    token = _get_breezeway_token()
    if not token:
        return []
    return _fetch_bw_reservations(token, {
        "checkin_date_ge": date_str, "checkin_date_le": date_str,
    })


def _fetch_breezeway_checkouts(date_str: str) -> list:
    token = _get_breezeway_token()
    if not token:
        return []
    return _fetch_bw_reservations(token, {
        "checkout_date_ge": date_str, "checkout_date_le": date_str,
    })


_BLOCK_TYPES = {"block", "maintenance", "hold", "owner_block", "management_block"}

def _classify_reservation(r: dict) -> str:
    """Returns 'lease', 'owner', 'block', or 'guest'.

    Priority order:
      1. type_stay field (Breezeway's own classification — most authoritative)
      2. tags containing 'owner' or 'lease'
      3. Duration fallback: stays >= 30 days → lease
    Blocks are detected early so they don't inflate lease/guest counts.
    """
    ts   = (r.get("type_stay") or "").lower().strip()
    tags = [str(t).lower() for t in (r.get("tags") or [])]

    if ts in _BLOCK_TYPES or any(t in ("block", "hold") for t in tags):
        return "block"
    if ts == "owner" or any("owner" in t for t in tags):
        return "owner"
    if ts == "lease" or any("lease" in t for t in tags):
        return "lease"

    # Duration fallback only when type_stay gives no signal
    checkin  = r.get("checkin_date")  or ""
    checkout = r.get("checkout_date") or ""
    if checkin and checkout:
        try:
            days = (date_cls.fromisoformat(checkout[:10]) -
                    date_cls.fromisoformat(checkin[:10])).days
            if days >= 30:
                return "lease"
        except Exception:
            pass
    return "guest"


def _fmt_time(hhmm: str) -> str:
    """Convert 'HH:MM:SS' or 'HH:MM' to '3:00 PM'."""
    try:
        parts = hhmm.split(":")
        h, m  = int(parts[0]), int(parts[1])
        return f"{h % 12 or 12}:{m:02d} {'AM' if h < 12 else 'PM'}"
    except Exception:
        return hhmm


def _guest_name(r: dict) -> str:
    guests = r.get("guests") or []
    if guests:
        g = guests[0]
        return f"{g.get('first_name','')} {g.get('last_name','')}".strip()
    return ""


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
               FROM saved_routes r JOIN users u ON r.created_by = u.id
               WHERE r.route_date = %s AND r.team_id = %s
               ORDER BY r.updated_at DESC""",
            (date_str, team_id),
        )
    else:
        cur.execute(
            """SELECT r.id, r.name, r.assigned_to, r.stops_json, r.notes, u.name AS created_by_name
               FROM saved_routes r JOIN users u ON r.created_by = u.id
               WHERE r.route_date = %s
               ORDER BY r.updated_at DESC""",
            (date_str,),
        )
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows


# ── Claude briefing ───────────────────────────────────────────────

def _summarise_routes(routes: list) -> list:
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


def _build_prompt(date_str: str, routes: list, checkins: list,
                  checkouts: list, notes: str = "") -> str:
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
            line     = f'- "{r["name"]}"'
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

    # Breezeway arrivals — exclude blocks
    checkins  = [r for r in checkins  if _classify_reservation(r) != "block"]
    checkouts = [r for r in checkouts if _classify_reservation(r) != "block"]

    if checkins:
        counts = {"guest": 0, "owner": 0, "lease": 0, "block": 0}
        arr_lines = []
        for r in checkins:
            kind = _classify_reservation(r)
            counts[kind] += 1
            name = _guest_name(r)
            t    = r.get("checkin_time", "")
            out_date = r.get("checkout_date", "")
            prefix = {"lease": "[LEASE] ", "owner": "[OWNER] "}.get(kind, "")
            entry  = f"- {prefix}{name or 'Guest'}"
            if t:
                entry += f" checking in at {_fmt_time(t)}"
            if out_date:
                entry += f" (checkout {out_date})"
            arr_lines.append(entry)

        summary = []
        if counts["guest"]:  summary.append(f"{counts['guest']} guest arrival{'s' if counts['guest']!=1 else ''}")
        if counts["owner"]:  summary.append(f"{counts['owner']} owner stay{'s' if counts['owner']!=1 else ''}")
        if counts["lease"]:  summary.append(f"{counts['lease']} lease arrival{'s' if counts['lease']!=1 else ''}")
        lines.append(f"Arrivals today ({', '.join(summary)}):\n" + "\n".join(arr_lines))
    else:
        lines.append("No arrivals scheduled for today.")

    # Breezeway departures
    if checkouts:
        lease_ct = sum(1 for r in checkouts if _classify_reservation(r) == "lease")
        dep_lines = []
        for r in checkouts:
            kind = _classify_reservation(r)
            name = _guest_name(r)
            t    = r.get("checkout_time", "")
            prefix = {"lease": "[LEASE] ", "owner": "[OWNER] "}.get(kind, "")
            entry  = f"- {prefix}{name or 'Guest'}"
            if t:
                entry += f" checking out by {_fmt_time(t)}"
            dep_lines.append(entry)
        lease_note = f" including {lease_ct} lease{'s' if lease_ct!=1 else ''}" if lease_ct else ""
        lines.append(
            f"Departures today ({len(checkouts)} total{lease_note}):\n" + "\n".join(dep_lines)
        )
    else:
        lines.append("No departures scheduled for today.")

    if not checkins and not checkouts and not _get_breezeway_token():
        lines[-2] = "(Breezeway data not available — credentials not configured.)"
        lines[-1] = ""

    if notes:
        lines.append(f"Additional notes from the dispatcher:\n{notes}")

    return "\n\n".join(l for l in lines if l)


def _generate_briefing(date_str: str, routes: list, checkins: list,
                        checkouts: list, notes: str = "") -> tuple[str | None, str | None]:
    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        return None, "ANTHROPIC_API_KEY is not set."

    try:
        prompt = _build_prompt(date_str, routes, checkins, checkouts, notes)
        client = anthropic.Anthropic(api_key=key)
        msg    = client.messages.create(
            model      = "claude-haiku-4-5-20251001",
            max_tokens = 180,
            system     = (
                "You are a concise operations briefer for a vacation rental cleaning company "
                "in Lake Tahoe. Write 2-3 sentences covering: how many routes are planned (use "
                "'planned', never 'dispatched'); how many guest arrivals and departures; and "
                "call out lease arrivals or departures specifically if any exist (30+ day stays). "
                "Do NOT name individual technicians or routes — that list appears below your summary. "
                "Use the actual day name (e.g. 'Thursday') — never use the word 'today'. "
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
    cur.execute(
        "SELECT note_text, staff_list, staff_updated_at FROM briefing_notes WHERE note_date = %s",
        (date_str,)
    )
    row = cur.fetchone()
    cur.close(); conn.close()

    staff_entries = []
    if row:
        raw = (row["staff_list"] or "").strip()
        if raw:
            try:
                parsed = json.loads(raw)
                staff_entries = parsed if isinstance(parsed, list) \
                    else [{"text": raw, "saved_at": row["staff_updated_at"] or ""}]
            except Exception:
                staff_entries = [{"text": raw, "saved_at": (row["staff_updated_at"] or "")}]

    return jsonify({
        "note_text":     (row["note_text"] or "").strip() if row else "",
        "staff_entries": staff_entries,
        "date":          date_str,
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
            cur.execute("SELECT staff_list FROM briefing_notes WHERE note_date = %s", (date_str,))
            existing_row = cur.fetchone()
            existing_raw = (existing_row["staff_list"] or "").strip() if existing_row else ""
            try:
                existing = json.loads(existing_raw) if existing_raw else []
                if not isinstance(existing, list):
                    existing = [{"text": existing_raw, "saved_at": now}] if existing_raw else []
            except Exception:
                existing = [{"text": existing_raw, "saved_at": now}] if existing_raw else []
            entries    = [{"text": new_text, "saved_at": now}] + existing
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
    _briefing_cache.pop(date_str, None)
    return jsonify({"success": True})


@briefing_bp.route("/briefing")
@login_required
def daily_briefing():
    date_str      = request.args.get("date") or datetime.utcnow().strftime("%Y-%m-%d")
    team_id       = request.args.get("team_id") or None
    force_refresh = request.args.get("refresh") == "1"
    now           = time.time()

    try:
        cache_key = f"{date_str}:{team_id or ''}"
        if not force_refresh and cache_key in _briefing_cache:
            ts, payload = _briefing_cache[cache_key]
            if now - ts < CACHE_TTL:
                return jsonify({**payload, "cached": True})

        routes    = _fetch_todays_routes(date_str, team_id=team_id)
        checkins  = _fetch_breezeway_checkins(date_str)
        checkouts = _fetch_breezeway_checkouts(date_str)
        notes     = _fetch_briefing_notes(date_str)
        blurb, err_msg = _generate_briefing(date_str, routes, checkins, checkouts, notes)

        if blurb:
            payload = {"blurb": blurb, "routes": _summarise_routes(routes)}
            _briefing_cache[cache_key] = (now, payload)
            return jsonify({**payload, "cached": False})

        return jsonify({"blurb": None, "error": err_msg or "Unknown error generating briefing."})

    except Exception as e:
        import flask
        flask.current_app.logger.error(f"daily_briefing unhandled: {type(e).__name__}: {e}")
        return jsonify({"blurb": None, "error": f"Server error: {type(e).__name__}: {e}"}), 500


@briefing_bp.route("/briefing/debug-reservations")
@login_required
def debug_reservations():
    """Return raw Breezeway reservation fields for a date — for diagnosing classification and discovering field names."""
    try:
        date_str  = request.args.get("date") or datetime.utcnow().strftime("%Y-%m-%d")
        checkins  = _fetch_breezeway_checkins(date_str)
        checkouts = _fetch_breezeway_checkouts(date_str)

        def safe(v):
            """Convert any value to something jsonify can handle."""
            try:
                json.dumps(v)
                return v
            except (TypeError, ValueError):
                return str(v)

        def summarise(rs):
            out = []
            for r in rs:
                # Dump every top-level key with a safe-serialized value
                raw_safe = {k: safe(v) for k, v in r.items()}
                out.append({
                    "classified_as": _classify_reservation(r),
                    "type_stay":     r.get("type_stay"),
                    "tags":          r.get("tags"),
                    "checkin_date":  r.get("checkin_date"),
                    "checkout_date": r.get("checkout_date"),
                    "checkin_time":  r.get("checkin_time"),
                    "checkout_time": r.get("checkout_time"),
                    "guest_name":    _guest_name(r),
                    "_all_keys":     list(r.keys()),
                    "_raw":          raw_safe,
                })
            return out

        return jsonify({
            "date":      date_str,
            "checkins":  summarise(checkins),
            "checkouts": summarise(checkouts),
        })
    except Exception as e:
        return jsonify({"error": str(e), "error_type": type(e).__name__}), 500


@briefing_bp.route("/briefing/calendar-activity")
@login_required
def calendar_activity():
    """Return arrival/departure/lease counts per date for a given month."""
    try:
        year  = int(request.args.get("year",  datetime.utcnow().year))
        month = int(request.args.get("month", datetime.utcnow().month))
    except ValueError:
        return jsonify({}), 400

    now       = time.time()
    cache_key = (year, month)
    if cache_key in _calendar_cache:
        ts, data = _calendar_cache[cache_key]
        if now - ts < CALENDAR_CACHE_TTL:
            return jsonify(data)

    token = _get_breezeway_token()
    if not token:
        return jsonify({})

    last_day = cal_mod.monthrange(year, month)[1]
    first_ds = f"{year}-{month:02d}-01"
    last_ds  = f"{year}-{month:02d}-{last_day:02d}"

    activity: dict = {}

    def ensure(ds):
        if ds not in activity:
            activity[ds] = {"arrivals": 0, "departures": 0, "leases": 0}

    # Single API call: all reservations that overlap this month.
    # checkin_date_le=last_ds  → checked in before month end
    # checkout_date_ge=first_ds → checked out after month start
    # Together they select every stay with any overlap with this month.
    for r in _fetch_bw_reservations(token, {
        "checkin_date_le":  last_ds,
        "checkout_date_ge": first_ds,
    }):
        checkin_ds  = r.get("checkin_date",  "") or ""
        checkout_ds = r.get("checkout_date", "") or ""
        kind = _classify_reservation(r)

        if first_ds <= checkin_ds <= last_ds:
            ensure(checkin_ds)
            activity[checkin_ds]["arrivals"] += 1
            if kind == "lease":
                activity[checkin_ds]["leases"] += 1

        if first_ds <= checkout_ds <= last_ds:
            ensure(checkout_ds)
            activity[checkout_ds]["departures"] += 1

    # Only cache non-empty results; an empty response likely means the API
    # call failed or timed out, and we want the next navigation to retry.
    if activity:
        _calendar_cache[cache_key] = (now, activity)
    return jsonify(activity)
