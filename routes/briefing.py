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
from datetime import date as date_cls, datetime, timedelta
from zoneinfo import ZoneInfo

import anthropic
import requests
from flask import Blueprint, jsonify, request
from flask_login import login_required, current_user

from db import get_db, get_cursor

briefing_bp = Blueprint("briefing", __name__)

_PACIFIC = ZoneInfo("America/Los_Angeles")

def _fmt_pacific(ts: float) -> str:
    """Format a unix timestamp as 12-hour Pacific time, e.g. '2:34 PM PT'."""
    return datetime.fromtimestamp(ts, tz=_PACIFIC).strftime("%I:%M %p PT").lstrip("0")

ANTHROPIC_API_KEY       = os.environ.get("ANTHROPIC_API_KEY", "")
BREEZEWAY_CLIENT_ID     = os.environ.get("BREEZEWAY_CLIENT_ID", "")
BREEZEWAY_CLIENT_SECRET = os.environ.get("BREEZEWAY_CLIENT_SECRET", "")

CACHE_TTL          = 15 * 60   # 15 minutes for briefing
CALENDAR_CACHE_TTL = 30 * 60   # 30 minutes for calendar activity

# ── In-memory caches ──────────────────────────────────────────────
_briefing_cache:    dict  = {}   # {cache_key: (timestamp, payload)}
_calendar_cache:    dict  = {}   # {(year, month): (timestamp, activity_dict)}
_day_summary_cache: dict  = {}   # {date_str: (timestamp, payload)}
_bw_token:          dict  = {"value": None, "expires_at": 0}
_property_cache:    dict  = {}   # {property_id: name}
_property_cache_ts: float = 0


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


_property_cache_error: str  = ""   # last error from property fetch, for diagnostics
_property_addr_cache:  dict = {}   # {property_id: address_string}


def _load_property_cache() -> str:
    """Fetch all Breezeway properties into _property_cache. Returns error string or ''."""
    global _property_cache, _property_addr_cache, _property_cache_ts, _property_cache_error
    token = _get_breezeway_token()
    if not token:
        _property_cache_error = "No Breezeway token"
        return _property_cache_error
    try:
        page, limit = 1, 200
        fetched      = {}
        fetched_addr = {}
        while True:
            resp = requests.get(
                "https://api.breezeway.io/public/inventory/v1/property",
                headers={"Authorization": f"JWT {token}"},
                params={"limit": limit, "page": page},
                timeout=15,
            )
            if not resp.ok:
                _property_cache_error = f"HTTP {resp.status_code}: {resp.text[:200]}"
                return _property_cache_error
            data = resp.json()
            items = (data.get("results", data.get("data", data if isinstance(data, list) else [])) or [])
            for p in items:
                pid  = p.get("id")
                name = (p.get("name") or p.get("property_name") or
                        p.get("title") or p.get("display_name") or str(pid))
                # Try several common address field names Breezeway might use
                addr = (p.get("address") or p.get("full_address") or
                        p.get("street_address") or p.get("location") or "")
                if isinstance(addr, dict):
                    # Some APIs return address as a nested object
                    parts = [
                        addr.get("street") or addr.get("line1") or "",
                        addr.get("city") or "",
                        addr.get("state") or "",
                    ]
                    addr = ", ".join(x for x in parts if x)
                if pid:
                    fetched[pid]      = name
                    fetched_addr[pid] = str(addr).strip()
            if len(items) < limit:
                break
            page += 1
        _property_cache      = fetched
        _property_addr_cache = fetched_addr
        _property_cache_ts   = time.time()
        _property_cache_error = ""
        return ""
    except Exception as e:
        _property_cache_error = f"{type(e).__name__}: {e}"
        return _property_cache_error


def _ensure_property_cache():
    if not _property_cache or time.time() - _property_cache_ts > 3600:
        _load_property_cache()


def _get_property_name(property_id) -> str:
    """Return a property's display name by Breezeway property_id, cached 1 hour."""
    if not property_id:
        return "Unknown Property"
    _ensure_property_cache()
    return _property_cache.get(property_id, f"Property {property_id}")


def _get_property_address(property_id) -> str:
    """Return a property's address by Breezeway property_id, cached 1 hour."""
    if not property_id:
        return ""
    _ensure_property_cache()
    return _property_addr_cache.get(property_id, "")


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

def _extract_str(val) -> str:
    """Safely pull a lowercase string out of whatever Breezeway sends.
    type_stay / type_reservation are dicts like {"code": "owner", "name": "Owner Stay"}.
    Prefer 'code' — it is the machine-readable standardised value.
    Tags are {"id": int, "name": str} with no code field, so name is used as fallback.
    """
    if not val:
        return ""
    if isinstance(val, dict):
        return (val.get("code") or val.get("name") or
                val.get("label") or val.get("type") or "").lower().strip()
    return str(val).lower().strip()


def _classify_reservation(r: dict) -> str:
    """Returns 'lease', 'owner', 'block', or 'guest'.

    Priority order:
      1. type_reservation.code == hold/block → block (overrides everything)
      2. type_stay.code == owner → owner
      3. Tag "Owner Next" → owner (manual marker for BW-miscategorised owner bookings)
      4. Duration >= 30 days → lease (applies to all non-owner, non-block guest stays)
      5. type_stay.code == lease → lease
      6. guest
    """
    ts = _extract_str(r.get("type_stay"))
    tr = _extract_str(r.get("type_reservation"))
    tag_names = [_extract_str(t) for t in (r.get("tags") or [])]

    # Holds/blocks take priority — even over Owner Next tag
    if tr in _BLOCK_TYPES or ts in _BLOCK_TYPES:
        return "block"

    # Owner stays
    if ts == "owner":
        return "owner"
    if "owner next" in tag_names:
        return "owner"

    # Duration check runs for ALL remaining reservations — a paying guest
    # stay of 30+ nights is a lease regardless of how Breezeway labels it
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

    if ts == "lease":
        return "lease"

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
            kind     = _classify_reservation(r)
            counts[kind] = counts.get(kind, 0) + 1
            prop     = _get_property_name(r.get("property_id"))
            t        = r.get("checkin_time", "")
            out_date = r.get("checkout_date", "")
            prefix   = {"lease": "[LEASE] ", "owner": "[OWNER] "}.get(kind, "")
            entry    = f"- {prefix}{prop}"
            if t:
                entry += f" — check-in at {_fmt_time(t)}"
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
            prop = _get_property_name(r.get("property_id"))
            t    = r.get("checkout_time", "")
            prefix = {"lease": "[LEASE] ", "owner": "[OWNER] "}.get(kind, "")
            entry  = f"- {prefix}{prop}"
            if t:
                entry += f" — checkout by {_fmt_time(t)}"
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
                "in Lake Tahoe. Write 2-3 sentences using ONLY the data provided — never infer, "
                "invent, or add any detail not explicitly present.\n"
                "Cover: how many routes are planned (use 'planned', never 'dispatched'); "
                "how many guest arrivals and departures; call out lease arrivals or departures "
                "if any are marked [LEASE] in the data.\n"
                "Rules:\n"
                "- Never rename or reclassify a reservation. [OWNER] = owner stay, "
                "[LEASE] = long-term paying guest (30+ days), [GUEST] = regular guest, "
                "[BLOCK] = maintenance/hold. Use these exactly as given.\n"
                "- Do not name individual properties.\n"
                "- Do not name individual technicians or routes.\n"
                "- Do not describe transitions between consecutive reservations.\n"
                "- Use the actual day name (e.g. 'Thursday') — never 'today'.\n"
                "- Be direct. Do not start with a greeting."
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
                return jsonify({**payload, "cached": True, "cached_at": _fmt_pacific(ts)})

        routes    = _fetch_todays_routes(date_str, team_id=team_id)
        checkins  = _fetch_breezeway_checkins(date_str)
        checkouts = _fetch_breezeway_checkouts(date_str)
        notes     = _fetch_briefing_notes(date_str)
        blurb, err_msg = _generate_briefing(date_str, routes, checkins, checkouts, notes)

        if blurb:
            payload = {"blurb": blurb, "routes": _summarise_routes(routes)}
            _briefing_cache[cache_key] = (now, payload)
            return jsonify({**payload, "cached": False, "cached_at": _fmt_pacific(now)})

        return jsonify({"blurb": None, "error": err_msg or "Unknown error generating briefing."})

    except Exception as e:
        import flask
        flask.current_app.logger.error(f"daily_briefing unhandled: {type(e).__name__}: {e}")
        return jsonify({"blurb": None, "error": f"Server error: {type(e).__name__}: {e}"}), 500


@briefing_bp.route("/briefing/day-summary")
@login_required
def day_summary():
    """Return arrivals and departures grouped by type for a given date.

    Results are cached per-date until explicitly refreshed via ?refresh=1.
    """
    date_str = request.args.get("date") or datetime.utcnow().strftime("%Y-%m-%d")
    force    = request.args.get("refresh") == "1"

    cached = _day_summary_cache.get(date_str)
    if cached and not force:
        ts, payload = cached
        return jsonify({**payload, "cached_at": _fmt_pacific(ts)})

    token = _get_breezeway_token()
    if not token:
        return jsonify({"arrivals": {}, "departures": {}, "cached_at": None})

    checkins  = _fetch_bw_reservations(token, {
        "checkin_date_ge": date_str, "checkin_date_le": date_str,
    })
    checkouts = _fetch_bw_reservations(token, {
        "checkout_date_ge": date_str, "checkout_date_le": date_str,
    })

    arrivals   = {"guest": [], "owner": [], "lease": []}
    departures = {"guest": [], "owner": [], "lease": []}

    for r in checkins:
        kind = _classify_reservation(r)
        if kind == "block":
            continue
        prop = _get_property_name(r.get("property_id"))
        t    = (r.get("checkin_time") or "")[:5]
        arrivals.setdefault(kind, []).append({"name": prop, "time": t})

    for r in checkouts:
        kind = _classify_reservation(r)
        if kind == "block":
            continue
        prop = _get_property_name(r.get("property_id"))
        t    = (r.get("checkout_time") or "")[:5]
        departures.setdefault(kind, []).append({"name": prop, "time": t})

    payload = {"date": date_str, "arrivals": arrivals, "departures": departures}
    ts      = time.time()
    _day_summary_cache[date_str] = (ts, payload)
    cached_at = _fmt_pacific(ts)

    return jsonify({**payload, "cached_at": cached_at})


@briefing_bp.route("/briefing/pri-check")
@login_required
def pri_check():
    """Scan next 60 days of short-term guest checkouts for PRI needs.

    PRI required when a short-term guest (<30 days) checks out AND:
      - The immediately next reservation at that property is OWNER or BLOCK
        → needs "owner next" tag in Breezeway (or already tagged = done)
      - OR there is no upcoming reservation within 60 days
        → vacancy PRI must be created manually by ops
    """
    start_param = request.args.get("start_date")
    try:
        today = date_cls.fromisoformat(start_param) if start_param else date_cls.today()
    except Exception:
        today = date_cls.today()

    # days=30 → quick report (only show first 30 days), days=60 → full (default)
    try:
        report_days = int(request.args.get("days", 60))
    except Exception:
        report_days = 60
    report_days = max(1, min(report_days, 60))

    scan_end = today + timedelta(days=60)          # always scan 60 days for vacancy calc
    report_end = today + timedelta(days=report_days)  # display cutoff
    far_end  = today + timedelta(days=150)  # look further ahead to find what follows late checkouts

    token = _get_breezeway_token()
    if not token:
        return jsonify({"error": "Breezeway not configured."}), 500

    today_str      = today.isoformat()
    scan_end_str   = scan_end.isoformat()
    report_end_str = report_end.isoformat()
    far_end_str    = far_end.isoformat()

    # Short-term guest checkouts in next 60 days (always full window for vacancy calc)
    raw_checkouts = _fetch_bw_reservations(token, {
        "checkout_date_ge": today_str,
        "checkout_date_le": scan_end_str,
    })
    # All upcoming reservations in next 150 days (to find what follows each checkout)
    raw_upcoming = _fetch_bw_reservations(token, {
        "checkin_date_ge": today_str,
        "checkin_date_le": far_end_str,
    })

    # Validate dates server-side — Breezeway may not filter precisely for range queries
    # Filter to report_end (30 or 60 days) for display, but scan_end used for vacancy calc
    checkouts = [
        r for r in raw_checkouts
        if today_str <= (r.get("checkout_date") or "")[:10] <= report_end_str
    ]
    upcoming = [
        r for r in raw_upcoming
        if (r.get("checkin_date") or "")[:10] >= today_str
    ]

    # Group upcoming by property, sorted ascending by checkin date
    by_prop = {}
    for r in upcoming:
        pid = r.get("property_id")
        if pid:
            by_prop.setdefault(pid, []).append(r)
    for pid in by_prop:
        by_prop[pid].sort(key=lambda r: r.get("checkin_date", ""))

    needs_tag    = []   # 🔴 next is OWNER/BLOCK, not yet tagged
    already_done = []   # 🟢 next is OWNER/BLOCK, already tagged "owner next"
    no_booking   = []   # 🟠 no upcoming reservation found → vacancy PRI

    for co in checkouts:
        # Only short-term guest stays trigger a PRI (< 30 days, classified "guest")
        if _classify_reservation(co) != "guest":
            continue

        pid = co.get("property_id")
        if not pid:
            continue

        co_date_str = (co.get("checkout_date") or "")[:10]
        try:
            co_date = date_cls.fromisoformat(co_date_str)
        except Exception:
            continue

        prop_name = _get_property_name(pid)

        # Find the immediately next reservation at this property
        next_r       = None
        next_ci_date = None
        for r in by_prop.get(pid, []):
            ci_str = (r.get("checkin_date") or "")[:10]
            try:
                ci_date = date_cls.fromisoformat(ci_str)
            except Exception:
                continue
            if ci_date > co_date:
                next_r       = r
                next_ci_date = ci_date
                break

        # No upcoming reservation in scan window → vacancy PRI
        if not next_r or not next_ci_date:
            vacancy_days = (scan_end - co_date).days
            no_booking.append({
                "property":      prop_name,
                "checkout_date": co_date_str,
                "vacancy_days":  vacancy_days,
            })
            continue

        # PRI only triggered if next reservation is OWNER or BLOCK
        next_kind = _classify_reservation(next_r)
        if next_kind not in ("owner", "block"):
            continue   # guest or lease following → no PRI needed

        # Check for existing "owner next" tag on the upcoming booking
        tag_names = [_extract_str(t) for t in (next_r.get("tags") or [])]
        tagged    = "owner next" in tag_names

        gap_days = (next_ci_date - co_date).days
        entry = {
            "property":      prop_name,
            "checkout_date": co_date_str,
            "next_checkin":  next_ci_date.isoformat(),
            "next_type":     next_kind,
            "vacancy_days":  gap_days if gap_days >= 30 else None,
        }
        (already_done if tagged else needs_tag).append(entry)

    needs_tag.sort(key=lambda r: r["checkout_date"])
    already_done.sort(key=lambda r: r["checkout_date"])
    no_booking.sort(key=lambda r: r["checkout_date"])

    return jsonify({
        "needs_tag":       needs_tag,
        "already_tagged":  already_done,
        "no_booking":      no_booking,
        "scanned_through": report_end_str,
        "report_days":     report_days,
    })


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
                    "classified_as":   _classify_reservation(r),
                    "property_id":     r.get("property_id"),
                    "property_name":   _get_property_name(r.get("property_id")),
                    "type_stay":       r.get("type_stay"),
                    "type_reservation":r.get("type_reservation"),
                    "tags":            r.get("tags"),
                    "checkin_date":    r.get("checkin_date"),
                    "checkout_date":   r.get("checkout_date"),
                    "checkin_time":    r.get("checkin_time"),
                    "checkout_time":   r.get("checkout_time"),
                    "guest_name":      _guest_name(r),
                    "_all_keys":       list(r.keys()),
                    "_raw":            raw_safe,
                })
            return out

        return jsonify({
            "date":      date_str,
            "checkins":  summarise(checkins),
            "checkouts": summarise(checkouts),
        })
    except Exception as e:
        return jsonify({"error": str(e), "error_type": type(e).__name__}), 500


@briefing_bp.route("/briefing/debug-properties")
@login_required
def debug_properties():
    """Show Breezeway property cache state and raw fields from one property."""
    token = _get_breezeway_token()
    raw_sample = None
    if token:
        try:
            resp = requests.get(
                "https://api.breezeway.io/public/inventory/v1/property",
                headers={"Authorization": f"JWT {token}"},
                params={"limit": 1, "page": 1},
                timeout=15,
            )
            data  = resp.json()
            items = data.get("results", data.get("data", data if isinstance(data, list) else []))
            if items:
                raw_sample = items[0]
        except Exception as e:
            raw_sample = {"error": str(e)}

    err = _load_property_cache()
    return jsonify({
        "property_count":    len(_property_cache),
        "cache_error":       err or None,
        "sample_names":      dict(list(_property_cache.items())[:5]),
        "sample_addresses":  dict(list(_property_addr_cache.items())[:5]),
        "raw_fields_sample": raw_sample,
    })


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

        if kind == "block":
            continue  # blocks are internal holds, not real guest/owner activity

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
