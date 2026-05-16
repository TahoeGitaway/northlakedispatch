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
from routes.auth import admin_required

briefing_bp = Blueprint("briefing", __name__)

_PACIFIC = ZoneInfo("America/Los_Angeles")

def _fmt_pacific(ts: float) -> str:
    """Format a unix timestamp as Pacific date + time, e.g. 'May 3, 2:34 PM PT'."""
    dt = datetime.fromtimestamp(ts, tz=_PACIFIC)
    time_str = dt.strftime("%I:%M %p PT").lstrip("0")
    date_str = dt.strftime("%b ") + str(dt.day)
    return f"{date_str}, {time_str}"

ANTHROPIC_API_KEY       = os.environ.get("ANTHROPIC_API_KEY", "")
BREEZEWAY_CLIENT_ID     = os.environ.get("BREEZEWAY_CLIENT_ID", "")
BREEZEWAY_CLIENT_SECRET = os.environ.get("BREEZEWAY_CLIENT_SECRET", "")

CACHE_TTL          = 15 * 60   # 15 minutes for briefing
CALENDAR_CACHE_TTL = 30 * 60   # 30 minutes for calendar activity

# ── In-memory caches ──────────────────────────────────────────────
_briefing_cache:      dict  = {}   # {cache_key: (timestamp, payload)}
_calendar_cache:      dict  = {}   # {(year, month): (timestamp, activity_dict)}
_day_summary_cache:   dict  = {}   # {date_str: (timestamp, payload)}
_prop_status_cache:   dict  = {}   # {property_id: (timestamp, payload)}
_PROP_STATUS_TTL            = 20 * 60   # 20 minutes per property
_bw_token:            dict  = {"value": None, "expires_at": 0}
_property_cache:      dict  = {}   # {property_id: name}
_property_cache_ts:   float = 0


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


def _fetch_bw_endpoint(token: str, path: str, params: dict) -> tuple:
    """Generic paginated GET for any Breezeway endpoint.
    Returns (results_list, error_string, http_status).
    Tries the path exactly as given — caller decides what to do with 404/403.
    """
    all_results = []
    page, limit = 1, 100
    last_status = None
    try:
        while True:
            resp = requests.get(
                f"https://api.breezeway.io{path}",
                headers={"Authorization": f"JWT {token}"},
                params={**params, "limit": limit, "page": page},
                timeout=15,
            )
            last_status = resp.status_code
            if not resp.ok:
                try:
                    detail = resp.json()
                except Exception:
                    detail = resp.text[:300]
                return [], f"HTTP {resp.status_code}: {detail}", last_status
            data = resp.json()
            page_results = (data.get("results", data.get("data", data.get("tasks", []))) or []) \
                           if isinstance(data, dict) else (data or [])
            all_results.extend(page_results)
            if len(page_results) < limit:
                break
            page += 1
    except requests.exceptions.Timeout:
        return [], "Request timed out — Breezeway API did not respond within 15 s", last_status
    except Exception as ex:
        return [], str(ex), last_status
    return all_results, "", last_status


def _fetch_bw_tasks(token: str, base_params: dict, date_param_sets: list = None) -> tuple:
    """Fetch Breezeway tasks for a date/property filter.
    Tries known task endpoint paths, then multiple date-param conventions.
    Returns (results, error_message).
    """
    candidate_paths = [
        "/public/work/v1/task",
        "/public/work/v2/task",
        "/public/inventory/v1/task",
        "/public/v1/task",
    ]
    # Date filter conventions to try in order
    if date_param_sets is None:
        date_param_sets = [{}]  # caller already merged date params into base_params

    last_err = "No task endpoint responded — task API may not be enabled on this Breezeway plan."

    for path in candidate_paths:
        # Quick probe: try base_params first
        results, err, status = _fetch_bw_endpoint(token, path, base_params)

        if status == 404:
            continue  # wrong path entirely, try next
        if status == 403:
            return [], ("Task data requires elevated API access on your Breezeway plan. "
                        "Contact Breezeway support to request task API access.")
        if status and status not in (200, 422):
            last_err = err or f"HTTP {status}"
            continue

        if status == 200:
            return results, ""  # success with base params

        # 422 means this path exists but our params are wrong — try date_param_sets
        if status == 422:
            first_422_body = err  # preserve the raw Breezeway error from the probe call
            non_date = {k: v for k, v in base_params.items()
                        if not any(x in k for x in ("date", "start", "end"))}
            last_422_body = first_422_body
            for dp in date_param_sets:
                merged = {**non_date, **dp}
                res2, err2, st2 = _fetch_bw_endpoint(token, path, merged)
                if st2 == 200:
                    return res2, ""
                if st2 == 403:
                    return [], ("Task data requires elevated API access on your Breezeway plan.")
                if st2 == 422:
                    last_422_body = err2 or last_422_body
                last_err = err2 or f"HTTP {st2} on {path} with {dp}"
            # Fell through all param sets — surface the raw Breezeway error body
            last_err = (
                f"422 on {path} — all date-param formats rejected. "
                f"Last Breezeway error body: {last_422_body}"
            )
            break  # stop path-hunting — we found the real path, params are just wrong

        if err:
            last_err = err

    return [], last_err


_property_cache_error: str  = ""   # last error from property fetch, for diagnostics
_property_addr_cache:  dict = {}   # {property_id: address_string}
_property_ref_cache:   dict = {}   # {property_id: reference_property_id string} for task API


def _load_property_cache() -> str:
    """Fetch all Breezeway properties into _property_cache. Returns error string or ''."""
    global _property_cache, _property_addr_cache, _property_cache_ts, _property_cache_error
    global _property_ref_cache
    token = _get_breezeway_token()
    if not token:
        _property_cache_error = "No Breezeway token"
        return _property_cache_error
    try:
        page, limit = 1, 200
        fetched      = {}
        fetched_addr = {}
        fetched_ref  = {}
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
                raw_name = (p.get("name") or p.get("property_name") or
                            p.get("title") or p.get("display_name") or pid)
                name = raw_name if isinstance(raw_name, str) else str(pid)
                # Capture external reference ID for the task API (reference_property_id)
                ref_id = (p.get("reference_property_id") or p.get("reference_id") or
                          p.get("external_id") or p.get("external_property_id") or "")
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
                    if ref_id:
                        fetched_ref[pid] = str(ref_id)
            if len(items) < limit:
                break
            page += 1
        _property_cache      = fetched
        _property_addr_cache = fetched_addr
        _property_ref_cache  = fetched_ref
        _property_cache_ts   = time.time()
        _property_cache_error = ""
        return ""
    except Exception as e:
        _property_cache_error = f"{type(e).__name__}: {e}"
        return _property_cache_error


def _ensure_property_cache():
    if not _property_cache or time.time() - _property_cache_ts > 3600:
        _load_property_cache()


def _get_live_property_cache() -> dict:
    """Return the current _property_cache, refreshing if stale.
    Use this instead of importing _property_cache directly — a direct import
    captures the reference at import time and misses subsequent reassignments."""
    _ensure_property_cache()
    return _property_cache


def _get_live_ref_cache() -> dict:
    """Return the current _property_ref_cache, refreshing if stale."""
    _ensure_property_cache()
    return _property_ref_cache


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
    cur.close(); conn.rollback(); conn.close()
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
    cur.close(); conn.rollback(); conn.close()
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

    # Dispatcher notes — placed first so the AI treats them as the lead
    if notes:
        lines.append(f"DISPATCHER NOTES (most important — lead with this):\n{notes}")

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
            max_tokens = 240,
            system     = (
                "You are a terse operations briefer for a vacation rental cleaning company "
                "in Lake Tahoe. Write exactly 1 sentence using ONLY the data provided.\n\n"
                "If dispatcher notes are present, lead with the key point from those notes. "
                "Otherwise, state the single most operationally important fact — a priority "
                "check-in deadline, a lease or owner arrival/departure, or anything that "
                "affects timing. If nothing is notable, say so in one plain sentence.\n\n"
                "Rules:\n"
                "- One sentence only. No lists, no paragraphs.\n"
                "- NEVER characterize the workload. Do not use: heavy, busy, light, big, "
                "significant, demanding, packed, full, or any similar word.\n"
                "- Do not name properties, technicians, or routes — those are listed below.\n"
                "- Use the actual day name (e.g. 'Thursday') — never 'today'.\n"
                "- No greeting. Start with the fact."
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
    peek_only     = request.args.get("peek") == "1"   # return saved blurb only, no generation
    now           = time.time()

    try:
        cache_key = f"{date_str}:{team_id or ''}"

        # 1. In-memory cache (fast path)
        if not force_refresh and cache_key in _briefing_cache:
            ts, payload = _briefing_cache[cache_key]
            if now - ts < CACHE_TTL:
                return jsonify({**payload, "cached": True, "cached_at": _fmt_pacific(ts)})

        # 2. DB-persisted blurb (survives server restarts)
        if not force_refresh:
            conn = get_db()
            cur  = get_cursor(conn)
            cur.execute(
                "SELECT blurb, blurb_generated_at FROM briefing_notes "
                "WHERE note_date = %s AND blurb IS NOT NULL AND blurb != ''",
                (date_str,)
            )
            row = cur.fetchone()
            cur.close(); conn.close()
            if row and row["blurb"]:
                routes  = _fetch_todays_routes(date_str, team_id=team_id)
                payload = {"blurb": row["blurb"], "routes": _summarise_routes(routes)}
                _briefing_cache[cache_key] = (now, payload)
                return jsonify({**payload, "cached": True,
                                "cached_at": row["blurb_generated_at"] or ""})

        # 3. Peek mode — only return what's saved, never generate
        if peek_only:
            return jsonify({"blurb": None, "peek": True})

        # 4. Generate fresh
        routes    = _fetch_todays_routes(date_str, team_id=team_id)
        checkins  = _fetch_breezeway_checkins(date_str)
        checkouts = _fetch_breezeway_checkouts(date_str)
        notes     = _fetch_briefing_notes(date_str)
        blurb, err_msg = _generate_briefing(date_str, routes, checkins, checkouts, notes)

        if blurb:
            generated_at = _fmt_pacific(now)
            # Auto-save to DB so it persists across server restarts
            try:
                conn = get_db()
                cur  = get_cursor(conn)
                cur.execute(
                    """INSERT INTO briefing_notes (note_date, note_text, blurb, blurb_generated_at, updated_at)
                       VALUES (%s, '', %s, %s, %s)
                       ON CONFLICT (note_date) DO UPDATE
                       SET blurb = EXCLUDED.blurb,
                           blurb_generated_at = EXCLUDED.blurb_generated_at""",
                    (date_str, blurb, generated_at, datetime.utcnow().isoformat())
                )
                conn.commit()
                cur.close(); conn.close()
            except Exception:
                pass
            payload = {"blurb": blurb, "routes": _summarise_routes(routes)}
            _briefing_cache[cache_key] = (now, payload)
            return jsonify({**payload, "cached": False, "cached_at": generated_at})

        return jsonify({"blurb": None, "error": err_msg or "Unknown error generating briefing."})

    except Exception as e:
        import flask
        flask.current_app.logger.error(f"daily_briefing unhandled: {type(e).__name__}: {e}")
        return jsonify({"blurb": None, "error": f"Server error: {type(e).__name__}: {e}"}), 500


@briefing_bp.route("/briefing/day-summary")
@login_required
def day_summary():
    """Return arrivals and departures grouped by type for a given date.

    Priority: 1) saved DB snapshot  2) in-memory cache  3) live Breezeway fetch
    Pass ?refresh=1 to force a live re-fetch (overwrites neither DB nor cache automatically).
    """
    date_str = request.args.get("date") or datetime.utcnow().strftime("%Y-%m-%d")
    force    = request.args.get("refresh") == "1"

    # 1) Check DB snapshot first (unless force-refresh requested)
    if not force:
        try:
            conn = get_db()
            cur  = get_cursor(conn)
            cur.execute(
                "SELECT arrivals, departures, saved_at FROM saved_day_summaries WHERE route_date = %s",
                (date_str,)
            )
            row = cur.fetchone()
            cur.close(); conn.rollback(); conn.close()
            if row:
                return jsonify({
                    "date":       date_str,
                    "arrivals":   json.loads(row["arrivals"]),
                    "departures": json.loads(row["departures"]),
                    "cached_at":  row["saved_at"],
                    "source":     "saved",
                })
        except Exception:
            pass

    # 2) In-memory cache
    cached = _day_summary_cache.get(date_str)
    if cached and not force:
        ts, payload = cached
        return jsonify({**payload, "cached_at": _fmt_pacific(ts), "source": "live"})

    # 3) Live Breezeway fetch
    token = _get_breezeway_token()
    if not token:
        return jsonify({"arrivals": {}, "departures": {}, "cached_at": None, "source": "live"})

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

    return jsonify({**payload, "cached_at": _fmt_pacific(ts), "source": "live"})


@briefing_bp.route("/briefing/save-day-summary", methods=["POST"])
@login_required
def save_day_summary():
    """Persist the current day's arrivals/departures snapshot to the DB."""
    data       = request.get_json(force=True)
    date_str   = (data.get("date") or "").strip()
    arrivals   = data.get("arrivals")
    departures = data.get("departures")

    if not date_str or arrivals is None or departures is None:
        return jsonify({"success": False, "error": "date, arrivals, and departures required"}), 400

    saved_at = _fmt_pacific(time.time())
    try:
        conn = get_db()
        cur  = get_cursor(conn)
        cur.execute("""
            INSERT INTO saved_day_summaries (route_date, arrivals, departures, saved_by, saved_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (route_date) DO UPDATE
              SET arrivals   = EXCLUDED.arrivals,
                  departures = EXCLUDED.departures,
                  saved_by   = EXCLUDED.saved_by,
                  saved_at   = EXCLUDED.saved_at
        """, (date_str, json.dumps(arrivals), json.dumps(departures),
              current_user.id, saved_at))
        conn.commit()
        cur.close(); conn.close()
        # Bust in-memory cache so next load comes from DB
        _day_summary_cache.pop(date_str, None)
        return jsonify({"success": True, "saved_at": saved_at})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@briefing_bp.route("/briefing/pri-check")
@login_required
def pri_check():
    """Scan short-term guest checkouts from 30 days ago through the forward window for PRI needs.

    PRI required when a short-term guest (<30 days) checks out AND:
      - The immediately next reservation at that property is OWNER or BLOCK
        → needs "owner next" tag in Breezeway (or already tagged = done)
      - OR there is no upcoming reservation within 60 days
        → vacancy PRI must be created manually by ops

    Looks back 30 days so owner stays added late to Streamline are still caught.
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

    lookback_start   = today - timedelta(days=30)    # scan back 30 days for late-added owner stays
    reso_lookback    = today - timedelta(days=180)   # look back 180 days for active owner/block stays
    scan_end         = today + timedelta(days=60)    # always scan 60 days forward for vacancy calc
    report_end       = today + timedelta(days=report_days)  # display cutoff
    far_end          = today + timedelta(days=150)   # look further ahead to find what follows late checkouts

    token = _get_breezeway_token()
    if not token:
        return jsonify({"error": "Breezeway not configured."}), 500

    lookback_str      = lookback_start.isoformat()
    reso_lookback_str = reso_lookback.isoformat()
    today_str         = today.isoformat()
    scan_end_str      = scan_end.isoformat()
    report_end_str    = report_end.isoformat()
    far_end_str       = far_end.isoformat()

    # Checkouts from 30 days ago through the forward window (catches last-minute owner additions)
    raw_checkouts = _fetch_bw_reservations(token, {
        "checkout_date_ge": lookback_str,
        "checkout_date_le": scan_end_str,
    })
    # Fetch all reservations that haven't ended yet (checkout >= today) plus upcoming ones.
    # Using checkout_date_ge instead of checkin_date_ge ensures currently active stays
    # (e.g. an owner in house since months ago) are always included regardless of checkin date.
    raw_upcoming = _fetch_bw_reservations(token, {
        "checkout_date_ge": today_str,
        "checkin_date_le":  far_end_str,
    })

    # Filter checkouts to the display window (lookback through report_end)
    checkouts = [
        r for r in raw_checkouts
        if lookback_str <= (r.get("checkout_date") or "")[:10] <= report_end_str
    ]
    upcoming = [
        r for r in raw_upcoming
        if (r.get("checkin_date") or "")[:10] >= lookback_str
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
            if ci_date >= co_date:  # >= catches same-day owner turnovers
                next_r       = r
                next_ci_date = ci_date
                break

        # No reservation within 60 days of this checkout → vacancy PRI
        vacancy_cutoff = co_date + timedelta(days=60)
        if not next_r or not next_ci_date or next_ci_date > vacancy_cutoff:
            no_booking.append({
                "property":      prop_name,
                "checkout_date": co_date_str,
                "vacancy_days":  60,
            })
            continue

        # PRI only triggered if next reservation is OWNER or BLOCK
        next_kind = _classify_reservation(next_r)
        if next_kind not in ("owner", "block"):
            continue   # guest or lease following → no PRI needed

        # Owner/block already arrived — PRI window has passed, nothing to action
        if next_ci_date < today:
            continue

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


def refresh_pri_banner_alerts(alert_days=3):
    """Recompute PRI alerts for the next `alert_days` days and write to DB.
    Called daily by the scheduler at 7:30 AM (and on-demand via admin route).
    Preserves dismissed status — only upserts metadata, never clears dismissed_at.
    """
    from db import get_db, get_cursor
    from datetime import timezone

    token = _get_breezeway_token()
    if not token:
        return

    today      = date_cls.today()
    window_end = today + timedelta(days=alert_days)

    lookback_start = today - timedelta(days=1)
    far_end        = today + timedelta(days=150)

    raw_checkouts = _fetch_bw_reservations(token, {
        "checkout_date_ge": lookback_start.isoformat(),
        "checkout_date_le": window_end.isoformat(),
    })
    raw_upcoming = _fetch_bw_reservations(token, {
        "checkin_date_ge": lookback_start.isoformat(),
        "checkin_date_le": far_end.isoformat(),
    })

    checkouts = [
        r for r in raw_checkouts
        if today.isoformat() <= (r.get("checkout_date") or "")[:10] <= window_end.isoformat()
    ]

    by_prop = {}
    for r in raw_upcoming:
        pid = r.get("property_id")
        if pid:
            by_prop.setdefault(pid, []).append(r)
    for pid in by_prop:
        by_prop[pid].sort(key=lambda r: r.get("checkin_date", ""))

    active_keys = set()
    rows_to_upsert = []

    for co in checkouts:
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

        next_r = next_ci_date = None
        for r in by_prop.get(pid, []):
            ci_str = (r.get("checkin_date") or "")[:10]
            try:
                ci_date = date_cls.fromisoformat(ci_str)
            except Exception:
                continue
            if ci_date >= co_date:
                next_r, next_ci_date = r, ci_date
                break

        if not next_r:
            key = f"{prop_name}::{co_date_str}"
            active_keys.add(key)
            rows_to_upsert.append((key, prop_name, co_date_str, None, "vacancy_pri"))
            continue

        next_kind = _classify_reservation(next_r)
        if next_kind not in ("owner", "block"):
            continue
        if next_ci_date < today:
            continue

        tag_names = [_extract_str(t) for t in (next_r.get("tags") or [])]
        if "owner next" in tag_names:
            continue  # already tagged — no alert needed

        key = f"{prop_name}::{co_date_str}::on"
        active_keys.add(key)
        rows_to_upsert.append((key, prop_name, co_date_str, next_ci_date.isoformat(), "needs_owner_next"))

    now = datetime.utcnow().isoformat()
    conn = get_db(); cur = get_cursor(conn)
    try:
        for (key, prop, co, nci, atype) in rows_to_upsert:
            cur.execute(
                """INSERT INTO pri_banner_alerts
                       (item_key, property_name, checkout_date, next_checkin, alert_type, created_at)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON CONFLICT (item_key) DO UPDATE SET
                       property_name = EXCLUDED.property_name,
                       checkout_date = EXCLUDED.checkout_date,
                       next_checkin  = EXCLUDED.next_checkin,
                       alert_type    = EXCLUDED.alert_type""",
                (key, prop, co, nci, atype, now),
            )
        # Remove alerts no longer in the active set and not yet dismissed
        if active_keys:
            placeholders = ",".join(["%s"] * len(active_keys))
            cur.execute(
                f"DELETE FROM pri_banner_alerts WHERE item_key NOT IN ({placeholders}) "
                "AND dismissed_at IS NULL",
                list(active_keys),
            )
        else:
            cur.execute("DELETE FROM pri_banner_alerts WHERE dismissed_at IS NULL")
        # Clean up old dismissed alerts (checkout more than 7 days ago)
        cutoff = (today - timedelta(days=7)).isoformat()
        cur.execute("DELETE FROM pri_banner_alerts WHERE checkout_date < %s", (cutoff,))
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        cur.close(); conn.close()


@briefing_bp.route("/api/pri-alerts")
@login_required
def api_pri_alerts():
    conn = get_db(); cur = get_cursor(conn)
    cur.execute(
        "SELECT item_key, property_name, checkout_date, next_checkin, alert_type "
        "FROM pri_banner_alerts WHERE dismissed_at IS NULL "
        "ORDER BY checkout_date ASC"
    )
    alerts = [dict(r) for r in cur.fetchall()]
    cur.close(); conn.rollback(); conn.close()
    return jsonify({"alerts": alerts})


@briefing_bp.route("/api/pri-alert/dismiss", methods=["POST"])
@login_required
def api_pri_alert_dismiss():
    key = (request.get_json(force=True) or {}).get("key", "").strip()
    if not key:
        return jsonify({"error": "key required"}), 400
    now  = datetime.utcnow().isoformat()
    conn = get_db(); cur = get_cursor(conn)
    cur.execute(
        "UPDATE pri_banner_alerts SET dismissed_at=%s, dismissed_by=%s WHERE item_key=%s",
        (now, current_user.id, key),
    )
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})


@briefing_bp.route("/api/cron/pri-check", methods=["POST"])
def cron_pri_check():
    """Unauthenticated cron endpoint — secured by Bearer token in CRON_SECRET env var.
    Called by Railway Cron Service at 7:30 AM PT so the check is reliable across deploys.
    """
    secret = os.environ.get("CRON_SECRET", "").strip()
    if not secret:
        return jsonify({"error": "CRON_SECRET not configured on server"}), 500
    auth = request.headers.get("Authorization", "")
    if auth != f"Bearer {secret}":
        return jsonify({"error": "unauthorized"}), 401
    try:
        refresh_pri_banner_alerts(alert_days=3)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@briefing_bp.route("/briefing/property-status")
@login_required
def property_status():
    """Return current occupancy status + upcoming bookings for one property (by name).
    Results cached 20 minutes per property — zero cost on repeat clicks.
    """
    prop_name = (request.args.get("name") or "").strip()
    if not prop_name:
        return jsonify({"error": "name required"}), 400

    token = _get_breezeway_token()
    if not token:
        return jsonify({"error": "Breezeway not configured"}), 500

    # Reverse-lookup: name → property_id using the existing property cache
    _ensure_property_cache()
    pid = next((k for k, v in _property_cache.items()
                if v.lower() == prop_name.lower()), None)
    if not pid:
        return jsonify({"error": f"Property not found in Breezeway: {prop_name}"}), 404

    # Serve from cache if fresh
    cached = _prop_status_cache.get(pid)
    if cached and time.time() - cached[0] < _PROP_STATUS_TTL:
        return jsonify(cached[1])

    today     = date_cls.today()
    today_str = today.isoformat()
    end_str   = (today + timedelta(days=90)).isoformat()

    raw = _fetch_bw_reservations(token, {
        "checkin_date_ge":  today_str,
        "checkin_date_le":  end_str,
    }) + _fetch_bw_reservations(token, {
        "checkout_date_ge": today_str,
        "checkout_date_le": end_str,
    })

    # Deduplicate by reservation id and filter to this property
    seen = set()
    prop_res = []
    for r in raw:
        rid = r.get("id")
        if r.get("property_id") == pid and rid not in seen:
            seen.add(rid)
            prop_res.append(r)
    prop_res.sort(key=lambda r: (r.get("checkin_date") or ""))

    # Determine current status
    status      = "vacant"
    status_kind = None
    checkout_today = None
    checkin_today  = None

    for r in prop_res:
        ci = (r.get("checkin_date")  or "")[:10]
        co = (r.get("checkout_date") or "")[:10]
        kind = _classify_reservation(r)
        if ci <= today_str <= co:
            status      = "occupied"
            status_kind = kind
        if co == today_str:
            checkout_today = kind
        if ci == today_str:
            checkin_today = kind

    # Build upcoming list (next 5 bookings starting from today or later)
    upcoming = []
    for r in prop_res:
        ci = (r.get("checkin_date")  or "")[:10]
        co = (r.get("checkout_date") or "")[:10]
        if co < today_str:
            continue
        upcoming.append({
            "type":     _classify_reservation(r),
            "checkin":  ci,
            "checkout": co,
        })
        if len(upcoming) >= 5:
            break

    # Days until next booking (if currently vacant)
    days_until_next = None
    if status == "vacant" and upcoming:
        try:
            days_until_next = (date_cls.fromisoformat(upcoming[0]["checkin"]) - today).days
        except Exception:
            pass

    payload = {
        "property":       prop_name,
        "status":         status,        # "occupied" | "vacant"
        "status_kind":    status_kind,   # "guest" | "owner" | "lease" | None
        "checkout_today": checkout_today,
        "checkin_today":  checkin_today,
        "days_until_next": days_until_next,
        "upcoming":       upcoming,
    }
    _prop_status_cache[pid] = (time.time(), payload)
    return jsonify(payload)


@briefing_bp.route("/briefing/debug-reservations")
@login_required
@admin_required
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
@admin_required
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
