"""
routes/lease_prep.py — Lease arrival prep scanner.

Finds all lease reservations (stays >= 30 days) arriving in the next 30 days.
For each, fetches every task scheduled or completed in the 30 days prior to
the arrival date and shows full detail: name, date/time, status, assignee.

Reads come from the overnight cache (saved_lease_scans), not from Breezeway.
Scanning this page live costs two Breezeway calls per lease on top of the
reservation sweep, so doing it mid-day meant a long wait against an API that
refuses roughly one call in twenty. The 4:30am job fills the table; the catch-up
jobs through the day fix whatever was refused, keeping everything already read.

Endpoints:
  GET  /admin/lease-prep         — page
  POST /admin/lease-prep/scan    — arrivals (JSON), cache-first
  POST /admin/lease-prep/scan-post — departures (JSON), cache-first
  POST /admin/lease-prep/retry   — re-read only the leases that failed
"""

import json
import time
import requests
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Blueprint, render_template, jsonify, request
from flask_login import login_required

from routes.auth import admin_required
from routes.bw_api_log import bw_get

lease_prep_bp = Blueprint("lease_prep", __name__)

BW_BASE = "https://api.breezeway.io"

# The window of tasks shown around each lease's anchor date: 30 days before an
# arrival, 30 days after a departure.
WINDOW_DAYS = 30

# Workers for the per-lease fan-out. The scheduled job is gentler than a person
# waiting on a page: it works a 30-day span of leases back to back against a rate
# limit shared with every other sweep in the app, where a page open is one span.
_JOB_WORKERS  = 4
_READ_WORKERS = 8

# Pause before re-trying what was refused. Breezeway's limit resets on the order
# of a minute, so an immediate retry just gets refused again — but a person
# watching a page cannot be made to wait that long, hence two figures. Anything
# still failing after the read-path retry is left to the scheduled catch-ups.
_JOB_RETRY_PAUSE_S  = 10.0
_READ_RETRY_PAUSE_S = 3.0

# Ceiling on how many failed leases a single page open will re-read live. Without
# it, a morning when Breezeway is refusing everything turns every page open back
# into the full live sweep this cache exists to remove.
_READ_TOPUP_MAX = 12


def _get_token():
    from routes.briefing import _get_breezeway_token
    return _get_breezeway_token()


def _get_property_name(pid):
    from routes.briefing import _get_live_property_cache, _ensure_property_cache
    _ensure_property_cache()
    cache = _get_live_property_cache()
    return (cache.get(str(pid)) or
            cache.get(int(pid) if str(pid).isdigit() else pid) or
            str(pid))


def _gated_get(token: str, path: str, params: dict = None, timeout: int = 15) -> tuple:
    """One un-paginated Breezeway GET, through the shared rate gate.
    Returns (json_body, error_string, http_status). Never raises.

    briefing's _fetch_bw_endpoint covers the paginated list endpoints, but it only
    ever hands back the results ARRAY — it cannot read a property detail body,
    where the tags hang off the object itself. This is the same gate and the same
    request log, for the single-object case. Going through the gate is the point:
    this module used to call bw_get directly, so every lease scan spent budget the
    gate had no idea it was spending, and the rest of the app absorbed the 429s.
    """
    from routes.bw_ratelimit import gate, LOCAL_THROTTLE_STATUS
    if not gate.acquire():
        return None, ("Held back by this app's rate limiter — not sent to "
                      "Breezeway"), LOCAL_THROTTLE_STATUS
    try:
        r = bw_get(f"{BW_BASE}{path}",
                   headers={"Authorization": f"JWT {token}"},
                   params=params or {}, timeout=timeout)
    except requests.exceptions.Timeout:
        return None, f"Request timed out — no response within {timeout} s", None
    except Exception as ex:
        return None, f"{type(ex).__name__}: {ex}", None
    gate.on_response(r.status_code)
    if r.status_code != 200:
        return None, f"HTTP {r.status_code}: {(r.text or '')[:200]}", r.status_code
    try:
        return r.json(), "", r.status_code
    except Exception as ex:
        return None, f"unreadable response body: {ex}", r.status_code


def _fetch_property_tags(token: str, pid: str) -> tuple:
    """(tags, error) for one property — tries the /tags endpoint, then the detail.

    The error half is what stops a refused lookup being recorded as an answer. A
    200 with no tags means this house carries none; a 429 means we don't know, and
    the two must not both arrive here as [].
    """
    errors = []
    for path in (f"/public/inventory/v1/property/{pid}/tags",
                 f"/public/inventory/v1/property/{pid}"):
        body, err, _st = _gated_get(token, path)
        if err:
            errors.append(err)
            continue
        if isinstance(body, list):
            return body, ""
        if isinstance(body, dict):
            return (body.get("tags") or body.get("property_tags") or []), ""
    return [], "; ".join(errors) or "no tag endpoint responded"


def _property_has_hot_tub(token: str, pid: str) -> tuple:
    """(True | False | None, error). None means the tags could not be read —
    deliberately not False, so an unreadable house is never cached overnight as
    one without a hot tub."""
    tags, err = _fetch_property_tags(token, pid)
    if err:
        return None, err
    for tag in tags:
        name = (tag.get("name") or tag.get("label") or "") if isinstance(tag, dict) else tag
        if "hot tub" in str(name).lower():
            return True, ""
    return False, ""


def _is_lease(r: dict) -> bool:
    """Lease = guest stay of 30+ nights. Excludes owner stays and blocks."""
    # Exclude owner stays and blocks first
    ts = r.get("type_stay") or {}
    tr = r.get("type_reservation") or {}
    ts_val = str((ts.get("code") or ts.get("name") or "") if isinstance(ts, dict) else ts).lower()
    tr_val = str((tr.get("code") or tr.get("name") or "") if isinstance(tr, dict) else tr).lower()
    if "owner" in ts_val or "block" in ts_val or "hold" in ts_val:
        return False
    if "block" in tr_val or "hold" in tr_val:
        return False
    # An "Owner Next" tag also marks an owner stay (Breezeway sometimes
    # miscategorises owner bookings) — matches _classify_reservation in briefing.py.
    for t in (r.get("tags") or []):
        tag = (str(t.get("name") or t.get("code") or "") if isinstance(t, dict) else str(t)).lower()
        if "owner next" in tag:
            return False
    # Must be 30+ nights
    checkin  = r.get("checkin_date",  "")[:10]
    checkout = r.get("checkout_date", "")[:10]
    if checkin and checkout:
        try:
            return (date.fromisoformat(checkout) - date.fromisoformat(checkin)).days >= 30
        except ValueError:
            pass
    return False


def _fetch_reservations(token: str, start: date, end: date,
                        date_field: str = "checkin") -> tuple:
    """(reservations, error) whose checkin (default) or checkout falls in the span.

    Paginated and paced by briefing's shared fetcher. The error half is not
    cosmetic: this used to break out of the page loop on any non-200 and return
    whatever it had, so a 429 on page two read as "that is all the leases there
    are" — a lease silently absent from the page, which is worse than an error.
    """
    from routes.briefing import _fetch_bw_endpoint
    results, err, _st = _fetch_bw_endpoint(
        token, "/public/inventory/v1/reservation",
        {f"{date_field}_date_ge": start.isoformat(),
         f"{date_field}_date_le": end.isoformat()})
    return (results or []), err


def _fetch_tasks_for_property(token: str, pid: str, ref_id: str,
                               start: date, end: date) -> tuple:
    """(tasks, error) for one property's task window.

    Breezeway accepts a different id key depending on the property, so up to three
    are tried. Empty is reported as an ANSWER only when a key actually answered:
    if every key that was tried errored, the result is unknown and the caller
    keeps the lease pending. Collapsing those two cases is how a throttled lookup
    ends up cached overnight as "this lease has no prep tasks at all".
    """
    from routes.briefing import _fetch_bw_endpoint
    date_range = f"{start.isoformat()},{end.isoformat()}"
    id_pairs = ([("reference_property_id", ref_id)] if ref_id else []) + \
               [("property_id", pid), ("home_id", pid)]
    errors, answered = [], False
    for key, val in id_pairs:
        results, err, _st = _fetch_bw_endpoint(
            token, "/public/inventory/v1/task/",
            {"scheduled_date": date_range, key: val})
        if err:
            errors.append(f"{key}: {err}")
            continue
        answered = True
        if results:
            return results, ""
    return [], ("" if answered else "; ".join(errors))


def _safe_str(val) -> str:
    """Extract a plain lowercase string from any Breezeway field (string or dict)."""
    if not val:
        return ""
    if isinstance(val, dict):
        return str(val.get("code") or val.get("name") or val.get("label") or "").lower().strip()
    return str(val).lower().strip()


def _guest_from_reservation(r: dict) -> str:
    """Best-effort guest name. Prefers the `guests` list (where Breezeway
    actually stores it), then falls back to flat fields."""
    for g in (r.get("guests") or []):
        if isinstance(g, dict):
            name = f"{g.get('first_name','').strip()} {g.get('last_name','').strip()}".strip()
            if name:
                return name
            if g.get("name"):
                return str(g["name"]).strip()
    guest_raw = (r.get("guest_name") or r.get("primary_guest") or r.get("name") or "")
    if isinstance(guest_raw, dict):
        return str(guest_raw.get("name") or
                   f"{guest_raw.get('first_name','')} {guest_raw.get('last_name','')}".strip()).strip()
    return str(guest_raw).strip()


def _first(r: dict, *keys):
    """Return the first present, non-empty value among keys (top-level)."""
    for k in keys:
        v = r.get(k)
        if v not in (None, "", [], {}):
            return v
    return None


def _reservation_info(r: dict) -> list:
    """Best-effort list of {label, value} facts about a reservation.

    Breezeway's reservation object varies by channel, so every field is
    optional — we only emit rows that are actually present. This is what
    powers the "more info" block; add promotions here as fields surface.
    """
    info = []

    def add(label, value):
        if value not in (None, "", [], {}):
            info.append({"label": label, "value": str(value)})

    # Reservation type / status
    ts = r.get("type_stay")
    tr = r.get("type_reservation")
    add("Stay type", (ts.get("name") if isinstance(ts, dict) else ts))
    add("Reservation type", (tr.get("name") if isinstance(tr, dict) else tr))
    add("Status", _first(r, "status", "reservation_status", "state"))

    # Contact — from the first guest record, then flat fallbacks
    guest = (r.get("guests") or [{}])
    g0 = guest[0] if guest and isinstance(guest[0], dict) else {}
    add("Email", g0.get("email") or _first(r, "guest_email", "email"))
    add("Phone", g0.get("phone") or g0.get("phone_number") or
                 _first(r, "guest_phone", "phone", "phone_number"))

    # Party size
    add("Guests", _first(r, "number_guests", "num_guests", "guests_count",
                         "total_guests", "occupancy"))
    add("Adults", _first(r, "adults", "num_adults"))
    add("Children", _first(r, "children", "num_children"))
    add("Pets", _first(r, "pets", "num_pets"))

    # Booking provenance
    add("Booked / created", _first(r, "created_at", "created", "date_created",
                                   "booked_at", "date_booked", "reservation_date"))
    add("Last updated", _first(r, "updated_at", "modified_at", "date_updated"))
    add("Source / channel", _first(r, "source", "channel", "booking_source",
                                   "origin", "booking_channel"))
    add("Confirmation #", _first(r, "confirmation_code", "confirmation_number",
                                 "reference_id", "external_reservation_id",
                                 "external_id"))

    # Check-in/out times (dates already shown in the header)
    add("Check-in time", _first(r, "checkin_time", "check_in_time"))
    add("Check-out time", _first(r, "checkout_time", "check_out_time"))

    return info


def _fmt_task(t: dict) -> dict:
    """Normalize a raw Breezeway task into a clean dict for the frontend."""
    title = t.get("title") or t.get("name") or ""
    if isinstance(title, dict):
        title = title.get("value") or title.get("name") or ""

    done_at_raw = (t.get("finished_at") or t.get("completed_at") or
                   t.get("completed_date") or "")
    raw_status  = (_safe_str(t.get("type_task_status")) or
                   _safe_str(t.get("status")) or
                   _safe_str(t.get("state")))
    if raw_status in ("complete", "completed", "done", "finished", "approved"):
        status = "complete"
    elif raw_status in ("in_progress", "in progress", "started"):
        status = "in_progress"
    else:
        status = "pending"

    # Assignee
    assignees = []
    for a in (t.get("assignments") or []):
        if isinstance(a, dict):
            n = (a.get("name") or a.get("full_name") or
                 f"{a.get('first_name','').strip()} {a.get('last_name','').strip()}".strip())
            if n:
                assignees.append(n)

    sched_date = str(t.get("scheduled_date") or "")
    sched_time = str(t.get("scheduled_time") or "")
    done_at    = str(done_at_raw)

    return {
        "title":      title,
        "sched_date": sched_date[:10] if sched_date else None,
        "sched_time": sched_time[:5]  if sched_time else None,
        "done_at":    done_at[:16]    if done_at    else None,
        "status":     status,
        "raw_status": raw_status,  # debug: remove once status mapping is confirmed
        "assignees":  assignees,
    }


def _fmt_tasks(raw_tasks: list) -> list:
    """Format and date-sort a property's raw task list. A task that will not
    format is kept as a visible error row rather than dropped — a missing task on
    a lease prep sheet reads as work nobody has to do."""
    out = []
    for t in (raw_tasks or []):
        try:
            out.append(_fmt_task(t))
        except Exception as e:
            import traceback
            out.append({"title": f"[ERROR formatting task: {e}]",
                        "sched_date": None, "sched_time": None,
                        "done_at": None, "status": "pending",
                        "assignees": [], "debug": traceback.format_exc()[-300:]})
    out.sort(key=lambda x: (x["sched_date"] or ""))
    return out


def _window_for(mode: str, anchor: date) -> tuple:
    """The task window for a lease: the 30 days before an arrival, or the
    departure day and the 30 days after it."""
    if mode == "post":
        return anchor, anchor + timedelta(days=WINDOW_DAYS)
    return anchor - timedelta(days=WINDOW_DAYS), anchor


def _anchor_of(reservation: dict, mode: str) -> str:
    raw = reservation.get("checkout_date" if mode == "post" else "checkin_date") or ""
    return str(raw)[:10]


# ── Overnight cache ───────────────────────────────────────────────
#
# Read helpers never raise: a cache that throws on read is worse than one that
# misses, because a miss just falls through to the live path the page had before.

def _now_stamp() -> str:
    from routes.briefing import _fmt_pacific
    return _fmt_pacific(time.time())


def _read_scan_state(mode: str) -> dict:
    """How far the overnight sweep has covered this direction, or {}."""
    from db import get_db, get_cursor
    try:
        conn = get_db(); cur = get_cursor(conn)
        try:
            cur.execute("SELECT * FROM lease_scan_state WHERE mode = %s", (mode,))
            return dict(cur.fetchone() or {})
        finally:
            cur.close(); conn.rollback(); conn.close()
    except Exception:
        return {}


def _write_scan_state(mode: str, span_from: date, span_to: date,
                      resv_ok: bool, last_error: str = "") -> None:
    from db import get_db, get_cursor
    try:
        conn = get_db(); cur = get_cursor(conn)
        try:
            cur.execute("""
                INSERT INTO lease_scan_state
                       (mode, span_from, span_to, resv_ok, last_error, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (mode) DO UPDATE
                  SET span_from  = EXCLUDED.span_from,
                      span_to    = EXCLUDED.span_to,
                      resv_ok    = EXCLUDED.resv_ok,
                      last_error = EXCLUDED.last_error,
                      updated_at = EXCLUDED.updated_at
            """, (mode, span_from.isoformat(), span_to.isoformat(),
                  bool(resv_ok), (last_error or "")[:500], _now_stamp()))
            conn.commit()
        finally:
            cur.close(); conn.close()
    except Exception:
        pass


def _span_is_covered(mode: str, start: date, end: date) -> bool:
    """True when the overnight sweep has a COMPLETE reservation list spanning the
    dates asked for. A partial sweep is not coverage: serving from it would show
    an empty page for a span whose leases were simply never fetched."""
    st = _read_scan_state(mode)
    if not st or not st.get("resv_ok"):
        return False
    return (str(st.get("span_from") or "9999") <= start.isoformat()
            and str(st.get("span_to") or "") >= end.isoformat())


def _read_cached_leases(mode: str, start: date, end: date) -> list:
    """Cached lease rows whose anchor date falls in the span, earliest first."""
    from db import get_db, get_cursor
    try:
        conn = get_db(); cur = get_cursor(conn)
        try:
            cur.execute("""
                SELECT * FROM saved_lease_scans
                 WHERE mode = %s AND anchor_date >= %s AND anchor_date <= %s
                 ORDER BY anchor_date
            """, (mode, start.isoformat(), end.isoformat()))
            return [dict(r) for r in cur.fetchall()]
        finally:
            cur.close(); conn.rollback(); conn.close()
    except Exception:
        return []


def _write_cached_lease(mode: str, pid: str, anchor: str,
                        reservation: dict = None, property_name: str = None,
                        tasks_attempted: bool = False, tasks: list = None,
                        tasks_ok: bool = False, tasks_error: str = "",
                        hot_tub_attempted: bool = False,
                        has_hot_tub: bool = None, hot_tub_ok: bool = False) -> None:
    """Upsert one lease row, keeping everything already read.

    The merge rules are the whole point of this function:

      * A payload is overwritten only by a SUCCESSFUL read of it. A refused retry
        leaves yesterday's tasks in place — stale beats blank on a prep sheet.
      * The ok flags always follow the latest ATTEMPT, so a nightly refresh that
        gets refused re-opens the row for the catch-up jobs even though the row
        still holds good data from before.
      * An attribute nobody asked about this round is left entirely alone, which
        is what lets the retry path re-read a lease's tasks without disturbing the
        hot-tub flag it already has (the two calls fail independently).
    """
    from db import get_db, get_cursor
    conn = get_db(); cur = get_cursor(conn)
    try:
        cur.execute("""
            INSERT INTO saved_lease_scans
                   (mode, property_id, anchor_date, property_name, reservation,
                    tasks, tasks_ok, tasks_error, has_hot_tub, hot_tub_ok, scanned_at)
            VALUES (%(mode)s, %(pid)s, %(anchor)s, %(pname)s, %(resv)s,
                    -- NULL, not '[]', when this insert did not read the tasks:
                    -- an empty list is an answer ("no prep work on this lease")
                    -- and a row that has never been read must not claim one.
                    CASE WHEN %(t_attempted)s AND %(t_ok)s THEN %(tasks)s END,
                    %(t_attempted)s AND %(t_ok)s, %(t_err)s,
                    CASE WHEN %(h_attempted)s AND %(h_ok)s THEN %(tub)s END,
                    %(h_attempted)s AND %(h_ok)s, %(stamp)s)
            ON CONFLICT (mode, property_id, anchor_date) DO UPDATE SET
                property_name = COALESCE(EXCLUDED.property_name, saved_lease_scans.property_name),
                reservation = COALESCE(EXCLUDED.reservation, saved_lease_scans.reservation),
                tasks       = CASE WHEN %(t_attempted)s AND %(t_ok)s
                                   THEN EXCLUDED.tasks ELSE saved_lease_scans.tasks END,
                tasks_ok    = CASE WHEN %(t_attempted)s
                                   THEN %(t_ok)s ELSE saved_lease_scans.tasks_ok END,
                tasks_error = CASE WHEN %(t_attempted)s
                                   THEN %(t_err)s ELSE saved_lease_scans.tasks_error END,
                has_hot_tub = CASE WHEN %(h_attempted)s AND %(h_ok)s
                                   THEN EXCLUDED.has_hot_tub ELSE saved_lease_scans.has_hot_tub END,
                hot_tub_ok  = CASE WHEN %(h_attempted)s
                                   THEN %(h_ok)s ELSE saved_lease_scans.hot_tub_ok END,
                scanned_at  = EXCLUDED.scanned_at
        """, {
            "mode": mode, "pid": str(pid), "anchor": anchor,
            "pname": property_name,
            "resv": json.dumps(reservation) if reservation is not None else None,
            "tasks": json.dumps(tasks or []),
            "t_attempted": bool(tasks_attempted), "t_ok": bool(tasks_ok),
            "t_err": (tasks_error or "")[:500] or None,
            "tub": has_hot_tub,
            "h_attempted": bool(hot_tub_attempted), "h_ok": bool(hot_tub_ok),
            "stamp": _now_stamp(),
        })
        conn.commit()
    finally:
        cur.close(); conn.close()


def _prune_cached_leases(mode: str, start: date, end: date, keep: set) -> int:
    """Drop cached rows in the span that the sweep no longer sees as leases, and
    anything long past. Returns how many were removed.

    Only ever called after a COMPLETE reservation sweep. A partial list here would
    read as "these leases are gone" and delete rows for houses that were merely
    throttled — the cache would lose data every time Breezeway had a bad morning.

    Without the pruning half, a cancelled or shortened lease stays on the page for
    good: nothing else ever revisits a row once its anchor date is written.
    """
    from db import get_db, get_cursor
    cutoff = (date.today() - timedelta(days=90)).isoformat()
    try:
        conn = get_db(); cur = get_cursor(conn)
        try:
            cur.execute("""
                DELETE FROM saved_lease_scans
                 WHERE mode = %s
                   AND (anchor_date < %s
                        OR (anchor_date >= %s AND anchor_date <= %s
                            AND NOT (property_id || '@' || anchor_date) = ANY(%s)))
            """, (mode, cutoff, start.isoformat(), end.isoformat(), list(keep) or [""]))
            removed = cur.rowcount
            conn.commit()
            return removed
        finally:
            cur.close(); conn.close()
    except Exception:
        return 0


def _row_to_result(row: dict) -> dict:
    """Rebuild the shape the page renders from one cached row.

    The raw reservation is stored rather than the fields derived from it, so the
    guest/party/channel block can gain rows without invalidating the cache.
    """
    try:
        reservation = json.loads(row.get("reservation") or "{}")
    except Exception:
        reservation = {}
    pid      = str(row.get("property_id") or "")
    checkin  = str(reservation.get("checkin_date")  or "")[:10]
    checkout = str(reservation.get("checkout_date") or "")[:10]
    try:
        nights = (date.fromisoformat(checkout) - date.fromisoformat(checkin)).days
    except ValueError:
        nights = None
    try:
        tasks = json.loads(row.get("tasks") or "[]")
    except Exception:
        tasks = []
    return {
        "pid":         pid,
        # Stored name first: looking it up live would warm the shared property
        # cache, and a cached read is supposed to touch Breezeway not at all.
        "property":    row.get("property_name") or _get_property_name(pid),
        "checkin":     checkin,
        "checkout":    checkout,
        "nights":      nights,
        "guest":       _guest_from_reservation(reservation),
        "has_hot_tub": row.get("has_hot_tub"),
        "info":        _reservation_info(reservation),
        "raw":         reservation,
        "tasks":       tasks,
        # Per-lease honesty about the cache. `stale` marks a row holding a real
        # earlier read that a later refresh could not renew; `unread` marks one
        # never read at all. The page must not present either as finished data.
        "unread":      not row.get("tasks_ok") and row.get("tasks") is None,
        "stale":       not row.get("tasks_ok") and row.get("tasks") is not None,
        "read_error":  row.get("tasks_error") or "",
        "scanned_at":  row.get("scanned_at") or "",
    }


def _work_leases(token: str, mode: str, rows: list, ref_cache: dict,
                 workers: int = _JOB_WORKERS) -> list:
    """Read whatever each row is still missing; return the rows still incomplete.

    Every success is written the moment it lands, so a pass that is throttled
    half way through keeps the half it got. Rows are mutated in place as they
    succeed, which is what stops the next retry pass re-reading the call that
    already worked and spending budget on data we hold.
    """
    def one(row):
        pid    = str(row.get("property_id") or "")
        anchor = str(row.get("anchor_date") or "")
        try:
            w_start, w_end = _window_for(mode, date.fromisoformat(anchor))
        except ValueError:
            return None                      # unparseable anchor: nothing to retry
        need_tasks = not row.get("tasks_ok")
        need_tub   = not row.get("hot_tub_ok")
        if not (need_tasks or need_tub):
            return None

        tasks, t_err = None, ""
        tub,   h_err = None, ""
        if need_tasks:
            raw, t_err = _fetch_tasks_for_property(
                token, pid, ref_cache.get(pid, ""), w_start, w_end)
            tasks = _fmt_tasks(raw)
        if need_tub:
            tub, h_err = _property_has_hot_tub(token, pid)

        try:
            _write_cached_lease(
                mode, pid, anchor,
                tasks_attempted=need_tasks, tasks=tasks,
                tasks_ok=(need_tasks and not t_err), tasks_error=t_err,
                hot_tub_attempted=need_tub, has_hot_tub=tub,
                hot_tub_ok=(need_tub and not h_err))
        except Exception as ex:
            row["write_error"] = f"{type(ex).__name__}: {ex}"
            return row

        if need_tasks and not t_err:
            row["tasks"], row["tasks_ok"], row["tasks_error"] = json.dumps(tasks), True, None
        if need_tub and not h_err:
            row["has_hot_tub"], row["hot_tub_ok"] = tub, True
        if need_tasks and t_err:
            row["tasks_error"] = t_err
        return row if ((need_tasks and t_err) or (need_tub and h_err)) else None

    if not rows:
        return []
    with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        return [r for r in ex.map(one, rows) if r]


def _work_with_retries(token: str, mode: str, rows: list, ref_cache: dict,
                       passes: int, pause_s: float, workers: int) -> list:
    """Work the rows, pause, and work only what failed — up to `passes` times.

    Retrying immediately is pointless (Breezeway's limit resets on the order of a
    minute) and retrying the whole set is wasteful, so each pass gets smaller and
    is separated by a real pause. Whatever is still failing at the end is left in
    the table marked pending, for the next scheduled catch-up.
    """
    pending = list(rows)
    for attempt in range(max(1, passes)):
        if not pending:
            break
        if attempt:
            time.sleep(pause_s)
        pending = _work_leases(token, mode, pending, ref_cache, workers=workers)
    return pending


def refresh_lease_scans(mode: str = "pre", days: int = WINDOW_DAYS,
                        only_missing: bool = False, passes: int = 2) -> dict:
    """Fill the lease cache for today → +`days`, for one direction.

    only_missing=True is the catch-up job: it re-reads just the leases a previous
    run could not, and costs two SQL queries and zero API calls once everything is
    covered — so it is safe to schedule several times a day.

    Never raises. A scheduled job that throws takes the scheduler thread with it.
    """
    out = {"mode": mode, "saved": [], "skipped": [], "errors": [], "pending": []}
    today = date.today()
    start, end = today, today + timedelta(days=days)

    try:
        token = _get_token()
        if not token:
            # Nothing was attempted, so coverage must not be claimed — leave the
            # span open for the next run rather than recording a clean sweep.
            _write_scan_state(mode, start, end, False, "no Breezeway token")
            out["errors"].append("no Breezeway token")
            return out

        from routes.briefing import _ensure_property_cache, _get_live_ref_cache
        _ensure_property_cache()
        ref_cache = _get_live_ref_cache()

        # Step 1: the reservation sweep. Skipped entirely when a catch-up run finds
        # the span already covered — the lease population is in the table, and only
        # the per-lease reads are outstanding.
        covered = _span_is_covered(mode, start, end)
        if not (only_missing and covered):
            date_field = "checkout" if mode == "post" else "checkin"
            reservations, err = _fetch_reservations(token, start, end,
                                                    date_field=date_field)
            if err:
                _write_scan_state(mode, start, end, False, err)
                out["errors"].append(f"reservation sweep: {err}")
                # Deliberately no return: the leases found by earlier runs are
                # still in the table and still worth completing.
            else:
                leases = [r for r in reservations if _is_lease(r)]
                keep = set()
                for r in leases:
                    pid    = str(r.get("property_id") or r.get("home_id") or "")
                    anchor = _anchor_of(r, mode)
                    if not pid or not anchor:
                        continue
                    keep.add(f"{pid}@{anchor}")
                    try:
                        # Reservation only — the task and tag reads happen below,
                        # and passing no attempt flags leaves their state untouched
                        # for a lease already completed by an earlier run.
                        _write_cached_lease(mode, pid, anchor, reservation=r,
                                            property_name=_get_property_name(pid))
                    except Exception as ex:
                        out["errors"].append(f"{pid} {anchor}: {type(ex).__name__}: {ex}")
                dropped = _prune_cached_leases(mode, start, end, keep)
                _write_scan_state(mode, start, end, True, "")
                out["saved"].append(
                    f"{len(leases)} leases between {start.isoformat()} and {end.isoformat()}"
                    + (f"; dropped {dropped} cancelled or expired" if dropped else ""))

        # Step 2: the per-lease reads.
        rows = _read_cached_leases(mode, start, end)
        if only_missing:
            todo = [r for r in rows if not r.get("tasks_ok") or not r.get("hot_tub_ok")]
        else:
            # A full refresh re-reads everything: a lease arriving in three weeks
            # has its prep tasks added and completed daily, so a row cached once
            # and never renewed would quietly go out of date.
            todo = rows
            for r in todo:
                r["tasks_ok"], r["hot_tub_ok"] = False, False
        if not todo:
            return out

        pending = _work_with_retries(token, mode, todo, ref_cache,
                                     passes=passes, pause_s=_JOB_RETRY_PAUSE_S,
                                     workers=_JOB_WORKERS)
        out["saved"].append(f"{len(todo) - len(pending)} of {len(todo)} leases read")
        out["pending"] = [f"{r.get('property_id')} {r.get('anchor_date')}: "
                          f"{r.get('tasks_error') or r.get('write_error') or 'hot tub tag'}"
                          for r in pending]
        if pending:
            out["skipped"].append(
                f"{len(pending)} leases incomplete — left for the next catch-up run")
    except Exception as ex:
        out["errors"].append(f"{type(ex).__name__}: {ex}")
    return out


@lease_prep_bp.route("/admin/lease-prep")
@login_required
def lease_prep_page():
    return render_template("lease_prep.html")


@lease_prep_bp.route("/admin/lease-prep/scan", methods=["POST"])
@login_required
def lease_prep_scan():
    try:
        return _lease_prep_scan_inner(mode="pre")
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        return jsonify({"error": f"{type(e).__name__}: {e}\n\n{tb}"}), 500


@lease_prep_bp.route("/admin/lease-prep/scan-post", methods=["POST"])
@login_required
def lease_prep_scan_post():
    try:
        return _lease_prep_scan_inner(mode="post")
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        return jsonify({"error": f"{type(e).__name__}: {e}\n\n{tb}"}), 500


@lease_prep_bp.route("/admin/lease-prep/retry", methods=["POST"])
@login_required
def lease_prep_retry():
    """Re-read only the leases in the span that are still incomplete.

    The same work the catch-up jobs do, on demand, for when you would rather not
    wait for the next one. Deliberately does NOT re-read the leases that are
    already complete: the point is to fill the gaps, not to spend the rate budget
    re-fetching data we hold.
    """
    payload = request.get_json(silent=True) or {}
    mode    = "post" if payload.get("mode") == "post" else "pre"
    start, end = _parse_span(payload)
    try:
        token = _get_token()
        if not token:
            return jsonify({"error": "Breezeway not configured."})
        from routes.briefing import _ensure_property_cache, _get_live_ref_cache
        _ensure_property_cache()
        ref_cache = _get_live_ref_cache()

        rows = [r for r in _read_cached_leases(mode, start, end)
                if not r.get("tasks_ok") or not r.get("hot_tub_ok")]
        if not rows:
            return _serve_from_cache(mode, start, end, topped_up=0)
        pending = _work_with_retries(token, mode, rows, ref_cache, passes=2,
                                     pause_s=_READ_RETRY_PAUSE_S,
                                     workers=_READ_WORKERS)
        return _serve_from_cache(mode, start, end,
                                 topped_up=len(rows) - len(pending))
    except Exception as e:
        import traceback
        return jsonify({"error": f"{type(e).__name__}: {e}\n\n{traceback.format_exc()}"}), 500


@lease_prep_bp.route("/admin/lease-prep/house-week")
@login_required
def lease_prep_house_week():
    """Day-by-day tasks around one lease's anchor date.

    pre  → the 7 days BEFORE arrival, ending on the checkin day.
    post → the checkout (departure) day and the 7 days AFTER it.

    Mirrors the VIP /vip/house-tasks week view, generalised for either direction.
    """
    from routes.briefing import (_ensure_property_cache, _get_live_ref_cache,
                                  _fetch_bw_endpoint, _classify_reservation)
    from routes.dispatch import _bw_task_title
    from routes.vip import _task_status, _assignees, _guest_name

    pid    = (request.args.get("pid") or "").strip()
    anchor = (request.args.get("anchor") or "").strip()[:10]
    mode   = (request.args.get("mode") or "pre").strip()
    if not pid or not anchor:
        return jsonify({"error": "pid and anchor required"}), 400
    try:
        anchor_date = date.fromisoformat(anchor)
    except ValueError:
        return jsonify({"error": "bad anchor date"}), 400

    if mode == "post":
        start, end, anchor_label = anchor_date, anchor_date + timedelta(days=7), "departure"
    else:
        start, end, anchor_label = anchor_date - timedelta(days=7), anchor_date, "arrival"

    token = _get_token()
    if not token:
        return jsonify({"error": "Breezeway not configured."}), 503
    _ensure_property_cache()
    ref_cache = _get_live_ref_cache()
    ref_id    = ref_cache.get(pid) or str(pid)

    drange = f"{start.isoformat()},{end.isoformat()}"
    results, _err, _st = _fetch_bw_endpoint(
        token, "/public/inventory/v1/task",
        {"reference_property_id": ref_id, "scheduled_date": drange})

    tasks = []
    for t in (results or []):
        td = (t.get("scheduled_date") or "")[:10]
        if not (start.isoformat() <= td <= end.isoformat()):
            continue
        dept = t.get("type_department")
        if isinstance(dept, dict):
            dept = dept.get("name") or dept.get("code")
        tasks.append({
            "id":        t.get("id"),
            "name":      _bw_task_title(t),
            "date":      td,
            "time":      (str(t.get("scheduled_time") or "")[:5]) or None,
            "status":    _task_status(t),
            "assignees": _assignees(t),
            "department": dept,
        })
    tasks.sort(key=lambda x: (x["date"], x["time"] or "~"))

    # Reservations overlapping the window — who is at the house, and what type.
    res_results, _re2, _rs2 = _fetch_bw_endpoint(
        token, "/public/inventory/v1/reservation",
        {"reference_property_id": ref_id,
         "checkout_date_ge": start.isoformat(),
         "checkin_date_le":  end.isoformat()})
    reservations = []
    for r in (res_results or []):
        rpid = str(r.get("property_id") or r.get("home_id") or "")
        if rpid and rpid != str(pid):
            continue                                   # endpoint may ignore the property filter
        cin  = (r.get("checkin_date") or "")[:10]
        cout = (r.get("checkout_date") or "")[:10]
        if not cin or not cout or cout < start.isoformat() or cin > end.isoformat():
            continue
        reservations.append({
            "type":     _classify_reservation(r),
            "checkin":  cin,
            "checkout": cout,
            "guest":    _guest_name(r),
        })
    reservations.sort(key=lambda x: x["checkin"])

    return jsonify({"matched": True, "matched_name": _get_property_name(pid),
                    "start": start.isoformat(), "end": end.isoformat(),
                    "anchor": anchor_date.isoformat(), "anchor_label": anchor_label,
                    "reservations": reservations, "tasks": tasks})


def _parse_span(payload: dict) -> tuple:
    """The From/To span to scan, defaulting to today → +30 days. Pre mode reads
    lease ARRIVALS (checkin), post mode lease DEPARTURES (checkout). A backwards
    range is swapped rather than rejected."""
    today = date.today()

    def _parse_date(val, default):
        try:
            return date.fromisoformat(str(val)[:10])
        except (ValueError, TypeError):
            return default

    start = _parse_date(payload.get("from"), today)
    end   = _parse_date(payload.get("to"),   today + timedelta(days=WINDOW_DAYS))
    return (end, start) if end < start else (start, end)


def _serve_from_cache(mode: str, start: date, end: date, topped_up: int = 0,
                      partial: bool = False):
    """Build the page response out of stored rows — no Breezeway calls.

    The `cache` block is what keeps this honest. A page served from a cache that
    is quietly missing three houses looks exactly like a complete one, and on a
    lease prep sheet that reads as three houses with no prep work outstanding.
    So the counts travel with the data and the page states them.
    """
    rows    = _read_cached_leases(mode, start, end)
    results = [_row_to_result(r) for r in rows]
    results.sort(key=lambda x: x["checkin"] if mode != "post" else x["checkout"])
    incomplete = [r for r in results if r["unread"] or r["stale"]]
    stamps     = [r["scanned_at"] for r in results if r["scanned_at"]]
    state      = _read_scan_state(mode)
    return jsonify({
        "leases": results,
        "range":  {"from": start.isoformat(), "to": end.isoformat()},
        "cache":  {
            "source":     "cache",
            "cached_at":  max(stamps) if stamps else (state.get("updated_at") or ""),
            "total":      len(results),
            "incomplete": len(incomplete),
            "unread":     sum(1 for r in results if r["unread"]),
            "stale":      sum(1 for r in results if r["stale"]),
            "topped_up":  topped_up,
            # True when the sweep has not covered this whole span yet, so the
            # list itself may be short a lease — a different worry from a lease
            # that is present but unread, and the page says so separately.
            "partial":    partial,
            "houses":     [r["property"] for r in incomplete][:20],
        },
    })


def _lease_prep_scan_inner(mode="pre"):
    """Cache first, Breezeway only when the cache cannot answer.

    Three paths, in order of cost:

      1. The span is covered by the overnight sweep → serve stored rows, and
         quietly re-read a bounded number of the ones that failed. That top-up is
         the same retry the scheduled jobs do, taken at the moment someone is
         actually looking; the bound is what stops a bad Breezeway morning turning
         every page open back into the full live sweep.
      2. force=true → skip the cache and read live, writing everything through.
      3. The span reaches outside the overnight window (a hand-picked range, or
         the sweep has not run yet) → live, and everything read is cached, so the
         work is not thrown away when the page is closed.
    """
    payload = request.get_json(silent=True) or {}
    start, end = _parse_span(payload)
    force = bool(payload.get("force"))

    # The page asks with cache_only on load, so opening it can never itself start
    # a live sweep — an instant page or an untouched one, never a five-minute
    # wait nobody asked for. The Scan button is what authorises going live.
    #
    # Short of a fully swept span, whatever leases ARE stored still go up, flagged
    # as partial. Holding them back until the span is perfect is how a page ends
    # up blank on the morning after a half-finished sweep — the very case where
    # seeing eleven of thirteen leases immediately is worth most.
    if payload.get("cache_only") and not (force or _span_is_covered(mode, start, end)):
        if not _read_cached_leases(mode, start, end):
            return jsonify({"needs_scan": True,
                            "range": {"from": start.isoformat(), "to": end.isoformat()}})
        return _serve_from_cache(mode, start, end, partial=True)

    if not force and _span_is_covered(mode, start, end):
        rows = _read_cached_leases(mode, start, end)
        todo = [r for r in rows if not r.get("tasks_ok") or not r.get("hot_tub_ok")]
        topped_up = 0
        if todo:
            token = _get_token()
            if token:
                from routes.briefing import _ensure_property_cache, _get_live_ref_cache
                _ensure_property_cache()
                # One pass only, over at most _READ_TOPUP_MAX houses: somebody is
                # waiting on this response. What it cannot finish stays marked
                # pending and the scheduled catch-ups take it from there.
                batch   = todo[:_READ_TOPUP_MAX]
                pending = _work_leases(token, mode, batch, _get_live_ref_cache(),
                                       workers=_READ_WORKERS)
                topped_up = len(batch) - len(pending)
        return _serve_from_cache(mode, start, end, topped_up=topped_up)

    token = _get_token()
    if not token:
        return jsonify({"error": "Breezeway not configured."})

    from routes.briefing import _get_live_ref_cache, _ensure_property_cache
    _ensure_property_cache()
    ref_cache = _get_live_ref_cache()

    # Step A: reservations for the span.
    date_field = "checkout" if mode == "post" else "checkin"
    try:
        reservations, resv_err = _fetch_reservations(token, start, end,
                                                     date_field=date_field)
    except Exception as e:
        import traceback
        return jsonify({"error": f"STEP A (fetch reservations): {e}\n{traceback.format_exc()}"})
    if resv_err and not reservations:
        # Nothing came back at all. Reporting "no leases" here would be a lie with
        # operational consequences, so say what actually happened.
        return jsonify({"error": f"Breezeway would not return the reservations for "
                                 f"this span: {resv_err}\n\nThis is usually rate "
                                 f"limiting — try again in a minute."})

    # Step B: classify leases.
    try:
        leases = [r for r in reservations if _is_lease(r)]
    except Exception as e:
        import traceback
        return jsonify({"error": f"STEP B (classify leases): {e}\n{traceback.format_exc()}"})

    range_used = {"from": start.isoformat(), "to": end.isoformat()}
    if not leases:
        return jsonify({"leases": [], "range": range_used,
                        "cache": {"source": "live", "total": 0, "incomplete": 0,
                                  "unread": 0, "stale": 0, "topped_up": 0,
                                  "houses": [], "cached_at": "",
                                  "warning": resv_err}})

    # Step C: record the leases, then read each one's tasks and hot-tub tag.
    # Writing the reservations first means a scan that dies half way through has
    # still left the population behind for the catch-up jobs to complete.
    rows, keep = [], set()
    for r in leases:
        pid    = str(r.get("property_id") or r.get("home_id") or "")
        anchor = _anchor_of(r, mode)
        if not pid or not anchor:
            continue
        keep.add(f"{pid}@{anchor}")
        try:
            _write_cached_lease(mode, pid, anchor, reservation=r,
                                property_name=_get_property_name(pid))
        except Exception:
            pass          # the cache is an optimisation; never fail the page for it
        rows.append({"property_id": pid, "anchor_date": anchor,
                     "tasks_ok": False, "hot_tub_ok": False})

    # Only safe because resv_err is empty here: a short reservation list would
    # otherwise delete the rows of leases Breezeway merely declined to mention.
    if not resv_err:
        _prune_cached_leases(mode, start, end, keep)

    try:
        _work_with_retries(token, mode, rows, ref_cache, passes=2,
                           pause_s=_READ_RETRY_PAUSE_S, workers=_READ_WORKERS)
    except Exception as e:
        import traceback
        return jsonify({"error": f"STEP C (fetch tasks): {e}\n{traceback.format_exc()}"})

    # Step D: hand back what is now stored. Reading it back rather than returning
    # the in-flight values means the live path and the cached path render from one
    # source, so "incomplete" is counted the same way on both.
    resp = _serve_from_cache(mode, start, end)
    body = resp.get_json()
    body["cache"]["source"] = "live"
    if resv_err:
        body["cache"]["warning"] = (
            f"The reservation list may be short some leases: {resv_err}")
    return jsonify(body)


