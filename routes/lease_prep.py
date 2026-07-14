"""
routes/lease_prep.py — Lease arrival prep scanner.

Finds all lease reservations (stays >= 30 days) arriving in the next 30 days.
For each, fetches every task scheduled or completed in the 30 days prior to
the arrival date and shows full detail: name, date/time, status, assignee.

Endpoints:
  GET  /admin/lease-prep        — page
  POST /admin/lease-prep/scan   — scan and return results (JSON)
"""

import requests
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Blueprint, render_template, jsonify, request
from flask_login import login_required

from routes.auth import admin_required

lease_prep_bp = Blueprint("lease_prep", __name__)

BW_BASE = "https://api.breezeway.io"


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


def _fetch_property_tags(token: str, pid: str) -> list:
    """Fetch tags for one property — tries the /tags endpoint, then the detail."""
    for path in (f"/public/inventory/v1/property/{pid}/tags",
                 f"/public/inventory/v1/property/{pid}"):
        try:
            r = requests.get(f"{BW_BASE}{path}",
                             headers={"Authorization": f"JWT {token}"}, timeout=15)
            if r.status_code == 200:
                body = r.json()
                if isinstance(body, list):
                    return body
                tags = body.get("tags") or body.get("property_tags") or []
                if tags:
                    return tags
        except Exception:
            pass
    return []


def _property_has_hot_tub(token: str, pid: str) -> bool:
    """True if the property carries a 'Hot Tub' tag in Breezeway."""
    for tag in _fetch_property_tags(token, pid):
        name = (tag.get("name") or tag.get("label") or "") if isinstance(tag, dict) else tag
        if "hot tub" in str(name).lower():
            return True
    return False


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
                        date_field: str = "checkin") -> list:
    """Fetch reservations whose checkin (default) or checkout date falls in the span."""
    ge_key = f"{date_field}_date_ge"
    le_key = f"{date_field}_date_le"
    all_results = []
    page = 1
    while True:
        try:
            r = requests.get(
                f"{BW_BASE}/public/inventory/v1/reservation",
                headers={"Authorization": f"JWT {token}"},
                params={ge_key: start.isoformat(),
                        le_key: end.isoformat(),
                        "limit": 100, "page": page},
                timeout=20,
            )
            if r.status_code != 200:
                break
            body    = r.json()
            results = body.get("results", body.get("data", []))
            if not results:
                break
            all_results.extend(results)
            if len(results) < 100:
                break
            page += 1
        except Exception:
            break
    return all_results


def _fetch_tasks_for_property(token: str, pid: str, ref_id: str,
                               start: date, end: date) -> list:
    date_range = f"{start.isoformat()},{end.isoformat()}"
    id_pairs = []
    if ref_id:
        id_pairs.append(("reference_property_id", ref_id))
    id_pairs += [("property_id", pid), ("home_id", pid)]
    for key, val in id_pairs:
        try:
            r = requests.get(
                f"{BW_BASE}/public/inventory/v1/task/",
                headers={"Authorization": f"JWT {token}"},
                params={"scheduled_date": date_range, key: val, "limit": 200},
                timeout=15,
            )
            if r.status_code == 200:
                body    = r.json()
                results = body.get("results", body.get("data",
                          body if isinstance(body, list) else []))
                if results:
                    return results
        except Exception:
            pass
    return []


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


def _lease_prep_scan_inner(mode="pre"):
    token = _get_token()
    if not token:
        return jsonify({"error": "Breezeway not configured."})

    from routes.briefing import _get_live_ref_cache, _ensure_property_cache
    _ensure_property_cache()

    # Date span to scan. Pre mode scans lease ARRIVALS (checkin); post mode scans
    # lease DEPARTURES (checkout). Defaults to today → +30 days, but the user can
    # pick a shorter (or different) range. End before start is swapped.
    today = date.today()

    def _parse_date(val, default):
        try:
            return date.fromisoformat(str(val)[:10])
        except (ValueError, TypeError):
            return default

    payload = request.get_json(silent=True) or {}
    start   = _parse_date(payload.get("from"), today)
    end     = _parse_date(payload.get("to"),   today + timedelta(days=30))
    if end < start:
        start, end = end, start

    # Step A: fetch reservations — by checkin (pre) or checkout (post) date
    date_field = "checkout" if mode == "post" else "checkin"
    try:
        reservations = _fetch_reservations(token, start, end, date_field=date_field)
    except Exception as e:
        import traceback; return jsonify({"error": f"STEP A (fetch reservations): {e}\n{traceback.format_exc()}"})

    # Step B: classify leases
    try:
        leases = [r for r in reservations if _is_lease(r)]
    except Exception as e:
        import traceback; return jsonify({"error": f"STEP B (classify leases): {e}\n{traceback.format_exc()}"})

    range_used = {"from": start.isoformat(), "to": end.isoformat()}
    if not leases:
        return jsonify({"leases": [], "range": range_used})

    ref_cache = _get_live_ref_cache()

    # Step C: fetch tasks per lease.
    #   pre  → the 30 days BEFORE arrival, up to the checkin day.
    #   post → the checkout (departure) day and the 30 days AFTER it.
    def fetch_lease_tasks(r):
        pid = str(r.get("property_id") or r.get("home_id") or "")
        if mode == "post":
            anchor_raw = r.get("checkout_date") or ""
        else:
            anchor_raw = r.get("checkin_date") or ""
        anchor_str = str(anchor_raw)[:10]
        try:
            anchor = date.fromisoformat(anchor_str)
        except ValueError:
            return r, [], None
        if mode == "post":
            window_start, window_end = anchor, anchor + timedelta(days=30)
        else:
            window_start, window_end = anchor - timedelta(days=30), anchor
        tasks = _fetch_tasks_for_property(
            token, pid, ref_cache.get(pid, ""), window_start, window_end
        )
        has_hot_tub = _property_has_hot_tub(token, pid)
        return r, tasks, has_hot_tub

    try:
        pairs = list(ThreadPoolExecutor(max_workers=8).map(fetch_lease_tasks, leases))
    except Exception as e:
        import traceback; return jsonify({"error": f"STEP C (fetch tasks): {e}\n{traceback.format_exc()}"})

    # Step D: build results
    results = []
    for reservation, tasks, has_hot_tub in pairs:
        try:
            pid      = str(reservation.get("property_id") or reservation.get("home_id") or "")
            checkin  = str(reservation.get("checkin_date")  or "")[:10]
            checkout = str(reservation.get("checkout_date") or "")[:10]
            guest = _guest_from_reservation(reservation)
            try:
                nights = (date.fromisoformat(checkout) - date.fromisoformat(checkin)).days
            except ValueError:
                nights = None

            fmt_tasks = []
            for t in tasks:
                try:
                    fmt_tasks.append(_fmt_task(t))
                except Exception as e:
                    import traceback
                    fmt_tasks.append({"title": f"[ERROR formatting task: {e}]",
                                      "sched_date": None, "sched_time": None,
                                      "done_at": None, "status": "pending",
                                      "assignees": [], "debug": traceback.format_exc()[-300:]})

            fmt_tasks.sort(key=lambda x: (x["sched_date"] or ""))
            results.append({
                "pid":         pid,
                "property":    _get_property_name(pid),
                "checkin":     checkin,
                "checkout":    checkout,
                "nights":      nights,
                "guest":       guest,
                "has_hot_tub": has_hot_tub,
                "info":        _reservation_info(reservation),
                "raw":         reservation,
                "tasks":       fmt_tasks,
            })
        except Exception as e:
            import traceback
            results.append({"property": f"[ERROR: {e}]", "checkin": "", "checkout": "",
                            "nights": None, "guest": "", "tasks": [],
                            "debug": traceback.format_exc()[-300:]})

    results.sort(key=lambda x: x["checkin"])
    return jsonify({"leases": results, "range": range_used})
