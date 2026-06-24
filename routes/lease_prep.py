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


def _fetch_reservations(token: str, start: date, end: date) -> list:
    all_results = []
    page = 1
    while True:
        try:
            r = requests.get(
                f"{BW_BASE}/public/inventory/v1/reservation",
                headers={"Authorization": f"JWT {token}"},
                params={"checkin_date_ge": start.isoformat(),
                        "checkin_date_le": end.isoformat(),
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
        return _lease_prep_scan_inner()
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        return jsonify({"error": f"{type(e).__name__}: {e}\n\n{tb}"}), 500


def _lease_prep_scan_inner():
    token = _get_token()
    if not token:
        return jsonify({"error": "Breezeway not configured."})

    from routes.briefing import _get_live_ref_cache, _ensure_property_cache
    _ensure_property_cache()

    # Date span to scan for lease ARRIVALS. Defaults to today → +30 days, but the
    # user can pick a shorter (or different) range. End before start is swapped.
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

    # Step A: fetch reservations
    try:
        reservations = _fetch_reservations(token, start, end)
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

    # Step C: fetch tasks per lease
    def fetch_lease_tasks(r):
        pid = str(r.get("property_id") or r.get("home_id") or "")
        checkin_raw = r.get("checkin_date") or ""
        checkin = str(checkin_raw)[:10]
        try:
            arrival = date.fromisoformat(checkin)
        except ValueError:
            return r, [], None
        window_start = arrival - timedelta(days=30)
        tasks = _fetch_tasks_for_property(
            token, pid, ref_cache.get(pid, ""), window_start, arrival
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
            guest_raw = (reservation.get("guest_name") or reservation.get("primary_guest") or
                         reservation.get("name") or "")
            if isinstance(guest_raw, dict):
                guest = str(guest_raw.get("name") or
                            f"{guest_raw.get('first_name','')} {guest_raw.get('last_name','')}".strip())
            else:
                guest = str(guest_raw)
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
                "property":    _get_property_name(pid),
                "checkin":     checkin,
                "checkout":    checkout,
                "nights":      nights,
                "guest":       guest,
                "has_hot_tub": has_hot_tub,
                "tasks":       fmt_tasks,
            })
        except Exception as e:
            import traceback
            results.append({"property": f"[ERROR: {e}]", "checkin": "", "checkout": "",
                            "nights": None, "guest": "", "tasks": [],
                            "debug": traceback.format_exc()[-300:]})

    results.sort(key=lambda x: x["checkin"])
    return jsonify({"leases": results, "range": range_used})
