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

from flask import Blueprint, render_template, jsonify
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


def _classify(r: dict) -> str:
    from routes.briefing import _classify_reservation
    return _classify_reservation(r)


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


def _fmt_task(t: dict) -> dict:
    """Normalize a raw Breezeway task into a clean dict for the frontend."""
    title = t.get("title") or t.get("name") or ""
    if isinstance(title, dict):
        title = title.get("value") or title.get("name") or ""

    raw_status = (t.get("type_task_status") or t.get("status") or
                  t.get("state") or "").lower().strip()
    if raw_status in ("complete", "completed", "done", "finished"):
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

    sched_date = t.get("scheduled_date") or ""
    sched_time = t.get("scheduled_time") or ""
    done_at    = (t.get("finished_at") or t.get("completed_at") or
                  t.get("completed_date") or "")

    return {
        "title":      title,
        "sched_date": sched_date[:10] if sched_date else None,
        "sched_time": sched_time[:5]  if sched_time else None,
        "done_at":    done_at[:16]    if done_at    else None,
        "status":     status,
        "assignees":  assignees,
    }


@lease_prep_bp.route("/admin/lease-prep")
@login_required
@admin_required
def lease_prep_page():
    return render_template("lease_prep.html")


@lease_prep_bp.route("/admin/lease-prep/scan", methods=["POST"])
@login_required
@admin_required
def lease_prep_scan():
    token = _get_token()
    if not token:
        return jsonify({"error": "Breezeway not configured."}), 500

    from routes.briefing import _get_live_ref_cache

    today    = date.today()
    horizon  = today + timedelta(days=30)

    # Step 1: fetch all reservations arriving in the next 30 days
    reservations = _fetch_reservations(token, today, horizon)

    # Step 2: keep only leases
    leases = [r for r in reservations if _classify(r) == "lease"]
    if not leases:
        return jsonify({"leases": []})

    # Step 3: for each lease, fetch tasks in the 30 days prior to arrival
    ref_cache = _get_live_ref_cache()

    def fetch_lease_tasks(r):
        pid      = str(r.get("property_id") or r.get("home_id") or "")
        checkin  = r.get("checkin_date", "")[:10]
        try:
            arrival = date.fromisoformat(checkin)
        except ValueError:
            return r, []
        window_start = arrival - timedelta(days=30)
        tasks = _fetch_tasks_for_property(
            token, pid, ref_cache.get(pid, ""), window_start, arrival
        )
        return r, tasks

    results = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        for reservation, tasks in ex.map(fetch_lease_tasks, leases):
            pid       = str(reservation.get("property_id") or
                            reservation.get("home_id") or "")
            checkin   = reservation.get("checkin_date", "")[:10]
            checkout  = reservation.get("checkout_date", "")[:10]
            guest     = (reservation.get("guest_name") or
                         reservation.get("primary_guest") or
                         reservation.get("name") or "")
            if isinstance(guest, dict):
                guest = (guest.get("name") or
                         f"{guest.get('first_name','')} {guest.get('last_name','')}".strip())

            # Duration
            try:
                nights = (date.fromisoformat(checkout) -
                          date.fromisoformat(checkin)).days
            except ValueError:
                nights = None

            fmt_tasks = sorted(
                [_fmt_task(t) for t in tasks],
                key=lambda x: (x["sched_date"] or "")
            )

            results.append({
                "property":  _get_property_name(pid),
                "checkin":   checkin,
                "checkout":  checkout,
                "nights":    nights,
                "guest":     guest,
                "tasks":     fmt_tasks,
            })

    results.sort(key=lambda x: x["checkin"])
    return jsonify({"leases": results})
