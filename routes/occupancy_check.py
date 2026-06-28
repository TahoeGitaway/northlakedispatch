"""
routes/occupancy_check.py — "Serviced while occupied" check.

For ONE selected day: find every property that has a guest or long-term
tenant in the house that day, then list the Breezeway tasks scheduled there
that day. Everything is pulled live from Breezeway; nothing is cached or
written to the DB.

Occupied on day D = a guest/lease reservation with checkin < D < checkout
(strictly mid-stay, so neither the arrival nor the departure/turnover day
counts as "someone in the house"). Owner stays and blocks are ignored.

Tasks are split into:
  • unexpected — the surprises (default for any task)
  • expected   — work that is normally fine during a stay (hot tub service,
                 mid-stay clean, trash valet, inspection, …) — see
                 _EXPECTED_KEYWORDS, which is meant to be edited freely.

Endpoints:
  GET /briefing/occupancy-check?date=YYYY-MM-DD   — scan one day, returns JSON
  GET /admin/occupancy-check                      — the scan page
"""

import time as _time
from datetime import date as date_cls
from concurrent.futures import ThreadPoolExecutor

from flask import Blueprint, jsonify, request, render_template
from flask_login import login_required

occupancy_bp = Blueprint("occupancy", __name__)


# ── Task-title classification ─────────────────────────────────────
# Title keywords for work that is NORMALLY fine while a guest/tenant is in the
# house. Anything not matching is treated as an "unexpected" overlap — the
# surprise she actually wants surfaced. Edit this list freely; matching is a
# simple case-insensitive substring test.
_EXPECTED_KEYWORDS = [
    "hot tub", "hottub", "spa",
    "pool",
    "mid-stay", "mid stay", "midstay", "in-stay", "in stay",
    "trash", "valet", "garbage", "recycl",
    "inspection", "inspect", "walk thru", "walk-thru", "walkthru",
    "landscap", "yard", "lawn", "snow", "plow", "shovel",
    "restock", "replenish", "deliver", "drop off", "drop-off",
    "firewood", "propane",
    "guest request", "guest-request", "maintenance request", "guest report",
]


def _expected_reason(title: str):
    """Return the matched keyword if the task title looks like normal in-stay
    work, else None (→ unexpected)."""
    t = (title or "").lower()
    for kw in _EXPECTED_KEYWORDS:
        if kw in t:
            return kw
    return None


def _task_title(t: dict) -> str:
    title = (t.get("name") or t.get("task_name") or t.get("task_type") or t.get("type") or "Task")
    if isinstance(title, dict):
        title = title.get("value") or title.get("name") or "Task"
    return str(title).strip()


def _task_status(t: dict) -> str:
    # Breezeway sends type_task_status as a coded dict ({"code": ..., "name": ...}),
    # not a plain string — pull a string out of whatever shape arrives.
    val = t.get("type_task_status") or t.get("status") or t.get("state") or ""
    if isinstance(val, dict):
        val = val.get("code") or val.get("name") or val.get("value") or ""
    raw = str(val).lower().strip()
    if raw in ("complete", "completed", "done", "finished"):
        return "complete"
    if raw in ("in_progress", "in progress", "started"):
        return "in_progress"
    return "pending"


def _task_assignees(t: dict) -> list:
    out = []
    for a in (t.get("assignments") or []):
        if not isinstance(a, dict):
            continue
        n = (a.get("full_name") or a.get("name") or
             f"{a.get('first_name','').strip()} {a.get('last_name','').strip()}".strip())
        if n:
            out.append(n)
    return out


def _robust_property_tasks(token, ref_id, date_str):
    """Fetch ONE property's tasks for a single day with retry/backoff, so a
    momentary Breezeway throttle (429 / 5xx) doesn't silently drop the property.
    Returns (tasks, ok); ok=False means it genuinely couldn't be loaded."""
    from routes.briefing import _fetch_bw_endpoint
    for attempt in range(3):
        r, _, status = _fetch_bw_endpoint(
            token, "/public/inventory/v1/task",
            {"reference_property_id": ref_id, "scheduled_date": f"{date_str},{date_str}"})
        if status == 200:
            return (r or [], True)
        if status is None or status == 429 or status >= 500:
            _time.sleep(0.3 * (attempt + 1))
            continue
        r2, _, st2 = _fetch_bw_endpoint(
            token, "/public/inventory/v1/task",
            {"reference_property_id": ref_id, "start_date": date_str, "end_date": date_str})
        return (r2 or [], True) if st2 == 200 else ([], False)
    return ([], False)


@occupancy_bp.route("/briefing/occupancy-check")
@login_required
def occupancy_check():
    """For the selected day, list tasks scheduled where a guest/tenant is in the house."""
    from routes.briefing import (
        _get_breezeway_token, _fetch_bw_reservations, _classify_reservation,
        _get_property_name, _get_live_ref_cache, _guest_name, _ensure_property_cache,
    )

    date_param = request.args.get("date")
    try:
        day = date_cls.fromisoformat(date_param) if date_param else date_cls.today()
    except Exception:
        day = date_cls.today()
    day_str = day.isoformat()

    token = _get_breezeway_token()
    if not token:
        return jsonify({"error": "Breezeway not configured."}), 500
    _ensure_property_cache()

    # Reservations spanning the selected day: checked in on/before it AND
    # checking out on/after it. Filtering BOTH ends server-side keeps this to a
    # few hundred records. The old 370-day look-back (checkin only) pulled ~7700
    # reservations / 78 API pages — ~5 min per call, which blew the request
    # timeout and returned a 500 every time. This span query is ~50x faster and
    # returns the identical occupied set.
    raw = _fetch_bw_reservations(token, {
        "checkin_date_le": day_str,
        "checkout_date_ge": day_str,
    })

    # property_id -> the guest/lease stay occupying it that day
    occupied = {}
    for r in raw:
        kind = _classify_reservation(r)
        if kind not in ("guest", "lease"):
            continue  # owners / blocks aren't "someone in the house" here
        pid = r.get("property_id")
        if not pid:
            continue
        ci = (r.get("checkin_date") or "")[:10]
        co = (r.get("checkout_date") or "")[:10]
        try:
            ci_d = date_cls.fromisoformat(ci)
            co_d = date_cls.fromisoformat(co)
        except Exception:
            continue
        # Strictly mid-stay: in the house the night of D, not arriving/departing that day.
        if ci_d < day < co_d:
            occupied[pid] = {"ci": ci, "co": co, "kind": kind, "guest": _guest_name(r)}

    if not occupied:
        return jsonify({
            "date": day_str, "unexpected": [], "expected": [],
            "occupied_properties": 0, "failed_properties": 0,
        })

    ref_cache = _get_live_ref_cache()

    def _job(pid):
        ref = ref_cache.get(pid) or str(pid)
        tasks, ok = _robust_property_tasks(token, ref, day_str)
        return pid, tasks, ok

    failed   = 0
    overlaps = []
    with ThreadPoolExecutor(max_workers=16) as ex:
        for pid, tasks, ok in ex.map(_job, list(occupied.keys())):
            if not ok:
                failed += 1
                continue
            stay      = occupied[pid]
            prop_name = _get_property_name(pid)
            for t in tasks:
                t_date = (t.get("scheduled_date") or "")[:10]
                # Strict date match — only tasks ON the selected day. Breezeway
                # occasionally returns off-date tasks; drop them.
                if t_date and t_date != day_str:
                    continue
                title  = _task_title(t)
                reason = _expected_reason(title)
                overlaps.append({
                    "property":        prop_name,
                    "task":            title,
                    "guest":           stay["guest"],
                    "kind":            stay["kind"],     # guest | lease
                    "checkin":         stay["ci"],
                    "checkout":        stay["co"],
                    "assignees":       _task_assignees(t),
                    "status":          _task_status(t),  # pending | in_progress | complete
                    "expected":        bool(reason),
                    "expected_reason": reason or "",
                })

    overlaps.sort(key=lambda o: (o["property"], o["task"]))
    return jsonify({
        "date":                day_str,
        "unexpected":          [o for o in overlaps if not o["expected"]],
        "expected":            [o for o in overlaps if o["expected"]],
        "occupied_properties": len(occupied),
        "failed_properties":   failed,
    })


@occupancy_bp.route("/admin/occupancy-check")
@login_required
def occupancy_check_page():
    return render_template("occupancy_check.html")
