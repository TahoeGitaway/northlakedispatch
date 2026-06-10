"""
routes/walk_thru_rename.py — Walk Thru task rename tool.

Scans Breezeway for Walk Thru tasks (today → +30 days) that don't already
have a date in the name, finds the next reservation arrival on or after each
task's scheduled date for that property, and proposes a rename like
"Walk Thru for 6/15". Admin reviews and approves before anything is changed.

Two endpoints:
  GET  /admin/walk-thru-rename        — page
  POST /admin/walk-thru-rename/scan   — scan and return proposals (JSON)
  POST /admin/walk-thru-rename/apply  — PATCH approved renames (JSON)
"""

import re
import requests
from datetime import date, timedelta

from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required

from routes.auth import admin_required

walk_thru_bp = Blueprint("walk_thru", __name__)

BW_BASE = "https://api.breezeway.io"

WALK_THRU_PATTERNS = re.compile(
    r"\b(walk[\s\-]?thru|walk[\s\-]?through|lease[\s\-]?walk|move[\s\-]?in[\s\-]?inspection|arrival[\s\-]?task|guest[\s\-]?arrival)\b",
    re.IGNORECASE,
)
ALREADY_DATED = re.compile(r"\bfor\s+\d{1,2}/\d{1,2}\b", re.IGNORECASE)


def _get_token():
    from routes.briefing import _get_breezeway_token
    return _get_breezeway_token()


def _get_property_name(pid):
    from routes.briefing import _get_live_property_cache
    return _get_live_property_cache().get(pid, str(pid))


def _fetch_tasks_range(token: str, start: date, end: date) -> list:
    """Fetch all Breezeway tasks in a date range."""
    params = {
        "scheduled_date": f"{start.isoformat()},{end.isoformat()}",
        "limit": 200,
    }
    try:
        r = requests.get(
            f"{BW_BASE}/public/inventory/v1/task/",
            headers={"Authorization": f"JWT {token}"},
            params=params,
            timeout=20,
        )
        if r.status_code == 200:
            body = r.json()
            return body.get("results", body.get("data", body if isinstance(body, list) else []))
    except Exception:
        pass
    return []


def _fetch_reservations_range(token: str, start: date, end: date) -> list:
    """Fetch all Breezeway reservations with checkin in a date range."""
    all_results = []
    page = 1
    while True:
        try:
            r = requests.get(
                f"{BW_BASE}/public/inventory/v1/reservation",
                headers={"Authorization": f"JWT {token}"},
                params={
                    "checkin_date_ge": start.isoformat(),
                    "checkin_date_le": end.isoformat(),
                    "limit": 100,
                    "page": page,
                },
                timeout=20,
            )
            if r.status_code != 200:
                break
            body = r.json()
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


def _patch_task_title(token: str, task_id, new_title: str) -> tuple[bool, str]:
    """PATCH a Breezeway task's title. Tries 'title' then 'name'."""
    headers = {"Authorization": f"JWT {token}", "Content-Type": "application/json"}
    url = f"{BW_BASE}/public/inventory/v1/task/{task_id}"
    for payload in [{"title": new_title}, {"name": new_title}]:
        try:
            r = requests.patch(url, headers=headers, json=payload, timeout=15)
            if r.status_code in (200, 201):
                return True, f"status={r.status_code}"
            # try next payload variant
        except Exception as e:
            return False, str(e)
    try:
        return False, f"status={r.status_code} body={r.text[:200]}"
    except Exception:
        return False, "all payload variants failed"


@walk_thru_bp.route("/admin/walk-thru-rename")
@login_required
@admin_required
def walk_thru_page():
    return render_template("walk_thru_rename.html")


@walk_thru_bp.route("/admin/walk-thru-rename/scan", methods=["POST"])
@login_required
@admin_required
def walk_thru_scan():
    token = _get_token()
    if not token:
        return jsonify({"error": "Breezeway not configured."}), 500

    today = date.today()
    end   = today + timedelta(days=30)

    tasks        = _fetch_tasks_range(token, today, end)
    reservations = _fetch_reservations_range(token, today, end + timedelta(days=7))

    # Index reservations by property_id → sorted list of checkin dates
    reso_by_prop: dict[str, list[date]] = {}
    for r in reservations:
        pid      = str(r.get("property_id") or r.get("home_id") or "")
        checkin  = r.get("checkin_date") or ""
        if pid and checkin:
            try:
                d = date.fromisoformat(checkin[:10])
                reso_by_prop.setdefault(pid, []).append(d)
            except ValueError:
                pass
    for pid in reso_by_prop:
        reso_by_prop[pid].sort()

    proposals = []
    for t in tasks:
        title = (t.get("title") or t.get("name") or "")
        if isinstance(title, dict):
            title = title.get("value") or title.get("name") or ""

        if not WALK_THRU_PATTERNS.search(title):
            continue
        if ALREADY_DATED.search(title):
            continue

        task_id  = t.get("id") or t.get("task_id")
        pid      = str(t.get("property_id") or t.get("home_id") or "")
        sched    = t.get("scheduled_date") or ""
        try:
            task_date = date.fromisoformat(sched[:10])
        except (ValueError, TypeError):
            continue

        arrivals = reso_by_prop.get(pid, [])
        arrival  = next((d for d in arrivals if d >= task_date), None)
        if not arrival:
            continue

        prop_name    = _get_property_name(pid)
        proposed     = f"{title} for {arrival.month}/{arrival.day}"
        proposals.append({
            "task_id":       task_id,
            "property":      prop_name,
            "current_title": title,
            "task_date":     sched[:10],
            "arrival_date":  arrival.isoformat(),
            "proposed_title": proposed,
        })

    proposals.sort(key=lambda x: x["task_date"])
    return jsonify({"proposals": proposals})


@walk_thru_bp.route("/admin/walk-thru-rename/apply", methods=["POST"])
@login_required
@admin_required
def walk_thru_apply():
    token = _get_token()
    if not token:
        return jsonify({"error": "Breezeway not configured."}), 500

    items = request.json.get("items", [])
    results = []
    for item in items:
        ok, msg = _patch_task_title(token, item["task_id"], item["proposed_title"])
        results.append({
            "task_id":       item["task_id"],
            "property":      item.get("property", ""),
            "proposed_title": item["proposed_title"],
            "success":       ok,
            "detail":        msg,
        })

    return jsonify({"results": results})
