"""
routes/pri_rename.py — Post Rental Inspection (PRI) rename tool.

DELIBERATELY separate from the Walk Thru rename and the PRI Check — its own
blueprint and template so these features can never break one another.

Scans Breezeway for existing "Post Rental Inspection" tasks in a date range and,
for each task's property, finds the NEXT homeowner / hold / block arrival on or
after the task's scheduled date. Proposes renaming the task to
"Post Rental Inspection for M/D" (re-dating even ones that already carry a date).
Admin reviews and approves before anything changes.

Endpoints:
  GET  /admin/pri-rename        — page (a tab in the PRI workflow area)
  POST /admin/pri-rename/scan   — scan and return proposals (JSON)
  POST /admin/pri-rename/apply  — PATCH approved renames (JSON)
"""

import re
import requests
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required

from routes.auth import admin_required

pri_rename_bp = Blueprint("pri_rename", __name__)

BW_BASE = "https://api.breezeway.io"

# Per date-range scan cache — survives a proxy timeout so a retry is instant.
import time as _time
_scan_cache: dict = {}      # (start_iso, end_iso) -> (timestamp, result_dict)
_SCAN_TTL = 90

# How far ahead to look for the next owner/hold/block arrival — it can be well
# beyond the inspection's own date.
LOOKAHEAD_DAYS = 180

PRI_PATTERN   = re.compile(r"post[\s\-]?rental[\s\-]?inspection", re.IGNORECASE)
# Trailing date suffix to strip before re-dating: " for 6/22", " *6/22", " 6/22".
TRAILING_DATE = re.compile(r"\s*(?:for\s+|\*\s*)?\d{1,2}/\d{1,2}\s*$", re.IGNORECASE)
# Back-to-back marker: a "b/b " prefix on the title. These are usually left alone, so
# the UI tucks them into a separate collapsed section — see is_bb on each proposal.
BB_PREFIX     = re.compile(r"^\s*b\s*/\s*b\b", re.IGNORECASE)


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


def _strip_trailing_date(title: str) -> str:
    """Remove an existing 'for M/D' / '*M/D' / 'M/D' from the end of a title."""
    prev, out = None, title.strip()
    while out != prev:
        prev = out
        out = TRAILING_DATE.sub("", out).strip()
    return out


def _build_proposed_title(title: str, arrival: date) -> str:
    return f"{_strip_trailing_date(title)} for {arrival.month}/{arrival.day}"


def _fetch_tasks_for_property(token, pid, ref_id, start, end):
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
                params={"scheduled_date": date_range, key: val, "limit": 100},
                timeout=15,
            )
            if r.status_code == 200:
                body = r.json()
                results = body.get("results", body.get("data", body if isinstance(body, list) else []))
                if results:
                    return results
        except Exception:
            pass
    return []


def _fetch_tasks_for_pids(token, pids, start, end):
    from routes.briefing import _get_live_ref_cache
    ref_cache = _get_live_ref_cache()
    all_tasks, seen = [], set()
    with ThreadPoolExecutor(max_workers=16) as ex:
        futures = {ex.submit(_fetch_tasks_for_property, token, pid, ref_cache.get(pid, ""), start, end): pid
                   for pid in pids}
        for fut in as_completed(futures):
            for t in (fut.result() or []):
                tid = t.get("id")
                if tid is None or tid not in seen:
                    if tid is not None:
                        seen.add(tid)
                    all_tasks.append(t)
    return all_tasks


def _fetch_reservations_range(token, start, end):
    all_results, page = [], 1
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


def _patch_task_name(token, task_id, new_name):
    headers = {"Authorization": f"JWT {token}", "Content-Type": "application/json"}
    url = f"{BW_BASE}/public/inventory/v1/task/{task_id}"
    try:
        r = requests.patch(url, headers=headers, json={"name": new_name}, timeout=15)
        ok = r.status_code in (200, 201)
        try:
            returned = r.json().get("name") or "(not in response)"
            msg = f"status={r.status_code} name='{returned}'"
        except Exception:
            msg = f"status={r.status_code} body={r.text[:200]}"
        return ok, msg
    except Exception as e:
        return False, str(e)


@pri_rename_bp.route("/admin/pri-rename")
@login_required
@admin_required
def pri_rename_page():
    return render_template("pri_rename.html")


@pri_rename_bp.route("/admin/pri-rename/scan", methods=["POST"])
@login_required
@admin_required
def pri_rename_scan():
    from routes.briefing import _classify_reservation
    token = _get_token()
    if not token:
        return jsonify({"error": "Breezeway not configured."}), 500

    today = date.today()
    body  = request.get_json(silent=True) or {}
    try:
        start = date.fromisoformat(body["start"]) if "start" in body else today
        end   = date.fromisoformat(body["end"])   if "end"   in body else today + timedelta(days=30)
    except ValueError:
        start, end = today, today + timedelta(days=30)

    ck     = (start.isoformat(), end.isoformat())
    cached = _scan_cache.get(ck)
    if cached and not body.get("force") and _time.time() - cached[0] < _SCAN_TTL:
        return jsonify(cached[1])

    # Homeowner / hold / block arrivals across a wide forward window (the next such
    # arrival can be well past the inspection date). Holds fold into "block".
    reservations = _fetch_reservations_range(token, start, end + timedelta(days=LOOKAHEAD_DAYS))
    owner_arrivals = {}    # pid -> sorted [date]
    for r in reservations:
        if _classify_reservation(r) not in ("owner", "block"):
            continue
        pid     = str(r.get("property_id") or r.get("home_id") or "")
        checkin = r.get("checkin_date") or ""
        if pid and checkin:
            try:
                owner_arrivals.setdefault(pid, []).append(date.fromisoformat(checkin[:10]))
            except ValueError:
                pass
    for pid in owner_arrivals:
        owner_arrivals[pid].sort()

    # Only scan properties that actually have an upcoming owner/hold/block arrival.
    pids  = list(owner_arrivals.keys())
    tasks = _fetch_tasks_for_pids(token, pids, start, end) if pids else []

    proposals = []
    for t in tasks:
        title = (t.get("title") or t.get("name") or "")
        if isinstance(title, dict):
            title = title.get("value") or title.get("name") or ""
        if not PRI_PATTERN.search(title):
            continue

        task_id = t.get("id") or t.get("task_id")
        pid     = str(t.get("property_id") or t.get("home_id") or "")
        sched   = t.get("scheduled_date") or ""
        try:
            task_date = date.fromisoformat(sched[:10])
        except (ValueError, TypeError):
            continue

        arrival = next((d for d in owner_arrivals.get(pid, []) if d >= task_date), None)
        if not arrival:
            continue

        proposed = _build_proposed_title(title, arrival)
        if proposed == title.strip():
            continue   # already correctly dated — nothing to change

        proposals.append({
            "task_id":        task_id,
            "pid":            pid,
            "property":       _get_property_name(pid),
            "current_title":  title,
            "task_date":      sched[:10],
            "arrival_date":   arrival.isoformat(),
            "proposed_title": proposed,
            "is_bb":          bool(BB_PREFIX.match(title.strip())),
        })

    proposals.sort(key=lambda x: x["task_date"])
    result = {"proposals": proposals}
    _scan_cache[ck] = (_time.time(), result)
    return jsonify(result)


@pri_rename_bp.route("/admin/pri-rename/apply", methods=["POST"])
@login_required
@admin_required
def pri_rename_apply():
    token = _get_token()
    if not token:
        return jsonify({"error": "Breezeway not configured."}), 500

    items   = request.json.get("items", [])
    results = []
    for item in items:
        ok, msg = _patch_task_name(token, item["task_id"], item["proposed_title"])
        results.append({
            "task_id":        item["task_id"],
            "property":       item.get("property", ""),
            "proposed_title": item["proposed_title"],
            "success":        ok,
            "detail":         msg,
        })
    _scan_cache.clear()   # names changed — next scan should be fresh
    return jsonify({"results": results})
