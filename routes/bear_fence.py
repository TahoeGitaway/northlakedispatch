"""
routes/bear_fence.py — Bear Fence date change tool.

Scans Breezeway for Walk Thru tasks that have a matching "Disarm Bear Fence"
task at the same property. If the Walk Thru's scheduled date differs from the
bear fence task's date, proposes moving the Walk Thru to the bear fence date.
Admin reviews and approves before anything is changed.

Endpoints:
  GET  /admin/bear-fence          — page
  POST /admin/bear-fence/scan     — scan and return proposals (JSON)
  POST /admin/bear-fence/apply    — PATCH approved date changes (JSON)
"""

import re
import requests
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required

from routes.auth import admin_required

bear_fence_bp = Blueprint("bear_fence", __name__)

BW_BASE = "https://api.breezeway.io"

WALK_THRU_PATTERNS = re.compile(
    r"\b(walk[\s\-]?thru|walk[\s\-]?through|lease[\s\-]?walk|move[\s\-]?in[\s\-]?inspection|arrival[\s\-]?task|guest[\s\-]?arrival)\b",
    re.IGNORECASE,
)
BB_PREFIX           = re.compile(r"^b/b\s+", re.IGNORECASE)
BEAR_FENCE_PATTERN  = re.compile(r"disarm[\s\-]*bear[\s\-]*fence", re.IGNORECASE)
ARRIVAL_HTS_PATTERN = re.compile(r"arrival[\s\-]+hot[\s\-]*tub", re.IGNORECASE)


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


def _fetch_tasks_for_property(token: str, pid: str, ref_id: str, start: date, end: date) -> list:
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


def _fetch_tasks_for_pids(token: str, pids: list, start: date, end: date) -> list:
    from routes.briefing import _get_live_ref_cache
    ref_cache = _get_live_ref_cache()
    all_tasks = []
    seen_ids: set = set()
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(_fetch_tasks_for_property, token, pid, ref_cache.get(pid, ""), start, end): pid
                   for pid in pids}
        for future in as_completed(futures):
            for t in (future.result() or []):
                tid = t.get("id")
                if tid is None or tid not in seen_ids:
                    if tid is not None:
                        seen_ids.add(tid)
                    all_tasks.append(t)
    return all_tasks


def _fetch_reservations_range(token: str, start: date, end: date) -> list:
    all_results = []
    page = 1
    while True:
        try:
            r = requests.get(
                f"{BW_BASE}/public/inventory/v1/reservation",
                headers={"Authorization": f"JWT {token}"},
                params={"checkin_date_ge": start.isoformat(), "checkin_date_le": end.isoformat(),
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


def _patch_task(token: str, task_id, payload: dict) -> tuple:
    headers = {"Authorization": f"JWT {token}", "Content-Type": "application/json"}
    url = f"{BW_BASE}/public/inventory/v1/task/{task_id}"
    try:
        r = requests.patch(url, headers=headers, json=payload, timeout=15)
        ok = r.status_code in (200, 201)
        try:
            body = r.json()
            returned_date = body.get("scheduled_date") or "(not in response)"
            sent_date     = payload.get("scheduled_date", "")
            if ok:
                match = "✓ confirmed" if returned_date[:10] == sent_date[:10] else f"⚠ returned {returned_date} (expected {sent_date})"
                msg = f"status={r.status_code} {match}"
            else:
                msg = f"status={r.status_code} body={r.text[:300]}"
        except Exception:
            msg = f"status={r.status_code} body={r.text[:200]}"
        return ok, msg
    except Exception as e:
        return False, str(e)


@bear_fence_bp.route("/admin/bear-fence")
@login_required
@admin_required
def bear_fence_page():
    return render_template("bear_fence.html")


@bear_fence_bp.route("/admin/bear-fence/scan", methods=["POST"])
@login_required
@admin_required
def bear_fence_scan():
    token = _get_token()
    if not token:
        return jsonify({"error": "Breezeway not configured."}), 500

    today = date.today()
    body  = request.get_json(silent=True) or {}
    try:
        start = date.fromisoformat(body["start"]) if "start" in body else today
        end   = date.fromisoformat(body["end"])   if "end"   in body else today + timedelta(days=7)
    except ValueError:
        start, end = today, today + timedelta(days=7)

    # Fetch arrivals to narrow down which properties to scan
    reservations = _fetch_reservations_range(token, start, end + timedelta(days=1))
    arrival_pids = list({
        str(r.get("property_id") or r.get("home_id") or "")
        for r in reservations
        if r.get("checkin_date")
    } - {""})

    tasks = _fetch_tasks_for_pids(token, arrival_pids, start, end) if arrival_pids else []

    # Index tasks by pid
    walk_thrus:  dict[str, list[dict]] = {}
    bear_fences: dict[str, list[dict]] = {}
    hts_tasks:   dict[str, list[dict]] = {}  # Arrival Hot Tub Service

    for t in tasks:
        title = (t.get("title") or t.get("name") or "")
        if isinstance(title, dict):
            title = title.get("value") or title.get("name") or ""
        pid   = str(t.get("property_id") or t.get("home_id") or "")
        sched = t.get("scheduled_date") or ""
        entry = {"title": title, "date": sched[:10], "id": t.get("id")}

        if BEAR_FENCE_PATTERN.search(title):
            bear_fences.setdefault(pid, []).append(entry)
        elif ARRIVAL_HTS_PATTERN.search(title):
            hts_tasks.setdefault(pid, []).append(entry)
        elif WALK_THRU_PATTERNS.search(title) and not BB_PREFIX.match(title):
            walk_thrus.setdefault(pid, []).append(entry)

    def _find_bf_match(bf_list, ref_date):
        """Return the nearest bear fence task on or after ref_date, or None."""
        for bf in sorted(bf_list, key=lambda x: x["date"]):
            try:
                bf_d = date.fromisoformat(bf["date"])
                if bf_d >= ref_date:
                    return bf, bf_d
            except (ValueError, TypeError):
                pass
        return None, None

    proposals = []

    # All property IDs that appear in either walk_thrus or hts_tasks
    candidate_pids = set(walk_thrus.keys()) | set(hts_tasks.keys())

    for pid in candidate_pids:
        bf_list = bear_fences.get(pid)
        if not bf_list:
            continue

        prop_name = _get_property_name(pid)

        # Walk Thru tasks
        for wt in walk_thrus.get(pid, []):
            try:
                wt_date = date.fromisoformat(wt["date"])
            except (ValueError, TypeError):
                continue
            bf_match, bf_d = _find_bf_match(bf_list, wt_date)
            if not bf_match or bf_d == wt_date:
                continue
            proposals.append({
                "task_id":          wt["id"],
                "property":         prop_name,
                "task_title":       wt["title"],
                "task_type":        "Walk Thru",
                "current_date":     wt["date"],
                "bear_fence_title": bf_match["title"],
                "bear_fence_date":  bf_match["date"],
            })

        # Arrival Hot Tub Service tasks
        for hts in hts_tasks.get(pid, []):
            try:
                hts_date = date.fromisoformat(hts["date"])
            except (ValueError, TypeError):
                continue
            bf_match, bf_d = _find_bf_match(bf_list, hts_date)
            if not bf_match or bf_d == hts_date:
                continue
            proposals.append({
                "task_id":          hts["id"],
                "property":         prop_name,
                "task_title":       hts["title"],
                "task_type":        "Arrival Hot Tub Service",
                "current_date":     hts["date"],
                "bear_fence_title": bf_match["title"],
                "bear_fence_date":  bf_match["date"],
            })

    # Sort: by property then current date
    proposals.sort(key=lambda x: (x["property"], x["current_date"]))
    return jsonify({"proposals": proposals})


@bear_fence_bp.route("/admin/bear-fence/apply", methods=["POST"])
@login_required
@admin_required
def bear_fence_apply():
    token = _get_token()
    if not token:
        return jsonify({"error": "Breezeway not configured."}), 500

    items   = request.json.get("items", [])
    results = []
    for item in items:
        ok, msg = _patch_task(token, item["task_id"], {"scheduled_date": item["bear_fence_date"]})
        results.append({
            "task_id":         item["task_id"],
            "property":        item.get("property", ""),
            "task_title":      item.get("task_title", ""),
            "bear_fence_date": item["bear_fence_date"],
            "success":         ok,
            "detail":          msg,
        })

    return jsonify({"results": results})
