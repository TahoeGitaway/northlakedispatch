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

from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required

from routes.auth import admin_required
from routes.group_assign import _assignee_names
from routes.bw_api_log import bw_get, bw_patch

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


# The per-property task sweep and the reservation pull used to live here as private
# copies that swallowed every failure as an empty list — a throttled house was
# indistinguishable from a house with no bear fence task, and a 429 on page 3 of the
# reservations silently truncated the candidate list. Both now come from briefing.py,
# which reports what it could not read, so this tool inherits those fixes instead of
# needing them applied a third time.
def _fetch_tasks_for_pids(token: str, pids: list, start: date, end: date) -> tuple:
    """(tasks, failed_count, failure_statuses) — see briefing.fetch_tasks_for_pids."""
    from routes.briefing import fetch_tasks_for_pids
    return fetch_tasks_for_pids(token, pids, start, end)


def _fetch_reservations_range(token: str, start: date, end: date) -> tuple:
    """(reservations, error) — error is non-empty when the set is INCOMPLETE.

    Incompleteness matters more here than almost anywhere: the arrivals in this
    range are what decides which properties get scanned at all, so a truncated pull
    drops houses out of the proposals with nothing on screen to say so.
    """
    from routes.briefing import _fetch_bw_reservations_status
    return _fetch_bw_reservations_status(token, {
        "checkin_date_ge": start.isoformat(),
        "checkin_date_le": end.isoformat(),
    })


def _fetch_task_by_id(token: str, task_id) -> dict | None:
    """Fetch a single task's live record so we can confirm its real scheduled
    date right before patching. Returns None if it can't be read."""
    headers = {"Authorization": f"JWT {token}"}
    for path in (f"/public/inventory/v1/task/{task_id}",
                 f"/public/inventory/v1/task/{task_id}/"):
        try:
            r = bw_get(f"{BW_BASE}{path}", headers=headers, timeout=15)
            if r.status_code == 200:
                body = r.json()
                if isinstance(body, dict):
                    # Some endpoints wrap the record under "data"/"result".
                    return body.get("data") or body.get("result") or body
        except Exception:
            pass
    return None


def _live_scheduled_date(task: dict) -> str:
    sched = task.get("scheduled_date") or ""
    return str(sched)[:10]


def _patch_task(token: str, task_id, payload: dict, meta: dict = None) -> tuple:
    """meta carries the pre-write context (task name, property, current date) so the
    audit log can record what this replaced. Breezeway keeps no history of its own."""
    from routes.bw_audit import log_bw_write
    meta = meta or {}
    headers = {"Authorization": f"JWT {token}", "Content-Type": "application/json"}
    url = f"{BW_BASE}/public/inventory/v1/task/{task_id}"
    try:
        r = bw_patch(url, headers=headers, json=payload, timeout=15)
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
        log_bw_write("bear_fence", "scheduled_date", task_id=task_id,
                     task_name=meta.get("name"), property_name=meta.get("property"),
                     task_date=meta.get("old_date"), old_value=meta.get("old_date"),
                     new_value=payload.get("scheduled_date"), ok=ok, detail=msg)
        return ok, msg
    except Exception as e:
        log_bw_write("bear_fence", "scheduled_date", task_id=task_id,
                     task_name=meta.get("name"), property_name=meta.get("property"),
                     task_date=meta.get("old_date"), old_value=meta.get("old_date"),
                     new_value=payload.get("scheduled_date"), ok=False,
                     detail=f"{type(e).__name__}: {e}")
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

    # Fetch arrivals to narrow down which properties to scan. A short read here is
    # not a smaller day — it is a smaller SCAN, and every house it drops produces no
    # proposal at all, which reads as "nothing to change".
    reservations, resv_error = _fetch_reservations_range(token, start, end + timedelta(days=1))
    arrival_pids = list({
        str(r.get("property_id") or r.get("home_id") or "")
        for r in reservations
        if r.get("checkin_date")
    } - {""})

    tasks, failed_props, failure_statuses = (
        _fetch_tasks_for_pids(token, arrival_pids, start, end) if arrival_pids
        else ([], 0, {}))

    # Index tasks by pid
    walk_thrus:  dict[str, list[dict]] = {}
    bear_fences: dict[str, list[dict]] = {}
    hts_tasks:   dict[str, list[dict]] = {}  # Arrival Hot Tub Service

    for t in tasks:
        title = (t.get("title") or t.get("name") or "")
        if isinstance(title, dict):
            title = title.get("value") or title.get("name") or ""
        # The pid this task was FETCHED for, in the same id space the arrival
        # map is keyed in. Payload fields are the fallback, home_id first — the
        # precedence dispatch.py uses; property_id first is what broke the match.
        pid   = (t.get("_swept_pid")
                 or str(t.get("home_id") or t.get("property_id") or ""))
        sched = t.get("scheduled_date") or ""
        entry = {"title": title, "date": sched[:10], "id": t.get("id"),
                 "assignees": _assignee_names(t)}

        if BEAR_FENCE_PATTERN.search(title):
            bear_fences.setdefault(pid, []).append(entry)
        elif ARRIVAL_HTS_PATTERN.search(title):
            hts_tasks.setdefault(pid, []).append(entry)
        elif WALK_THRU_PATTERNS.search(title) and not BB_PREFIX.match(title):
            walk_thrus.setdefault(pid, []).append(entry)

    def _find_bf_match(bf_list, ref_date, max_gap_days=2):
        """Return the nearest bear fence task on or after ref_date within
        max_gap_days, or None. The gap cap avoids pairing a task with an
        unrelated arrival's bear fence (e.g. owner-stay hot tub services,
        which have no bear fence of their own)."""
        for bf in sorted(bf_list, key=lambda x: x["date"]):
            try:
                bf_d = date.fromisoformat(bf["date"])
                if bf_d >= ref_date:
                    return (bf, bf_d) if (bf_d - ref_date).days <= max_gap_days else (None, None)
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
                "assignees":        wt.get("assignees", []),
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
                "assignees":        hts.get("assignees", []),
                "bear_fence_title": bf_match["title"],
                "bear_fence_date":  bf_match["date"],
            })

    # Sort: by property then current date
    proposals.sort(key=lambda x: (x["property"], x["current_date"]))
    return jsonify({
        "proposals": proposals,
        # What this scan could NOT see. A proposal list is an argument from absence
        # twice over — a house with no bear fence task is skipped, and a house that
        # never loaded looks exactly the same — so an incomplete scan that says
        # nothing is indistinguishable from a clean one that found nothing.
        "failed_properties": failed_props,
        "failure_statuses":  failure_statuses,
        "scanned_properties": len(arrival_pids),
        # Non-empty when the arrivals themselves came back short, so the candidate
        # list was already incomplete before a single task was read.
        "reservations_error": resv_error,
    })


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
        task_id       = item["task_id"]
        expected_date = str(item.get("current_date") or "")[:10]
        target_date   = str(item["bear_fence_date"])[:10]

        # Safety check: re-read the task's LIVE scheduled date and confirm it still
        # matches the date the admin reviewed. If the task moved (or can't be read)
        # since the scan, skip it rather than risk moving the wrong day — this is
        # what prevents tasks landing on the wrong date when data went stale.
        live_task = _fetch_task_by_id(token, task_id)
        if live_task is None:
            results.append({
                "task_id": task_id, "property": item.get("property", ""),
                "task_title": item.get("task_title", ""), "bear_fence_date": target_date,
                "success": False,
                "detail": "⚠ skipped — couldn't re-read the task to verify its current date. Rescan and try again.",
            })
            continue

        live_date = _live_scheduled_date(live_task)
        if expected_date and live_date != expected_date:
            results.append({
                "task_id": task_id, "property": item.get("property", ""),
                "task_title": item.get("task_title", ""), "bear_fence_date": target_date,
                "success": False,
                "detail": f"⚠ skipped — task is now on {live_date}, not {expected_date} as shown. "
                          f"It changed since the scan; rescan to see the real dates.",
            })
            continue

        if live_date == target_date:
            results.append({
                "task_id": task_id, "property": item.get("property", ""),
                "task_title": item.get("task_title", ""), "bear_fence_date": target_date,
                "success": True,
                "detail": f"already on {target_date} — no change needed",
            })
            continue

        ok, msg = _patch_task(token, task_id, {"scheduled_date": target_date},
                              meta={"name": item.get("task_title", ""),
                                    "property": item.get("property", ""),
                                    "old_date": item.get("walk_thru_date") or item.get("current_date", "")})
        results.append({
            "task_id":         task_id,
            "property":        item.get("property", ""),
            "task_title":      item.get("task_title", ""),
            "bear_fence_date": target_date,
            "success":         ok,
            "detail":          f"moved {live_date} → {target_date} · {msg}",
        })

    return jsonify({"results": results})
