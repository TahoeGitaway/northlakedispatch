"""
routes/hot_tub.py — Hot Tub service overdue scanner.

Only applies to properties tagged "Hot Tub - TG Service" in Breezeway.
Looks back 45 days for tasks whose title contains "hot tub" AND
("arrival" OR "biweekly"). Alerts on any property where the last
service was more than 14 days ago (or never found in the window).

Endpoints:
  GET  /admin/hot-tub        — page
  POST /admin/hot-tub/scan   — scan and return results (JSON)
"""

import re
import requests
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required

from routes.auth import admin_required

hot_tub_bp = Blueprint("hot_tub", __name__)

BW_BASE = "https://api.breezeway.io"

HOT_TUB_PATTERN = re.compile(
    r"(?=.*\bhot[\s\-]?tub\b)(?=.*\b(arrival|biweekly|bi[\s\-]?weekly|lease|d\s*&\s*s)\b)",
    re.IGNORECASE,
)
HOT_TUB_TAG_NAME = "hot tub - tg service"


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


def _fetch_hot_tub_tag_id(token: str) -> int | None:
    """Fetch all available property tags and find the Hot Tub - TG Service tag ID."""
    try:
        r = requests.get(
            f"{BW_BASE}/public/inventory/v1/property/tags",
            headers={"Authorization": f"JWT {token}"},
            timeout=15,
        )
        if r.status_code != 200:
            return None
        body = r.json()
        tags = body if isinstance(body, list) else body.get("results", body.get("data", []))
        for tag in tags:
            name = (tag.get("name") or tag.get("label") or "").lower().strip()
            if name == HOT_TUB_TAG_NAME:
                return tag.get("id")
    except Exception:
        pass
    return None


def _fetch_property_tags(token: str, pid: str) -> list:
    """Fetch tags for a single property. Returns list of tag objects."""
    for path in [
        f"/public/inventory/v1/property/{pid}/tags",
        f"/public/inventory/v1/property/{pid}",
    ]:
        try:
            r = requests.get(
                f"{BW_BASE}{path}",
                headers={"Authorization": f"JWT {token}"},
                timeout=15,
            )
            if r.status_code == 200:
                body = r.json()
                # /tags endpoint returns a list directly
                if isinstance(body, list):
                    return body
                # property detail endpoint — tags may be nested
                tags = body.get("tags") or body.get("property_tags") or []
                if tags:
                    return tags
        except Exception:
            pass
    return []


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


@hot_tub_bp.route("/admin/hot-tub")
@login_required
@admin_required
def hot_tub_page():
    return render_template("hot_tub.html")


@hot_tub_bp.route("/admin/hot-tub/scan", methods=["POST"])
@login_required
@admin_required
def hot_tub_scan():
    token = _get_token()
    if not token:
        return jsonify({"error": "Breezeway not configured."}), 500

    from routes.briefing import _get_live_property_cache, _get_live_ref_cache, _ensure_property_cache
    _ensure_property_cache()
    prop_cache = _get_live_property_cache()
    ref_cache  = _get_live_ref_cache()

    # Step 1: find the Hot Tub - TG Service tag ID
    tag_id = _fetch_hot_tub_tag_id(token)
    if tag_id is None:
        return jsonify({"error": "Could not find 'Hot Tub - TG Service' tag in Breezeway. Check the tag name matches exactly."}), 500

    # Step 2: find all properties with that tag (concurrent)
    def has_hot_tub_tag(pid):
        tags = _fetch_property_tags(token, pid)
        for t in tags:
            tid = t.get("id") or t.get("tag_id")
            if tid == tag_id:
                return True
            name = (t.get("name") or t.get("label") or "").lower().strip()
            if name == HOT_TUB_TAG_NAME:
                return True
        return False

    all_pids = list(prop_cache.keys())
    tagged_pids = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(has_hot_tub_tag, pid): pid for pid in all_pids}
        for future in as_completed(futures):
            if future.result():
                tagged_pids.append(futures[future])

    if not tagged_pids:
        return jsonify({"results": [], "tag_id": tag_id,
                        "warning": "No properties found with 'Hot Tub - TG Service' tag."})

    # Step 3: fetch tasks — 45 days back AND 45 days forward to find last + next service
    today    = date.today()
    lookback = today - timedelta(days=45)
    lookahead = today + timedelta(days=45)

    def fetch_tasks(pid):
        return pid, _fetch_tasks_for_property(token, pid, ref_cache.get(pid, ""), lookback, lookahead)

    tasks_by_pid: dict[str, list] = {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        for pid, tasks in ex.map(lambda p: fetch_tasks(p), tagged_pids):
            tasks_by_pid[pid] = tasks

    def _assignee_name(t: dict) -> str:
        for a in (t.get("assignments") or []):
            if isinstance(a, dict):
                n = (a.get("name") or a.get("full_name") or
                     f"{a.get('first_name','').strip()} {a.get('last_name','').strip()}".strip())
                if n:
                    return n
        return ""

    # Step 4: find last service (past) and next upcoming service (future) per property
    results = []
    for pid in tagged_pids:
        prop_name = _get_property_name(pid)
        tasks     = tasks_by_pid.get(pid, [])

        past_services   = []
        future_services = []

        for t in tasks:
            title = (t.get("title") or t.get("name") or "")
            if isinstance(title, dict):
                title = title.get("value") or title.get("name") or ""
            if not HOT_TUB_PATTERN.search(title):
                continue
            sched = t.get("scheduled_date") or ""
            try:
                d = date.fromisoformat(sched[:10])
            except (ValueError, TypeError):
                continue
            entry = {
                "date":     d,
                "title":    title,
                "time":     t.get("scheduled_time") or "",
                "assignee": _assignee_name(t),
            }
            if d <= today:
                past_services.append(entry)
            else:
                future_services.append(entry)

        if past_services:
            past_services.sort(key=lambda x: x["date"], reverse=True)
            last = past_services[0]
            last_date       = last["date"]
            last_task_title = last["title"]
            days_since      = (today - last_date).days
        else:
            last_date       = None
            last_task_title = None
            days_since      = None

        if future_services:
            future_services.sort(key=lambda x: x["date"])
            nxt = future_services[0]
            next_task = {
                "title":    nxt["title"],
                "date":     nxt["date"].isoformat(),
                "time":     nxt["time"],
                "assignee": nxt["assignee"],
            }
        else:
            next_task = None

        overdue = days_since is None or days_since > 21

        results.append({
            "property":        prop_name,
            "last_task":       last_task_title,
            "last_date":       last_date.isoformat() if last_date else None,
            "days_since":      days_since,
            "overdue":         overdue,
            "next_task":       next_task,
        })

    # Sort: overdue first, then by days_since descending
    results.sort(key=lambda x: (not x["overdue"], -(x["days_since"] or 9999)))
    return jsonify({"results": results})
