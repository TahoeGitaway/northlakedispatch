"""
routes/hot_tub.py — Hot Tub service overdue scanner.

Applies to properties tagged "Hot Tub - TG Service" in Breezeway, OR
properties with a current 30+ night guest lease that also carry a plain
"Hot Tub" tag.
Looks back 45 days for tasks whose title contains "hot tub" AND
("arrival" OR "biweekly"). Alerts on any property where the last
service was more than 14 days ago (or never found in the window).

Also flags "too close" services: any two hot tub services on the same
property scheduled within 6 days of each other (likely an accidental
double-booking), looking 45 days back and 45 days forward.

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

# Scan-result cache: the all-properties tag sweep can run past the hosting proxy's
# timeout (→ "upstream error"); caching means the backend finishes and a retry
# returns instantly. The scan takes no params, so one entry is enough.
import time as _time
_scan_cache = {"ts": 0.0, "data": None}
_SCAN_TTL = 300

HOT_TUB_PATTERN = re.compile(
    r"(?=.*\bhot[\s\-]?tub\b)(?=.*\b(arrival|biweekly|bi[\s\-]?weekly|lease|d\s*&\s*s)\b)",
    re.IGNORECASE,
)
HOT_TUB_TAG_NAME = "hot tub - tg service"
# A house also qualifies for the scan if it has a current 30+ night guest lease
# AND carries this plain "Hot Tub" tag (matched exactly, NOT as a substring —
# otherwise "Hot Tub - TG Service" would also satisfy it).
HOT_TUB_PLAIN_TAG_NAME = "hot tub"

# Two services scheduled this many days apart (or fewer) are flagged as a
# possible accidental double-booking. Normal cadence is biweekly (~14 days).
TOO_CLOSE_DAYS = 6

# A pair is only flagged if at least one side is a biweekly or lease service —
# those run on a fixed cadence and should never land this close to another
# service. (Arrival / D&S services can legitimately cluster around a stay.)
BIWEEKLY_OR_LEASE_PATTERN = re.compile(
    r"\b(biweekly|bi[\s\-]?weekly|lease)\b",
    re.IGNORECASE,
)

# The double-booking check only inspects services within this many days on
# either side of today (a 2-week window centred on the scan date).
TOO_CLOSE_WINDOW_DAYS = 7


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


def _fetch_current_lease_pids(token: str, today: date) -> set:
    """Property IDs with a guest lease (30+ nights) active today.

    A reservation is "current" if checkin <= today <= checkout. Lease vs
    owner/block classification reuses lease_prep._is_lease so the rules stay
    in one place.
    """
    from routes.lease_prep import _is_lease
    pids: set[str] = set()
    page = 1
    while True:
        try:
            r = requests.get(
                f"{BW_BASE}/public/inventory/v1/reservation",
                headers={"Authorization": f"JWT {token}"},
                params={"checkin_date_le": today.isoformat(),
                        "checkout_date_ge": today.isoformat(),
                        "limit": 100, "page": page},
                timeout=20,
            )
            if r.status_code != 200:
                break
            body    = r.json()
            results = body.get("results", body.get("data", body if isinstance(body, list) else []))
            if not results:
                break
            for res in results:
                if _is_lease(res):
                    pid = str(res.get("property_id") or res.get("home_id") or "")
                    if pid:
                        pids.add(pid)
            if len(results) < 100:
                break
            page += 1
        except Exception:
            break
    return pids


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

    # Serve a fresh cached result instantly (also rescues a prior proxy timeout).
    if not (request.get_json(silent=True) or {}).get("force") \
            and _scan_cache["data"] is not None and _time.time() - _scan_cache["ts"] < _SCAN_TTL:
        return jsonify(_scan_cache["data"])

    from routes.briefing import _get_live_property_cache, _get_live_ref_cache, _ensure_property_cache
    _ensure_property_cache()
    prop_cache = _get_live_property_cache()
    ref_cache  = _get_live_ref_cache()

    # Step 1: find the Hot Tub - TG Service tag ID
    tag_id = _fetch_hot_tub_tag_id(token)
    if tag_id is None:
        return jsonify({"error": "Could not find 'Hot Tub - TG Service' tag in Breezeway. Check the tag name matches exactly."}), 500

    today = date.today()

    # Step 2: classify each property's tags in one sweep — does it carry the
    # "Hot Tub - TG Service" tag, and/or a plain "Hot Tub" tag?
    def classify_tags(pid):
        has_tg = has_plain = False
        for t in _fetch_property_tags(token, pid):
            if isinstance(t, dict):
                tid  = t.get("id") or t.get("tag_id")
                name = (t.get("name") or t.get("label") or "").lower().strip()
            else:
                tid, name = None, str(t).lower().strip()
            if tid == tag_id or name == HOT_TUB_TAG_NAME:
                has_tg = True
            if name == HOT_TUB_PLAIN_TAG_NAME:
                has_plain = True
        return has_tg, has_plain

    all_pids = list(prop_cache.keys())
    tg_pids: set[str]         = set()
    plain_hot_tub_pids: set[str] = set()
    with ThreadPoolExecutor(max_workers=16) as ex:
        futures = {ex.submit(classify_tags, pid): pid for pid in all_pids}
        for future in as_completed(futures):
            pid = futures[future]
            has_tg, has_plain = future.result()
            if has_tg:
                tg_pids.add(pid)
            if has_plain:
                plain_hot_tub_pids.add(pid)

    # A house qualifies via EITHER the TG Service tag, OR a current 30+ night
    # guest lease combined with a plain "Hot Tub" tag.
    lease_pids        = _fetch_current_lease_pids(token, today)
    lease_qualified   = lease_pids & plain_hot_tub_pids
    tagged_pids       = sorted(tg_pids | lease_qualified)

    if not tagged_pids:
        return jsonify({"results": [], "tag_id": tag_id,
                        "warning": "No properties found with 'Hot Tub - TG Service' "
                                   "tag, nor a current lease + 'Hot Tub' tag."})

    # Step 3: fetch tasks — 45 days back AND 45 days forward to find last + next service
    lookback = today - timedelta(days=45)
    lookahead = today + timedelta(days=45)

    def fetch_tasks(pid):
        return pid, _fetch_tasks_for_property(token, pid, ref_cache.get(pid, ""), lookback, lookahead)

    tasks_by_pid: dict[str, list] = {}
    with ThreadPoolExecutor(max_workers=16) as ex:
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
    results   = []
    too_close = []   # possible accidental double-bookings (services <= 6 days apart)
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

        # Too-close check: within a 2-week window centred on today, flag any two
        # consecutive services <= TOO_CLOSE_DAYS apart where at least one is a
        # biweekly or lease service (a likely accidental double-booking). Sort by
        # date+time so consecutive pairs are truly adjacent.
        tc_start = today - timedelta(days=TOO_CLOSE_WINDOW_DAYS)
        tc_end   = today + timedelta(days=TOO_CLOSE_WINDOW_DAYS)
        window_services = sorted(
            (s for s in (past_services + future_services) if tc_start <= s["date"] <= tc_end),
            key=lambda x: (x["date"], x["time"] or ""),
        )
        for prev_svc, next_svc in zip(window_services, window_services[1:]):
            gap = (next_svc["date"] - prev_svc["date"]).days
            if gap > TOO_CLOSE_DAYS:
                continue
            if not (BIWEEKLY_OR_LEASE_PATTERN.search(prev_svc["title"])
                    or BIWEEKLY_OR_LEASE_PATTERN.search(next_svc["title"])):
                continue
            too_close.append({
                "property": prop_name,
                "gap_days": gap,
                "first": {
                    "title":    prev_svc["title"],
                    "date":     prev_svc["date"].isoformat(),
                    "time":     prev_svc["time"],
                    "assignee": prev_svc["assignee"],
                },
                "second": {
                    "title":    next_svc["title"],
                    "date":     next_svc["date"].isoformat(),
                    "time":     next_svc["time"],
                    "assignee": next_svc["assignee"],
                },
            })

        overdue = days_since is None or days_since > 18

        results.append({
            "property":        prop_name,
            "last_task":       last_task_title,
            "last_date":       last_date.isoformat() if last_date else None,
            "days_since":      days_since,
            "overdue":         overdue,
        })

    # Sort: overdue first, then by days_since descending
    results.sort(key=lambda x: (not x["overdue"], -(x["days_since"] or 9999)))
    # Tightest gaps first, then by the earlier service date.
    too_close.sort(key=lambda x: (x["gap_days"], x["first"]["date"]))
    payload = {"results": results, "too_close": too_close}
    _scan_cache["data"] = payload          # cache before returning (survives proxy timeout)
    _scan_cache["ts"]   = _time.time()
    return jsonify(payload)
