"""
routes/group_assign.py — Batch-assign tasks by property group.

Pick a date → see every Breezeway task that day, bucketed by the property's
top-level group (North Shore, Palisades, Tahoe Donner, West Shore, Martis Valley,
…). Tick any tasks and assign them to a person in one shot.

Confirmed against the Breezeway API docs:
  Assign:  PATCH /public/inventory/v1/task/{id}  body {"assignments": [person_id, …]}
  Roster:  GET   /public/inventory/v1/people?status=active   (person.id, person.name)
  Groups:  each /property carries a `groups` array of {id, name, parent_group_id}

Endpoints:
  GET  /admin/group-assign         — page
  POST /admin/group-assign/scan    — tasks for a date, grouped (JSON)
  POST /admin/group-assign/assign  — PATCH assignees onto selected tasks (JSON)
"""

import time
import requests
from datetime import date
from concurrent.futures import ThreadPoolExecutor

from flask import Blueprint, render_template, jsonify, request
from flask_login import login_required

from routes.auth import admin_required

group_assign_bp = Blueprint("group_assign", __name__)

BW_BASE = "https://api.breezeway.io"

# Property → groups cache. The shared property cache drops the `groups` field, so
# we keep our own map here, refreshed hourly.
_group_map_pid: dict = {}   # {str(property_id): [group dicts]}
_group_by_id:   dict = {}   # {group_id: group dict}  (to walk the hierarchy)
_group_ts:      float = 0.0

# Per-date scan-result cache. The per-property task sweep (~hundreds of Breezeway
# calls) can run long enough that the hosting proxy times out ("upstream error")
# even though the backend finishes — caching the result means the retry returns
# instantly. Cleared whenever an assignment is written.
_scan_cache:   dict  = {}   # date_str -> (timestamp, result_dict)
_SCAN_TTL            = 60

# Staff roster cache (fetched on every scan otherwise).
_people_cache: dict  = {"ts": 0.0, "data": []}


def _get_token():
    from routes.briefing import _get_breezeway_token
    return _get_breezeway_token()


def _results(data):
    """Breezeway list endpoints return either a bare list or {results|data: [...]}."""
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("results") or data.get("data") or []
    return []


def _refresh_group_map(token: str):
    """Fetch every active property's `groups` array (the property list carries it)."""
    global _group_map_pid, _group_by_id, _group_ts
    if _group_map_pid and time.time() - _group_ts < 3600:
        return
    pid_map, by_id = {}, {}
    page = 1
    while page <= 6:
        try:
            r = requests.get(f"{BW_BASE}/public/inventory/v1/property",
                             headers={"Authorization": f"JWT {token}"},
                             params={"limit": 200, "page": page, "status": "active"},
                             timeout=20)
        except Exception:
            break
        if not r.ok:
            break
        items = _results(r.json())
        for p in items:
            if not isinstance(p, dict):
                continue
            groups = p.get("groups") or []
            pid_map[str(p.get("id"))] = groups
            for g in groups:
                if isinstance(g, dict) and g.get("id") is not None:
                    by_id[g["id"]] = g
        if len(items) < 200:
            break
        page += 1
    if pid_map:
        _group_map_pid, _group_by_id, _group_ts = pid_map, by_id, time.time()


def _top_group_name(groups: list) -> str:
    """Top-level group name (root of the hierarchy) for a property's groups array."""
    if not groups:
        return "Ungrouped"
    # A property usually lists its leaf AND its ancestors, so the root (parent=None)
    # is right there.
    for g in groups:
        if isinstance(g, dict) and g.get("parent_group_id") is None:
            return g.get("name") or "Ungrouped"
    # Otherwise walk the first group up to its root via the global id map.
    g, seen = groups[0], set()
    while isinstance(g, dict) and g.get("parent_group_id") is not None and g.get("id") not in seen:
        seen.add(g.get("id"))
        g = _group_by_id.get(g.get("parent_group_id"))
    return (g.get("name") if isinstance(g, dict) else None) or (groups[0].get("name") or "Ungrouped")


def _fetch_people(token: str) -> list:
    """Active staff roster: [{id, name}], sorted by name. Cached for 1 hour."""
    if _people_cache["data"] and time.time() - _people_cache["ts"] < 3600:
        return _people_cache["data"]
    people, page = [], 1
    while page <= 10:
        try:
            r = requests.get(f"{BW_BASE}/public/inventory/v1/people",
                             headers={"Authorization": f"JWT {token}"},
                             params={"status": "active", "limit": 200, "page": page},
                             timeout=20)
        except Exception:
            break
        if not r.ok:
            break
        items = _results(r.json())
        for p in items:
            if not isinstance(p, dict):
                continue
            pid  = p.get("id")
            name = (p.get("name") or
                    f"{p.get('first_name','').strip()} {p.get('last_name','').strip()}".strip() or
                    p.get("email") or str(pid))
            if pid is not None:
                people.append({"id": pid, "name": name})
        if len(items) < 200:
            break
        page += 1
    people.sort(key=lambda x: x["name"].lower())
    _people_cache["data"] = people
    _people_cache["ts"]   = time.time()
    return people


def _assignee_names(task: dict) -> list:
    out = []
    for a in (task.get("assignments") or []):
        n = (a.get("name") or
             f"{a.get('first_name','').strip()} {a.get('last_name','').strip()}".strip())
        if n:
            out.append(n)
    return out


@group_assign_bp.route("/admin/group-assign")
@login_required
@admin_required
def group_assign_page():
    return render_template("group_assign.html")


@group_assign_bp.route("/admin/group-assign/scan", methods=["POST"])
@login_required
@admin_required
def group_assign_scan():
    try:
        return _scan_inner()
    except Exception as e:
        import traceback
        return jsonify({"error": f"{type(e).__name__}: {e}\n\n{traceback.format_exc()}"}), 500


def _scan_inner():
    from routes.briefing import (_fetch_bw_endpoint, _ensure_property_cache,
                                 _get_live_property_cache, _get_live_ref_cache,
                                 _get_property_name, _fetch_breezeway_checkins,
                                 _classify_reservation)
    from routes.dispatch import _bw_task_title, _title_has_pci

    token = _get_token()
    if not token:
        return jsonify({"error": "Breezeway not configured."})

    payload  = request.get_json(silent=True) or {}
    date_str = (payload.get("date") or date.today().isoformat())[:10]
    force    = bool(payload.get("force"))

    # Serve a fresh cached result instantly (also rescues a prior proxy timeout) —
    # UNLESS the caller forced a fresh sweep (e.g. tasks were still loading in BW).
    cached = _scan_cache.get(date_str)
    if cached and not force and time.time() - cached[0] < _SCAN_TTL:
        return jsonify(cached[1])

    _ensure_property_cache()
    prop_cache = _get_live_property_cache()
    ref_cache  = _get_live_ref_cache()
    _refresh_group_map(token)

    # Candidate ref ids — reference_property_id when present, else the bw id.
    pid_candidates = {}
    for bw_pid in prop_cache:
        ref_id = ref_cache.get(bw_pid)
        pid_candidates.setdefault(ref_id if ref_id else str(bw_pid), bw_pid)

    def _tasks_for_ref(ref_id):
        """Return (tasks, ok). ok=False means we could NOT load this property
        (throttled / errored) — so its tasks must NOT be silently treated as 'none'."""
        for attempt in range(3):
            r, _, status = _fetch_bw_endpoint(
                token, "/public/inventory/v1/task",
                {"reference_property_id": ref_id, "scheduled_date": f"{date_str},{date_str}"})
            if status == 200:
                return (r or [], True)
            # Throttle / transient server error / no response → back off and retry.
            if status is None or status == 429 or status >= 500:
                time.sleep(0.3 * (attempt + 1))
                continue
            # Other non-200 (e.g. 400) → try the alternate date param once, else fail.
            r2, _, st2 = _fetch_bw_endpoint(
                token, "/public/inventory/v1/task",
                {"reference_property_id": ref_id, "start_date": date_str, "end_date": date_str})
            return (r2 or [], True) if st2 == 200 else ([], False)
        return ([], False)

    # Moderate concurrency + the retry/backoff above so the sweep doesn't trip
    # Breezeway rate limits, which would silently drop a property's whole list.
    all_tasks, failed_props = [], 0
    with ThreadPoolExecutor(max_workers=16) as ex:
        for tasks, ok in ex.map(_tasks_for_ref, list(pid_candidates.keys())):
            all_tasks.extend(tasks)
            if not ok:
                failed_props += 1

    # Guest/owner arrivals that day → BW property ids (for the CHECK-IN badge).
    # Matched by property_id directly — no local name-matching needed.
    arrival_pids = set()
    try:
        for r in _fetch_breezeway_checkins(date_str):
            if _classify_reservation(r) == "block":
                continue
            apid = r.get("property_id") or r.get("home_id")
            if apid is not None:
                arrival_pids.add(str(apid))
    except Exception:
        pass

    # Bucket by top-level group. STRICT date guard (only this exact date — the
    # per-property query returns undated/off-date recurring tasks otherwise).
    seen, buckets, checkins = set(), {}, []
    hidden_cleaning = 0
    dept_counts = {}        # every department value seen (for diagnostics)
    for t in all_tasks:
        tid = t.get("id")
        if tid in seen:
            continue
        seen.add(tid)
        t_date = (t.get("scheduled_date") or "")[:10]
        if t_date != date_str:
            continue
        # Hide cleaning / housekeeping department tasks entirely — never touched here.
        dept = t.get("type_department")
        if isinstance(dept, dict):
            dept = dept.get("code") or dept.get("name") or ""
        dl = str(dept).strip().lower()
        dept_counts[dl or "(none)"] = dept_counts.get(dl or "(none)", 0) + 1
        if "clean" in dl or "housekeep" in dl:
            hidden_cleaning += 1
            continue
        home_id    = t.get("home_id") or t.get("property_id")
        title      = _bw_task_title(t)
        group      = _top_group_name(_group_map_pid.get(str(home_id), []))
        is_arrival = str(home_id) in arrival_pids
        entry = {
            "task_id":   tid,
            "name":      title,
            "property":  _get_property_name(home_id),
            "date":      t_date,
            "time":      (str(t.get("scheduled_time") or "")[:5]) or None,
            "arrival":   is_arrival,
            "pci":       _title_has_pci(title),
            "assignees":    _assignee_names(t),
            "assignee_ids": [a.get("assignee_id") for a in (t.get("assignments") or [])
                             if a.get("assignee_id") is not None],
            "group":        group,
        }
        # Check-in houses get their OWN section (easy selection) — pulled out of the
        # group buckets so a task never appears, or is selected, twice.
        if is_arrival:
            checkins.append(entry)
        else:
            buckets.setdefault(group, []).append(entry)

    groups_out = []
    for g in sorted(buckets, key=lambda x: (x == "Ungrouped", x.lower())):
        tasks = sorted(buckets[g], key=lambda x: ((x["property"] or "").lower(),
                                                  (x["name"] or "").lower()))
        groups_out.append({"group": g, "tasks": tasks})

    checkins.sort(key=lambda x: ((x["property"] or "").lower(), (x["name"] or "").lower()))
    result = {
        "date":        date_str,
        "people":      _fetch_people(token),
        "checkins":    checkins,
        "groups":      groups_out,
        "total_tasks": len(checkins) + sum(len(b["tasks"]) for b in groups_out),
        "hidden_cleaning": hidden_cleaning,
        "dept_counts": dept_counts,
        "failed_properties": failed_props,
        "scanned_properties": len(pid_candidates),
    }
    # Cache before returning — so even if the proxy already timed out, the retry
    # gets this result instantly instead of re-running the whole sweep.
    _scan_cache[date_str] = (time.time(), result)
    return jsonify(result)


@group_assign_bp.route("/admin/group-assign/assign", methods=["POST"])
@login_required
@admin_required
def group_assign_apply():
    token = _get_token()
    if not token:
        return jsonify({"error": "Breezeway not configured."})

    payload  = request.get_json(silent=True) or {}
    task_ids = payload.get("task_ids") or []
    try:
        assignee_id = int(payload.get("assignee_id"))
    except (TypeError, ValueError):
        return jsonify({"error": "Pick a person to assign to."}), 400
    if not task_ids:
        return jsonify({"error": "No tasks selected."}), 400

    headers = {"Authorization": f"JWT {token}", "Content-Type": "application/json"}

    def _assign_one(tid):
        url = f"{BW_BASE}/public/inventory/v1/task/{tid}"
        last = "no attempt"
        # Retry on throttle / transient server errors so an assignment never gets
        # silently dropped because Breezeway was momentarily busy.
        for attempt in range(3):
            try:
                r = requests.patch(url, headers=headers,
                                   json={"assignments": [assignee_id]}, timeout=20)
                last = f"status={r.status_code}"
                if r.status_code in (200, 201):
                    # Re-read from Breezeway so the raw panel shows who is actually
                    # assigned now — confirmation, not just the PATCH status.
                    after = None
                    try:
                        g = requests.get(url, headers={"Authorization": f"JWT {token}"}, timeout=15)
                        if g.ok:
                            after = _assignee_names(g.json())
                    except Exception:
                        pass
                    return {"task_id": tid, "ok": True, "assignees_after": after, "detail": last}
                if r.status_code == 429 or r.status_code >= 500:
                    last += f" {r.text[:120]}"
                    time.sleep(0.4 * (attempt + 1))
                    continue
                # Non-retryable (e.g. 400/404)
                return {"task_id": tid, "ok": False, "assignees_after": None,
                        "detail": f"{last} {r.text[:160]}"}
            except Exception as e:
                last = str(e)
                time.sleep(0.4 * (attempt + 1))
        return {"task_id": tid, "ok": False, "assignees_after": None,
                "detail": f"failed after retries — {last}"}

    results = list(ThreadPoolExecutor(max_workers=8).map(_assign_one, task_ids))
    _scan_cache.clear()   # assignees changed — next scan should be fresh
    return jsonify({
        "results":    results,
        "ok_count":   sum(1 for x in results if x["ok"]),
        "fail_count": sum(1 for x in results if not x["ok"]),
    })
