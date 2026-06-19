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
    """Active staff roster: [{id, name}], sorted by name."""
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
        for dp in ({"scheduled_date": f"{date_str},{date_str}"},
                   {"start_date": date_str, "end_date": date_str},
                   {"date": date_str}):
            r, _, status = _fetch_bw_endpoint(token, "/public/inventory/v1/task",
                                              {"reference_property_id": ref_id, **dp})
            if status == 200:
                return r or []
        return []

    all_tasks = []
    with ThreadPoolExecutor(max_workers=25) as ex:
        for tasks in ex.map(_tasks_for_ref, list(pid_candidates.keys())):
            all_tasks.extend(tasks)

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
    seen, buckets = set(), {}
    for t in all_tasks:
        tid = t.get("id")
        if tid in seen:
            continue
        seen.add(tid)
        t_date = (t.get("scheduled_date") or "")[:10]
        if t_date != date_str:
            continue
        home_id = t.get("home_id") or t.get("property_id")
        title   = _bw_task_title(t)
        group   = _top_group_name(_group_map_pid.get(str(home_id), []))
        buckets.setdefault(group, []).append({
            "task_id":   tid,
            "name":      title,
            "property":  _get_property_name(home_id),
            "date":      t_date,
            "time":      (str(t.get("scheduled_time") or "")[:5]) or None,
            "arrival":   str(home_id) in arrival_pids,
            "pci":       _title_has_pci(title),
            "assignees": _assignee_names(t),
        })

    groups_out = []
    for g in sorted(buckets, key=lambda x: (x == "Ungrouped", x.lower())):
        tasks = sorted(buckets[g], key=lambda x: ((x["property"] or "").lower(),
                                                  (x["name"] or "").lower()))
        groups_out.append({"group": g, "tasks": tasks})

    return jsonify({
        "date":        date_str,
        "people":      _fetch_people(token),
        "groups":      groups_out,
        "total_tasks": sum(len(b["tasks"]) for b in groups_out),
    })


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
        try:
            r  = requests.patch(url, headers=headers,
                                json={"assignments": [assignee_id]}, timeout=20)
            ok = r.status_code in (200, 201)
            after = None
            if ok:
                try:
                    g = requests.get(url, headers={"Authorization": f"JWT {token}"}, timeout=15)
                    if g.ok:
                        after = _assignee_names(g.json())
                except Exception:
                    pass
            return {"task_id": tid, "ok": ok, "assignees_after": after,
                    "detail": f"status={r.status_code}" + ("" if ok else f" {r.text[:160]}")}
        except Exception as e:
            return {"task_id": tid, "ok": False, "assignees_after": None, "detail": str(e)}

    results = list(ThreadPoolExecutor(max_workers=10).map(_assign_one, task_ids))
    return jsonify({
        "results":    results,
        "ok_count":   sum(1 for x in results if x["ok"]),
        "fail_count": sum(1 for x in results if not x["ok"]),
    })
