"""
routes/lease_stamp.py — Stamp Lease Tasks (deterministic, no AI in the loop).

Renames upcoming Asana lease tasks to
    "<M/D> <Arrival|Dept> - <House> - <original task name>"
and sets each task's due date to that same arrival / departure date.

DELIBERATELY has no model in the loop. My Bot's chat tool could pick the wrong
tasks — it sorted candidates oldest-first and capped at 50, so one apply run
stamped 50 stale 2025 tasks and dumped them all into Overdue — and it could lose
the preview from its context and invent one. This page scans, shows exactly what
it found, and changes only the rows you tick.

Guarantees:
  * Never proposes a task whose arrival/departure date is in the past.
  * Never truncates silently — if the cap is hit it says so, loudly.
  * Apply recomputes the plan server-side and writes only GIDs you approved, so a
    stale or tampered browser payload can't rename something else.
  * Reads every task back after writing and reports anything that didn't stick.

The lease-title parsing is imported from routes.my_bot rather than duplicated —
one source of truth for the messy parent-name formats. This module only reads
those helpers; it never modifies My Bot.

Endpoints:
  GET  /admin/lease-stamp        — page
  POST /admin/lease-stamp/scan   — build proposals (JSON, read-only)
  POST /admin/lease-stamp/apply  — apply approved GIDs (JSON)
"""

import re
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta

from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required

from routes.auth import admin_required
from routes.my_bot import (
    _asana_fetch_all,
    _asana_request,
    _clean_core,
    _clean_house_name,
    _get_asana_workspace,
    _detect_lease_kind,
    _iso_to_md,
    _lease_ancestor_name,
    _lease_dates_from_parent_name,
    _STAMP_RE,
)

lease_stamp_bp = Blueprint("lease_stamp", __name__)

# Wide default: her lease book runs 6+ months out, and a narrow window silently
# hides work. Breadth is safe here because nothing is written without approval.
DEFAULT_WINDOW_DAYS = 365
MAX_WINDOW_DAYS = 730
# Enough headroom that the cap is effectively never hit; if it is, the UI says so.
SCAN_CAP = 400

# Short-lived scan cache so a retry after a proxy timeout is instant, and so
# apply's server-side recompute doesn't pay for the whole walk again.
import time as _time
_scan_cache: dict = {}          # (window_days, include_dept, property) -> (ts, result)
_SCAN_TTL = 90


def _cached_plan(window_days, include_departures, property_filter):
    key = (window_days, bool(include_departures), (property_filter or "").strip().lower())
    hit = _scan_cache.get(key)
    if hit and _time.time() - hit[0] < _SCAN_TTL:
        return hit[1]
    result = _build_plan(window_days, include_departures, property_filter)
    if result[0] is not None:
        _scan_cache[key] = (_time.time(), result)
    return result


def _fetch_incomplete_tasks():
    """Every incomplete task in the signed-in user's Asana My Tasks list."""
    ws = _get_asana_workspace()
    if not ws:
        return None, "Could not retrieve the Asana workspace."
    utl, err = _asana_request("GET", "/users/me/user_task_list", {"workspace": ws})
    if err or not isinstance(utl, dict) or not utl.get("gid"):
        return None, f"Could not read the user task list: {err or 'no data'}"
    tasks, err = _asana_fetch_all(f"/user_task_lists/{utl['gid']}/tasks", {
        "opt_fields": "name,gid,due_on,completed,parent.name,parent.gid",
        "completed_since": "now",
        "limit": 100,
    })
    if err:
        return None, f"Could not fetch tasks: {err}"
    return [t for t in (tasks or []) if not t.get("completed")], None


def _resolve_lease_ancestors(candidates):
    """parent_gid -> lease task name, resolved in PARALLEL.

    The walk up to the lease task costs one API call per level. Done serially over
    ~1k tasks it ran past a minute and would time out the gateway, so resolve each
    unique parent concurrently instead.
    """
    parents = {}
    for t in candidates:
        par = t.get("parent") or {}
        if par.get("gid"):
            parents.setdefault(par["gid"], par)
    if not parents:
        return {}

    def _one(item):
        gid, par = item
        return gid, _lease_ancestor_name(par, None)

    with ThreadPoolExecutor(max_workers=8) as ex:
        return dict(ex.map(_one, parents.items()))


def _build_plan(window_days, include_departures, property_filter):
    """Return (proposals, skipped, stats). Read-only — touches nothing."""
    tasks, err = _fetch_incomplete_tasks()
    if err:
        return None, None, {"error": err}

    today = date.today()
    win_end = today + timedelta(days=window_days)
    pf = (property_filter or "").strip().lower()

    # Narrow to lease tasks BEFORE resolving ancestors — that is the expensive step.
    candidates = []
    for t in tasks:
        kind = _detect_lease_kind(t.get("name") or "")
        if not kind:
            continue                      # ambiguous or not a lease task — never guess
        if kind == "post" and not include_departures:
            continue
        candidates.append((t, kind))
    ancestors = _resolve_lease_ancestors([t for t, _k in candidates])

    proposals, skipped = [], []
    n_past = n_ahead = 0

    for t, kind in candidates:
        name = t.get("name") or ""
        gid = str(t.get("gid") or "")
        par = t.get("parent") or {}
        lease = ancestors.get(par.get("gid")) or par.get("name") or ""
        house = _clean_house_name(lease)
        if not house or re.match(r"^\s*\d{1,2}/\d{1,2}\s+(arrival|dept)\b", house, re.I):
            skipped.append({"gid": gid, "name": name, "parent": lease,
                            "why": "couldn't identify the house from the parent task"})
            continue
        if pf and pf not in house.lower() and pf not in name.lower():
            continue

        arr_iso, dep_iso = _lease_dates_from_parent_name(lease)
        iso = arr_iso if kind == "pre" else dep_iso
        if not iso:
            which = "arrival" if kind == "pre" else "departure"
            skipped.append({"gid": gid, "name": name, "parent": lease,
                            "why": f"no readable {which} date in the parent title"})
            continue

        when = date.fromisoformat(iso)
        if when < today:                  # HARD RULE: the past is never touched
            n_past += 1
            continue
        if when > win_end:
            n_ahead += 1
            continue

        md = _iso_to_md(iso)
        if not md:
            skipped.append({"gid": gid, "name": name, "parent": lease,
                            "why": f"unreadable date '{iso}'"})
            continue

        new_name = f"{md} {'Arrival' if kind == 'pre' else 'Dept'} - {house} - {_clean_core(name)}"
        if new_name == name and (t.get("due_on") or None) == iso:
            continue                      # already correct

        was_stamped = bool(_STAMP_RE.match(name))
        proposals.append({
            "gid": gid,
            "old_name": name,
            "new_name": new_name,
            "old_due": t.get("due_on"),
            "new_due": iso,
            "house": house,
            "kind": "Arrival" if kind == "pre" else "Dept",
            "parent": lease,
            "was_stamped": was_stamped,
            # Previously-stamped rows start unticked: they're re-stamps, not the
            # plain "Operations- …" cleanup this page is for.
            "default_on": not was_stamped,
        })

    proposals.sort(key=lambda p: (p["new_due"], p["gid"]))
    stats = {
        "scanned": len(tasks),
        "window_start": today.isoformat(),
        "window_end": win_end.isoformat(),
        "window_days": window_days,
        "skipped_past": n_past,
        "skipped_beyond_window": n_ahead,
        "total_proposals": len(proposals),
        "capped": False,
    }
    if len(proposals) > SCAN_CAP:
        stats["capped"] = True
        stats["shown"] = SCAN_CAP
        proposals = proposals[:SCAN_CAP]
    return proposals, skipped, stats


def _read_params(body):
    try:
        wd = int(body.get("window_days") or DEFAULT_WINDOW_DAYS)
    except (TypeError, ValueError):
        wd = DEFAULT_WINDOW_DAYS
    # Departures default ON: "Operations- Post Lease Activities" needs the same
    # treatment as arrivals — house name + DEPARTURE date, due on departure.
    inc = body.get("include_departures")
    return (max(1, min(wd, MAX_WINDOW_DAYS)),
            True if inc is None else bool(inc),
            body.get("property") or "")


@lease_stamp_bp.route("/admin/lease-stamp")
@login_required
@admin_required
def lease_stamp_page():
    return render_template("lease_stamp.html",
                           default_window=DEFAULT_WINDOW_DAYS)


@lease_stamp_bp.route("/admin/lease-stamp/scan", methods=["POST"])
@login_required
@admin_required
def lease_stamp_scan():
    body = request.get_json(force=True) or {}
    proposals, skipped, stats = _cached_plan(*_read_params(body))
    if proposals is None:
        return jsonify({"error": stats.get("error", "scan failed")}), 500
    return jsonify({"proposals": proposals, "skipped": skipped, "stats": stats})


@lease_stamp_bp.route("/admin/lease-stamp/apply", methods=["POST"])
@login_required
@admin_required
def lease_stamp_apply():
    body = request.get_json(force=True) or {}
    approved = {str(g).strip() for g in (body.get("gids") or []) if str(g).strip()}
    if not approved:
        return jsonify({"error": "No tasks were selected — nothing was changed."}), 400

    # Recompute server-side so the browser can only choose WHICH tasks to change,
    # never what they get renamed to.
    proposals, _skipped, stats = _cached_plan(*_read_params(body))
    if proposals is None:
        return jsonify({"error": stats.get("error", "scan failed")}), 500

    by_gid = {p["gid"]: p for p in proposals}
    to_do = [by_gid[g] for g in approved if g in by_gid]
    missing = sorted(approved - set(by_gid))

    def _write(p):
        _, err = _asana_request("PUT", f"/tasks/{p['gid']}",
                                {"data": {"name": p["new_name"], "due_on": p["new_due"]}})
        if err:
            return {**p, "status": "failed", "detail": err}
        chk, cerr = _asana_request("GET", f"/tasks/{p['gid']}",
                                   {"opt_fields": "name,due_on"})
        if cerr or not isinstance(chk, dict):
            return {**p, "status": "unverified", "detail": "wrote, but read-back failed"}
        problems = []
        if chk.get("name") != p["new_name"]:
            problems.append("title didn't stick")
        if chk.get("due_on") != p["new_due"]:
            problems.append(f"due is {chk.get('due_on')}, expected {p['new_due']}")
        return {**p, "status": "ok" if not problems else "mismatch",
                "detail": "; ".join(problems)}

    results = []
    if to_do:
        with ThreadPoolExecutor(max_workers=8) as ex:
            results = list(ex.map(_write, to_do))

    _scan_cache.clear()          # tasks just changed — never serve the old plan
    return jsonify({
        "results": results,
        "ok": sum(1 for r in results if r["status"] == "ok"),
        "failed": [r for r in results if r["status"] != "ok"],
        # A GID that vanished from the recomputed plan means someone else changed
        # the task since the scan — say so rather than silently doing nothing.
        "stale": missing,
    })
