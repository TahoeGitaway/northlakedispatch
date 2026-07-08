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
  GET  /admin/group-assign              — page
  POST /admin/group-assign/scan         — tasks for a date, grouped (JSON)
  POST /admin/group-assign/assign       — PATCH assignees onto selected tasks (JSON)
  POST /admin/group-assign/change-date  — PATCH scheduled_date onto selected tasks (JSON)
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

# People who must NEVER be assignable from the batcher. The Task API was seen
# auto-assigning a task to Derek — which can never validly happen — so these
# names are filtered out of the roster the assign / saved-view dropdowns are
# built from. Matched case-insensitively against any token of the person's name.
_BLOCKED_ASSIGNEE_NAMES = {"derek", "christy"}


def _is_blocked_assignee(name: str) -> bool:
    toks = (name or "").lower().replace(",", " ").split()
    return any(t in _BLOCKED_ASSIGNEE_NAMES for t in toks)


def _candidate_names() -> list:
    """The editable assignment allow-list (display names), alphabetical."""
    from db import get_db, get_cursor
    conn = get_db(); cur = get_cursor(conn)
    try:
        cur.execute("SELECT name FROM assignment_candidates ORDER BY LOWER(name)")
        return [r["name"] for r in cur.fetchall()]
    finally:
        cur.close(); conn.rollback(); conn.close()


def _candidate_keys() -> set:
    """Lowercased name tokens the batcher is allowed to assign to."""
    return {n.lower().strip() for n in _candidate_names()}


def _is_candidate(name: str, keys: set = None) -> bool:
    """A Breezeway person is an allowed target if ANY token of their name (or
    their full name) is on the allow-list — the grid uses first names, so
    'Jeremy Garcia' matches the candidate 'Jeremy'. Blocked names never pass."""
    if _is_blocked_assignee(name):
        return False
    if keys is None:
        keys = _candidate_keys()
    nl = (name or "").lower().strip()
    if nl in keys:
        return True
    return any(t in keys for t in nl.replace(",", " ").split())


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
    people, page, ok = [], 1, False
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
        ok = True
        items = _results(r.json())
        for p in items:
            if not isinstance(p, dict):
                continue
            pid  = p.get("id")
            name = (p.get("name") or
                    f"{p.get('first_name','').strip()} {p.get('last_name','').strip()}".strip() or
                    p.get("email") or str(pid))
            if pid is not None and not _is_blocked_assignee(name):
                people.append({"id": pid, "name": name})
        if len(items) < 200:
            break
        page += 1
    people.sort(key=lambda x: x["name"].lower())
    # Only cache a roster we actually loaded — never let a transient failure poison
    # the cache with an empty list for an hour (which silently blocks all assigning).
    if ok:
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
                                 _classify_reservation, _fetch_bw_reservations)
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
    # No property cache → we'd scan zero houses and return "no tasks", which looks
    # identical to a genuinely empty day. Fail loudly instead so it's never silent.
    if not prop_cache:
        return jsonify({"error": "Breezeway property cache is empty — try again in a moment."}), 502
    _refresh_group_map(token)

    # Candidate ref ids — reference_property_id when present, else the bw id.
    # A house with NO reference_property_id gets queried below by its internal bw
    # id, which the task endpoint's reference_property_id filter won't match — so it
    # returns zero tasks *silently*. Count those houses so the UI can warn instead of
    # the day just looking lighter than it is.
    pid_candidates = {}
    no_ref_id_pids = []
    for bw_pid in prop_cache:
        ref_id = ref_cache.get(bw_pid)
        if not ref_id:
            no_ref_id_pids.append(bw_pid)
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

    # Guest/owner/lease arrivals that day → BW property ids (for the CHECK-IN badge),
    # plus a by-type tally of the arrivals THEMSELVES (every check-in reservation that
    # day, even at houses with no task in this batch). Matched by property_id directly.
    # The tally counts arrivals/reservations; arrival_pids counts distinct houses —
    # they differ, so the summary can show both honestly (blocks/holds excluded).
    arrival_pids = set()
    arrival_counts = {"guest": 0, "owner": 0, "lease": 0}
    seen_resv = set()
    try:
        for r in _fetch_breezeway_checkins(date_str):
            kind = _classify_reservation(r)
            if kind == "block":
                continue
            rid = r.get("id")
            if rid is not None:
                if rid in seen_resv:   # de-dupe paginated reservations
                    continue
                seen_resv.add(rid)
            apid = r.get("property_id") or r.get("home_id")
            if apid is not None:
                arrival_pids.add(str(apid))
            arrival_counts[kind] = arrival_counts.get(kind, 0) + 1
    except Exception:
        pass

    # Occupancy: what's in each house STRICTLY mid-stay on the selected day
    # (checkin < D < checkout) — i.e. present that night, not arriving or departing
    # that day. We capture the KIND so the UI can distinguish a guest, a long-term
    # tenant (lease), an owner stay, and a block/hold — not just "occupied vs not".
    # A span query (checked-in on/before D and out on/after D) keeps it to a few
    # hundred records. If several span the day, keep the most "present" one.
    _OCC_PRIORITY = {"guest": 0, "lease": 1, "owner": 2, "block": 3}
    occupancy: dict = {}     # str(property_id) -> {"kind": ..., "until": checkout ISO}
    try:
        day_d = date.fromisoformat(date_str)
        for r in _fetch_bw_reservations(token, {"checkin_date_le": date_str,
                                                "checkout_date_ge": date_str}):
            kind = _classify_reservation(r)
            opid = r.get("property_id") or r.get("home_id")
            if opid is None:
                continue
            ci = (r.get("checkin_date") or "")[:10]
            co = (r.get("checkout_date") or "")[:10]
            try:
                if not (date.fromisoformat(ci) < day_d < date.fromisoformat(co)):
                    continue
            except (ValueError, TypeError):
                continue
            key  = str(opid)
            prev = occupancy.get(key)
            if prev is None or _OCC_PRIORITY.get(kind, 9) < _OCC_PRIORITY.get(prev["kind"], 9):
                occupancy[key] = {"kind": kind, "until": co}
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
            "property_id": home_id,   # for the Breezeway calendar deep-link in the UI
            "date":      t_date,
            "time":      (str(t.get("scheduled_time") or "")[:5]) or None,
            "arrival":   is_arrival,
            # PCI is a priority check-in only when the arrival is that same day; a PCI
            # prepping for a next-day arrival is just a task and isn't flagged loudly.
            "pci":       is_arrival and _title_has_pci(title),
            "assignees":    _assignee_names(t),
            "assignee_ids": [a.get("assignee_id") for a in (t.get("assignments") or [])
                             if a.get("assignee_id") is not None],
            "group":        group,
            # Who/what is in the house that day (strictly mid-stay)? Shown as a badge
            # on non-arrival tasks so it's clear the work happens during a stay.
            # occupancy_kind: guest | lease | owner | block | None (nobody).
            "occupied":       str(home_id) in occupancy,
            "occupancy_kind": (occupancy.get(str(home_id)) or {}).get("kind"),
            "occupied_until": (occupancy.get(str(home_id)) or {}).get("until"),
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

    # Dropdown shows ONLY allow-listed candidates (people leave / get hired —
    # managed from the panel at the bottom of the page). This is the guard that
    # keeps tasks from being assigned to anyone off the approved roster.
    cand_keys = _candidate_keys()
    people = [p for p in _fetch_people(token) if _is_candidate(p["name"], cand_keys)]

    checkins.sort(key=lambda x: ((x["property"] or "").lower(), (x["name"] or "").lower()))
    result = {
        "date":        date_str,
        "people":      people,
        "checkins":    checkins,
        "groups":      groups_out,
        "total_tasks": len(checkins) + sum(len(b["tasks"]) for b in groups_out),
        "hidden_cleaning": hidden_cleaning,
        "dept_counts": dept_counts,
        "failed_properties": failed_props,
        "scanned_properties": len(pid_candidates),
        # Houses with no Breezeway reference_property_id — their tasks can't be
        # fetched and silently won't appear. Surfaced as a warning in the UI.
        "no_ref_id_properties": len(no_ref_id_pids),
        "arrival_counts":   arrival_counts,                 # {guest, owner, lease} — ALL arrivals that day
        "arrival_total":    sum(arrival_counts.values()),   # total check-in reservations that day
        "arriving_houses":  len(arrival_pids),              # distinct houses with an arrival (any type)
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

    # SAFETY CHECK — never assign to anyone off the approved allow-list, even if a
    # stale/tampered dropdown sends an id that isn't a candidate. Resolve the id to
    # a name against the live roster and confirm it's allowed before writing.
    roster = _fetch_people(token)  # already excludes hard-blocked names
    target = next((p for p in roster if p["id"] == assignee_id), None)
    if target is None:
        return jsonify({"error": "Could not verify that person against the Breezeway roster — "
                                 "refusing to assign. Rescan and try again."}), 400
    if not _is_candidate(target["name"]):
        return jsonify({"error": f"“{target['name']}” is not an approved assignment candidate — "
                                 "blocked. Add them in the candidates panel first if this is intended."}), 403

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


# ── Assignment candidates (allow-list) management ─────────────────

def _resolve_and_persist_candidates(roster: list) -> list:
    """Upgrade bare first-name candidates to the FULL Breezeway name when exactly
    one active person has that first name (so the allow-list carries last names and
    can tell two people apart). Returns [{name, display, ambiguous, options?}].
    Duplicates are left as-is and flagged so she can pick the right full name."""
    from db import get_db, get_cursor
    by_first = {}
    for p in roster:
        toks = (p.get("name") or "").split()
        if toks:
            by_first.setdefault(toks[0].lower(), []).append(p["name"])

    out, upgrades = [], []
    for nm in _candidate_names():
        s = nm.strip()
        if " " in s:                              # already a full name
            out.append({"name": nm, "display": nm, "ambiguous": False})
            continue
        matches = by_first.get(s.lower(), [])
        if len(matches) == 1:                     # unique first name → upgrade
            full = matches[0]
            out.append({"name": full, "display": full, "ambiguous": False})
            upgrades.append((s.lower(), full))
        elif len(matches) > 1:                    # duplicate first name → make her pick
            out.append({"name": nm, "display": nm, "ambiguous": True, "options": matches})
        else:                                     # not currently in the roster
            out.append({"name": nm, "display": nm, "ambiguous": False})

    if upgrades:
        conn = get_db(); cur = get_cursor(conn)
        try:
            for oldkey, full in upgrades:
                cur.execute("SELECT 1 FROM assignment_candidates WHERE name_key=%s",
                            (full.lower().strip(),))
                if cur.fetchone():                # full name already present → drop the bare dup
                    cur.execute("DELETE FROM assignment_candidates WHERE name_key=%s", (oldkey,))
                else:
                    cur.execute("UPDATE assignment_candidates SET name=%s, name_key=%s WHERE name_key=%s",
                                (full, full.lower().strip(), oldkey))
            conn.commit()
        finally:
            cur.close(); conn.close()
    return out


@group_assign_bp.route("/admin/group-assign/candidates", methods=["GET"])
@login_required
@admin_required
def group_assign_candidates():
    """Allow-list (resolved to full names) + roster full names not yet on it."""
    token  = _get_token()
    roster = _fetch_people(token) if token else []   # already excludes hard-blocked names
    resolved = _resolve_and_persist_candidates(roster)

    # Hide a roster person from "add" only on an EXACT full-name match, so a bare
    # ambiguous candidate (e.g. "Chris") doesn't block adding the specific person.
    full_keys = {(c["name"] or "").lower().strip() for c in resolved}
    addable, seen = [], set()
    for p in roster:
        nl = (p["name"] or "").lower().strip()
        if not nl or nl in full_keys or nl in seen:
            continue
        seen.add(nl)
        addable.append(p["name"])
    addable.sort(key=str.lower)
    return jsonify({"candidates": resolved, "addable": addable})


@group_assign_bp.route("/admin/group-assign/candidates/add", methods=["POST"])
@login_required
@admin_required
def group_assign_candidates_add():
    from db import get_db, get_cursor
    from datetime import datetime
    from flask_login import current_user
    name = ((request.get_json(silent=True) or {}).get("name") or "").strip()
    if not name:
        return jsonify({"error": "Enter a name."}), 400
    if _is_blocked_assignee(name):
        return jsonify({"error": f"“{name}” is hard-blocked and can't be a candidate."}), 400
    conn = get_db(); cur = get_cursor(conn)
    try:
        cur.execute(
            "INSERT INTO assignment_candidates (name, name_key, created_at, created_by) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT (name_key) DO NOTHING",
            (name, name.lower().strip(), datetime.utcnow().isoformat(), current_user.id),
        )
        conn.commit()
    finally:
        cur.close(); conn.close()
    _scan_cache.clear()   # dropdown is rebuilt from candidates on next scan
    return jsonify({"ok": True, "candidates": _candidate_names()})


@group_assign_bp.route("/admin/group-assign/candidates/remove", methods=["POST"])
@login_required
@admin_required
def group_assign_candidates_remove():
    from db import get_db, get_cursor
    name = ((request.get_json(silent=True) or {}).get("name") or "").strip()
    if not name:
        return jsonify({"error": "Name required."}), 400
    conn = get_db(); cur = get_cursor(conn)
    try:
        cur.execute("DELETE FROM assignment_candidates WHERE name_key = %s", (name.lower().strip(),))
        conn.commit()
    finally:
        cur.close(); conn.close()
    _scan_cache.clear()
    return jsonify({"ok": True, "candidates": _candidate_names()})


@group_assign_bp.route("/admin/group-assign/change-date", methods=["POST"])
@login_required
@admin_required
def group_assign_change_date():
    """Move selected tasks to a different scheduled DATE in Breezeway.

    IRREVERSIBLE write. Same proven mechanism as the bear-fence mover: PATCH a
    date-only `scheduled_date` (Breezeway keeps the separate `scheduled_time`), then
    READ BACK the stored date so the response confirms the actual result, not just
    the PATCH status. Per-task pass/fail is reported — never a silent partial move."""
    token = _get_token()
    if not token:
        return jsonify({"error": "Breezeway not configured."})

    payload   = request.get_json(silent=True) or {}
    task_ids  = payload.get("task_ids") or []
    new_date  = (payload.get("new_date")  or "").strip()[:10]
    from_date = (payload.get("from_date") or "").strip()[:10]

    if not task_ids:
        return jsonify({"error": "No tasks selected."}), 400
    try:
        date.fromisoformat(new_date)
    except (TypeError, ValueError):
        return jsonify({"error": "Pick a valid target date (YYYY-MM-DD)."}), 400
    if new_date == from_date:
        return jsonify({"error": "The target date is the same as the current date — nothing to move."}), 400

    headers = {"Authorization": f"JWT {token}", "Content-Type": "application/json"}

    def _move_one(tid):
        url  = f"{BW_BASE}/public/inventory/v1/task/{tid}"
        last = "no attempt"
        # Retry on throttle / transient server errors so a move never gets silently
        # dropped because Breezeway was momentarily busy.
        for attempt in range(3):
            try:
                r = requests.patch(url, headers=headers,
                                   json={"scheduled_date": new_date}, timeout=20)
                last = f"status={r.status_code}"
                if r.status_code in (200, 201):
                    # Confirm by reading the date Breezeway actually stored.
                    after = None
                    try:
                        after = (r.json().get("scheduled_date") or "")[:10]
                    except Exception:
                        after = None
                    if not after:
                        try:
                            g = requests.get(url, headers={"Authorization": f"JWT {token}"}, timeout=15)
                            if g.ok:
                                after = (g.json().get("scheduled_date") or "")[:10]
                        except Exception:
                            pass
                    confirmed = (after == new_date)
                    return {"task_id": tid, "ok": True, "date_after": after, "confirmed": confirmed,
                            "detail": last + (" ✓ confirmed" if confirmed
                                              else f" ⚠ Breezeway returned {after or '?'} (expected {new_date})")}
                if r.status_code == 429 or r.status_code >= 500:
                    last += f" {r.text[:120]}"
                    time.sleep(0.4 * (attempt + 1))
                    continue
                # Non-retryable (e.g. 400/404)
                return {"task_id": tid, "ok": False, "date_after": None,
                        "detail": f"{last} {r.text[:160]}"}
            except Exception as e:
                last = str(e)
                time.sleep(0.4 * (attempt + 1))
        return {"task_id": tid, "ok": False, "date_after": None,
                "detail": f"failed after retries — {last}"}

    results = list(ThreadPoolExecutor(max_workers=8).map(_move_one, task_ids))
    _scan_cache.clear()   # dates changed — next scan should be fresh
    return jsonify({
        "results":    results,
        "from_date":  from_date,
        "new_date":   new_date,
        "ok_count":   sum(1 for x in results if x["ok"]),
        "fail_count": sum(1 for x in results if not x["ok"]),
    })
