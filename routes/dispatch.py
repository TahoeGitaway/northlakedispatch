"""
routes/dispatch.py — core dispatch routes: home map, saved routes,
optimize, matrix-row, public route viewer, portfolio.
"""

import json
import math
import os
from datetime import datetime

import requests
from flask import (Blueprint, render_template, request, jsonify,
                   redirect, url_for, flash, current_app)
from flask_login import login_required, current_user
from routes.auth import admin_required
from ortools.constraint_solver import pywrapcp, routing_enums_pb2

from db import (get_db, get_cursor, DEFAULT_START,
                CHECKIN_DEADLINE_HHMM, PRIORITY_CHECKIN_DEADLINE_HHMM,
                hhmm_to_minutes, minutes_to_hhmm)

dispatch_bp = Blueprint("dispatch", __name__)

GOOGLE_MAPS_KEY = os.environ.get("GOOGLE_MAPS_API_KEY", "")


# ── Breezeway import helper ───────────────────────────────────────

def _match_local_property_scored(bw_name: str, db_props: dict):
    """Fuzzy-match a Breezeway property name to a local DB property row.
    db_props: {lower_name: row_dict}.

    Returns (row_or_None, score, tier) where tier is one of
    exact / substring / keyword / none, and score is a 0..1
    character-level similarity used to flag low-confidence matches so the
    user can confirm or reject them (a Breezeway home not yet in the system
    otherwise silently matches the closest wrong house).

    There is deliberately NO character-similarity ("fuzzy") tier. It matched
    on shared text rather than the house name — e.g. "Front and Center at
    Olympic Valley" scored >0.72 against "Crown Peak at Olympic Valley" purely
    on the shared " at Olympic Valley" suffix and was applied as the wrong
    house. Per the strict-matching rule we prefer "not found" (add the home to
    the DB) over any silent wrong guess.
    """
    from difflib import SequenceMatcher
    if not bw_name:
        return None, 0.0, "none"
    key = bw_name.lower().strip()
    if key in db_props:
        return db_props[key], 1.0, "exact"
    # Substring — WORD-aligned only, so "venture" can't match inside "ad-venture".
    pk = " " + key + " "
    for dk, row in db_props.items():
        pdk = " " + dk + " "
        if pk in pdk or pdk in pk:
            return row, SequenceMatcher(None, key, dk).ratio(), "substring"
    kwords = set(key.split())
    for dk, row in db_props.items():
        if kwords and kwords.issubset(set(dk.split())):
            return row, SequenceMatcher(None, key, dk).ratio(), "keyword"
    # No fuzzy fallback — see the docstring. An unrecognized name is "not found",
    # never the closest-looking wrong house.
    return None, 0.0, "none"


# A match below this character-level similarity (and not exact) is treated as
# uncertain and surfaced to the user for confirmation.
_MATCH_CONFIDENT = 0.72


def _match_local_property(bw_name: str, db_props: dict):
    """Return the matched row only when we're CONFIDENT. Low-confidence matches
    return None here (used by the silent paths — discrepancy check, check-in
    detection — which must never act on a shaky match). The import uses the
    scored version directly so it can surface uncertain matches for confirmation."""
    row, score, tier = _match_local_property_scored(bw_name, db_props)
    return row if (tier == "exact" or score >= _MATCH_CONFIDENT) else None


def _title_has_pci(title: str) -> bool:
    """True if 'PCI' appears as a standalone token in a task title. A PCI Walk Thru
    is a priority check-in (must arrive by noon) — it lives only in the title and is
    easy to overlook, so we surface it automatically. ANY punctuation around the
    token counts as a separator, so '(PCI)', 'PCI.', 'PCI*', 'Walk Thru-PCI' all
    still match — a stray bracket must never let a noon arrival slip through."""
    import re
    t = " " + re.sub(r"[^a-z0-9]+", " ", (title or "").lower()) + " "
    return " pci " in t


def _haversine_matrix(locations):
    """Fallback NxN drive-time matrix (seconds) when Google Maps API is unavailable."""
    n   = len(locations)
    mat = [[0.0] * n for _ in range(n)]
    for i in range(n):
        for j in range(n):
            if i == j:
                continue
            lat1, lng1 = math.radians(locations[i]["lat"]), math.radians(locations[i]["lng"])
            lat2, lng2 = math.radians(locations[j]["lat"]), math.radians(locations[j]["lng"])
            dlat, dlng = lat2 - lat1, lng2 - lng1
            a    = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlng/2)**2
            dist = 6_371_000 * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
            # Tahoe roads: 1.3× winding-road factor, avg 40 mph (17.88 m/s).
            # Calibrated against real Google drive times (the old 1.8×/25 mph
            # overestimated by ~2.2×, e.g. 101 min for a 45-min leg).
            mat[i][j] = dist * 1.3 / 17.88
    return mat


def _decode_polyline(encoded):
    """Decode a Google Maps encoded polyline string to [[lat, lng], ...]."""
    coords = []
    index = lat = lng = 0
    while index < len(encoded):
        shift = result = 0
        while True:
            b = ord(encoded[index]) - 63
            index += 1
            result |= (b & 0x1f) << shift
            shift += 5
            if b < 0x20:
                break
        lat += ~(result >> 1) if result & 1 else result >> 1
        shift = result = 0
        while True:
            b = ord(encoded[index]) - 63
            index += 1
            result |= (b & 0x1f) << shift
            shift += 5
            if b < 0x20:
                break
        lng += ~(result >> 1) if result & 1 else result >> 1
        coords.append([lat / 1e5, lng / 1e5])
    return coords


def _google_distance_matrix(locations):
    """NxN drive-time matrix (seconds) via Google Distance Matrix API.
    Returns (matrix, error_str). error_str is None on success.
    Batches requests to stay within the 100-element-per-request limit."""
    if not GOOGLE_MAPS_KEY:
        return _haversine_matrix(locations), "GOOGLE_MAPS_API_KEY is not set on this server."
    n   = len(locations)
    mat = [[0.0] * n for _ in range(n)]
    # API limits: 25 origins, 25 destinations, 100 elements (origins×dests) per request.
    MAX_ORIG = 25
    MAX_DEST = 25
    MAX_ELEM = 100
    try:
        for orig_start in range(0, n, MAX_ORIG):
            orig_end   = min(orig_start + MAX_ORIG, n)
            orig_count = orig_end - orig_start
            orig_pipe  = "|".join(f"{loc['lat']},{loc['lng']}" for loc in locations[orig_start:orig_end])
            # Cap at MAX_DEST — tail batches with few origins would otherwise
            # exceed Google's 25-destination-per-request hard limit.
            dest_batch = max(1, min(MAX_DEST, MAX_ELEM // orig_count))

            for dest_start in range(0, n, dest_batch):
                dest_end  = min(dest_start + dest_batch, n)
                dest_pipe = "|".join(f"{loc['lat']},{loc['lng']}" for loc in locations[dest_start:dest_end])

                resp = requests.get(
                    "https://maps.googleapis.com/maps/api/distancematrix/json",
                    params={"origins": orig_pipe, "destinations": dest_pipe,
                            "mode": "driving", "key": GOOGLE_MAPS_KEY},
                    timeout=15,
                )
                data   = resp.json()
                status = data.get("status")
                if status != "OK":
                    msg = data.get("error_message") or status or "Unknown error"
                    return _haversine_matrix(locations), f"Distance Matrix API: {msg}"

                for i_local, row in enumerate(data.get("rows", [])):
                    i = orig_start + i_local
                    for j_local, elem in enumerate(row.get("elements", [])):
                        j = dest_start + j_local
                        if elem.get("status") == "OK":
                            mat[i][j] = float(elem["duration"]["value"])
                        elif i != j:
                            mat[i][j] = _haversine_matrix([locations[i], locations[j]])[0][1]
        return mat, None
    except Exception as e:
        return _haversine_matrix(locations), f"Distance Matrix request failed: {e}"


def _google_route_polyline(locations):
    """Decoded route coords [[lat, lng], ...] via Google Directions API.
    Returns (coords, error_str). error_str is None on success."""
    if not GOOGLE_MAPS_KEY or len(locations) < 2:
        return None, "GOOGLE_MAPS_API_KEY is not set on this server."
    try:
        origin = f"{locations[0]['lat']},{locations[0]['lng']}"
        dest   = f"{locations[-1]['lat']},{locations[-1]['lng']}"
        params = {"origin": origin, "destination": dest,
                  "mode": "driving", "key": GOOGLE_MAPS_KEY}
        if len(locations) > 2:
            params["waypoints"] = "|".join(
                f"{loc['lat']},{loc['lng']}" for loc in locations[1:-1]
            )
        resp = requests.get(
            "https://maps.googleapis.com/maps/api/directions/json",
            params=params, timeout=10,
        )
        data = resp.json()
        status = data.get("status")
        if status != "OK" or not data.get("routes"):
            msg = data.get("error_message") or status or "No routes returned"
            return None, f"Directions API: {msg}"
        return _decode_polyline(data["routes"][0]["overview_polyline"]["points"]), None
    except Exception as e:
        return None, f"Directions request failed: {e}"


# ── Home (map) ────────────────────────────────────────────────────

@dispatch_bp.route("/")
@login_required
def home():
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        'SELECT "Property Name", "Unit Address", "Latitude", "Longitude" '
        'FROM properties WHERE "Latitude" IS NOT NULL AND "Longitude" IS NOT NULL'
    )
    rows = cur.fetchall()
    cur.execute("SELECT id, name FROM teams ORDER BY name ASC")
    teams = [{"id": t["id"], "name": t["name"]} for t in cur.fetchall()]
    cur.execute("""
        SELECT t.id FROM teams t
        JOIN team_memberships tm ON tm.team_id = t.id
        WHERE tm.user_id = %s ORDER BY t.name ASC LIMIT 1
    """, (current_user.id,))
    row = cur.fetchone()
    user_team_id = row["id"] if row else None
    cur.close(); conn.close()
    properties = [
        {"name": r["Property Name"], "address": r["Unit Address"],
         "lat": float(r["Latitude"]), "lng": float(r["Longitude"])}
        for r in rows
    ]
    return render_template(
        "map.html",
        properties=properties,
        property_count=len(properties),
        default_start=DEFAULT_START,
        teams=teams,
        user_team_id=user_team_id,
    )


# ── Portfolio (public) ────────────────────────────────────────────

@dispatch_bp.route("/portfolio")
def portfolio():
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        'SELECT "Property Name", "Unit Address", "Latitude", "Longitude" '
        'FROM properties WHERE "Latitude" IS NOT NULL AND "Longitude" IS NOT NULL '
        'ORDER BY "Property Name" ASC'
    )
    rows = cur.fetchall()
    cur.close(); conn.close()
    properties = [
        {"name": r["Property Name"], "address": r["Unit Address"],
         "lat": float(r["Latitude"]), "lng": float(r["Longitude"])}
        for r in rows
    ]
    return render_template("portfolio.html", properties=properties)


# ── Saved routes ──────────────────────────────────────────────────

@dispatch_bp.route("/routes")
@login_required
def saved_routes():
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("""
        SELECT r.id, r.name, r.assigned_to, r.route_date, r.created_at, r.updated_at,
               r.total_duration, r.driving_duration, r.distance, r.team_id,
               COALESCE(r.archived, 0) AS archived,
               COALESCE(r.created_by_display, u.name) AS created_by_name,
               lu.name AS last_edited_by_name
        FROM saved_routes r
        JOIN users u ON r.created_by = u.id
        LEFT JOIN users lu ON r.last_edited_by = lu.id
        ORDER BY r.route_date DESC, r.updated_at DESC
    """)
    routes = cur.fetchall()
    cur.execute("SELECT id, name FROM teams ORDER BY name ASC")
    teams = [{"id": t["id"], "name": t["name"]} for t in cur.fetchall()]
    cur.execute("""
        SELECT t.id FROM teams t
        JOIN team_memberships tm ON tm.team_id = t.id
        WHERE tm.user_id = %s ORDER BY t.name ASC LIMIT 1
    """, (current_user.id,))
    row = cur.fetchone()
    user_team_id = row["id"] if row else None
    cur.close(); conn.close()
    today = datetime.utcnow().strftime("%Y-%m-%d")
    return render_template("routes.html", routes=routes, now_date=today, teams=teams,
                           user_team_id=user_team_id)


@dispatch_bp.route("/routes/save", methods=["POST"])
@login_required
def save_route():
    data              = request.json or {}
    name              = (data.get("name") or "").strip()
    assigned_to       = (data.get("assigned_to") or "").strip()
    route_date        = (data.get("route_date") or "").strip()
    start_time        = (data.get("startTime") or "").strip() or None
    start_loc         = data.get("startLocation") or None
    end_loc           = data.get("endLocation") or None
    schedule          = data.get("schedule", [])
    stats             = data.get("stats", {})
    notes             = (data.get("notes") or "").strip() or None
    notes_public      = int(bool(data.get("notes_public", False)))
    team_id           = data.get("team_id") or None
    start_loc_json    = json.dumps(start_loc) if start_loc else None
    end_loc_json      = json.dumps(end_loc)   if end_loc   else None

    if not name:
        return jsonify({"error": "Route name is required."}), 400
    if not route_date:
        return jsonify({"error": "Route date is required."}), 400
    if not schedule:
        return jsonify({"error": "No stops to save."}), 400

    # Default to Property Specialist if no team given
    now  = datetime.utcnow().isoformat()
    conn = get_db()
    cur  = get_cursor(conn)
    if not team_id:
        cur.execute("SELECT id FROM teams WHERE name = 'Property Specialist'")
        ps = cur.fetchone()
        if ps:
            team_id = ps["id"]

    cur.execute(
        """INSERT INTO saved_routes
           (name, assigned_to, route_date, start_time, start_location_json, end_location_json,
            stops_json, total_duration, driving_duration, service_duration, distance,
            notes, notes_public, team_id,
            created_by, last_edited_by, created_at, updated_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
        (name, assigned_to or None, route_date, start_time, start_loc_json, end_loc_json,
         json.dumps(schedule),
         stats.get("total_duration", 0), stats.get("driving_duration", 0),
         stats.get("service_duration", 0), stats.get("distance", 0),
         notes, notes_public, team_id,
         current_user.id, current_user.id, now, now)
    )
    route_id = cur.fetchone()["id"]
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"success": True, "id": route_id})


@dispatch_bp.route("/routes/<int:route_id>/update", methods=["POST"])
@login_required
def update_route(route_id):
    data         = request.json or {}
    name           = (data.get("name") or "").strip()
    assigned_to    = (data.get("assigned_to") or "").strip()
    route_date     = (data.get("route_date") or "").strip()
    start_time     = (data.get("startTime") or "").strip() or None
    start_loc      = data.get("startLocation") or None
    end_loc        = data.get("endLocation") or None
    start_loc_json = json.dumps(start_loc) if start_loc else None
    end_loc_json   = json.dumps(end_loc)   if end_loc   else None
    schedule       = data.get("schedule", [])
    stats          = data.get("stats", {})
    notes          = (data.get("notes") or "").strip() or None
    notes_public   = int(bool(data.get("notes_public", False)))
    team_id        = data.get("team_id") or None

    if not schedule:
        return jsonify({"error": "No stops to save."}), 400

    now  = datetime.utcnow().isoformat()
    conn = get_db()
    cur  = get_cursor(conn)

    # Only update team_id when one is explicitly sent; otherwise leave it unchanged
    if team_id is not None:
        cur.execute(
            """UPDATE saved_routes SET
               name=%s, assigned_to=%s, route_date=%s, start_time=%s,
               start_location_json=%s, end_location_json=%s,
               stops_json=%s, total_duration=%s, driving_duration=%s,
               service_duration=%s, distance=%s,
               notes=%s, notes_public=%s, team_id=%s,
               last_edited_by=%s, updated_at=%s
               WHERE id=%s""",
            (name or None, assigned_to or None, route_date or None, start_time,
             start_loc_json, end_loc_json,
             json.dumps(schedule),
             stats.get("total_duration", 0), stats.get("driving_duration", 0),
             stats.get("service_duration", 0), stats.get("distance", 0),
             notes, notes_public, team_id,
             current_user.id, now, route_id)
        )
    else:
        cur.execute(
            """UPDATE saved_routes SET
               name=%s, assigned_to=%s, route_date=%s, start_time=%s,
               start_location_json=%s, end_location_json=%s,
               stops_json=%s, total_duration=%s, driving_duration=%s,
               service_duration=%s, distance=%s,
               notes=%s, notes_public=%s,
               last_edited_by=%s, updated_at=%s
               WHERE id=%s""",
            (name or None, assigned_to or None, route_date or None, start_time,
             start_loc_json, end_loc_json,
             json.dumps(schedule),
             stats.get("total_duration", 0), stats.get("driving_duration", 0),
             stats.get("service_duration", 0), stats.get("distance", 0),
             notes, notes_public,
             current_user.id, now, route_id)
        )

    conn.commit()
    cur.close(); conn.close()
    return jsonify({"success": True})


@dispatch_bp.route("/routes/<int:route_id>")
@login_required
def load_route(route_id):
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("SELECT * FROM saved_routes WHERE id = %s", (route_id,))
    row  = cur.fetchone()
    cur.close(); conn.close()

    if not row:
        flash("Route not found.", "error")
        return redirect(url_for("dispatch.saved_routes"))

    schedule = json.loads(row["stops_json"])
    try:
        start_loc = json.loads(row["start_location_json"]) if row.get("start_location_json") else None
    except Exception:
        start_loc = None
    try:
        end_loc = json.loads(row["end_location_json"]) if row.get("end_location_json") else None
    except Exception:
        end_loc = None

    return jsonify({
        "id":               row["id"],
        "name":             row["name"],
        "assigned_to":      row["assigned_to"] or "",
        "route_date":       row["route_date"],
        "start_time":       row.get("start_time") or "",
        "start_location":   start_loc,
        "end_location":     end_loc,
        "schedule":         schedule,
        "total_duration":   row["total_duration"],
        "driving_duration": row["driving_duration"],
        "service_duration": row["service_duration"],
        "distance":         row["distance"],
        "notes":            row.get("notes") or "",
        "notes_public":     bool(row.get("notes_public")),
        "team_id":          row.get("team_id"),
    })


@dispatch_bp.route("/routes/<int:route_id>/delete", methods=["POST"])
@login_required
def delete_route(route_id):
    from flask import jsonify
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("DELETE FROM saved_routes WHERE id = %s RETURNING id", (route_id,))
    deleted = cur.fetchone()
    conn.commit()
    cur.close(); conn.close()
    if not deleted:
        return jsonify({"error": f"Route {route_id} not found."}), 404
    return jsonify({"success": True})


@dispatch_bp.route("/routes/<int:route_id>/archive", methods=["POST"])
@login_required
def archive_route(route_id):
    """Soft-cancel a route: keep the record but hide it from the saved-routes
    tiles AND exclude it from the AI day summary. Body {archived: false} to undo."""
    data     = request.get_json(silent=True) or {}
    archived = 0 if data.get("archived") is False else 1   # default: archive it
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("UPDATE saved_routes SET archived = %s WHERE id = %s RETURNING id",
                (archived, route_id))
    row = cur.fetchone()
    conn.commit()
    cur.close(); conn.close()
    if not row:
        return jsonify({"error": f"Route {route_id} not found."}), 404
    return jsonify({"success": True, "archived": archived})


# ── OR-Tools solver ───────────────────────────────────────────────

# OR-Tools RoutingModel.status() codes → human names, so a failed solve
# reports WHAT the solver actually returned instead of a hardcoded guess.
_ROUTING_STATUS_NAMES = {
    0: "ROUTING_NOT_SOLVED",
    1: "ROUTING_SUCCESS",
    2: "ROUTING_FAIL",            # no feasible solution exists (over-constrained)
    3: "ROUTING_FAIL_TIMEOUT",    # ran out of time before finding one
    4: "ROUTING_INVALID",         # the model itself is malformed
    5: "ROUTING_PARTIAL_SUCCESS_LOCAL_OPTIMUM_NOT_REACHED",
    6: "ROUTING_INFEASIBLE",
}


def _solve_route(
    duration_matrix, service_times_sec, checkin_flags, priority_flags,
    deadline_offset_sec=None, priority_deadline_offset_sec=None,
    hard_deadline=False, soft_deadline_penalty=False,
    end_node=0, front_flags=None, pass_label="",
):
    size    = len(duration_matrix)
    if end_node == 0:
        manager = pywrapcp.RoutingIndexManager(size, 1, 0)
    else:
        manager = pywrapcp.RoutingIndexManager(size, 1, [0], [end_node])
    routing = pywrapcp.RoutingModel(manager)

    def time_callback(from_index, to_index):
        fn = manager.IndexToNode(from_index)
        tn = manager.IndexToNode(to_index)
        drive = float(duration_matrix[fn][tn] or 0)
        if math.isnan(drive): drive = 0.0
        svc = float(service_times_sec[fn] or 0)
        if math.isnan(svc): svc = 0.0
        return max(0, int(drive + svc))

    transit_cb = routing.RegisterTransitCallback(time_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_cb)

    # Horizon must exceed worst-case route time. 86 400 s (24 h) handles most
    # days, but 8+ properties with 3-hour cleans hits 86 400 s of service alone.
    # Scale up so OR-Tools can always find a feasible solution.
    total_svc = sum(int(t or 0) for t in service_times_sec)
    horizon = max(86400, total_svc * 2 + 7200)
    routing.AddDimension(transit_cb, horizon, horizon, True, "Time")
    time_dim = routing.GetDimensionOrDie("Time")

    # Penalty must be large enough that violating a deadline is always worse
    # than any drive time savings. 100000 seconds >> any realistic Tahoe drive time.
    PENALTY = 100000
    for node_idx in range(1, size):
        if node_idx == end_node:
            continue  # end depot has no time-window constraints
        idx          = manager.NodeToIndex(node_idx)
        service_here = int(service_times_sec[node_idx] or 0)
        is_checkin   = bool(checkin_flags[node_idx])
        is_priority  = bool(priority_flags[node_idx])

        if is_priority and priority_deadline_offset_sec is not None:
            latest = max(0, int(priority_deadline_offset_sec - service_here))
            if hard_deadline:         time_dim.CumulVar(idx).SetRange(0, latest)
            if soft_deadline_penalty: time_dim.SetCumulVarSoftUpperBound(idx, latest, PENALTY * 2)
        elif is_checkin and deadline_offset_sec is not None:
            latest = max(0, int(deadline_offset_sec - service_here))
            if hard_deadline:         time_dim.CumulVar(idx).SetRange(0, latest)
            if soft_deadline_penalty: time_dim.SetCumulVarSoftUpperBound(idx, latest, PENALTY)

    # "Go first" stops form a FRONT BLOCK: every front stop is visited before every
    # non-front stop (pure ordering, no deadline). The solver still optimizes the
    # order WITHIN the front block and WITHIN the rest. Applied in every pass.
    if front_flags and any(front_flags[1:]):
        cp_solver = routing.solver()
        front_idx = [manager.NodeToIndex(n) for n in range(1, size)
                     if n != end_node and front_flags[n]]
        rear_idx  = [manager.NodeToIndex(n) for n in range(1, size)
                     if n != end_node and not front_flags[n]]
        for fi in front_idx:
            for ri in rear_idx:
                cp_solver.Add(time_dim.CumulVar(fi) <= time_dim.CumulVar(ri))

    params = pywrapcp.DefaultRoutingSearchParameters()
    params.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH

    # When time-window constraints are active, PATH_CHEAPEST_ARC often cannot
    # build a feasible initial solution (it ignores time windows during greedy
    # construction). LOCAL_CHEAPEST_INSERTION inserts each node into the cheapest
    # *feasible* position, so it respects hard deadlines from the start and gives
    # GLS a valid solution to improve. For unconstrained passes PATH_CHEAPEST_ARC
    # is fine and slightly faster.
    if hard_deadline or soft_deadline_penalty or (front_flags and any(front_flags[1:])):
        params.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.LOCAL_CHEAPEST_INSERTION
    else:
        params.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC

    # Give OR-Tools enough time to find good solutions.
    # Priority-constrained passes get more time because getting check-ins
    # before noon is worth the extra seconds. Google Matrix runs also get
    # more time — real drive times create tighter windows and harder problems.
    n_stops = max(1, size - 1)
    if hard_deadline:
        secs = max(8, min(15, n_stops))     # 8-15 s; GLS needs room to explore
    elif soft_deadline_penalty:
        secs = max(6, min(12, n_stops))     # 6-12 s
    else:
        secs = max(5, min(10, n_stops))     # 5-10 s; unconstrained TSP
    params.time_limit.FromSeconds(secs)

    solution = routing.SolveWithParameters(params)
    status_code = routing.status()
    status_name = _ROUTING_STATUS_NAMES.get(status_code, f"UNKNOWN({status_code})")
    if not solution:
        # Log the REAL reason. horizon/n_stops/time-budget included so a
        # timeout vs. genuine infeasibility is distinguishable from the logs.
        current_app.logger.error(
            f"optimize: pass '{pass_label or 'unlabeled'}' found no solution — "
            f"solver status={status_name}, horizon={horizon}s, n_stops={n_stops}, "
            f"time_limit={secs}s, hard_deadline={hard_deadline}, "
            f"soft_penalty={soft_deadline_penalty}, "
            f"front_block={bool(front_flags and any(front_flags[1:]))}"
        )
        return None, None, status_name

    index = routing.Start(0)
    ordered_nodes, arrival_times_sec = [], []
    while True:
        node = manager.IndexToNode(index)
        ordered_nodes.append(node)
        arrival_times_sec.append(solution.Value(time_dim.CumulVar(index)))
        if routing.IsEnd(index): break
        index = solution.Value(routing.NextVar(index))

    return ordered_nodes, arrival_times_sec, status_name


# ── Optimize ──────────────────────────────────────────────────────

@dispatch_bp.route("/optimize", methods=["POST"])
@login_required
def optimize():
    data            = request.json or {}
    stops           = data.get("stops", [])
    start           = data.get("start") or DEFAULT_START
    end_raw         = data.get("end")  or data.get("start") or DEFAULT_START
    start_time_hhmm = (data.get("startTime") or "09:30").strip()
    drive_only      = bool(data.get("drive_only", False))

    if not stops:
        return jsonify({"error": "No stops were included in the request. Add at least one property before optimizing."}), 400

    try:
        start_minutes = hhmm_to_minutes(start_time_hhmm)
    except ValueError as e:
        return jsonify({"error": f"The start time '{start_time_hhmm}' isn't valid. Use HH:MM format (e.g. 09:30). Detail: {e}"}), 400

    deadline_minutes          = hhmm_to_minutes(CHECKIN_DEADLINE_HHMM)
    priority_deadline_minutes = hhmm_to_minutes(PRIORITY_CHECKIN_DEADLINE_HHMM)

    try:
        start = {
            "name": start.get("name"),
            "lat":  float(start.get("lat")),
            "lng":  float(start.get("lng")),
        }
    except Exception as e:
        return jsonify({"error": f"The start location is missing valid coordinates. Make sure lat and lng are numbers. Detail: {e}"}), 400

    try:
        end = {
            "name": end_raw.get("name"),
            "lat":  float(end_raw.get("lat")),
            "lng":  float(end_raw.get("lng")),
        }
    except Exception:
        end = start

    # Determine whether start and end are the same depot
    same_depot = (abs(start["lat"] - end["lat"]) < 1e-5 and
                  abs(start["lng"] - end["lng"]) < 1e-5)

    cleaned_stops = []
    for s in stops:
        try:
            cleaned_stops.append({
                "name":             s.get("name"),
                "lat":              float(s.get("lat")),
                "lng":              float(s.get("lng")),
                "arrival":          bool(s.get("arrival", False)),
                "priority_checkin": bool(s.get("priority_checkin", False)),
                "go_first":         bool(s.get("go_first", False)),
                "serviceMinutes":   int(s.get("serviceMinutes", 60)),
            })
        except Exception:
            continue

    if not cleaned_stops:
        return jsonify({"error": "None of the submitted stops had valid coordinates (lat/lng). This usually means the property list is out of sync — try refreshing the page and re-adding your stops."}), 400

    preserve_order = bool(data.get("preserve_order", False))

    if not preserve_order:
        # Pre-sort so priority check-ins become low-numbered nodes (1, 2, 3…).
        # LOCAL_CHEAPEST_INSERTION processes nodes in order, so earlier nodes get
        # inserted first and tend to land earlier in the final route — which is
        # exactly what we need for stops that must finish before noon.
        cleaned_stops.sort(key=lambda s: (
            0 if s.get("priority_checkin") else (1 if s.get("arrival") else 2)
        ))

    # Build location list. When end differs from start, append it as the
    # final node so OR-Tools can route to it instead of looping back.
    if same_depot:
        all_locations = [start] + cleaned_stops
        end_node      = 0
    else:
        all_locations = [start] + cleaned_stops + [end]
        end_node      = len(all_locations) - 1

    # Build drive-time matrix.
    # Default: haversine approximation (free, fast, good enough for ordering).
    # Optional: Google Distance Matrix API (accurate real drive times; ~$0.005/element).
    use_google_matrix = bool(data.get("use_google_matrix", False))
    if use_google_matrix:
        duration_matrix, google_error = _google_distance_matrix(all_locations)
        if google_error:
            return jsonify({"error": f"Google Maps API failed — {google_error}"}), 502
    else:
        duration_matrix = _haversine_matrix(all_locations)

    # ── Pre-flight data sanity ────────────────────────────────────────
    # Catch the problems that make OR-Tools infeasible on EVERY pass and
    # report the SPECIFIC offending stop, instead of the old generic guess.
    #
    # (a) Unresolved coordinates. A stop whose address failed to geocode
    #     lands at ~0,0 (null island). float(0.0) passes the earlier lat/lng
    #     validation, so it slips through — but the haversine leg to reach it
    #     is ~400 h, which alone blows the solver horizon and kills all passes.
    def _loc_label(i, loc):
        return loc.get("name") or (f"end location" if i == end_node
                                    else f"start location" if i == 0
                                    else f"stop #{i}")
    null_island = [
        _loc_label(i, loc) for i, loc in enumerate(all_locations)
        if abs(loc["lat"]) < 0.01 and abs(loc["lng"]) < 0.01
    ]
    if null_island:
        names = ", ".join(null_island)
        current_app.logger.error(
            f"optimize: unresolved (0,0) coordinates on: {names}")
        return jsonify({"error":
            f"These locations didn't resolve to a real place — their "
            f"coordinates are 0, 0, which means the address failed to "
            f"geocode: {names}. Fix or remove them, then re-optimize."}), 400

    # (b) Unreachable stop. Even the shortest way in exceeds the horizon that
    #     _solve_route will use, so no route can ever include it. Report which
    #     stop and how long its best leg is, in hours.
    _svc_for_horizon = 0 if drive_only else sum(
        max(0, int(s.get("serviceMinutes", 60))) * 60 for s in cleaned_stops)
    _horizon = max(86400, _svc_for_horizon * 2 + 7200)
    if not preserve_order:
        for j in range(1, len(all_locations)):
            if j == end_node:
                continue
            incoming = [float(duration_matrix[i][j] or 0)
                        for i in range(len(all_locations)) if i != j]
            if incoming and min(incoming) > _horizon:
                label = _loc_label(j, all_locations[j])
                best_h = min(incoming) / 3600.0
                current_app.logger.error(
                    f"optimize: '{label}' is unreachable — shortest leg in is "
                    f"{best_h:.1f}h, horizon is {_horizon/3600:.1f}h "
                    f"(likely a bad coordinate at "
                    f"{all_locations[j]['lat']},{all_locations[j]['lng']})")
                return jsonify({"error":
                    f"'{label}' can't be reached: the shortest drive to it is "
                    f"{best_h:.0f} hours, which is impossible in a day. Its "
                    f"coordinate ({all_locations[j]['lat']}, "
                    f"{all_locations[j]['lng']}) is almost certainly wrong — "
                    f"check that its address geocoded correctly."}), 400

    if preserve_order:
        # Skip OR-Tools — keep stops in the order provided and compute arrivals
        # by summing sequential drive legs: depot→1→2→…→N.
        t = 0
        node_arrival_sec = {0: 0}
        for i in range(1, len(all_locations)):
            t += float(duration_matrix[i - 1][i]) if duration_matrix[i - 1][i] else 0.0
            node_arrival_sec[i] = t
        ordered_nodes             = list(range(len(all_locations)))
        used_deadline_constraints = False
        used_soft_penalties       = False
    elif drive_only:
        service_times_sec = [0] * len(all_locations)
        checkin_flags     = [False] * len(all_locations)
        priority_flags    = [False] * len(all_locations)
        ordered_nodes, arrival_times_sec, drive_status = _solve_route(
            duration_matrix, service_times_sec, checkin_flags, priority_flags,
            end_node=end_node, pass_label="drive-only"
        )
        if ordered_nodes is None:
            return jsonify({"error":
                f"The optimizer found no drive-only route. OR-Tools returned "
                f"'{drive_status}' for {len(cleaned_stops)} stops. If that's "
                f"ROUTING_FAIL_TIMEOUT the problem is just slow; anything else "
                f"means the model rejected it. (Full detail is in the server "
                f"log.)"}), 500
        used_deadline_constraints = used_soft_penalties = False
    else:
        stop_service = [max(0, int(s.get("serviceMinutes", 60))) * 60 for s in cleaned_stops]
        stop_checkin = [bool(s.get("arrival", False)) for s in cleaned_stops]
        stop_priority= [bool(s.get("priority_checkin", False)) for s in cleaned_stops]
        stop_front   = [bool(s.get("go_first", False)) for s in cleaned_stops]
        # End node (when different from start) gets zero service time / no flags
        if same_depot:
            service_times_sec = [0] + stop_service
            checkin_flags     = [False] + stop_checkin
            priority_flags    = [False] + stop_priority
            front_flags       = [False] + stop_front
        else:
            service_times_sec = [0] + stop_service + [0]
            checkin_flags     = [False] + stop_checkin + [False]
            priority_flags    = [False] + stop_priority + [False]
            front_flags       = [False] + stop_front + [False]

        has_checkins = any(checkin_flags[1:])
        has_priority = any(priority_flags[1:])

        # Always compute offsets. Cap at 0 when already past the deadline so
        # soft penalties still fire (bound=0 means "as early as possible").
        deadline_offset_sec          = max(0, (deadline_minutes - start_minutes) * 60)
        priority_deadline_offset_sec = max(0, (priority_deadline_minutes - start_minutes) * 60)

        # Only pass an offset to _solve_route when the relevant flag type exists.
        checkin_deadline_sec  = deadline_offset_sec  if has_checkins else None
        priority_deadline_sec = priority_deadline_offset_sec if has_priority else None

        ordered_nodes, arrival_times_sec = None, None
        used_deadline_constraints = used_soft_penalties = False
        # Record which passes ran and what the solver returned for each, so a
        # total failure can report the truth ("pass 3 = ROUTING_FAIL") rather
        # than guess at causes. Passes we skip are labelled "not attempted".
        pass_status = {"1 hard": "not attempted",
                       "2 soft": "not attempted",
                       "3 unconstrained": "not attempted"}

        # Pass 1 — hard constraints. Only attempt when we haven't already blown
        # past the deadline (a hard bound of 0 would make everything infeasible).
        before_checkin_deadline  = start_minutes < deadline_minutes
        before_priority_deadline = start_minutes < priority_deadline_minutes
        if (has_checkins and before_checkin_deadline) or (has_priority and before_priority_deadline):
            ordered_nodes, arrival_times_sec, pass_status["1 hard"] = _solve_route(
                duration_matrix, service_times_sec, checkin_flags, priority_flags,
                deadline_offset_sec=checkin_deadline_sec if before_checkin_deadline else None,
                priority_deadline_offset_sec=priority_deadline_sec if before_priority_deadline else None,
                hard_deadline=True, end_node=end_node, front_flags=front_flags,
                pass_label="1 hard"
            )
            if ordered_nodes is not None:
                used_deadline_constraints = True

        # Pass 2 — soft penalties.
        if ordered_nodes is None and (has_checkins or has_priority):
            ordered_nodes, arrival_times_sec, pass_status["2 soft"] = _solve_route(
                duration_matrix, service_times_sec, checkin_flags, priority_flags,
                deadline_offset_sec=checkin_deadline_sec,
                priority_deadline_offset_sec=priority_deadline_sec,
                soft_deadline_penalty=True, end_node=end_node, front_flags=front_flags,
                pass_label="2 soft"
            )
            if ordered_nodes is not None:
                used_soft_penalties = True

        # Pass 3 — fallback (no deadlines, but still honor the go-first front block).
        if ordered_nodes is None:
            ordered_nodes, arrival_times_sec, pass_status["3 unconstrained"] = _solve_route(
                duration_matrix, service_times_sec, checkin_flags, priority_flags,
                end_node=end_node, front_flags=front_flags,
                pass_label="3 unconstrained"
            )
            if ordered_nodes is None:
                # Report the ACTUAL per-pass solver statuses. Note pass 3 has no
                # time windows at all, so "widen the time window" was always a
                # lie when this fired — the real signal is the status codes.
                detail = "; ".join(f"pass {k} → {v}" for k, v in pass_status.items())
                current_app.logger.error(
                    f"optimize: all passes failed for {len(cleaned_stops)} stops "
                    f"(front_block={any(front_flags[1:])}, "
                    f"has_checkins={has_checkins}, has_priority={has_priority}) — {detail}")
                return jsonify({"error":
                    f"No route on any pass. What the solver actually returned: "
                    f"{detail}. ROUTING_FAIL_TIMEOUT = too slow (fewer stops / "
                    f"simpler constraints). ROUTING_FAIL/INFEASIBLE = the model "
                    f"is over-constrained even with NO deadlines — usually a "
                    f"'go first' stop that can't be ordered, or a stop that "
                    f"can't be reached. Full detail is in the server log."}), 500

    if not preserve_order:
        node_arrival_sec = {}
        for pos, node in enumerate(ordered_nodes):
            if node not in node_arrival_sec:
                node_arrival_sec[node] = arrival_times_sec[pos]

    ordered_stop_nodes = [n for n in ordered_nodes[1:] if n != 0 and n != end_node]
    ordered_stops      = [all_locations[n] for n in ordered_stop_nodes]

    # Compute driving duration by summing matrix legs along the ordered route.
    driving_duration = 0.0
    prev = 0  # depot index
    for node in ordered_stop_nodes:
        row = duration_matrix[prev] if prev < len(duration_matrix) else []
        driving_duration += float(row[node]) if node < len(row) and row[node] else 0.0
        prev = node
    # Add the final leg from the last stop to the end location (when it differs from start).
    if end_node != 0:
        row = duration_matrix[prev] if prev < len(duration_matrix) else []
        driving_duration += float(row[end_node]) if end_node < len(row) and row[end_node] else 0.0

    service_duration = 0 if drive_only else sum(
        int(s.get("serviceMinutes", 60)) * 60 for s in ordered_stops
    )
    total_duration = driving_duration + service_duration

    schedule               = []
    late_checkins          = []
    late_priority_checkins = []

    for node in ordered_stop_nodes:
        stop             = all_locations[node]
        eta_minutes      = start_minutes + int(node_arrival_sec.get(node, 0) // 60)
        service_min      = 0 if drive_only else int(stop.get("serviceMinutes", 60))
        finish_min       = eta_minutes + service_min
        is_checkin       = False if drive_only else bool(stop.get("arrival", False))
        is_priority      = False if drive_only else bool(stop.get("priority_checkin", False))
        is_late          = is_checkin and finish_min > deadline_minutes
        is_priority_late = is_priority and finish_min > priority_deadline_minutes

        if is_late:          late_checkins.append(stop.get("name"))
        if is_priority_late: late_priority_checkins.append(stop.get("name"))

        schedule.append({
            "name":             stop.get("name"),
            "arrival":          is_checkin,
            "priority_checkin": is_priority,
            "go_first":         False if drive_only else bool(stop.get("go_first", False)),
            "late":             is_late,
            "priority_late":    is_priority_late,
            "serviceMinutes":   service_min,
            "eta":              minutes_to_hhmm(eta_minutes),
            "eta_minutes":      eta_minutes,
            "lat":              float(stop.get("lat")),
            "lng":              float(stop.get("lng")),
            "matrix_index":     node,
        })

    # Compute route polyline only when using Google Matrix (already paid for the API).
    # Haversine routes skip this — the frontend draws a dashed straight-line fallback.
    route_polyline = None
    if use_google_matrix:
        polyline_locs = [{"lat": start["lat"], "lng": start["lng"]}] + [
            {"lat": s["lat"], "lng": s["lng"]} for s in ordered_stops
        ]
        if not same_depot:
            polyline_locs.append({"lat": end["lat"], "lng": end["lng"]})
        if len(polyline_locs) <= 27:  # Google Directions cap: 25 waypoints + origin + dest
            route_polyline, _ = _google_route_polyline(polyline_locs)

    return jsonify({
        "distance":                  0,
        "total_duration":            total_duration,
        "driving_duration":          driving_duration,
        "service_duration":          service_duration,
        "start_time":                start_time_hhmm,
        "checkin_deadline":          CHECKIN_DEADLINE_HHMM,
        "priority_checkin_deadline": PRIORITY_CHECKIN_DEADLINE_HHMM,
        "schedule":                  schedule,
        "late_checkins":             late_checkins,
        "late_priority_checkins":    late_priority_checkins,
        "deadline_constraints_used": used_deadline_constraints,
        "soft_penalties_used":       used_soft_penalties,
        "drive_only":                drive_only,
        "duration_matrix":           duration_matrix,
        "route_polyline":            route_polyline,
        "start_minutes":             start_minutes,
    })


# ── Matrix row (work-in a stop) ───────────────────────────────────

@dispatch_bp.route("/matrix-row", methods=["POST"])
@login_required
def matrix_row():
    data     = request.json or {}
    new_stop = data.get("new_stop")
    existing = data.get("existing_stops", [])

    if not new_stop or not existing:
        return jsonify({"error": "Request is missing required fields. Expected 'new_stop' (a single stop object) and 'existing_stops' (a list of current stops)."}), 400

    all_locs = [new_stop] + existing
    matrix   = _haversine_matrix(all_locs)
    return jsonify({
        "from_new": matrix[0][1:],
        "to_new":   [matrix[i + 1][0] for i in range(len(existing))],
    })


# ── Geocode (address → lat/lng via Google Geocoding API) ─────────

@dispatch_bp.route("/geocode", methods=["POST"])
@login_required
def geocode():
    address = (request.json or {}).get("address", "").strip()
    if not address:
        return jsonify({"error": "No address was provided. Type an address before searching."}), 400
    if not GOOGLE_MAPS_KEY:
        return jsonify({"error": "Address lookup is unavailable — the Google Maps API key is not configured on the server. Contact your administrator. (GOOGLE_MAPS_API_KEY env var is missing.)"}), 500
    try:
        resp = requests.get(
            "https://maps.googleapis.com/maps/api/geocode/json",
            params={"address": address, "key": GOOGLE_MAPS_KEY},
            timeout=8,
        )
        data = resp.json()
        api_status = data.get("status")
        if api_status != "OK" or not data.get("results"):
            return jsonify({"error": f"Couldn't find that address. Try adding more detail (e.g. city and state). (Google Geocoding API status: {api_status})"}), 404
        result = data["results"][0]
        loc    = result["geometry"]["location"]
        name   = result.get("formatted_address", address)
        return jsonify({"name": name, "lat": loc["lat"], "lng": loc["lng"]})
    except requests.exceptions.Timeout:
        return jsonify({"error": "The address lookup timed out. Check your internet connection and try again. (Google Geocoding API did not respond within 8 seconds.)"}), 504
    except Exception as e:
        return jsonify({"error": f"Something went wrong during address lookup. Try again or contact your administrator. Detail: {e}"}), 500


# ── Route geometry (Google Directions polyline for Leaflet map) ───

@dispatch_bp.route("/route-geometry", methods=["POST"])
@login_required
def route_geometry():
    data      = request.json or {}
    locations = data.get("locations", [])
    # Google Directions allows 25 waypoints + origin + destination = 27 total
    if len(locations) < 2 or len(locations) > 27:
        return jsonify({"coords": None})
    coords, _ = _google_route_polyline(locations)
    return jsonify({"coords": coords})


# ── Public route viewer ───────────────────────────────────────────

@dispatch_bp.route("/view/<int:route_id>")
def view_route(route_id):
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        """SELECT r.id, r.name, r.assigned_to, r.route_date, r.stops_json,
                  r.total_duration, r.driving_duration, r.distance,
                  u.name AS created_by_name
           FROM saved_routes r
           JOIN users u ON r.created_by = u.id
           WHERE r.id = %s""",
        (route_id,)
    )
    row = cur.fetchone()
    cur.close(); conn.close()

    if not row:
        return render_template("view_route.html", error="Route not found."), 404

    schedule = json.loads(row["stops_json"])

    # Compute totals dynamically from the saved stop data so they always
    # reflect the current serviceMinutes, not the cached value from optimize time.
    real_stops = [s for s in schedule if not s.get("isLunch")]
    computed_service = sum(s.get("serviceMinutes", 0) * 60 for s in real_stops)
    computed_driving = 0
    for i in range(1, len(real_stops)):
        gap = (
            real_stops[i]["eta_minutes"]
            - real_stops[i - 1]["eta_minutes"]
            - real_stops[i - 1]["serviceMinutes"]
        ) * 60
        if gap > 0:
            computed_driving += gap
    computed_total = computed_service + computed_driving

    route_polyline = None

    return render_template("view_route.html",
        route_id         = row["id"],
        route_name       = row["name"],
        assigned_to      = row["assigned_to"] or "",
        route_date       = row["route_date"],
        schedule         = schedule,
        total_duration   = computed_total,
        driving_duration = computed_driving,
        distance         = row["distance"],
        created_by       = row["created_by_name"],
        route_polyline   = json.dumps(route_polyline or []),
        error            = None,
    )


# ── Breezeway import endpoint ─────────────────────────────────────

@dispatch_bp.route("/api/routes-for-date")
@login_required
def routes_for_date():
    date_str = (request.args.get("date") or "").strip()
    if not date_str:
        return jsonify({"routes": []})
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("""
        SELECT id, name, assigned_to, route_date
        FROM saved_routes
        WHERE route_date = %s AND COALESCE(archived, 0) = 0
        ORDER BY assigned_to ASC, name ASC
    """, (date_str,))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return jsonify({"routes": [dict(r) for r in rows]})


# Per-route discrepancy result cache. The check is an all-properties Breezeway scan
# that can run past the hosting proxy's timeout (→ HTTP 503 / "upstream error"). The
# backend keeps working after the gateway gives up, so we store the finished result
# and a quick retry returns it instantly instead of re-running the heavy scan — same
# rescue pattern as hot_tub_scan. Keyed by route_id; the explicit Recheck (force=1)
# bypasses it for a truly live re-check.
import time as _dt_time
_route_disc_cache: dict[int, dict] = {}   # route_id -> {"ts": float, "data": dict}
_ROUTE_DISC_TTL = 120


def _robust_property_tasks(token, ref_id, date_str):
    """Fetch ONE property's tasks for a date with retry/backoff, so a momentary
    Breezeway throttle (429 / 5xx) doesn't SILENTLY drop the whole property's
    tasks. Returns (tasks, ok); ok=False means it genuinely couldn't be loaded.
    Shared by the import, the discrepancy check, and clear-times."""
    from routes.briefing import _fetch_bw_endpoint
    import time as _time
    for attempt in range(3):
        r, _, status = _fetch_bw_endpoint(
            token, "/public/inventory/v1/task",
            {"reference_property_id": ref_id, "scheduled_date": f"{date_str},{date_str}"})
        if status == 200:
            return (r or [], True)
        if status is None or status == 429 or status >= 500:
            _time.sleep(0.3 * (attempt + 1))
            continue
        # Non-throttle error (e.g. 400) → try the alternate date param once.
        r2, _, st2 = _fetch_bw_endpoint(
            token, "/public/inventory/v1/task",
            {"reference_property_id": ref_id, "start_date": date_str, "end_date": date_str})
        return (r2 or [], True) if st2 == 200 else ([], False)
    return ([], False)


@dispatch_bp.route("/api/bw-import", methods=["POST"])
@login_required
def bw_import():
    """Fetch Breezeway tasks for a date; group by assignee when multiple are requested."""
    from routes.briefing import (
        _get_breezeway_token, _fetch_bw_endpoint,
        _get_property_name, _ensure_property_cache,
        _get_live_property_cache, _get_live_ref_cache,
    )
    from concurrent.futures import ThreadPoolExecutor

    body     = request.get_json() or {}
    date_str = (body.get("date") or "").strip()

    # Accept "assignees" (list) or legacy "assignee" (single string)
    raw = body.get("assignees") or []
    if isinstance(raw, str):
        raw = [raw]
    if not raw and body.get("assignee"):
        raw = [body["assignee"]]
    assignees = [a.strip() for a in raw if a.strip()]

    if not date_str:
        return jsonify({"error": "date is required"}), 400

    token = _get_breezeway_token()
    if not token:
        return jsonify({"error": "Could not authenticate with Breezeway"}), 503

    _ensure_property_cache()
    prop_cache = _get_live_property_cache()
    ref_cache  = _get_live_ref_cache()

    if not prop_cache:
        return jsonify({"error": "Breezeway property cache is empty — try again in a moment."}), 502

    pid_candidates = {}
    for bw_pid in prop_cache:
        ref_id = ref_cache.get(bw_pid)
        candidate = ref_id if ref_id else str(bw_pid)
        if candidate not in pid_candidates:
            pid_candidates[candidate] = bw_pid

    # Fetch tasks per property in parallel — retry/backoff so a throttled house
    # isn't silently dropped (which made the sidebar show fewer tasks than reality).
    all_results, failed_props = [], 0
    with ThreadPoolExecutor(max_workers=16) as executor:
        for tasks, ok in executor.map(
                lambda ref: _robust_property_tasks(token, ref, date_str),
                list(pid_candidates.keys())):
            all_results.extend(tasks)
            if not ok:
                failed_props += 1

    if not all_results:
        return jsonify({"matched": [], "unmatched": [], "failed_properties": failed_props,
                        "message": "No Breezeway tasks found for that date."
                                   + (f" (⚠ {failed_props} properties couldn't be loaded — retry.)" if failed_props else "")})

    # Load DB properties once
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        'SELECT "Property Name", "Latitude", "Longitude" FROM properties '
        'WHERE "Latitude" IS NOT NULL AND "Longitude" IS NOT NULL'
    )
    rows = cur.fetchall()
    cur.close(); conn.close()
    db_props = {r["Property Name"].lower().strip(): dict(r) for r in rows}

    # Fetch same-day check-ins; match to local DB property names by name
    # (ID-based matching is unreliable across Breezeway API endpoints).
    # Name matching is strict (exact/word-substring/keyword only) — an
    # unrecognized home is left unmatched rather than guessed.
    from routes.briefing import (
        _fetch_breezeway_checkins, _classify_reservation, _get_property_name
    )
    checkin_db_names = set()
    checkin_pids     = set()   # authoritative Breezeway property_ids that have an arrival
    for r in _fetch_breezeway_checkins(date_str):
        if _classify_reservation(r) == "block":
            continue
        # Collect the raw Breezeway property_id — the authoritative arrival signal,
        # independent of any name matching (same id-join the group-assign tool uses).
        pid = r.get("property_id") or r.get("home_id")
        if pid is not None:
            checkin_pids.add(str(pid))
        bw_name = _get_property_name(r.get("property_id"))
        row = _match_local_property(bw_name, db_props)
        if row:
            checkin_db_names.add(row["Property Name"])

    def _filter_by_assignee(tasks, asgn_lower):
        filtered = []
        for t in tasks:
            for a in (t.get("assignments") or []):
                names = [
                    a.get("name", ""),
                    a.get("full_name", ""),
                    f"{a.get('first_name','').strip()} {a.get('last_name','').strip()}".strip(),
                ]
                if any(asgn_lower in n.lower() for n in names if n):
                    filtered.append(t)
                    break
        return filtered

    def _matched_for(tasks_subset):
        seen_ids       = set()
        bw_names       = []
        bw_name_tasks  = {}
        bw_name_homeid = {}  # bw_name -> str(property_id) for arrival lookup
        for t in tasks_subset:
            # Date guard — never surface tasks from another date. Breezeway's
            # task param variants occasionally return off-date tasks, which then
            # bled into the route's task summary.
            t_date = (t.get("scheduled_date") or "")[:10]
            if t_date and t_date != date_str:
                continue
            home_id = t.get("home_id") or t.get("property_id")
            if home_id:
                bw_name = _get_property_name(home_id)
                if home_id not in seen_ids:
                    seen_ids.add(home_id)
                    bw_names.append(bw_name)
                    bw_name_homeid[bw_name] = str(home_id)
            else:
                bw_name = (t.get("property_name") or "").strip()
                if bw_name and bw_name not in bw_names:
                    bw_names.append(bw_name)
            if bw_name:
                task_name = (
                    t.get("name") or t.get("task_name") or
                    t.get("task_type") or t.get("type") or "Task"
                ).strip()
                asgn_list = []
                for a in (t.get("assignments") or []):
                    n = (a.get("full_name") or a.get("name") or
                         f"{a.get('first_name','').strip()} {a.get('last_name','').strip()}".strip())
                    if n:
                        asgn_list.append(n)
                bw_name_tasks.setdefault(bw_name, []).append(
                    {"task_name": task_name, "assignees": asgn_list, "date": t_date}
                )
        matched, uncertain, unmatched = [], [], []
        for bw_name in bw_names:
            row, score, tier = _match_local_property_scored(bw_name, db_props)
            if not row:
                unmatched.append(bw_name)
                continue
            tasks_here  = bw_name_tasks.get(bw_name, [])
            # Two independent signals, OR'd: the original property-NAME match PLUS an
            # authoritative property-ID match (this house's task home_id appears among
            # the day's arrival reservations). The id match can only ADD an arrival the
            # name match missed — it never removes one — so a currently-correct route
            # cannot regress. Fixes silent misses when a reservation name doesn't match.
            home_pid    = bw_name_homeid.get(bw_name)
            has_checkin = (row["Property Name"] in checkin_db_names) or \
                          (home_pid is not None and home_pid in checkin_pids)
            # "PCI" in a (Walk Thru) title → priority check-in, arrive by noon — but
            # ONLY when the arrival is the SAME day this schedule is for (a same-day
            # check-in exists). A PCI prepping for a next-day arrival is just a task
            # today: it must not grab priority attention, and isn't an arrival yet.
            is_pci = has_checkin and any(_title_has_pci(t.get("task_name", "")) for t in tasks_here)
            entry = {
                "name":             row["Property Name"],
                "lat":              float(row["Latitude"]),
                "lng":              float(row["Longitude"]),
                "tasks":            tasks_here,
                "arrival":          has_checkin,
                "priority_checkin": is_pci,
                # Breezeway's own home_id for this task — authoritative (not a fuzzy
                # match), so the sidebar can link straight to the property's calendar.
                "property_id":      bw_name_homeid.get(bw_name),
            }
            if tier == "exact" or score >= _MATCH_CONFIDENT:
                matched.append(entry)
            else:
                entry["bw_name"]     = bw_name
                entry["match_score"] = round(score, 2)
                uncertain.append(entry)
        return matched, uncertain, unmatched

    if len(assignees) > 1:
        by_assignee = {}
        for asgn in assignees:
            matched, uncertain, unmatched = _matched_for(_filter_by_assignee(all_results, asgn.lower()))
            by_assignee[asgn] = {"matched": matched, "uncertain": uncertain, "unmatched": unmatched}
        return jsonify({"by_assignee": by_assignee, "failed_properties": failed_props})

    subset = _filter_by_assignee(all_results, assignees[0].lower()) if assignees else all_results
    matched, uncertain, unmatched = _matched_for(subset)
    if not matched and not uncertain and not unmatched:
        return jsonify({"matched": [], "uncertain": [], "unmatched": [], "failed_properties": failed_props,
                        "message": "No Breezeway tasks found for that date/assignee."})
    return jsonify({"matched": matched, "uncertain": uncertain, "unmatched": unmatched,
                    "failed_properties": failed_props})


# ── Route discrepancy check ───────────────────────────────────────

def _bw_task_title(t: dict) -> str:
    title = (t.get("name") or t.get("task_name") or t.get("task_type") or t.get("type") or "Task")
    if isinstance(title, dict):
        title = title.get("value") or title.get("name") or "Task"
    return str(title).strip()


def _bw_assignee_match(task: dict, asgn_lower: str) -> bool:
    if not asgn_lower:
        return True
    for a in (task.get("assignments") or []):
        for n in (a.get("name", ""), a.get("full_name", ""),
                  f"{a.get('first_name','').strip()} {a.get('last_name','').strip()}".strip()):
            if n and asgn_lower in n.lower():
                return True
    return False


def _bw_get_raw(token: str, path: str):
    """Single raw GET against Breezeway. Returns (json_or_none, status_or_none)."""
    try:
        r = requests.get(f"https://api.breezeway.io{path}",
                         headers={"Authorization": f"JWT {token}"}, timeout=15)
        try:
            body = r.json()
        except Exception:
            body = None
        return body, r.status_code
    except Exception:
        return None, None


def _task_history_summary(token: str, task: dict) -> dict:
    """Best-effort 'who/when this task landed on the list'.

    Breezeway's public API exposure of task history is uncertain, so we look at
    fields already on the task, then try a few history/audit endpoints, and report
    what we found plus whether anything was available.
    """
    task_id = task.get("id")

    # Fields that may already be on the task object
    when = task.get("created_at") or task.get("date_added") or task.get("created")
    who  = None
    cb   = task.get("created_by") or task.get("added_by") or task.get("creator")
    if isinstance(cb, dict):
        who = cb.get("name") or f"{cb.get('first_name','')} {cb.get('last_name','')}".strip()
    elif isinstance(cb, str):
        who = cb

    # Try candidate history endpoints (may 404 / require elevated access)
    history = []
    for path in (f"/public/inventory/v1/task/{task_id}/history",
                 f"/public/inventory/v1/task/{task_id}/audit",
                 f"/public/inventory/v1/task/{task_id}/activity"):
        body, status = _bw_get_raw(token, path)
        if status == 200 and body:
            history = body if isinstance(body, list) else body.get("results", body.get("data", []))
            if history:
                break

    assigned_when = assigned_by = None
    for ev in (history or []):
        if not isinstance(ev, dict):
            continue
        ev_type = str(ev.get("type") or ev.get("action") or ev.get("event") or "").lower()
        if "assign" in ev_type or "create" in ev_type or "add" in ev_type:
            assigned_when = ev.get("created_at") or ev.get("timestamp") or ev.get("date") or assigned_when
            actor = ev.get("user") or ev.get("actor") or ev.get("created_by") or ev.get("by")
            if isinstance(actor, dict):
                assigned_by = actor.get("name") or f"{actor.get('first_name','')} {actor.get('last_name','')}".strip()
            elif isinstance(actor, str):
                assigned_by = actor

    return {
        "available":    bool(assigned_when or who or when),
        "who":          (assigned_by or who) or None,
        "when":         (assigned_when or when) or None,
        "from_history": bool(history),
    }


@dispatch_bp.route("/api/route-discrepancies")
@login_required
def route_discrepancies():
    """Compare a saved route against the assignee's CURRENT Breezeway tasks for that
    day. Reports tasks added to / removed from the person's list and time changes,
    with best-effort who/when for added tasks."""
    from routes.briefing import (
        _get_breezeway_token, _fetch_bw_endpoint, _get_property_name,
        _ensure_property_cache, _get_live_property_cache, _get_live_ref_cache,
    )
    from concurrent.futures import ThreadPoolExecutor

    try:
        route_id = int(request.args.get("route_id", ""))
    except (TypeError, ValueError):
        return jsonify({"error": "route_id required"}), 400

    # Serve a fresh cached result instantly unless the user asked for a live recheck
    # (force=1). This both absorbs the every-reopen refetches and rescues a request the
    # gateway already 503'd: the prior call finished server-side and cached the result.
    force = request.args.get("force") in ("1", "true", "yes")
    if not force:
        hit = _route_disc_cache.get(route_id)
        if hit and _dt_time.time() - hit["ts"] < _ROUTE_DISC_TTL:
            return jsonify({**hit["data"], "cached": True})

    conn = get_db(); cur = get_cursor(conn)
    cur.execute("SELECT id, name, assigned_to, route_date, stops_json "
                "FROM saved_routes WHERE id = %s", (route_id,))
    row = cur.fetchone()
    if not row:
        cur.close(); conn.close()
        return jsonify({"error": "route not found"}), 404
    assignee = (row["assigned_to"] or "").strip()
    date_str = str(row["route_date"])[:10]
    schedule = json.loads(row["stops_json"]) or []
    cur.execute('SELECT "Property Name", "Latitude", "Longitude" FROM properties '
                'WHERE "Latitude" IS NOT NULL AND "Longitude" IS NOT NULL')
    db_props = {r["Property Name"].lower().strip(): dict(r) for r in cur.fetchall()}
    cur.close(); conn.close()

    if not assignee:
        return jsonify({"error": "This route has no assignee, so there is no task list to compare against."}), 400

    # Saved route: property -> planned ETA minutes
    route_props, route_time, seen = [], {}, set()
    for s in schedule:
        if s.get("isLunch") or s.get("isGap"):
            continue
        nm = (s.get("name") or "").strip()
        if nm and nm.lower() not in seen:
            seen.add(nm.lower())
            route_props.append(nm)
            route_time[nm.lower()] = s.get("eta_minutes")

    token = _get_breezeway_token()
    if not token:
        return jsonify({"error": "Could not authenticate with Breezeway"}), 503
    _ensure_property_cache()
    prop_cache = _get_live_property_cache()
    ref_cache  = _get_live_ref_cache()
    if not prop_cache:
        return jsonify({"error": "Breezeway property cache empty — try again in a moment"}), 502

    pid_candidates = {}
    for bw_pid in prop_cache:
        ref_id = ref_cache.get(bw_pid)
        pid_candidates.setdefault(ref_id if ref_id else str(bw_pid), bw_pid)

    # Per-property fetch with retry/backoff (shared helper) so a throttled house
    # isn't silently dropped — same fix as the import. Count genuine failures so the
    # sidebar can warn instead of quietly showing fewer tasks than really exist.
    all_tasks = []
    failed_props = 0
    with ThreadPoolExecutor(max_workers=16) as ex:
        for tasks, ok in ex.map(lambda ref: _robust_property_tasks(token, ref, date_str),
                                 list(pid_candidates.keys())):
            all_tasks.extend(tasks)
            if not ok:
                failed_props += 1

    asgn_lower = assignee.lower()
    seen_ids, mine = set(), []
    for t in all_tasks:
        # STRICT date guard — the per-property task query returns EVERY task for the
        # house (Breezeway ignores the date param here), including recurring/undated
        # tasks (e.g. "Biweekly Hot Tub Service") that carry no scheduled_date. Those
        # must NOT appear: only tasks whose scheduled date is exactly this route's
        # date belong. No date / off-date → drop it.
        t_date = (t.get("scheduled_date") or "")[:10]
        if t_date != date_str:
            continue
        tid = t.get("id")
        if tid is not None and tid in seen_ids:
            continue
        if tid is not None:
            seen_ids.add(tid)
        if _bw_assignee_match(t, asgn_lower):
            mine.append(t)

    tasks_by_prop = {}
    for t in mine:
        pid = t.get("home_id") or t.get("property_id")
        bw_name = _get_property_name(pid) if pid else (t.get("property_name") or "")
        local = _match_local_property(bw_name, db_props)
        prop_name = local["Property Name"] if local else (bw_name or "Unknown property")
        tasks_by_prop.setdefault(prop_name, []).append(t)

    route_set = {p.lower() for p in route_props}
    present   = {p.lower() for p in tasks_by_prop}

    # Which of these properties have a guest check-in that day, so an added stop can
    # be auto-flagged as an arrival (matching the Breezeway import behaviour).
    from routes.briefing import _fetch_breezeway_checkins, _classify_reservation
    arrival_names = set()
    try:
        for r in _fetch_breezeway_checkins(date_str):
            if _classify_reservation(r) == "block":
                continue
            local = _match_local_property(_get_property_name(r.get("property_id")), db_props)
            if local:
                arrival_names.add(local["Property Name"])
    except Exception:
        pass

    added = []
    for prop_name, tlist in tasks_by_prop.items():
        if prop_name.lower() not in route_set:
            is_arrival = prop_name in arrival_names
            # PCI counts as a priority check-in only when its arrival is that same day
            # (a same-day check-in exists). A PCI for a next-day arrival is just a task.
            is_pci     = is_arrival and any(_title_has_pci(_bw_task_title(t)) for t in tlist)
            for t in tlist:
                added.append({"property": prop_name, "task_name": _bw_task_title(t),
                              "task_id": t.get("id"),
                              "arrival": is_arrival, "pci": is_pci,
                              "history": _task_history_summary(token, t)})

    removed = [{"property": p} for p in route_props if p.lower() not in present]

    # moved: property is in the route but its task time-of-day differs from the plan
    moved = []
    for prop_name, tlist in tasks_by_prop.items():
        key = prop_name.lower()
        if key not in route_set:
            continue
        planned = route_time.get(key)
        if planned is None:
            continue
        for t in tlist:
            sched = t.get("scheduled_date") or ""
            tod = sched[11:16] if len(sched) >= 16 else ""
            if not tod or tod == "00:00":
                continue
            task_min = int(tod[:2]) * 60 + int(tod[3:5])
            if abs(task_min - int(planned)) > 15:
                ph, pm = divmod(int(planned), 60)
                moved.append({"property": prop_name, "task_name": _bw_task_title(t),
                              "was": f"{ph % 24:02d}:{pm:02d}", "now": tod})

    # Full current task list for this person that day, grouped by house. `pci` marks a
    # SAME-DAY priority check-in (PCI title + a same-day arrival) so the route view can
    # flag it loudly; a PCI prepping for a next-day arrival is left unflagged.
    current_tasks = sorted(
        ({"property": p,
          "tasks": [_bw_task_title(t) for t in tlist],
          "pci": (p in arrival_names) and any(_title_has_pci(_bw_task_title(t)) for t in tlist)}
         for p, tlist in tasks_by_prop.items()),
        key=lambda x: x["property"].lower(),
    )

    payload = {
        "route_id": route_id, "assignee": assignee, "date": date_str,
        "added":   sorted(added,   key=lambda x: x["property"].lower()),
        "removed": sorted(removed, key=lambda x: x["property"].lower()),
        "moved":   sorted(moved,   key=lambda x: x["property"].lower()),
        "current_tasks": current_tasks,
        "history_available": any(a["history"].get("available") for a in added),
        "failed_properties": failed_props,
        "summary": {"added": len(added), "removed": len(removed), "moved": len(moved)},
    }
    # Cache before returning so the result survives even if the gateway already timed
    # out THIS request — the next call returns it instantly. Don't cache a run that
    # lost houses to throttling, or we'd pin an incomplete list for the whole TTL.
    if not failed_props:
        _route_disc_cache[route_id] = {"ts": _dt_time.time(), "data": payload}
    return jsonify(payload)


@dispatch_bp.route("/api/bw-task-probe")
@login_required
def bw_task_probe():
    """Admin diagnostic: dump a task's detail so we can see the `assignments` shape
    (needed to learn the assignee-id field for batch assign). Usage: ?task_id=123 —
    or NO args to AUTO-FIND a task that already has an assignee (optional
    ?date=YYYY-MM-DD, default today)."""
    from routes.briefing import (_get_breezeway_token, _fetch_bw_endpoint,
                                  _ensure_property_cache, _get_live_property_cache,
                                  _get_live_ref_cache)
    if not getattr(current_user, "is_admin", False):
        return jsonify({"error": "admin only"}), 403
    token = _get_breezeway_token()
    if not token:
        return jsonify({"error": "Could not authenticate with Breezeway"}), 503

    out = {}
    task_id = (request.args.get("task_id") or "").strip()

    # No id given → auto-find an already-assigned task so nobody has to hunt for one.
    if not task_id:
        from concurrent.futures import ThreadPoolExecutor
        from datetime import date as _date
        _ensure_property_cache()
        prop_cache = _get_live_property_cache()
        ref_cache  = _get_live_ref_cache()
        date_str   = (request.args.get("date") or _date.today().isoformat())[:10]
        ref_ids    = [ref_cache.get(p) or str(p) for p in prop_cache]

        def _tasks(ref_id):
            for dp in ({"scheduled_date": f"{date_str},{date_str}"},
                       {"start_date": date_str, "end_date": date_str}):
                r, _, status = _fetch_bw_endpoint(token, "/public/inventory/v1/task",
                                                  {"reference_property_id": ref_id, **dp})
                if status == 200:
                    return r or []
            return []

        found = None
        with ThreadPoolExecutor(max_workers=25) as ex:
            for tasks in ex.map(_tasks, ref_ids):
                for t in (tasks or []):
                    if t.get("assignments"):
                        found = t
                        break
                if found:
                    break
        if not found:
            out["auto_find"] = (f"No assigned tasks found on {date_str}. "
                                "Try ?date=YYYY-MM-DD with a busy day, or ?task_id=<id>.")
            return jsonify(out)
        out["auto_found"] = {"date": date_str, "task_id": found.get("id")}
        task_id = str(found.get("id"))

    detail, st = _bw_get_raw(token, f"/public/inventory/v1/task/{task_id}")
    out["task_detail"] = {
        "status":      st,
        "keys":        list(detail.keys()) if isinstance(detail, dict) else None,
        "assignments": detail.get("assignments") if isinstance(detail, dict) else None,
        "body":        detail,
    }
    return jsonify(out)


@dispatch_bp.route("/api/bw-task-history-probe")
@login_required
def bw_task_history_probe():
    """Admin READ-ONLY diagnostic: discover what task-history / assignment-audit data
    Breezeway's public API actually exposes, so we can honestly decide whether
    "who added / who removed this from the list" (and WHEN) is feasible. Makes only
    GET requests — writes nothing.

    Usage: ?task_id=123   or   no args (auto-finds an assigned task; optional ?date=).

    Read three things from the result:
      1. task_detail.creation_related_fields — does the task itself carry created_by /
         created_at, and is that CREATION (not who put it on this person's list)?
      2. task_detail.assignments — does an assignment entry carry a date / assigner, i.e.
         WHO added it to the list and WHEN (the thing we actually want)?
      3. history_endpoints — does any /history|/audit|/activity|/events|/log endpoint
         return 200 with real events? If all 404/403, "who removed it" is NOT feasible
         via the public API and we won't promise it.
    """
    from routes.briefing import (_get_breezeway_token, _fetch_bw_endpoint,
                                  _ensure_property_cache, _get_live_property_cache,
                                  _get_live_ref_cache)
    if not getattr(current_user, "is_admin", False):
        return jsonify({"error": "admin only"}), 403
    token = _get_breezeway_token()
    if not token:
        return jsonify({"error": "Could not authenticate with Breezeway"}), 503

    out = {}
    task_id = (request.args.get("task_id") or "").strip()

    # Auto-find an assigned task if none given (same approach as /api/bw-task-probe) so
    # nobody has to hunt for a task id just to run the probe.
    if not task_id:
        from concurrent.futures import ThreadPoolExecutor
        from datetime import date as _date
        _ensure_property_cache()
        prop_cache = _get_live_property_cache()
        ref_cache  = _get_live_ref_cache()
        date_str   = (request.args.get("date") or _date.today().isoformat())[:10]
        ref_ids    = [ref_cache.get(p) or str(p) for p in prop_cache]

        def _tasks(ref_id):
            r, _, status = _fetch_bw_endpoint(token, "/public/inventory/v1/task",
                                              {"reference_property_id": ref_id,
                                               "scheduled_date": f"{date_str},{date_str}"})
            return (r or []) if status == 200 else []

        found = None
        with ThreadPoolExecutor(max_workers=25) as ex:
            for tasks in ex.map(_tasks, ref_ids):
                for t in (tasks or []):
                    if t.get("assignments"):
                        found = t
                        break
                if found:
                    break
        if not found:
            return jsonify({"error": f"No assigned task found on {date_str}. "
                                     "Pass ?task_id=<id>, or ?date=<a busy day>."})
        out["auto_found"] = {"date": date_str, "task_id": found.get("id")}
        task_id = str(found.get("id"))

    # 1) Full task detail — surface any creation/assignment metadata already present.
    detail, st = _bw_get_raw(token, f"/public/inventory/v1/task/{task_id}")
    creation_fields, assignment_fields = {}, None
    if isinstance(detail, dict):
        for k in ("created_at", "created", "date_added", "added_at", "created_by",
                  "creator", "added_by", "updated_at", "updated_by",
                  "assigned_at", "assigned_by"):
            if k in detail:
                creation_fields[k] = detail.get(k)
        assignment_fields = detail.get("assignments")
    out["task_detail"] = {
        "status":                  st,
        "keys":                    list(detail.keys()) if isinstance(detail, dict) else None,
        "creation_related_fields": creation_fields,
        "assignments":             assignment_fields,
    }

    # 2) Candidate history/audit endpoints — report status + a small body sample for each,
    #    so we can see definitively whether Breezeway exposes assignment history at all.
    out["history_endpoints"] = {}
    for path in (f"/public/inventory/v1/task/{task_id}/history",
                 f"/public/inventory/v1/task/{task_id}/audit",
                 f"/public/inventory/v1/task/{task_id}/activity",
                 f"/public/inventory/v1/task/{task_id}/events",
                 f"/public/inventory/v1/task/{task_id}/log"):
        body, status = _bw_get_raw(token, path)
        if isinstance(body, list):
            sample = body[:3]
        elif isinstance(body, dict):
            sample = {k: body.get(k) for k in list(body.keys())[:8]}
        else:
            sample = body
        out["history_endpoints"][path] = {"status": status, "sample": sample}

    return jsonify(out)


@dispatch_bp.route("/api/bw-assign-test")
@login_required
def bw_assign_test():
    """Admin diagnostic that WRITES — run ONLY on a throwaway task. Tries several
    PATCH payload shapes to learn how Breezeway sets a task's assignee, re-reading
    the task after each to see which one actually stuck (and whether it ADDS or
    REPLACES). Also pulls staff-roster endpoints in the same call.
      Usage: ?task_id=123                 (assignee defaults to 250595 / Brian Nigon)
             ?task_id=123&assignee_id=250606
    """
    from routes.briefing import _get_breezeway_token
    if not getattr(current_user, "is_admin", False):
        return jsonify({"error": "admin only"}), 403
    task_id = (request.args.get("task_id") or "").strip()
    if not task_id:
        return jsonify({"error": "task_id required — use a THROWAWAY task; this WRITES."}), 400
    try:
        assignee_id = int(request.args.get("assignee_id") or 250595)
    except ValueError:
        return jsonify({"error": "assignee_id must be a number"}), 400
    token = _get_breezeway_token()
    if not token:
        return jsonify({"error": "Could not authenticate with Breezeway"}), 503

    headers = {"Authorization": f"JWT {token}", "Content-Type": "application/json"}
    url = f"https://api.breezeway.io/public/inventory/v1/task/{task_id}"

    def _assignees_now():
        d, _ = _bw_get_raw(token, f"/public/inventory/v1/task/{task_id}")
        if isinstance(d, dict):
            return [a.get("assignee_id") for a in (d.get("assignments") or [])]
        return None

    before = _assignees_now()
    attempts, winner = [], None
    payloads = [
        {"assignments": [{"assignee_id": assignee_id}]},
        {"assignee_ids": [assignee_id]},
        {"assignments": [assignee_id]},
        {"assigned_to":  [assignee_id]},
    ]
    for p in payloads:
        try:
            r = requests.patch(url, headers=headers, json=p, timeout=15)
            after = _assignees_now()
            stuck = bool(after and assignee_id in after)
            attempts.append({"PATCH": p, "status": r.status_code,
                             "assignees_after": after, "stuck": stuck,
                             "resp": (r.text or "")[:200]})
            if stuck:
                winner = {"PATCH": p}
                break
        except Exception as e:
            attempts.append({"PATCH": p, "error": str(e)})

    if not winner:
        for sub in (f"{url}/assignment", f"{url}/assignments"):
            try:
                r = requests.post(sub, headers=headers,
                                  json={"assignee_id": assignee_id}, timeout=15)
                after = _assignees_now()
                stuck = bool(after and assignee_id in after)
                attempts.append({"POST": sub, "status": r.status_code,
                                 "assignees_after": after, "stuck": stuck,
                                 "resp": (r.text or "")[:200]})
                if stuck:
                    winner = {"POST": sub, "body": {"assignee_id": assignee_id}}
                    break
            except Exception as e:
                attempts.append({"POST": sub, "error": str(e)})

    # Staff roster — try the likely list endpoints so the tool's dropdown has names.
    rosters = {}
    for path in ("/public/inventory/v1/user", "/public/inventory/v1/users",
                 "/public/inventory/v1/employee", "/public/inventory/v1/supplier"):
        body, status = _bw_get_raw(token, path)
        if status is not None:
            rosters[path] = {"status": status,
                             "sample": body[:3] if isinstance(body, list) else body}

    return jsonify({"task_id": task_id, "assignee_id": assignee_id,
                    "assignees_before": before, "winner": winner,
                    "attempts": attempts, "rosters": rosters})


@dispatch_bp.route("/api/bw-property-probe")
@login_required
def bw_property_probe():
    """Admin diagnostic (READ-ONLY): dump a property's full detail so we can find
    whether Breezeway exposes a 'group' field (and its exact name) for the
    batch-assign-by-group tool, plus try the likely group-list endpoints.
    Usage: ?property_id=123  (omit to auto-dump the first active property)."""
    from routes.briefing import _get_breezeway_token
    if not getattr(current_user, "is_admin", False):
        return jsonify({"error": "admin only"}), 403
    token = _get_breezeway_token()
    if not token:
        return jsonify({"error": "Could not authenticate with Breezeway"}), 503

    out = {}
    pid = (request.args.get("property_id") or "").strip()
    if pid:
        detail, st = _bw_get_raw(token, f"/public/inventory/v1/property/{pid}")
        out["property_detail"] = {"status": st,
                                  "keys": list(detail.keys()) if isinstance(detail, dict) else None,
                                  "body": detail}

    # Breezeway embeds groups per-property (no group-list endpoint — those 404).
    # Scan every active property to find ones that actually HAVE groups, so we can
    # see the array's shape (id/name?) and collect the full list of distinct groups.
    distinct = {}          # json-string -> group object (dedupes)
    props_with_groups = 0
    total_scanned = 0
    sample_array = None
    sample_property = None
    page = 1
    while page <= 5:
        listing, st = _bw_get_raw(
            token, f"/public/inventory/v1/property?limit=200&page={page}&status=active")
        items = []
        if isinstance(listing, dict):
            items = listing.get("results") or listing.get("data") or []
        elif isinstance(listing, list):
            items = listing
        if not items:
            break
        for p in items:
            if not isinstance(p, dict):
                continue
            total_scanned += 1
            groups = p.get("groups") or []
            if groups:
                props_with_groups += 1
                if sample_array is None:
                    sample_array    = groups
                    sample_property = p.get("name")
                for g in groups:
                    try:
                        distinct[json.dumps(g, sort_keys=True)] = g
                    except Exception:
                        distinct[str(g)] = g
        if len(items) < 200:
            break
        page += 1

    out["groups_scan"] = {
        "total_scanned":          total_scanned,
        "properties_with_groups": props_with_groups,
        "sample_groups_array":    sample_array,      # shape of one property's groups
        "sample_from_property":   sample_property,
        "distinct_groups":        list(distinct.values()),  # every group seen
    }
    return jsonify(out)

# ── Remove all assigned task times for a person on a day (admin, destructive) ──

def _clear_task_time(token: str, task_id) -> tuple:
    """Clear a task's scheduled start time in Breezeway (PATCH scheduled_time=null)."""
    headers = {"Authorization": f"JWT {token}", "Content-Type": "application/json"}
    url = f"https://api.breezeway.io/public/inventory/v1/task/{task_id}"
    try:
        r = requests.patch(url, headers=headers, json={"scheduled_time": None}, timeout=15)
        ok = r.status_code in (200, 201)
        return ok, f"status={r.status_code}" + ("" if ok else f" {r.text[:160]}")
    except Exception as e:
        return False, str(e)


@dispatch_bp.route("/admin/clear-task-times", methods=["POST"])
@login_required
@admin_required
def clear_task_times():
    """Remove the assigned start time from EVERY Breezeway task in a person's name
    on a given day. Destructive — only invoked from the confirmed UI action."""
    from routes.briefing import (
        _get_breezeway_token, _fetch_bw_endpoint, _get_property_name,
        _ensure_property_cache, _get_live_property_cache, _get_live_ref_cache,
    )
    from concurrent.futures import ThreadPoolExecutor

    body     = request.get_json() or {}
    date_str = (body.get("date") or "").strip()
    # Accept multiple people: "assignees" (list) or legacy "assignee" (single).
    raw = body.get("assignees")
    if isinstance(raw, list):
        assignees = [a.strip() for a in raw if a and str(a).strip()]
    else:
        single = (body.get("assignee") or "").strip()
        assignees = [single] if single else []
    if not date_str:
        return jsonify({"error": "A date is required."}), 400
    if not assignees:
        return jsonify({"error": "At least one person is required."}), 400
    asgn_lowers = [a.lower() for a in assignees]

    token = _get_breezeway_token()
    if not token:
        return jsonify({"error": "Could not authenticate with Breezeway"}), 503
    _ensure_property_cache()
    prop_cache = _get_live_property_cache()
    ref_cache  = _get_live_ref_cache()
    if not prop_cache:
        return jsonify({"error": "Breezeway property cache empty — try again in a moment"}), 502

    pid_candidates = {}
    for bw_pid in prop_cache:
        ref_id = ref_cache.get(bw_pid)
        pid_candidates.setdefault(ref_id if ref_id else str(bw_pid), bw_pid)

    # Per-property fetch with retry/backoff (shared helper) so a throttled house
    # isn't silently dropped — same fix as the import.
    all_tasks = []
    with ThreadPoolExecutor(max_workers=16) as ex:
        for tasks, _ok in ex.map(lambda ref: _robust_property_tasks(token, ref, date_str),
                                 list(pid_candidates.keys())):
            all_tasks.extend(tasks)

    seen, mine = set(), []
    for t in all_tasks:
        t_date = (t.get("scheduled_date") or "")[:10]
        if t_date and t_date != date_str:          # only this exact day
            continue
        tid = t.get("id")
        if tid is None or tid in seen:
            continue
        seen.add(tid)
        if any(_bw_assignee_match(t, al) for al in asgn_lowers):
            mine.append(t)

    # Clear the times in PARALLEL — the person can have many tasks, and a
    # sequential loop (15s timeout each) was the slowest, avoidable part.
    def _clear_one(t):
        ok, detail = _clear_task_time(token, t.get("id"))
        pid = t.get("home_id") or t.get("property_id")
        asgn = []
        for a in (t.get("assignments") or []):
            n = (a.get("full_name") or a.get("name") or
                 f"{a.get('first_name','').strip()} {a.get('last_name','').strip()}".strip())
            if n:
                asgn.append(n)
        return {"task":      _bw_task_title(t),
                "property":  _get_property_name(pid) if pid else "",
                "date":      (t.get("scheduled_date") or "")[:16],
                "assignees": asgn,
                "task_id":   t.get("id"),
                "ok":        ok,
                "detail":    detail}

    results = list(ThreadPoolExecutor(max_workers=10).map(_clear_one, mine)) if mine else []
    cleared = sum(1 for r in results if r["ok"])
    failed  = sum(1 for r in results if not r["ok"])

    return jsonify({"date": date_str, "assignee": ", ".join(assignees),
                    "assignees": assignees,
                    "total": len(mine), "cleared": cleared, "failed": failed,
                    "results": results})


# ── Template-change PROBE (admin diagnostics) ─────────────────────
# Discovery only: list company templates, and test whether PATCHing a task's
# template_id actually re-templates it. Used to decide if a bulk Walk Thru →
# Light Walk Thru tool is possible via the API.

@dispatch_bp.route("/admin/bw-templates")
@login_required
@admin_required
def bw_templates():
    """List Breezeway company task templates (id + name)."""
    from routes.briefing import _get_breezeway_token
    token = _get_breezeway_token()
    if not token:
        return jsonify({"error": "Breezeway not configured"}), 503
    out = {}
    for path in ("/public/inventory/v1/companies/templates",
                 "/public/inventory/v1/company/templates",
                 "/public/inventory/v1/templates",
                 "/public/inventory/v1/task/templates"):
        body, status = _bw_get_raw(token, path)
        entry = {"status": status}
        if status == 200 and body is not None:
            items = body if isinstance(body, list) else body.get("results", body.get("data", []))
            entry["count"] = len(items or [])
            entry["templates"] = [{"id": t.get("id"),
                                   "name": t.get("name") or t.get("title") or t.get("label")}
                                  for t in (items or []) if isinstance(t, dict)]
        out[path] = entry
    return jsonify(out)


def _task_template_snapshot(token, task_id):
    """Capture a task's template_id + name + checklist (requirements) for before/after compare."""
    detail, ds = _bw_get_raw(token, f"/public/inventory/v1/task/{task_id}")
    d = detail if isinstance(detail, dict) else {}
    reqs, rs, req_path = None, None, None
    for p in (f"/public/inventory/v1/task/{task_id}/requirements",
              f"/public/inventory/v1/task/{task_id}/checklist",
              f"/public/inventory/v1/task/{task_id}/template-requirements"):
        body, status = _bw_get_raw(token, p)
        if status == 200:
            reqs, rs, req_path = body, status, p
            break
        if rs is None:
            rs = status
    req_list = reqs if isinstance(reqs, list) else (
        reqs.get("results", reqs.get("data", [])) if isinstance(reqs, dict) else [])
    return {
        "detail_status":       ds,
        "template_id":         d.get("template_id"),
        "name":                d.get("name"),
        "type_department":     d.get("type_department"),
        "task_keys":           list(d.keys())[:30],
        "requirements_path":   req_path,
        "requirements_status": rs,
        "requirements_count":  len(req_list or []),
        "requirements":        [(r.get("name") or r.get("title") or r.get("label"))
                                for r in (req_list or []) if isinstance(r, dict)][:60],
    }


@dispatch_bp.route("/admin/bw-task-template-test", methods=["GET", "POST"])
@login_required
@admin_required
def bw_task_template_test():
    """Test changing ONE task's template. Captures the task (template_id + checklist)
    before and after PATCHing template_id, so we can see if Breezeway re-templates it.
    Usage: /admin/bw-task-template-test?task_id=123&template_id=456"""
    from routes.briefing import _get_breezeway_token
    token = _get_breezeway_token()
    if not token:
        return jsonify({"error": "Breezeway not configured"}), 503

    src = request.get_json(silent=True) or request.args
    task_id     = str(src.get("task_id") or "").strip()
    template_id = src.get("template_id")
    if not task_id or template_id in (None, ""):
        return jsonify({"error": "task_id and template_id are required"}), 400
    try:
        template_id = int(template_id)
    except (TypeError, ValueError):
        pass

    before = _task_template_snapshot(token, task_id)

    headers = {"Authorization": f"JWT {token}", "Content-Type": "application/json"}
    url     = f"https://api.breezeway.io/public/inventory/v1/task/{task_id}"
    patch   = {}
    try:
        pr = requests.patch(url, headers=headers, json={"template_id": template_id}, timeout=15)
        patch["status"] = pr.status_code
        try:
            patch["body"] = pr.json()
        except Exception:
            patch["body"] = pr.text[:400]
    except Exception as e:
        patch["status"] = None
        patch["body"] = str(e)

    after = _task_template_snapshot(token, task_id)
    return jsonify({
        "task_id":               task_id,
        "requested_template_id": template_id,
        "patch":                 patch,
        "before":                before,
        "after":                 after,
        "template_id_changed":   before.get("template_id") != after.get("template_id"),
        "checklist_changed":     before.get("requirements") != after.get("requirements"),
    })
