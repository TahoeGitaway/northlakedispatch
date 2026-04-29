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
                   redirect, url_for, flash)
from flask_login import login_required, current_user
from ortools.constraint_solver import pywrapcp, routing_enums_pb2

from db import (get_db, get_cursor, DEFAULT_START,
                CHECKIN_DEADLINE_HHMM, PRIORITY_CHECKIN_DEADLINE_HHMM,
                hhmm_to_minutes, minutes_to_hhmm)

dispatch_bp = Blueprint("dispatch", __name__)

GOOGLE_MAPS_KEY = os.environ.get("GOOGLE_MAPS_API_KEY", "")


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
            # Tahoe mountain roads: 1.8× winding-road factor, avg 25 mph (11.2 m/s)
            mat[i][j] = dist * 1.8 / 11.2
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
    MAX_ELEM = 100
    try:
        for orig_start in range(0, n, MAX_ORIG):
            orig_end   = min(orig_start + MAX_ORIG, n)
            orig_count = orig_end - orig_start
            orig_pipe  = "|".join(f"{loc['lat']},{loc['lng']}" for loc in locations[orig_start:orig_end])
            dest_batch = max(1, MAX_ELEM // orig_count)

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
    data         = request.json or {}
    name         = (data.get("name") or "").strip()
    assigned_to  = (data.get("assigned_to") or "").strip()
    route_date   = (data.get("route_date") or "").strip()
    schedule     = data.get("schedule", [])
    stats        = data.get("stats", {})
    notes        = (data.get("notes") or "").strip() or None
    notes_public = int(bool(data.get("notes_public", False)))
    team_id      = data.get("team_id") or None

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
           (name, assigned_to, route_date, stops_json, total_duration,
            driving_duration, service_duration, distance,
            notes, notes_public, team_id,
            created_by, last_edited_by, created_at, updated_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
        (name, assigned_to or None, route_date, json.dumps(schedule),
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
    name         = (data.get("name") or "").strip()
    assigned_to  = (data.get("assigned_to") or "").strip()
    route_date   = (data.get("route_date") or "").strip()
    schedule     = data.get("schedule", [])
    stats        = data.get("stats", {})
    notes        = (data.get("notes") or "").strip() or None
    notes_public = int(bool(data.get("notes_public", False)))
    team_id      = data.get("team_id") or None

    if not schedule:
        return jsonify({"error": "No stops to save."}), 400

    now  = datetime.utcnow().isoformat()
    conn = get_db()
    cur  = get_cursor(conn)

    # Only update team_id when one is explicitly sent; otherwise leave it unchanged
    if team_id is not None:
        cur.execute(
            """UPDATE saved_routes SET
               name=%s, assigned_to=%s, route_date=%s,
               stops_json=%s, total_duration=%s, driving_duration=%s,
               service_duration=%s, distance=%s,
               notes=%s, notes_public=%s, team_id=%s,
               last_edited_by=%s, updated_at=%s
               WHERE id=%s""",
            (name or None, assigned_to or None, route_date or None,
             json.dumps(schedule),
             stats.get("total_duration", 0), stats.get("driving_duration", 0),
             stats.get("service_duration", 0), stats.get("distance", 0),
             notes, notes_public, team_id,
             current_user.id, now, route_id)
        )
    else:
        cur.execute(
            """UPDATE saved_routes SET
               name=%s, assigned_to=%s, route_date=%s,
               stops_json=%s, total_duration=%s, driving_duration=%s,
               service_duration=%s, distance=%s,
               notes=%s, notes_public=%s,
               last_edited_by=%s, updated_at=%s
               WHERE id=%s""",
            (name or None, assigned_to or None, route_date or None,
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
    return jsonify({
        "id":               row["id"],
        "name":             row["name"],
        "assigned_to":      row["assigned_to"] or "",
        "route_date":       row["route_date"],
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


# ── OR-Tools solver ───────────────────────────────────────────────

def _solve_route(
    duration_matrix, service_times_sec, checkin_flags, priority_flags,
    deadline_offset_sec=None, priority_deadline_offset_sec=None,
    hard_deadline=False, soft_deadline_penalty=False,
    end_node=0,
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
        return int((duration_matrix[fn][tn] or 0) + (service_times_sec[fn] or 0))

    transit_cb = routing.RegisterTransitCallback(time_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_cb)

    horizon = 24 * 60 * 60
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

    params = pywrapcp.DefaultRoutingSearchParameters()
    params.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH

    # When time-window constraints are active, PATH_CHEAPEST_ARC often cannot
    # build a feasible initial solution (it ignores time windows during greedy
    # construction). LOCAL_CHEAPEST_INSERTION inserts each node into the cheapest
    # *feasible* position, so it respects hard deadlines from the start and gives
    # GLS a valid solution to improve. For unconstrained passes PATH_CHEAPEST_ARC
    # is fine and slightly faster.
    if hard_deadline or soft_deadline_penalty:
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

    # For unconstrained pure-distance TSP, GLOBAL_CHEAPEST_ARC builds a much
    # better initial tour than PATH_CHEAPEST_ARC, giving GLS a stronger start.
    if not hard_deadline and not soft_deadline_penalty:
        params.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.GLOBAL_CHEAPEST_ARC

    solution = routing.SolveWithParameters(params)
    if not solution:
        return None, None

    index = routing.Start(0)
    ordered_nodes, arrival_times_sec = [], []
    while True:
        node = manager.IndexToNode(index)
        ordered_nodes.append(node)
        arrival_times_sec.append(solution.Value(time_dim.CumulVar(index)))
        if routing.IsEnd(index): break
        index = solution.Value(routing.NextVar(index))

    return ordered_nodes, arrival_times_sec


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
                "serviceMinutes":   int(s.get("serviceMinutes", 60)),
            })
        except Exception:
            continue

    if not cleaned_stops:
        return jsonify({"error": "None of the submitted stops had valid coordinates (lat/lng). This usually means the property list is out of sync — try refreshing the page and re-adding your stops."}), 400

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

    if drive_only:
        service_times_sec = [0] * len(all_locations)
        checkin_flags     = [False] * len(all_locations)
        priority_flags    = [False] * len(all_locations)
        ordered_nodes, arrival_times_sec = _solve_route(
            duration_matrix, service_times_sec, checkin_flags, priority_flags,
            end_node=end_node
        )
        if ordered_nodes is None:
            return jsonify({"error": "The route optimizer couldn't find a valid solution. Try reducing the number of stops or widening the time window. (OR-Tools returned no solution after all three passes.)"}), 500
        used_deadline_constraints = used_soft_penalties = False
    else:
        stop_service = [max(0, int(s.get("serviceMinutes", 60))) * 60 for s in cleaned_stops]
        stop_checkin = [bool(s.get("arrival", False)) for s in cleaned_stops]
        stop_priority= [bool(s.get("priority_checkin", False)) for s in cleaned_stops]
        # End node (when different from start) gets zero service time / no flags
        if same_depot:
            service_times_sec = [0] + stop_service
            checkin_flags     = [False] + stop_checkin
            priority_flags    = [False] + stop_priority
        else:
            service_times_sec = [0] + stop_service + [0]
            checkin_flags     = [False] + stop_checkin + [False]
            priority_flags    = [False] + stop_priority + [False]

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

        # Pass 1 — hard constraints. Only attempt when we haven't already blown
        # past the deadline (a hard bound of 0 would make everything infeasible).
        before_checkin_deadline  = start_minutes < deadline_minutes
        before_priority_deadline = start_minutes < priority_deadline_minutes
        if (has_checkins and before_checkin_deadline) or (has_priority and before_priority_deadline):
            ordered_nodes, arrival_times_sec = _solve_route(
                duration_matrix, service_times_sec, checkin_flags, priority_flags,
                deadline_offset_sec=checkin_deadline_sec if before_checkin_deadline else None,
                priority_deadline_offset_sec=priority_deadline_sec if before_priority_deadline else None,
                hard_deadline=True, end_node=end_node
            )
            if ordered_nodes is not None:
                used_deadline_constraints = True

        # Pass 2 — soft penalties.
        if ordered_nodes is None and (has_checkins or has_priority):
            ordered_nodes, arrival_times_sec = _solve_route(
                duration_matrix, service_times_sec, checkin_flags, priority_flags,
                deadline_offset_sec=checkin_deadline_sec,
                priority_deadline_offset_sec=priority_deadline_sec,
                soft_deadline_penalty=True, end_node=end_node
            )
            if ordered_nodes is not None:
                used_soft_penalties = True

        # Pass 3 — unconstrained fallback.
        if ordered_nodes is None:
            ordered_nodes, arrival_times_sec = _solve_route(
                duration_matrix, service_times_sec, checkin_flags, priority_flags,
                end_node=end_node
            )
            if ordered_nodes is None:
                return jsonify({"error": "The route optimizer failed on all three passes (hard constraints, soft penalties, and unconstrained). This is unexpected — check that OR-Tools is installed correctly and that the stop coordinates are in a reachable area."}), 500

    node_arrival_sec   = {}
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