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
from routes.auth import admin_required

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
            # Tahoe mountain roads ≈ 1.4× straight-line, avg 35 mph (15.6 m/s)
            mat[i][j] = dist * 1.4 / 15.6
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
    Falls back to haversine if the key is missing or the request fails."""
    if not GOOGLE_MAPS_KEY:
        return _haversine_matrix(locations)
    n    = len(locations)
    pipe = "|".join(f"{loc['lat']},{loc['lng']}" for loc in locations)
    try:
        resp = requests.get(
            "https://maps.googleapis.com/maps/api/distancematrix/json",
            params={"origins": pipe, "destinations": pipe,
                    "mode": "driving", "key": GOOGLE_MAPS_KEY},
            timeout=10,
        )
        data = resp.json()
        if data.get("status") != "OK":
            return _haversine_matrix(locations)
        mat = [[0.0] * n for _ in range(n)]
        for i, row in enumerate(data.get("rows", [])):
            for j, elem in enumerate(row.get("elements", [])):
                if elem.get("status") == "OK":
                    mat[i][j] = float(elem["duration"]["value"])
                elif i != j:
                    mat[i][j] = _haversine_matrix([locations[i], locations[j]])[0][1]
        return mat
    except Exception:
        return _haversine_matrix(locations)


def _google_route_polyline(locations):
    """Decoded route coords [[lat, lng], ...] via Google Directions API.
    Returns None on failure — callers should draw a straight-line fallback."""
    if not GOOGLE_MAPS_KEY or len(locations) < 2:
        return None
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
        if data.get("status") != "OK" or not data.get("routes"):
            return None
        return _decode_polyline(data["routes"][0]["overview_polyline"]["points"])
    except Exception:
        return None


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
    q = """SELECT r.id, r.name, r.assigned_to, r.route_date, r.created_at, r.updated_at,
                  r.total_duration, r.driving_duration, r.distance,
                  u.name AS created_by_name, lu.name AS last_edited_by_name
           FROM saved_routes r
           JOIN users u ON r.created_by = u.id
           LEFT JOIN users lu ON r.last_edited_by = lu.id
           {where}
           ORDER BY r.route_date DESC, r.updated_at DESC"""

    if current_user.is_admin:
        cur.execute(q.format(where=""))
    else:
        cur.execute(q.format(where="WHERE r.created_by = %s"), (current_user.id,))

    routes = cur.fetchall()
    cur.close(); conn.close()
    today = datetime.utcnow().strftime("%Y-%m-%d")
    return render_template("routes.html", routes=routes, now_date=today)


@dispatch_bp.route("/routes/save", methods=["POST"])
@login_required
def save_route():
    data        = request.json or {}
    name        = (data.get("name") or "").strip()
    assigned_to = (data.get("assigned_to") or "").strip()
    route_date  = (data.get("route_date") or "").strip()
    schedule    = data.get("schedule", [])
    stats       = data.get("stats", {})
    notes       = (data.get("notes") or "").strip() or None
    notes_public = int(bool(data.get("notes_public", False)))

    if not name:
        return jsonify({"error": "Route name is required."}), 400
    if not route_date:
        return jsonify({"error": "Route date is required."}), 400
    if not schedule:
        return jsonify({"error": "No stops to save."}), 400

    now  = datetime.utcnow().isoformat()
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        """INSERT INTO saved_routes
           (name, assigned_to, route_date, stops_json, total_duration,
            driving_duration, service_duration, distance,
            notes, notes_public,
            created_by, last_edited_by, created_at, updated_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
        (name, assigned_to or None, route_date, json.dumps(schedule),
         stats.get("total_duration", 0), stats.get("driving_duration", 0),
         stats.get("service_duration", 0), stats.get("distance", 0),
         notes, notes_public,
         current_user.id, current_user.id, now, now)
    )
    route_id = cur.fetchone()["id"]
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"success": True, "id": route_id})


@dispatch_bp.route("/routes/<int:route_id>/update", methods=["POST"])
@login_required
def update_route(route_id):
    data        = request.json or {}
    name        = (data.get("name") or "").strip()
    assigned_to = (data.get("assigned_to") or "").strip()
    route_date  = (data.get("route_date") or "").strip()
    schedule    = data.get("schedule", [])
    stats       = data.get("stats", {})
    notes       = (data.get("notes") or "").strip() or None
    notes_public = int(bool(data.get("notes_public", False)))

    if not schedule:
        return jsonify({"error": "No stops to save."}), 400

    now  = datetime.utcnow().isoformat()
    conn = get_db()
    cur  = get_cursor(conn)

    # Always update everything we have
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
    })


@dispatch_bp.route("/routes/<int:route_id>/delete", methods=["POST"])
@login_required
@admin_required
def delete_route(route_id):
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("DELETE FROM saved_routes WHERE id = %s", (route_id,))
    conn.commit()
    cur.close(); conn.close()
    flash("Route deleted.", "success")
    return redirect(url_for("dispatch.saved_routes"))


# ── OR-Tools solver ───────────────────────────────────────────────

def _solve_route(
    duration_matrix, service_times_sec, checkin_flags, priority_flags,
    deadline_offset_sec=None, priority_deadline_offset_sec=None,
    hard_deadline=False, soft_deadline_penalty=False,
):
    size    = len(duration_matrix)
    manager = pywrapcp.RoutingIndexManager(size, 1, 0)
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
    params.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    params.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH

    # Scale time limit to problem size. PATH_CHEAPEST_ARC gives a good initial
    # solution in milliseconds; GLS then improves it. For Tahoe-sized routes
    # (5-15 stops in a tight geography) meaningful improvements happen early —
    # running longer rarely changes the result.
    #   hard pass  : needs time to prove feasibility under constraints
    #   soft/unconstrained: good initial solution is usually good enough
    n_stops = max(1, size - 1)
    if hard_deadline:
        secs = max(1, min(3, math.ceil(n_stops / 4)))   # 1 s ≤5, 2 s ≤8, 3 s ≤12
    else:
        secs = max(1, min(2, math.ceil(n_stops / 6)))   # 1 s ≤6, 2 s 7+
    params.time_limit.FromSeconds(secs)

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
    start_time_hhmm = (data.get("startTime") or "09:30").strip()
    drive_only      = bool(data.get("drive_only", False))

    if not stops:
        return jsonify({"error": "No stops provided"}), 400

    try:
        start_minutes = hhmm_to_minutes(start_time_hhmm)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    deadline_minutes          = hhmm_to_minutes(CHECKIN_DEADLINE_HHMM)
    priority_deadline_minutes = hhmm_to_minutes(PRIORITY_CHECKIN_DEADLINE_HHMM)

    try:
        start = {
            "name": start.get("name"),
            "lat":  float(start.get("lat")),
            "lng":  float(start.get("lng")),
        }
    except Exception:
        return jsonify({"error": "Start location must have valid lat/lng."}), 400

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
        return jsonify({"error": "No valid stops (missing lat/lng)."}), 400

    all_locations = [start] + cleaned_stops
    n             = len(all_locations)

    # Build drive-time matrix via Google Distance Matrix API (haversine fallback on error).
    duration_matrix = _google_distance_matrix(all_locations)

    if drive_only:
        service_times_sec = [0] * len(all_locations)
        checkin_flags     = [False] * len(all_locations)
        priority_flags    = [False] * len(all_locations)
        ordered_nodes, arrival_times_sec = _solve_route(
            duration_matrix, service_times_sec, checkin_flags, priority_flags
        )
        if ordered_nodes is None:
            return jsonify({"error": "No solution found"}), 500
        used_deadline_constraints = used_soft_penalties = False
    else:
        service_times_sec = [0] + [max(0, int(s.get("serviceMinutes", 60))) * 60 for s in cleaned_stops]
        checkin_flags     = [False] + [bool(s.get("arrival", False)) for s in cleaned_stops]
        priority_flags    = [False] + [bool(s.get("priority_checkin", False)) for s in cleaned_stops]

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
                hard_deadline=True
            )
            if ordered_nodes is not None:
                used_deadline_constraints = True

        # Pass 2 — soft penalties. Always applied when check-ins exist, including
        # when starting past the deadline (offset=0 pushes them to the front).
        if ordered_nodes is None and (has_checkins or has_priority):
            ordered_nodes, arrival_times_sec = _solve_route(
                duration_matrix, service_times_sec, checkin_flags, priority_flags,
                deadline_offset_sec=checkin_deadline_sec,
                priority_deadline_offset_sec=priority_deadline_sec,
                soft_deadline_penalty=True
            )
            if ordered_nodes is not None:
                used_soft_penalties = True

        # Pass 3 — unconstrained fallback (no check-ins, or truly unsolvable).
        if ordered_nodes is None:
            ordered_nodes, arrival_times_sec = _solve_route(
                duration_matrix, service_times_sec, checkin_flags, priority_flags
            )
            if ordered_nodes is None:
                return jsonify({"error": "No solution found"}), 500

    node_arrival_sec   = {}
    for pos, node in enumerate(ordered_nodes):
        if node not in node_arrival_sec:
            node_arrival_sec[node] = arrival_times_sec[pos]

    ordered_stop_nodes = [n for n in ordered_nodes[1:] if n != 0]
    ordered_stops      = [all_locations[n] for n in ordered_stop_nodes]

    # Compute driving duration by summing matrix legs along the ordered route.
    driving_duration = 0.0
    prev = 0  # depot index
    for node in ordered_stop_nodes:
        row = duration_matrix[prev] if prev < len(duration_matrix) else []
        driving_duration += float(row[node]) if node < len(row) and row[node] else 0.0
        prev = node

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

    # Compute route polyline for the map via Google Directions API.
    polyline_locs = [{"lat": start["lat"], "lng": start["lng"]}] + [
        {"lat": s["lat"], "lng": s["lng"]} for s in ordered_stops
    ]
    route_polyline = _google_route_polyline(polyline_locs)

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
        return jsonify({"error": "Missing new_stop or existing_stops"}), 400

    all_locs = [new_stop] + existing
    mat      = _google_distance_matrix(all_locs)
    return jsonify({
        "from_new": mat[0][1:],
        "to_new":   [mat[i + 1][0] for i in range(len(existing))],
    })


# ── Route geometry (Google Directions polyline for Leaflet map) ───

@dispatch_bp.route("/route-geometry", methods=["POST"])
@login_required
def route_geometry():
    data      = request.json or {}
    locations = data.get("locations", [])
    if len(locations) < 2:
        return jsonify({"coords": None})
    coords = _google_route_polyline(locations)
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

    # Pre-compute the route polyline so view_route.html needs no external API calls.
    stops_with_coords = [s for s in schedule if s.get("lat") and s.get("lng")]
    polyline_locs = [{"lat": DEFAULT_START["lat"], "lng": DEFAULT_START["lng"]}] + [
        {"lat": float(s["lat"]), "lng": float(s["lng"])} for s in stops_with_coords
    ]
    route_polyline = _google_route_polyline(polyline_locs) if len(polyline_locs) >= 2 else None

    return render_template("view_route.html",
        route_id         = row["id"],
        route_name       = row["name"],
        assigned_to      = row["assigned_to"] or "",
        route_date       = row["route_date"],
        schedule         = schedule,
        total_duration   = row["total_duration"],
        driving_duration = row["driving_duration"],
        distance         = row["distance"],
        created_by       = row["created_by_name"],
        route_polyline   = json.dumps(route_polyline or []),
        error            = None,
    )