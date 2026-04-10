"""
routes/dispatch.py — core dispatch routes: home map, saved routes,
optimize, matrix-row, public route viewer, portfolio.
"""

import json
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
    # Give hard deadline pass more time to find a feasible solution
    params.time_limit.FromSeconds(5 if hard_deadline else 3)

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
    coords        = ";".join(f"{float(s['lng'])},{float(s['lat'])}" for s in all_locations)
    matrix_url    = f"https://router.project-osrm.org/table/v1/driving/{coords}?annotations=duration"

    resp = requests.get(matrix_url, timeout=30)
    if resp.status_code != 200:
        return jsonify({"error": "OSRM matrix request failed"}), 500

    duration_matrix = resp.json().get("durations")
    if not duration_matrix:
        return jsonify({"error": "Invalid matrix response"}), 500

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

    coords_final = ";".join(f"{float(s['lng'])},{float(s['lat'])}" for s in [start] + ordered_stops)
    route_url    = f"https://router.project-osrm.org/route/v1/driving/{coords_final}?overview=full&geometries=geojson"
    route_resp   = requests.get(route_url, timeout=30)
    if route_resp.status_code != 200:
        return jsonify({"error": "OSRM route request failed"}), 500

    route_data = route_resp.json().get("routes", [{}])[0]
    if not route_data:
        return jsonify({"error": "Invalid OSRM route response"}), 500

    driving_duration = float(route_data.get("duration", 0.0))
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

    return jsonify({
        "distance":                  route_data.get("distance", 0.0),
        "total_duration":            total_duration,
        "driving_duration":          driving_duration,
        "service_duration":          service_duration,
        "geometry":                  route_data.get("geometry"),
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

    all_coords = [new_stop] + existing
    coords     = ";".join(f"{float(s['lng'])},{float(s['lat'])}" for s in all_coords)
    matrix_url = f"https://router.project-osrm.org/table/v1/driving/{coords}?annotations=duration"

    try:
        resp = requests.get(matrix_url, timeout=15)
        if resp.status_code != 200:
            return jsonify({"error": "OSRM request failed"}), 500
        matrix = resp.json().get("durations", [])
        return jsonify({
            "from_new": matrix[0][1:],
            "to_new":   [row[0] for row in matrix[1:]]
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


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
        error            = None,
    )