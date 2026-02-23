from flask import Flask, render_template, request, jsonify
import sqlite3
import requests
from ortools.constraint_solver import pywrapcp, routing_enums_pb2

app = Flask(__name__)

DB_PATH = "data/properties.db"

DEFAULT_START = {
    "name": "Tahoe Getaways Office",
    "lat": 39.3279,
    "lng": -120.1833
}

CHECKIN_DEADLINE_HHMM = "16:00"  # 4PM hard deadline


# ---------------- TIME HELPERS ---------------- #

def hhmm_to_minutes(hhmm: str) -> int:
    try:
        parts = hhmm.strip().split(":")
        if len(parts) != 2:
            raise ValueError
        hh = int(parts[0])
        mm = int(parts[1])
        if hh < 0 or hh > 23 or mm < 0 or mm > 59:
            raise ValueError
        return hh * 60 + mm
    except Exception:
        raise ValueError("Invalid time format. Use HH:MM (24-hour).")


def minutes_to_hhmm(m: int) -> str:
    m = max(0, int(m))
    hh = (m // 60) % 24
    mm = m % 60
    return f"{hh:02d}:{mm:02d}"


# ---------------- HOME ROUTE ---------------- #

@app.route("/")
def home():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT "Property Name", "Unit Address", Latitude, Longitude
        FROM properties
        WHERE Latitude IS NOT NULL AND Longitude IS NOT NULL
    """)

    rows = cursor.fetchall()
    conn.close()

    properties = []
    for r in rows:
        properties.append({
            "name": r[0],
            "address": r[1],
            "lat": float(r[2]),
            "lng": float(r[3])
        })

    return render_template(
        "map.html",
        properties=properties,
        property_count=len(properties),
        default_start=DEFAULT_START
    )


# ---------------- OPTIMIZE ROUTE ---------------- #

def _validate_locations(start, stops):
    # start must have coords
    if not start or start.get("lat") is None or start.get("lng") is None:
        raise ValueError("Start location must have lat/lng. Choose Office or a Property.")

    # stops must have coords
    cleaned = []
    for s in stops:
        if s.get("lat") is None or s.get("lng") is None:
            continue
        cleaned.append(s)

    if not cleaned:
        raise ValueError("No valid stops (missing lat/lng).")

    return start, cleaned


def _get_osrm_duration_matrix(all_locations):
    coords = ";".join(f"{float(s['lng'])},{float(s['lat'])}" for s in all_locations)
    matrix_url = f"http://router.project-osrm.org/table/v1/driving/{coords}?annotations=duration"

    resp = requests.get(matrix_url, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError("OSRM matrix request failed")

    matrix_data = resp.json()
    durations = matrix_data.get("durations")
    if not durations or any(row is None for row in durations):
        raise RuntimeError("OSRM returned an invalid duration matrix")

    return durations


def _get_osrm_route_geometry(ordered_locations):
    coords_final = ";".join(f"{float(s['lng'])},{float(s['lat'])}" for s in ordered_locations)
    route_url = f"http://router.project-osrm.org/route/v1/driving/{coords_final}?overview=full&geometries=geojson"

    route_resp = requests.get(route_url, timeout=30)
    if route_resp.status_code != 200:
        raise RuntimeError("OSRM route request failed")

    route_data = route_resp.json()["routes"][0]
    return route_data


def _solve_route(duration_matrix, service_times_sec, checkin_deadline_offset_sec=None, checkin_flags=None):
    """
    Solves route with OR-Tools.
    If checkin_deadline_offset_sec and checkin_flags provided, applies hard arrival windows.
    Returns: ordered_nodes (includes start=0, ends with end node), arrival_times_sec aligned to ordered_nodes
    """
    size = len(duration_matrix)
    manager = pywrapcp.RoutingIndexManager(size, 1, 0)
    routing = pywrapcp.RoutingModel(manager)

    def time_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        travel_time = duration_matrix[from_node][to_node] or 0
        service_time = service_times_sec[from_node]  # service at FROM node
        return int(travel_time + service_time)

    transit_cb = routing.RegisterTransitCallback(time_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_cb)

    # Time dimension: allow waiting by giving slack.
    # If you set slack=0, time windows can become impossible even when a feasible sequence exists.
    horizon = 24 * 60 * 60
    routing.AddDimension(
        transit_cb,
        horizon,   # slack max (waiting)
        horizon,   # max cumul
        True,      # force start at 0
        "Time"
    )
    time_dim = routing.GetDimensionOrDie("Time")

    # ----- HARD COMPLETION DEADLINE FOR CHECK-INS -----

checkin_deadline_offset_sec = (deadline_minutes - start_minutes) * 60

for node_idx in range(1, size):
    stop = all_locations[node_idx]
    is_checkin = bool(stop.get("arrival", False))

    if is_checkin:
        idx = manager.NodeToIndex(node_idx)

        service_time_here = service_times_sec[node_idx]

        # Latest arrival so that finish <= 4PM
        latest_arrival = checkin_deadline_offset_sec - service_time_here

        if latest_arrival < 0:
            latest_arrival = 0

        time_dim.CumulVar(idx).SetRange(0, int(latest_arrival))


    # Optional: Hard arrival window for check-ins
    if checkin_deadline_offset_sec is not None and checkin_flags is not None:
        for node_idx in range(1, size):
            if bool(checkin_flags[node_idx]):
                idx = manager.NodeToIndex(node_idx)
                # Must arrive by deadline offset (seconds since start)
                time_dim.CumulVar(idx).SetRange(0, int(checkin_deadline_offset_sec))

    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    search_parameters.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    search_parameters.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    search_parameters.time_limit.FromSeconds(3)

    solution = routing.SolveWithParameters(search_parameters)
    if not solution:
        return None, None

    index = routing.Start(0)
    ordered_nodes = []
    arrival_times_sec = []

    while True:
        node = manager.IndexToNode(index)
        ordered_nodes.append(node)
        arrival_times_sec.append(solution.Value(time_dim.CumulVar(index)))

        if routing.IsEnd(index):
            break

        index = solution.Value(routing.NextVar(index))

    return ordered_nodes, arrival_times_sec


@app.route("/optimize", methods=["POST"])
def optimize():
    data = request.json or {}
    stops = data.get("stops", [])
    start = data.get("start") or DEFAULT_START
    start_time_hhmm = (data.get("startTime") or "09:00").strip()

    if not stops:
        return jsonify({"error": "No stops provided"}), 400

    try:
        start_minutes = hhmm_to_minutes(start_time_hhmm)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    deadline_minutes = hhmm_to_minutes(CHECKIN_DEADLINE_HHMM)

    try:
        start, stops = _validate_locations(start, stops)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    # Combine start + stops (node 0 is start)
    all_locations = [start] + stops

    # OSRM travel times
    try:
        duration_matrix = _get_osrm_duration_matrix(all_locations)
    except RuntimeError as e:
        return jsonify({"error": str(e)}), 500

    size = len(duration_matrix)

    # Service times per node (seconds). Node 0 (start) = 0
    service_times_sec = [0]
    for stop in stops:
        minutes = int(stop.get("serviceMinutes", 60))
        minutes = max(0, minutes)
        service_times_sec.append(minutes * 60)

    # Check-in flags aligned to all_locations index (0=start, 1..n stops)
    checkin_flags = [False]
    for stop in stops:
        checkin_flags.append(bool(stop.get("arrival", False)))

    # Deadline offset seconds since start
    checkin_deadline_offset_sec = (deadline_minutes - start_minutes) * 60

    # If start is after 4PM, deadline logic is meaningless; we’ll still route, mark late.
    enforce_deadline = start_minutes < deadline_minutes

    # 1) Try SOLVE WITH HARD TIME WINDOWS (Option 3 correct)
    ordered_nodes, arrival_times_sec = (None, None)
    if enforce_deadline:
        ordered_nodes, arrival_times_sec = _solve_route(
            duration_matrix=duration_matrix,
            service_times_sec=service_times_sec,
            checkin_deadline_offset_sec=checkin_deadline_offset_sec,
            checkin_flags=checkin_flags
        )

    used_deadline_constraints = ordered_nodes is not None

    # 2) Fallback: solve WITHOUT windows if infeasible
    if ordered_nodes is None:
        ordered_nodes, arrival_times_sec = _solve_route(
            duration_matrix=duration_matrix,
            service_times_sec=service_times_sec,
            checkin_deadline_offset_sec=None,
            checkin_flags=None
        )
        if ordered_nodes is None:
            return jsonify({"error": "No solution found"}), 500

    # ordered_nodes includes start and ends with end node. Extract stops only (exclude node 0 and final end node if present)
    # In this model, the end node is a virtual end index; manager.IndexToNode(end) returns 0? No—here we used a single depot,
    # routing ends at an end index that maps to node 0 as well in many cases; safest is:
    ordered_stop_nodes = [n for n in ordered_nodes[1:] if n != 0]

    # Build ordered stops objects (exclude start)
    ordered_stops = [all_locations[n] for n in ordered_stop_nodes]

    # Build geometry using OSRM route (start + ordered stops)
    ordered_locations_for_geom = [start] + ordered_stops
    try:
        route_data = _get_osrm_route_geometry(ordered_locations_for_geom)
    except RuntimeError as e:
        return jsonify({"error": str(e)}), 500

    driving_duration = float(route_data["duration"])  # seconds
    service_duration = sum(int(s.get("serviceMinutes", 60)) * 60 for s in ordered_stops)
    total_duration = driving_duration + service_duration

    # Build schedule and compute lateness (always compute late flags)
    schedule = []
    late_checkins = []

    # arrival_times_sec aligns with ordered_nodes positions.
    # We need the arrival time for each stop in ordered_stop_nodes order.
    # Build mapping node->arrival_sec from the first occurrence in the route walk.
    node_arrival_sec = {}
    for pos, node in enumerate(ordered_nodes):
        if node not in node_arrival_sec:
            node_arrival_sec[node] = arrival_times_sec[pos]

    for node in ordered_stop_nodes:
        stop = all_locations[node]
        eta_minutes = start_minutes + int(node_arrival_sec.get(node, 0) // 60)

        is_checkin = bool(stop.get("arrival", False))
        is_late = False
        finish_minutes = eta_minutes + int(stop.get("serviceMinutes", 60))

    if is_checkin and finish_minutes > deadline_minutes:
        is_late = True
        late_checkins.append(stop.get("name"))

        schedule.append({
            "name": stop.get("name"),
            "arrival": is_checkin,
            "late": is_late,
            "serviceMinutes": int(stop.get("serviceMinutes", 60)),
            "eta": minutes_to_hhmm(eta_minutes),
            "eta_minutes": eta_minutes,
            "lat": float(stop.get("lat")),
            "lng": float(stop.get("lng"))
        })

    return jsonify({
        "distance": route_data["distance"],
        "total_duration": total_duration,
        "driving_duration": driving_duration,
        "service_duration": service_duration,
        "geometry": route_data["geometry"],
        "ordered_stops": ordered_stops,  # keep for compatibility
        "start_time": start_time_hhmm,
        "checkin_deadline": CHECKIN_DEADLINE_HHMM,
        "schedule": schedule,
        "late_checkins": late_checkins,
        "deadline_constraints_used": used_deadline_constraints
    })


if __name__ == "__main__":
    app.run(debug=True)