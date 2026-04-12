"""
routes/employee.py — Mobile-friendly employee routing tool.
Cleaners enter their stops, choose Keep or Optimize order, get a Google Maps link.
"""

import math
import os

from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required

from db import get_db, get_cursor, DEFAULT_START

employee_bp = Blueprint("employee", __name__)


# ── Haversine helper (same constants as dispatch.py) ─────────────────────────

def _hav_seconds(a, b):
    """Drive-time estimate in seconds between two {lat, lng} dicts."""
    lat1, lng1 = math.radians(a["lat"]), math.radians(a["lng"])
    lat2, lng2 = math.radians(b["lat"]), math.radians(b["lng"])
    dlat, dlng = lat2 - lat1, lng2 - lng1
    h = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlng / 2) ** 2
    dist = 6_371_000 * 2 * math.atan2(math.sqrt(h), math.sqrt(1 - h))
    return dist * 1.4 / 15.6   # 1.4× road factor, 35 mph


def _haversine_matrix(locations):
    n = len(locations)
    mat = [[0.0] * n for _ in range(n)]
    for i in range(n):
        for j in range(n):
            if i != j:
                mat[i][j] = _hav_seconds(locations[i], locations[j])
    return mat


def _nearest_neighbor(start_idx, stops):
    """Greedy nearest-neighbor tour starting from start_idx over stops list."""
    all_locs = [stops[start_idx]] + [s for i, s in enumerate(stops) if i != start_idx]
    n = len(all_locs)
    mat = _haversine_matrix(all_locs)
    visited = [False] * n
    visited[0] = True
    order = [0]
    for _ in range(n - 1):
        last = order[-1]
        nearest = min(
            (j for j in range(n) if not visited[j]),
            key=lambda j: mat[last][j],
        )
        visited[nearest] = True
        order.append(nearest)
    # order[0] is the start; the rest are indices into all_locs
    # map back: all_locs[0] = start; all_locs[1..] = original stops minus start
    result = []
    original_stops_no_start = [s for i, s in enumerate(stops) if i != start_idx]
    for idx in order[1:]:          # skip the depot (index 0)
        result.append(original_stops_no_start[idx - 1])
    return result


# ── Routes ───────────────────────────────────────────────────────────────────

@employee_bp.route("/employee")
@login_required
def employee():
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        'SELECT "Property Name", "Unit Address", "Latitude", "Longitude" '
        'FROM properties '
        'WHERE "Latitude" IS NOT NULL AND "Longitude" IS NOT NULL '
        'ORDER BY "Property Name"'
    )
    rows = cur.fetchall()
    cur.close(); conn.close()

    properties = [
        {
            "name":    r["Property Name"],
            "address": r["Unit Address"] or "",
            "lat":     float(r["Latitude"]),
            "lng":     float(r["Longitude"]),
        }
        for r in rows
    ]
    return render_template(
        "employee.html",
        properties=properties,
        default_start=DEFAULT_START,
    )


@employee_bp.route("/employee/route", methods=["POST"])
@login_required
def employee_route():
    data  = request.get_json(force=True)
    mode  = data.get("mode", "keep")          # "keep" or "optimize"
    stop_names = data.get("stops", [])        # ordered list of property names
    start_data = data.get("start", {})        # {name, lat, lng}

    if not stop_names:
        return jsonify({"error": "No stops were included. Add at least one property before building a route."}), 400

    # Resolve stop coords from DB
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        'SELECT "Property Name", "Latitude", "Longitude" FROM properties '
        'WHERE "Latitude" IS NOT NULL AND "Longitude" IS NOT NULL'
    )
    rows = cur.fetchall()
    cur.close(); conn.close()

    prop_map = {
        r["Property Name"]: {"lat": float(r["Latitude"]), "lng": float(r["Longitude"])}
        for r in rows
    }

    stops = []
    for name in stop_names:
        coords = prop_map.get(name)
        if not coords:
            return jsonify({"error": f"Property '{name}' wasn't found in the database or is missing coordinates. It may have been renamed or removed — refresh the page and try again."}), 400
        stops.append({"name": name, **coords})

    # Start location
    start = {
        "name": start_data.get("name", DEFAULT_START["name"]),
        "lat":  float(start_data.get("lat", DEFAULT_START["lat"])),
        "lng":  float(start_data.get("lng", DEFAULT_START["lng"])),
    }

    if mode == "optimize" and len(stops) > 1:
        # Nearest-neighbor greedy from start
        all_locs = [start] + stops
        mat = _haversine_matrix(all_locs)
        n = len(all_locs)
        visited = [False] * n
        visited[0] = True
        order = [0]
        for _ in range(n - 1):
            last = order[-1]
            nearest = min(
                (j for j in range(n) if not visited[j]),
                key=lambda j: mat[last][j],
            )
            visited[nearest] = True
            order.append(nearest)
        ordered_stops = [all_locs[i] for i in order[1:]]
    else:
        ordered_stops = stops

    # Compute per-leg drive times
    all_locs = [start] + ordered_stops
    drive_times = []
    total_seconds = 0
    for i in range(len(all_locs) - 1):
        secs = _hav_seconds(all_locs[i], all_locs[i + 1])
        drive_times.append(round(secs / 60))   # minutes
        total_seconds += secs

    return jsonify({
        "ordered_stops": ordered_stops,
        "drive_times":   drive_times,         # minutes per leg (stop i → stop i+1)
        "total_minutes": round(total_seconds / 60),
        "start":         start,
    })
