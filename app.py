from flask import Flask, render_template, request, jsonify
import sqlite3
import requests

app = Flask(__name__)

DB_PATH = "data/properties.db"

DEFAULT_START = {
    "name": "Tahoe Getaways Office",
    "lat": 39.3279,
    "lng": -120.1833
}


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
            "lat": r[2],
            "lng": r[3]
        })

    return render_template(
        "map.html",
        properties=properties,
        property_count=len(properties),
        default_start=DEFAULT_START
    )


@app.route("/optimize", methods=["POST"])
def optimize():

    data = request.json
    stops = data.get("stops", [])
    start = data.get("start")

    if not stops:
        return jsonify({"error": "No stops provided"}), 400

    # arrivals first
    stops_sorted = sorted(stops, key=lambda x: not x.get("arrival", False))

    # prepend start
    if start:
        stops_sorted = [start] + stops_sorted
    else:
        stops_sorted = [DEFAULT_START] + stops_sorted

    coords = ";".join([
        f"{float(s['lng'])},{float(s['lat'])}"
        for s in stops_sorted
    ])

    url = f"http://router.project-osrm.org/route/v1/driving/{coords}?overview=full&geometries=geojson"

    response = requests.get(url)

    if response.status_code != 200:
        return jsonify({"error": "Routing failed"}), 500

    route_data = response.json()
    route = route_data["routes"][0]

    return jsonify({
        "distance": route["distance"],
        "duration": route["duration"],
        "geometry": route["geometry"],
        "ordered_stops": stops_sorted
    })


if __name__ == "__main__":
    app.run(debug=True)
