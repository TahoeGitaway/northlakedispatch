from flask import Flask, render_template
import sqlite3

app = Flask(__name__)

DATABASE = "data/properties.db"


def get_db_connection():
    """Create a database connection with row access by column name."""
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn


@app.route("/")
def home():
    conn = get_db_connection()
    cursor = conn.cursor()

    # Only select what we actually need for maps and routing
    cursor.execute("""
        SELECT
            "Property Name",
            "Unit Address",
            Latitude,
            Longitude,
            Neighborhood
        FROM properties
        WHERE Latitude IS NOT NULL
        AND Longitude IS NOT NULL
    """)

    rows = cursor.fetchall()
    conn.close()

    # Convert to clean dictionaries (JSON-safe)
    properties = [
        {
            "name": row["Property Name"],
            "address": row["Unit Address"],
            "lat": row["Latitude"],
            "lng": row["Longitude"],
            "neighborhood": row["Neighborhood"]
        }
        for row in rows
    ]

    return render_template(
        "map.html",
        properties=properties,
        property_count=len(properties)
    )


if __name__ == "__main__":
    app.run(debug=True)
