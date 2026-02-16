import pandas as pd
import sqlite3
import folium

DB_FILE = "data/properties.db"
OUTPUT_MAP = "map.html"

def main():
    # Load data from SQLite
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql_query("SELECT * FROM properties", conn)
    conn.close()

    # Drop rows without coordinates
    df = df.dropna(subset=["Latitude", "Longitude"])

    if df.empty:
        print("No valid coordinates found.")
        return

    # Center map on the average location of properties
    center_lat = df["Latitude"].mean()
    center_lng = df["Longitude"].mean()

    m = folium.Map(location=[center_lat, center_lng], zoom_start=11)

    # Add markers
    for _, row in df.iterrows():
        lat = row["Latitude"]
        lng = row["Longitude"]
        address = row.get("Unit Address", "Unknown")

        folium.Marker(
            location=[lat, lng],
            popup=str(address)
        ).add_to(m)

    # Save map
    m.save(OUTPUT_MAP)
    print(f"Map saved to {OUTPUT_MAP}")

if __name__ == "__main__":
    main()
