import pandas as pd
import folium

INPUT_FILE = "data/properties_geocoded.csv"
OUTPUT_MAP = "map.html"

def main():
    df = pd.read_csv(INPUT_FILE)

    # Drop rows without coordinates
    df = df.dropna(subset=["Latitude", "Longitude"])

    # Center map roughly on Tahoe
    start_location = [39.3279, -120.1833]
    m = folium.Map(location=start_location, zoom_start=11)

    for _, row in df.iterrows():
        lat = row["Latitude"]
        lng = row["Longitude"]
        address = row.get("Unit Address", "Unknown")

        folium.Marker(
            location=[lat, lng],
            popup=str(address)
        ).add_to(m)

    m.save(OUTPUT_MAP)
    print(f"Map saved to {OUTPUT_MAP}")

if __name__ == "__main__":
    main()
