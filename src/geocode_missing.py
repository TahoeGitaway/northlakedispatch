import pandas as pd
import requests
import time
from config import GOOGLE_MAPS_API_KEY

INPUT_FILE = "data/properties_geocoded.csv"
OUTPUT_FILE = "data/properties_geocoded_updated.csv"
SLEEP_TIME = 0.2


def clean_address(address):
    """Clean address formatting issues before geocoding."""
    if not isinstance(address, str):
        return ""

    # Trim and normalize spaces
    address = address.strip()
    address = " ".join(address.split())

    # Known corrections you encountered
    replacements = {
        "Squaw Valley": "Olympic Valley",
        "Truckee, CA, CA": "Truckee, CA",
        "Tahoe Donner, Truckee": "Truckee",
    }

    for wrong, correct in replacements.items():
        address = address.replace(wrong, correct)

    return address


def geocode_address(address):
    """Call Google Geocoding API."""
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {
        "address": address,
        "key": GOOGLE_MAPS_API_KEY
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        data = response.json()

        if data.get("status") == "OK":
            location = data["results"][0]["geometry"]["location"]
            return location["lat"], location["lng"], "OK"

        return None, None, data.get("status")

    except Exception as e:
        print(f"Request error for {address}: {e}")
        return None, None, "REQUEST_FAILED"


def main():
    df = pd.read_csv(INPUT_FILE)

    # Add status column if it doesn't exist
    if "GeocodeStatus" not in df.columns:
        df["GeocodeStatus"] = ""

    total_rows = len(df)
    updated_count = 0

    for i, row in df.iterrows():
        raw_address = row.get("Unit Address")
        address = clean_address(raw_address)

        if address == "":
            df.at[i, "GeocodeStatus"] = "NO_ADDRESS"
            continue

        # Skip rows that already have coordinates
        if pd.notna(row.get("Latitude")) and pd.notna(row.get("Longitude")):
            continue

        print(f"[{i+1}/{total_rows}] Geocoding: {address}")

        lat, lng, status = geocode_address(address)

        df.at[i, "Latitude"] = lat
        df.at[i, "Longitude"] = lng
        df.at[i, "GeocodeStatus"] = status

        updated_count += 1
        time.sleep(SLEEP_TIME)

    df.to_csv(OUTPUT_FILE, index=False)

    print("\nFinished.")
    print(f"Rows updated: {updated_count}")
    print(f"Output saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
