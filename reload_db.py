import pandas as pd
import sqlite3

CSV_PATH = "data/properties_geocoded.csv"
DB_PATH = "data/properties.db"

df = pd.read_csv(CSV_PATH)

conn = sqlite3.connect(DB_PATH)
df.to_sql("properties", conn, if_exists="replace", index=False)
conn.close()

print("Database successfully reloaded.")


"""
UTILITY SCRIPT

Purpose:
Reloads the SQLite database from data/properties_geocoded.csv

Run this script anytime you:
- Replace properties_geocoded.csv
- Update geocoded data
- Want to refresh the database

Command:
    py reload_db.py
"""