"""
reload_db.py
------------
1. Rebuilds the properties table from properties_geocoded.csv
2. Creates users and saved_routes tables if they don't exist
3. Seeds the admin account (operations@tahoegetaways.com) if it doesn't exist

Run this:
  - Whenever properties_geocoded.csv is updated
  - On first setup (creates DB schema and admin user)
"""

import sqlite3
import csv
import os
from werkzeug.security import generate_password_hash
from datetime import datetime

DB_PATH  = "data/properties.db"
CSV_PATH = "data/properties_geocoded.csv"

ADMIN_EMAIL    = "operations@tahoegetaways.com"
ADMIN_NAME     = "Operations Admin"
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "ChangeMe123!")  # override via env var


def reload():
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c    = conn.cursor()

    # ── Properties table ──────────────────────────────────────────
    c.execute("DROP TABLE IF EXISTS properties")
    c.execute("""
        CREATE TABLE properties (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            "Property Name" TEXT,
            "Unit Address"  TEXT,
            Latitude        REAL,
            Longitude       REAL
        )
    """)

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                lat = float(row.get("Latitude") or row.get("lat") or "")
                lng = float(row.get("Longitude") or row.get("lng") or row.get("lon") or "")
            except ValueError:
                continue
            c.execute(
                'INSERT INTO properties ("Property Name","Unit Address",Latitude,Longitude) VALUES (?,?,?,?)',
                (
                    row.get("Property Name") or row.get("name", ""),
                    row.get("Unit Address")  or row.get("address", ""),
                    lat,
                    lng,
                )
            )

    print(f"✓ Properties reloaded from {CSV_PATH}")

    # ── Users table ───────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            email                 TEXT UNIQUE NOT NULL,
            name                  TEXT NOT NULL,
            role                  TEXT NOT NULL DEFAULT 'user',
            password_hash         TEXT NOT NULL,
            is_active             INTEGER NOT NULL DEFAULT 1,
            reset_token           TEXT,
            reset_token_expires   TEXT,
            created_at            TEXT NOT NULL
        )
    """)

    # Seed admin if not present
    existing = c.execute("SELECT id FROM users WHERE email = ?", (ADMIN_EMAIL,)).fetchone()
    if not existing:
        c.execute(
            "INSERT INTO users (email, name, role, password_hash, is_active, created_at) VALUES (?,?,?,?,1,?)",
            (
                ADMIN_EMAIL,
                ADMIN_NAME,
                "admin",
                generate_password_hash(ADMIN_PASSWORD),
                datetime.utcnow().isoformat(),
            )
        )
        print(f"✓ Admin account created: {ADMIN_EMAIL}")
        print(f"  Password: {ADMIN_PASSWORD}")
        print(f"  *** Change this password after first login! ***")
    else:
        print(f"✓ Admin account already exists: {ADMIN_EMAIL}")

    # ── Saved routes table ────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS saved_routes (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            name             TEXT NOT NULL,
            route_date       TEXT NOT NULL,
            stops_json       TEXT NOT NULL,
            total_duration   REAL DEFAULT 0,
            driving_duration REAL DEFAULT 0,
            service_duration REAL DEFAULT 0,
            distance         REAL DEFAULT 0,
            created_by       INTEGER NOT NULL REFERENCES users(id),
            last_edited_by   INTEGER REFERENCES users(id),
            created_at       TEXT NOT NULL,
            updated_at       TEXT NOT NULL
        )
    """)
    print("✓ saved_routes table ready")

    conn.commit()
    conn.close()
    print("\nDatabase successfully reloaded.")


if __name__ == "__main__":
    reload()
