"""
migrate_to_postgres.py
──────────────────────
One-time script to migrate data from the local SQLite DB to Railway Postgres.

Run this ONCE from your local machine after:
1. Railway Postgres addon is provisioned
2. DATABASE_URL is set in your Railway environment
3. You have a local copy of data/properties.db with your data

Usage:
    pip install psycopg2-binary
    python migrate_to_postgres.py

Set DATABASE_URL in your environment before running:
    $env:DATABASE_URL = "postgresql://..." (Windows PowerShell)
    export DATABASE_URL="postgresql://..."  (Mac/Linux)

Find your DATABASE_URL in Railway:
    Your Postgres service → Variables → DATABASE_URL
"""

import sqlite3
import psycopg2
import psycopg2.extras
import json
import os
import sys

SQLITE_PATH  = "data/properties.db"
DATABASE_URL = os.environ.get("DATABASE_URL", "")

if not DATABASE_URL:
    print("ERROR: DATABASE_URL environment variable is not set.")
    print("Set it to your Railway Postgres connection string and try again.")
    sys.exit(1)

print("Connecting to SQLite...")
sqlite_conn = sqlite3.connect(SQLITE_PATH)
sqlite_conn.row_factory = sqlite3.Row

print("Connecting to Postgres...")
pg_conn = psycopg2.connect(DATABASE_URL)
pg_cur  = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# ── Create tables in Postgres ──────────────────────────────────────

print("Creating tables in Postgres...")

pg_cur.execute("""CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    role TEXT DEFAULT 'user',
    password_hash TEXT NOT NULL,
    is_active INTEGER DEFAULT 1,
    reset_token TEXT,
    reset_token_expires TEXT,
    created_at TEXT
)""")

pg_cur.execute("""CREATE TABLE IF NOT EXISTS invites (
    id SERIAL PRIMARY KEY,
    email TEXT NOT NULL,
    token TEXT UNIQUE NOT NULL,
    invited_by INTEGER NOT NULL,
    expires_at TEXT NOT NULL,
    used INTEGER DEFAULT 0,
    created_at TEXT
)""")

pg_cur.execute("""CREATE TABLE IF NOT EXISTS saved_routes (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    route_date TEXT,
    assigned_to TEXT,
    stops_json TEXT,
    total_duration REAL,
    driving_duration REAL,
    service_duration REAL,
    distance REAL,
    created_by INTEGER,
    last_edited_by INTEGER,
    created_at TEXT,
    updated_at TEXT
)""")

pg_cur.execute("""CREATE TABLE IF NOT EXISTS properties (
    id SERIAL PRIMARY KEY,
    "Property Name" TEXT,
    "Unit Address" TEXT,
    "Latitude" REAL,
    "Longitude" REAL
)""")

pg_conn.commit()

# ── Migrate users ──────────────────────────────────────────────────

print("Migrating users...")
users = sqlite_conn.execute("SELECT * FROM users").fetchall()
for u in users:
    try:
        pg_cur.execute(
            """INSERT INTO users (id, email, name, role, password_hash, is_active,
               reset_token, reset_token_expires, created_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (email) DO NOTHING""",
            (u["id"], u["email"], u["name"], u["role"], u["password_hash"],
             u["is_active"], u["reset_token"], u["reset_token_expires"], u["created_at"])
        )
    except Exception as e:
        print(f"  Skipped user {u['email']}: {e}")

pg_conn.commit()
print(f"  Migrated {len(users)} users")

# Reset the sequence so new inserts don't collide with migrated IDs
pg_cur.execute("SELECT setval('users_id_seq', (SELECT MAX(id) FROM users))")
pg_conn.commit()

# ── Migrate invites ────────────────────────────────────────────────

print("Migrating invites...")
try:
    invites = sqlite_conn.execute("SELECT * FROM invites").fetchall()
    for inv in invites:
        try:
            pg_cur.execute(
                """INSERT INTO invites (id, email, token, invited_by, expires_at, used, created_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (token) DO NOTHING""",
                (inv["id"], inv["email"], inv["token"], inv["invited_by"],
                 inv["expires_at"], inv["used"], inv["created_at"])
            )
        except Exception as e:
            print(f"  Skipped invite {inv['email']}: {e}")
    pg_conn.commit()
    if invites:
        pg_cur.execute("SELECT setval('invites_id_seq', (SELECT MAX(id) FROM invites))")
        pg_conn.commit()
    print(f"  Migrated {len(invites)} invites")
except Exception as e:
    print(f"  No invites table or error: {e}")

# ── Migrate saved_routes ───────────────────────────────────────────

print("Migrating saved routes...")
try:
    routes = sqlite_conn.execute("SELECT * FROM saved_routes").fetchall()
    for r in routes:
        assigned_to = r["assigned_to"] if "assigned_to" in r.keys() else None
        try:
            pg_cur.execute(
                """INSERT INTO saved_routes
                   (id, name, assigned_to, route_date, stops_json, total_duration,
                    driving_duration, service_duration, distance,
                    created_by, last_edited_by, created_at, updated_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (id) DO NOTHING""",
                (r["id"], r["name"], assigned_to, r["route_date"], r["stops_json"],
                 r["total_duration"], r["driving_duration"], r["service_duration"],
                 r["distance"], r["created_by"], r["last_edited_by"],
                 r["created_at"], r["updated_at"])
            )
        except Exception as e:
            print(f"  Skipped route {r['name']}: {e}")
    pg_conn.commit()
    if routes:
        pg_cur.execute("SELECT setval('saved_routes_id_seq', (SELECT MAX(id) FROM saved_routes))")
        pg_conn.commit()
    print(f"  Migrated {len(routes)} saved routes")
except Exception as e:
    print(f"  No saved_routes or error: {e}")

# ── Migrate properties ─────────────────────────────────────────────

print("Migrating properties...")
try:
    props = sqlite_conn.execute(
        'SELECT "Property Name", "Unit Address", Latitude, Longitude FROM properties'
    ).fetchall()
    for p in props:
        try:
            pg_cur.execute(
                """INSERT INTO properties ("Property Name", "Unit Address", "Latitude", "Longitude")
                   VALUES (%s,%s,%s,%s)""",
                (p["Property Name"], p["Unit Address"], p["Latitude"], p["Longitude"])
            )
        except Exception as e:
            print(f"  Skipped property: {e}")
    pg_conn.commit()
    print(f"  Migrated {len(props)} properties")
except Exception as e:
    print(f"  No properties table or error: {e}")

# ── Done ───────────────────────────────────────────────────────────

sqlite_conn.close()
pg_cur.close()
pg_conn.close()

print("\n✅ Migration complete! Your data is now in Postgres.")
print("You can now deploy the Postgres version of app.py to Railway.")