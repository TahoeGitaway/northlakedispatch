"""
db.py — shared database helpers, config, and User model.
Imported by app.py and all blueprint modules.
"""

import os
import psycopg2
import psycopg2.extras
from flask_login import UserMixin
from werkzeug.security import generate_password_hash
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────
DATABASE_URL = os.environ.get("DATABASE_URL", "")
APP_BASE_URL = os.environ.get("APP_BASE_URL", "http://localhost:5000")

DEFAULT_START = {
    "name": "Tahoe Getaways Office",
    "lat":  39.3279,
    "lng":  -120.1833,
}

CHECKIN_DEADLINE_HHMM          = "16:00"
PRIORITY_CHECKIN_DEADLINE_HHMM = "12:00"

CARPET_CLEANERS = [
    "Irving", "Trevor", "Julie", "Chris",
    "Andy", "Alec", "Calder", "Jonah"
]


# ── DB helpers ────────────────────────────────────────────────────

def get_db():
    return psycopg2.connect(DATABASE_URL)

def get_cursor(conn):
    """RealDictCursor so rows behave like dicts."""
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


# ── Schema ────────────────────────────────────────────────────────

def init_db():
    conn = get_db()
    cur  = get_cursor(conn)

    cur.execute("""CREATE TABLE IF NOT EXISTS users (
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

    cur.execute("""CREATE TABLE IF NOT EXISTS invites (
        id SERIAL PRIMARY KEY,
        email TEXT NOT NULL,
        token TEXT UNIQUE NOT NULL,
        invited_by INTEGER NOT NULL,
        expires_at TEXT NOT NULL,
        used INTEGER DEFAULT 0,
        created_at TEXT
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS saved_routes (
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

    cur.execute("""CREATE TABLE IF NOT EXISTS properties (
        id SERIAL PRIMARY KEY,
        "Property Name" TEXT,
        "Unit Address" TEXT,
        "Latitude" REAL,
        "Longitude" REAL
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS carpet_log (
        id SERIAL PRIMARY KEY,
        log_date TEXT NOT NULL,
        cleaner_name TEXT NOT NULL,
        property_name TEXT,
        notes TEXT,
        logged_by INTEGER NOT NULL,
        created_at TEXT
    )""")

    # Safe migrations
    cur.execute("ALTER TABLE saved_routes ADD COLUMN IF NOT EXISTS assigned_to TEXT")
    cur.execute("ALTER TABLE saved_routes ADD COLUMN IF NOT EXISTS notes TEXT")
    cur.execute("ALTER TABLE saved_routes ADD COLUMN IF NOT EXISTS notes_public INTEGER DEFAULT 0")
    cur.execute("ALTER TABLE saved_routes ADD COLUMN IF NOT EXISTS created_by_display TEXT")
    cur.execute("ALTER TABLE carpet_log ADD COLUMN IF NOT EXISTS property_name TEXT")
    cur.execute("ALTER TABLE carpet_log ADD COLUMN IF NOT EXISTS cleaner_name_2 TEXT")
    cur.execute("ALTER TABLE carpet_log ADD COLUMN IF NOT EXISTS rescheduled INTEGER DEFAULT 0")

    # Ensure admin user exists
    cur.execute("SELECT id FROM users WHERE role='admin'")
    if not cur.fetchone():
        admin_password = os.environ.get("APP_ADMIN_PW", "ChangeMe123!")
        cur.execute(
            "INSERT INTO users (email, name, role, password_hash, is_active, created_at) "
            "VALUES (%s,%s,%s,%s,1,%s)",
            ("operations@tahoegetaways.com", "Admin", "admin",
             generate_password_hash(admin_password), datetime.utcnow().isoformat())
        )

    conn.commit()
    cur.close()
    conn.close()


# ── User model ────────────────────────────────────────────────────

class User(UserMixin):
    def __init__(self, id, email, name, role, is_active):
        self.id      = id
        self.email   = email
        self.name    = name
        self.role    = role
        self._active = is_active

    @property
    def is_active(self):
        return bool(self._active)

    @property
    def is_admin(self):
        return self.role == "admin"


# ── Time helpers ──────────────────────────────────────────────────

def hhmm_to_minutes(hhmm: str) -> int:
    parts = (hhmm or "").strip().split(":")
    if len(parts) != 2:
        raise ValueError("Invalid time format. Use HH:MM.")
    hh, mm = int(parts[0]), int(parts[1])
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        raise ValueError("Invalid time format. Use HH:MM.")
    return hh * 60 + mm


def minutes_to_hhmm(m: int) -> str:
    m = max(0, int(m))
    return f"{(m // 60) % 24:02d}:{m % 60:02d}"