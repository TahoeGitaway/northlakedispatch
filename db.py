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

    cur.execute("""CREATE TABLE IF NOT EXISTS briefing_notes (
        id SERIAL PRIMARY KEY,
        note_date TEXT NOT NULL UNIQUE,
        note_text TEXT NOT NULL DEFAULT '',
        updated_by INTEGER,
        updated_at TEXT
    )""")
    cur.execute("ALTER TABLE briefing_notes ADD COLUMN IF NOT EXISTS staff_list TEXT DEFAULT ''")
    cur.execute("ALTER TABLE briefing_notes ADD COLUMN IF NOT EXISTS staff_updated_at TEXT")
    cur.execute("ALTER TABLE briefing_notes ADD COLUMN IF NOT EXISTS blurb TEXT DEFAULT ''")
    cur.execute("ALTER TABLE briefing_notes ADD COLUMN IF NOT EXISTS blurb_generated_at TEXT")

    cur.execute("""CREATE TABLE IF NOT EXISTS teams (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        created_by INTEGER,
        created_at TEXT
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS team_memberships (
        user_id INTEGER NOT NULL,
        team_id INTEGER NOT NULL,
        PRIMARY KEY (user_id, team_id)
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS projects (
        id          SERIAL PRIMARY KEY,
        name        TEXT NOT NULL,
        description TEXT DEFAULT '',
        status      TEXT DEFAULT 'active',
        created_by  INTEGER REFERENCES users(id),
        created_at  TEXT
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS project_properties (
        id            SERIAL PRIMARY KEY,
        project_id    INTEGER REFERENCES projects(id) ON DELETE CASCADE,
        property_name TEXT NOT NULL,
        address       TEXT DEFAULT '',
        lat           DOUBLE PRECISION,
        lng           DOUBLE PRECISION,
        added_at      TEXT,
        added_by      INTEGER REFERENCES users(id)
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS task_completions (
        id                  SERIAL PRIMARY KEY,
        project_property_id INTEGER REFERENCES project_properties(id) ON DELETE CASCADE,
        completed_by        INTEGER REFERENCES users(id),
        completed_at        TEXT,
        comment             TEXT DEFAULT '',
        task_type           TEXT DEFAULT 'departure_clean'
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS chatbot_knowledge (
        id         SERIAL PRIMARY KEY,
        title      TEXT NOT NULL,
        category   TEXT DEFAULT 'General',
        body       TEXT NOT NULL,
        is_active  INTEGER DEFAULT 1,
        created_by INTEGER REFERENCES users(id),
        updated_by INTEGER REFERENCES users(id),
        created_at TEXT,
        updated_at TEXT
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS bot_interactions (
        id           SERIAL PRIMARY KEY,
        user_id      INTEGER REFERENCES users(id),
        session_id   TEXT DEFAULT '',
        query        TEXT NOT NULL,
        response     TEXT NOT NULL,
        action_taken TEXT DEFAULT '',
        dates_loaded TEXT DEFAULT '',
        created_at   TEXT
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS saved_day_summaries (
        route_date  TEXT PRIMARY KEY,
        arrivals    TEXT NOT NULL DEFAULT '{}',
        departures  TEXT NOT NULL DEFAULT '{}',
        saved_by    INTEGER REFERENCES users(id),
        saved_at    TEXT NOT NULL
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS chatbot_sessions (
        id            SERIAL PRIMARY KEY,
        user_id       INTEGER REFERENCES users(id),
        session_id    TEXT NOT NULL UNIQUE,
        title         TEXT DEFAULT '',
        messages_json TEXT NOT NULL DEFAULT '[]',
        created_at    TEXT,
        updated_at    TEXT
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS pri_dismissals (
        id            SERIAL PRIMARY KEY,
        item_key      TEXT NOT NULL UNIQUE,
        dismissed_by  INTEGER REFERENCES users(id),
        dismissed_at  TEXT NOT NULL
    )""")

    # Temporary "snooze" for the PRI Check page only (separate from the red
    # banner ✕). Hides a flagged PRI until snoozed_until, then it returns so
    # ops can re-check after a reservation has had time to change.
    cur.execute("""CREATE TABLE IF NOT EXISTS pri_snoozes (
        id            SERIAL PRIMARY KEY,
        item_key      TEXT NOT NULL UNIQUE,
        snoozed_until TEXT NOT NULL,
        snoozed_by    INTEGER REFERENCES users(id)
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS pri_banner_alerts (
        id            SERIAL PRIMARY KEY,
        item_key      TEXT NOT NULL UNIQUE,
        property_name TEXT NOT NULL,
        checkout_date TEXT NOT NULL,
        next_checkin  TEXT,
        alert_type    TEXT NOT NULL,
        created_at    TEXT NOT NULL,
        dismissed_at  TEXT,
        dismissed_by  INTEGER REFERENCES users(id)
    )""")
    # Same-day "snooze" for the red banner ✕: hides the alert until this UTC
    # timestamp (the snoozer's local midnight). Distinct from dismissed_at, which
    # is the permanent "✓ Done" action from the PRI Check page.
    cur.execute("ALTER TABLE pri_banner_alerts ADD COLUMN IF NOT EXISTS snoozed_until TEXT")

    cur.execute("""CREATE TABLE IF NOT EXISTS asana_notifications (
        id           SERIAL PRIMARY KEY,
        item_key     TEXT NOT NULL UNIQUE,
        task_gid     TEXT NOT NULL,
        task_name    TEXT NOT NULL,
        story_gid    TEXT NOT NULL,
        commenter    TEXT NOT NULL DEFAULT '',
        comment_text TEXT NOT NULL DEFAULT '',
        asana_created_at TEXT,
        created_at   TEXT NOT NULL,
        dismissed_at TEXT,
        replied_at   TEXT
    )""")
    cur.execute("ALTER TABLE asana_notifications ADD COLUMN IF NOT EXISTS parent_name TEXT")

    cur.execute("""CREATE TABLE IF NOT EXISTS asana_poll_state (
        id           SERIAL PRIMARY KEY,
        key          TEXT NOT NULL UNIQUE,
        value        TEXT NOT NULL
    )""")

    # Temporary VIP reservation tracker — checklist + notes per reservation.
    cur.execute("""CREATE TABLE IF NOT EXISTS vip_tracker (
        id          SERIAL PRIMARY KEY,
        item_key    TEXT NOT NULL UNIQUE,
        done        INTEGER NOT NULL DEFAULT 0,
        notes       TEXT NOT NULL DEFAULT '',
        updated_at  TEXT,
        updated_by  INTEGER REFERENCES users(id)
    )""")

    # Attributed notes/comments on each VIP reservation (who said what).
    cur.execute("""CREATE TABLE IF NOT EXISTS vip_comments (
        id          SERIAL PRIMARY KEY,
        item_key    TEXT NOT NULL,
        author_id   INTEGER REFERENCES users(id),
        author      TEXT NOT NULL DEFAULT '',
        body        TEXT NOT NULL,
        created_at  TEXT NOT NULL
    )""")

    # Group-batcher assignment allow-list: the ONLY people the batcher may assign
    # tasks to. Editable from the group-assign page (people leave / get hired).
    cur.execute("""CREATE TABLE IF NOT EXISTS assignment_candidates (
        id          SERIAL PRIMARY KEY,
        name        TEXT NOT NULL,
        name_key    TEXT NOT NULL UNIQUE,
        created_at  TEXT NOT NULL,
        created_by  INTEGER REFERENCES users(id)
    )""")
    # Seed the initial roster ONCE (only when the table is completely empty, so
    # later removals aren't undone on every restart).
    # Canonical maintenance-team allow-list — full Breezeway names + the two dispatch
    # zone accounts. Seeded when empty; the upgrade block corrects earlier first-name
    # rows on existing installs (roster auto-resolve can't disambiguate "Sean"/"Alec"
    # because Breezeway also lists combined "Sean Kearney, Zack…" entries).
    _CANDIDATE_FIRST_TO_FULL = {
        "jeremy": "Jeremy Neifert", "sean": "Sean Kearney", "andy": "Andy Rosman",
        "chris": "Chris Marin", "calder": "Calder McCarron", "jonah": "Jonah Buchanan-Caldwell",
        "irving": "Irving Pantoja", "julie": "Julie Rohrback", "trevor": "Trevor Bales",
        "drew": "Drew Schott", "alec": "Alec Carlson", "steve": "Steve Rauch",
    }
    _CANDIDATE_SEED = list(_CANDIDATE_FIRST_TO_FULL.values()) + ["89 Zone", "267 Zone"]
    _now = datetime.utcnow().isoformat()

    cur.execute("SELECT COUNT(*) AS n FROM assignment_candidates")
    if (cur.fetchone() or {}).get("n", 0) == 0:
        for _nm in _CANDIDATE_SEED:
            cur.execute(
                "INSERT INTO assignment_candidates (name, name_key, created_at) "
                "VALUES (%s, %s, %s) ON CONFLICT (name_key) DO NOTHING",
                (_nm, _nm.lower().strip(), _now),
            )

    # Upgrade any legacy bare first-name rows to the full name (idempotent; a no-op
    # once done, and it never re-adds a name the user later removed).
    for _first, _full in _CANDIDATE_FIRST_TO_FULL.items():
        cur.execute("SELECT 1 FROM assignment_candidates WHERE name_key = %s", (_full.lower(),))
        if cur.fetchone():
            cur.execute("DELETE FROM assignment_candidates WHERE name_key = %s", (_first,))
        else:
            cur.execute("UPDATE assignment_candidates SET name = %s, name_key = %s WHERE name_key = %s",
                        (_full, _full.lower(), _first))

    # One-time: add the two zone accounts to existing installs, guarded by a flag so a
    # later manual removal is respected (never re-added on subsequent boots).
    cur.execute("""CREATE TABLE IF NOT EXISTS app_migration_flags (
        flag TEXT PRIMARY KEY, applied_at TEXT NOT NULL)""")
    cur.execute("SELECT 1 FROM app_migration_flags WHERE flag = %s", ("candidate_zones_2026_06",))
    if not cur.fetchone():
        for _z in ("89 Zone", "267 Zone"):
            cur.execute(
                "INSERT INTO assignment_candidates (name, name_key, created_at) "
                "VALUES (%s, %s, %s) ON CONFLICT (name_key) DO NOTHING",
                (_z, _z.lower(), _now),
            )
        cur.execute("INSERT INTO app_migration_flags (flag, applied_at) VALUES (%s, %s) "
                    "ON CONFLICT (flag) DO NOTHING", ("candidate_zones_2026_06", _now))

    # Off-list-assignee monitor: department-name keywords to EXCLUDE from the scan
    # (cleaning + vendor work uses rosters other than the maintenance allow-list, so
    # those tasks must not flag). Editable from the monitor page. Substring match,
    # case-insensitive (e.g. "clean" matches "Cleaning", "housekeep" → "Housekeeping").
    cur.execute("""CREATE TABLE IF NOT EXISTS assignee_monitor_ignored_depts (
        id          SERIAL PRIMARY KEY,
        keyword     TEXT NOT NULL UNIQUE,
        created_at  TEXT NOT NULL,
        created_by  INTEGER REFERENCES users(id)
    )""")
    cur.execute("SELECT COUNT(*) AS n FROM assignee_monitor_ignored_depts")
    if (cur.fetchone() or {}).get("n", 0) == 0:
        _now = datetime.utcnow().isoformat()
        for _kw in ("clean", "housekeep"):
            cur.execute(
                "INSERT INTO assignee_monitor_ignored_depts (keyword, created_at) "
                "VALUES (%s, %s) ON CONFLICT (keyword) DO NOTHING",
                (_kw, _now),
            )

    # Hot Tub Billing — durable storage so nothing is lost when the host wipes
    # the ephemeral filesystem on deploy/restart. `hot_tub_worksheets` holds each
    # month's read-only scan output (JSON); `hot_tub_overrides` holds Madeline's
    # local billing adjustments (comps / credits / resolutions) as JSON. Neither
    # is ever sent to Breezeway — this is app-side persistence only.
    cur.execute("""CREATE TABLE IF NOT EXISTS hot_tub_worksheets (
        month        TEXT PRIMARY KEY,
        payload      TEXT NOT NULL,
        generated_at TEXT,
        updated_at   TEXT NOT NULL
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS hot_tub_overrides (
        month      TEXT PRIMARY KEY,
        doc        TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )""")
    # A FROZEN/archived month: once she finishes billing a month she archives it,
    # which locks it read-only and records the final owner total for reference.
    cur.execute("""CREATE TABLE IF NOT EXISTS hot_tub_archived (
        month       TEXT PRIMARY KEY,
        revenue     INTEGER NOT NULL DEFAULT 0,
        archived_at TEXT NOT NULL,
        archived_by INTEGER
    )""")

    # Safe migrations
    cur.execute("ALTER TABLE saved_routes ADD COLUMN IF NOT EXISTS start_time TEXT")
    cur.execute("ALTER TABLE saved_routes ADD COLUMN IF NOT EXISTS start_location_json TEXT")
    cur.execute("ALTER TABLE saved_routes ADD COLUMN IF NOT EXISTS end_location_json TEXT")
    cur.execute("ALTER TABLE saved_routes ADD COLUMN IF NOT EXISTS assigned_to TEXT")
    cur.execute("ALTER TABLE saved_routes ADD COLUMN IF NOT EXISTS notes TEXT")
    cur.execute("ALTER TABLE saved_routes ADD COLUMN IF NOT EXISTS notes_public INTEGER DEFAULT 0")
    cur.execute("ALTER TABLE saved_routes ADD COLUMN IF NOT EXISTS created_by_display TEXT")
    cur.execute("ALTER TABLE saved_routes ADD COLUMN IF NOT EXISTS team_id INTEGER")
    cur.execute("ALTER TABLE saved_routes ADD COLUMN IF NOT EXISTS archived INTEGER DEFAULT 0")
    cur.execute("ALTER TABLE task_completions ADD COLUMN IF NOT EXISTS task_type TEXT DEFAULT 'departure_clean'")
    cur.execute("ALTER TABLE carpet_log ADD COLUMN IF NOT EXISTS property_name TEXT")
    cur.execute("ALTER TABLE carpet_log ADD COLUMN IF NOT EXISTS cleaner_name_2 TEXT")
    cur.execute("ALTER TABLE carpet_log ADD COLUMN IF NOT EXISTS rescheduled INTEGER DEFAULT 0")

    # Normalize smart (curly) apostrophes in property names to straight ones so
    # Breezeway names ("Bear's Lair", straight ') match the DB. One curly char
    # (U+2019) imported from the source CSV silently broke every name lookup for
    # that house. Idempotent — runs each boot; only touches rows still holding one.
    cur.execute(
        'UPDATE properties SET "Property Name" = '
        'REPLACE(REPLACE("Property Name", chr(8217), chr(39)), chr(8216), chr(39)) '
        "WHERE \"Property Name\" LIKE '%' || chr(8217) || '%' "
        "OR \"Property Name\" LIKE '%' || chr(8216) || '%'"
    )

    # Ensure default teams exist
    now_iso = datetime.utcnow().isoformat()
    for team_name in ("Property Specialist", "Project Management"):
        cur.execute(
            "INSERT INTO teams (name, created_at) VALUES (%s, %s) ON CONFLICT (name) DO NOTHING",
            (team_name, now_iso)
        )

    cur.execute("SELECT id FROM teams WHERE name = 'Property Specialist'")
    ps = cur.fetchone()
    if ps:
        ps_id = ps["id"]
        cur.execute("UPDATE saved_routes SET team_id = %s WHERE team_id IS NULL", (ps_id,))
        # Add every existing user to Property Specialist if not already a member
        cur.execute("SELECT id FROM users")
        for u in cur.fetchall():
            cur.execute(
                "INSERT INTO team_memberships (user_id, team_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (u["id"], ps_id)
            )

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