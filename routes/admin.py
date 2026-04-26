"""
routes/admin.py — admin routes: user management, invites, properties, CSV upload.
"""

import json
import os
import re
import secrets
import csv
import io
from datetime import datetime, timedelta

import psycopg2.extras
import requests
from flask import (Blueprint, render_template, request, redirect,
                   url_for, flash, current_app, jsonify)
from flask_login import login_required, current_user
from werkzeug.security import generate_password_hash

from db import get_db, get_cursor, APP_BASE_URL
from routes.auth import admin_required

admin_bp = Blueprint("admin", __name__)


# ── User management ───────────────────────────────────────────────

@admin_bp.route("/admin/users")
@login_required
@admin_required
def admin_users():
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        "SELECT id, email, name, role, is_active, created_at "
        "FROM users ORDER BY created_at DESC"
    )
    users = cur.fetchall()
    cur.execute(
        """SELECT i.email, i.expires_at, i.created_at, u.name AS invited_by_name
           FROM invites i
           JOIN users u ON i.invited_by = u.id
           WHERE i.used = 0 AND i.expires_at > %s
           ORDER BY i.created_at DESC""",
        (datetime.utcnow().isoformat(),)
    )
    invites = cur.fetchall()
    cur.close(); conn.close()
    return render_template("admin.html", users=users, invites=invites)


@admin_bp.route("/admin/users/add", methods=["POST"])
@login_required
@admin_required
def admin_add_user():
    email    = (request.form.get("email") or "").strip().lower()
    name     = (request.form.get("name") or "").strip()
    role     = request.form.get("role", "user")
    password = request.form.get("password") or ""

    if not email or not name or not password:
        flash("Email, name, and password are all required.", "error")
        return redirect(url_for("admin.admin_users"))

    if role not in ("admin", "user"):
        role = "user"

    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("SELECT id FROM users WHERE email = %s", (email,))
    if cur.fetchone():
        flash(f"{email} already exists.", "error")
        cur.close(); conn.close()
        return redirect(url_for("admin.admin_users"))

    cur.execute(
        "INSERT INTO users (email, name, role, password_hash, is_active, created_at) "
        "VALUES (%s,%s,%s,%s,1,%s)",
        (email, name, role, generate_password_hash(password), datetime.utcnow().isoformat())
    )
    conn.commit()
    cur.close(); conn.close()
    flash(f"User {name} ({email}) created.", "success")
    return redirect(url_for("admin.admin_users"))


@admin_bp.route("/admin/users/<int:user_id>/toggle", methods=["POST"])
@login_required
@admin_required
def admin_toggle_user(user_id):
    if user_id == current_user.id:
        flash("You cannot deactivate your own account.", "error")
        return redirect(url_for("admin.admin_users"))

    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("SELECT is_active, name FROM users WHERE id = %s", (user_id,))
    row = cur.fetchone()
    if row:
        new_state = 0 if row["is_active"] else 1
        cur.execute("UPDATE users SET is_active = %s WHERE id = %s", (new_state, user_id))
        conn.commit()
        status = "activated" if new_state else "deactivated"
        flash(f"{row['name']} has been {status}.", "success")
    cur.close(); conn.close()
    return redirect(url_for("admin.admin_users"))


@admin_bp.route("/admin/users/<int:user_id>/reset-password", methods=["POST"])
@login_required
@admin_required
def admin_reset_password(user_id):
    new_password = request.form.get("password") or ""
    if len(new_password) < 8:
        flash("Password must be at least 8 characters.", "error")
        return redirect(url_for("admin.admin_users"))

    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        "UPDATE users SET password_hash = %s WHERE id = %s",
        (generate_password_hash(new_password), user_id)
    )
    conn.commit()
    cur.close(); conn.close()
    flash("Password updated.", "success")
    return redirect(url_for("admin.admin_users"))


@admin_bp.route("/admin/users/<int:user_id>/delete", methods=["POST"])
@login_required
@admin_required
def admin_delete_user(user_id):
    if user_id == current_user.id:
        flash("You cannot delete your own account.", "error")
        return redirect(url_for("admin.admin_users"))

    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("SELECT name, email, role FROM users WHERE id = %s", (user_id,))
    row = cur.fetchone()
    if not row:
        flash("User not found.", "error")
        cur.close(); conn.close()
        return redirect(url_for("admin.admin_users"))

    name = row["name"]

    # Stamp the deleted user's display name on their routes BEFORE reassigning,
    # so the "created by" attribution remains visible even after the account is gone.
    # Only stamp rows that don't already have a display override (e.g. a prior deletion).
    cur.execute(
        "UPDATE saved_routes SET created_by_display = %s "
        "WHERE created_by = %s AND (created_by_display IS NULL OR created_by_display = '')",
        (f"{name} (deleted)", user_id)
    )
    # Now reassign the FK so the JOIN in all queries continues to resolve.
    cur.execute(
        "UPDATE saved_routes SET created_by = %s WHERE created_by = %s",
        (current_user.id, user_id)
    )
    cur.execute(
        "UPDATE saved_routes SET last_edited_by = %s WHERE last_edited_by = %s",
        (current_user.id, user_id)
    )
    # Reassign carpet log entries the same way
    cur.execute(
        "UPDATE carpet_log SET logged_by = %s WHERE logged_by = %s",
        (current_user.id, user_id)
    )
    # Cancel any pending invites sent by this user
    cur.execute("DELETE FROM invites WHERE invited_by = %s AND used = 0", (user_id,))
    # Cancel any unused invite this user was sent (cleanup)
    cur.execute("DELETE FROM invites WHERE email = %s AND used = 0", (row["email"],))
    # Delete the user
    cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
    conn.commit()
    cur.close(); conn.close()

    flash(f"Account for {name} has been permanently deleted. Their routes have been reassigned to you.", "success")
    return redirect(url_for("admin.admin_users"))


# ── Invite system ─────────────────────────────────────────────────

@admin_bp.route("/admin/invite", methods=["POST"])
@login_required
@admin_required
def admin_invite():
    data  = request.get_json(force=True) if request.is_json else None
    email = ((data or {}).get("email") or request.form.get("email") or "").strip().lower()

    if not email:
        if request.is_json:
            return jsonify({"error": "Email address is required."}), 400
        flash("Email address is required.", "error")
        return redirect(url_for("admin.admin_users"))

    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("SELECT id FROM users WHERE email = %s", (email,))
    if cur.fetchone():
        cur.close(); conn.close()
        if request.is_json:
            return jsonify({"error": f"{email} already has an account."}), 400
        flash(f"{email} already has an account.", "error")
        return redirect(url_for("admin.admin_users"))

    cur.execute("DELETE FROM invites WHERE email = %s AND used = 0", (email,))

    token        = secrets.token_urlsafe(32)
    expires_at   = (datetime.utcnow() + timedelta(hours=48)).isoformat()
    now          = datetime.utcnow().isoformat()
    register_url = f"{APP_BASE_URL}/register/{token}"

    cur.execute(
        "INSERT INTO invites (email, token, invited_by, expires_at, used, created_at) "
        "VALUES (%s,%s,%s,%s,0,%s)",
        (email, token, current_user.id, expires_at, now)
    )
    conn.commit()
    cur.close(); conn.close()

    email_subject = "You're invited to North Lake Dispatch"
    email_body = (
        f"Hi,\n\n"
        f"You've been invited to join North Lake Dispatch — "
        f"Tahoe Getaways' internal routing and dispatch tool.\n\n"
        f"Click the link below to create your account. "
        f"This link expires in 48 hours:\n\n"
        f"{register_url}\n\n"
        f"Questions? Reach out to the operations team.\n\n"
        f"— Tahoe Getaways Operations"
    )

    return jsonify({
        "success":       True,
        "invite_to":     email,
        "register_url":  register_url,
        "email_subject": email_subject,
        "email_body":    email_body,
    })


# ── Property management ───────────────────────────────────────────

@admin_bp.route("/admin/properties")
@login_required
@admin_required
def admin_properties():
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("""
        SELECT id, "Property Name", "Unit Address", "Latitude", "Longitude"
        FROM properties ORDER BY "Property Name" ASC
    """)
    props = cur.fetchall()
    cur.close(); conn.close()
    return render_template("admin_properties.html", properties=props)


@admin_bp.route("/admin/properties/geocode", methods=["POST"])
@login_required
@admin_required
def geocode_address():
    data    = request.json or {}
    address = (data.get("address") or "").strip()
    name    = (data.get("name") or "").strip()
    if not address:
        return jsonify({"error": "Address is required. Enter a street address before geocoding."}), 400

    # ── Strip unit numbers — Nominatim works at street level only ──
    geocode_base = re.sub(
        r'\s*,?\s*(#\s*\d+[a-zA-Z]?|apt\.?\s+\w+|suite\s+\w+|unit\s+\w+|ste\.?\s+\w+)',
        '', address, flags=re.IGNORECASE
    ).strip().rstrip(',').strip()

    suffixes = [
        ", Carnelian Bay, CA", ", Truckee, CA",
        ", Lake Tahoe, CA", ", Tahoe City, CA", ", South Lake Tahoe, CA",
        ", Kings Beach, CA", ", Tahoe Vista, CA", ", Tahoma, CA",
        ", Incline Village, NV", ", Crystal Bay, NV",
        ", California", "",
    ]

    for suffix in suffixes:
        query = geocode_base + suffix
        try:
            resp = requests.get(
                "https://nominatim.openstreetmap.org/search",
                params={"q": query, "format": "json", "limit": 1},
                headers={"User-Agent": "TahoeDispatch/1.0"},
                timeout=10
            )
            results = resp.json()
            if results:
                r   = results[0]
                lat = float(r["lat"])
                lng = float(r["lon"])
                if 38.5 <= lat <= 40.0 and -120.8 <= lng <= -119.4:
                    return jsonify({
                        "lat":            lat,
                        "lng":            lng,
                        "display_name":   r.get("display_name", ""),
                        "resolved_query": query,
                    })
        except Exception as e:
            current_app.logger.warning(f"Nominatim geocode attempt failed for '{query}': {e}")
            continue

    # ── Nominatim failed — try Breezeway property list ──
    bw_error = None
    if name:
        from routes.briefing import _get_breezeway_token
        bw_token = _get_breezeway_token()
        if not bw_token:
            bw_error = "Breezeway credentials not configured"
        else:
            try:
                name_lower = name.lower().strip()
                page = 1
                bw_found_any = False
                while True:
                    resp = requests.get(
                        "https://api.breezeway.io/public/inventory/v1/property",
                        headers={"Authorization": f"JWT {bw_token}"},
                        params={"limit": 100, "page": page, "status": "active"},
                        timeout=15,
                    )
                    if not resp.ok:
                        bw_error = f"Breezeway API returned {resp.status_code}: {resp.text[:200]}"
                        break
                    bw_data = resp.json()
                    props = (bw_data.get("results") or bw_data.get("data") or []) \
                            if isinstance(bw_data, dict) else (bw_data or [])
                    if not props:
                        bw_error = f"Breezeway returned 0 properties on page {page}"
                        break
                    bw_found_any = True
                    for prop in props:
                        bw_name = (prop.get("name") or prop.get("property_name") or "").lower().strip()
                        if bw_name == name_lower:
                            lat = prop.get("latitude")
                            lng = prop.get("longitude")
                            if lat and lng:
                                lat, lng = float(lat), float(lng)
                                if 38.5 <= lat <= 40.0 and -120.8 <= lng <= -119.4:
                                    return jsonify({
                                        "lat":            lat,
                                        "lng":            lng,
                                        "display_name":   prop.get("name") or name,
                                        "resolved_query": f"Breezeway: {name}",
                                    })
                    if len(props) < 100:
                        if bw_found_any:
                            bw_error = f"Breezeway searched {page} page(s) — '{name}' not found (name may differ in Breezeway)"
                        break
                    page += 1
            except Exception as e:
                bw_error = f"{type(e).__name__}: {e}"
                current_app.logger.warning(f"Breezeway property lookup failed for '{name}': {e}")

    # ── Last resort: find similar properties in our own DB ──
    suggestions = []
    keywords = [w for w in re.split(r'\W+', (name + " " + geocode_base)) if len(w) >= 4]
    if keywords:
        conn = get_db()
        cur  = get_cursor(conn)
        conditions = ' OR '.join(['"Property Name" ILIKE %s OR "Unit Address" ILIKE %s'
                                  for _ in keywords])
        params = []
        for kw in keywords:
            params += [f'%{kw}%', f'%{kw}%']
        cur.execute(
            f'SELECT "Property Name", "Unit Address", "Latitude", "Longitude" '
            f'FROM properties WHERE "Latitude" IS NOT NULL AND ({conditions}) LIMIT 5',
            params
        )
        suggestions = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()

    debug = bw_error or "Breezeway: no name provided"
    current_app.logger.warning(f"Geocode failed for '{name}' / '{geocode_base}': {debug}")
    return jsonify({
        "error":       f"Nominatim couldn't place this address, and Breezeway lookup failed ({debug}). "
                       + (f"Found {len(suggestions)} similar propert{'y' if len(suggestions)==1 else 'ies'} in database — pick one below, or enter manually."
                          if suggestions else "Use 'Enter coordinates manually' below."),
        "suggestions": [
            {"name": s["Property Name"], "address": s["Unit Address"],
             "lat": float(s["Latitude"]), "lng": float(s["Longitude"])}
            for s in suggestions
        ],
    }), 404


@admin_bp.route("/admin/properties/add", methods=["POST"])
@login_required
@admin_required
def add_property():
    data    = request.json or {}
    name    = (data.get("name") or "").strip()
    address = (data.get("address") or "").strip()
    lat     = data.get("lat")
    lng     = data.get("lng")

    if not name or not address or lat is None or lng is None:
        missing = [f for f, v in [("name", name), ("address", address), ("lat", lat), ("lng", lng)] if not v and v != 0]
        return jsonify({"error": f"Can't save the property — the following fields are missing: {', '.join(missing)}. Geocode the address first to get coordinates."}), 400

    try:
        lat = float(lat); lng = float(lng)
    except (TypeError, ValueError) as e:
        return jsonify({"error": f"Coordinates must be numbers (e.g. lat: 39.34, lng: -120.21). Got lat={lat!r}, lng={lng!r}. Detail: {e}"}), 400

    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        'INSERT INTO properties ("Property Name", "Unit Address", "Latitude", "Longitude") '
        'VALUES (%s,%s,%s,%s) RETURNING id',
        (name, address, lat, lng)
    )
    new_id = cur.fetchone()["id"]
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"success": True, "id": new_id})


@admin_bp.route("/admin/properties/<int:prop_id>/update", methods=["POST"])
@login_required
@admin_required
def update_property(prop_id):
    data    = request.json or {}
    name    = (data.get("name") or "").strip()
    address = (data.get("address") or "").strip()
    lat     = data.get("lat")
    lng     = data.get("lng")

    if not name or not address or lat is None or lng is None:
        missing = [f for f, v in [("name", name), ("address", address), ("lat", lat), ("lng", lng)] if not v and v != 0]
        return jsonify({"error": f"Can't update the property — the following fields are missing: {', '.join(missing)}."}), 400

    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        'UPDATE properties SET "Property Name"=%s, "Unit Address"=%s, '
        '"Latitude"=%s, "Longitude"=%s WHERE id=%s',
        (name, address, float(lat), float(lng), prop_id)
    )
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"success": True})


@admin_bp.route("/admin/properties/<int:prop_id>/delete", methods=["POST"])
@login_required
@admin_required
def delete_property(prop_id):
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("DELETE FROM properties WHERE id = %s", (prop_id,))
    conn.commit()
    cur.close(); conn.close()
    flash("Property removed.", "success")
    return redirect(url_for("admin.admin_properties"))


# ── CSV upload ────────────────────────────────────────────────────

@admin_bp.route("/admin/upload-csv", methods=["POST"])
@login_required
@admin_required
def upload_csv():
    f = request.files.get("csv_file")
    if not f or not f.filename.endswith(".csv"):
        flash("Please upload a .csv file. Other file types are not accepted.", "error")
        return redirect(url_for("admin.admin_users"))

    try:
        stream  = io.StringIO(f.stream.read().decode("utf-8-sig"), newline=None)
        reader  = csv.DictReader(stream)

        required = {"Property Name", "Unit Address", "Latitude", "Longitude"}
        if not required.issubset(set(reader.fieldnames or [])):
            missing = required - set(reader.fieldnames or [])
            flash(
                f"The CSV is missing required columns: {', '.join(sorted(missing))}. "
                f"Required headers are: Property Name, Unit Address, Latitude, Longitude.",
                "error"
            )
            return redirect(url_for("admin.admin_users"))

        rows      = []
        skipped   = 0
        for i, row in enumerate(reader, start=2):   # row 1 is the header
            try:
                rows.append((
                    row["Property Name"], row["Unit Address"],
                    float(row["Latitude"]), float(row["Longitude"])
                ))
            except (ValueError, KeyError) as e:
                skipped += 1
                current_app.logger.warning(f"CSV row {i} skipped: {e} — data: {dict(row)}")
                continue

        if not rows:
            flash(
                "No valid rows were found in the CSV. "
                "Check that Latitude and Longitude columns contain numbers and that no rows are empty.",
                "error"
            )
            return redirect(url_for("admin.admin_users"))

        conn = get_db()
        cur  = get_cursor(conn)
        cur.execute("DELETE FROM properties")
        psycopg2.extras.execute_values(
            cur,
            'INSERT INTO properties ("Property Name", "Unit Address", '
            '"Latitude", "Longitude") VALUES %s',
            rows
        )
        conn.commit()
        cur.close(); conn.close()

        msg = f"Properties reloaded — {len(rows)} imported."
        if skipped:
            msg += f" {skipped} row(s) were skipped due to missing or non-numeric coordinates (see server logs for details)."
        flash(msg, "success")

    except Exception as e:
        flash(
            f"Upload failed — the file couldn't be processed. "
            f"Make sure it's a valid UTF-8 CSV and try again. Detail: {e}",
            "error"
        )

    return redirect(url_for("admin.admin_users"))


# ── Teams ─────────────────────────────────────────────────────────

@admin_bp.route("/admin/teams", methods=["GET"])
@login_required
@admin_required
def admin_get_teams():
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("""
        SELECT t.id, t.name,
               COALESCE(json_agg(
                   json_build_object('id', u.id, 'name', u.name, 'email', u.email)
                   ORDER BY u.name
               ) FILTER (WHERE u.id IS NOT NULL), '[]') AS members
        FROM teams t
        LEFT JOIN team_memberships tm ON tm.team_id = t.id
        LEFT JOIN users u ON u.id = tm.user_id
        GROUP BY t.id, t.name
        ORDER BY t.name ASC
    """)
    teams = cur.fetchall()
    cur.execute("SELECT id, name, email FROM users WHERE is_active = 1 ORDER BY name ASC")
    users = cur.fetchall()
    cur.close(); conn.close()
    return jsonify({
        "teams": [dict(t) for t in teams],
        "users": [dict(u) for u in users],
    })


@admin_bp.route("/admin/teams/create", methods=["POST"])
@login_required
@admin_required
def admin_create_team():
    data = request.get_json(force=True)
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "Team name is required."}), 400
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("SELECT id FROM teams WHERE name = %s", (name,))
    if cur.fetchone():
        cur.close(); conn.close()
        return jsonify({"error": f"A team named '{name}' already exists."}), 400
    cur.execute(
        "INSERT INTO teams (name, created_by, created_at) VALUES (%s, %s, %s) RETURNING id",
        (name, current_user.id, datetime.utcnow().isoformat())
    )
    team_id = cur.fetchone()["id"]
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"success": True, "id": team_id, "name": name})


@admin_bp.route("/admin/teams/<int:team_id>/members", methods=["POST"])
@login_required
@admin_required
def admin_add_team_member(team_id):
    data    = request.get_json(force=True)
    user_id = data.get("user_id")
    if not user_id:
        return jsonify({"error": "user_id is required."}), 400
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        "INSERT INTO team_memberships (user_id, team_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
        (user_id, team_id)
    )
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"success": True})


@admin_bp.route("/admin/teams/<int:team_id>/members/<int:user_id>", methods=["DELETE"])
@login_required
@admin_required
def admin_remove_team_member(team_id, user_id):
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        "DELETE FROM team_memberships WHERE user_id = %s AND team_id = %s",
        (user_id, team_id)
    )
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"success": True})


@admin_bp.route("/admin/teams/<int:team_id>/delete", methods=["POST"])
@login_required
@admin_required
def admin_delete_team(team_id):
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("SELECT name FROM teams WHERE id = %s", (team_id,))
    row = cur.fetchone()
    if not row:
        cur.close(); conn.close()
        return jsonify({"error": "Team not found."}), 404
    if row["name"] == "Property Specialist":
        cur.close(); conn.close()
        return jsonify({"error": "Cannot delete the Property Specialist team."}), 400
    cur.execute("SELECT id FROM teams WHERE name = 'Property Specialist'")
    ps = cur.fetchone()
    if ps:
        cur.execute("UPDATE saved_routes SET team_id = %s WHERE team_id = %s", (ps["id"], team_id))
    cur.execute("DELETE FROM team_memberships WHERE team_id = %s", (team_id,))
    cur.execute("DELETE FROM teams WHERE id = %s", (team_id,))
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"success": True})


# ── DB download stub ──────────────────────────────────────────────

@admin_bp.route("/admin/download-db")
@login_required
@admin_required
def download_db():
    return jsonify({"info": "Database is PostgreSQL. Use Railway dashboard for backups."}), 200


# ── AI Chatbot ────────────────────────────────────────────────────

@admin_bp.route("/admin/chatbot")
@login_required
@admin_required
def chatbot_page():
    return render_template("admin_chatbot.html")


@admin_bp.route("/admin/chatbot/chat", methods=["POST"])
@login_required
@admin_required
def chatbot_chat():
    import anthropic
    from routes.briefing import (
        _fetch_todays_routes, _fetch_breezeway_checkins,
        _fetch_breezeway_checkouts, _classify_reservation,
        _get_property_name,
    )

    data     = request.get_json(force=True)
    messages = data.get("messages", [])
    dates    = data.get("dates", [])

    if not dates:
        return jsonify({"error": "Select at least one date first."}), 400
    if not messages:
        return jsonify({"error": "No message provided."}), 400

    # Fetch and build context for up to 7 dates
    context_blocks   = []
    context_summary  = []

    for date_str in dates[:7]:
        try:
            routes    = _fetch_todays_routes(date_str)
            checkins  = _fetch_breezeway_checkins(date_str)
            checkouts = _fetch_breezeway_checkouts(date_str)

            block = [f"\n=== {date_str} ==="]

            if routes:
                block.append(f"Saved routes ({len(routes)}):")
                for r in routes:
                    stops = [s for s in json.loads(r["stops_json"] or "[]")
                             if not s.get("isLunch")]
                    line  = f"  - \"{r['name']}\""
                    if r["assigned_to"]:
                        line += f" → {r['assigned_to']}"
                    line += f": {len(stops)} stop{'s' if len(stops) != 1 else ''}"
                    if (r.get("notes") or "").strip():
                        line += f". Notes: {r['notes'].strip()}"
                    block.append(line)
            else:
                block.append("No routes saved for this date.")

            if checkins:
                block.append(f"Arrivals ({len(checkins)}):")
                for r in checkins:
                    kind = _classify_reservation(r)
                    prop = _get_property_name(r.get("property_id"))
                    t    = r.get("checkin_time", "")
                    line = f"  - [{kind.upper()}] {prop}"
                    if t:
                        line += f" at {t[:5]}"
                    block.append(line)
            else:
                block.append("No arrivals this date.")

            if checkouts:
                block.append(f"Departures ({len(checkouts)}):")
                for r in checkouts:
                    kind = _classify_reservation(r)
                    prop = _get_property_name(r.get("property_id"))
                    t    = r.get("checkout_time", "")
                    line = f"  - [{kind.upper()}] {prop}"
                    if t:
                        line += f" by {t[:5]}"
                    block.append(line)
            else:
                block.append("No departures this date.")

            context_blocks.append("\n".join(block))
            context_summary.append({
                "date":       date_str,
                "routes":     len(routes),
                "arrivals":   len(checkins),
                "departures": len(checkouts),
            })
        except Exception as e:
            context_blocks.append(f"\n=== {date_str} ===\nData load error: {e}")
            context_summary.append({"date": date_str, "error": str(e)})

    system_prompt = (
        "You are an AI operations assistant for Tahoe Getaways, a vacation rental company "
        "in Lake Tahoe. You help the operations team understand their schedule, guest "
        "arrivals and departures, and flag any issues.\n\n"
        "Classification key used in the data below:\n"
        "  GUEST  = paying guest stay\n"
        "  OWNER  = owner stay or owner-booked reservation\n"
        "  LEASE  = long-term stay (30+ days)\n"
        "  BLOCK  = maintenance block, hold, or owner block — no guests, property unavailable\n\n"
        "A Post Rental Inspection (PRI) is required whenever a non-guest reservation "
        "(OWNER, BLOCK) follows directly after a GUEST stay at the same property. "
        "It is a 1-hour damage inspection before the owner arrives.\n\n"
        "Data loaded for the selected date(s):\n"
        + "\n".join(context_blocks)
        + "\n\nIf asked about something not in this data, say so clearly."
    )

    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        return jsonify({"error": "ANTHROPIC_API_KEY not configured."}), 500

    try:
        client = anthropic.Anthropic(api_key=key)
        resp   = client.messages.create(
            model      = "claude-sonnet-4-6",
            max_tokens = 1024,
            system     = system_prompt,
            messages   = messages,
        )
        return jsonify({
            "reply":           resp.content[0].text,
            "context_summary": context_summary,
            "system_prompt":   system_prompt,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500