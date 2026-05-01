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

# ── Breezeway context cache (avoids re-fetching on every chat message) ──
import time as _time
_bw_ctx_cache: dict = {}   # {cache_key: (timestamp, checkins, checkouts)}
_BW_CTX_TTL = 10 * 60     # 10 minutes


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
    primary_admin_email = os.environ.get("PRIMARY_ADMIN_EMAIL", "operations@tahoegetaways.com")
    is_primary = current_user.email.lower() == primary_admin_email.lower()
    return render_template("admin.html", users=users, invites=invites, is_primary_admin=is_primary)


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


@admin_bp.route("/admin/users/<int:user_id>/role", methods=["POST"])
@login_required
@admin_required
def admin_change_role(user_id):
    primary_admin_email = os.environ.get("PRIMARY_ADMIN_EMAIL", "operations@tahoegetaways.com")
    if current_user.email.lower() != primary_admin_email.lower():
        flash("Only the primary admin can change user roles.", "error")
        return redirect(url_for("admin.admin_users"))
    if user_id == current_user.id:
        flash("You cannot change your own role.", "error")
        return redirect(url_for("admin.admin_users"))
    new_role = request.form.get("role")
    if new_role not in ("admin", "user"):
        flash("Invalid role.", "error")
        return redirect(url_for("admin.admin_users"))
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("UPDATE users SET role=%s WHERE id=%s", (new_role, user_id))
    conn.commit()
    cur.close(); conn.close()
    flash(f"Role updated to '{new_role}'.", "success")
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


# ── Knowledge Base ───────────────────────────────────────────────

@admin_bp.route("/admin/knowledge")
@login_required
@admin_required
def knowledge_list():
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("""
        SELECT k.*, u.name AS author
        FROM chatbot_knowledge k
        LEFT JOIN users u ON u.id = k.updated_by
        ORDER BY k.category, k.title
    """)
    entries = cur.fetchall()
    cur.close(); conn.close()
    return render_template("admin_knowledge.html", entries=entries)


@admin_bp.route("/admin/knowledge/save", methods=["POST"])
@login_required
@admin_required
def knowledge_save():
    data     = request.get_json(force=True)
    entry_id = data.get("id")
    title    = (data.get("title") or "").strip()
    category = (data.get("category") or "General").strip()
    body     = (data.get("body") or "").strip()

    if not title or not body:
        return jsonify({"error": "Title and body are required."}), 400

    now  = datetime.utcnow().isoformat()
    conn = get_db()
    cur  = get_cursor(conn)

    if entry_id:
        cur.execute("""
            UPDATE chatbot_knowledge
            SET title=%s, category=%s, body=%s, updated_by=%s, updated_at=%s
            WHERE id=%s
        """, (title, category, body, current_user.id, now, entry_id))
    else:
        cur.execute("""
            INSERT INTO chatbot_knowledge (title, category, body, is_active, created_by, updated_by, created_at, updated_at)
            VALUES (%s, %s, %s, 1, %s, %s, %s, %s)
        """, (title, category, body, current_user.id, current_user.id, now, now))

    conn.commit()
    cur.close(); conn.close()
    return jsonify({"success": True})


@admin_bp.route("/admin/knowledge/<int:entry_id>/toggle", methods=["POST"])
@login_required
@admin_required
def knowledge_toggle(entry_id):
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("SELECT is_active FROM chatbot_knowledge WHERE id=%s", (entry_id,))
    row = cur.fetchone()
    if not row:
        cur.close(); conn.close()
        return jsonify({"error": "Not found"}), 404
    new_state = 0 if row["is_active"] else 1
    cur.execute("UPDATE chatbot_knowledge SET is_active=%s WHERE id=%s", (new_state, entry_id))
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"success": True, "is_active": new_state})


@admin_bp.route("/admin/knowledge/<int:entry_id>/delete", methods=["POST"])
@login_required
@admin_required
def knowledge_delete(entry_id):
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("DELETE FROM chatbot_knowledge WHERE id=%s", (entry_id,))
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"success": True})


# ── AI Chatbot ────────────────────────────────────────────────────

@admin_bp.route("/admin/chatbot")
@login_required
def chatbot_page():
    return render_template("admin_chatbot.html")


@admin_bp.route("/admin/pri-check")
@login_required
@admin_required
def pri_check_page():
    return render_template("admin_pri_check.html")


@admin_bp.route("/admin/chatbot/chat", methods=["POST"])
@login_required
def chatbot_chat():
    import anthropic
    from routes.briefing import (
        _fetch_todays_routes, _fetch_bw_reservations,
        _get_breezeway_token, _classify_reservation,
        _get_property_name, _get_property_address, _extract_str,
    )

    data     = request.get_json(force=True)
    messages = data.get("messages", [])
    dates    = data.get("dates", [])

    if not dates:
        return jsonify({"error": "Select at least one date first."}), 400
    if not messages:
        return jsonify({"error": "No message provided."}), 400

    # Fetch Breezeway data for the full date range — cached 10 min per date range
    capped_dates = sorted(dates)[:7]
    min_date     = capped_dates[0]
    max_date     = capped_dates[-1]
    date_set     = set(capped_dates)

    cache_key = f"{min_date}:{max_date}"
    cached    = _bw_ctx_cache.get(cache_key)
    if cached and _time.time() - cached[0] < _BW_CTX_TTL:
        all_checkins, all_checkouts = cached[1], cached[2]
    else:
        try:
            token = _get_breezeway_token()
            if token:
                all_checkins  = _fetch_bw_reservations(token, {
                    "checkin_date_ge": min_date, "checkin_date_le": max_date,
                })
                all_checkouts = _fetch_bw_reservations(token, {
                    "checkout_date_ge": min_date, "checkout_date_le": max_date,
                })
            else:
                all_checkins = all_checkouts = []
        except Exception:
            all_checkins = all_checkouts = []
        _bw_ctx_cache[cache_key] = (_time.time(), all_checkins, all_checkouts)

    # Index by date
    checkins_by_date  = {}
    checkouts_by_date = {}
    for r in all_checkins:
        d = (r.get("checkin_date") or "")[:10]
        if d in date_set:
            checkins_by_date.setdefault(d, []).append(r)
    for r in all_checkouts:
        d = (r.get("checkout_date") or "")[:10]
        if d in date_set:
            checkouts_by_date.setdefault(d, []).append(r)

    context_blocks  = []
    context_summary = []

    for date_str in capped_dates:
        try:
            routes    = _fetch_todays_routes(date_str)
            checkins  = checkins_by_date.get(date_str, [])
            checkouts = checkouts_by_date.get(date_str, [])

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
                    kind     = _classify_reservation(r)
                    pid      = r.get("property_id")
                    prop     = _get_property_name(pid)
                    addr     = _get_property_address(pid)
                    t        = r.get("checkin_time", "")
                    checkout = (r.get("checkout_date") or "")[:10]
                    checkin  = (r.get("checkin_date")  or "")[:10]
                    tag_names = [_extract_str(tg) for tg in (r.get("tags") or [])]
                    nights   = ""
                    if checkin and checkout:
                        try:
                            from datetime import date as _date
                            n = (_date.fromisoformat(checkout) - _date.fromisoformat(checkin)).days
                            nights = f", {n} nights"
                        except Exception:
                            pass
                    prop_str = f"{prop}" + (f" — {addr}" if addr else "")
                    line = f"  - [{kind.upper()}] {prop_str} (checkin {checkin}, checkout {checkout}{nights})"
                    if t:
                        line += f" at {t[:5]}"
                    if tag_names:
                        line += f" [tags: {', '.join(tag_names)}]"
                    block.append(line)
            else:
                block.append("No arrivals this date.")

            if checkouts:
                block.append(f"Departures ({len(checkouts)}):")
                for r in checkouts:
                    kind     = _classify_reservation(r)
                    pid      = r.get("property_id")
                    prop     = _get_property_name(pid)
                    addr     = _get_property_address(pid)
                    t        = r.get("checkout_time", "")
                    checkout = (r.get("checkout_date") or "")[:10]
                    checkin  = (r.get("checkin_date")  or "")[:10]
                    tag_names = [_extract_str(tg) for tg in (r.get("tags") or [])]
                    nights   = ""
                    if checkin and checkout:
                        try:
                            from datetime import date as _date
                            n = (_date.fromisoformat(checkout) - _date.fromisoformat(checkin)).days
                            nights = f", {n} nights"
                        except Exception:
                            pass
                    prop_str = f"{prop}" + (f" — {addr}" if addr else "")
                    line = f"  - [{kind.upper()}] {prop_str} (checkin {checkin}, checkout {checkout}{nights})"
                    if t:
                        line += f" by {t[:5]}"
                    if tag_names:
                        line += f" [tags: {', '.join(tag_names)}]"
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

    # Load active knowledge base entries
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("""
        SELECT title, category, body FROM chatbot_knowledge
        WHERE is_active = 1 ORDER BY category, title
    """)
    knowledge_rows = cur.fetchall()
    cur.close(); conn.close()

    if knowledge_rows:
        kb_lines = ["=== COMPANY KNOWLEDGE BASE ===",
                    "The following policies and SOPs are from Tahoe Getaways. "
                    "Use them to answer questions accurately.\n"]
        for row in knowledge_rows:
            kb_lines.append(f"[{row['category']}: {row['title']}]")
            kb_lines.append(row["body"].strip())
            kb_lines.append("")
        knowledge_section = "\n".join(kb_lines) + "\n\n"
    else:
        knowledge_section = ""

    system_prompt = (
        f"You are the TG Operations Bot for Tahoe Getaways. Staff: {current_user.name}.\n\n"
        "RULES — follow exactly, no exceptions:\n"
        "1. Answer ONLY from the context provided below (SOPs, policies, Breezeway data).\n"
        "2. If you cannot answer from the provided context, respond: "
        "\"I don't have that information. Please refer to [name the relevant source] or check with your manager.\"\n"
        "3. Never guess, assume, or use general knowledge not present in this context.\n"
        "4. Be as brief as possible. Use bullet points for multi-part answers.\n"
        "5. Do not offer opinions, suggestions, or unrequested information.\n"
        "6. When staff asks you to take a write action (save note, flag property, mark complete), "
        "respond with a line starting exactly with 'CONFIRM_ACTION:' followed by a short description. "
        "Do not consider the action done until the staff member confirms.\n\n"
        + knowledge_section
        + "RESERVATION TYPES:\n"
        "  GUEST = paying guest stay\n"
        "  OWNER = owner stay or owner-booked reservation\n"
        "  LEASE = paying guest stay of 30+ days (long-term rental — still a paying guest, not an owner)\n"
        "  BLOCK = maintenance hold or owner block — no guests, property unavailable\n\n"
        "POST RENTAL INSPECTION (PRI):\n"
        "Required when a short-term GUEST (<30 days) checks out AND the next reservation at that "
        "property is OWNER or BLOCK. Also required if no upcoming reservation within 60 days (vacancy PRI).\n"
        "Flagged in Breezeway by adding 'owner next' tag to the incoming OWNER/BLOCK booking.\n"
        "Groups: 🔴 Needs tagging · 🟢 Already tagged · 🟠 Vacancy PRI (no upcoming booking found).\n\n"
        "LOADED DATA (routes + arrivals + departures for selected dates):\n"
        + "\n".join(context_blocks)
        + "\n\nIf asked about anything outside this context, say you don't have that information."
    )

    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        return jsonify({"error": "ANTHROPIC_API_KEY not configured."}), 500

    try:
        client = anthropic.Anthropic(api_key=key)
        # Keep last 20 messages to prevent unbounded context growth
        trimmed = messages[-20:] if len(messages) > 20 else messages
        resp   = client.messages.create(
            model      = "claude-sonnet-4-6",
            max_tokens = 1200,
            system     = system_prompt,
            messages   = trimmed,
        )
        reply_text = resp.content[0].text
        # Log the interaction (best-effort — never block the response)
        try:
            user_msg = messages[-1]["content"] if messages else ""
            conn_log = get_db()
            cur_log  = get_cursor(conn_log)
            cur_log.execute(
                "INSERT INTO bot_interactions "
                "(user_id, session_id, query, response, dates_loaded, created_at) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (current_user.id, data.get("session_id", ""),
                 user_msg, reply_text, json.dumps(dates),
                 datetime.utcnow().isoformat()),
            )
            conn_log.commit()
            cur_log.close(); conn_log.close()
        except Exception:
            pass
        return jsonify({
            "reply":           reply_text,
            "context_summary": context_summary,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/admin/chatbot/save-flag", methods=["POST"])
@login_required
def chatbot_save_flag():
    """Save a bot-suggested action as a note on the given date's briefing."""
    data        = request.get_json(force=True)
    description = (data.get("description") or "").strip()
    date_str    = (data.get("date") or "").strip()
    if not description or not date_str:
        return jsonify({"error": "description and date required"}), 400

    note_line = f"[Bot flag — {current_user.name}: {description}]"
    now       = datetime.utcnow().isoformat()
    conn      = get_db()
    cur       = get_cursor(conn)
    cur.execute("SELECT note_text FROM briefing_notes WHERE note_date = %s", (date_str,))
    row = cur.fetchone()
    if row:
        new_text = (row["note_text"] or "").rstrip() + "\n" + note_line
        cur.execute(
            "UPDATE briefing_notes SET note_text=%s, updated_by=%s, updated_at=%s WHERE note_date=%s",
            (new_text, current_user.id, now, date_str),
        )
    else:
        cur.execute(
            "INSERT INTO briefing_notes (note_date, note_text, updated_by, updated_at) VALUES (%s,%s,%s,%s)",
            (date_str, note_line, current_user.id, now),
        )
    conn.commit()
    cur.close(); conn.close()

    # Log the action against the most recent interaction from this user
    try:
        conn_log = get_db()
        cur_log  = get_cursor(conn_log)
        cur_log.execute(
            "UPDATE bot_interactions SET action_taken=%s "
            "WHERE id = (SELECT id FROM bot_interactions WHERE user_id=%s ORDER BY id DESC LIMIT 1)",
            (f"Saved flag: {description}", current_user.id),
        )
        conn_log.commit()
        cur_log.close(); conn_log.close()
    except Exception:
        pass

    return jsonify({"success": True})


# ── Security overview page ────────────────────────────────────────

_SECURITY_PIN = os.environ.get("SECURITY_PAGE_PIN", "")

@admin_bp.route("/admin/security", methods=["GET", "POST"])
@login_required
@admin_required
def security_page():
    unlocked = request.session_key = None  # reset
    session_key = "security_unlocked"
    from flask import session
    if request.method == "POST":
        pin = (request.form.get("pin") or "").strip()
        if _SECURITY_PIN and pin != _SECURITY_PIN:
            return render_template("admin_security.html", locked=True, error=True)
        session[session_key] = True
        return redirect(url_for("admin.security_page"))
    locked = bool(_SECURITY_PIN) and not session.get(session_key)
    return render_template("admin_security.html", locked=locked, error=False)