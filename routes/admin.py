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
                   url_for, flash, current_app, jsonify, Response, stream_with_context)
from flask_login import login_required, current_user
from werkzeug.security import generate_password_hash

from db import get_db, get_cursor, APP_BASE_URL
from routes.auth import admin_required

admin_bp = Blueprint("admin", __name__)


def _safe_trim(messages, limit):
    """Trim history to `limit` messages without orphaning tool_result blocks.

    If a naive slice leaves a user message whose content is entirely tool_results
    at position 0, there is no preceding tool_use for the API to reference and it
    raises a 400. We walk forward past any such orphaned pairs.
    """
    if len(messages) <= limit:
        return list(messages)
    trimmed = list(messages[-limit:])
    while trimmed:
        first = trimmed[0]
        content = first.get("content", "")
        role = first.get("role", "")
        is_tool_result_msg = (
            role == "user"
            and isinstance(content, list)
            and all(isinstance(b, dict) and b.get("type") == "tool_result" for b in content)
        )
        if is_tool_result_msg or role == "assistant":
            trimmed.pop(0)
        else:
            break
    return trimmed


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


@admin_bp.route("/admin/knowledge/upload", methods=["POST"])
@login_required
@admin_required
def knowledge_upload():
    """Upload a .txt, .md, or .pdf file and create a knowledge entry from its content."""
    f = request.files.get("file")
    if not f or not f.filename:
        return jsonify({"error": "No file provided."}), 400

    filename  = f.filename
    ext       = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    raw_title = filename.rsplit(".", 1)[0].replace("_", " ").replace("-", " ").strip()
    category  = (request.form.get("category") or "Uploaded Documents").strip()

    if ext in ("txt", "md"):
        try:
            body = f.read().decode("utf-8", errors="replace").strip()
        except Exception as e:
            return jsonify({"error": f"Could not read file: {e}"}), 400
    elif ext == "pdf":
        try:
            import pypdf
            reader = pypdf.PdfReader(f)
            pages  = [page.extract_text() or "" for page in reader.pages]
            body   = "\n\n".join(p.strip() for p in pages if p.strip())
        except ImportError:
            return jsonify({"error": "PDF support requires pypdf — run: pip install pypdf"}), 500
        except Exception as e:
            return jsonify({"error": f"Could not read PDF: {e}"}), 400
    else:
        return jsonify({"error": f"Unsupported file type '.{ext}'. Upload .txt, .md, or .pdf files."}), 400

    if not body:
        return jsonify({"error": "The file appears to be empty or unreadable."}), 400

    now  = datetime.utcnow().isoformat()
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("""
        INSERT INTO chatbot_knowledge (title, category, body, is_active, created_by, updated_by, created_at, updated_at)
        VALUES (%s, %s, %s, 1, %s, %s, %s, %s)
    """, (raw_title, category, body, current_user.id, current_user.id, now, now))
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"success": True, "title": raw_title})


@admin_bp.route("/admin/pri-check")
@login_required
def pri_check_page():
    return render_template("admin_pri_check.html")


def _execute_fetch_tasks_multi_standalone(start_str, end_str, property_names, status_filter=None):
    """Module-level concurrent task fetcher — shared by both ops bot and my-bot."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from routes.briefing import (
        _fetch_bw_endpoint, _get_breezeway_token, _get_property_name,
        _get_live_property_cache, _get_live_ref_cache,
    )
    import difflib

    def _fetch_one(name):
        from datetime import date as _d, timedelta
        try:
            s = _d.fromisoformat(start_str); e = _d.fromisoformat(end_str)
        except ValueError:
            return "Error: invalid date format."
        if (e - s).days > 30:
            e = s + timedelta(days=30)
        tok = _get_breezeway_token()
        if not tok:
            return "Breezeway not configured."
        params = {"scheduled_date": f"{start_str},{e.isoformat()}"}
        cache  = _get_live_property_cache()
        nl     = name.lower().strip()
        rev    = {v.lower(): k for k, v in cache.items() if isinstance(v, str)}

        def _norm(s):
            return s.lower().replace(" ", "").replace("-", "").replace("'", "")

        nl_norm  = _norm(nl)
        norm_rev = {_norm(k): k for k in rev}

        if nl in rev:
            pid = rev[nl]
        else:
            qw = set(nl.split())
            matches = (
                [k for k in rev if k.startswith(nl)] or
                [k for k in rev if nl in k] or
                [k for k in rev if qw and qw.issubset(set(k.split()))] or
                difflib.get_close_matches(nl, rev.keys(), n=3, cutoff=0.4) or
                [norm_rev[nk] for nk in norm_rev if nk.startswith(nl_norm)] or
                [norm_rev[nk] for nk in norm_rev if nl_norm in nk] or
                [norm_rev[nk] for nk in norm_rev if len(nk) > 4 and nk in nl_norm]
            )
            pid = rev[matches[0]] if matches else None
        if not pid:
            return f"Property '{name}' not found."
        ref_id = _get_live_ref_cache().get(pid)
        for prop_key, prop_val in (
            [("reference_property_id", ref_id)] if ref_id else []
        ) + [("property_id", pid), ("home_id", pid)]:
            t, _, sc = _fetch_bw_endpoint(tok, "/public/inventory/v1/task/", {**params, prop_key: prop_val})
            if sc == 200:
                if not t:
                    return f"No tasks found at {name} between {start_str} and {e.isoformat()}."
                lines = [f"Tasks at {name}:"]
                for task in t:
                    title = (task.get("title") or task.get("name") or "Untitled")
                    if isinstance(title, dict):
                        title = title.get("value") or title.get("name") or "Untitled"
                    sdate = task.get("scheduled_date") or ""
                    stime = task.get("scheduled_time") or ""
                    assignees = task.get("assignments") or []
                    names_list = [
                        (a.get("name") or f"{a.get('first_name','')} {a.get('last_name','')}").strip()
                        for a in assignees if isinstance(a, dict)
                    ]
                    raw_status = (task.get("type_task_status") or task.get("status") or task.get("state") or "").lower().strip()
                    if raw_status in ("complete", "completed", "done", "finished"):
                        status_label = "✓ Complete"
                    elif raw_status in ("in_progress", "in progress", "started"):
                        status_label = "🔄 In Progress"
                    else:
                        status_label = "⏳ Pending"
                    lines.append(f"  • [{status_label}] {title} | {sdate} {stime} | assigned: {', '.join(names_list) or 'unassigned'}")
                return "\n".join(lines)
        return f"Could not retrieve tasks for '{name}'."

    if not property_names:
        return "No property names provided."
    if len(property_names) == 1:
        return _fetch_one(property_names[0])
    results = {}
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(_fetch_one, n): n for n in property_names}
        for future in as_completed(futures):
            results[futures[future]] = future.result()
    return "\n\n".join(f"=== {n} ===\n{results.get(n,'No data.')}" for n in property_names)

# ── PRI dismissals (shared across all users/browsers) ────────────────

@admin_bp.route("/admin/pri-dismissals", methods=["GET"])
@login_required
def pri_dismissals_get():
    conn = get_db(); cur = get_cursor(conn)
    cur.execute("SELECT item_key FROM pri_dismissals")
    keys = [r["item_key"] for r in cur.fetchall()]
    cur.close(); conn.rollback(); conn.close()
    return jsonify({"keys": keys})


@admin_bp.route("/admin/pri-dismissal", methods=["POST"])
@login_required
def pri_dismissal_add():
    key = (request.get_json(force=True) or {}).get("key", "").strip()
    if not key:
        return jsonify({"error": "key required"}), 400
    now  = datetime.utcnow().isoformat()
    conn = get_db(); cur = get_cursor(conn)
    cur.execute(
        "INSERT INTO pri_dismissals (item_key, dismissed_by, dismissed_at) "
        "VALUES (%s, %s, %s) ON CONFLICT (item_key) DO NOTHING",
        (key, current_user.id, now),
    )
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})


@admin_bp.route("/admin/pri-dismissals/clear", methods=["POST"])
@login_required
def pri_dismissals_clear():
    # type: "owner_next" clears only ::on keys; "vacancy" clears non-::on; omit to clear all
    kind = (request.get_json(force=True) or {}).get("type", "all")
    conn = get_db(); cur = get_cursor(conn)
    if kind == "owner_next":
        cur.execute("DELETE FROM pri_dismissals WHERE item_key LIKE %s", ("%::on",))
    elif kind == "vacancy":
        cur.execute("DELETE FROM pri_dismissals WHERE item_key NOT LIKE %s", ("%::on",))
    else:
        cur.execute("DELETE FROM pri_dismissals")
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})


# ── PRI Check page snooze (temporary, 1 week — separate from the banner ✕) ──

_PRI_SNOOZE_DAYS = 7

@admin_bp.route("/admin/pri-snoozes", methods=["GET"])
@login_required
def pri_snoozes_get():
    """Return keys snoozed and not yet expired; opportunistically purge expired rows."""
    now = datetime.utcnow().isoformat()
    conn = get_db(); cur = get_cursor(conn)
    cur.execute("DELETE FROM pri_snoozes WHERE snoozed_until <= %s", (now,))
    conn.commit()
    cur.execute("SELECT item_key FROM pri_snoozes")
    keys = [r["item_key"] for r in cur.fetchall()]
    cur.close(); conn.close()
    return jsonify({"keys": keys})


@admin_bp.route("/admin/pri-snooze", methods=["POST"])
@login_required
def pri_snooze_add():
    key = (request.get_json(force=True) or {}).get("key", "").strip()
    if not key:
        return jsonify({"error": "key required"}), 400
    until = (datetime.utcnow() + timedelta(days=_PRI_SNOOZE_DAYS)).isoformat()
    conn = get_db(); cur = get_cursor(conn)
    cur.execute(
        "INSERT INTO pri_snoozes (item_key, snoozed_until, snoozed_by) "
        "VALUES (%s, %s, %s) ON CONFLICT (item_key) DO UPDATE SET "
        "snoozed_until = EXCLUDED.snoozed_until, snoozed_by = EXCLUDED.snoozed_by",
        (key, until, current_user.id),
    )
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})


@admin_bp.route("/admin/pri-snoozes/clear", methods=["POST"])
@login_required
def pri_snoozes_clear():
    # type: "owner_next" clears only ::on keys; "vacancy" clears non-::on; omit to clear all
    kind = (request.get_json(force=True) or {}).get("type", "all")
    conn = get_db(); cur = get_cursor(conn)
    if kind == "owner_next":
        cur.execute("DELETE FROM pri_snoozes WHERE item_key LIKE %s", ("%::on",))
    elif kind == "vacancy":
        cur.execute("DELETE FROM pri_snoozes WHERE item_key NOT LIKE %s", ("%::on",))
    else:
        cur.execute("DELETE FROM pri_snoozes")
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})


# ── PRI alert manual refresh ─────────────────────────────────────

@admin_bp.route("/admin/pri-alert-refresh", methods=["POST"])
@login_required
def pri_alert_refresh():
    from routes.pri_check import refresh_pri_banner_alerts
    try:
        refresh_pri_banner_alerts(alert_days=3)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


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