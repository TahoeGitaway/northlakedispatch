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


# ── AI Chatbot ────────────────────────────────────────────────────

@admin_bp.route("/admin/chatbot")
@login_required
@admin_required
def chatbot_page():
    return render_template("admin_chatbot.html")


@admin_bp.route("/admin/pri-check")
@login_required
@admin_required
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
        if nl in rev:
            pid = rev[nl]
        else:
            qw = set(nl.split())
            matches = (
                [k for k in rev if k.startswith(nl)] or
                [k for k in rev if nl in k] or
                [k for k in rev if qw and qw.issubset(set(k.split()))] or
                difflib.get_close_matches(nl, rev.keys(), n=3, cutoff=0.4)
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
                    lines.append(f"  • {title} | {sdate} {stime} | assigned: {', '.join(names_list) or 'unassigned'}")
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


@admin_bp.route("/admin/chatbot/chat", methods=["POST"])
@login_required
@admin_required
def chatbot_chat():
    import anthropic
    from routes.briefing import (
        _fetch_todays_routes, _fetch_bw_reservations,
        _fetch_bw_endpoint, _get_breezeway_token, _classify_reservation,
        _get_property_name, _get_property_address, _extract_str,
        _get_live_property_cache, _get_live_ref_cache,
    )

    data     = request.get_json(force=True)
    messages = data.get("messages", [])
    dates    = data.get("dates", [])
    images   = data.get("images", [])  # [{data, media_type}] attached to latest message

    if not messages:
        return jsonify({"error": "No message provided."}), 400

    # If images were attached, rewrite the last user message as a multimodal content list.
    # The history entry the frontend already built has image blocks, so we just ensure
    # the server-side copy also has them (frontend sends the full history including images).
    # Validate and sanitise: only allow known image MIME types, cap payload size.
    ALLOWED_IMAGE_TYPES = {"image/jpeg", "image/png", "image/gif", "image/webp"}
    if images:
        safe_images = [
            img for img in images
            if isinstance(img, dict)
            and img.get("media_type") in ALLOWED_IMAGE_TYPES
            and isinstance(img.get("data"), str)
            and len(img["data"]) < 20 * 1024 * 1024  # 20 MB base64 cap per image
        ]
        if safe_images and messages and messages[-1].get("role") == "user":
            last = messages[-1]
            existing_content = last.get("content", "")
            if isinstance(existing_content, str):
                content_blocks = [{"type": "text", "text": existing_content}] if existing_content else []
            else:
                content_blocks = list(existing_content)
            image_blocks = [
                {"type": "image", "source": {"type": "base64",
                                             "media_type": img["media_type"],
                                             "data": img["data"]}}
                for img in safe_images
            ]
            messages[-1] = {"role": "user", "content": image_blocks + content_blocks}

    today_str    = datetime.utcnow().strftime("%Y-%m-%d")
    user_name    = current_user.name
    user_id      = current_user.id
    session_id   = data.get("session_id", "")

    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        return jsonify({"error": "ANTHROPIC_API_KEY not configured."}), 500

    tools = [
        {
            "name": "fetch_reservation_data",
            "description": (
                "Fetch Breezeway reservation data (arrivals, departures, routes) for a date range. "
                "Use this whenever the user asks about dates not already in the loaded context — "
                "e.g. 'next week', 'this Friday', 'next month', 'June', 'this summer'. "
                "Resolve relative references using today's date before calling. "
                "Maximum range is 30 days per call."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "start_date": {"type": "string", "description": "Start date YYYY-MM-DD"},
                    "end_date":   {"type": "string", "description": "End date YYYY-MM-DD (inclusive, max 30 days after start)"},
                },
                "required": ["start_date", "end_date"],
            },
        },
        {
            "name": "fetch_task_data",
            "description": (
                "Fetch Breezeway task data for a SINGLE property. "
                "Use fetch_tasks_multi instead when asking about 2 or more properties at once — "
                "it fetches all of them in parallel and is much faster. "
                "Use this only when fetching exactly one property. "
                "Maximum date range is 30 days per call. "
                "Use status='housekeeping' for cleaning tasks, 'maintenance' for maintenance, 'inspection' for inspections."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "start_date":    {"type": "string", "description": "Start date YYYY-MM-DD"},
                    "end_date":      {"type": "string", "description": "End date YYYY-MM-DD (max 30 days after start)"},
                    "property_name": {"type": "string", "description": "Required: property name (partial match ok)."},
                    "status":        {"type": "string", "description": "Optional: 'housekeeping', 'maintenance', 'inspection', 'safety', 'complete', 'pending', or 'in_progress'."},
                },
                "required": ["start_date", "end_date", "property_name"],
            },
        },
        {
            "name": "fetch_tasks_multi",
            "description": (
                "Fetch Breezeway task data for MULTIPLE properties simultaneously (in parallel). "
                "ALWAYS use this instead of multiple fetch_task_data calls when the user asks about "
                "2 or more properties. It runs all fetches concurrently so results come back in seconds "
                "regardless of how many properties are requested. "
                "Maximum date range is 30 days per call."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "start_date":      {"type": "string", "description": "Start date YYYY-MM-DD"},
                    "end_date":        {"type": "string", "description": "End date YYYY-MM-DD (max 30 days after start)"},
                    "property_names":  {"type": "array", "items": {"type": "string"},
                                        "description": "List of property names to fetch tasks for simultaneously."},
                    "status":          {"type": "string", "description": "Optional: 'housekeeping', 'maintenance', 'inspection', 'safety', 'complete', 'pending', or 'in_progress'."},
                },
                "required": ["start_date", "end_date", "property_names"],
            },
        },
    ]

    def _execute_fetch(start_str, end_str):
        from datetime import date as _date2
        from collections import defaultdict
        try:
            s = _date2.fromisoformat(start_str)
            e = _date2.fromisoformat(end_str)
        except ValueError:
            return "Error: invalid date format. Use YYYY-MM-DD."
        if (e - s).days > 30:
            e = s + timedelta(days=30)
            end_str = e.isoformat()
        if e < s:
            return "Error: end_date must be on or after start_date."

        tok = _get_breezeway_token()
        if not tok:
            return "Breezeway not configured — cannot fetch reservation data."
        try:
            cis  = _fetch_bw_reservations(tok, {"checkin_date_ge":  start_str, "checkin_date_le":  end_str})
            cos  = _fetch_bw_reservations(tok, {"checkout_date_ge": start_str, "checkout_date_le": end_str})
        except Exception as ex:
            return f"Error fetching data: {ex}"

        ci_by  = defaultdict(list)
        co_by  = defaultdict(list)
        for r in cis:
            d = (r.get("checkin_date")  or "")[:10]
            if start_str <= d <= end_str:
                ci_by[d].append(r)
        for r in cos:
            d = (r.get("checkout_date") or "")[:10]
            if start_str <= d <= end_str:
                co_by[d].append(r)

        all_days = sorted(set(list(ci_by.keys()) + list(co_by.keys())))
        lines = [f"Data for {start_str} through {end_str}:"]
        if not all_days:
            lines.append("No arrivals or departures found in this period.")
        for d in all_days:
            lines.append(f"\n--- {d} ---")
            try:
                rts = _fetch_todays_routes(d)
                for r in rts:
                    stops = [x for x in json.loads(r["stops_json"] or "[]") if not x.get("isLunch")]
                    ln = f"  Route: \"{r['name']}\""
                    if r["assigned_to"]: ln += f" → {r['assigned_to']}"
                    ln += f" ({len(stops)} stops)"
                    lines.append(ln)
            except Exception:
                pass
            for r in ci_by.get(d, []):
                kind = _classify_reservation(r)
                prop = _get_property_name(r.get("property_id"))
                co_d = (r.get("checkout_date") or "")[:10]
                ci_d = (r.get("checkin_date")  or "")[:10]
                nights = ""
                if ci_d and co_d:
                    try:
                        n = (_date2.fromisoformat(co_d) - _date2.fromisoformat(ci_d)).days
                        nights = f", {n} nights"
                    except Exception:
                        pass
                lines.append(f"  ARRIVAL  [{kind.upper()}] {prop} (out {co_d}{nights})")
            for r in co_by.get(d, []):
                kind = _classify_reservation(r)
                prop = _get_property_name(r.get("property_id"))
                ci_d = (r.get("checkin_date")  or "")[:10]
                co_d = (r.get("checkout_date") or "")[:10]
                nights = ""
                if ci_d and co_d:
                    try:
                        n = (_date2.fromisoformat(co_d) - _date2.fromisoformat(ci_d)).days
                        nights = f", {n} nights"
                    except Exception:
                        pass
                lines.append(f"  DEPARTURE [{kind.upper()}] {prop} (in since {ci_d}{nights})")
        return "\n".join(lines)

    def _execute_fetch_tasks(start_str, end_str, property_name_filter=None, status_filter=None):
        from datetime import date as _date2
        import difflib
        try:
            s = _date2.fromisoformat(start_str)
            e = _date2.fromisoformat(end_str)
        except ValueError:
            return "Error: invalid date format. Use YYYY-MM-DD."
        if (e - s).days > 30:
            e = s + timedelta(days=30)
            end_str = e.isoformat()

        tok = _get_breezeway_token()
        if not tok:
            return "Breezeway not configured."

        # Breezeway task API: GET /public/inventory/v1/task/
        # Date: scheduled_date=YYYY-MM-DD,YYYY-MM-DD  (comma-separated single param)
        # Property: home_id (Breezeway integer ID)
        # Category: type_department (housekeeping/maintenance/inspection/safety)
        params = {"scheduled_date": f"{start_str},{end_str}"}

        if not property_name_filter:
            return ("A property name is required to fetch task data — "
                    "the Breezeway task API does not support global queries. "
                    "Please ask the user which property they want to check.")

        _property_cache = _get_live_property_cache()
        name_lower = property_name_filter.lower().strip()
        rev = {v.lower(): k for k, v in _property_cache.items() if isinstance(v, str)}
        if name_lower in rev:
            pid = rev[name_lower]
            matched_prop_name = property_name_filter
        else:
            # Multi-strategy matching — tried in order of confidence:
            # 1. Query is a prefix of the full name ("Kodiak Cabin" → "Kodiak Cabin at Tahoe Donner")
            prefix_m = [k for k in rev if k.startswith(name_lower)]
            # 2. Query appears as a substring of the full name
            substr_m = [k for k in rev if name_lower in k]
            # 3. All words in the query appear in the full name (handles reordering or extra words)
            query_words = set(name_lower.split())
            word_m = [k for k in rev if query_words and query_words.issubset(set(k.split()))]
            # 4. Fuzzy matching (catches typos and minor variations)
            fuzzy_m = (difflib.get_close_matches(name_lower, rev.keys(), n=3, cutoff=0.6) or
                       difflib.get_close_matches(name_lower, rev.keys(), n=3, cutoff=0.4))
            # 5. Full name is a substring of the query (user typed extra words)
            reverse_m = [k for k in rev if len(k) > 4 and k in name_lower]

            matches = prefix_m or substr_m or word_m or fuzzy_m or reverse_m
            # If multiple candidates, prefer the shortest (most specific match)
            if len(matches) > 1:
                matches = sorted(matches, key=len)
            pid = rev[matches[0]] if matches else None
            matched_prop_name = _property_cache.get(pid, property_name_filter) if pid else None

        if not pid:
            cache_size = len(_property_cache)
            if cache_size == 0:
                return (f"Property cache is empty — Breezeway property list could not be loaded. "
                        f"This may be a token or API connectivity issue. Cannot look up tasks for "
                        f"'{property_name_filter}'.")
            # Surface near candidates so the bot can retry with the exact Breezeway name
            candidates = difflib.get_close_matches(name_lower, rev.keys(), n=5, cutoff=0.3)
            candidate_str = (", ".join(f'"{_property_cache[rev[c]]}"' for c in candidates)
                             if candidates else "none found")
            # Also include a sample of all cached names so the exact name is visible
            all_names_sample = sorted(_property_cache.values())[:30]
            return (f"Could not find a property matching '{property_name_filter}' "
                    f"({cache_size} properties in cache). "
                    f"Closest matches: {candidate_str}. "
                    f"All cached property names: {all_names_sample}. "
                    f"Retry using the exact name from that list.")

        matched_prop_name = matched_prop_name or property_name_filter

        # type_department filter for category-based queries
        dept_map = {"housekeeping": "housekeeping", "cleaning": "housekeeping",
                    "maintenance": "maintenance", "inspection": "inspection", "safety": "safety"}
        dept_filter = dept_map.get((status_filter or "").lower())
        if dept_filter:
            params["type_department"] = dept_filter

        # Try multiple property param name conventions — stop on first 200 response.
        # 'property_id' was the working name pre-deployment; also try 'home_id' and
        # 'reference_property_id' (external string ID from cache) as fallbacks.
        ref_id = _get_live_ref_cache().get(pid)
        prop_params_to_try = []
        if ref_id:
            prop_params_to_try.append(("reference_property_id", ref_id))
        prop_params_to_try.extend([
            ("property_id", pid),
            ("home_id", pid),
        ])

        tasks, error = [], "property not found"
        for prop_key, prop_val in prop_params_to_try:
            attempt_params = {**params, prop_key: prop_val}
            t, e, status_code = _fetch_bw_endpoint(tok, "/public/inventory/v1/task/", attempt_params)
            if status_code == 200:
                tasks, error = t, ""
                break
            if "403" in e or "access" in e.lower():
                return ("Task data requires elevated API access on your Breezeway plan. "
                        "Contact Breezeway support to request task API access.")
            error = e

        if error and not tasks:
            return f"Could not fetch tasks: {error}"

        def _task_status(t):
            """Extract task status as a plain lowercase string — fields may be dicts."""
            for key in ("type_task_status", "status", "state"):
                v = t.get(key)
                if v is None:
                    continue
                if isinstance(v, str):
                    return v.lower()
                if isinstance(v, dict):
                    s = v.get("value") or v.get("name") or v.get("label") or ""
                    if s:
                        return str(s).lower()
            return "unknown"

        # Client-side status filter (pending/complete/in_progress) if not a dept keyword
        if status_filter and not dept_filter:
            status_lower = status_filter.lower()
            tasks = [t for t in tasks if _task_status(t) == status_lower]

        if not tasks:
            prop_label   = f" at {matched_prop_name or property_name_filter}" if property_name_filter else ""
            status_label = f" ({status_filter})" if status_filter else ""
            return f"No tasks found{prop_label}{status_label} between {start_str} and {end_str}."

        lines = [f"Tasks for {start_str} through {end_str}"]
        if matched_prop_name or property_name_filter:
            lines[0] += f" — {matched_prop_name or property_name_filter}"
        lines.append(f"({len(tasks)} task{'s' if len(tasks) != 1 else ''} found)\n")

        by_status = {}
        for t in tasks:
            st = _task_status(t)
            by_status.setdefault(st, []).append(t)

        status_order = ["complete", "in_progress", "pending", "blocked", "cancelled", "unknown"]
        for st in status_order + [k for k in by_status if k not in status_order]:
            group = by_status.get(st)
            if not group:
                continue
            lines.append(f"── {st.upper()} ({len(group)}) ──")
            for t in group:
                def _sf(v):  # safe string extraction from potentially nested field
                    if isinstance(v, str): return v
                    if isinstance(v, dict): return v.get("value") or v.get("name") or v.get("label") or ""
                    return ""
                title = (_sf(t.get("title")) or _sf(t.get("name")) or
                         _sf(t.get("type_department")) or "Untitled")
                dept  = _sf(t.get("type_department"))
                home_id  = t.get("home_id") or t.get("property_id")
                prop_name = _get_property_name(home_id) if home_id else (t.get("property_name") or "")
                # Assignee: use 'assignments' list — each entry is a dict with 'name' and 'status'
                raw_assignments = t.get("assignments") or []
                if isinstance(raw_assignments, list) and raw_assignments:
                    names = []
                    for a in raw_assignments:
                        if isinstance(a, dict):
                            n = (a.get("name") or a.get("full_name") or
                                 (a.get("first_name", "") + " " + a.get("last_name", "")).strip())
                            if n:
                                names.append(n)
                        elif a:
                            names.append(str(a))
                    assignee = ", ".join(names)
                else:
                    assignee = ""

                def _fmt_dt(raw):
                    """Return date + time string from an ISO datetime or date-only string."""
                    if not raw:
                        return ""
                    s = str(raw)
                    date_part = s[:10]
                    time_part = ""
                    if len(s) > 10:
                        t_raw = s[11:16]  # HH:MM
                        if t_raw:
                            try:
                                h, m = int(t_raw[:2]), int(t_raw[3:5])
                                suffix = "AM" if h < 12 else "PM"
                                h12 = h % 12 or 12
                                time_part = f" {h12}:{m:02d} {suffix}"
                            except Exception:
                                time_part = f" {t_raw}"
                    return date_part + time_part

                # Combine scheduled_date + scheduled_time (separate fields in Breezeway API)
                sched_date = t.get("scheduled_date") or ""
                sched_time = t.get("scheduled_time") or ""
                if sched_date and sched_time:
                    sched = _fmt_dt(f"{sched_date}T{sched_time}")
                else:
                    sched = _fmt_dt(sched_date or t.get("start_time") or t.get("scheduled_start") or "")
                finished = _fmt_dt(t.get("finished_at") or t.get("completed_at") or "")
                notes    = (t.get("notes") or t.get("description") or "")[:120]

                line = f"  • {title}"
                if dept:       line += f" [{dept}]"
                if prop_name:  line += f" — {prop_name}"
                if sched:      line += f" | scheduled {sched}"
                if finished:   line += f" | done {finished}"
                if assignee:   line += f" | assigned: {assignee}"
                if notes:      line += f"\n    {notes}"
                lines.append(line)
            lines.append("")

        return "\n".join(lines)

    def _execute_fetch_tasks_multi(start_str, end_str, property_names, status_filter=None):
        """Fetch tasks for multiple properties concurrently using a thread pool."""
        from concurrent.futures import ThreadPoolExecutor, as_completed
        if not property_names:
            return "No property names provided."
        results = {}
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {
                executor.submit(_execute_fetch_tasks, start_str, end_str, name, status_filter): name
                for name in property_names
            }
            for future in as_completed(futures):
                name = futures[future]
                try:
                    results[name] = future.result()
                except Exception as ex:
                    results[name] = f"Error fetching tasks: {ex}"
        sections = []
        for name in property_names:
            sections.append(f"=== {name} ===\n{results.get(name, 'No data returned.')}")
        return "\n\n".join(sections)

    def generate():
        def sse(obj):
            return f"data: {json.dumps(obj)}\n\n"

        # ── Load all context data here so errors surface as SSE events ──
        yield sse({"type": "status", "text": "Loading schedule data…"})

        capped_dates = sorted(dates if dates else [today_str])[:7]
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
                        stops = [s for s in json.loads(r["stops_json"] or "[]") if not s.get("isLunch")]
                        line  = f"  - \"{r['name']}\""
                        if r["assigned_to"]: line += f" → {r['assigned_to']}"
                        line += f": {len(stops)} stop{'s' if len(stops) != 1 else ''}"
                        if (r.get("notes") or "").strip(): line += f". Notes: {r['notes'].strip()}"
                        block.append(line)
                else:
                    block.append("No routes saved for this date.")
                if checkins:
                    block.append(f"Arrivals ({len(checkins)}):")
                    for r in checkins:
                        kind      = _classify_reservation(r)
                        pid       = r.get("property_id")
                        prop      = _get_property_name(pid)
                        addr      = _get_property_address(pid)
                        t         = r.get("checkin_time", "")
                        checkout  = (r.get("checkout_date") or "")[:10]
                        checkin   = (r.get("checkin_date")  or "")[:10]
                        tag_names = [_extract_str(tg) for tg in (r.get("tags") or [])]
                        nights    = ""
                        if checkin and checkout:
                            try:
                                from datetime import date as _date
                                n = (_date.fromisoformat(checkout) - _date.fromisoformat(checkin)).days
                                nights = f", {n} nights"
                            except Exception:
                                pass
                        line = f"  - [{kind.upper()}] {prop}" + (f" — {addr}" if addr else "")
                        line += f" (checkin {checkin}, checkout {checkout}{nights})"
                        if t:         line += f" at {t[:5]}"
                        if tag_names: line += f" [tags: {', '.join(tag_names)}]"
                        block.append(line)
                else:
                    block.append("No arrivals this date.")
                if checkouts:
                    block.append(f"Departures ({len(checkouts)}):")
                    for r in checkouts:
                        kind      = _classify_reservation(r)
                        pid       = r.get("property_id")
                        prop      = _get_property_name(pid)
                        addr      = _get_property_address(pid)
                        t         = r.get("checkout_time", "")
                        checkout  = (r.get("checkout_date") or "")[:10]
                        checkin   = (r.get("checkin_date")  or "")[:10]
                        tag_names = [_extract_str(tg) for tg in (r.get("tags") or [])]
                        nights    = ""
                        if checkin and checkout:
                            try:
                                from datetime import date as _date
                                n = (_date.fromisoformat(checkout) - _date.fromisoformat(checkin)).days
                                nights = f", {n} nights"
                            except Exception:
                                pass
                        line = f"  - [{kind.upper()}] {prop}" + (f" — {addr}" if addr else "")
                        line += f" (checkin {checkin}, checkout {checkout}{nights})"
                        if t:         line += f" by {t[:5]}"
                        if tag_names: line += f" [tags: {', '.join(tag_names)}]"
                        block.append(line)
                else:
                    block.append("No departures this date.")
                context_blocks.append("\n".join(block))
                context_summary.append({"date": date_str, "routes": len(routes),
                                        "arrivals": len(checkins), "departures": len(checkouts)})
            except Exception as e:
                context_blocks.append(f"\n=== {date_str} ===\nData load error: {e}")
                context_summary.append({"date": date_str, "error": str(e)})

        # Knowledge base
        try:
            conn = get_db(); cur = get_cursor(conn)
            cur.execute("SELECT title, category, body FROM chatbot_knowledge WHERE is_active = 1 ORDER BY category, title")
            knowledge_rows = cur.fetchall()
            cur.close(); conn.rollback(); conn.close()
        except Exception:
            knowledge_rows = []

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
            f"You are the TG Operations Bot for Tahoe Getaways, a vacation rental company in Lake Tahoe. "
            f"You are talking to {user_name}. Today's date is {today_str}.\n\n"
            "HOW TO ANSWER:\n"
            "- For questions about specific properties, reservations, schedules, or company SOPs: "
            "use the knowledge base and loaded Breezeway data below as your primary source.\n"
            "- For general property management, hospitality, or operations questions: use your own knowledge "
            "to give a helpful, practical answer — you don't need to restrict yourself to the provided context.\n"
            "- If asked about a specific property or reservation that isn't in the loaded data, say so clearly "
            "and suggest the staff member check Breezeway or Streamline directly.\n"
            "- Be concise and direct. Use bullet points for multi-part answers.\n"
            "- When staff asks you to take a write action (save a note, flag a property, mark something complete), "
            "respond with a line starting exactly with 'CONFIRM_ACTION:' followed by a short description. "
            "Do not consider the action done until confirmed.\n"
            "- SCOPE BEFORE FETCHING: Before calling any fetch tool for a date range longer than 7 days, "
            "confirm the exact range with the user unless they stated it explicitly "
            "(e.g. 'next month' → ask 'Do you mean all of June, or a specific week?'). "
            "For ranges ≤7 days or when the user named specific dates, fetch immediately without asking.\n"
            "- PROPERTY SCOPE: If the user asks about tasks without naming a property, ask which property "
            "before fetching — do not fetch all properties speculatively.\n"
            "- TOOL USAGE: When the user asks about tasks at 2+ properties, ALWAYS use fetch_tasks_multi "
            "(not multiple fetch_task_data calls) — it fetches all properties in parallel in one shot. "
            "Never say 'I'll pull these simultaneously' and then use individual fetch_task_data calls.\n"
            "- TOOL ACCURACY: fetch_task_data and fetch_tasks_multi return ALL tasks from Breezeway for the given property "
            "and date range — they do NOT filter by task title or keyword. If tasks are missing, the cause is "
            "an API error (e.g. property ID not found) — say exactly what the error was. "
            "Tasks may have prefixes like 'Dept' or date stamps — report them exactly as returned. "
            "Always report full task title, scheduled date/time, status, and assignee for every task. "
            "If a field is blank in the data, say 'not listed' rather than claiming the API can't provide it. "
            "Do NOT tell the user to 'flag it to whoever manages the integration' — they are the ones managing it. "
            "If data is missing, describe exactly what was and wasn't returned so they can act on it.\n\n"
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
            + "\n\nYou also have a tool — fetch_reservation_data — to look up Breezeway data "
            "for any other date range the user asks about (next week, this Friday, June, etc.)."
        )

        def _trunc_for_history(content, limit=800):
            if not isinstance(content, str) or len(content) <= limit:
                return content
            cut = content[:limit].rfind('\n')
            if cut < limit // 2:
                cut = limit
            return content[:cut] + "\n[…truncated — bot will re-fetch if needed]"

        # ── Claude streaming ──
        ai_client         = anthropic.Anthropic(api_key=key)
        trimmed           = _safe_trim(messages, 12)
        history_additions = []
        reply_text        = ""

        try:
            for _turn in range(6):
                turn_text    = ""
                asst_content = []

                with ai_client.messages.stream(
                    model      = "claude-haiku-4-5-20251001",
                    max_tokens = 1500,
                    system     = system_prompt,
                    messages   = trimmed,
                    tools      = tools,
                ) as stream:
                    for chunk in stream.text_stream:
                        turn_text  += chunk
                        reply_text += chunk
                        yield sse({"type": "delta", "text": chunk})

                    final_msg = stream.get_final_message()

                if final_msg.stop_reason == "tool_use":
                    for b in final_msg.content:
                        if b.type == "tool_use":
                            asst_content.append({"type": "tool_use", "id": b.id, "name": b.name, "input": b.input})
                        elif b.type == "text":
                            asst_content.append({"type": "text", "text": b.text})
                        else:
                            asst_content.append({"type": b.type})
                    trimmed.append({"role": "assistant", "content": asst_content})
                    history_additions.append({"role": "assistant", "content": asst_content})

                    tool_results         = []
                    tool_results_history = []
                    tool_blocks          = [b for b in final_msg.content if b.type == "tool_use"]
                    tool_total           = len(tool_blocks)
                    tool_idx             = 0
                    for block in final_msg.content:
                        if block.type == "tool_use":
                            tool_idx += 1
                            counter  = f" ({tool_idx}/{tool_total})" if tool_total > 1 else ""
                            if block.name == "fetch_reservation_data":
                                yield sse({"type": "status", "text": f"Fetching reservation data{counter}…"})
                                result = _execute_fetch(
                                    block.input.get("start_date", ""),
                                    block.input.get("end_date",   ""),
                                )
                            elif block.name == "fetch_task_data":
                                prop = block.input.get("property_name") or ""
                                prop_label = f" for {prop}" if prop else ""
                                yield sse({"type": "status", "text": f"Fetching tasks{prop_label}{counter}…"})
                                result = _execute_fetch_tasks(
                                    block.input.get("start_date", ""),
                                    block.input.get("end_date",   ""),
                                    prop or None,
                                    block.input.get("status"),
                                )
                            elif block.name == "fetch_tasks_multi":
                                names = block.input.get("property_names") or []
                                n = len(names)
                                yield sse({"type": "status", "text": f"Fetching tasks for {n} properties simultaneously…"})
                                result = _execute_fetch_tasks_multi(
                                    block.input.get("start_date", ""),
                                    block.input.get("end_date",   ""),
                                    names,
                                    block.input.get("status"),
                                )
                            else:
                                result = f"Unknown tool: {block.name}"
                            tool_results.append({
                                "type":        "tool_result",
                                "tool_use_id": block.id,
                                "content":     result,
                            })
                            tool_results_history.append({
                                "type":        "tool_result",
                                "tool_use_id": block.id,
                                "content":     _trunc_for_history(result),
                            })
                    trimmed.append({"role": "user", "content": tool_results})
                    history_additions.append({"role": "user", "content": tool_results_history})
                else:
                    break

            # Log (best-effort)
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

            yield sse({
                "type":              "done",
                "history_additions": history_additions,
                "context_summary":   context_summary,
                "kb_count":          len(knowledge_rows),
            })
        except Exception as e:
            yield sse({"type": "error", "text": str(e)})

    return Response(
        stream_with_context(generate()),
        mimetype = "text/event-stream",
        headers  = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@admin_bp.route("/admin/chatbot/session/save", methods=["POST"])
@login_required
@admin_required
def chatbot_session_save():
    data       = request.get_json(force=True)
    session_id = (data.get("session_id") or "").strip()
    messages   = data.get("messages", [])
    if not session_id or not messages:
        return jsonify({"error": "session_id and messages required"}), 400

    # Strip base64 image data before saving — keep structure but replace data with placeholder
    def _strip_images(msgs):
        out = []
        for m in msgs:
            content = m.get("content")
            if isinstance(content, list):
                stripped = []
                for block in content:
                    if isinstance(block, dict) and block.get("type") == "image":
                        stripped.append({"type": "image", "source": {"type": "placeholder"}})
                    else:
                        stripped.append(block)
                out.append({**m, "content": stripped})
            else:
                out.append(m)
        return out

    safe_messages = _strip_images(messages)
    # Title = first user text message, truncated
    title = ""
    for m in safe_messages:
        if m.get("role") == "user":
            c = m.get("content")
            if isinstance(c, str):
                title = c[:80]
            elif isinstance(c, list):
                for b in c:
                    if isinstance(b, dict) and b.get("type") == "text":
                        title = (b.get("text") or "")[:80]
                        break
            if title:
                break

    now = datetime.utcnow().isoformat()
    conn = get_db(); cur = get_cursor(conn)
    try:
        cur.execute("""
            INSERT INTO chatbot_sessions (user_id, session_id, title, messages_json, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (session_id) DO UPDATE
              SET messages_json = EXCLUDED.messages_json,
                  title         = EXCLUDED.title,
                  updated_at    = EXCLUDED.updated_at
        """, (current_user.id, session_id, title, json.dumps(safe_messages), now, now))
        conn.commit()
    finally:
        conn.rollback(); cur.close(); conn.close()
    return jsonify({"success": True})


@admin_bp.route("/admin/chatbot/sessions", methods=["GET"])
@login_required
@admin_required
def chatbot_sessions_list():
    conn = get_db(); cur = get_cursor(conn)
    try:
        cur.execute("""
            SELECT session_id, title, updated_at
            FROM chatbot_sessions
            WHERE user_id = %s
            ORDER BY updated_at DESC
            LIMIT 30
        """, (current_user.id,))
        rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.rollback(); cur.close(); conn.close()
    return jsonify({"sessions": rows})


@admin_bp.route("/admin/chatbot/session/<session_id>", methods=["GET"])
@login_required
@admin_required
def chatbot_session_load(session_id):
    conn = get_db(); cur = get_cursor(conn)
    try:
        cur.execute("""
            SELECT messages_json FROM chatbot_sessions
            WHERE session_id = %s AND user_id = %s
        """, (session_id, current_user.id))
        row = cur.fetchone()
    finally:
        conn.rollback(); cur.close(); conn.close()
    if not row:
        return jsonify({"error": "Not found"}), 404
    try:
        messages = json.loads(row["messages_json"])
    except Exception:
        messages = []
    return jsonify({"messages": messages})


@admin_bp.route("/admin/chatbot/session/<session_id>", methods=["DELETE"])
@login_required
@admin_required
def chatbot_session_delete(session_id):
    conn = get_db(); cur = get_cursor(conn)
    try:
        cur.execute("DELETE FROM chatbot_sessions WHERE session_id = %s AND user_id = %s",
                    (session_id, current_user.id))
        conn.commit()
    finally:
        conn.rollback(); cur.close(); conn.close()
    return jsonify({"success": True})


@admin_bp.route("/admin/chatbot/save-flag", methods=["POST"])
@login_required
@admin_required
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


# ── PRI dismissals (shared across all users/browsers) ────────────────

@admin_bp.route("/admin/pri-dismissals", methods=["GET"])
@login_required
@admin_required
def pri_dismissals_get():
    conn = get_db(); cur = get_cursor(conn)
    cur.execute("SELECT item_key FROM pri_dismissals")
    keys = [r["item_key"] for r in cur.fetchall()]
    cur.close(); conn.rollback(); conn.close()
    return jsonify({"keys": keys})


@admin_bp.route("/admin/pri-dismissal", methods=["POST"])
@login_required
@admin_required
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
@admin_required
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


# ── PRI alert manual refresh ─────────────────────────────────────

@admin_bp.route("/admin/pri-alert-refresh", methods=["POST"])
@login_required
@admin_required
def pri_alert_refresh():
    from routes.briefing import refresh_pri_banner_alerts
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