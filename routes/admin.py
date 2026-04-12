"""
routes/admin.py — admin routes: user management, invites, properties, CSV upload.
"""

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
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

from db import (get_db, get_cursor,
                SENDGRID_API_KEY, FROM_EMAIL, APP_BASE_URL)
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
    email = (request.form.get("email") or "").strip().lower()

    if not email:
        flash("Email address is required.", "error")
        return redirect(url_for("admin.admin_users"))

    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("SELECT id FROM users WHERE email = %s", (email,))
    if cur.fetchone():
        flash(f"{email} already has an account.", "error")
        cur.close(); conn.close()
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

    email_ok, email_error = _send_invite_email(email, token)

    if email_ok:
        flash(f"Invite email sent to {email}. Link expires in 48 hours.", "success")
    else:
        flash(
            f"Could not send email to {email} ({email_error}). "
            f"Share this link manually (expires 48 hrs): {register_url}",
            "error"
        )

    return redirect(url_for("admin.admin_users"))


def _send_invite_email(to_email: str, token: str):
    """Returns (success: bool, error_message: str|None)."""
    if not SENDGRID_API_KEY:
        return False, "SENDGRID_API_KEY is not set in environment variables"

    register_url = f"{APP_BASE_URL}/register/{token}"
    message = Mail(
        from_email=FROM_EMAIL,
        to_emails=to_email,
        subject="You're invited to North Lake Dispatch",
        html_content=f"""
            <p>Hi,</p>
            <p>You've been invited to join <strong>North Lake Dispatch</strong> —
               Tahoe Getaways' internal routing tool.</p>
            <p>Click the link below to create your account.
               This link expires in <strong>48 hours</strong>.</p>
            <p><a href="{register_url}" style="
                display:inline-block; background:#4f46e5; color:#fff;
                padding:10px 20px; border-radius:8px;
                text-decoration:none; font-weight:600;">
                Create My Account →
            </a></p>
            <p style="color:#6b7280;font-size:0.85em;">
                Or copy this link: {register_url}
            </p>
            <p>— Tahoe Getaways Operations</p>
        """
    )
    try:
        sg       = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        if response.status_code >= 400:
            return False, f"SendGrid returned status {response.status_code}"
        return True, None
    except Exception as e:
        return False, str(e)


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
    if not address:
        return jsonify({"error": "Address is required. Enter a street address before geocoding."}), 400

    suffixes = [
        ", Lake Tahoe, CA", ", Tahoe City, CA", ", South Lake Tahoe, CA",
        ", Kings Beach, CA", ", Incline Village, NV", ", California", "",
    ]

    for suffix in suffixes:
        query = address + suffix
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

    return jsonify({"error": f"Couldn't place '{address}' within the Tahoe region (lat 38.5–40.0, lng -120.8 to -119.4). Try including the city and state, or verify the address is in the service area. (Nominatim returned no results after {len(suffixes)} attempts.)"}), 404


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


# ── DB download stub ──────────────────────────────────────────────

@admin_bp.route("/admin/download-db")
@login_required
@admin_required
def download_db():
    return jsonify({"info": "Database is PostgreSQL. Use Railway dashboard for backups."}), 200