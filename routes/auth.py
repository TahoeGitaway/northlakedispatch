"""
routes/auth.py — authentication: login, logout, password reset, invite registration.
"""

import secrets
from datetime import datetime, timedelta
from functools import wraps

from flask import (Blueprint, render_template, request, redirect,
                   url_for, flash, session, current_app)
from flask_login import login_user, logout_user, login_required, current_user
from werkzeug.security import check_password_hash, generate_password_hash

from db import get_db, get_cursor, User, APP_BASE_URL

auth_bp = Blueprint("auth", __name__)


# ── Admin decorator (used across blueprints via import) ───────────

def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.is_admin:
            flash("Admin access required.", "error")
            return redirect(url_for("dispatch.home"))
        return f(*args, **kwargs)
    return decorated


# ── Login / Logout ────────────────────────────────────────────────

@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("dispatch.home"))

    if request.method == "POST":
        email    = (request.form.get("email") or "").strip().lower()
        password = request.form.get("password") or ""
        remember = bool(request.form.get("remember"))

        conn = get_db()
        cur  = get_cursor(conn)
        cur.execute(
            "SELECT id, email, name, role, password_hash, is_active "
            "FROM users WHERE email = %s", (email,)
        )
        row = cur.fetchone()
        cur.close(); conn.close()

        if not row or not check_password_hash(row["password_hash"], password):
            flash("Invalid email or password.", "error")
            return render_template("login.html")

        if not row["is_active"]:
            flash("Your account has been deactivated. Contact operations@tahoegetaways.com.", "error")
            return render_template("login.html")

        user = User(row["id"], row["email"], row["name"], row["role"], row["is_active"])
        login_user(user, remember=remember)
        session.permanent = True
        return redirect(request.args.get("next") or url_for("dispatch.home"))

    return render_template("login.html")


@auth_bp.route("/logout")
@login_required
def logout():
    logout_user()
    flash("You've been logged out.", "info")
    return redirect(url_for("auth.login"))


# ── Password reset ────────────────────────────────────────────────

@auth_bp.route("/forgot-password", methods=["GET", "POST"])
def forgot_password():
    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        conn  = get_db()
        cur   = get_cursor(conn)
        cur.execute("SELECT id FROM users WHERE email = %s", (email,))
        row = cur.fetchone()

        if row:
            token   = secrets.token_urlsafe(32)
            expires = (datetime.utcnow() + timedelta(hours=1)).isoformat()
            cur.execute(
                "UPDATE users SET reset_token = %s, reset_token_expires = %s WHERE id = %s",
                (token, expires, row["id"])
            )
            conn.commit()
            cur.close(); conn.close()
            reset_url = f"{APP_BASE_URL}/reset-password/{token}"
            flash(
                f"Reset link (expires in 1 hour): {reset_url}  —  "
                f"Copy this link and open it in your browser, or ask your admin to send it to you.",
                "info"
            )
            return redirect(url_for("auth.forgot_password"))

        cur.close(); conn.close()
        flash("No account found with that email address.", "error")
        return redirect(url_for("auth.forgot_password"))

    return render_template("forgot_password.html")


@auth_bp.route("/reset-password/<token>", methods=["GET", "POST"])
def reset_password(token):
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        "SELECT id, reset_token_expires FROM users WHERE reset_token = %s", (token,)
    )
    row = cur.fetchone()

    if not row:
        cur.close(); conn.close()
        flash("Invalid or expired reset link.", "error")
        return redirect(url_for("auth.login"))

    if datetime.utcnow() > datetime.fromisoformat(row["reset_token_expires"]):
        cur.close(); conn.close()
        flash("This reset link has expired. Please request a new one.", "error")
        return redirect(url_for("auth.forgot_password"))

    if request.method == "POST":
        password = request.form.get("password") or ""
        confirm  = request.form.get("confirm") or ""

        if len(password) < 8:
            flash("Password must be at least 8 characters.", "error")
            cur.close(); conn.close()
            return render_template("reset_password.html", token=token)

        if password != confirm:
            flash("Passwords do not match.", "error")
            cur.close(); conn.close()
            return render_template("reset_password.html", token=token)

        cur.execute(
            "UPDATE users SET password_hash = %s, reset_token = NULL, "
            "reset_token_expires = NULL WHERE id = %s",
            (generate_password_hash(password), row["id"])
        )
        conn.commit()
        cur.close(); conn.close()
        flash("Password updated. You can now log in.", "success")
        return redirect(url_for("auth.login"))

    cur.close(); conn.close()
    return render_template("reset_password.html", token=token)


# ── Invite registration ───────────────────────────────────────────

@auth_bp.route("/register/<token>", methods=["GET", "POST"])
def register(token):
    conn   = get_db()
    cur    = get_cursor(conn)
    cur.execute(
        "SELECT id, email, expires_at, used FROM invites WHERE token = %s", (token,)
    )
    invite = cur.fetchone()

    if not invite:
        cur.close(); conn.close()
        flash("This invite link is invalid.", "error")
        return redirect(url_for("auth.login"))

    if invite["used"]:
        cur.close(); conn.close()
        flash("This invite link has already been used.", "error")
        return redirect(url_for("auth.login"))

    if datetime.utcnow() > datetime.fromisoformat(invite["expires_at"]):
        cur.close(); conn.close()
        flash("This invite link has expired. Ask your admin to send a new one.", "error")
        return redirect(url_for("auth.login"))

    invited_email = invite["email"]

    if request.method == "POST":
        name     = (request.form.get("name") or "").strip()
        password = request.form.get("password") or ""
        confirm  = request.form.get("confirm") or ""

        if not name:
            flash("Please enter your name.", "error")
            cur.close(); conn.close()
            return render_template("register.html", token=token, email=invited_email)

        if len(password) < 8:
            flash("Password must be at least 8 characters.", "error")
            cur.close(); conn.close()
            return render_template("register.html", token=token, email=invited_email)

        if password != confirm:
            flash("Passwords do not match.", "error")
            cur.close(); conn.close()
            return render_template("register.html", token=token, email=invited_email)

        cur.execute("SELECT id FROM users WHERE email = %s", (invited_email,))
        if cur.fetchone():
            cur.close(); conn.close()
            flash("An account with this email already exists. Try logging in.", "error")
            return redirect(url_for("auth.login"))

        now = datetime.utcnow().isoformat()
        cur.execute(
            "INSERT INTO users (email, name, role, password_hash, is_active, created_at) "
            "VALUES (%s,%s,%s,%s,1,%s)",
            (invited_email, name, "user", generate_password_hash(password), now)
        )
        cur.execute("UPDATE invites SET used = 1 WHERE token = %s", (token,))
        conn.commit()

        cur.execute(
            "SELECT id, email, name, role, is_active FROM users WHERE email = %s",
            (invited_email,)
        )
        row = cur.fetchone()
        cur.close(); conn.close()

        user = User(row["id"], row["email"], row["name"], row["role"], row["is_active"])
        login_user(user)
        session.permanent = True
        flash(f"Welcome to North Lake Dispatch, {name}! Your account is ready.", "success")
        return redirect(url_for("dispatch.home"))

    cur.close(); conn.close()
    return render_template("register.html", token=token, email=invited_email)