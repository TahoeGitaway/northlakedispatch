"""
app.py — application factory. Registers blueprints and Flask-Login.
All route logic lives in routes/ and db.py.
"""

import os
from datetime import timedelta

from flask import Flask
from flask_login import LoginManager
from dotenv import load_dotenv

load_dotenv()

from db import get_db, get_cursor, User, init_db

# ── Create app ────────────────────────────────────────────────────
app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "change-this-in-production")
app.permanent_session_lifetime = timedelta(hours=12)

# ── Flask-Login ───────────────────────────────────────────────────
login_manager = LoginManager(app)
login_manager.login_view = "auth.login"
login_manager.login_message = "Please log in to access Tahoe Dispatch."
login_manager.login_message_category = "info"

@login_manager.user_loader
def load_user(user_id):
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        "SELECT id, email, name, role, is_active FROM users WHERE id = %s", (user_id,)
    )
    row = cur.fetchone()
    cur.close(); conn.close()
    if row:
        return User(row["id"], row["email"], row["name"], row["role"], row["is_active"])
    return None

# ── Register blueprints ───────────────────────────────────────────
from routes.auth     import auth_bp
from routes.admin    import admin_bp
from routes.dispatch import dispatch_bp
from routes.carpet   import carpet_bp

app.register_blueprint(auth_bp)
app.register_blueprint(admin_bp)
app.register_blueprint(dispatch_bp)
app.register_blueprint(carpet_bp)

# ── Init DB ───────────────────────────────────────────────────────
with app.app_context():
    init_db()

# ── Run ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)