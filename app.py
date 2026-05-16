"""
app.py — application factory. Registers blueprints and Flask-Login.
All route logic lives in routes/ and db.py.
"""

import os
from datetime import timedelta

from flask import Flask
from flask_login import LoginManager
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

load_dotenv()

from db import get_db, get_cursor, User, init_db

# ── Create app ────────────────────────────────────────────────────
app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "change-this-in-production")
app.permanent_session_lifetime = timedelta(hours=8)
app.config["TEMPLATES_AUTO_RELOAD"] = True

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
from routes.auth           import auth_bp
from routes.admin          import admin_bp
from routes.dispatch       import dispatch_bp
from routes.carpet         import carpet_bp
from routes.briefing       import briefing_bp
from routes.pri_check      import pri_bp
from routes.employee       import employee_bp
from routes.projects       import projects_bp
from routes.my_bot         import my_bot_bp
from routes.breezeway_sync import bw_sync_bp
from routes.spi            import spi_bp
from routes.ops_bot        import ops_bot_bp

app.register_blueprint(auth_bp)
app.register_blueprint(admin_bp)
app.register_blueprint(dispatch_bp)
app.register_blueprint(carpet_bp)
app.register_blueprint(briefing_bp)
app.register_blueprint(pri_bp)
app.register_blueprint(employee_bp)
app.register_blueprint(projects_bp)
app.register_blueprint(my_bot_bp)
app.register_blueprint(bw_sync_bp)
app.register_blueprint(spi_bp)
app.register_blueprint(ops_bot_bp)

# ── Init DB ───────────────────────────────────────────────────────
with app.app_context():
    init_db()

# ── Template context ──────────────────────────────────────────────
@app.context_processor
def inject_globals():
    from flask_login import current_user
    try:
        my_bot_ok = current_user.is_authenticated and current_user.is_admin
    except Exception:
        my_bot_ok = False
    return {"my_bot_allowed": my_bot_ok}

# ── Scheduled jobs ────────────────────────────────────────────────
def _scheduled_pri_check():
    with app.app_context():
        try:
            from routes.pri_check import refresh_pri_banner_alerts
            refresh_pri_banner_alerts(alert_days=3)
        except Exception:
            pass

def _scheduled_asana_poll():
    with app.app_context():
        try:
            from routes.my_bot import poll_asana_notifications
            poll_asana_notifications()
        except Exception:
            pass

scheduler = BackgroundScheduler(timezone="America/Los_Angeles")
scheduler.add_job(
    _scheduled_pri_check,
    CronTrigger(hour=7, minute=30, timezone="America/Los_Angeles"),
    id="pri_alert_check",
    replace_existing=True,
)
scheduler.add_job(
    _scheduled_asana_poll,
    "interval",
    minutes=30,
    id="asana_poll",
    replace_existing=True,
)
scheduler.start()

# ── Run ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)