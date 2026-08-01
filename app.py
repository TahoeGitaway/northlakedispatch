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
from routes.walk_thru_rename import walk_thru_bp
from routes.bear_fence       import bear_fence_bp
from routes.bear_fence_delete import bear_fence_delete_bp
from routes.hot_tub          import hot_tub_bp
from routes.lease_prep         import lease_prep_bp
from routes.quick_complete     import quick_complete_bp
from routes.group_assign       import group_assign_bp
from routes.pri_rename         import pri_rename_bp
from routes.vip                import vip_bp   # TEMPORARY VIP arrivals tracker
from routes.productivity       import productivity_bp
from routes.hot_tub_billing     import hot_tub_billing_bp
from routes.occupancy_check      import occupancy_bp
from routes.assignee_monitor      import assignee_monitor_bp
from routes.bw_comments           import bw_comments_bp
from routes.bw_probe              import bw_probe_bp   # admin-only Breezeway capability probe

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
app.register_blueprint(walk_thru_bp)
app.register_blueprint(bear_fence_bp)
app.register_blueprint(bear_fence_delete_bp)
app.register_blueprint(hot_tub_bp)
app.register_blueprint(lease_prep_bp)
app.register_blueprint(quick_complete_bp)
app.register_blueprint(group_assign_bp)
app.register_blueprint(pri_rename_bp)
app.register_blueprint(vip_bp)
app.register_blueprint(productivity_bp)
app.register_blueprint(hot_tub_billing_bp)
app.register_blueprint(occupancy_bp)
app.register_blueprint(assignee_monitor_bp)
app.register_blueprint(bw_comments_bp)
app.register_blueprint(bw_probe_bp)

# ── Init DB ───────────────────────────────────────────────────────
with app.app_context():
    init_db()


# ── Unhandled errors on JSON endpoints ────────────────────────────
@app.errorhandler(Exception)
def _json_errors(e):
    """An unhandled exception on an API route returned Flask's HTML error page.
    The browser then failed to parse it as JSON, so the actual fault — the
    exception type, message and location — was lost, and the user could only
    report "500 Internal Server Error".

    Return the detail as JSON for API routes so it lands in the Copy-error blob.
    HTML pages keep Flask's normal error page.
    """
    from werkzeug.exceptions import HTTPException
    from flask import request as _rq, jsonify as _js
    import traceback

    # Real HTTP errors (404, 403, …) already carry a sensible status/body.
    if isinstance(e, HTTPException):
        return e

    app.logger.exception("Unhandled error on %s", _rq.path)

    wants_json = (_rq.path.startswith("/api/")
                  or "application/json" in (_rq.headers.get("Accept") or "")
                  or _rq.is_json)
    if not wants_json:
        raise e

    tb = traceback.extract_tb(e.__traceback__)
    where = ""
    if tb:
        last = tb[-1]
        where = f"{last.filename.split('/')[-1]}:{last.lineno} in {last.name}"
    return _js({
        "error": f"Server error: {type(e).__name__}: {e}",
        "diagnostics": {
            "stage": "unhandled_exception",
            "exception": type(e).__name__,
            "message": str(e)[:500],
            "where": where,
            "endpoint": _rq.path,
        },
    }), 500

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

def _log_day_summary_run(tag, res):
    app.logger.info("[day-summaries%s] saved=%d skipped=%d errors=%d pending=%d",
                    tag, len(res.get("saved", [])), len(res.get("skipped", [])),
                    len(res.get("errors", [])), len(res.get("pending", [])))
    for line in res.get("skipped", []) + res.get("errors", []):
        app.logger.warning("[day-summaries%s] %s", tag, line)


def _scheduled_day_summaries():
    """Store arrivals/departures for the coming week, once, early.

    The Saved Routes page reads a stored snapshot before it will call Breezeway,
    so filling that table is what keeps day-clicks off the API. Running early,
    at 5:30am means the fetch happens when nothing else competes for the limit."""
    with app.app_context():
        try:
            from routes.briefing import refresh_day_summaries
            _log_day_summary_run("", refresh_day_summaries(days=8))
        except Exception:
            app.logger.exception("[day-summaries] refresh failed")


def _scheduled_day_summaries_retry():
    """Keep after any date the morning run couldn't store.

    An unwritten date is the expensive case: with no snapshot, every day-click
    hits Breezeway live for the rest of the day. So retry through the morning
    rather than giving up. Once every date is covered this costs one SQL query
    and no API calls, so running it often is cheap."""
    with app.app_context():
        try:
            from routes.briefing import refresh_day_summaries, _day_summary_pending
            res = refresh_day_summaries(days=8, only_missing=True)
            if res.get("saved") or res.get("pending"):
                _log_day_summary_run(":retry", res)
        except Exception:
            app.logger.exception("[day-summaries:retry] failed")


scheduler = BackgroundScheduler(timezone="America/Los_Angeles")
scheduler.add_job(
    _scheduled_day_summaries,
    CronTrigger(hour=5, minute=30, timezone="America/Los_Angeles"),
    id="day_summaries_refresh",
    replace_existing=True,
)
# Catch-up for anything the 5:30 run missed: 5:45, 6:15, 6:45 — clustered right
# after the main run and finished by quarter to seven. Breezeway's limit resets in
# about a minute, so a date that failed at 5:30 is usually fine fifteen minutes
# later; waiting an hour to find that out just leaves the gap open longer. Two
# triggers rather than one so nothing fires BEFORE the 5:30 run. Each does nothing
# at all (one SQL query, zero API calls) when every date is already covered.
scheduler.add_job(
    _scheduled_day_summaries_retry,
    CronTrigger(hour=5, minute=45, timezone="America/Los_Angeles"),
    id="day_summaries_retry_1",
    replace_existing=True,
)
scheduler.add_job(
    _scheduled_day_summaries_retry,
    CronTrigger(hour=6, minute="15,45", timezone="America/Los_Angeles"),
    id="day_summaries_retry_2",
    replace_existing=True,
)
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