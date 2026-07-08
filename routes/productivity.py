r"""
routes/productivity.py — In-app viewer for the "Productivity Past 365 Days" report.

This does NOT run the (slow, hundreds-of-calls) Breezeway scan itself — that lives
in the standalone, read-only program `productivity_past_365_days.py`, which is the
single source of truth. This page simply PRESENTS the tables that program writes:

    productivity_past_365_days.json          ← the current 365-day cycle
    productivity_past_365_days_prior.json     ← an optional prior cycle (run with --prior)

Refresh / generate the data by running, from the project folder:
    .\.venv\Scripts\python.exe tools\productivity_past_365_days.py                 # current cycle
    .\.venv\Scripts\python.exe tools\productivity_past_365_days.py --end <YYYY-MM-DD last year> --prior

Admin-only (it's employee productivity data).

Endpoints:
  GET /admin/productivity                 — the page
  GET /admin/productivity/data            — {current, prior} report payloads
  GET /admin/productivity/download?which= — current|prior per-day CSV
"""

import os
import json

from flask import Blueprint, render_template, jsonify, send_file, request, abort
from flask_login import login_required

from routes.auth import admin_required

productivity_bp = Blueprint("productivity", __name__)

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Generated report artifacts live in <root>/reports (gitignored), not the repo root.
REPORTS_DIR = os.path.join(_ROOT, "reports")

_FILES = {
    "current": {
        "json": os.path.join(REPORTS_DIR, "productivity_past_365_days.json"),
        "csv":  os.path.join(REPORTS_DIR, "productivity_past_365_days.csv"),
    },
    "prior": {
        "json": os.path.join(REPORTS_DIR, "productivity_past_365_days_prior.json"),
        "csv":  os.path.join(REPORTS_DIR, "productivity_past_365_days_prior.csv"),
    },
}


def _load(which: str) -> dict:
    paths = _FILES[which]
    if not os.path.exists(paths["json"]):
        return {"exists": False, "reason": "No report has been generated yet."}
    try:
        with open(paths["json"], encoding="utf-8") as f:
            payload = json.load(f)
    except Exception as e:
        return {"exists": False, "reason": f"Report file exists but could not be read: {e}"}
    payload["exists"] = True
    payload["has_csv"] = os.path.exists(paths["csv"])
    return payload


@productivity_bp.route("/admin/productivity")
@login_required
@admin_required
def productivity_page():
    return render_template("productivity.html")


@productivity_bp.route("/admin/productivity/data")
@login_required
@admin_required
def productivity_data():
    return jsonify({"current": _load("current"), "prior": _load("prior")})


@productivity_bp.route("/admin/productivity/download")
@login_required
@admin_required
def productivity_download():
    which = request.args.get("which", "current")
    if which not in _FILES:
        abort(400, "which must be 'current' or 'prior'.")
    csv_path = _FILES[which]["csv"]
    if not os.path.exists(csv_path):
        abort(404, f"No {which} CSV has been generated yet. Run tools/productivity_past_365_days.py first.")
    return send_file(csv_path, as_attachment=True,
                     download_name=os.path.basename(csv_path), mimetype="text/csv")
