"""
routes/productivity.py — In-app viewer for the "Productivity Past 365 Days" report.

This does NOT run the (slow, hundreds-of-calls) Breezeway scan itself — that lives
in the standalone, read-only program `productivity_past_365_days.py`, which is the
single source of truth. This page simply PRESENTS the tables that program writes to
`productivity_past_365_days.json` in the project root. Refresh the data by running:

    python productivity_past_365_days.py

Admin-only (it's employee productivity data).

Endpoints:
  GET /admin/productivity            — the page
  GET /admin/productivity/data       — the latest report JSON (or {exists:false})
  GET /admin/productivity/download   — the per-day CSV, if present
"""

import os
import json

from flask import Blueprint, render_template, jsonify, send_file, abort
from flask_login import login_required

from routes.auth import admin_required

productivity_bp = Blueprint("productivity", __name__)

# Project root = one level up from routes/. The CLI writes its report files here
# when run from the project directory (its default --outdir is ".").
_ROOT     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JSON_PATH = os.path.join(_ROOT, "productivity_past_365_days.json")
CSV_PATH  = os.path.join(_ROOT, "productivity_past_365_days.csv")


@productivity_bp.route("/admin/productivity")
@login_required
@admin_required
def productivity_page():
    return render_template("productivity.html")


@productivity_bp.route("/admin/productivity/data")
@login_required
@admin_required
def productivity_data():
    """Return the latest generated report. Never silently pretends data exists:
    if the file is missing or unreadable, that is surfaced explicitly."""
    if not os.path.exists(JSON_PATH):
        return jsonify({"exists": False,
                        "reason": "No report has been generated yet."})
    try:
        with open(JSON_PATH, encoding="utf-8") as f:
            payload = json.load(f)
    except Exception as e:
        return jsonify({"exists": False,
                        "reason": f"Report file exists but could not be read: {e}"})
    payload["exists"] = True
    payload["has_csv"] = os.path.exists(CSV_PATH)
    return jsonify(payload)


@productivity_bp.route("/admin/productivity/download")
@login_required
@admin_required
def productivity_download():
    if not os.path.exists(CSV_PATH):
        abort(404, "No CSV report has been generated yet. Run productivity_past_365_days.py first.")
    return send_file(CSV_PATH, as_attachment=True,
                     download_name="productivity_past_365_days.csv",
                     mimetype="text/csv")
