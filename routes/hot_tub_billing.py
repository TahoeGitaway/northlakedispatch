r"""
routes/hot_tub_billing.py — In-app viewer for the Hot Tub Billing worksheet.

Like routes/productivity.py, this page does NOT run the (slow, read-only)
Breezeway scan itself — that lives in the standalone program `hot_tub_billing.py`,
the single source of truth. This page just PRESENTS the monthly worksheet that
program writes, so Madeline can review every service and bill owners by hand in
Streamline. It never writes to Breezeway and never bills.

Generate / refresh a month's worksheet from the project folder:
    .\.venv\Scripts\python.exe hot_tub_billing.py --month 2026-05

That writes  hot_tub_billing_<month>.json / .csv / .md  (and updates
hot_tub_billing_latest.json). This page reads those files.

Admin-only (it's owner billing data).

Endpoints:
  GET /admin/hot-tub-billing               — the page (rules table + worksheet)
  GET /admin/hot-tub-billing/months        — list of months that have a worksheet
  GET /admin/hot-tub-billing/data?month=   — one month's worksheet JSON
  GET /admin/hot-tub-billing/download?month= — that month's CSV
"""

import os
import re
import glob
import json

from flask import Blueprint, render_template, jsonify, send_file, request, abort
from flask_login import login_required

from routes.auth import admin_required

hot_tub_billing_bp = Blueprint("hot_tub_billing", __name__)

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_MONTH_RE = re.compile(r"hot_tub_billing_(\d{4}-\d{2})\.json$")


def _json_path(month: str) -> str:
    return os.path.join(_ROOT, f"hot_tub_billing_{month}.json")


def _csv_path(month: str) -> str:
    return os.path.join(_ROOT, f"hot_tub_billing_{month}.csv")


def _available_months() -> list:
    months = []
    for p in glob.glob(os.path.join(_ROOT, "hot_tub_billing_*.json")):
        m = _MONTH_RE.search(os.path.basename(p))
        if m:
            months.append(m.group(1))
    return sorted(set(months), reverse=True)


@hot_tub_billing_bp.route("/admin/hot-tub-billing")
@login_required
@admin_required
def hot_tub_billing_page():
    return render_template("hot_tub_billing.html")


@hot_tub_billing_bp.route("/admin/hot-tub-billing/months")
@login_required
@admin_required
def hot_tub_billing_months():
    return jsonify({"months": _available_months()})


@hot_tub_billing_bp.route("/admin/hot-tub-billing/data")
@login_required
@admin_required
def hot_tub_billing_data():
    month = (request.args.get("month") or "").strip()
    if month and not re.fullmatch(r"\d{4}-\d{2}", month):
        abort(400, "month must be YYYY-MM.")
    if not month:
        avail = _available_months()
        if not avail:
            return jsonify({"exists": False,
                            "reason": "No worksheet generated yet. Run hot_tub_billing.py --month YYYY-MM."})
        month = avail[0]
    path = _json_path(month)
    if not os.path.exists(path):
        return jsonify({"exists": False, "month": month,
                        "reason": f"No worksheet for {month}. Run hot_tub_billing.py --month {month}."})
    try:
        with open(path, encoding="utf-8") as f:
            payload = json.load(f)
    except Exception as e:
        return jsonify({"exists": False, "month": month,
                        "reason": f"Worksheet file exists but could not be read: {e}"})
    payload["exists"] = True
    payload["has_csv"] = os.path.exists(_csv_path(month))
    payload["available_months"] = _available_months()
    return jsonify(payload)


@hot_tub_billing_bp.route("/admin/hot-tub-billing/download")
@login_required
@admin_required
def hot_tub_billing_download():
    month = (request.args.get("month") or "").strip()
    if not re.fullmatch(r"\d{4}-\d{2}", month or ""):
        abort(400, "month must be YYYY-MM.")
    path = _csv_path(month)
    if not os.path.exists(path):
        abort(404, f"No CSV for {month}. Run hot_tub_billing.py --month {month} first.")
    return send_file(path, as_attachment=True,
                     download_name=os.path.basename(path), mimetype="text/csv")
