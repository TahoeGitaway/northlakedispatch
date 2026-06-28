r"""
routes/hot_tub_billing.py — In-app viewer for the Hot Tub Billing worksheet.

Like routes/productivity.py, this page does NOT classify hot-tub services
itself — that lives in the standalone program `hot_tub_billing.py`, the single
source of truth. This page PRESENTS the monthly worksheet that program writes so
Madeline can review every service and bill owners by hand in Streamline. It never
writes to Breezeway and never bills.

What's different from productivity: the user can pick ANY month here and, if that
month hasn't been scanned yet, kick off the (read-only, ~1-3 min) Breezeway scan
on demand. We do that by launching the same standalone engine as a background
subprocess and letting the page poll for completion — the engine stays the one
place the rules live.

Generate / refresh a month's worksheet from the CLI (still works, same files):
    .\.venv\Scripts\python.exe hot_tub_billing.py --month 2026-05

That writes  hot_tub_billing_<month>.json / .csv / .md  (and updates
hot_tub_billing_latest.json). This page reads those files.

Admin-only (it's owner billing data).

Endpoints:
  GET  /admin/hot-tub-billing                  — the page (rules table + worksheet)
  GET  /admin/hot-tub-billing/months           — pickable months + which are generated
  GET  /admin/hot-tub-billing/data?month=      — one month's worksheet JSON
  GET  /admin/hot-tub-billing/download?month=  — that month's CSV
  POST /admin/hot-tub-billing/generate?month=  — launch a background scan for a month
  GET  /admin/hot-tub-billing/status?month=    — progress of a launched scan
"""

import os
import re
import sys
import glob
import json
import subprocess
import threading
from datetime import date, datetime

from flask import Blueprint, render_template, jsonify, send_file, request, abort
from flask_login import login_required

from routes.auth import admin_required

hot_tub_billing_bp = Blueprint("hot_tub_billing", __name__)

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_MONTH_RE = re.compile(r"hot_tub_billing_(\d{4}-\d{2})\.json$")
_MONTH_FMT = re.compile(r"\d{4}-\d{2}")

# How many months back the picker offers (current month + this many older).
_PICK_BACK = 18

# In-process registry of running scans, keyed by month → {proc, started_at, log}.
# Guarded by a lock. The on-disk JSON file is the cross-process source of truth
# for "is it done"; this dict is just "is a scan running right now in this app".
_JOBS = {}
_JOBS_LOCK = threading.Lock()


def _json_path(month: str) -> str:
    return os.path.join(_ROOT, f"hot_tub_billing_{month}.json")


def _csv_path(month: str) -> str:
    return os.path.join(_ROOT, f"hot_tub_billing_{month}.csv")


def _log_path(month: str) -> str:
    return os.path.join(_ROOT, f"hot_tub_billing_{month}.log")


def _available_months() -> list:
    """Months that already have a worksheet JSON on disk (newest first)."""
    months = []
    for p in glob.glob(os.path.join(_ROOT, "hot_tub_billing_*.json")):
        m = _MONTH_RE.search(os.path.basename(p))
        if m:
            months.append(m.group(1))
    return sorted(set(months), reverse=True)


def _pickable_months(n_back: int = _PICK_BACK) -> list:
    """Current month back through n_back older months (newest first)."""
    today = date.today()
    y, m = today.year, today.month
    out = []
    for _ in range(n_back + 1):
        out.append(f"{y:04d}-{m:02d}")
        m -= 1
        if m == 0:
            m, y = 12, y - 1
    return out


def _current_month() -> str:
    today = date.today()
    return f"{today.year:04d}-{today.month:02d}"


def _generated_at(month: str):
    path = _json_path(month)
    if not os.path.exists(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f).get("generated_at")
    except Exception:
        return None


def _engine_python() -> str:
    """The venv python if present (matches the documented CLI invocation),
    otherwise whatever interpreter is running this app."""
    cand = os.path.join(_ROOT, ".venv", "Scripts", "python.exe")
    return cand if os.path.exists(cand) else sys.executable


def _tail(path: str, n: int = 4000) -> str:
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            return f.read()[-n:]
    except Exception:
        return ""


@hot_tub_billing_bp.route("/admin/hot-tub-billing")
@login_required
@admin_required
def hot_tub_billing_page():
    return render_template("hot_tub_billing.html")


@hot_tub_billing_bp.route("/admin/hot-tub-billing/months")
@login_required
@admin_required
def hot_tub_billing_months():
    generated = _available_months()
    pickable = _pickable_months()
    current = _current_month()
    # Default landing: the newest already-generated month (usually last full
    # month), so the page always opens on real data; else last full month.
    if generated:
        default = generated[0]
    else:
        default = pickable[1] if len(pickable) > 1 else pickable[0]
    return jsonify({
        "generated": generated,
        "pickable": pickable,
        "current_month": current,
        "default": default,
    })


@hot_tub_billing_bp.route("/admin/hot-tub-billing/data")
@login_required
@admin_required
def hot_tub_billing_data():
    month = (request.args.get("month") or "").strip()
    if month and not _MONTH_FMT.fullmatch(month):
        abort(400, "month must be YYYY-MM.")
    if not month:
        avail = _available_months()
        if not avail:
            return jsonify({"exists": False,
                            "reason": "No worksheet generated yet. Pick a month and Generate it."})
        month = avail[0]
    path = _json_path(month)
    if not os.path.exists(path):
        return jsonify({"exists": False, "month": month,
                        "reason": f"No worksheet for {month} yet — click Generate to scan it."})
    try:
        with open(path, encoding="utf-8") as f:
            payload = json.load(f)
    except Exception as e:
        return jsonify({"exists": False, "month": month,
                        "reason": f"Worksheet file exists but could not be read: {e}"})
    payload["exists"] = True
    payload["has_csv"] = os.path.exists(_csv_path(month))
    return jsonify(payload)


@hot_tub_billing_bp.route("/admin/hot-tub-billing/download")
@login_required
@admin_required
def hot_tub_billing_download():
    month = (request.args.get("month") or "").strip()
    if not _MONTH_FMT.fullmatch(month or ""):
        abort(400, "month must be YYYY-MM.")
    path = _csv_path(month)
    if not os.path.exists(path):
        abort(404, f"No CSV for {month}. Generate the month first.")
    return send_file(path, as_attachment=True,
                     download_name=os.path.basename(path), mimetype="text/csv")


@hot_tub_billing_bp.route("/admin/hot-tub-billing/generate", methods=["POST"])
@login_required
@admin_required
def hot_tub_billing_generate():
    """Launch the standalone engine for one month as a background subprocess.

    Read-only against Breezeway; takes ~1-3 min. Returns immediately — the page
    polls /status to know when the worksheet file is ready.
    """
    month = (request.args.get("month") or "").strip()
    if not _MONTH_FMT.fullmatch(month or ""):
        abort(400, "month must be YYYY-MM.")
    if month > _current_month():
        abort(400, "Can't scan a future month.")

    with _JOBS_LOCK:
        job = _JOBS.get(month)
        if job and job["proc"].poll() is None:
            return jsonify({"started": False, "running": True, "month": month,
                            "message": "A scan for this month is already running."})

        log_path = _log_path(month)
        try:
            logf = open(log_path, "w", encoding="utf-8")
            proc = subprocess.Popen(
                [_engine_python(), os.path.join(_ROOT, "hot_tub_billing.py"),
                 "--month", month, "--outdir", _ROOT],
                cwd=_ROOT, stdout=logf, stderr=subprocess.STDOUT,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        except Exception as e:
            return jsonify({"started": False, "running": False, "month": month,
                            "message": f"Could not launch the scan: {e}"}), 500

        _JOBS[month] = {"proc": proc, "logf": logf,
                        "started_at": datetime.now().astimezone().isoformat(timespec="seconds"),
                        "log": log_path}
    return jsonify({"started": True, "running": True, "month": month})


@hot_tub_billing_bp.route("/admin/hot-tub-billing/status")
@login_required
@admin_required
def hot_tub_billing_status():
    month = (request.args.get("month") or "").strip()
    if not _MONTH_FMT.fullmatch(month or ""):
        abort(400, "month must be YYYY-MM.")

    exists = os.path.exists(_json_path(month))
    with _JOBS_LOCK:
        job = _JOBS.get(month)
        if job is None:
            # Not running here. Report whatever is on disk.
            return jsonify({"month": month, "running": False, "exists": exists,
                            "generated_at": _generated_at(month)})

        rc = job["proc"].poll()
        if rc is None:
            return jsonify({"month": month, "running": True, "exists": exists,
                            "started_at": job["started_at"]})

        # Finished — clean up and report. rc 0/1 = COMPLETE/PARTIAL (file written,
        # the page shows the partial banner from the JSON); rc 2 = HALTED, no file.
        try:
            job["logf"].close()
        except Exception:
            pass
        log = job["log"]
        _JOBS.pop(month, None)
        exists = os.path.exists(_json_path(month))
        if exists:
            return jsonify({"month": month, "running": False, "exists": True,
                            "returncode": rc, "generated_at": _generated_at(month)})
        return jsonify({"month": month, "running": False, "exists": False,
                        "returncode": rc, "failed": True,
                        "message": "Scan finished but wrote no worksheet.",
                        "log_tail": _tail(log)})
