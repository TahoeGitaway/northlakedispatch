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
import time
import calendar
import subprocess
import threading
from datetime import date, datetime

from flask import Blueprint, render_template, jsonify, send_file, request, abort
from flask_login import login_required

from routes.auth import admin_required
# Reuse briefing's proven, read-only Breezeway helpers so the tape charts share
# exactly one auth + reservation-classification code path with the rest of the app.
from routes.briefing import (
    _get_breezeway_token,
    _fetch_bw_reservations,
    _classify_reservation,
)

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

# Cached per-month reservation buckets for the tape charts: month → (ts, payload).
# Reservations move slowly relative to a billing review session, so a short TTL
# keeps the charts responsive without hammering Breezeway on every page load.
_RES_CACHE = {}
_RES_CACHE_LOCK = threading.Lock()
_RES_CACHE_TTL = 15 * 60   # 15 minutes


def _json_path(month: str) -> str:
    return os.path.join(_ROOT, f"hot_tub_billing_{month}.json")


def _csv_path(month: str) -> str:
    return os.path.join(_ROOT, f"hot_tub_billing_{month}.csv")


def _log_path(month: str) -> str:
    return os.path.join(_ROOT, f"hot_tub_billing_{month}.log")


def _overrides_path(month: str) -> str:
    """Local, app-side file holding Madeline's manual adjustments for a month
    (comps / review resolutions / hypothetical credit+service lines). This is the
    ONLY place her edits live — it is never sent to Breezeway, so the scan stays
    strictly read-only. Kept separate from the worksheet JSON so a re-scan never
    clobbers her decisions (they're keyed by task_id)."""
    return os.path.join(_ROOT, f"hot_tub_billing_{month}_overrides.json")


_EMPTY_OVERRIDES = {"rows": {}, "manual": []}


def _load_overrides(month: str) -> dict:
    path = _overrides_path(month)
    if not os.path.exists(path):
        return {"month": month, **_EMPTY_OVERRIDES}
    try:
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
        if not isinstance(doc, dict):
            return {"month": month, **_EMPTY_OVERRIDES}
        doc.setdefault("rows", {})
        doc.setdefault("manual", [])
        return doc
    except Exception:
        return {"month": month, **_EMPTY_OVERRIDES}


_ALLOWED_ROW_ACTIONS = {"comp", "exclude", "include"}
_ALLOWED_MANUAL_KINDS = {"service", "credit"}


def _sanitize_overrides(month: str, body: dict) -> dict:
    """Coerce a client-submitted overrides doc into a safe, minimal shape before
    writing it. Defensive because this file drives the billed totals — we never
    trust arbitrary keys or unbounded values."""
    rows_in = body.get("rows") or {}
    manual_in = body.get("manual") or []
    rows = {}
    if isinstance(rows_in, dict):
        for tid, o in list(rows_in.items())[:2000]:
            if not isinstance(o, dict):
                continue
            action = str(o.get("action") or "").strip().lower()
            if action not in _ALLOWED_ROW_ACTIONS:
                continue
            entry = {"action": action}
            if action == "include":
                try:
                    entry["price"] = max(0, min(100000, int(round(float(o.get("price", 0))))))
                except (TypeError, ValueError):
                    entry["price"] = 0
                st = str(o.get("service_type") or "").strip()[:40]
                if st:
                    entry["service_type"] = st
            note = str(o.get("note") or "").strip()[:500]
            if note:
                entry["note"] = note
            rows[str(tid)[:40]] = entry
    manual = []
    if isinstance(manual_in, list):
        for m in manual_in[:500]:
            if not isinstance(m, dict):
                continue
            kind = str(m.get("kind") or "").strip().lower()
            if kind not in _ALLOWED_MANUAL_KINDS:
                continue
            try:
                amount = max(0, min(100000, int(round(float(m.get("amount", 0))))))
            except (TypeError, ValueError):
                amount = 0
            manual.append({
                "id": str(m.get("id") or "")[:40] or f"m{len(manual)+1}",
                "property_id": str(m.get("property_id") or "")[:40],
                "kind": kind,
                "service_type": str(m.get("service_type") or "").strip()[:40],
                "amount": amount,
                "date": str(m.get("date") or "")[:10],
                "note": str(m.get("note") or "").strip()[:500],
            })
    return {
        "month": month,
        "updated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "rows": rows,
        "manual": manual,
    }


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


@hot_tub_billing_bp.route("/admin/hot-tub-billing/reservations")
@login_required
@admin_required
def hot_tub_billing_reservations():
    """Reservations overlapping a month, bucketed by property, for the per-house
    tape charts on the worksheet page.

    This is READ-ONLY and independent of the billing engine — it fetches live
    from Breezeway so already-generated worksheets get tape charts without a
    re-scan. One overlap query returns every reservation touching the month
    (including ones that span it entirely); we classify each with the same
    guest/owner/lease/block logic the rest of the app uses and return only the
    fields the chart needs.
    """
    month = (request.args.get("month") or "").strip()
    if not _MONTH_FMT.fullmatch(month or ""):
        abort(400, "month must be YYYY-MM.")

    now = time.time()
    with _RES_CACHE_LOCK:
        hit = _RES_CACHE.get(month)
        if hit and now - hit[0] < _RES_CACHE_TTL:
            return jsonify(hit[1])

    y, mo = int(month[:4]), int(month[5:7])
    first = date(y, mo, 1)
    last = date(y, mo, calendar.monthrange(y, mo)[1])

    token = _get_breezeway_token()
    if not token:
        # Never fail silently — tell the page so it can show a small notice
        # instead of pretending there are no reservations.
        return jsonify({"ok": False, "month": month,
                        "reason": "Could not authenticate with Breezeway."}), 200

    # A reservation overlaps the month iff it checks in on/before the last day
    # AND checks out on/after the first day. Breezeway ANDs these two filters,
    # so this single query captures full-month spans that a checkin-only query
    # would miss (verified live).
    raw = _fetch_bw_reservations(token, {
        "checkin_date_le": last.isoformat(),
        "checkout_date_ge": first.isoformat(),
    })

    by_property = {}
    seen = set()
    for r in raw:
        rid = r.get("id")
        if rid in seen:
            continue
        seen.add(rid)
        pid = r.get("property_id")
        if pid is None:
            continue
        ci = (r.get("checkin_date") or "")[:10]
        co = (r.get("checkout_date") or "")[:10]
        if not ci or not co:
            continue
        by_property.setdefault(str(pid), []).append({
            "checkin": ci,
            "checkout": co,
            "kind": _classify_reservation(r),
        })
    for lst in by_property.values():
        lst.sort(key=lambda x: x["checkin"])

    payload = {
        "ok": True,
        "month": month,
        "first": first.isoformat(),
        "last": last.isoformat(),
        "days": (last - first).days + 1,
        "count": len(seen),
        "by_property": by_property,
    }
    with _RES_CACHE_LOCK:
        _RES_CACHE[month] = (now, payload)
    return jsonify(payload)


@hot_tub_billing_bp.route("/admin/hot-tub-billing/overrides")
@login_required
@admin_required
def hot_tub_billing_overrides_get():
    """Return the local, app-side manual adjustments for a month (never touches
    Breezeway)."""
    month = (request.args.get("month") or "").strip()
    if not _MONTH_FMT.fullmatch(month or ""):
        abort(400, "month must be YYYY-MM.")
    return jsonify(_load_overrides(month))


@hot_tub_billing_bp.route("/admin/hot-tub-billing/overrides", methods=["POST"])
@login_required
@admin_required
def hot_tub_billing_overrides_save():
    """Save the month's manual adjustments locally. The client sends the full
    overrides doc; we sanitize and write it. This is the ONLY write in the whole
    feature and it writes ONLY to a local file — Breezeway is never modified."""
    month = (request.args.get("month") or "").strip()
    if not _MONTH_FMT.fullmatch(month or ""):
        abort(400, "month must be YYYY-MM.")
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        abort(400, "Expected a JSON overrides object.")
    doc = _sanitize_overrides(month, body)
    try:
        with open(_overrides_path(month), "w", encoding="utf-8") as f:
            json.dump(doc, f, indent=2)
    except Exception as e:
        return jsonify({"saved": False, "message": f"Could not save adjustments: {e}"}), 500
    return jsonify({"saved": True, **doc})
