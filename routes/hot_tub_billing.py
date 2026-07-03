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
import io
import re
import csv
import sys
import glob
import json
import time
import calendar
import subprocess
import threading
from datetime import date, datetime

from flask import Blueprint, render_template, jsonify, send_file, request, abort, Response
from flask_login import login_required

from routes.auth import admin_required
from db import get_db, get_cursor
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
# Short TTL: she cross-checks the tape chart against Breezeway live, so stale
# reservations are confusing. 2 min still dedupes rapid reloads without lag.
_RES_CACHE_TTL = 2 * 60


def _month_reservations(month: str) -> dict:
    """All reservations overlapping a month, bucketed by property_id (cached).
    Read-only; classifies each guest/owner/lease/block. Shared by the tape-chart
    endpoint and the lease/floor-waiver logic so they always agree."""
    now = time.time()
    with _RES_CACHE_LOCK:
        hit = _RES_CACHE.get(month)
        if hit and now - hit[0] < _RES_CACHE_TTL:
            return hit[1]

    y, mo = int(month[:4]), int(month[5:7])
    first = date(y, mo, 1)
    last = date(y, mo, calendar.monthrange(y, mo)[1])

    token = _get_breezeway_token()
    if not token:
        return {"ok": False, "month": month,
                "reason": "Could not authenticate with Breezeway."}

    # Overlap: checks in on/before the last day AND out on/after the first day.
    raw = _fetch_bw_reservations(token, {
        "checkin_date_le": last.isoformat(),
        "checkout_date_ge": first.isoformat(),
    })
    by_property, seen = {}, set()
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
        by_property.setdefault(str(pid), []).append(
            {"checkin": ci, "checkout": co, "kind": _classify_reservation(r)})
    for lst in by_property.values():
        lst.sort(key=lambda x: x["checkin"])

    payload = {"ok": True, "month": month, "first": first.isoformat(),
               "last": last.isoformat(), "days": (last - first).days + 1,
               "count": len(seen), "by_property": by_property}
    with _RES_CACHE_LOCK:
        _RES_CACHE[month] = (now, payload)
    return payload


def _leased_property_ids(month: str) -> set:
    """Property ids (str) that had a LEASE overlapping the month. The owner floor
    minimum is WAIVED for these — during a lease the tenant covers service, so
    charging the owner a minimum would be a lie. Returns None if reservations
    couldn't be fetched (caller then leaves the floor as-is rather than guessing)."""
    data = _month_reservations(month)
    if not data or data.get("ok") is False:
        return None
    leased = set()
    for pid, lst in (data.get("by_property") or {}).items():
        if any(x.get("kind") == "lease" for x in lst):
            leased.add(str(pid))
    return leased


def _json_path(month: str) -> str:
    return os.path.join(_ROOT, f"hot_tub_billing_{month}.json")


def _csv_path(month: str) -> str:
    return os.path.join(_ROOT, f"hot_tub_billing_{month}.csv")


def _log_path(month: str) -> str:
    return os.path.join(_ROOT, f"hot_tub_billing_{month}.log")


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


# ── Durable storage (Postgres) ───────────────────────────────────────────────
# The host wipes the local filesystem on every deploy/restart, so worksheets and
# adjustments must live in the DB or they vanish. All app-side; never Breezeway.

def _db_save_worksheet(month: str, payload: dict) -> None:
    conn = get_db()
    try:
        cur = get_cursor(conn)
        cur.execute(
            "INSERT INTO hot_tub_worksheets (month, payload, generated_at, updated_at) "
            "VALUES (%s,%s,%s,%s) ON CONFLICT (month) DO UPDATE SET "
            "payload=EXCLUDED.payload, generated_at=EXCLUDED.generated_at, updated_at=EXCLUDED.updated_at",
            (month, json.dumps(payload), payload.get("generated_at") or _now_iso(), _now_iso()),
        )
        conn.commit()
    finally:
        conn.close()


def _db_load_worksheet(month: str):
    conn = get_db()
    try:
        cur = get_cursor(conn)
        cur.execute("SELECT payload FROM hot_tub_worksheets WHERE month=%s", (month,))
        row = cur.fetchone()
    finally:
        conn.close()
    if not row:
        return None
    try:
        return json.loads(row["payload"])
    except Exception:
        return None


def _db_worksheet_months() -> list:
    conn = get_db()
    try:
        cur = get_cursor(conn)
        cur.execute("SELECT month FROM hot_tub_worksheets")
        rows = cur.fetchall()
    finally:
        conn.close()
    return [r["month"] for r in rows]


def _db_load_overrides(month: str):
    conn = get_db()
    try:
        cur = get_cursor(conn)
        cur.execute("SELECT doc FROM hot_tub_overrides WHERE month=%s", (month,))
        row = cur.fetchone()
    finally:
        conn.close()
    if not row:
        return None
    try:
        return json.loads(row["doc"])
    except Exception:
        return None


def _db_save_overrides(month: str, doc: dict) -> None:
    conn = get_db()
    try:
        cur = get_cursor(conn)
        cur.execute(
            "INSERT INTO hot_tub_overrides (month, doc, updated_at) VALUES (%s,%s,%s) "
            "ON CONFLICT (month) DO UPDATE SET doc=EXCLUDED.doc, updated_at=EXCLUDED.updated_at",
            (month, json.dumps(doc), doc.get("updated_at") or _now_iso()),
        )
        conn.commit()
    finally:
        conn.close()


def _import_file_to_db_if_needed(month: str):
    """If the DB has no worksheet for `month` but a JSON file is on disk (a fresh
    scan this dyno, or the committed sample month), load it into the DB so it
    survives the next deploy. Returns the payload (from DB or file) or None."""
    payload = _db_load_worksheet(month)
    if payload is not None:
        return payload
    path = _json_path(month)
    if not os.path.exists(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            payload = json.load(f)
    except Exception:
        return None
    try:
        _db_save_worksheet(month, payload)
    except Exception:
        pass   # serving still works even if the persist fails
    return payload


def _worksheet_exists(month: str) -> bool:
    return _db_load_worksheet(month) is not None or os.path.exists(_json_path(month))


def _build_csv_text(payload: dict) -> str:
    """Rebuild the worksheet CSV from a stored payload (the on-disk CSV is gone
    after a deploy). Mirrors the engine's columns; still the RAW scan."""
    fields = ["property", "property_floor", "scheduled_date", "completed_date", "status",
              "disposition", "service_type", "price", "title", "description", "summary",
              "flags", "assignees", "task_tags", "reason"]
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fields)
    w.writeheader()
    for prop in payload.get("props", []):
        for r in prop.get("rows", []):
            billed = r.get("disposition") == "billable" and r.get("completed")
            w.writerow({
                "property": prop.get("property", ""),
                "property_floor": prop.get("floor", ""),
                "scheduled_date": r.get("scheduled_date", ""),
                "completed_date": r.get("completed_date", ""),
                "status": r.get("status", ""),
                "disposition": r.get("disposition", ""),
                "service_type": r.get("service_type", ""),
                "price": r.get("price", "") if billed else "",
                "title": r.get("title", ""),
                "description": r.get("description", ""),
                "summary": r.get("summary", ""),
                "flags": " | ".join(r.get("flags", []) or []),
                "assignees": "; ".join(r.get("assignees", []) or []),
                "task_tags": "; ".join(r.get("task_tags", []) or []),
                "reason": r.get("reason", ""),
            })
    return buf.getvalue()


def _adjusted_csv_text(payload: dict, overrides: dict, leased: set = None) -> str:
    """Billing-ready CSV that REFLECTS her local adjustments (comps, resolutions,
    do-not-bill, manual credits/services, floor minimums) — mirrors exactly what
    the page shows and totals. Same effective logic as the frontend so the CSV
    and the screen never disagree. Still app-side; Breezeway is untouched."""
    rows_ov = (overrides or {}).get("rows", {}) or {}
    manual = (overrides or {}).get("manual", []) or []
    fields = ["property", "scheduled_date", "completed_date", "status", "service_type",
              "charge", "title", "summary", "adjustment_note"]
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fields)
    w.writeheader()
    grand = 0
    for prop in payload.get("props", []):
        pid = str(prop.get("property_id"))
        pname = prop.get("property", "")
        floor = prop.get("floor", 0) or 0
        visits, subtotal = 0, 0
        for r in prop.get("rows", []):
            o = rows_ov.get(str(r.get("task_id")))
            disp = r.get("disposition")
            completed = bool(r.get("completed"))
            stype = r.get("service_type", "")
            charge = r.get("price", 0) if (disp == "billable" and completed) else 0
            status = "Billed" if (disp == "billable" and completed) else \
                     ("Review" if disp == "review" else "Not a service")
            note = ""
            included = False
            if o:
                act = o.get("action")
                note = o.get("note", "")
                if act == "comp":
                    status, charge = "Comped ($0)", 0
                elif act == "exclude":
                    status, charge = "Not billed", 0
                elif act == "include":
                    status, charge, included = "Billed", int(o.get("price", 0) or 0), True
                    if o.get("service_type"):
                        stype = o.get("service_type")
            performed = status in ("Billed", "Comped ($0)") and (completed or included)
            if performed:
                visits += 1
            if status == "Billed":
                subtotal += charge
            w.writerow({
                "property": pname, "scheduled_date": r.get("scheduled_date", ""),
                "completed_date": r.get("completed_date", ""), "status": status,
                "service_type": stype, "charge": charge if status in ("Billed", "Comped ($0)") else "",
                "title": r.get("title", ""), "summary": r.get("summary", ""),
                "adjustment_note": note,
            })
        # Manual lines. A logged SERVICE counts toward the floor minimum, so tally
        # them before deciding whether a top-up is needed.
        prop_manual = [m for m in manual if str(m.get("property_id")) == pid]
        manual_services = sum(1 for m in prop_manual if m.get("kind") != "credit")
        for m in prop_manual:
            amt = int(m.get("amount", 0) or 0)
            signed = -amt if m.get("kind") == "credit" else amt
            subtotal += signed
            w.writerow({"property": pname, "scheduled_date": m.get("date", ""),
                        "status": "Manual credit" if m.get("kind") == "credit" else "Manual service",
                        "service_type": m.get("service_type", ""), "charge": signed,
                        "adjustment_note": m.get("note", "")})
        accounted = visits + manual_services
        topup = 0 if (leased and pid in leased) else max(0, floor - accounted)
        if topup:
            amt = topup * PRICE_REGULAR_CSV
            subtotal += amt
            w.writerow({"property": pname, "status": "Floor minimum", "service_type": "regular",
                        "charge": amt, "adjustment_note": f"+{topup} to meet {floor}/mo floor"})
        elif floor and (leased and pid in leased) and accounted < floor:
            w.writerow({"property": pname, "status": "Floor waived", "service_type": "",
                        "charge": 0, "adjustment_note": f"{floor}/mo minimum waived — lease active"})
        w.writerow({"property": pname, "status": "SUBTOTAL", "charge": subtotal})
        grand += subtotal
    w.writerow({"property": "ALL PROPERTIES", "status": "GRAND TOTAL", "charge": grand})
    return buf.getvalue()


PRICE_REGULAR_CSV = 50   # floor top-up unit (matches the engine's Regular price)

_NUM_WORDS = ["Zero", "One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eight",
              "Nine", "Ten", "Eleven", "Twelve", "Thirteen", "Fourteen", "Fifteen",
              "Sixteen", "Seventeen", "Eighteen", "Nineteen", "Twenty"]


def _num_word(n: int) -> str:
    return _NUM_WORDS[n] if 0 <= n < len(_NUM_WORDS) else str(n)


def _join_and(items: list) -> str:
    items = [i for i in items if i]
    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    if len(items) == 2:
        return f"{items[0]} and {items[1]}"
    return ", ".join(items[:-1]) + f", and {items[-1]}"


def _month_name(month: str) -> str:
    try:
        y, m = int(month[:4]), int(month[5:7])
        return f"{calendar.month_name[m]} {y}"
    except Exception:
        return month


def _summary_csv_text(payload: dict, overrides: dict, month: str, leased: set = None) -> str:
    """One row per house: name, a plain-English tally of what was billed, and the
    total — reflecting her adjustments. E.g. 'One Regular Hot Tub Service and Two
    Dump & Scrub Services, June 2026'. WWM reads as 'Bacterial Treatment'; cold plunge as
    'Cold Plunge Service'. This is what she hands to billing."""
    rows_ov = (overrides or {}).get("rows", {}) or {}
    manual = (overrides or {}).get("manual", []) or []
    mlabel = _month_name(month)
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=["house", "summary", "total"])
    w.writeheader()
    for prop in payload.get("props", []):
        pid = str(prop.get("property_id"))
        counts = {"regular": 0, "dump_scrub": 0, "wwm": 0, "cold_plunge": 0}
        extra_parts, comped = [], 0
        visits, total = 0, 0
        for r in prop.get("rows", []):
            o = rows_ov.get(str(r.get("task_id")))
            disp, completed = r.get("disposition"), bool(r.get("completed"))
            stype = r.get("service_type", "")
            charge = r.get("price", 0) if (disp == "billable" and completed) else 0
            billed = (disp == "billable" and completed)
            if o:
                act = o.get("action")
                if act == "comp":
                    comped += 1; visits += 1; continue
                if act == "exclude":
                    continue
                if act == "include":
                    billed = True; charge = int(o.get("price", 0) or 0)
                    stype = o.get("service_type") or stype
            if not billed:
                continue
            visits += 1
            total += charge
            counts[stype] = counts.get(stype, 0) + 1
        # Manual lines first — a logged SERVICE counts toward the floor minimum
        # (she's accounting for a real service, so we must not also force a top-up).
        credit_note, manual_services = 0, 0
        for m in manual:
            if str(m.get("property_id")) != pid:
                continue
            amt = int(m.get("amount", 0) or 0)
            if m.get("kind") == "credit":
                total -= amt; credit_note += amt
            else:
                total += amt; manual_services += 1
                st = m.get("service_type", "")
                if st in counts:
                    counts[st] += 1
                else:
                    extra_parts.append(f"One {st or 'Manual Service'}")

        # Floor minimum → billed as Regular hot-tub services, UNLESS the house was
        # leased this month (tenant covers service; the owner minimum is waived).
        floor = prop.get("floor", 0) or 0
        is_leased = bool(leased and pid in leased)
        topup = 0 if is_leased else max(0, floor - (visits + manual_services))
        if topup:
            counts["regular"] += topup
            total += topup * PRICE_REGULAR_CSV

        # Build the phrase. Regular carries the full "Hot Tub Service(s)" noun;
        # Dump & Scrub gets its own "Service(s)".
        parts = []
        if counts.get("regular"):
            c = counts["regular"]; parts.append(f"{_num_word(c)} Regular Hot Tub Service" + ("s" if c > 1 else ""))
        if counts.get("dump_scrub"):
            c = counts["dump_scrub"]; parts.append(f"{_num_word(c)} Dump & Scrub Service" + ("s" if c > 1 else ""))
        if counts.get("wwm"):
            c = counts["wwm"]; parts.append(f"{_num_word(c)} Bacterial Treatment" + ("s" if c > 1 else ""))
        if counts.get("cold_plunge"):
            c = counts["cold_plunge"]; parts.append(f"{_num_word(c)} Cold Plunge Service" + ("s" if c > 1 else ""))
        parts.extend(extra_parts)

        body = _join_and(parts)
        summary = (f"{body}, {mlabel}" if body else f"No billable services, {mlabel}")
        notes = []
        if is_leased and floor and (visits + manual_services) < floor:
            notes.append("lease — owner floor waived")
        if comped:
            notes.append(f"{comped} comped")
        if credit_note:
            notes.append(f"less ${credit_note} credit")
        if notes:
            summary += f" ({'; '.join(notes)})"
        w.writerow({"house": prop.get("property", ""), "summary": summary, "total": total})
    return buf.getvalue()


def _overrides_path(month: str) -> str:
    """Local, app-side file holding Madeline's manual adjustments for a month
    (comps / review resolutions / hypothetical credit+service lines). This is the
    ONLY place her edits live — it is never sent to Breezeway, so the scan stays
    strictly read-only. Kept separate from the worksheet JSON so a re-scan never
    clobbers her decisions (they're keyed by task_id)."""
    return os.path.join(_ROOT, f"hot_tub_billing_{month}_overrides.json")


_EMPTY_OVERRIDES = {"rows": {}, "manual": []}


def _load_overrides(month: str) -> dict:
    # DB is the source of truth (survives deploys).
    doc = None
    try:
        doc = _db_load_overrides(month)
    except Exception:
        doc = None
    # One-time migration: if the DB has nothing but a legacy local file exists
    # (written before we moved to the DB), adopt it and persist to the DB.
    if doc is None:
        path = _overrides_path(month)
        if os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as f:
                    doc = json.load(f)
                if isinstance(doc, dict):
                    try:
                        _db_save_overrides(month, doc)
                    except Exception:
                        pass
            except Exception:
                doc = None
    if not isinstance(doc, dict):
        return {"month": month, **_EMPTY_OVERRIDES}
    doc.setdefault("rows", {})
    doc.setdefault("manual", [])
    return doc


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
    """Months that have a worksheet — in the DB (durable) or as a JSON file on
    disk (a fresh scan this dyno, or the committed sample). Newest first."""
    months = set()
    try:
        months.update(_db_worksheet_months())
    except Exception:
        pass
    for p in glob.glob(os.path.join(_ROOT, "hot_tub_billing_*.json")):
        m = _MONTH_RE.search(os.path.basename(p))
        if m:
            months.add(m.group(1))
    return sorted(months, reverse=True)


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
    payload = _db_load_worksheet(month)
    if payload is not None:
        return payload.get("generated_at")
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
    # DB first (durable); fall back to a fresh/committed file and import it so it
    # survives the next deploy.
    payload = _import_file_to_db_if_needed(month)
    if payload is None:
        return jsonify({"exists": False, "month": month,
                        "reason": f"No worksheet for {month} yet — click Generate to scan it."})
    payload["exists"] = True
    payload["has_csv"] = True   # rebuilt from the stored payload on demand
    return jsonify(payload)


@hot_tub_billing_bp.route("/admin/hot-tub-billing/download")
@login_required
@admin_required
def hot_tub_billing_download():
    month = (request.args.get("month") or "").strip()
    if not _MONTH_FMT.fullmatch(month or ""):
        abort(400, "month must be YYYY-MM.")
    # Default = the per-house summary she bills from (name, plain-English tally,
    # total), reflecting her adjustments. ?detail=1 = full line-item CSV with her
    # adjustments; ?raw=1 = the untouched scan.
    payload = _import_file_to_db_if_needed(month)
    if payload is None:
        abort(404, f"No worksheet for {month}. Generate the month first.")
    leased = _leased_property_ids(month)   # None if reservations couldn't be fetched
    if request.args.get("raw"):
        text, suffix = _build_csv_text(payload), "_raw"
    elif request.args.get("detail"):
        text, suffix = _adjusted_csv_text(payload, _load_overrides(month), leased), "_detail"
    else:
        text, suffix = _summary_csv_text(payload, _load_overrides(month), month, leased), "_summary"
    return Response(
        text, mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="hot_tub_billing_{month}{suffix}.csv"'},
    )


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

    exists = _worksheet_exists(month)
    with _JOBS_LOCK:
        job = _JOBS.get(month)
        if job is None:
            # Not running here. Report whatever is durably stored.
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
        # Persist the just-written file to the DB immediately so it survives the
        # next deploy/restart (the file itself lives on the ephemeral disk).
        payload = _import_file_to_db_if_needed(month)
        if payload is not None:
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
    return jsonify(_month_reservations(month))


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
    """Save the month's manual adjustments to the DB (durable across deploys).
    The client sends the full overrides doc; we sanitize and store it. This
    writes ONLY to our own database — Breezeway is never modified."""
    month = (request.args.get("month") or "").strip()
    if not _MONTH_FMT.fullmatch(month or ""):
        abort(400, "month must be YYYY-MM.")
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        abort(400, "Expected a JSON overrides object.")
    doc = _sanitize_overrides(month, body)
    try:
        _db_save_overrides(month, doc)
    except Exception as e:
        return jsonify({"saved": False, "message": f"Could not save adjustments: {e}"}), 500
    return jsonify({"saved": True, **doc})
