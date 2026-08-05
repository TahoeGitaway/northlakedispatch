"""
routes/bw_api_log.py — a record of every Breezeway REQUEST, success or failure.

Standalone. Nothing else imports from here except a single call in
briefing._fetch_bw_endpoint and the blueprint registration in app.py. It owns its
own table, its own buffer and its own page, and it holds no reference to any
other feature's state — so it can be deleted by removing this file, its template,
two lines in app.py and one call site, with nothing else to unpick.

What it is for
--------------
bw_write_log answers "what did this app change". This answers "what did Breezeway
do to us". They are separate questions and deliberately separate tables: one is a
repair record for wrong-but-successful writes, the other is evidence for a
rate-limit conversation with the vendor.

Successes are recorded as well as failures. "261 refused out of 1,847" is a very
different conversation from "we got some errors", and only the first can be
argued from.

Why buffered
------------
A single day-scan makes ~442 requests. A database round-trip per request would
add real time to a scan that is already the slowest thing in the app, so rows
accumulate in memory and are flushed in batches. A crash can therefore lose the
last few seconds of log — acceptable for a diagnostic, and the alternative
(slowing every scan) is not.

Nothing here may raise. A logging failure must never surface in a feature that
was merely making an API call.
"""

import threading
import time
from datetime import datetime, timezone, timedelta

from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required

from routes.auth import admin_required

bw_api_log_bp = Blueprint("bw_api_log", __name__)

# Flush when either threshold is hit. Small enough that the page is near-live,
# large enough that a 442-call sweep costs a handful of inserts, not 442.
_FLUSH_ROWS    = 100
_FLUSH_SECONDS = 10.0
_RETENTION_DAYS = 21          # pruned opportunistically on flush

_buf: list = []
_buf_lock = threading.Lock()
_last_flush = 0.0
_last_prune = 0.0


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _feature() -> str:
    """Which page/endpoint triggered the call. Flask already knows; no feature
    needs to pass it in, which keeps the call site to one line."""
    try:
        return (request.endpoint or request.path or "")[:80]
    except Exception:
        return ""            # scheduled job — no request context


def record(endpoint: str, status, ok: bool, ref: str = "",
           elapsed_ms: int = 0, detail: str = "") -> None:
    """Buffer one Breezeway request outcome. Never raises, never blocks on I/O."""
    try:
        with _buf_lock:
            _buf.append((_now_iso(), _feature(), (endpoint or "")[:120],
                         int(status) if isinstance(status, int) else None,
                         bool(ok), (ref or "")[:80], int(elapsed_ms or 0),
                         (detail or "")[:300]))
            due = len(_buf) >= _FLUSH_ROWS or (time.time() - _last_flush) > _FLUSH_SECONDS
        if due:
            flush()
    except Exception:
        pass


def flush() -> int:
    """Write buffered rows. Returns how many were written. Never raises."""
    global _last_flush, _last_prune
    try:
        with _buf_lock:
            if not _buf:
                _last_flush = time.time()
                return 0
            rows, _buf[:] = list(_buf), []
            _last_flush = time.time()

        from db import get_db, get_cursor
        conn = get_db(); cur = get_cursor(conn)
        try:
            cur.executemany("""
                INSERT INTO bw_api_log
                       (ts, feature, endpoint, status, ok, ref, elapsed_ms, detail)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """, rows)
            # Keep the table from growing without bound. Once an hour is plenty.
            if time.time() - _last_prune > 3600:
                cutoff = (datetime.now(timezone.utc)
                          - timedelta(days=_RETENTION_DAYS)).isoformat()
                cur.execute("DELETE FROM bw_api_log WHERE ts < %s", (cutoff,))
                _last_prune = time.time()
            conn.commit()
        finally:
            cur.close(); conn.close()
        return len(rows)
    except Exception:
        return 0


def bw_get(url, **kwargs):
    """requests.get for Breezeway, recorded. Drop-in replacement: identical
    arguments, return value and exceptions.

    Every Breezeway read should go through this. The alternative — a record()
    call next to each requests.get — is what left two dozen call sites
    uninstrumented and the 429 count wrong; with a wrapper, a new caller is
    logged by default and only an explicit `requests.get` can escape it."""
    import requests
    t0 = time.time()
    params = kwargs.get("params") or {}
    ref = str(params.get("reference_property_id") or params.get("home_id")
              or params.get("property_id") or "")[:80]
    path = url.split("breezeway.io", 1)[-1] if "breezeway.io" in url else url
    try:
        r = requests.get(url, **kwargs)
        record(path, r.status_code, r.ok, ref,
               int((time.time() - t0) * 1000),
               "" if r.ok else (r.text or "")[:200])
        return r
    except Exception as e:
        record(path, None, False, ref, int((time.time() - t0) * 1000),
               f"{type(e).__name__}: {e}")
        raise


# ── Reading ───────────────────────────────────────────────────────

def _rows(show: str = "all", day: str = "", status: str = "", limit: int = 500) -> list:
    """Newest first. `show` is all | ok | failed."""
    from db import get_db, get_cursor
    where, args = [], []
    if show == "ok":
        where.append("ok = TRUE")
    elif show == "failed":
        where.append("ok = FALSE")
    day_sql, day_args = _day_clause(day)
    if day_sql:
        where.append(day_sql); args += day_args
    if status:
        if status == "timeout":
            where.append("status IS NULL")
        else:
            where.append("status = %s"); args.append(int(status))
    sql = "SELECT * FROM bw_api_log"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id DESC LIMIT %s"
    args.append(max(1, min(5000, limit)))

    conn = get_db(); cur = get_cursor(conn)
    try:
        cur.execute(sql, tuple(args))
        return [dict(r) for r in cur.fetchall()]
    finally:
        cur.close(); conn.rollback(); conn.close()


def _day_clause(day: str) -> tuple:
    """(sql_fragment, args) limiting to one day, or ('', []) for all time.
    One place builds it so the two queries below can't drift apart."""
    if not day:
        return "", []
    return "ts >= %s AND ts < %s", [f"{day}T00:00:00", f"{day}T23:59:59.999"]


def _totals(day: str = "") -> dict:
    """Counts for the header line — the denominator that turns a raw number into a
    rate. "261 refused out of 1,847" is arguable; "261 errors" is not."""
    from db import get_db, get_cursor
    day_sql, day_args = _day_clause(day)

    summary_where = f" WHERE {day_sql}" if day_sql else ""
    # Failures only, and the day filter if present — built as one list so a second
    # WHERE can never be concatenated onto the first.
    code_conds = ["NOT ok"] + ([day_sql] if day_sql else [])
    code_where = " WHERE " + " AND ".join(code_conds)

    conn = get_db(); cur = get_cursor(conn)
    try:
        cur.execute(f"""SELECT COUNT(*)                            AS total,
                               COUNT(*) FILTER (WHERE ok)          AS ok,
                               COUNT(*) FILTER (WHERE NOT ok)      AS failed,
                               COUNT(*) FILTER (WHERE status = 429) AS throttled
                        FROM bw_api_log{summary_where}""", tuple(day_args))
        t = dict(cur.fetchone() or {})
        cur.execute(f"""SELECT COALESCE(CAST(status AS TEXT), 'timeout') AS code,
                               COUNT(*) AS n
                        FROM bw_api_log{code_where}
                        GROUP BY 1 ORDER BY n DESC""", tuple(day_args))
        t["by_code"] = [dict(r) for r in cur.fetchall()]
        return t
    finally:
        cur.close(); conn.rollback(); conn.close()


@bw_api_log_bp.route("/admin/breezeway-log")
@login_required
@admin_required
def breezeway_log_page():
    flush()                                   # show what just happened
    day    = (request.args.get("day") or "").strip()[:10]
    show   = request.args.get("show") or "all"
    status = (request.args.get("status") or "").strip()
    return render_template("breezeway_log.html",
                           rows=_rows(show, day, status,
                                      int(request.args.get("limit") or 500)),
                           totals=_totals(day),
                           f_day=day, f_show=show, f_status=status)


@bw_api_log_bp.route("/admin/breezeway-log.json")
@login_required
@admin_required
def breezeway_log_json():
    flush()
    day = (request.args.get("day") or "").strip()[:10]
    return jsonify({"totals": _totals(day),
                    "rows": _rows(request.args.get("show") or "all", day,
                                  (request.args.get("status") or "").strip(),
                                  int(request.args.get("limit") or 500))})
