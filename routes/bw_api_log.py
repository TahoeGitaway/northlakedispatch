"""
routes/bw_api_log.py — a record of every Breezeway REQUEST, success or failure.

Standalone. Features import the bw_get / bw_patch / bw_post / bw_delete wrappers
and nothing else; the one exception is a name lookup on the admin page, which
reads briefing's property cache and degrades to a bare id if it isn't there. It
owns its own table, its own buffer and its own page, so it can be deleted by
removing this file, its template, two lines in app.py and swapping the wrappers
back for plain `requests` calls, with nothing else to unpick.

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

import contextvars
import re
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


# Fallback trigger label for calls made OFF the request thread.
#
# The per-property sweeps run inside ThreadPoolExecutor workers, and a worker
# thread has no Flask request context — so `request.endpoint` raised and every
# one of the ~442 task calls in every sweep logged a blank "triggered by". The
# column was only ever populated for the handful of calls that happen to run on
# the request thread, which made the log useless for the one question it exists
# to answer: which feature is burning the rate limit.
#
# A ContextVar carries the label instead. ThreadPoolExecutor does NOT propagate
# context automatically, so callers snapshot it with bw_worker_context() below
# and run the worker inside that copy.
_feature_cv: contextvars.ContextVar = contextvars.ContextVar("bw_feature", default="")


def _feature() -> str:
    """Which page/endpoint triggered the call.

    The ContextVar is checked FIRST, and that order is load-bearing. copy_context()
    also copies Flask's request context (Flask implements it with ContextVars), so
    inside a worker's ctx.run() `request` still resolves — checking it first meant
    the endpoint name won and the ":retry" / ":retry-auto" suffix was silently
    dropped, which is the entire reason for capturing a label. The ContextVar is
    only ever set by bw_worker_context(), so on a normal request thread it is empty
    and this falls through to Flask exactly as before.
    """
    explicit = _feature_cv.get()
    if explicit:
        return explicit
    try:
        return (request.endpoint or request.path or "")[:80]
    except Exception:
        return ""            # scheduled job — no request context, nothing captured


def bw_worker_context(suffix: str = ""):
    """Snapshot the current trigger label into a context worker threads can run under.

    Call on the REQUEST thread, then submit work as `ctx.run(fn, ...)`:

        ctx = bw_worker_context()
        futures = {executor.submit(ctx.run, fn, a, b): a for a in items}

    `suffix` distinguishes callers that share one Flask endpoint — an import and
    its retry both POST /api/bw-import, and telling them apart is the whole point
    of looking at this log after a 429 storm.
    """
    try:
        label = (request.endpoint or request.path or "")[:80]
    except Exception:
        label = _feature_cv.get() or ""
    if suffix:
        label = f"{label}:{suffix}"[:80]
    ctx = contextvars.copy_context()
    ctx.run(_feature_cv.set, label)
    return ctx


# Every write puts the thing it is changing in the path, not the query:
# /public/inventory/v1/task/526104. Params can't see those, so read both.
_PATH_ID_RE = re.compile(r"/(task|reservation|home|property)/(\d+)")

# The JWT travels in a header, not the query — but a future caller may be less
# careful, and this table is read by humans in a browser.
_SECRET_HINT = ("token", "key", "secret", "auth", "password")


def _ref_from(endpoint: str, params) -> str:
    """What the call was about, labelled so a task id can't be mistaken for a
    property id. Empty when the call is not scoped to one thing — a date-window
    reservation sweep covers the whole portfolio, and saying so beats a number
    that would be a guess."""
    try:
        p = params if isinstance(params, dict) else {}
        # reference_property_id and property_id are DIFFERENT id spaces — the task
        # API takes the external reference, the inventory API takes Breezeway's own.
        # Labelling them apart is what lets the name lookup pick the right map.
        for key, label in (("reference_property_id", "property ref"),
                           ("home_id",               "property"),
                           ("property_id",           "property"),
                           ("task_id",               "task")):
            if p.get(key):
                return f"{label} {p[key]}"[:80]
        m = _PATH_ID_RE.search(endpoint or "")
        return f"{m.group(1)} {m.group(2)}"[:80] if m else ""
    except Exception:
        return ""


def _params_text(params) -> str:
    """The query as sent. For a call with no property filter this is the only
    thing that identifies it — without it, every reservation fetch in the table
    is the same row repeated."""
    try:
        parts = []
        for k, v in (params if isinstance(params, dict) else {}).items():
            if any(h in str(k).lower() for h in _SECRET_HINT):
                v = "***"
            parts.append(f"{k}={v}")
        return ", ".join(parts)[:300]
    except Exception:
        return ""


def record(endpoint: str, status, ok: bool, ref: str = "",
           elapsed_ms: int = 0, detail: str = "",
           method: str = "GET", params=None) -> None:
    """Buffer one Breezeway request outcome. Never raises, never blocks on I/O.

    `ref` is derived from `params` and `endpoint` when the caller doesn't pass
    one, so a new call site gets it right by doing nothing."""
    try:
        with _buf_lock:
            _buf.append((_now_iso(), _feature(), (endpoint or "")[:120],
                         int(status) if isinstance(status, int) else None,
                         bool(ok), (ref or _ref_from(endpoint, params))[:80],
                         int(elapsed_ms or 0), (detail or "")[:300],
                         (method or "GET")[:10], _params_text(params)))
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
                       (ts, feature, endpoint, status, ok, ref, elapsed_ms,
                        detail, method, params)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
    path = url.split("breezeway.io", 1)[-1] if "breezeway.io" in url else url
    try:
        r = requests.get(url, **kwargs)
        record(path, r.status_code, r.ok,
               elapsed_ms=int((time.time() - t0) * 1000),
               detail="" if r.ok else (r.text or "")[:200], params=params)
        return r
    except Exception as e:
        record(path, None, False, elapsed_ms=int((time.time() - t0) * 1000),
               detail=f"{type(e).__name__}: {e}", params=params)
        raise


def bw_write(method: str, url, **kwargs):
    """requests.<method> for Breezeway writes, recorded. Drop-in replacement:
    identical arguments, return value and exceptions.

    Same wrapper shape as bw_get, for a sharper reason. bw_audit records a write
    once it knows what changed — so a PATCH that times out, or is refused, is the
    one write that leaves no trace, and it is precisely the one you go looking for
    afterwards. Here it lands next to the reads, with the task id and the payload.

    Costs no extra database work: record() buffers in memory and the existing
    batch flush carries these rows along with the reads.

    Deliberately does NOT touch the rate gate. Callers that pace themselves
    (breezeway_sync) already do it around their own call; making this wrapper
    gate too would double-charge the budget and change behaviour, and this is
    meant to be observational only.
    """
    import requests
    t0 = time.time()
    path = url.split("breezeway.io", 1)[-1] if "breezeway.io" in url else url
    # For a write the identity is the body, not the query string — "which task"
    # comes from the path, "what did we try to set" only from the payload.
    sent = dict(kwargs.get("params") or {})
    body = kwargs.get("json")
    if isinstance(body, dict):
        sent.update(body)
    elif body is not None:
        sent["body"] = body
    verb = (method or "").upper()
    try:
        r = requests.request(verb, url, **kwargs)
        record(path, r.status_code, r.ok,
               elapsed_ms=int((time.time() - t0) * 1000),
               detail="" if r.ok else (r.text or "")[:200],
               method=verb, params=sent)
        return r
    except Exception as e:
        record(path, None, False, elapsed_ms=int((time.time() - t0) * 1000),
               detail=f"{type(e).__name__}: {e}", method=verb, params=sent)
        raise


def bw_patch(url, **kwargs):
    return bw_write("PATCH", url, **kwargs)


def bw_post(url, **kwargs):
    return bw_write("POST", url, **kwargs)


def bw_delete(url, **kwargs):
    return bw_write("DELETE", url, **kwargs)


# ── Reading ───────────────────────────────────────────────────────

def _rows(show: str = "all", day: str = "", status: str = "", limit: int = 500) -> list:
    """Newest first. `show` is all | ok | failed."""
    from db import get_db, get_cursor
    where, args = [], []
    if show == "ok":
        where.append("ok = TRUE")
    elif show == "failed":
        where.append("ok = FALSE")
    elif show == "writes":
        # Rows predating the method column are all reads, hence NOT NULL rather
        # than <> 'GET' alone — a NULL would otherwise be counted as a write.
        where.append("method IS NOT NULL AND method <> 'GET'")
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
        return _name_properties([dict(r) for r in cur.fetchall()])
    finally:
        cur.close(); conn.rollback(); conn.close()


def _name_properties(rows: list) -> list:
    """Fill r["ref_name"] with the house name for rows scoped to a property.

    Done here, on the admin page, and not in record(): record() runs on the hot
    path of every Breezeway call and is buffered precisely so it touches no I/O.
    A cache miss here costs one admin page view; there it would cost a scan."""
    try:
        from routes.briefing import _get_live_property_cache, _get_live_ref_cache
        names = _get_live_property_cache() or {}      # {property_id: house name}
        # {property_id: reference_property_id} — inverted, because the task API
        # logs the reference and the names are keyed by the Breezeway id.
        by_ref = {str(v): k for k, v in (_get_live_ref_cache() or {}).items()}
    except Exception:
        return rows                       # names are a nicety; the id still shows

    def _name(pid):
        return names.get(pid) or names.get(int(pid) if str(pid).isdigit() else pid)

    for r in rows:
        ref = str(r.get("ref") or "")
        label, _, ident = ref.partition(" ")
        if not ident:
            continue
        if ref.startswith("property ref "):
            ident = ref.split(" ", 2)[2]
            pid = by_ref.get(ident)
            nm = _name(pid) if pid is not None else None
        elif label == "property":
            nm = _name(ident)
        else:
            continue                      # a task id — no house name to give
        if nm:
            r["ref_name"] = nm
    return rows


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
