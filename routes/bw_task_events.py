"""
routes/bw_task_events.py — raw Breezeway 'task' webhook capture.

A TEMPORARY DIAGNOSTIC, not a feature. It exists to answer one question with
evidence instead of inference: what does Breezeway actually put in a task
webhook payload, and above all what shape is `status`?

WHY THIS EXISTS: Breezeway's 2026-09-02 doc correction disclosed that task
completion arrives as a `task-updated` event and that the task status rides in
the payload. That is the data seven per-property fan-outs currently spend ~442
API calls per full-day view to re-derive (see bw_ratelimit.py). Before any of
that is rebuilt around push, we need a real envelope in hand — the docs
describe `status` as a TypeTaskStatus object with code/id/name, and nobody here
has ever seen one.

WHY A SEPARATE RECEIVER: the app already has a Breezeway 'task' webhook
receiver, and it is the comment @mention feature. Subscribing to that endpoint
would switch on user-facing banner alerts as a side effect of wanting to read a
payload. So this is deliberately its own everything — own path, own secret, own
env var, own table, own blueprint — and imports nothing from that module. The
two derived path secrets are kept apart only by their hash prefix ("bwte:" here
vs "bwcw:" there); do not copy that prefix from the other file.

WHY SAMPLING, NOT EVERYTHING: this account's task feed is already consumed by a
third-party vendor and is high-traffic, and task-updated will dominate it. A
plain newest-N cap would evict the rare events we most want (task-started,
task-paused, task-cost-updated) within minutes. So the quota is PER event name,
and once an event type has its quota we keep at most one sample a minute. A
sampled-out delivery returns before touching the database at all — get_db()
opens a fresh connection with no pool, so that fast path is the difference
between a connect per delivery and roughly one per event name per minute.

ALWAYS 200: Breezeway deactivates a subscription after a 3-day grace period
with 10+ consecutive failures. Every path here — kill switch, parse failure,
unhandled exception — returns 200, so a bug in this file cannot silently kill
the subscription.

TO REMOVE ENTIRELY: delete this file, the two lines in app.py, and the
bw_task_event_log block in db.py; then DELETE the subscription via
  DELETE https://api.breezeway.io/public/webhook/v1/unsubscribe/{id}
checking the URL of the id first — the account has other task subscriptions.
Optionally DROP TABLE bw_task_event_log.
"""

import os
import json
import hmac
import time
import hashlib
import threading
from datetime import datetime, timedelta, timezone

from flask import Blueprint, request, jsonify
from flask_login import login_required

from db import get_db, get_cursor
from routes.auth import admin_required
from routes.bw_api_log import bw_get

bw_task_events_bp = Blueprint("bw_task_events", __name__)

BW_BASE           = "https://api.breezeway.io"
WEBHOOK_PATH      = "/api/bw-task-event/"   # the rollback guard matches on this
PER_EVENT_KEEP    = 25      # rows retained per distinct event name
SAMPLE_INTERVAL_S = 60      # once the quota is full, at most one sample a minute
MAX_PAYLOAD_CHARS = 16000   # truncation; the true length is kept in payload_bytes
MAX_AGE_DAYS      = 14      # self-expiry, pruned opportunistically
PRUNE_AGE_EVERY_S = 3600


# ── secret / URL ──────────────────────────────────────────────────

def _webhook_secret() -> str:
    """Independent of the comment receiver's secret ON PURPOSE: its own env var,
    and a fallback seeded with a different prefix so the two paths can never
    collide even though they share BREEZEWAY_CLIENT_SECRET. Derivable so that
    production needs no new env var for this to work."""
    s = os.environ.get("BW_TASK_EVENT_SECRET", "").strip()
    if s:
        return s
    seed = os.environ.get("BREEZEWAY_CLIENT_SECRET", "") or "bw-task-event-fallback"
    return hashlib.sha256(("bwte:" + seed).encode()).hexdigest()[:32]


def _secret_source() -> str:
    return "env" if os.environ.get("BW_TASK_EVENT_SECRET", "").strip() else "derived"


def _webhook_url() -> str:
    from db import APP_BASE_URL
    return APP_BASE_URL.rstrip("/") + WEBHOOK_PATH + _webhook_secret()


def _enabled() -> bool:
    return os.environ.get("BW_TASK_EVENTS_ENABLED", "1").strip() != "0"


# ── payload extraction (total — these never raise) ────────────────

_EVENT_KEYS = ("event", "event_type", "type", "event_name", "action")


def _first_str(d, keys) -> str:
    if not isinstance(d, dict):
        return ""
    for k in keys:
        v = d.get(k)
        if v not in (None, "", [], {}) and not isinstance(v, (dict, list)):
            return str(v).strip()[:80]
    return ""


def _event_name(payload) -> str:
    """Breezeway's own subscribe-time test call sends {"event": ...}, so that key
    is tried first. Empty becomes a visible bucket rather than vanishing."""
    name = _first_str(payload, _EVENT_KEYS)
    if not name and isinstance(payload, dict):
        name = _first_str(payload.get("data"), _EVENT_KEYS)
    return name or "(none)"


def _task_id(payload) -> str:
    """The task webhook pushes the whole task, so a top-level id is the task."""
    if not isinstance(payload, dict):
        return ""
    for k in ("task_id", "taskId", "id"):
        v = payload.get(k)
        if v not in (None, "") and not isinstance(v, (dict, list)):
            return str(v)[:64]
    for parent in ("task", "data"):
        node = payload.get(parent)
        if isinstance(node, dict):
            for k in ("id", "task_id"):
                v = node.get(k)
                if v not in (None, "") and not isinstance(v, (dict, list)):
                    return str(v)[:64]
            inner = node.get("task")
            if isinstance(inner, dict) and inner.get("id") not in (None, ""):
                return str(inner["id"])[:64]
    return ""


def _status_node(payload):
    """status may sit at the top level, under task, or under data."""
    if not isinstance(payload, dict):
        return None
    if "status" in payload:
        return payload.get("status")
    for parent in ("task", "data"):
        node = payload.get(parent)
        if isinstance(node, dict) and "status" in node:
            return node.get("status")
    return None


def _status_fields(payload):
    """-> (raw_type, code, name, id). THE question this table exists to answer:
    the docs say TypeTaskStatus {code,id,name}; spi.py assumes a bare string."""
    node = _status_node(payload)
    if node is None:
        return ("missing", "", "", None)
    if isinstance(node, dict):
        sid = node.get("id")
        try:
            sid = int(sid)
        except (TypeError, ValueError):
            sid = None
        return ("object",
                str(node.get("code") or "")[:64],
                str(node.get("name") or "")[:64],
                sid)
    if isinstance(node, bool):
        return ("other:bool", str(node), "", None)
    if isinstance(node, int):
        return ("int", str(node), "", node)
    if isinstance(node, str):
        return ("string", node[:64], "", None)
    return (f"other:{type(node).__name__}", "", "", None)


# ── sampling (in-process; Procfile pins --workers 1) ──────────────

_lock       = threading.Lock()
_seen       = {}    # event name -> TRUE deliveries since boot
_stored     = {}    # event name -> rows actually written since boot
_last_store = {}    # event name -> monotonic ts of the last stored row
_last_age_prune = 0.0


def _note_and_decide(name: str) -> bool:
    """Count every delivery, then decide whether to store this one. A brand-new
    event name is always stored immediately, however loud task-updated is."""
    now = time.monotonic()
    with _lock:
        _seen[name] = _seen.get(name, 0) + 1
        if _stored.get(name, 0) < PER_EVENT_KEEP:
            _stored[name] = _stored.get(name, 0) + 1
            _last_store[name] = now
            return True
        if now - _last_store.get(name, 0.0) >= SAMPLE_INTERVAL_S:
            _stored[name] = _stored.get(name, 0) + 1
            _last_store[name] = now
            return True
        return False


# ── receiver (public, secret-guarded) ─────────────────────────────

@bw_task_events_bp.route("/api/bw-task-event/<secret>", methods=["POST", "GET"])
def bw_task_event(secret):
    # Constant-time compare. A wrong secret is indistinguishable from a 404.
    if not hmac.compare_digest(secret, _webhook_secret()):
        return ("", 404)

    # Kill switch: stop writing without a deploy, while still answering 200 so
    # the subscription stays healthy.
    if not _enabled():
        return jsonify({"ok": True})

    # Breezeway validates the URL on subscribe and requires a success code.
    if request.method == "GET":
        return jsonify({"ok": True})

    raw     = request.get_data(as_text=True) or ""
    payload = request.get_json(force=True, silent=True) or {}
    name    = _event_name(payload)

    if not _note_and_decide(name):
        return jsonify({"ok": True})    # sampled out — no database work at all

    task_id = _task_id(payload)
    raw_type, code, sname, sid = _status_fields(payload)
    body = raw[:MAX_PAYLOAD_CHARS]

    conn = None
    try:
        conn = get_db()
        cur  = get_cursor(conn)
        cur.execute(
            """INSERT INTO bw_task_event_log
               (received_at, event_name, task_id, status_raw_type, status_code,
                status_name, status_id, payload_bytes, truncated, payload)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (datetime.now(timezone.utc).isoformat(), name, task_id, raw_type,
             code, sname, sid, len(raw), len(raw) > MAX_PAYLOAD_CHARS, body),
        )
        # Scoped to one event name so the (event_name, id DESC) index covers it,
        # and only ever runs on a row we actually stored.
        cur.execute(
            """DELETE FROM bw_task_event_log
                WHERE event_name = %s
                  AND id NOT IN (SELECT id FROM bw_task_event_log
                                  WHERE event_name = %s
                               ORDER BY id DESC LIMIT %s)""",
            (name, name, PER_EVENT_KEEP),
        )
        global _last_age_prune
        if time.time() - _last_age_prune > PRUNE_AGE_EVERY_S:
            cutoff = (datetime.now(timezone.utc)
                      - timedelta(days=MAX_AGE_DAYS)).isoformat()
            cur.execute("DELETE FROM bw_task_event_log WHERE received_at < %s", (cutoff,))
            _last_age_prune = time.time()
        conn.commit()
        cur.close()
    except Exception as e:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        print(f"[bw-task-event] capture error: {e}")
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    # Always 200 so Breezeway doesn't deactivate the subscription.
    return jsonify({"ok": True})


# ── admin: status / summary (browser-only; admin_required redirects) ──

@bw_task_events_bp.route("/admin/bw-task-events/status.json")
@login_required
@admin_required
def bw_task_events_status():
    """Where to read our webhook URL without echoing it into a shell, plus the
    live subscription list with ours tagged."""
    from routes.briefing import _get_breezeway_token
    url   = _webhook_url()
    token = _get_breezeway_token()
    subs, err = None, None
    if token:
        try:
            r = bw_get(f"{BW_BASE}/public/webhook/v1/webhooks",
                       headers={"Authorization": f"JWT {token}"}, timeout=15)
            if r.ok:
                subs = [{**s, "ours": WEBHOOK_PATH in str(s.get("url") or "")}
                        for s in (r.json() or [])]
            else:
                err = f"HTTP {r.status_code}: {r.text[:300]}"
        except Exception as e:
            err = str(e)
    else:
        err = "No Breezeway token configured (BREEZEWAY_CLIENT_ID/SECRET)"
    return jsonify({
        "webhook_url":   url,
        "secret_source": _secret_source(),
        "enabled":       _enabled(),
        "subscribed":    bool(subs) and any(s["ours"] for s in subs),
        "subscriptions": subs,
        "error":         err,
    })


@bw_task_events_bp.route("/admin/bw-task-events/summary.json")
@login_required
@admin_required
def bw_task_events_summary():
    """What has actually arrived, and what shape `status` really is."""
    conn = get_db()
    cur  = get_cursor(conn)
    try:
        cur.execute(
            """SELECT event_name, COUNT(*) AS stored,
                      MIN(received_at) AS first_seen, MAX(received_at) AS last_seen
                 FROM bw_task_event_log GROUP BY event_name ORDER BY stored DESC"""
        )
        counts = [dict(r) for r in cur.fetchall()]

        cur.execute(
            """SELECT event_name, status_raw_type, status_code, status_name,
                      COUNT(*) AS n
                 FROM bw_task_event_log
             GROUP BY 1,2,3,4 ORDER BY event_name, n DESC"""
        )
        shapes = [dict(r) for r in cur.fetchall()]

        cur.execute(
            """SELECT DISTINCT ON (event_name)
                      event_name, id, received_at, task_id, status_raw_type,
                      status_code, status_name, status_id, payload_bytes,
                      truncated, payload
                 FROM bw_task_event_log ORDER BY event_name, id DESC"""
        )
        samples = {r["event_name"]: dict(r) for r in cur.fetchall()}
    finally:
        cur.close(); conn.rollback(); conn.close()

    events = []
    for c in counts:
        name = c["event_name"]
        events.append({
            **c,
            "status_shapes": [
                {k: v for k, v in s.items() if k != "event_name"}
                for s in shapes if s["event_name"] == name
            ],
            "sample": samples.get(name),
        })

    with _lock:
        live = dict(sorted(_seen.items(), key=lambda kv: -kv[1]))

    return jsonify({
        "webhook_url":             _webhook_url(),
        "enabled":                 _enabled(),
        "stored_rows":             sum(c["stored"] for c in counts),
        "distinct_events":         len(counts),
        "live_seen_since_restart": live,
        "events":                  events,
    })
