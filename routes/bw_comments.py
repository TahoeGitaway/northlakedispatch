"""
routes/bw_comments.py — Breezeway task-comment @mention alerts.

Receives Breezeway 'task' webhooks (which fire on task-comment-created among
other task events), matches each new comment against a set of name/keyword
rules, and drops a per-user banner alert for every matched recipient. Surfaced
in base.html via /api/bw-mentions — same shape as the PRI and Asana banners.

WHY A WEBHOOK (not polling): Breezeway exposes no notifications feed and no
per-user "mention" field. Polling would mean one comments API call per open
task per cycle (hundreds of calls, rate-limit territory). The 'task' webhook
pushes the whole task — with its comments — on each comment event, so we
subscribe once (POST /public/webhook/v1/subscribe, webhook_type "task") and do
all matching here.

WHY IT'S DEFENSIVE: Breezeway documents the TaskComment fields (comment / id /
created_at / comment_by) but ships no real example of the delivered envelope,
and the observer webhook re-sends the ENTIRE task (all its comments) on every
event. So:
  - _extract_comments is shape-tolerant (task.comments / top-level comments /
    single top-level comment / data.comments).
  - every raw payload is logged to bw_comment_webhook_log for inspection.
  - a freshness guard drops comments older than FRESH_DAYS so a new comment on
    an old task doesn't backfill its whole history as "new" alerts.
  - item_key "<comment_id>::<user_id>" + ON CONFLICT DO NOTHING dedupes
    re-delivery and never resurrects a dismissed alert.

MATCHING (per Madeline's spec):
  - generic: any active user notified when their full name or handle appears.
  - "@madelinegall" / "@madeline" / "madeline"  -> operations user + Madeline.
  - the words "schedule" / "logistics"          -> operations user + Madeline.
Unresolvable recipient matchers simply notify nobody — never an error.
"""

import os
import hmac
import hashlib
from datetime import datetime, timedelta

import requests
from flask import Blueprint, request, jsonify
from flask_login import login_required, current_user

from db import get_db, get_cursor
from routes.auth import admin_required

bw_comments_bp = Blueprint("bw_comments", __name__)

BW_BASE    = "https://api.breezeway.io"
FRESH_DAYS = 2   # ignore comments older than this (full task re-delivers history)

# Keyword / mention routing rules. `recipient_matchers` are matched
# case-insensitively against each active user's name (substring) or email
# (exact). These route to SPECIFIC people regardless of whose name is in the
# text — that's the "also notify me" behaviour Madeline asked for.
_KEYWORD_RULES = [
    {"label": "Madeline mentioned",
     "terms": ["@madelinegall", "@madeline", "madeline"],
     "recipient_matchers": ["madeline", "operations@tahoegetaways.com"]},
    {"label": "schedule / logistics",
     "terms": ["schedule", "logistics"],
     "recipient_matchers": ["madeline", "operations@tahoegetaways.com"]},
]


# ── secret / URL ──────────────────────────────────────────────────

def _webhook_secret() -> str:
    """Unguessable path token guarding the public receiver (Breezeway webhooks
    aren't signed). Stable without extra config: falls back to a hash of the
    Breezeway client secret so it survives restarts."""
    s = os.environ.get("BW_WEBHOOK_SECRET", "").strip()
    if s:
        return s
    seed = os.environ.get("BREEZEWAY_CLIENT_SECRET", "") or "bw-comment-fallback"
    return hashlib.sha256(("bwcw:" + seed).encode()).hexdigest()[:32]


def _webhook_url() -> str:
    from db import APP_BASE_URL
    return APP_BASE_URL.rstrip("/") + "/api/bw-comment-webhook/" + _webhook_secret()


# ── matching ──────────────────────────────────────────────────────

def _active_users(cur) -> list:
    cur.execute("SELECT id, name, email FROM users WHERE is_active=1")
    return [dict(r) for r in cur.fetchall()]


def _resolve_matchers(matchers, users) -> set:
    """User ids whose email == matcher or whose name contains matcher."""
    ids = set()
    for m in matchers:
        ml = (m or "").lower().strip()
        if not ml:
            continue
        for u in users:
            nm = (u.get("name") or "").lower()
            em = (u.get("email") or "").lower()
            if ml == em or ml in nm:
                ids.add(u["id"])
    return ids


def _user_handles(name):
    """Return (name_lower, tokens, {handles}) for a display name.
    Handles: "@fullnamenospace" and "@firstname" (e.g. @madelinegall, @madeline)."""
    nl = (name or "").lower().strip()
    tokens = [t for t in nl.split() if t]
    handles = set()
    if nl:
        handles.add("@" + nl.replace(" ", ""))
    if tokens:
        handles.add("@" + tokens[0])
    return nl, tokens, handles


def _match_recipients(text, users) -> dict:
    """comment text -> {user_id: reason_label}. First reason per user wins."""
    text_l = (text or "").lower()
    recips = {}

    # Generic: a user's full name (needs >=2 tokens to avoid common-first-name
    # noise) or an @handle appearing in the comment.
    for u in users:
        nl, tokens, handles = _user_handles(u.get("name"))
        if any(h in text_l for h in handles) or (len(tokens) >= 2 and nl and nl in text_l):
            recips.setdefault(u["id"], f"@{u.get('name')}")

    # Keyword / mention routing rules (also-notify behaviour).
    for rule in _KEYWORD_RULES:
        if any(term in text_l for term in rule["terms"]):
            for uid in _resolve_matchers(rule["recipient_matchers"], users):
                recips.setdefault(uid, rule["label"])

    return recips


# ── payload extraction ────────────────────────────────────────────

def _as_list(v):
    return v if isinstance(v, list) else ([v] if v else [])


def _extract_task_id(payload):
    for k in ("task_id", "taskId"):
        if payload.get(k):
            return payload.get(k)
    task = payload.get("task")
    if isinstance(task, dict):
        return task.get("id") or task.get("task_id")
    return payload.get("id")


def _extract_comments(payload) -> list:
    """Pull comment dicts out of whatever shape arrives. Returns normalized
    dicts: {id, comment, comment_by, created_at}."""
    if not isinstance(payload, dict):
        return []
    raw = []
    task = payload.get("task")
    if isinstance(task, dict):
        raw += _as_list(task.get("comments"))
    raw += _as_list(payload.get("comments"))
    data = payload.get("data")
    if isinstance(data, dict):
        raw += _as_list(data.get("comments"))
        if isinstance(data.get("task"), dict):
            raw += _as_list(data["task"].get("comments"))
    # Single top-level comment object.
    if payload.get("comment") is not None or payload.get("comment_by") is not None:
        raw.append({
            "id":         payload.get("id") or payload.get("comment_id"),
            "comment":    payload.get("comment"),
            "comment_by": payload.get("comment_by"),
            "created_at": payload.get("created_at") or payload.get("last_updated"),
        })

    norm = []
    for c in raw:
        if not isinstance(c, dict):
            continue
        norm.append({
            "id":         c.get("id") or c.get("comment_id"),
            "comment":    c.get("comment") if c.get("comment") is not None else c.get("text"),
            "comment_by": c.get("comment_by") or c.get("created_by") or c.get("author"),
            "created_at": c.get("created_at") or c.get("createdAt"),
        })
    return norm


def _commenter_name(cb) -> str:
    if isinstance(cb, dict):
        nm = cb.get("name") or " ".join(
            filter(None, [cb.get("first_name"), cb.get("last_name")]))
        return (nm or "Someone").strip()
    if isinstance(cb, str):
        return cb.strip() or "Someone"
    return "Someone"


def _is_fresh(created_at, now) -> bool:
    """True if the comment is recent enough to alert on. Missing/unparseable
    timestamps are treated as fresh (dedup still protects against duplicates)."""
    if not created_at:
        return True
    try:
        dt = datetime.fromisoformat(str(created_at).replace("Z", "+00:00"))
        return dt.replace(tzinfo=None) >= now - timedelta(days=FRESH_DAYS)
    except Exception:
        return True


# ── webhook receiver (public, secret-guarded) ─────────────────────

@bw_comments_bp.route("/api/bw-comment-webhook/<secret>", methods=["POST", "GET"])
def bw_comment_webhook(secret):
    # Constant-time compare of the path secret. Wrong secret looks like 404.
    if not hmac.compare_digest(secret, _webhook_secret()):
        return ("", 404)
    # Breezeway validates the URL on subscribe and requires a success code —
    # answer 200 to a bare probe.
    if request.method == "GET":
        return jsonify({"ok": True})

    raw     = request.get_data(as_text=True) or ""
    payload = request.get_json(force=True, silent=True) or {}
    now     = datetime.utcnow()
    now_iso = now.isoformat()

    conn = get_db()
    cur  = get_cursor(conn)

    # Always log the raw payload (capped) so real shapes can be inspected.
    try:
        cur.execute(
            "INSERT INTO bw_comment_webhook_log (received_at, event_type, payload) "
            "VALUES (%s,%s,%s)",
            (now_iso, str(payload.get("event_type") or payload.get("type") or "")[:80], raw[:8000]),
        )
        cur.execute(
            "DELETE FROM bw_comment_webhook_log WHERE id NOT IN "
            "(SELECT id FROM bw_comment_webhook_log ORDER BY id DESC LIMIT 100)"
        )
        conn.commit()
    except Exception:
        conn.rollback()

    try:
        users    = _active_users(cur)
        task_id  = _extract_task_id(payload)
        for c in _extract_comments(payload):
            cid  = c.get("id")
            text = (c.get("comment") or "")
            if cid is None or not text.strip():
                continue
            if not _is_fresh(c.get("created_at"), now):
                continue
            commenter = _commenter_name(c.get("comment_by"))
            recips    = _match_recipients(text, users)
            for uid, label in recips.items():
                # Never notify someone about their own comment.
                u = next((x for x in users if x["id"] == uid), None)
                if u and (u.get("name") or "").lower() == commenter.lower():
                    continue
                cur.execute(
                    """INSERT INTO bw_comment_alerts
                       (item_key, task_id, comment_id, recipient_user_id, commenter,
                        comment_text, matched_term, bw_created_at, created_at)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (item_key) DO NOTHING""",
                    (f"{cid}::{uid}", str(task_id or ""), str(cid), uid, commenter,
                     text[:2000], label, str(c.get("created_at") or ""), now_iso),
                )
        # Bound the table: drop alerts older than 30 days.
        cutoff = (now - timedelta(days=30)).isoformat()
        cur.execute("DELETE FROM bw_comment_alerts WHERE created_at < %s", (cutoff,))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[bw-comment-webhook] processing error: {e}")
    finally:
        cur.close()
        conn.close()

    # Always 200 so Breezeway doesn't disable the subscription.
    return jsonify({"ok": True})


# ── banner API (per-user) ─────────────────────────────────────────

@bw_comments_bp.route("/api/bw-mentions")
@login_required
def api_bw_mentions():
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        "SELECT item_key, task_id, commenter, comment_text, matched_term, bw_created_at "
        "FROM bw_comment_alerts "
        "WHERE recipient_user_id = %s AND dismissed_at IS NULL "
        "ORDER BY created_at DESC LIMIT 50",
        (current_user.id,),
    )
    rows = [dict(r) for r in cur.fetchall()]
    cur.close(); conn.rollback(); conn.close()
    return jsonify({"mentions": rows})


@bw_comments_bp.route("/api/bw-mention/dismiss", methods=["POST"])
@login_required
def api_bw_mention_dismiss():
    key = (request.get_json(force=True) or {}).get("key", "").strip()
    if not key:
        return jsonify({"error": "key required"}), 400
    conn = get_db()
    cur  = get_cursor(conn)
    # Scoped to the current user so nobody can dismiss someone else's alert.
    cur.execute(
        "UPDATE bw_comment_alerts SET dismissed_at=%s "
        "WHERE item_key=%s AND recipient_user_id=%s",
        (datetime.utcnow().isoformat(), key, current_user.id),
    )
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})


# ── admin: subscribe / status / test ──────────────────────────────

@bw_comments_bp.route("/admin/bw-comments/status")
@login_required
@admin_required
def bw_comments_status():
    from routes.briefing import _get_breezeway_token
    token = _get_breezeway_token()
    subs, err = None, None
    if token:
        try:
            r = requests.get(f"{BW_BASE}/public/webhook/v1/webhooks",
                             headers={"Authorization": f"JWT {token}"}, timeout=15)
            if r.ok:
                subs = r.json()
            else:
                err = f"HTTP {r.status_code}: {r.text[:300]}"
        except Exception as e:
            err = str(e)
    else:
        err = "No Breezeway token configured (BREEZEWAY_CLIENT_ID/SECRET)"
    return jsonify({"webhook_url": _webhook_url(), "subscriptions": subs, "error": err})


@bw_comments_bp.route("/admin/bw-comments/subscribe", methods=["POST"])
@login_required
@admin_required
def bw_comments_subscribe():
    from routes.briefing import _get_breezeway_token
    token = _get_breezeway_token()
    if not token:
        return jsonify({"error": "No Breezeway token configured"}), 400
    url = _webhook_url()
    if url.startswith("http://localhost") or url.startswith("http://127."):
        return jsonify({"error": f"APP_BASE_URL is not public ({url}); Breezeway "
                        "must be able to reach it. Set APP_BASE_URL first."}), 400
    try:
        r = requests.post(f"{BW_BASE}/public/webhook/v1/subscribe",
                          headers={"Authorization": f"JWT {token}"},
                          json={"url": url, "webhook_type": "task"}, timeout=20)
        try:
            body = r.json()
        except Exception:
            body = r.text[:500]
        return jsonify({"ok": r.ok, "status": r.status_code,
                        "webhook_url": url, "response": body}), (200 if r.ok else 400)
    except Exception as e:
        return jsonify({"error": str(e), "webhook_url": url}), 500


@bw_comments_bp.route("/admin/bw-comments/test", methods=["POST"])
@login_required
@admin_required
def bw_comments_test():
    """Inject a fake comment through the real matching path (no Breezeway call)
    so the banner can be verified end-to-end. Body: {text, commenter}."""
    body      = request.get_json(force=True, silent=True) or {}
    text      = (body.get("text") or "Test: please review the schedule").strip()
    commenter = (body.get("commenter") or "Test User").strip()
    now_iso   = datetime.utcnow().isoformat()

    conn = get_db()
    cur  = get_cursor(conn)
    users    = _active_users(cur)
    recips   = _match_recipients(text, users)
    fake_cid = "test-" + hashlib.sha256((text + now_iso).encode()).hexdigest()[:12]
    for uid, label in recips.items():
        u = next((x for x in users if x["id"] == uid), None)
        if u and (u.get("name") or "").lower() == commenter.lower():
            continue
        cur.execute(
            """INSERT INTO bw_comment_alerts
               (item_key, task_id, comment_id, recipient_user_id, commenter,
                comment_text, matched_term, bw_created_at, created_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (item_key) DO NOTHING""",
            (f"{fake_cid}::{uid}", "0", fake_cid, uid, commenter, text[:2000],
             label, now_iso, now_iso),
        )
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True, "matched_recipients": len(recips),
                    "matched_terms": sorted(set(recips.values()))})
