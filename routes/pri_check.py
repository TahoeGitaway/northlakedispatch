"""
routes/pri_check.py — PRI (Post-Reservation Inspection) check.

Standalone blueprint. Zero caching — always fetches live from Breezeway.
Endpoints:
  GET  /briefing/pri-check       — full PRI scan page
  GET  /api/pri-alerts           — banner alerts for nav
  POST /api/pri-alert/dismiss    — dismiss a banner alert
  POST /api/cron/pri-check       — cron refresh (secured by CRON_SECRET)
"""

import os
from datetime import date as date_cls, datetime, timedelta

from flask import Blueprint, jsonify, request
from flask_login import login_required, current_user

from db import get_db, get_cursor

pri_bp = Blueprint("pri", __name__)


def _shared():
    """Lazy import shared Breezeway helpers from briefing to avoid circular imports."""
    from routes.briefing import (
        _get_breezeway_token,
        _fetch_bw_reservations,
        _classify_reservation,
        _extract_str,
        _get_property_name,
    )
    return (
        _get_breezeway_token,
        _fetch_bw_reservations,
        _classify_reservation,
        _extract_str,
        _get_property_name,
    )


@pri_bp.route("/briefing/pri-check")
@login_required
def pri_check():
    """Scan short-term guest checkouts from 30 days ago through the forward window for PRI needs.

    PRI required when a short-term guest (<30 days) checks out AND:
      - The immediately next reservation at that property is OWNER or BLOCK
        → needs "owner next" tag in Breezeway (or already tagged = done)
      - OR there is no upcoming reservation within 60 days of that checkout date
        → vacancy PRI must be created manually by ops
    """
    _get_breezeway_token, _fetch_bw_reservations, _classify_reservation, _extract_str, _get_property_name = _shared()

    today = date_cls.today()

    # Caller picks an explicit checkout window (From / To). Defaults: today → +30 days.
    start_param = request.args.get("start_date")
    end_param   = request.args.get("end_date")
    try:
        win_start = date_cls.fromisoformat(start_param) if start_param else today
    except Exception:
        win_start = today
    try:
        win_end = date_cls.fromisoformat(end_param) if end_param else win_start + timedelta(days=30)
    except Exception:
        win_end = win_start + timedelta(days=30)
    if win_end < win_start:
        win_start, win_end = win_end, win_start

    lookback_start    = win_start                        # checkout window lower bound (From)
    reso_lookback     = win_start - timedelta(days=180)  # wider window for upcoming — catches long owner stays
    report_end        = win_end                          # checkout window upper bound (To)
    far_end           = win_end + timedelta(days=150)

    token = _get_breezeway_token()
    if not token:
        return jsonify({"error": "Breezeway not configured."}), 500

    lookback_str      = lookback_start.isoformat()
    reso_lookback_str = reso_lookback.isoformat()
    today_str         = today.isoformat()
    report_end_str    = report_end.isoformat()
    far_end_str       = far_end.isoformat()

    raw_checkouts = _fetch_bw_reservations(token, {
        "checkout_date_ge": lookback_str,
        "checkout_date_le": report_end_str,
    })
    raw_upcoming = _fetch_bw_reservations(token, {
        "checkin_date_ge": reso_lookback_str,
        "checkin_date_le": far_end_str,
    })

    checkouts = [
        r for r in raw_checkouts
        if lookback_str <= (r.get("checkout_date") or "")[:10] <= report_end_str
    ]

    by_prop = {}
    for r in raw_upcoming:
        pid = r.get("property_id")
        if pid:
            by_prop.setdefault(pid, []).append(r)
    for pid in by_prop:
        by_prop[pid].sort(key=lambda r: r.get("checkin_date", ""))

    needs_tag    = []
    already_done = []
    no_booking   = []

    for co in checkouts:
        if _classify_reservation(co) != "guest":
            continue
        pid = co.get("property_id")
        if not pid:
            continue
        co_date_str = (co.get("checkout_date") or "")[:10]
        try:
            co_date = date_cls.fromisoformat(co_date_str)
        except Exception:
            continue

        prop_name = _get_property_name(pid)

        next_r = next_ci_date = None
        for r in by_prop.get(pid, []):
            ci_str = (r.get("checkin_date") or "")[:10]
            try:
                ci_date = date_cls.fromisoformat(ci_str)
            except Exception:
                continue
            if ci_date >= co_date:
                next_r       = r
                next_ci_date = ci_date
                break

        vacancy_cutoff = co_date + timedelta(days=60)
        if not next_r or not next_ci_date or next_ci_date > vacancy_cutoff:
            no_booking.append({
                "property":      prop_name,
                "checkout_date": co_date_str,
                "vacancy_days":  60,
            })
            continue

        next_kind = _classify_reservation(next_r)
        if next_kind not in ("owner", "block"):
            continue

        if next_ci_date < today:
            continue

        tag_names = [_extract_str(t) for t in (next_r.get("tags") or [])]
        tagged    = "owner next" in tag_names

        gap_days = (next_ci_date - co_date).days
        entry = {
            "property":      prop_name,
            "checkout_date": co_date_str,
            "next_checkin":  next_ci_date.isoformat(),
            "next_type":     next_kind,
            "vacancy_days":  gap_days if gap_days >= 30 else None,
        }
        (already_done if tagged else needs_tag).append(entry)

    needs_tag.sort(key=lambda r: r["checkout_date"])
    already_done.sort(key=lambda r: r["checkout_date"])
    no_booking.sort(key=lambda r: r["checkout_date"])

    return jsonify({
        "needs_tag":       needs_tag,
        "already_tagged":  already_done,
        "no_booking":      no_booking,
        "scanned_from":    lookback_str,
        "scanned_through": report_end_str,
    })


def refresh_pri_banner_alerts(alert_days=3):
    """Recompute PRI banner alerts for the next `alert_days` days and write to DB.
    Called daily by the scheduler and on-demand via admin route.
    Preserves dismissed status — only upserts metadata, never clears dismissed_at.
    """
    _get_breezeway_token, _fetch_bw_reservations, _classify_reservation, _extract_str, _get_property_name = _shared()

    token = _get_breezeway_token()
    if not token:
        return

    today         = date_cls.today()
    window_end    = today + timedelta(days=alert_days)
    far_end       = today + timedelta(days=150)
    reso_lookback = today - timedelta(days=30)
    today_str     = today.isoformat()

    raw_checkouts = _fetch_bw_reservations(token, {
        "checkout_date_ge": today_str,
        "checkout_date_le": window_end.isoformat(),
    })
    raw_upcoming = _fetch_bw_reservations(token, {
        "checkin_date_ge": reso_lookback.isoformat(),
        "checkin_date_le": far_end.isoformat(),
    })

    checkouts = [
        r for r in raw_checkouts
        if today_str <= (r.get("checkout_date") or "")[:10] <= window_end.isoformat()
    ]

    by_prop = {}
    for r in raw_upcoming:
        pid = r.get("property_id")
        if pid:
            by_prop.setdefault(pid, []).append(r)
    for pid in by_prop:
        by_prop[pid].sort(key=lambda r: r.get("checkin_date", ""))

    active_keys    = set()
    rows_to_upsert = []

    for co in checkouts:
        if _classify_reservation(co) != "guest":
            continue
        pid = co.get("property_id")
        if not pid:
            continue
        co_date_str = (co.get("checkout_date") or "")[:10]
        try:
            co_date = date_cls.fromisoformat(co_date_str)
        except Exception:
            continue

        prop_name = _get_property_name(pid)

        next_r = next_ci_date = None
        for r in by_prop.get(pid, []):
            ci_str = (r.get("checkin_date") or "")[:10]
            try:
                ci_date = date_cls.fromisoformat(ci_str)
            except Exception:
                continue
            if ci_date >= co_date:
                next_r, next_ci_date = r, ci_date
                break

        if not next_r:
            key = f"{prop_name}::{co_date_str}"
            active_keys.add(key)
            rows_to_upsert.append((key, prop_name, co_date_str, None, "vacancy_pri"))
            continue

        next_kind = _classify_reservation(next_r)
        if next_kind not in ("owner", "block"):
            continue
        if next_ci_date < today:
            continue

        tag_names = [_extract_str(t) for t in (next_r.get("tags") or [])]
        if "owner next" in tag_names:
            continue

        key = f"{prop_name}::{co_date_str}::on"
        active_keys.add(key)
        rows_to_upsert.append((key, prop_name, co_date_str, next_ci_date.isoformat(), "needs_owner_next"))

    now  = datetime.utcnow().isoformat()
    conn = get_db()
    cur  = get_cursor(conn)
    try:
        for (key, prop, co, nci, atype) in rows_to_upsert:
            cur.execute(
                """INSERT INTO pri_banner_alerts
                       (item_key, property_name, checkout_date, next_checkin, alert_type, created_at)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON CONFLICT (item_key) DO UPDATE SET
                       property_name = EXCLUDED.property_name,
                       checkout_date = EXCLUDED.checkout_date,
                       next_checkin  = EXCLUDED.next_checkin,
                       alert_type    = EXCLUDED.alert_type""",
                (key, prop, co, nci, atype, now),
            )
        if active_keys:
            placeholders = ",".join(["%s"] * len(active_keys))
            cur.execute(
                f"DELETE FROM pri_banner_alerts WHERE item_key NOT IN ({placeholders}) "
                "AND dismissed_at IS NULL",
                list(active_keys),
            )
        else:
            cur.execute("DELETE FROM pri_banner_alerts WHERE dismissed_at IS NULL")
        cutoff = (today - timedelta(days=7)).isoformat()
        cur.execute("DELETE FROM pri_banner_alerts WHERE checkout_date < %s", (cutoff,))
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        cur.close()
        conn.close()


@pri_bp.route("/api/pri-alerts")
@login_required
def api_pri_alerts():
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        "SELECT item_key, property_name, checkout_date, next_checkin, alert_type "
        "FROM pri_banner_alerts WHERE dismissed_at IS NULL "
        "ORDER BY checkout_date ASC"
    )
    alerts = [dict(r) for r in cur.fetchall()]
    cur.close(); conn.rollback(); conn.close()
    return jsonify({"alerts": alerts})


@pri_bp.route("/api/pri-alert/dismiss", methods=["POST"])
@login_required
def api_pri_alert_dismiss():
    key = (request.get_json(force=True) or {}).get("key", "").strip()
    if not key:
        return jsonify({"error": "key required"}), 400
    now  = datetime.utcnow().isoformat()
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        "UPDATE pri_banner_alerts SET dismissed_at=%s, dismissed_by=%s WHERE item_key=%s",
        (now, current_user.id, key),
    )
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})


@pri_bp.route("/api/pri-debug")
@login_required
def pri_debug():
    """
    Diagnostic: show every Breezeway reservation for a named property,
    plus what the PRI logic sees when it processes that property's checkouts.

    Usage: /api/pri-debug?name=Ember+Ridge+Retreat
    """
    from routes.briefing import (
        _get_breezeway_token, _fetch_bw_reservations,
        _classify_reservation, _get_property_name,
        _ensure_property_cache, _get_live_property_cache,
    )

    prop_name_query = (request.args.get("name") or "").strip().lower()
    if not prop_name_query:
        return jsonify({"error": "name param required"}), 400

    token = _get_breezeway_token()
    if not token:
        return jsonify({"error": "Breezeway not configured"}), 500

    _ensure_property_cache()
    prop_cache = _get_live_property_cache()  # {bw_id: bw_name}

    # Find property id(s) matching the name
    matched = {pid: name for pid, name in prop_cache.items()
               if prop_name_query in name.lower()}
    if not matched:
        return jsonify({"error": f"No property matching '{prop_name_query}'",
                        "all_names": list(prop_cache.values())}), 404

    today    = date_cls.today()
    far_end  = today + timedelta(days=150)
    lookback = today - timedelta(days=60)

    pids = set(matched.keys())

    # Single query: all reservations for these properties, wide window, by checkin date
    all_resos = _fetch_bw_reservations(token, {
        "checkin_date_ge": lookback.isoformat(),
        "checkin_date_le": far_end.isoformat(),
    })

    resos_for_prop = [r for r in all_resos if r.get("property_id") in pids]
    resos_for_prop.sort(key=lambda r: r.get("checkin_date") or "")

    summarised = []
    for r in resos_for_prop:
        summarised.append({
            "checkin_date":     r.get("checkin_date"),
            "checkout_date":    r.get("checkout_date"),
            "classified_as":    _classify_reservation(r),
            "type_stay":        r.get("type_stay"),
            "type_reservation": r.get("type_reservation"),
            "tags":             [t.get("name") for t in (r.get("tags") or [])],
            "property_id":      r.get("property_id"),
            "id":               r.get("id"),
        })

    return jsonify({
        "matched_properties":    matched,
        "query_date":            today.isoformat(),
        "reservations":          summarised,
        "total_fetched_all_props": len(all_resos),
        "found_for_this_prop":   len(summarised),
    })


@pri_bp.route("/api/cron/pri-check", methods=["POST"])
def cron_pri_check():
    """Unauthenticated cron endpoint — secured by Bearer token in CRON_SECRET env var."""
    secret = os.environ.get("CRON_SECRET", "").strip()
    if not secret:
        return jsonify({"error": "CRON_SECRET not configured on server"}), 500
    auth = request.headers.get("Authorization", "")
    if auth != f"Bearer {secret}":
        return jsonify({"error": "unauthorized"}), 401
    try:
        refresh_pri_banner_alerts(alert_days=3)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
