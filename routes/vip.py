"""
routes/vip.py — VIP reservation tracker (TEMPORARY, ~one month).

A standalone checklist + per-reservation notes for the special VIP arrivals.
State persists in the vip_tracker table (shared across the team) until the page
is deleted. To remove the whole feature later: delete this file + templates/vip.html,
drop its blueprint from app.py, and (optionally) DROP TABLE vip_tracker.

Endpoints:
  GET  /vip        — page
  GET  /vip/state  — saved {item_key: {done, notes, updated_at}}
  POST /vip/save   — upsert one reservation's {done, notes}
"""

from datetime import datetime, date as _date, timedelta as _td
from zoneinfo import ZoneInfo

from flask import Blueprint, render_template, request, jsonify, Response
from flask_login import login_required, current_user

from db import get_db, get_cursor

vip_bp = Blueprint("vip", __name__)

# These reservations are all 2026 (the tracker's window).
_VIP_YEAR = 2026

# The business runs on Pacific time; collapse ("check-in passed") and the
# after-checkout purge both key off the Pacific calendar day so a card isn't
# collapsed or deleted early just because a server clock is in UTC.
_PACIFIC = ZoneInfo("America/Los_Angeles")


def _today_pacific():
    return datetime.now(_PACIFIC).date()


def _match_room_to_pid(room, prop_cache):
    """Match a reservation room name to a Breezeway property id (cache: {pid: name})."""
    import difflib
    key = (room or "").lower().strip()
    if not key:
        return None
    by_name = {(name or "").lower().strip(): pid for pid, name in prop_cache.items()}
    if key in by_name:
        return by_name[key]
    pk = " " + key + " "                       # word-aligned containment
    for nm, pid in by_name.items():
        pnm = " " + nm + " "
        if pk in pnm or pnm in pk:
            return pid
    hits = difflib.get_close_matches(key, list(by_name.keys()), n=1, cutoff=0.8)
    return by_name[hits[0]] if hits else None


def _task_status(t):
    raw = t.get("type_task_status")
    if isinstance(raw, dict):
        raw = raw.get("name") or raw.get("code") or ""
    raw = str(raw or t.get("status") or "").lower()
    if raw in ("complete", "completed", "done", "finished", "approved"):
        return "✓ Complete"
    if raw in ("in_progress", "in progress", "started"):
        return "🔄 In progress"
    return "⏳ Pending"


def _assignees(t):
    out = []
    for a in (t.get("assignments") or []):
        n = (a.get("name") or a.get("full_name") or
             f"{a.get('first_name','').strip()} {a.get('last_name','').strip()}".strip())
        if n:
            out.append(n)
    return out


def _guest_name(r):
    g = (r.get("guest_name") or r.get("primary_guest") or r.get("name") or "")
    if isinstance(g, dict):
        return str(g.get("name") or
                   f"{g.get('first_name','').strip()} {g.get('last_name','').strip()}".strip())
    return str(g)


def _res_guest(r):
    """Lead-guest name off a Breezeway reservation. The `guests` array holds
    contact records (first/last name); fall back to the generic extractor."""
    gs = r.get("guests")
    if isinstance(gs, list) and gs:
        g = gs[0] or {}
        nm = f"{(g.get('first_name') or '').strip()} {(g.get('last_name') or '').strip()}".strip()
        if nm:
            return nm
    return _guest_name(r)


def _norm_room(s):
    return " ".join((s or "").lower().split())


def _ci_iso(ci):
    """'M/D' (in _VIP_YEAR) -> 'YYYY-MM-DD'; '' on bad input."""
    try:
        mm, dd = str(ci).split("/")[:2]
        return _date(_VIP_YEAR, int(mm), int(dd)).isoformat()
    except Exception:
        return ""


def _iso_to_md(iso):
    """'YYYY-MM-DD' -> 'M/D' (the card's display form)."""
    try:
        d = _date.fromisoformat((iso or "")[:10])
        return f"{d.month}/{d.day}"
    except Exception:
        return iso or ""


def _checkout_iso_of(checkin_iso, nights):
    """Checkout date = check-in + nights. '' if it can't be computed."""
    try:
        return (_date.fromisoformat((checkin_iso or "")[:10]) + _td(days=int(nights or 0))).isoformat()
    except Exception:
        return ""


def _purge_departed(cur):
    """Permanently delete cards whose checkout is strictly before today (Pacific),
    along with their notes (vip_comments) and inspected-state (vip_tracker).

    Per Madeline's explicit choice: once a reservation is fully over, its tile is
    removed for good — this is irreversible, and there is no email backup, so the
    Export button is the way to keep a copy first. FAIL-SAFE: a row whose checkout
    date can't be computed is NEVER deleted. Returns the purged item_keys."""
    today = _today_pacific().isoformat()
    cur.execute("SELECT item_key, checkin_iso, nights FROM vip_reservations")
    gone = [r["item_key"] for r in cur.fetchall()
            if (_checkout_iso_of(r["checkin_iso"], r["nights"]) or today) < today]
    for k in gone:
        cur.execute("DELETE FROM vip_comments     WHERE item_key = %s", (k,))
        cur.execute("DELETE FROM vip_tracker      WHERE item_key = %s", (k,))
        cur.execute("DELETE FROM vip_reservations WHERE item_key = %s", (k,))
    return gone


# The original 27 hand-curated cards. Migrated into vip_reservations ONCE under
# these exact vip-NN keys so their inspected-state (vip_tracker) and notes
# (vip_comments) — both keyed by item_key — stay attached.
# (key, guest, ci, nights, co, guests, room, total, blue, first)
_SEED = [
    ("vip-01", "Sachin Rajpal",      "6/24", 31, "7/25", "5 | 3",  "Fleur Du Lac 18",                        "$53,250.00", True,  False),
    ("vip-02", "Kristina Klimaitis",  "6/24", 7,  "7/1",  "8 | 5",  "Beyond The Blue Lakefront Escape",       "$41,235.00", False, False),
    ("vip-03", "Jason Van Voorhis",   "6/24", 52, "8/15", "2 | 1",  "Clearwater Lake View",                   "$32,000.00", False, True),
    ("vip-04", "Sherry Saxton",       "6/25", 7,  "7/2",  "4 | 2",  "Towering Pines Lakefront",               "$17,539.30", False, False),
    ("vip-05", "Dan Carvalho",        "6/26", 42, "8/7",  "2 | 0",  "Rockwood Lodge at Lahontan",             "$64,500.00", True,  False),
    ("vip-06", "John Lupusor",        "6/26", 31, "7/27", "2 | 4",  "Fleur Du Lac 16",                        "$54,250.00", True,  False),
    ("vip-07", "Maya Leabman",        "6/26", 31, "7/27", "3 | 1",  "Glenbrook at Martis Camp",               "$47,845.00", True,  False),
    ("vip-08", "Linda Platt",         "6/27", 8,  "7/5",  "8 | 4",  "Sky Rocks",                              "$22,792.06", False, False),
    ("vip-09", "Traci Thomas",        "6/28", 7,  "7/5",  "6 | 4",  "Beach Haven Lakefront",                  "$19,890.26", False, False),
    ("vip-10", "Josh Thornton",       "6/29", 7,  "7/6",  "2 | 3",  "Cathedral Pines Lakefront",              "$43,221.10", False, False),
    ("vip-11", "Rachael Fry",         "6/29", 16, "7/15", "8 | 2",  "Tahoe Point of View",                    "$19,125.37", False, False),
    ("vip-12", "Brian Fyda",          "7/1",  11, "7/12", "8 | 0",  "Aqua Vista Lakefront Estate",            "$71,700.75", False, False),
    ("vip-13", "Jennifer Barsotti",   "7/1",  33, "8/3",  "8 | 0",  "Fleur Du Lac 7",                         "$78,650.00", True,  False),
    ("vip-14", "Yvonne Valiquette",   "7/1",  7,  "7/8",  "12 | 2", "Beyond The Blue Lakefront Escape",       "$66,223.32", False, False),
    ("vip-15", "Carl Hansen",         "7/1",  7,  "7/8",  "6 | 4",  "Glistening Shores Lakefront",            "$56,047.00", True,  False),
    ("vip-16", "Sara Doughty",        "7/1",  31, "8/1",  "3 | 0",  "Crown Peak at Olympic Valley",           "$23,300.00", False, True),
    ("vip-17", "Rebecca Gruss",       "7/2",  8,  "7/10", "6 | 0",  "Sapphire Shores Lakefront",              "$20,231.71", False, False),
    ("vip-18", "Andrew Hinkelman",    "7/3",  7,  "7/10", "12 | 0", "Valhalla Lakefront on the West Shore",   "$36,599.68", False, False),
    ("vip-19", "Tami Campbell",       "7/6",  5,  "7/11", "8 | 3",  "Cathedral Pines Lakefront",              "$23,041.72", False, False),
    ("vip-20", "Jane Etcheverry",     "7/8",  7,  "7/15", "10 | 4", "Beyond The Blue Lakefront Escape",       "$57,672.05", False, False),
    ("vip-21", "Ashley Eastwood",     "7/10", 7,  "7/17", "10 | 0", "Valhalla Lakefront on the West Shore",   "$32,774.23", False, False),
    ("vip-22", "Steven Brotherton",   "7/10", 31, "8/10", "2 | 2",  "Little Chief at Martis Camp",            "$21,850.00", False, False),
    ("vip-23", "Robert Mandel",       "7/11", 15, "7/26", "2 | 2",  "Glistening Shores Lakefront",            "$95,000.00", True,  False),
    ("vip-24", "Jorge Velasquez",     "7/11", 7,  "7/18", "12 | 0", "Cathedral Pines Lakefront",              "$30,129.30", False, False),
    ("vip-25", "Kathryn Mossawir",    "7/11", 7,  "7/18", "5 | 4",  "Aerial Grace Lakeside Retreat",          "$21,017.51", False, False),
    ("vip-26", "Craig Casca",         "7/11", 62, "9/11", "2 | 0",  "Lonestar Ranch",                         "$20,650.00", False, False),
    ("vip-27", "Kyle Bardet",         "7/14", 8,  "7/22", "5 | 4",  "Beach Haven Lakefront",                  "$20,094.03", False, False),
]


def _ensure_seeded():
    """Insert the original 27 cards ONCE (only when the table is empty), so later
    hand-removals aren't undone on every restart. Resolves each seed's Breezeway
    property_id from the cached property list (no per-reservation API calls) so
    the scan can dedupe scanned reservations against these seeds."""
    conn = get_db()
    cur = get_cursor(conn)
    cur.execute("SELECT COUNT(*) AS n FROM vip_reservations")
    if (cur.fetchone() or {}).get("n", 0) > 0:
        cur.close(); conn.close(); return
    prop_cache = {}
    try:
        from routes.briefing import _get_live_property_cache
        prop_cache = _get_live_property_cache() or {}
    except Exception:
        prop_cache = {}
    now = datetime.utcnow().isoformat()
    for key, name, ci, nts, co, guests, room, total, blue, first in _SEED:
        pid = _match_room_to_pid(room, prop_cache) if prop_cache else None
        iso = _ci_iso(ci)
        dk_pid  = f"{pid}|{iso}" if pid else ""
        dk_room = f"{_norm_room(room)}|{iso}"
        cur.execute(
            """INSERT INTO vip_reservations
               (item_key, reservation_id, property_id, dk_pid, dk_room, room, guest,
                ci, co, checkin_iso, nights, guests, total, blue, first_booking,
                source, active, created_at, updated_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'seed',1,%s,%s)
               ON CONFLICT (item_key) DO NOTHING""",
            (key, None, str(pid) if pid else None, dk_pid, dk_room, room, name,
             ci, co, iso, nts, guests, total, 1 if blue else 0, 1 if first else 0,
             now, now))
    conn.commit()
    cur.close(); conn.close()


@vip_bp.route("/vip/list")
@login_required
def vip_list():
    """The saved list of VIP cards (active only), earliest check-in first.
    Nothing is deleted here — departed cards stay so the page can show them in a
    'Checked out' section until the user exports + clears them. `today` and each
    card's checkin/checkout ISO dates drive the upcoming / in-house / departed
    grouping and collapse."""
    _ensure_seeded()
    conn = get_db()
    cur = get_cursor(conn)
    cur.execute("""SELECT item_key, room, guest, ci, co, checkin_iso, nights,
                          guests, total, blue, first_booking, source
                   FROM vip_reservations WHERE active = 1
                   ORDER BY checkin_iso, item_key""")
    rows = cur.fetchall()
    cur.close(); conn.close()
    out = [{
        "key":    r["item_key"], "room":  r["room"],  "name":   r["guest"],
        "ci":     r["ci"],       "co":    r["co"],    "nts":    r["nights"],
        "guests": r["guests"],   "total": r["total"],
        "blue":   bool(r["blue"]), "first": bool(r["first_booking"]),
        "source": r["source"],
        "checkin_iso":  r["checkin_iso"],
        "checkout_iso": _checkout_iso_of(r["checkin_iso"], r["nights"]),
    } for r in rows]
    return jsonify({"list": out, "today": _today_pacific().isoformat()})


@vip_bp.route("/vip/clear-departed", methods=["POST"])
@login_required
def vip_clear_departed():
    """Permanently delete every checked-out card (checkout before today) and its
    notes. Called by the 'Export & clear' button AFTER the notes file has been
    downloaded, so a copy is always saved first."""
    conn = get_db()
    cur = get_cursor(conn)
    gone = _purge_departed(cur)
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"ok": True, "removed": len(gone)})


def _html_escape(s):
    return (str(s if s is not None else "")
            .replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


@vip_bp.route("/vip/export")
@login_required
def vip_export():
    """Download every current VIP card and its notes as a styled HTML document —
    open it and print to PDF for a clean, shareable record. Dependency-free (the
    browser handles PDF), so it keeps a copy of your work before cards are cleared."""
    conn = get_db()
    cur = get_cursor(conn)
    cur.execute("""SELECT item_key, room, guest, ci, co, nights, guests, total
                   FROM vip_reservations WHERE active = 1
                   ORDER BY checkin_iso, item_key""")
    cards = cur.fetchall()
    cur.execute("SELECT item_key, author, body, created_at FROM vip_comments "
                "ORDER BY created_at ASC")
    comments = cur.fetchall()
    cur.execute("SELECT item_key, notes FROM vip_tracker WHERE COALESCE(notes,'') <> ''")
    legacy = cur.fetchall()
    cur.close(); conn.close()

    notes_by = {}
    for r in legacy:
        notes_by.setdefault(r["item_key"], []).append(("Earlier note", "", r["notes"]))
    for c in comments:
        notes_by.setdefault(c["item_key"], []).append(
            (c["author"] or "?", (c["created_at"] or "")[:16].replace("T", " "), c["body"] or ""))

    e = _html_escape
    today = _today_pacific().isoformat()
    total_notes = sum(len(v) for v in notes_by.values())

    blocks = []
    for c in cards:
        meta_bits = [b for b in [
            e(c["total"]) if c.get("total") else "",
            f"CI {e(c['ci'])} → CO {e(c['co'])}",
            f"{c['nights']} nights" if c.get("nights") else "",
            f"👥 {e(c['guests'])}" if c.get("guests") else "",
        ] if b]
        notes = notes_by.get(c["item_key"], [])
        if notes:
            note_html = "".join(
                f'<div class="note"><div class="who">{e(author)}'
                + (f'<span class="when"> · {e(when)}</span>' if when else "")
                + f'</div><div class="body">{e(body)}</div></div>'
                for author, when, body in notes)
        else:
            note_html = '<div class="nonotes">No notes.</div>'
        blocks.append(
            '<section class="card">'
            f'<div class="room">{e(c["room"])}</div>'
            f'<div class="guest">{e(c["guest"])}</div>'
            f'<div class="meta">{" &nbsp;·&nbsp; ".join(meta_bits)}</div>'
            f'{note_html}</section>')

    html = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>VIP Arrivals — Notes ({e(today)})</title>
<style>
  * {{ box-sizing: border-box; }}
  body {{ font-family: -apple-system, "Segoe UI", Roboto, Arial, sans-serif;
         color: #1f2937; background: #f3f4f6; margin: 0; padding: 28px 16px; }}
  .sheet {{ max-width: 820px; margin: 0 auto; }}
  header.top {{ margin-bottom: 22px; }}
  h1 {{ font-size: 22px; margin: 0 0 3px; }}
  .sub {{ color: #6b7280; font-size: 13px; }}
  .card {{ background: #fff; border: 1px solid #e5e7eb; border-radius: 12px;
          padding: 15px 18px; margin-bottom: 14px; }}
  .room {{ font-weight: 700; font-size: 16px; color: #111827; }}
  .guest {{ font-size: 13.5px; color: #374151; margin-top: 1px; }}
  .meta {{ font-size: 12.5px; color: #6b7280; margin: 3px 0 11px; }}
  .note {{ background: #f9fafb; border: 1px solid #eef2f7; border-radius: 8px;
          padding: 8px 11px; margin-bottom: 6px; }}
  .who {{ font-size: 12px; font-weight: 700; color: #374151; }}
  .when {{ font-weight: 400; color: #9ca3af; }}
  .body {{ font-size: 13.5px; color: #1f2937; white-space: pre-wrap;
          word-break: break-word; margin-top: 2px; }}
  .nonotes {{ font-size: 12.5px; color: #cbd5e1; font-style: italic; }}
  @media print {{
    body {{ background: #fff; padding: 0; }}
    .card {{ break-inside: avoid; border-color: #d1d5db; }}
    .hint {{ display: none; }}
  }}
  .hint {{ margin: 4px 0 18px; font-size: 12px; color: #9ca3af; }}
</style></head><body>
<div class="sheet">
  <header class="top">
    <h1>⭐ VIP Arrivals — Notes</h1>
    <div class="sub">Exported {e(today)} · {len(cards)} reservations · {total_notes} notes</div>
    <div class="hint">Tip: press Ctrl/Cmd&nbsp;+&nbsp;P and choose “Save as PDF” for a clean PDF copy.</div>
  </header>
  {"".join(blocks) if blocks else '<p class="sub">No reservations to export.</p>'}
</div>
</body></html>"""

    fname = f"vip-notes-{today}.html"
    return Response(html, mimetype="text/html; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="{fname}"'})


@vip_bp.route("/vip/scan", methods=["POST"])
@login_required
def vip_scan():
    """Scan Breezeway for VIP-tagged reservations checking in from 3 days ago
    through 21 days out and ADD any not already on the list. Insert-only: it
    never edits or removes an existing card, and a card removed by hand stays
    removed (its row is kept, so its dedupe keys still block re-adding)."""
    from routes.briefing import (_get_breezeway_token, _ensure_property_cache,
                                 _get_live_property_cache, _fetch_bw_reservations,
                                 _extract_str)
    _ensure_seeded()
    token = _get_breezeway_token()
    if not token:
        return jsonify({"error": "Breezeway not configured."}), 503

    today = _today_pacific()
    today_iso = today.isoformat()
    lo = (today - _td(days=3)).isoformat()
    hi = (today + _td(days=21)).isoformat()
    _ensure_property_cache()
    prop_cache = _get_live_property_cache()

    results = _fetch_bw_reservations(token, {"checkin_date_ge": lo, "checkin_date_le": hi})

    conn = get_db()
    cur = get_cursor(conn)
    # Dedupe against ALL rows (active OR removed) so a hand-removed card never returns.
    cur.execute("SELECT dk_pid, dk_room, reservation_id FROM vip_reservations")
    ex = cur.fetchall()
    have_pid  = {r["dk_pid"]  for r in ex if r["dk_pid"]}
    have_room = {r["dk_room"] for r in ex if r["dk_room"]}
    have_res  = {r["reservation_id"] for r in ex if r["reservation_id"]}

    now = datetime.utcnow().isoformat()
    uid = getattr(current_user, "id", None)
    added, scanned_vip = 0, 0
    seen = set()
    for r in (results or []):
        if not any("vip" in _extract_str(t) for t in (r.get("tags") or [])):
            continue
        scanned_vip += 1
        rid = str(r.get("id") or "")
        pid = str(r.get("property_id") or r.get("home_id") or "")
        iso = (r.get("checkin_date") or "")[:10]
        if not iso:
            continue
        room = (prop_cache.get(int(pid)) if pid.isdigit() else None) \
               or prop_cache.get(pid) or (f"Property {pid}" if pid else "Unknown")
        dk_pid  = f"{pid}|{iso}" if pid else ""
        dk_room = f"{_norm_room(room)}|{iso}"
        if (rid and rid in have_res) or (dk_pid and dk_pid in have_pid) or (dk_room in have_room):
            continue
        if (dk_pid and dk_pid in seen) or dk_room in seen:
            continue          # two reservations, same house+day, in one scan
        co = (r.get("checkout_date") or "")[:10]
        if co and co < today_iso:
            continue          # already departed — don't add (it would just be purged)
        seen.add(dk_pid); seen.add(dk_room)
        try:
            nts = (_date.fromisoformat(co) - _date.fromisoformat(iso)).days
        except Exception:
            nts = 0
        item_key = f"res-{rid}" if rid else f"scan-{dk_room}"
        cur.execute(
            """INSERT INTO vip_reservations
               (item_key, reservation_id, property_id, dk_pid, dk_room, room, guest,
                ci, co, checkin_iso, nights, guests, total, blue, first_booking,
                source, active, added_by, created_at, updated_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'','',0,0,'scan',1,%s,%s,%s)
               ON CONFLICT (item_key) DO NOTHING""",
            (item_key, rid or None, pid or None, dk_pid, dk_room, room, _res_guest(r),
             _iso_to_md(iso), _iso_to_md(co), iso, nts, uid, now, now))
        added += cur.rowcount
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"ok": True, "added": added, "scanned_vip": scanned_vip,
                    "window": {"from": lo, "to": hi}})


@vip_bp.route("/vip/edit", methods=["POST"])
@login_required
def vip_edit():
    """Edit the hand-entered fields (guest count, total) — Breezeway has neither."""
    body = request.get_json(silent=True) or {}
    key = (body.get("item_key") or "").strip()
    if not key:
        return jsonify({"error": "item_key required"}), 400
    fields, params = [], []
    if "guests" in body:
        fields.append("guests = %s"); params.append(str(body.get("guests") or "").strip())
    if "total" in body:
        fields.append("total = %s"); params.append(str(body.get("total") or "").strip())
    if not fields:
        return jsonify({"error": "nothing to update"}), 400
    fields.append("updated_at = %s"); params.append(datetime.utcnow().isoformat())
    params.append(key)
    conn = get_db()
    cur = get_cursor(conn)
    cur.execute(f"UPDATE vip_reservations SET {', '.join(fields)} WHERE item_key = %s", params)
    changed = cur.rowcount
    conn.commit(); cur.close(); conn.close()
    if not changed:
        return jsonify({"error": "card not found"}), 404
    return jsonify({"ok": True})


@vip_bp.route("/vip/remove", methods=["POST"])
@login_required
def vip_remove():
    """Soft-remove a card (active=0). Reversible; the row is KEPT so the scan
    won't re-add the reservation, and its notes/inspected-state are preserved."""
    body = request.get_json(silent=True) or {}
    key = (body.get("item_key") or "").strip()
    if not key:
        return jsonify({"error": "item_key required"}), 400
    conn = get_db()
    cur = get_cursor(conn)
    cur.execute("UPDATE vip_reservations SET active = 0, updated_at = %s WHERE item_key = %s",
                (datetime.utcnow().isoformat(), key))
    changed = cur.rowcount
    conn.commit(); cur.close(); conn.close()
    if not changed:
        return jsonify({"error": "card not found"}), 404
    return jsonify({"ok": True})


@vip_bp.route("/vip/house-tasks")
@login_required
def vip_house_tasks():
    """Tasks at one house for the 7 days leading up to a VIP check-in."""
    from routes.briefing import (_get_breezeway_token, _ensure_property_cache,
                                 _get_live_property_cache, _get_live_ref_cache,
                                 _fetch_bw_endpoint)
    from routes.dispatch import _bw_task_title

    room = (request.args.get("room") or "").strip()
    ci   = (request.args.get("ci") or "").strip()
    if not room or not ci:
        return jsonify({"error": "room and ci required"}), 400
    try:
        mm, dd = ci.split("/")[:2]
        ci_date = _date(_VIP_YEAR, int(mm), int(dd))
    except Exception:
        return jsonify({"error": "bad ci date"}), 400
    start = ci_date - _td(days=7)

    token = _get_breezeway_token()
    if not token:
        return jsonify({"error": "Breezeway not configured."}), 503
    _ensure_property_cache()
    prop_cache = _get_live_property_cache()
    ref_cache  = _get_live_ref_cache()

    bw_pid = _match_room_to_pid(room, prop_cache)
    if not bw_pid:
        return jsonify({"matched": False, "room": room,
                        "start": start.isoformat(), "ci": ci_date.isoformat()})

    ref_id = ref_cache.get(bw_pid) or str(bw_pid)
    drange = f"{start.isoformat()},{ci_date.isoformat()}"
    results, _err, _st = _fetch_bw_endpoint(
        token, "/public/inventory/v1/task",
        {"reference_property_id": ref_id, "scheduled_date": drange})

    tasks = []
    for t in (results or []):
        td = (t.get("scheduled_date") or "")[:10]
        if not (start.isoformat() <= td <= ci_date.isoformat()):
            continue
        dept = t.get("type_department")
        if isinstance(dept, dict):
            dept = dept.get("name") or dept.get("code")
        tasks.append({
            "name":       _bw_task_title(t),
            "date":       td,
            "time":       (str(t.get("scheduled_time") or "")[:5]) or None,
            "status":     _task_status(t),
            "assignees":  _assignees(t),
            "department": dept,
        })
    tasks.sort(key=lambda x: (x["date"], x["time"] or "~"))

    # Reservations overlapping the week-before window — who is at the house, and what type.
    from routes.briefing import _classify_reservation
    res_results, _re2, _rs2 = _fetch_bw_endpoint(
        token, "/public/inventory/v1/reservation",
        {"reference_property_id": ref_id,
         "checkout_date_ge": start.isoformat(),
         "checkin_date_le":  ci_date.isoformat()})
    reservations = []
    for r in (res_results or []):
        rpid = str(r.get("property_id") or r.get("home_id") or "")
        if rpid and rpid != str(bw_pid):
            continue                                   # endpoint may ignore the property filter
        cin  = (r.get("checkin_date") or "")[:10]
        cout = (r.get("checkout_date") or "")[:10]
        if not cin or not cout or cout < start.isoformat() or cin > ci_date.isoformat():
            continue
        reservations.append({
            "type":     _classify_reservation(r),
            "checkin":  cin,
            "checkout": cout,
            "guest":    _guest_name(r),
        })
    reservations.sort(key=lambda x: x["checkin"])

    return jsonify({"matched": True, "matched_name": prop_cache.get(bw_pid),
                    "start": start.isoformat(), "ci": ci_date.isoformat(),
                    "reservations": reservations,
                    "tasks": tasks})


@vip_bp.route("/vip")
@login_required
def vip_page():
    return render_template("vip.html")


@vip_bp.route("/vip/property-links", methods=["POST"])
@login_required
def vip_property_links():
    """Resolve VIP room names → Breezeway property ids for the 📅 calendar links.
    Reuses the same strict room→pid matcher as the week-before view, so a house that
    can't be confidently matched simply gets no link (never a wrong-house link)."""
    from routes.briefing import (_get_breezeway_token, _ensure_property_cache,
                                 _get_live_property_cache)

    body  = request.get_json(silent=True) or {}
    rooms = body.get("rooms") or []
    if not isinstance(rooms, list):
        return jsonify({"pids": {}})

    token = _get_breezeway_token()
    if not token:
        return jsonify({"pids": {}})
    _ensure_property_cache()
    prop_cache = _get_live_property_cache()

    pids = {}
    for room in rooms:
        if not isinstance(room, str) or room in pids:
            continue
        pid = _match_room_to_pid(room, prop_cache)
        if pid:
            pids[room] = str(pid)
    return jsonify({"pids": pids})


@vip_bp.route("/vip/state")
@login_required
def vip_state():
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("SELECT item_key, done, notes, updated_at FROM vip_tracker")
    rows = cur.fetchall()
    cur.close(); conn.close()
    state = {
        r["item_key"]: {
            "done":       bool(r["done"]),
            "notes":      r["notes"] or "",
            "updated_at": r["updated_at"],
        }
        for r in rows
    }
    return jsonify({"state": state})


@vip_bp.route("/vip/save", methods=["POST"])
@login_required
def vip_save():
    """Save just the 'inspected' checkbox (notes live in vip_comments now)."""
    body = request.get_json(silent=True) or {}
    key  = (body.get("item_key") or "").strip()
    if not key:
        return jsonify({"error": "item_key required"}), 400
    done = 1 if body.get("done") else 0
    now  = datetime.utcnow().isoformat()
    uid  = getattr(current_user, "id", None)

    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        """INSERT INTO vip_tracker (item_key, done, updated_at, updated_by)
           VALUES (%s, %s, %s, %s)
           ON CONFLICT (item_key) DO UPDATE SET
               done = EXCLUDED.done,
               updated_at = EXCLUDED.updated_at, updated_by = EXCLUDED.updated_by""",
        (key, done, now, uid),
    )
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"ok": True, "updated_at": now})


@vip_bp.route("/vip/comments")
@login_required
def vip_comments():
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("SELECT id, item_key, author, author_id, body, created_at FROM vip_comments ORDER BY created_at ASC")
    rows = cur.fetchall()
    # Legacy single-field notes (from before threaded comments) — surface them so
    # nothing that was ever written is lost. (Read-only; no id.)
    cur.execute("SELECT item_key, notes, updated_at FROM vip_tracker WHERE COALESCE(notes, '') <> ''")
    legacy = cur.fetchall()
    cur.close(); conn.close()

    out = {}
    for lr in legacy:                       # show earlier notes first
        out.setdefault(lr["item_key"], []).append({
            "id": None, "author": "Earlier note", "author_id": None,
            "body": lr["notes"], "created_at": lr["updated_at"],
        })
    for r in rows:
        out.setdefault(r["item_key"], []).append({
            "id": r["id"], "author": r["author"] or "?", "author_id": r["author_id"],
            "body": r["body"], "created_at": r["created_at"],
        })
    return jsonify({"comments": out})


@vip_bp.route("/vip/comment", methods=["POST"])
@login_required
def vip_comment():
    body = request.get_json(silent=True) or {}
    key  = (body.get("item_key") or "").strip()
    text = str(body.get("body") or "").strip()
    if not key or not text:
        return jsonify({"error": "item_key and body required"}), 400
    now    = datetime.utcnow().isoformat()
    author = (getattr(current_user, "name", None) or getattr(current_user, "email", None) or "Someone")
    uid    = getattr(current_user, "id", None)

    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        "INSERT INTO vip_comments (item_key, author_id, author, body, created_at) "
        "VALUES (%s, %s, %s, %s, %s) RETURNING id",
        (key, uid, author, text, now),
    )
    new_id = cur.fetchone()["id"]
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"ok": True, "comment": {
        "id": new_id, "author": author, "author_id": uid, "body": text, "created_at": now}})


@vip_bp.route("/vip/comment/edit", methods=["POST"])
@login_required
def vip_comment_edit():
    """Edit one of YOUR OWN comments (author-scoped — can't touch others')."""
    body = request.get_json(silent=True) or {}
    cid  = body.get("id")
    text = str(body.get("body") or "").strip()
    if not cid or not text:
        return jsonify({"error": "id and body required"}), 400
    uid = getattr(current_user, "id", None)

    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        "UPDATE vip_comments SET body = %s WHERE id = %s AND author_id = %s",
        (text, cid, uid),
    )
    changed = cur.rowcount
    conn.commit()
    cur.close(); conn.close()
    if not changed:
        return jsonify({"error": "Not found, or not your note."}), 403
    return jsonify({"ok": True, "body": text})


@vip_bp.route("/vip/comment/delete", methods=["POST"])
@login_required
def vip_comment_delete():
    """Delete one of YOUR OWN comments (author-scoped)."""
    body = request.get_json(silent=True) or {}
    cid  = body.get("id")
    if not cid:
        return jsonify({"error": "id required"}), 400
    uid = getattr(current_user, "id", None)

    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        "DELETE FROM vip_comments WHERE id = %s AND author_id = %s",
        (cid, uid),
    )
    changed = cur.rowcount
    conn.commit()
    cur.close(); conn.close()
    if not changed:
        return jsonify({"error": "Not found, or not your note."}), 403
    return jsonify({"ok": True})
