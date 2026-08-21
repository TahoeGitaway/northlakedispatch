"""
routes/carpet_scan.py — "when were this house's carpets last cleaned?"

For every property carrying the STR property tag, finds the most recent COMPLETED
task whose TITLE says "carpet", within one calendar year, excluding housekeeping
departments. Read-only: this module never writes to Breezeway.

What counts as a carpet clean
-----------------------------
The title only has to contain "carpet" — titles carry notes and phrasings vary
("Clean carpets", "Carpet shampoo", "Carpet clean — key under mat"), and demanding
an exact phrase quietly loses real cleanings. Housekeeping is then excluded by
DEPARTMENT, which is what keeps that loose rule honest: carpet cleaning is
maintenance work, and a housekeeper's carpet note is not the clean this page
reports. Both decisions live at the top of the file with the reasoning.

Private by design
-----------------
Every stored row is scoped to the user who ran the scan, and every query filters
on it. Two people opening /carpet see their own results and never each other's;
there is no shared or global view, and the page is not linked from the nav.

Why it is batched and cached
----------------------------
There is no company-wide task query — Breezeway wants a property id, verified via
/admin/bw-probe — so a year of carpet history is one paginated task query per
house. That does not fit in a page load; the request dies at the gateway timeout
with nothing to show for it. So:

  * results live in `carpet_last_clean` and the page renders from that table with
    zero API calls;
  * the scan runs a batch at a time and stops on its own time budget, well short
    of the 120s Gunicorn timeout, returning what it finished;
  * because each house is committed as it completes, an interrupted scan loses
    nothing and resuming only asks about houses still missing.

Endpoints:
  GET  /carpet              — page (cached results, no API calls)
  GET  /carpet/tags         — property tags available in Breezeway, for the picker
  POST /carpet/tag          — choose the tag and resolve which properties carry it
  POST /carpet/scan         — scan the next batch of unscanned houses (JSON)
  GET  /carpet/export.csv   — download the saved results as CSV
  POST /carpet/reset        — clear results (all, or only the failed ones)
  POST /carpet/delete       — delete everything this page has saved for this user
"""

import csv
import io
import json
import re
import time
from datetime import date, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Blueprint, render_template, request, jsonify, Response
from flask_login import login_required, current_user

from db import get_db, get_cursor
from routes.bw_api_log import bw_get
from routes.bw_ratelimit import gate

carpet_scan_bp = Blueprint("carpet_scan", __name__)

BW_BASE = "https://api.breezeway.io"
# The web app, not the API — for links a person clicks. Same forms the rest of the
# templates use: /property/<id>/calendar and /task/<id>.
BW_APP  = "https://app.breezeway.io"

# The year this page is about. Carpet cleaning is tracked per calendar year, and a
# scan pointed at the wrong year silently answers a different question, so the year
# is fixed here rather than taken from the request.
CARPET_YEAR = 2026

# The property tag that defines the house list. Only a default — the resolved tag
# is stored per user, so renaming it in Breezeway means re-picking it on the page
# rather than editing this file.
DEFAULT_TAG_NAME = "STR"

PAGE_LIMIT = 100
PAGE_CAP   = 12      # ~1200 tasks/house/year; hitting it flags the row, never truncates silently
BATCH      = 10      # houses per scan request
WORKERS    = 8       # concurrent property fetches, paced by the shared gate
TAG_WORKERS = 16     # concurrent tag lookups when membership must be swept

# Stop admitting new work after this long and return what finished. Gunicorn kills
# the request at 120s; a batch that dies there reports nothing, while one that
# stops early reports everything it did and the page just asks again.
BUDGET_S = 75

# TITLE ONLY. Saying "carpet" anywhere in the title is enough — the previous rule
# demanded "carpet" and "clean" adjacent and in that order, which silently missed
# real cleanings titled "Clean carpets", "Carpet shampoo" or "Annual carpet".
#
# Titles routinely carry notes ("Carpet clean — key under mat, do not enter
# master"). This is a substring search, so anything wrapped around the word is
# harmless; nothing outside the title is ever read.
#
# carpe?[tr] rather than carp\w*: it still takes carpet/carpets/carpeting and the
# "carpert"/"carpt" misspellings, but will not fire on "carpentry" or "carpool".
CARPET_RE = re.compile(r"carpe?[tr]", re.I)

# Housekeeping is excluded at the DEPARTMENT level, because carpet cleaning is
# maintenance work. This is what keeps the deliberately-loose title rule above
# honest: a housekeeper's "clean carpet in unit 4" is not the carpet clean this
# page reports, and counting it would date the house wrong.
#
# "housekeep" ONLY — deliberately narrower than group_assign.py / assignee_monitor.py,
# which also exclude on "clean". That keyword is safe for them and actively wrong
# here: a department named "Carpet Cleaning" would exclude the very tasks this page
# exists to find. Matching the department, never the title.
HOUSEKEEPING_RE = re.compile(r"housekeep", re.I)


def _no_token_error():
    """Why there is no token, in Breezeway's own words.

    _get_breezeway_token() returns None for several different reasons, and this
    page used to report all of them as "check BREEZEWAY_CLIENT_ID / SECRET on the
    host". That reads as a configuration fault, so the obvious response is to go
    hunting through environment variables — when the actual cause is usually that
    auth is rate-limited and the only fix is to wait. briefing.py already records
    the real reason for exactly this purpose; ask it instead of guessing.
    """
    from routes.briefing import _get_bw_token_last_error
    why = (_get_bw_token_last_error() or "").strip()
    return f"No Breezeway token — {why}" if why else (
        "No Breezeway token, and no reason recorded. Check BREEZEWAY_CLIENT_ID / "
        "BREEZEWAY_CLIENT_SECRET on the host.")


def _dept_of(t):
    """Task department, lowercased. Breezeway returns it as a bare string or as a
    {code, name} object depending on the endpoint."""
    dept = t.get("type_department")
    if isinstance(dept, dict):
        dept = dept.get("code") or dept.get("name") or ""
    return str(dept or "").strip().lower()


def _task_assignees(t):
    """Everyone assigned, comma-joined. Same unpacking as assignee_monitor.py:
    each assignment carries a name, or first/last name parts to rebuild it."""
    names = []
    for a in (t.get("assignments") or []):
        nm = (a.get("name") or
              f"{a.get('first_name','').strip()} {a.get('last_name','').strip()}".strip())
        if nm and nm not in names:
            names.append(nm)
    return ", ".join(names)

# Terminal statuses that mean the work actually happened. Anything outside this set
# is counted and surfaced on the page rather than being read as "not cleaned".
COMPLETED = {"approved", "finished", "closed", "completed", "complete"}


# ── Breezeway field readers ──────────────────────────────────────────────────

def _s(v):
    if isinstance(v, dict):
        return str(v.get("value") or v.get("name") or v.get("label") or "")
    return str(v or "")


def _task_title(t):
    for k in ("title", "name", "template_name", "task_name"):
        if t.get(k):
            return _s(t.get(k)).strip()
    return ""


def _task_status(t):
    for k in ("type_task_status", "status", "state"):
        if t.get(k):
            return _s(t.get(k)).lower().strip()
    return ""


def _task_completion_date(t):
    for k in ("finished_at", "completed_at", "date_completed", "completion_date", "closed_at"):
        raw = t.get(k)
        if raw:
            s = str(raw)
            if len(s) >= 10 and s[4] == "-" and s[7] == "-":
                return s[:10]
            try:
                return datetime.fromisoformat(s.replace("Z", "+00:00")).date().isoformat()
            except Exception:
                return s[:10]
    return ""


def _tag_ident(t):
    """(id, lowercased name) from a tag in any shape Breezeway returns it."""
    if isinstance(t, dict):
        return (str(t.get("id") or t.get("tag_id") or ""),
                (t.get("name") or t.get("label") or "").lower().strip())
    return ("", str(t).lower().strip())


# ── Tags and membership ──────────────────────────────────────────────────────

def _fetch_tag_list(token):
    """All property tags defined in Breezeway. Returns (tags, error)."""
    try:
        if not gate.acquire():
            return [], "held back by this app's rate limiter"
        r = bw_get(f"{BW_BASE}/public/inventory/v1/property/tags",
                   headers={"Authorization": f"JWT {token}"}, timeout=15)
    except Exception as e:
        return [], f"{type(e).__name__}: {e}"
    if r.status_code != 200:
        return [], f"HTTP {r.status_code}: {(r.text or '')[:120]}"
    try:
        body = r.json()
    except Exception as e:
        return [], f"unparseable JSON: {e}"
    raw = body if isinstance(body, list) else (body.get("results") or body.get("data") or [])
    out = []
    for t in raw:
        tid, name = _tag_ident(t)
        if name:
            out.append({"id": tid, "name": (t.get("name") or t.get("label") or "").strip()})
    return out, ""


def _fetch_properties_raw(token):
    """Active properties as Breezeway returns them, tags included if it sends any.

    Worth trying before the per-property sweep: if the list endpoint carries tags
    inline, membership costs a handful of calls instead of one per property."""
    out, page = [], 1
    while page <= 20:
        if not gate.acquire():
            return out, "held back by this app's rate limiter"
        try:
            r = bw_get(f"{BW_BASE}/public/inventory/v1/property",
                       headers={"Authorization": f"JWT {token}"},
                       params={"limit": 200, "page": page, "status": "active"},
                       timeout=20)
        except Exception as e:
            return out, f"{type(e).__name__}: {e}"
        if r.status_code != 200:
            return out, f"HTTP {r.status_code}: {(r.text or '')[:120]}"
        try:
            body = r.json()
        except Exception as e:
            return out, f"unparseable JSON: {e}"
        items = body.get("results", body.get("data", body if isinstance(body, list) else [])) or []
        out.extend(items)
        if len(items) < 200:
            return out, ""
        page += 1
    return out, ""


def _property_tags_inline(p):
    for k in ("tags", "property_tags", "propertyTags"):
        v = p.get(k)
        if isinstance(v, list) and v:
            return v
    return None


def _fetch_property_tags(token, pid):
    for path in (f"/public/inventory/v1/property/{pid}/tags",
                 f"/public/inventory/v1/property/{pid}"):
        if not gate.acquire():
            return []
        try:
            r = bw_get(f"{BW_BASE}{path}", headers={"Authorization": f"JWT {token}"}, timeout=15)
            if r.status_code == 200:
                body = r.json()
                if isinstance(body, list):
                    return body
                tags = body.get("tags") or body.get("property_tags") or []
                if tags:
                    return tags
        except Exception:
            pass
    return []


def _resolve_members(token, tag_id, tag_name):
    """Properties carrying the tag → [(pid, name)]. Returns (members, error, swept).

    Runs in one request on purpose. The sweep fallback is ~one call per property,
    which the shared gate paces to roughly 20s for a full portfolio — inside the
    budget here and well inside Gunicorn's 120s. If it ever does time out the call
    is idempotent, so retrying simply redoes it."""
    props, err = _fetch_properties_raw(token)
    if err and not props:
        return [], err, False
    want_id, want_name = str(tag_id or ""), (tag_name or "").lower().strip()

    def carries(tags):
        for t in tags or []:
            tid, name = _tag_ident(t)
            if (want_id and tid == want_id) or (want_name and name == want_name):
                return True
        return False

    def pname(p):
        raw = (p.get("name") or p.get("property_name") or p.get("title")
               or p.get("display_name") or p.get("id"))
        return raw if isinstance(raw, str) else str(p.get("id"))

    inline = [p for p in props if _property_tags_inline(p) is not None]
    if inline and len(inline) >= max(1, len(props) // 2):
        # The list endpoint carries tags — no sweep needed.
        return ([(str(p.get("id")), pname(p)) for p in props
                 if carries(_property_tags_inline(p))], "", False)

    members, t0 = [], time.time()
    with ThreadPoolExecutor(max_workers=TAG_WORKERS) as ex:
        futures = {}
        for p in props:
            if time.time() - t0 > BUDGET_S:
                break
            futures[ex.submit(_fetch_property_tags, token, p.get("id"))] = p
        for fut in as_completed(futures):
            p = futures[fut]
            try:
                if carries(fut.result()):
                    members.append((str(p.get("id")), pname(p)))
            except Exception:
                pass
    return members, "", True


# ── Task fetching ────────────────────────────────────────────────────────────

def _paginate(token, key, val, date_range):
    """Returns (tasks, error, truncated, http_ok). http_ok distinguishes "this id
    form works and the house has no tasks" from "this id form failed", which is
    what decides whether to try the next id form."""
    out, page = [], 1
    while page <= PAGE_CAP:
        if not gate.acquire():
            return out, "held back by this app's rate limiter — not sent to Breezeway", False, False
        try:
            r = bw_get(
                f"{BW_BASE}/public/inventory/v1/task/",
                headers={"Authorization": f"JWT {token}"},
                params={"scheduled_date": date_range, key: val,
                        "limit": PAGE_LIMIT, "page": page},
                timeout=20,
            )
        except Exception as e:
            return out, f"{type(e).__name__}: {e}", False, False
        if r.status_code != 200:
            return out, f"HTTP {r.status_code}: {(r.text or '')[:120]}", False, False
        try:
            body = r.json()
        except Exception as e:
            return out, f"unparseable JSON: {e}", False, False
        results = body.get("results", body.get("data", body if isinstance(body, list) else [])) or []
        out.extend(results)
        if len(results) < PAGE_LIMIT:
            return out, "", False, True
        page += 1
    return out, "", True, True


# Promoted to briefing.py, next to the cache it reads — this module was solving the
# key mismatch for itself while every other scanner went on missing the ref id.
from routes.briefing import _ref_for


def _fetch_year_tasks(token, pid, ref_id, year):
    """All tasks for one property across `year`, padded either side — a task
    scheduled on Dec 30th can be completed on Jan 1st, and that completion still
    belongs to the year it happened in."""
    start, end = date(year - 1, 12, 15), date(year + 1, 1, 15)
    date_range = f"{start.isoformat()},{end.isoformat()}"

    id_pairs = []
    if ref_id:
        id_pairs.append(("reference_property_id", ref_id))
    # home_id first. Breezeway aliases property_id onto reference_property_id,
    # so a raw Breezeway pid there can only ever 422 — it was costing every
    # house a guaranteed-failed request before the one that works. Kept last
    # rather than deleted: cheap insurance if home_id ever fails too.
    id_pairs += [("home_id", str(pid)), ("property_id", str(pid))]

    last_err = ""
    for key, val in id_pairs:
        tasks, err, truncated, http_ok = _paginate(token, key, val, date_range)
        if tasks:
            return tasks, "", truncated
        if http_ok:
            return [], "", False        # id form worked, house genuinely has none
        last_err = err or last_err
    return [], last_err or "no usable property id", False


def _scan_house(token, pid, ref_id, year):
    """One house → the row we store. Never raises: a house that fails is stored
    WITH its error, because a failure displayed as "no cleaning" is a lie."""
    try:
        tasks, err, truncated = _fetch_year_tasks(token, pid, ref_id, year)
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}", "clean_count": 0,
                "truncated": 0, "unread": 1}
    if err:
        # unread=1: this house was NOT read. It must never be presented as a house
        # with no cleaning — that is the difference between an answer and a gap.
        return {"error": err, "clean_count": 0, "truncated": 0, "unread": 1}

    hits, other_status, hk_skipped = [], set(), 0
    for t in tasks:
        if not CARPET_RE.search(_task_title(t)):
            continue
        # Department first: a housekeeping carpet task is not this page's answer.
        # Counted rather than dropped, so a house whose only carpet work was
        # housekeeping says so instead of silently reading as "never cleaned".
        if HOUSEKEEPING_RE.search(_dept_of(t)):
            hk_skipped += 1
            continue
        status = _task_status(t)
        if status not in COMPLETED:
            other_status.add(status or "(blank)")
            continue
        d = _task_completion_date(t)
        note = ""
        if not d:
            # Completed but no completion timestamp. Fall back to the scheduled
            # date and say so, rather than dropping a real cleaning.
            d = str(t.get("scheduled_date") or "")[:10]
            note = "scheduled date — no completion timestamp"
        if d[:4] == str(year):
            hits.append((d, _task_title(t), status, note,
                         str(t.get("id") or ""), _task_assignees(t)))

    hits.sort(reverse=True)
    row = {"clean_count": len(hits), "truncated": 1 if truncated else 0,
           "unread": 0, "error": ""}
    # Keep ALL of them, not just the winner. CARPET_RE is deliberately broad, so
    # the top hit is sometimes not a cleaning at all ("Pick up carpet fan"); when
    # it gets dismissed, the next one has to be available without re-reading the
    # house. They are already in hand here — throwing them away just to fetch them
    # again later is the expensive way to be wrong.
    row["hits_json"] = json.dumps([
        {"date": d, "title": title, "status": status, "note": note,
         "task_id": tid, "assignees": who}
        for d, title, status, note, tid, who in hits
    ])
    if hits:
        d, title, status, note, tid, who = hits[0]
        row.update(last_clean_date=d, task_title=title, task_status=status,
                   task_id=tid, assignees=who)
        if note:
            row["error"] = note
    if not hits and hk_skipped:
        row["error"] = (f"{hk_skipped} carpet task(s) found, but in housekeeping "
                        "— excluded as not maintenance work")
    if not hits and other_status:
        # Carpet tasks exist but none are complete — worth saying, since it is the
        # difference between "never scheduled" and "scheduled and not done".
        row["error"] = ("carpet task(s) found but not complete: "
                        + ", ".join(sorted(other_status))[:120])
    return row


# ── Storage (all of it scoped to the current user) ───────────────────────────

def _uid():
    return current_user.id


def _get_prefs():
    conn = get_db(); cur = get_cursor(conn)
    cur.execute("SELECT tag_id, tag_name FROM carpet_scan_prefs WHERE user_id = %s", (_uid(),))
    row = cur.fetchone()
    cur.close(); conn.close()
    return dict(row) if row else {"tag_id": None, "tag_name": None}


def _save_prefs(tag_id, tag_name):
    conn = get_db(); cur = get_cursor(conn)
    cur.execute("""
        INSERT INTO carpet_scan_prefs (user_id, tag_id, tag_name, updated_at)
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (user_id) DO UPDATE SET
            tag_id=EXCLUDED.tag_id, tag_name=EXCLUDED.tag_name,
            updated_at=EXCLUDED.updated_at
    """, (_uid(), str(tag_id or ""), tag_name, datetime.utcnow().isoformat()))
    conn.commit(); cur.close(); conn.close()


def _save_members(tag_key, members):
    conn = get_db(); cur = get_cursor(conn)
    cur.execute("DELETE FROM carpet_tag_members WHERE tag_id = %s", (tag_key,))
    now = datetime.utcnow().isoformat()
    for pid, name in members:
        cur.execute("""
            INSERT INTO carpet_tag_members (tag_id, property_id, property_name, resolved_at)
            VALUES (%s,%s,%s,%s)
            ON CONFLICT (tag_id, property_id) DO UPDATE SET
                property_name=EXCLUDED.property_name, resolved_at=EXCLUDED.resolved_at
        """, (tag_key, pid, name, now))
    conn.commit(); cur.close(); conn.close()


def _load_members(tag_key):
    if not tag_key:
        return []
    conn = get_db(); cur = get_cursor(conn)
    cur.execute("""SELECT property_id, property_name, resolved_at
                   FROM carpet_tag_members WHERE tag_id = %s
                   ORDER BY property_name ASC""", (tag_key,))
    rows = [dict(r) for r in cur.fetchall()]
    cur.close(); conn.close()
    return rows


def _tag_key(prefs):
    """What `carpet_tag_members` is keyed by. Prefer the tag id; fall back to the
    name so a tag list that returns no ids still works."""
    return str(prefs.get("tag_id") or "") or (prefs.get("tag_name") or "")


def _load_results(year):
    conn = get_db(); cur = get_cursor(conn)
    cur.execute("""
        SELECT property_id, house_name, last_clean_date, task_id, task_title,
               task_status, assignees, clean_count, truncated, unread, error,
               scanned_at, hits_json
        FROM carpet_last_clean WHERE user_id = %s AND year = %s
    """, (_uid(), year))
    rows = {str(r["property_id"]): dict(r) for r in cur.fetchall()}
    cur.close(); conn.close()
    return rows


def _save_result(year, pid, house, row):
    conn = get_db(); cur = get_cursor(conn)
    cur.execute("""
        INSERT INTO carpet_last_clean
            (user_id, year, property_id, house_name, last_clean_date, task_id,
             task_title, task_status, assignees, clean_count, truncated, unread,
             error, scanned_at, hits_json)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (user_id, year, property_id) DO UPDATE SET
            house_name=EXCLUDED.house_name, last_clean_date=EXCLUDED.last_clean_date,
            task_id=EXCLUDED.task_id, task_title=EXCLUDED.task_title,
            task_status=EXCLUDED.task_status, assignees=EXCLUDED.assignees,
            clean_count=EXCLUDED.clean_count, truncated=EXCLUDED.truncated,
            unread=EXCLUDED.unread, error=EXCLUDED.error, scanned_at=EXCLUDED.scanned_at,
            hits_json=EXCLUDED.hits_json
    """, (_uid(), year, str(pid), house, row.get("last_clean_date"),
          row.get("task_id"), row.get("task_title"), row.get("task_status"),
          row.get("assignees"), row.get("clean_count", 0),
          row.get("truncated", 0), row.get("unread", 0), row.get("error") or None,
          datetime.utcnow().isoformat(), row.get("hits_json")))
    conn.commit(); cur.close(); conn.close()


# ── The user's own corrections ───────────────────────────────────────────────
#
# Kept apart from the scan cache so Retry failed / Rescan all cannot destroy them.
# A note is the only thing on this page a person actually wrote; a dismissal is a
# judgement the scan cannot make for itself.

def _load_edits(year):
    conn = get_db(); cur = get_cursor(conn)
    cur.execute("""SELECT property_id, note, dismissed FROM carpet_row_edits
                   WHERE user_id = %s AND year = %s""", (_uid(), year))
    out = {}
    for r in cur.fetchall():
        try:
            dismissed = json.loads(r["dismissed"] or "[]")
        except Exception:
            dismissed = []
        out[str(r["property_id"])] = {
            "note": r["note"] or "",
            "dismissed": [str(t) for t in dismissed if str(t)],
        }
    cur.close(); conn.close()
    return out


def _save_edit(year, pid, note=None, dismissed=None):
    """Upsert one house's note and/or dismissal list. Passing None leaves that
    field as it was, so saving a note can't silently drop dismissals."""
    conn = get_db(); cur = get_cursor(conn)
    cur.execute("""SELECT note, dismissed FROM carpet_row_edits
                   WHERE user_id = %s AND year = %s AND property_id = %s""",
                (_uid(), year, str(pid)))
    cur_row = cur.fetchone()
    if note is None:
        note = (cur_row["note"] if cur_row else "") or ""
    if dismissed is None:
        dismissed = (cur_row["dismissed"] if cur_row else "[]") or "[]"
    else:
        dismissed = json.dumps(sorted({str(t) for t in dismissed if str(t)}))
    cur.execute("""
        INSERT INTO carpet_row_edits (user_id, year, property_id, note, dismissed, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (user_id, year, property_id) DO UPDATE SET
            note=EXCLUDED.note, dismissed=EXCLUDED.dismissed, updated_at=EXCLUDED.updated_at
    """, (_uid(), year, str(pid), note, dismissed, datetime.utcnow().isoformat()))
    conn.commit(); cur.close(); conn.close()
    return {"note": note, "dismissed": json.loads(dismissed)}


def _apply_edit(row, edit):
    """Fold one house's note + dismissals into its scanned row.

    Dismissing the top task promotes the next-most-recent surviving one, so the
    house shows its real last cleaning rather than dropping to "none". A row
    scanned before hits were stored has nothing to promote — it says so and asks
    for a rescan, because guessing "none" there would be inventing an answer."""
    if not edit:
        return row
    row = dict(row)
    row["note"] = edit.get("note") or ""
    dismissed = set(edit.get("dismissed") or [])
    row["dismissed_ids"] = sorted(dismissed)
    if not dismissed or row.get("unread"):
        return row

    try:
        hits = json.loads(row.get("hits_json") or "[]")
    except Exception:
        hits = []
    kept = [h for h in hits if str(h.get("task_id") or "") not in dismissed]
    row["dismissed_count"] = len(hits) - len(kept)

    if not hits and str(row.get("task_id") or "") in dismissed:
        # Pre-upgrade row: we know the shown task is wrong but not what came before
        # it. Clear the answer — leaving the dismissed task's date on display would
        # ignore the correction, and calling it "none" would invent one.
        row["needs_rescan"] = True
        row.update(last_clean_date=None, task_title=None, task_status=None,
                   task_id=None, assignees=None, clean_count=0)
        row["error"] = "dismissed — rescan this house to find the cleaning before it"
        return row

    row["clean_count"] = len(kept)
    if kept:
        top = kept[0]
        row.update(last_clean_date=top.get("date"), task_title=top.get("title"),
                   task_status=top.get("status"), task_id=top.get("task_id"),
                   assignees=top.get("assignees"))
        row["error"] = top.get("note") or ""
    else:
        row.update(last_clean_date=None, task_title=None, task_status=None,
                   task_id=None, assignees=None)
        row["error"] = "every carpet task here was dismissed as not a cleaning"
    return row


def _view_rows(year, members, results, edits=None):
    """Every house in the tag, cached result attached if we have one, with the
    user's note/dismissals folded in. A house with no row yet is 'not scanned',
    which the page must not confuse with 'no cleaning found'."""
    edits = edits if edits is not None else _load_edits(year)
    out = []
    for m in members:
        pid = str(m["property_id"])
        r = results.get(pid)
        if r:
            r = dict(r); r["scanned"] = True
        else:
            r = {"property_id": pid, "house_name": m["property_name"],
                 "scanned": False, "clean_count": 0}
        # A note belongs to the house, so it shows even on a house that has not
        # been scanned yet or could not be read.
        out.append(_apply_edit(r, edits.get(pid)))
    return out


# ── Routes ───────────────────────────────────────────────────────────────────

@carpet_scan_bp.route("/carpet")
@login_required
def carpet_page():
    prefs   = _get_prefs()
    members = _load_members(_tag_key(prefs))
    rows    = _view_rows(CARPET_YEAR, members, _load_results(CARPET_YEAR))
    return render_template(
        "carpet_scan.html",
        year=CARPET_YEAR,
        rows=rows,
        tag_name=prefs.get("tag_name") or DEFAULT_TAG_NAME,
        tag_chosen=bool(prefs.get("tag_name")),
        default_tag=DEFAULT_TAG_NAME,
        total=len(rows),
        scanned=sum(1 for r in rows if r["scanned"]),
    )


@carpet_scan_bp.route("/carpet/tags")
@login_required
def carpet_tags():
    from routes.briefing import _get_breezeway_token
    token = _get_breezeway_token()
    if not token:
        return jsonify({"error": _no_token_error()}), 502
    tags, err = _fetch_tag_list(token)
    if err:
        return jsonify({"error": f"Could not read property tags: {err}"}), 502
    return jsonify({"tags": sorted(tags, key=lambda t: t["name"].lower()),
                    "default": DEFAULT_TAG_NAME})


@carpet_scan_bp.route("/carpet/tag", methods=["POST"])
@login_required
def carpet_set_tag():
    """Choose the tag and work out which properties carry it."""
    from routes.briefing import _get_breezeway_token
    body = request.json or {}
    name = (body.get("tag_name") or DEFAULT_TAG_NAME).strip()

    token = _get_breezeway_token()
    if not token:
        return jsonify({"error": _no_token_error()}), 502

    tags, err = _fetch_tag_list(token)
    if err:
        return jsonify({"error": f"Could not read property tags: {err}"}), 502

    match = next((t for t in tags if t["name"].lower().strip() == name.lower()), None)
    if not match:
        known = ", ".join(sorted(t["name"] for t in tags)[:25]) or "(none returned)"
        return jsonify({"error": f"No property tag named “{name}” in Breezeway. "
                                 f"Tags found: {known}"}), 404

    members, err, swept = _resolve_members(token, match["id"], match["name"])
    if err:
        return jsonify({"error": f"Could not read properties: {err}"}), 502
    if not members:
        return jsonify({"error": f"No properties carry the “{match['name']}” tag."}), 404

    _save_prefs(match["id"], match["name"])
    _save_members(_tag_key({"tag_id": match["id"], "tag_name": match["name"]}), members)
    return jsonify({"success": True, "tag_name": match["name"],
                    "count": len(members), "swept": swept})


@carpet_scan_bp.route("/carpet/scan", methods=["POST"])
@login_required
def carpet_scan():
    """Scan the next batch of unscanned houses. Safe to call repeatedly — it only
    ever picks up houses with no row yet, so the client can keep calling until
    `remaining` reaches zero, and a timed-out call costs only its own batch."""
    from routes.briefing import _get_breezeway_token, _get_live_ref_cache

    body  = request.json or {}
    year  = CARPET_YEAR
    limit = max(1, min(int(body.get("limit") or BATCH), 25))

    prefs   = _get_prefs()
    members = _load_members(_tag_key(prefs))
    if not members:
        return jsonify({"error": f"No house list yet — choose the "
                                 f"“{prefs.get('tag_name') or DEFAULT_TAG_NAME}” tag first.",
                        "remaining": 0}), 400

    results = _load_results(year)
    pending = [m for m in members if str(m["property_id"]) not in results]
    if not pending:
        return jsonify({"done": True, "remaining": 0, "scanned": len(results),
                        "total": len(members), "rows": []})

    token = _get_breezeway_token()
    if not token:
        return jsonify({"error": _no_token_error(),
                        "remaining": len(pending)}), 502

    ref_cache = _get_live_ref_cache() or {}
    batch = pending[:limit]
    t0, saved = time.time(), []

    # Fetch concurrently, write in this thread — DB connections are not shared
    # across threads, and the gate paces the API side regardless of pool size.
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {}
        for m in batch:
            if time.time() - t0 > BUDGET_S:
                break        # out of budget: leave the rest pending, page resumes
            pid = str(m["property_id"])
            futures[ex.submit(_scan_house, token, pid, _ref_for(ref_cache, pid), year)] = m
        for fut in as_completed(futures):
            m   = futures[fut]
            pid = str(m["property_id"])
            try:
                row = fut.result()
            except Exception as e:
                row = {"clean_count": 0, "truncated": 0, "unread": 1,
                       "error": f"{type(e).__name__}: {e}"}
            _save_result(year, pid, m["property_name"], row)
            saved.append({"property_id": pid, "house_name": m["property_name"],
                          "scanned": True, **row})

    done_count = len(_load_results(year))
    return jsonify({
        "done": done_count >= len(members),
        "remaining": len(members) - done_count,
        "scanned": done_count,
        "total": len(members),
        "elapsed_s": round(time.time() - t0, 1),
        "rows": saved,
    })


@carpet_scan_bp.route("/carpet/export.csv")
@login_required
def carpet_export():
    """Download the saved results. Houses not yet scanned are included with a blank
    result and a "not scanned" note — a CSV that quietly omits them would read as a
    complete portfolio when it isn't."""
    prefs   = _get_prefs()
    members = _load_members(_tag_key(prefs))
    rows    = _view_rows(CARPET_YEAR, members, _load_results(CARPET_YEAR))

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["House", "Breezeway property ID", f"Last carpet clean ({CARPET_YEAR})",
                "Cleaned by", f"Cleans in {CARPET_YEAR}", "Task title", "Task status",
                "Result", "Notes", "My note", "Dismissed tasks",
                "Calendar link", "Task link", "Checked at (UTC)"])
    for r in rows:
        # "Result" states plainly what a blank date means, so the spreadsheet can
        # never be read as "no cleaning" for a house that was never actually read.
        if not r.get("scanned"):
            result, note = "NOT CHECKED", "no data for this house yet"
        elif r.get("unread"):
            result, note = "COULD NOT READ", r.get("error") or "Breezeway read failed"
        elif r.get("last_clean_date"):
            result, note = "CLEANED", r.get("error") or ""
        else:
            result, note = f"NO CLEAN IN {CARPET_YEAR}", r.get("error") or ""
        if r.get("truncated"):
            note = (note + " | hit page limit, may be incomplete").strip(" |")
        # Links so a row can be checked against Breezeway directly. The task link
        # is blank unless there is a task to point at.
        pid_s    = str(r.get("property_id") or "")
        cal_link = f"{BW_APP}/property/{pid_s}/calendar" if pid_s else ""
        tid_s    = str(r.get("task_id") or "")
        task_link = f"{BW_APP}/task/{tid_s}" if tid_s else ""
        # A completed clean with nobody assigned is stated, not left blank — an
        # empty cell reads as "not checked" rather than as the answer it is.
        who = r.get("assignees") or ("unassigned" if r.get("last_clean_date") else "")
        w.writerow([
            r.get("house_name", ""), pid_s,
            r.get("last_clean_date") or "", who, r.get("clean_count") or 0,
            r.get("task_title") or "", r.get("task_status") or "",
            result, note, r.get("note") or "",
            # Named plainly so the spreadsheet says the number shown was corrected
            # by hand — otherwise a dismissal is invisible outside the app.
            len(r.get("dismissed_ids") or []) or "",
            cal_link, task_link, r.get("scanned_at") or "",
        ])

    stamp = datetime.utcnow().strftime("%Y%m%d")
    return Response(
        buf.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition":
                 f'attachment; filename="carpet-cleaning-{CARPET_YEAR}-{stamp}.csv"'},
    )


@carpet_scan_bp.route("/carpet/reset", methods=["POST"])
@login_required
def carpet_reset():
    """Clear results so the next scan re-reads them.

    Scoped by request: 'errors' re-reads only the houses that failed, which is the
    common case after a throttled run and costs a fraction of a full rescan."""
    scope = (request.json or {}).get("scope", "all")
    conn = get_db(); cur = get_cursor(conn)
    if scope == "errors":
        # unread only. A house whose carpet task is merely "scheduled, not done"
        # also carries explanatory text, but it was read correctly and re-reading
        # it just spends API calls to get the same answer.
        cur.execute("""DELETE FROM carpet_last_clean
                       WHERE user_id = %s AND year = %s AND unread = 1""",
                    (_uid(), CARPET_YEAR))
    else:
        cur.execute("DELETE FROM carpet_last_clean WHERE user_id = %s AND year = %s",
                    (_uid(), CARPET_YEAR))
    removed = cur.rowcount
    conn.commit(); cur.close(); conn.close()
    return jsonify({"success": True, "cleared": removed})


@carpet_scan_bp.route("/carpet/delete", methods=["POST"])
@login_required
def carpet_delete():
    """Delete everything this page has saved for this user — results and the tag
    choice. Download the CSV first if you want to keep a copy; this does not touch
    Breezeway, only what the page stored."""
    conn = get_db(); cur = get_cursor(conn)
    cur.execute("DELETE FROM carpet_last_clean WHERE user_id = %s", (_uid(),))
    removed = cur.rowcount
    cur.execute("DELETE FROM carpet_scan_prefs WHERE user_id = %s", (_uid(),))
    # Notes and dismissals go too. This is the one button that means "forget all
    # of it" — leaving hand-written notes behind would resurrect them against a
    # fresh scan, attached to houses the user thought they had cleared.
    cur.execute("DELETE FROM carpet_row_edits WHERE user_id = %s", (_uid(),))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"success": True, "deleted": removed})


@carpet_scan_bp.route("/carpet/note", methods=["POST"])
@login_required
def carpet_note():
    """Save (or clear) the free-text note on one house. Survives every rescan."""
    body = request.json or {}
    pid  = str(body.get("property_id") or "").strip()
    if not pid:
        return jsonify({"error": "property_id is required"}), 400
    note = str(body.get("note") or "").strip()[:1000]
    edit = _save_edit(CARPET_YEAR, pid, note=note)
    return jsonify({"success": True, "property_id": pid, **edit})


@carpet_scan_bp.route("/carpet/dismiss", methods=["POST"])
@login_required
def carpet_dismiss():
    """Mark a task as "not really a carpet clean" — or put it back.

    The next surviving task for that house is promoted in its place, so the row
    keeps showing a real cleaning date instead of collapsing to "none". Nothing is
    sent to Breezeway: the task is untouched there, this only changes what this
    page counts."""
    body = request.json or {}
    pid  = str(body.get("property_id") or "").strip()
    tid  = str(body.get("task_id") or "").strip()
    if not pid or not tid:
        return jsonify({"error": "property_id and task_id are required"}), 400

    edits   = _load_edits(CARPET_YEAR)
    current = set((edits.get(pid) or {}).get("dismissed") or [])
    if body.get("undo"):
        current.discard(tid)
    else:
        current.add(tid)
    _save_edit(CARPET_YEAR, pid, dismissed=current)

    # Hand back the recomputed row so the page can repaint just this one without
    # reloading — and without the client duplicating the promotion rule.
    results = _load_results(CARPET_YEAR)
    row     = results.get(pid)
    if not row:
        return jsonify({"success": True, "property_id": pid, "row": None})

    # Scanned before the runners-up were stored, so there is nothing to promote in
    # place of the task just dismissed. Drop the cached row: that makes this one
    # house pending again, so "Scan remaining" re-reads it (a single house, not a
    # rescan of 293) and the dismissal — which lives in its own table — applies to
    # the fresh result. Leaving a stale row here would strand the house showing an
    # answer the user has already rejected.
    if not row.get("hits_json") and not body.get("undo"):
        conn = get_db(); cur = get_cursor(conn)
        cur.execute("""DELETE FROM carpet_last_clean
                       WHERE user_id = %s AND year = %s AND property_id = %s""",
                    (_uid(), CARPET_YEAR, pid))
        conn.commit(); cur.close(); conn.close()
        return jsonify({"success": True, "property_id": pid, "rescan_needed": True,
                        "row": {"property_id": pid, "house_name": row.get("house_name"),
                                "scanned": False, "clean_count": 0,
                                "note": (_load_edits(CARPET_YEAR).get(pid) or {}).get("note", ""),
                                "dismissed_ids": sorted(current)}})

    row = dict(row); row["scanned"] = True
    return jsonify({"success": True, "property_id": pid,
                    "row": _apply_edit(row, _load_edits(CARPET_YEAR).get(pid))})
