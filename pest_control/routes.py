"""
pest_control/routes.py — Pest / Rodent task lookup (hidden page, any logged-in user).

Self-contained package: this blueprint + its own templates/ live under pest_control/
so the feature is isolated from the rest of the codebase.

Finds Breezeway tasks named "Rodent Mitigation" or "Pest Control" between two
dates and shows the useful data — with a best-effort answer to "was a VENDOR
involved?". Breezeway models vendors / service providers as a distinct person
ROLE (separate from internal staff), so the signal lives on the assignee's person
record, plus task-level costs / bill-to / rate. We surface all of it (and the raw
data) so the exact vendor field can be confirmed against real tasks.

Hidden: reachable only by URL (/pest-check), not linked in the nav.

Endpoints:
  GET  /pest-check        — page
  POST /pest-check/scan   — scan a date range and return matches (JSON)
"""

import re
import time
import requests
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor

from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required

pest_check_bp = Blueprint("pest_check", __name__, template_folder="templates")

BW_BASE = "https://api.breezeway.io"

# Task name must contain one of these (flexible spacing).
NAME_RX = re.compile(r"rodent\s*mitigation|pest\s*control", re.IGNORECASE)

# Person fields / values that hint the assignee is an external vendor.
_VENDOR_WORDS = ("vendor", "supplier", "service provider", "service_provider",
                 "serviceprovider", "contractor", "partner", "3rd party", "third party")


def _get_token():
    from routes.briefing import _get_breezeway_token
    return _get_breezeway_token()


def _people_map(token: str) -> dict:
    """{person_id: full person dict} for every person (all statuses)."""
    from routes.briefing import _fetch_bw_endpoint
    out = {}
    # No status filter → all people; fall back to active if the API needs one.
    for params in ({}, {"status": "active"}):
        results, err, status = _fetch_bw_endpoint(token, "/public/inventory/v1/people", params)
        for p in (results or []):
            if isinstance(p, dict) and p.get("id") is not None:
                out[p["id"]] = p
        if out:
            break
    return out


_ROLE_FIELDS = ("type_user", "user_type", "role", "type", "account_type", "kind")


def _value_is_vendor(value) -> bool:
    return any(w in str(value).lower() for w in _VENDOR_WORDS)


def _person_role(person: dict):
    """(field, value) of the best role/type field on a person, else (None, None)."""
    if not isinstance(person, dict):
        return (None, None)
    for f in _ROLE_FIELDS:
        v = person.get(f)
        if v not in (None, ""):
            return (f, v)
    for f in ("company", "company_name"):
        if person.get(f):
            return (f, person.get(f))
    return (None, None)


def _assignee_role_fields(person: dict) -> dict:
    """Pull the role/type-ish fields off a person so the UI can show the truth."""
    if not isinstance(person, dict):
        return {}
    keys = ("type_user", "user_type", "role", "type", "account_type", "kind",
            "company", "company_name", "is_vendor", "is_supplier", "employee_code", "status")
    return {k: person.get(k) for k in keys if k in person}


def _vendor_verdict(task: dict, people_by_id: dict):
    """Returns (verdict, evidence). verdict ∈ {'vendor','internal','unknown'} and is
    decided SOLELY by the assignee's Breezeway role — the only thing that actually
    proves who did the work. Costs / bill-to / rate are shown as CONTEXT only; they
    never set the verdict (internal tasks have those too). Each evidence item is
    {label, field, value, means, vendor}."""
    evidence = []
    any_vendor    = False
    any_role_read = False

    # WHO performed it — the role on the assignee's person record is the proof.
    assignments = task.get("assignments") or []
    if not assignments:
        evidence.append({"label": "No one is assigned", "field": "task.assignments",
                         "value": "empty", "means": "nobody is recorded on this task", "vendor": False})
    for a in assignments:
        person = people_by_id.get(a.get("assignee_id")) or {}
        field, value = _person_role(person)
        is_v = _value_is_vendor(value) if value is not None else False
        if value is not None:
            any_role_read = True
        if is_v:
            any_vendor = True
        evidence.append({
            "label": f"Assigned to “{a.get('name') or 'unknown'}”",
            "field": (f"person record → {field}" if field else "person record (no role/type field present)"),
            "value": value if value is not None else "(role not found in the person record)",
            "means": ("this person's Breezeway role reads as an OUTSIDE VENDOR / provider" if is_v
                      else "this person's Breezeway role reads as INTERNAL staff" if value is not None
                      else "couldn't read a role here — can't tell vendor vs staff from this"),
            "vendor": is_v,
        })

    # CONTEXT only — never decides the verdict (internal tasks can have these too).
    costs = task.get("costs") or []
    if costs:
        amts = [str(c.get("amount") or c.get("cost") or c.get("total") or c.get("value") or "?")
                for c in costs[:6] if isinstance(c, dict)]
        evidence.append({"label": "Cost recorded (context)", "field": "task.costs",
                         "value": f"{len(costs)} line(s)" + (f" — {', '.join(amts)}" if amts else ""),
                         "means": "a charge was logged — NOT proof of a vendor on its own", "vendor": False})
    bt = task.get("bill_to")
    if bt and str(bt).strip().lower() not in ("", "review", "none", "null"):
        evidence.append({"label": "Bill-to set (context)", "field": "task.bill_to", "value": bt,
                         "means": "billed to a party — NOT proof of a vendor on its own", "vendor": False})
    if task.get("rate_paid"):
        evidence.append({"label": "Pay rate (context)", "field": "task.rate_paid / rate_type",
                         "value": f"{task.get('rate_paid')} ({task.get('rate_type')})",
                         "means": "a pay rate — staff or vendor, not decisive", "vendor": False})

    verdict = "vendor" if any_vendor else ("internal" if any_role_read else "unknown")
    return (verdict, evidence)


@pest_check_bp.route("/pest-check")
@login_required
def pest_check_page():
    return render_template("pest_check.html")


@pest_check_bp.route("/pest-check/scan", methods=["POST"])
@login_required
def pest_check_scan():
    try:
        return _scan_inner()
    except Exception as e:
        import traceback
        return jsonify({"error": f"{type(e).__name__}: {e}\n\n{traceback.format_exc()}"}), 500


def _scan_inner():
    from routes.briefing import (_fetch_bw_endpoint, _ensure_property_cache,
                                 _get_live_property_cache, _get_live_ref_cache,
                                 _get_property_name)

    token = _get_token()
    if not token:
        return jsonify({"error": "Breezeway not configured."})

    payload = request.get_json(silent=True) or {}
    today   = date.today()

    def _parse(v, default):
        try:
            return date.fromisoformat(str(v)[:10])
        except (ValueError, TypeError):
            return default

    start = _parse(payload.get("from"), today)
    end   = _parse(payload.get("to"),   today + timedelta(days=30))
    if end < start:
        start, end = end, start
    date_range = f"{start.isoformat()},{end.isoformat()}"

    _ensure_property_cache()
    prop_cache = _get_live_property_cache()
    ref_cache  = _get_live_ref_cache()
    people     = _people_map(token)

    pid_candidates = {}
    for bw_pid in prop_cache:
        ref_id = ref_cache.get(bw_pid)
        pid_candidates.setdefault(ref_id if ref_id else str(bw_pid), bw_pid)

    def _tasks_for_ref(ref_id):
        """(tasks, ok) — ok=False means the property couldn't be loaded (no silent drop)."""
        for attempt in range(3):
            results, err, status = _fetch_bw_endpoint(
                token, "/public/inventory/v1/task",
                {"reference_property_id": ref_id, "scheduled_date": date_range})
            if status == 200:
                return (results or [], True)
            if status is None or status == 429 or status >= 500:
                time.sleep(0.3 * (attempt + 1))
                continue
            return ([], False)
        return ([], False)

    all_tasks, failed = [], 0
    with ThreadPoolExecutor(max_workers=16) as ex:
        for tasks, ok in ex.map(_tasks_for_ref, list(pid_candidates.keys())):
            all_tasks.extend(tasks)
            if not ok:
                failed += 1

    seen, matches = set(), []
    for t in all_tasks:
        tid = t.get("id")
        if tid in seen:
            continue
        seen.add(tid)
        name = t.get("name") or t.get("title") or ""
        if isinstance(name, dict):
            name = name.get("value") or name.get("name") or ""
        if not NAME_RX.search(str(name)):
            continue

        home_id = t.get("home_id") or t.get("property_id")
        assignees = []
        for a in (t.get("assignments") or []):
            person = people.get(a.get("assignee_id")) or {}
            assignees.append({
                "name":  a.get("name") or "(unknown)",
                "id":    a.get("assignee_id"),
                "roles": _assignee_role_fields(person),   # the type/role truth
                "person_raw": person,                     # full record for verification
            })

        verdict, evidence = _vendor_verdict(t, people)
        status_obj = t.get("type_task_status") or {}
        status_str = (status_obj.get("name") if isinstance(status_obj, dict) else status_obj) or ""

        matches.append({
            "task_id":     tid,
            "name":        str(name),
            "property":    _get_property_name(home_id),
            "scheduled":   (str(t.get("scheduled_date") or "")[:10]) or None,
            "time":        (str(t.get("scheduled_time") or "")[:5]) or None,
            "status":      status_str,
            "finished_at": (str(t.get("finished_at") or "")[:16]) or None,
            "department":  t.get("type_department"),
            "assignees":   assignees,
            "vendor_verdict":  verdict,            # 'vendor' | 'internal' | 'unknown'
            "vendor_involved": verdict == "vendor",
            "evidence":        evidence,
            # Task-level vendor/cost signals, surfaced plainly:
            "bill_to":     t.get("bill_to"),
            "costs":       t.get("costs") or [],
            "rate_paid":   t.get("rate_paid"),
            "rate_type":   t.get("rate_type"),
            "description": (t.get("description") or "")[:1000],
            "raw":         t,        # full task for the collapsible raw view
        })

    matches.sort(key=lambda x: (x["scheduled"] or "", (x["property"] or "").lower()))
    return jsonify({
        "from": start.isoformat(), "to": end.isoformat(),
        "count": len(matches),
        "failed_properties": failed,
        "scanned_properties": len(pid_candidates),
        "people_loaded": len(people),
        "matches": matches,
    })
