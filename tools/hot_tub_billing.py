#!/usr/bin/env python3
"""
Hot Tub Billing — Monthly Owner Worksheet  (read-only Breezeway engine)
======================================================================

Counts & classifies hot-tub SERVICES for one month and produces a per-property
worksheet that Madeline bills OWNERS by hand in Streamline. This program NEVER
writes to Breezeway and never bills anything — it only reads, classifies, and
lays out the evidence for human approval. Nothing it outputs is final; every row
shows both dates and the reason it was included or excluded so it can be checked.

Mirrors the read-only/never-fail-silently spine of productivity_past_365_days.py.

------------------------------------------------------------------------------
RULES (locked with the operator — see memory project_hot_tub_billing):
  • Classify from the TITLE + DESCRIPTION TEXT ONLY (typo-tolerant). Task tags
    are shown for context but NEVER drive a billing decision (she rarely sets
    them and they can be wrong). bill_to / costs are ignored for logic.
  • Prices: Regular $50 (arrival / biweekly / weekly / monthly hot tub service),
    Dump & Scrub $155, WWM / White-Water-Mold / Bacterial $250.
    "partial" → billed as Regular $50 but FLAGGED.
  • Floors (bill at least the minimum even if fewer happened): property tag
    "Hot Tub - TG Service" ≥2/mo, "Weekly Service" ≥4/mo, "Monthly Service" ≥1/mo.
  • Excluded (tenant / not owner): "lease hot tub service", "prepaid",
    "post lease …" — BUT "lease arrival hot tub service" IS owner-billable.
    Also excluded: "*no charge" / "do not bill" / "do not service".
  • Excluded (not a service): leaks, repairs, covers, lifts, filters, locks,
    inspections, walk-thrus, post-rental, daily/lease issues, disarm bear fence…
  • Completed = status in approved / finished / closed.
  • Month boundary: a service on the 30th can show on the 1st — BOTH dates are
    shown and cross-month rows are flagged. Billing month = the SCHEDULED month.

------------------------------------------------------------------------------
USAGE
  Credentials come from .env / env (BREEZEWAY_CLIENT_ID, BREEZEWAY_CLIENT_SECRET).

      python hot_tub_billing.py --month 2026-05
        → writes hot_tub_billing_<month>.json   (for the in-app page)
               + hot_tub_billing_<month>.csv     (one row per candidate service)
               + hot_tub_billing_<month>.md      (human worksheet)

  Options:
      --month YYYY-MM     billing month (default: last full month)
      --outdir DIR        output dir (default ".")
      --workers N         parallel property fetches (default 12)
      --max-properties N  TESTING ONLY: first N tagged properties (forces PARTIAL)

  Exit code: 0 = COMPLETE, non-zero = PARTIAL or HALTED.
"""

from __future__ import annotations

import argparse
import calendar
import csv
import os
import re
import sys
import time
import json
from collections import defaultdict, Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta

import requests

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ── Config ───────────────────────────────────────────────────────────────────

BASE = "https://api.breezeway.io"
AUTH_URL     = f"{BASE}/public/auth/v1/"
PROPERTY_URL = f"{BASE}/public/inventory/v1/property"
TASK_URL     = f"{BASE}/public/inventory/v1/task"

CLIENT_ID     = os.environ.get("BREEZEWAY_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("BREEZEWAY_CLIENT_SECRET", "")

# WHICH HOUSES ARE SCANNED FOR HOT-TUB BILLING (selection by property tag only —
# there is NO minimum/floor charge). A house qualifies if it carries a "Hot Tub"
# property tag: either the plain "Hot Tub" tag OR any "Hot Tub - … Service"
# variant (TG / PRS / Vendor / Weekly / TG Service Once Per Month, etc.). We match
# by clause, not exact literal, so wording variants and trailing spaces still hit.
def hot_tub_tag(tags):
    """Return the property tag that qualifies this house for hot-tub billing —
    prefer the specific 'Hot Tub - … Service' tag, else the plain 'Hot Tub' tag —
    or '' if the house has neither. `tags` is a list of tag dicts or strings."""
    plain = service = ""
    for t in tags:
        name = (t.get("name") or t.get("label") or "") if isinstance(t, dict) else t
        n = str(name).strip()
        low = n.lower()
        if low == "hot tub":
            plain = n
        elif low.startswith("hot tub") and "service" in low:
            service = n   # a service variant is more specific than plain "Hot Tub"
    return service or plain


# Special houses that must ALWAYS be scanned even without a hot-tub tag.
# Keyed by Breezeway property id (string). Aerial Grace bills cold plunge (+ hot
# tub) but has no hot-tub tag, so without this it would be skipped entirely.
ALWAYS_INCLUDE = {
    "1137736": {"name": "Aerial Grace Lakeside Retreat"},
}

PRICE_REGULAR     = 50
PRICE_DS          = 155
PRICE_WWM         = 250
PRICE_COLD_PLUNGE = 75    # flat, any type (small tub — billed Regular even if D&S/WWM)

# Completed statuses (confirmed from the sample: approved/finished/closed).
DONE_STATUSES = {"approved", "finished", "closed", "complete", "completed", "done"}

# ── Classification regexes (TITLE/DESCRIPTION text only, typo-tolerant) ───────

# A task is a hot-tub SERVICE candidate when its TITLE names a hot-tub service.
# Repairs/issues/inspections ("hot tub leak", "ht not heating", "cover", …) do
# NOT contain "hot tub service" / "ht service", so this gate cleanly drops them.
SERVICE_TITLE = re.compile(r"hot\s*tub\s*service|\bht\s*service\b", re.IGNORECASE)

# Anything that merely mentions a tub but ISN'T caught as a service goes to
# NEEDS-REVIEW (never silently dropped) so the operator can eyeball it.
MENTIONS_TUB = re.compile(r"hot\s*tub|hottub|\bht\b|\bspa\b|\btub\b", re.IGNORECASE)

# Cold plunge is a distinct billable service (flat rate). "CPS" = Cold Plunge
# Service, which techs use in summaries ("CPS complete").
COLD_PLUNGE = re.compile(r"cold\s*plunge|\bcps\b", re.IGNORECASE)

NO_CHARGE   = re.compile(r"no\s*(ho\s*)?charge|do\s*not\s*bill|do\s*not\s*service|no\s*ho\s*charge", re.IGNORECASE)
POST_LEASE  = re.compile(r"post[\s\-]*lease", re.IGNORECASE)
LEASE_WORD  = re.compile(r"\blease\b|\bprepaid\b", re.IGNORECASE)
ARRIVAL     = re.compile(r"\barrival\b", re.IGNORECASE)

WWM_RX      = re.compile(r"\bwwm\b|white\s*water\s*mold|bacteri", re.IGNORECASE)
DS_RX       = re.compile(r"\bd\s*&\s*s\b|\bd\s*/\s*s\b|\bd\s*and\s*s\b|dump\s*&?\s*(and\s*)?scrub|dump\s*n+\s*scrub", re.IGNORECASE)
PARTIAL_RX  = re.compile(r"partial", re.IGNORECASE)

MAX_ATTEMPTS = 5
BACKOFF_CAP  = 60
PAGE_LIMIT   = 100
_ALLOWED_METHODS = {"GET", "POST"}
_SESSION = requests.Session()


# ── Report ───────────────────────────────────────────────────────────────────

class RunReport:
    def __init__(self):
        self.failures, self.warnings, self.log_lines = [], [], []
        self.properties_total = 0
        self.properties_tagged = 0
        self.properties_scanned = 0
        self.candidate_tasks = 0

    def fail(self, m): self._emit(f"[FAIL] {m}"); self.failures.append(m)
    def warn(self, m): self._emit(f"[WARN] {m}"); self.warnings.append(m)
    def info(self, m): self._emit(f"[info] {m}")

    def _emit(self, line):
        full = f"{datetime.now().strftime('%H:%M:%S')} {line}"
        self.log_lines.append(full); print(full, flush=True)

    @property
    def is_complete(self): return not self.failures


REPORT = RunReport()


# ── HTTP (read-only) ─────────────────────────────────────────────────────────

def _assert_readonly(method):
    if method.upper() not in _ALLOWED_METHODS:
        raise RuntimeError(f"READ-ONLY VIOLATION: attempted HTTP {method}. Aborting.")


def _retry_delay(resp, attempt):
    for h in ("Retry-After", "retry-after", "X-RateLimit-Reset", "x-ratelimit-reset"):
        v = resp.headers.get(h)
        if v:
            try: return min(BACKOFF_CAP, max(1.0, float(v)))
            except ValueError: pass
    return min(BACKOFF_CAP, float(2 ** attempt))


def bw_get(url, params, token, what):
    _assert_readonly("GET")
    last = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            resp = _SESSION.get(url, headers={"Authorization": f"JWT {token}"},
                                params=params, timeout=30)
        except requests.exceptions.Timeout:
            d = min(BACKOFF_CAP, float(2 ** attempt))
            REPORT.warn(f"{what}: timeout (attempt {attempt}/{MAX_ATTEMPTS}); waiting {d:.0f}s")
            time.sleep(d); continue
        except Exception as ex:
            return None, f"network error: {ex}", last
        last = resp.status_code
        if resp.status_code == 200:
            try: return resp.json(), "", 200
            except Exception as ex:
                return None, f"200 but unparseable JSON: {ex} | body={resp.text[:200]!r}", 200
        if resp.status_code == 429 or resp.status_code >= 500:
            d = _retry_delay(resp, attempt)
            REPORT.warn(f"{what}: HTTP {resp.status_code} (attempt {attempt}/{MAX_ATTEMPTS}); waiting {d:.0f}s")
            time.sleep(d); continue
        detail = resp.text[:300]
        try: detail = resp.json()
        except Exception: pass
        return None, f"HTTP {resp.status_code}: {detail}", resp.status_code
    return None, f"gave up after {MAX_ATTEMPTS} attempts (last {last})", last


def bw_get_paginated(url, params, token, what):
    out, page = [], 1
    while True:
        data, err, _ = bw_get(url, {**params, "limit": PAGE_LIMIT, "page": page}, token, f"{what} (page {page})")
        if err:
            return out, err
        if isinstance(data, dict):
            results = data.get("results", data.get("data", data.get("tasks", [])))
        elif isinstance(data, list):
            results = data
        else:
            results = None
        if results is None:
            return out, f"malformed page {page}: got {type(data).__name__}"
        out.extend(results)
        if len(results) < PAGE_LIMIT:
            return out, ""
        page += 1


def authenticate():
    _assert_readonly("POST")
    if not CLIENT_ID or not CLIENT_SECRET:
        REPORT.fail("Missing BREEZEWAY_CLIENT_ID / BREEZEWAY_CLIENT_SECRET."); _halt("No credentials.")
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            resp = _SESSION.post(AUTH_URL, json={"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET}, timeout=30)
        except Exception as ex:
            REPORT.warn(f"auth: network error {ex} (attempt {attempt}/{MAX_ATTEMPTS})")
            time.sleep(min(BACKOFF_CAP, float(2 ** attempt))); continue
        if resp.status_code == 200:
            try: token = resp.json().get("access_token")
            except Exception as ex:
                REPORT.fail(f"auth: 200 unparseable: {ex}"); _halt("Auth unparseable.")
            if not token:
                REPORT.fail(f"auth: no token: {resp.text[:200]!r}"); _halt("Auth no token.")
            REPORT.info("Authenticated with Breezeway."); return token
        if resp.status_code == 429:
            d = max(_retry_delay(resp, attempt), 60.0)
            REPORT.warn(f"auth: 429 (1/min). Waiting {d:.0f}s (attempt {attempt}/{MAX_ATTEMPTS}).")
            time.sleep(d); continue
        if resp.status_code >= 500:
            d = _retry_delay(resp, attempt)
            REPORT.warn(f"auth: {resp.status_code} transient (attempt {attempt}/{MAX_ATTEMPTS}); waiting {d:.0f}s")
            time.sleep(d); continue
        REPORT.fail(f"auth: HTTP {resp.status_code}: {resp.text[:300]!r}"); _halt("Auth failed.")
    REPORT.fail("auth: exhausted retries."); _halt("Auth failed after retries.")


# ── Field helpers ────────────────────────────────────────────────────────────

def _s(v):
    if isinstance(v, dict):
        return str(v.get("value") or v.get("name") or v.get("label") or "")
    return str(v or "")


def task_title(t):
    for k in ("title", "name", "template_name", "task_name"):
        if t.get(k):
            return _s(t.get(k)).strip()
    return ""


def task_desc(t):
    for k in ("description", "notes", "instructions"):
        if t.get(k):
            return _s(t.get(k)).strip()
    return ""


def task_summary(t):
    """The tech's completion write-up — this is where they confirm what they
    actually did (regular / WWM / D&S / partial …). Distinct from `description`
    (which is just the template name). Kept separate so it can be shown verbatim
    on the worksheet for human confirmation and used to flag mis-priced rows."""
    for k in ("summary", "report", "completion_note", "result"):
        if t.get(k):
            return _s(t.get(k)).strip()
    return ""


def task_status(t):
    for k in ("type_task_status", "status", "state"):
        if t.get(k):
            return _s(t.get(k)).lower().strip()
    return ""


def task_completion_date(t):
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


def task_assignees(t):
    out = []
    for a in (t.get("assignments") or []):
        if isinstance(a, dict):
            n = (a.get("name") or a.get("full_name") or
                 f"{a.get('first_name','').strip()} {a.get('last_name','').strip()}".strip())
            if n:
                out.append(n)
    return out


def task_tag_names(t):
    out = []
    for tag in (t.get("task_tags") or t.get("tags") or []):
        nm = (tag.get("name") or tag.get("label") or "") if isinstance(tag, dict) else tag
        nm = str(nm).strip()
        if nm:
            out.append(nm)
    return out


# ── Classifier (text only) ───────────────────────────────────────────────────

def classify(title, desc, summary=""):
    """Return dict: {disposition, service_type, price, reason, flags[]}.

    disposition ∈ {billable, excluded, review}. Pricing/inclusion decisions use
    the TITLE (+ description) ONLY — never tags — so the locked rules stay stable.
    The tech's completion `summary` is used ONLY to raise a confirm-flag when it
    names a heavier service than the title priced (regular vs WWM/D&S); it never
    silently changes the price. price is 0 unless billable.
    """
    blob = f"{title}  {desc}".strip()
    tl = title.lower()
    flags = []

    is_service_title = bool(SERVICE_TITLE.search(title))
    is_cold_plunge   = bool(COLD_PLUNGE.search(title))

    # 1) Explicit no-charge / do-not-bill wins over everything.
    if NO_CHARGE.search(blob):
        return {"disposition": "excluded", "service_type": "no_charge", "price": 0,
                "reason": "marked no-charge / do-not-bill", "flags": flags}

    # 2) Post-lease (incl. post-lease D&S) is excluded even if it says "arrival".
    if POST_LEASE.search(blob):
        return {"disposition": "excluded", "service_type": "post_lease", "price": 0,
                "reason": "post-lease service (tenant/owner handled elsewhere)", "flags": flags}

    # 3) Lease: a lease ARRIVAL service is owner-billable; any other lease/prepaid
    #    service is tenant-billed and excluded.
    if LEASE_WORD.search(blob):
        if ARRIVAL.search(blob) and (is_service_title or is_cold_plunge):
            flags.append("lease-arrival → owner-billable (confirm)")
            # falls through to be priced as a normal service below
        else:
            return {"disposition": "excluded", "service_type": "lease_tenant", "price": 0,
                    "reason": "lease/prepaid service — billed to tenant", "flags": flags}

    # 3b) Cold plunge is its own billable service: FLAT $75, any type. Techs bill
    #     Regular even when the note says D&S/WWM ("charge regular as this a cold
    #     plunge and only 1/3 of size"), so we do NOT raise an up-charge flag.
    if is_cold_plunge:
        if PARTIAL_RX.search(blob):
            flags.append("partial — confirm")
        return {"disposition": "billable", "service_type": "cold_plunge",
                "price": PRICE_COLD_PLUNGE, "reason": "owner-billable cold-plunge service",
                "flags": flags}

    # 4) Must read as an actual hot-tub service in the TITLE; otherwise it's a
    #    repair/issue/inspection (or unknown) — never silently dropped.
    if not is_service_title:
        if MENTIONS_TUB.search(blob):
            return {"disposition": "review", "service_type": "non_service?", "price": 0,
                    "reason": "mentions a hot tub but title is not a service — confirm", "flags": flags}
        return {"disposition": "excluded", "service_type": "non_service", "price": 0,
                "reason": "not a hot-tub service", "flags": flags}

    # 5) It's a service — price by type from the TITLE.
    partial = bool(PARTIAL_RX.search(blob))
    if WWM_RX.search(tl):
        stype, price = "wwm", PRICE_WWM
    elif DS_RX.search(tl):
        stype, price = "dump_scrub", PRICE_DS
    else:
        stype, price = "regular", PRICE_REGULAR
        # The description OR the tech's completion summary hints at a heavier
        # service the title didn't name — flag it, don't silently re-price (human
        # confirms). The tech summary is the operator's stated source of truth for
        # what was actually done, so scanning it here catches $50-vs-$250 misses.
        hint = f"{desc}  {summary}".strip()
        if WWM_RX.search(hint) and not re.search(r"do\s*not", hint, re.IGNORECASE):
            where = "tech summary" if (WWM_RX.search(summary) and not WWM_RX.search(desc)) else "description"
            flags.append(f"{where} mentions WWM — confirm $250")
        elif DS_RX.search(hint) and not re.search(r"do\s*not", hint, re.IGNORECASE):
            where = "tech summary" if (DS_RX.search(summary) and not DS_RX.search(desc)) else "description"
            flags.append(f"{where} mentions D&S — confirm $155")

    if partial:
        flags.append("partial — billed as Regular $50, confirm")
        stype, price = "regular", PRICE_REGULAR

    return {"disposition": "billable", "service_type": stype, "price": price,
            "reason": "owner-billable hot-tub service", "flags": flags}


# ── Properties + tags ────────────────────────────────────────────────────────

def property_id(p):
    return p.get("id") or p.get("property_id") or p.get("home_id")


def property_name(p):
    return (p.get("name") or p.get("property_name") or
            p.get("address") or _s(p.get("unit_address")) or str(property_id(p)))


def property_query_param(p):
    ref = p.get("reference_property_id")
    if ref:
        return {"reference_property_id": ref}
    pid = property_id(p)
    return {"home_id": pid} if pid is not None else {}


def fetch_properties(token):
    props, err = bw_get_paginated(PROPERTY_URL, {}, token, "properties")
    if err:
        REPORT.fail(f"Property list errored: {err} (got {len(props)} first).")
    if not props:
        REPORT.fail("Property list empty."); _halt("No properties.")
    return props


def fetch_property_tags(token, pid):
    for path in (f"/public/inventory/v1/property/{pid}/tags",
                 f"/public/inventory/v1/property/{pid}"):
        data, err, _ = bw_get(f"{BASE}{path}", {}, token, f"tags {pid}")
        if err:
            continue
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            tags = data.get("tags") or data.get("property_tags") or []
            if tags:
                return tags
    return []


def property_include_tag(token, pid):
    """The hot-tub property tag that qualifies this house for billing, or ''."""
    return hot_tub_tag(fetch_property_tags(token, pid))


def collect_tagged_properties(token, properties, workers):
    REPORT.properties_total = len(properties)
    REPORT.info(f"Classifying tags for {len(properties)} properties (workers={workers})…")
    tagged = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(property_include_tag, token, property_id(p)): p for p in properties}
        for fut in as_completed(futs):
            p = futs[fut]
            try:
                tag = fut.result()
            except Exception as e:
                REPORT.warn(f"tag classify failed for {property_name(p)}: {e}"); continue
            # Force-include special houses (e.g. Aerial Grace) even without a tag.
            if not tag and str(property_id(p)) in ALWAYS_INCLUDE:
                tag = "always-included (special house)"
            if tag:
                p["_include_tag"] = tag   # shown on each house entry so she knows WHY it's here
                tagged.append(p)
    REPORT.properties_tagged = len(tagged)
    REPORT.info(f"{len(tagged)} properties carry a billing floor tag.")
    return tagged


# ── Month + task fetch ───────────────────────────────────────────────────────

def month_bounds(month_str):
    y, m = int(month_str[:4]), int(month_str[5:7])
    first = date(y, m, 1)
    last = date(y, m, calendar.monthrange(y, m)[1])
    return first, last


def fetch_tasks(token, p, start, end):
    qp = property_query_param(p)
    if not qp:
        return [], "no usable property id"
    drange = f"{start.isoformat()},{end.isoformat()}"
    return bw_get_paginated(TASK_URL, {**qp, "scheduled_date": drange}, token,
                            f"tasks {property_name(p)}")


# ── Build worksheet ──────────────────────────────────────────────────────────

def build(token, properties, month_str, workers, max_props):
    first, last = month_bounds(month_str)
    # Pad ±4 days to catch the 30th-shows-as-1st boundary quirk.
    pad_start, pad_end = first - timedelta(days=4), last + timedelta(days=4)
    mkey = month_str  # "YYYY-MM"

    tagged = collect_tagged_properties(token, properties, workers)
    if max_props:
        REPORT.warn(f"TEST MODE: first {max_props} of {len(tagged)} tagged properties.")
        tagged = tagged[:max_props]

    def work(p):
        tasks, err = fetch_tasks(token, p, pad_start, pad_end)
        return p, tasks, err

    props_out = []
    type_totals = Counter()
    money_total = 0

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for p, tasks, err in ex.map(work, tagged):
            if err:
                REPORT.fail(f"{property_name(p)}: {err} — services MISSING from worksheet.")
                continue
            REPORT.properties_scanned += 1
            rows = []
            for t in tasks:
                title, desc = task_title(t), task_desc(t)
                summary = task_summary(t)
                sched = str(t.get("scheduled_date") or "")[:10]
                comp = task_completion_date(t)
                status = task_status(t)
                done = status in DONE_STATUSES

                # Belongs to this billing month by SCHEDULED month; but surface any
                # task COMPLETED in-month whose schedule landed in an adjacent month
                # (the boundary quirk) so nothing is silently lost.
                in_sched = sched[:7] == mkey
                in_comp = comp[:7] == mkey if comp else False
                if not (in_sched or in_comp):
                    continue
                blob = f"{title} {desc}"
                if not (MENTIONS_TUB.search(blob) or COLD_PLUNGE.search(blob)):
                    continue

                REPORT.candidate_tasks += 1
                c = classify(title, desc, summary)
                flags = list(c["flags"])
                if not done:
                    flags.append(f"NOT completed yet (status={status or 'unknown'})")
                if in_sched and in_comp is False and comp:
                    pass
                if sched and comp and sched[:7] != comp[:7]:
                    flags.append(f"BOUNDARY: scheduled {sched} but completed {comp} — confirm month")
                if (in_comp and not in_sched):
                    flags.append(f"shows here by COMPLETED date ({comp}); scheduled {sched or '—'}")

                rows.append({
                    "task_id": t.get("id"),
                    "report_url": _s(t.get("report_url")).strip(),  # deep link to the tech's report
                    "title": title,
                    "description": desc[:160],
                    "summary": summary[:400],   # tech's completion note (verbatim)
                    "scheduled_date": sched,
                    "completed_date": comp,
                    "status": status,
                    "completed": done,
                    "disposition": c["disposition"],
                    "service_type": c["service_type"],
                    "price": c["price"],
                    "reason": c["reason"],
                    "flags": flags,
                    "assignees": task_assignees(t),
                    "task_tags": task_tag_names(t),   # context only
                })

            # Only completed, billable services are billed. NO floor minimum —
            # a house with zero services bills $0 (never charge for a service that
            # didn't happen). The tag is used only to decide which houses to scan.
            billable = [r for r in rows if r["disposition"] == "billable" and r["completed"]]
            visits = len(billable)
            subtotal = sum(r["price"] for r in billable)
            for r in billable:
                type_totals[r["service_type"]] += 1

            prop_total = subtotal
            money_total += prop_total

            # Sort rows: billable first, then review, then excluded; by date.
            order = {"billable": 0, "review": 1, "excluded": 2}
            rows.sort(key=lambda r: (order.get(r["disposition"], 3), r["scheduled_date"]))

            props_out.append({
                "property": property_name(p),
                "property_id": property_id(p),
                "included_by": p.get("_include_tag", ""),  # WHY this house is on the scan
                "floor": 0,                       # floor rule removed — no minimum
                "visits_billable": visits,
                "subtotal": subtotal,
                "floor_topup": 0,
                "floor_amount": 0,
                "total": prop_total,
                "rows": rows,
                "needs_review": sum(1 for r in rows if r["disposition"] == "review"),
                "flagged": sum(1 for r in rows if r["flags"]),
            })

    props_out.sort(key=lambda x: x["property"].lower())
    return {
        "month": month_str,
        "props": props_out,
        "type_totals": dict(type_totals),
        "money_total": money_total,
    }


# ── Output ───────────────────────────────────────────────────────────────────

CSV_FIELDS = ["property", "property_floor", "scheduled_date", "completed_date", "status",
              "disposition", "service_type", "price", "title", "description", "summary",
              "flags", "assignees", "task_tags", "reason"]


def write_csv(path, data):
    n = 0
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        w.writeheader()
        for prop in data["props"]:
            for r in prop["rows"]:
                w.writerow({
                    "property": prop["property"],
                    "property_floor": prop["floor"],
                    "scheduled_date": r["scheduled_date"],
                    "completed_date": r["completed_date"],
                    "status": r["status"],
                    "disposition": r["disposition"],
                    "service_type": r["service_type"],
                    "price": r["price"] if r["disposition"] == "billable" and r["completed"] else "",
                    "title": r["title"],
                    "description": r["description"],
                    "summary": r.get("summary", ""),
                    "flags": " | ".join(r["flags"]),
                    "assignees": "; ".join(r["assignees"]),
                    "task_tags": "; ".join(r["task_tags"]),
                    "reason": r["reason"],
                })
                n += 1
    return n


def write_md(path, data):
    L, A = [], None
    out = L.append
    out(f"# Hot Tub Billing Worksheet — {data['month']}")
    out("")
    status = "✅ COMPLETE" if REPORT.is_complete else f"⚠️ PARTIAL — {len(REPORT.failures)} failure(s)"
    out(f"**Status: {status}** · Generated {datetime.now().astimezone().isoformat(timespec='seconds')}")
    out("")
    out("> ⚠️ DRAFT for human review. Bill owners by hand in Streamline. Classification "
        "is from task TITLE/DESCRIPTION text (typos tolerated); task tags are shown for "
        "context only and do NOT affect billing. Confirm every flagged row.")
    out("")
    out(f"- Properties scanned: {REPORT.properties_scanned} / {REPORT.properties_tagged} tagged")
    out(f"- Candidate hot-tub tasks examined: {REPORT.candidate_tasks}")
    out(f"- Service counts: " + (", ".join(f"{k}×{v}" for k, v in data['type_totals'].items()) or "none"))
    out(f"- **Estimated owner total (incl. floors): ${data['money_total']:,}**")
    out("")
    out("| Service | Price |")
    out("|---|---|")
    out(f"| Regular (arrival/biweekly/weekly/monthly) | ${PRICE_REGULAR} |")
    out(f"| Dump & Scrub | ${PRICE_DS} |")
    out(f"| WWM / Bacterial | ${PRICE_WWM} |")
    out("")
    for prop in data["props"]:
        flag_note = f" · ⚑ {prop['flagged']} flagged" if prop["flagged"] else ""
        rev_note = f" · 🔎 {prop['needs_review']} to review" if prop["needs_review"] else ""
        floor_note = ""
        if prop["floor_topup"]:
            floor_note = (f" · ⬆ floor {prop['floor']}: only {prop['visits_billable']} performed, "
                          f"+{prop['floor_topup']}×${PRICE_REGULAR} minimum")
        out(f"## {prop['property']} — **${prop['total']:,}**"
            f" ({prop['visits_billable']} billable){floor_note}{flag_note}{rev_note}")
        out("")
        out("| sched | done | type | $ | title | flags |")
        out("|---|---|---|---|---|---|")
        for r in prop["rows"]:
            price = f"${r['price']}" if (r["disposition"] == "billable" and r["completed"]) else ""
            mark = {"billable": "", "review": "🔎 ", "excluded": "✕ "}.get(r["disposition"], "")
            typ = mark + r["service_type"]
            flags = "; ".join(r["flags"])
            title = r["title"].replace("|", "\\|")[:60]
            out(f"| {r['scheduled_date']} | {r['completed_date'] or '—'} | {typ} | {price} | {title} | {flags} |")
        if prop["floor_topup"]:
            out(f"| — | — | ⬆ floor minimum | ${prop['floor_amount']} | "
                f"{prop['floor_topup']} × Regular to meet {prop['floor']}/mo floor | |")
        out("")
    out("## Failures")
    if REPORT.failures:
        for m in REPORT.failures:
            out(f"- ❌ {m}")
    else:
        out("- None. 🎉")
    out("")
    out("## Warnings")
    if REPORT.warnings:
        for w in REPORT.warnings[:80]:
            out(f"- ⚠️ {w}")
        if len(REPORT.warnings) > 80:
            out(f"- …and {len(REPORT.warnings) - 80} more.")
    else:
        out("- None.")
    out("")
    out("## Log")
    out("```"); L.extend(REPORT.log_lines); out("```")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(L) + "\n")


def write_json(path, data):
    payload = {
        "title": "Hot Tub Billing Worksheet",
        "month": data["month"],
        "status": "COMPLETE" if REPORT.is_complete else "PARTIAL",
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "prices": {"regular": PRICE_REGULAR, "dump_scrub": PRICE_DS, "wwm": PRICE_WWM},
        "properties_total": REPORT.properties_total,
        "properties_tagged": REPORT.properties_tagged,
        "properties_scanned": REPORT.properties_scanned,
        "candidate_tasks": REPORT.candidate_tasks,
        "type_totals": data["type_totals"],
        "money_total": data["money_total"],
        "props": data["props"],
        "failures": REPORT.failures,
        "warnings_count": len(REPORT.warnings),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)


def _halt(reason):
    print("\n" + "!" * 78, file=sys.stderr)
    print(f"HALTED: {reason}", file=sys.stderr)
    print("This run produced NO trustworthy worksheet.", file=sys.stderr)
    print("!" * 78, file=sys.stderr)
    sys.exit(2)


# ── Main ─────────────────────────────────────────────────────────────────────

def _default_month():
    today = date.today()
    first_this = today.replace(day=1)
    last_prev = first_this - timedelta(days=1)
    return f"{last_prev.year:04d}-{last_prev.month:02d}"


def main():
    ap = argparse.ArgumentParser(description="Hot Tub Billing monthly worksheet — read-only.")
    ap.add_argument("--month", default="", help="Billing month YYYY-MM (default: last full month).")
    ap.add_argument("--outdir",
                    default=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "reports"),
                    help="Output directory (default <repo>/reports).")
    ap.add_argument("--workers", type=int, default=12, help="Parallel property fetches.")
    ap.add_argument("--max-properties", type=int, default=0, help="TESTING ONLY: first N tagged properties.")
    args = ap.parse_args()

    month_str = args.month or _default_month()
    if not re.fullmatch(r"\d{4}-\d{2}", month_str):
        print("--month must be YYYY-MM.", file=sys.stderr); sys.exit(2)

    REPORT.info(f"Hot Tub Billing — month {month_str}")
    token = authenticate()
    properties = fetch_properties(token)

    data = build(token, properties, month_str, args.workers, args.max_properties)

    os.makedirs(args.outdir, exist_ok=True)
    base = f"hot_tub_billing_{month_str}"
    json_path = os.path.join(args.outdir, base + ".json")
    csv_path  = os.path.join(args.outdir, base + ".csv")
    md_path   = os.path.join(args.outdir, base + ".md")
    # Also write a stable "latest" json the page can default to.
    latest_path = os.path.join(args.outdir, "hot_tub_billing_latest.json")

    write_json(json_path, data)
    write_json(latest_path, data)
    rows = write_csv(csv_path, data)
    write_md(md_path, data)

    REPORT.info(f"Wrote {json_path}, {csv_path} ({rows} rows), {md_path}")
    print("\n" + "=" * 78)
    if REPORT.is_complete and not args.max_properties:
        print(f"COMPLETE — {month_str}: ${data['money_total']:,} across "
              f"{REPORT.properties_scanned} properties.")
        sys.exit(0)
    else:
        print(f"PARTIAL — {len(REPORT.failures)} failure(s). See {md_path}.")
        sys.exit(1)


if __name__ == "__main__":
    main()
