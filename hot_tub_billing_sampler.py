#!/usr/bin/env python3
"""
Hot Tub Billing — Data Sampler  (PHASE 1: learn the real data)
==============================================================

Standalone, STRICTLY READ-ONLY Breezeway pull whose ONLY job is to show us what
real hot-tub service tasks actually look like, so the billing classifier can be
built against reality instead of guesses.

It does NOT bill anything and NOTHING here is final. It collects every candidate
hot-tub task across the billing-relevant properties and dumps:
  • every title + summary text (so we can see "reg", "regulur", "rg", "D&S",
    "dump scrub", "WWM", "bacterial", "partial", and the random non-billable
    work orders that merely mention a hot tub),
  • BOTH the scheduled date and the completion date on every row (the 30th-shows-
    as-the-1st month-boundary quirk means we must never silently pick one),
  • status, assignee, who finished it, department, and which tag(s) made the
    property in-scope.

Output is for HUMAN REVIEW. A draft service-type guess is included, but it is
clearly marked DRAFT — the point of this phase is to correct it.

------------------------------------------------------------------------------
NON-NEGOTIABLE RULES (same spine as productivity_past_365_days.py):
  1. READ-ONLY. Only GET requests (plus the one auth POST). A hard guard
     (`_assert_readonly`) aborts on any other verb. It NEVER mutates Breezeway.
  2. NEVER FAIL SILENTLY. Every auth error, 429, 5xx, malformed page, or per-
     property failure is logged and reflected in the summary. If anything
     failed, the run is PARTIAL and exits non-zero. (See [[no-silent-task-drops]].)
  3. STOP AND ASK, DON'T GUESS. Service-type classification is DRAFT only; the
     raw text is always preserved so a human confirms it.

------------------------------------------------------------------------------
USAGE
  Credentials come from the same .env / env vars as the web app:
      BREEZEWAY_CLIENT_ID, BREEZEWAY_CLIENT_SECRET

  Inspect FIRST (mandatory — confirms where the "summary" text actually lives):
      python hot_tub_billing_sampler.py --inspect
        → finds the first hot-tub task and prints its FULL JSON, so we can see
          exactly which field holds the summary/description text.

  Full sample:
      python hot_tub_billing_sampler.py --days 120
        → writes hot_tub_billing_sample.csv   (one row per candidate task)
               + hot_tub_billing_vocab.md      (wording frequency, for tuning)
               + hot_tub_billing_sample.json   (machine-readable)

  Options:
      --days N            window length in days (default 120)
      --end YYYY-MM-DD    end of window (default today). Window = [end-days, end]
      --outdir DIR        where to write (default ".")
      --workers N         parallel property fetches (default 12)
      --max-properties N  TESTING ONLY: scan only the first N tagged properties
                          (forces a PARTIAL sample)

  Exit code: 0 = COMPLETE, non-zero = PARTIAL or HALTED.
"""

from __future__ import annotations

import argparse
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

# Resilient console on non-UTF-8 terminals (a stray unicode char must never crash
# a run — that would itself be a near-silent failure, which the rules forbid).
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

# ── Configuration ────────────────────────────────────────────────────────────

BASE = "https://api.breezeway.io"
AUTH_URL     = f"{BASE}/public/auth/v1/"
PROPERTY_URL = f"{BASE}/public/inventory/v1/property"
TASK_URL     = f"{BASE}/public/inventory/v1/task"

CLIENT_ID     = os.environ.get("BREEZEWAY_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("BREEZEWAY_CLIENT_SECRET", "")

# Property tags that put a house in the billing universe (matched case-insensitively,
# exact name). "hot tub" plain is included so we ALSO catch hot-tub houses that only
# carry the plain tag — useful for spotting coverage gaps in this sample phase.
BILLING_TAGS = {
    "hot tub - tg service": "tg_service",   # floor 2/mo
    "weekly service":       "weekly",        # floor 4/mo
    "monthly service":      "monthly",       # floor 1/mo
    "hot tub":              "plain_hot_tub", # not itself a billing tag; coverage signal
}

# A task is a hot-tub CANDIDATE if its title OR summary mentions a hot tub. Kept
# deliberately forgiving (hot tub / hottub / h.t. / "ht ") so we also surface the
# random non-billable work orders the operator wants to eyeball and exclude.
HOT_TUB_CANDIDATE = re.compile(r"hot[\s\-]?tub|hottub|\bh\.?\s?t\.?\b|\bspa\b", re.IGNORECASE)

# Fields that might hold the free-text "summary". We capture all of them so the
# --inspect step can confirm which one Breezeway actually populates.
SUMMARY_KEYS = ["summary", "description", "notes", "instructions", "details",
                "comment", "comments", "memo", "note"]
NAME_KEYS    = ["title", "name", "template_name", "task_template_name", "task_name"]
COMPLETION_KEYS = ["finished_at", "completed_at", "date_completed",
                   "completion_date", "finished", "date_finished", "closed_at"]

# ── DRAFT classifier (Phase 1 best-guess — to be corrected against real data) ──
# These are intentionally forgiving and WILL be wrong on edge cases; that's the
# point of sampling. Order matters: more specific types win.
DRAFT_RULES = [
    # (service_type, draft_price, regex over normalized "title + summary")
    ("bacterial_wwm", 250, re.compile(r"wwm|bacteri|www|water\s*master", re.IGNORECASE)),
    ("dump_scrub",    155, re.compile(r"\bd\s*&?\s*s\b|dump.{0,6}scrub|dump\s*&\s*scrub|d\s*and\s*s", re.IGNORECASE)),
    ("regular",        50, re.compile(r"\breg(ular|ulur|ulr|ler)?\b|\brg\b|biweek|bi-week|arrival|service", re.IGNORECASE)),
]
PARTIAL_FLAG = re.compile(r"partial", re.IGNORECASE)

MAX_ATTEMPTS = 5
BACKOFF_CAP  = 60
PAGE_LIMIT   = 100

_ALLOWED_METHODS = {"GET", "POST"}   # POST only for auth
_SESSION = requests.Session()


# ── Run report ───────────────────────────────────────────────────────────────

class RunReport:
    def __init__(self):
        self.failures: list[str] = []
        self.warnings: list[str] = []
        self.log_lines: list[str] = []
        self.properties_total = 0
        self.properties_tagged = 0
        self.properties_scanned = 0
        self.candidate_tasks = 0

    def fail(self, msg): self._emit(f"[FAIL] {msg}"); self.failures.append(msg)
    def warn(self, msg): self._emit(f"[WARN] {msg}"); self.warnings.append(msg)
    def info(self, msg): self._emit(f"[info] {msg}")

    def _emit(self, line):
        ts = datetime.now().strftime("%H:%M:%S")
        full = f"{ts} {line}"
        self.log_lines.append(full)
        print(full, flush=True)

    @property
    def is_complete(self): return not self.failures


REPORT = RunReport()


# ── HTTP (read-only, retry/backoff) ──────────────────────────────────────────

def _assert_readonly(method):
    if method.upper() not in _ALLOWED_METHODS:
        raise RuntimeError(f"READ-ONLY VIOLATION: attempted HTTP {method}. Aborting.")


def _retry_delay_from(resp, attempt):
    for h in ("Retry-After", "retry-after", "X-RateLimit-Reset", "x-ratelimit-reset"):
        v = resp.headers.get(h)
        if v:
            try: return min(BACKOFF_CAP, max(1.0, float(v)))
            except ValueError: pass
    return min(BACKOFF_CAP, float(2 ** attempt))


def bw_get(url, params, token, what):
    _assert_readonly("GET")
    last_status = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            resp = _SESSION.get(url, headers={"Authorization": f"JWT {token}"},
                                params=params, timeout=30)
        except requests.exceptions.Timeout:
            delay = min(BACKOFF_CAP, float(2 ** attempt))
            REPORT.warn(f"{what}: timeout (attempt {attempt}/{MAX_ATTEMPTS}); waiting {delay:.0f}s")
            time.sleep(delay); continue
        except Exception as ex:
            return None, f"network error: {ex}", last_status
        last_status = resp.status_code
        if resp.status_code == 200:
            try: return resp.json(), "", 200
            except Exception as ex:
                return None, f"200 but unparseable JSON: {ex} | body={resp.text[:200]!r}", 200
        if resp.status_code == 429 or resp.status_code >= 500:
            delay = _retry_delay_from(resp, attempt)
            REPORT.warn(f"{what}: HTTP {resp.status_code} (attempt {attempt}/{MAX_ATTEMPTS}); waiting {delay:.0f}s")
            time.sleep(delay); continue
        detail = resp.text[:300]
        try: detail = resp.json()
        except Exception: pass
        return None, f"HTTP {resp.status_code}: {detail}", resp.status_code
    return None, f"gave up after {MAX_ATTEMPTS} attempts (last status {last_status})", last_status


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


# ── Auth ─────────────────────────────────────────────────────────────────────

def authenticate():
    _assert_readonly("POST")
    if not CLIENT_ID or not CLIENT_SECRET:
        REPORT.fail("Missing BREEZEWAY_CLIENT_ID / BREEZEWAY_CLIENT_SECRET.")
        _halt("Cannot authenticate without credentials.")
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            resp = _SESSION.post(AUTH_URL, json={"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET}, timeout=30)
        except Exception as ex:
            REPORT.warn(f"auth: network error {ex} (attempt {attempt}/{MAX_ATTEMPTS})")
            time.sleep(min(BACKOFF_CAP, float(2 ** attempt))); continue
        if resp.status_code == 200:
            try: token = resp.json().get("access_token")
            except Exception as ex:
                REPORT.fail(f"auth: 200 but unparseable body: {ex}"); _halt("Auth response could not be parsed.")
            if not token:
                REPORT.fail(f"auth: 200 but no access_token: {resp.text[:200]!r}"); _halt("Auth returned no token.")
            REPORT.info("Authenticated with Breezeway.")
            return token
        if resp.status_code == 429:
            delay = max(_retry_delay_from(resp, attempt), 60.0)
            REPORT.warn(f"auth: HTTP 429 (1/min limit). Waiting {delay:.0f}s (attempt {attempt}/{MAX_ATTEMPTS}).")
            time.sleep(delay); continue
        if resp.status_code >= 500:
            delay = _retry_delay_from(resp, attempt)
            REPORT.warn(f"auth: HTTP {resp.status_code} transient (attempt {attempt}/{MAX_ATTEMPTS}); waiting {delay:.0f}s")
            time.sleep(delay); continue
        REPORT.fail(f"auth: HTTP {resp.status_code}: {resp.text[:300]!r}"); _halt("Authentication failed.")
    REPORT.fail("auth: exhausted retries."); _halt("Authentication failed after retries.")


# ── Field helpers ────────────────────────────────────────────────────────────

def _stringify(v):
    if isinstance(v, dict):
        return str(v.get("value") or v.get("name") or v.get("label") or "")
    return str(v or "")


def _first_present(task, keys):
    for k in keys:
        v = task.get(k)
        if v:
            return k, _stringify(v).strip()
    return None, ""


def task_title(task):
    _, v = _first_present(task, NAME_KEYS)
    return v


def task_summary(task):
    _, v = _first_present(task, SUMMARY_KEYS)
    return v


def task_completion(task):
    for k in COMPLETION_KEYS:
        raw = task.get(k)
        if raw:
            s = str(raw)
            if len(s) >= 10 and s[4] == "-" and s[7] == "-":
                return k, s[:10]
            try:
                return k, datetime.fromisoformat(s.replace("Z", "+00:00")).date().isoformat()
            except Exception:
                return k, s
    return None, ""


def task_status(task):
    for k in ("type_task_status", "status", "state"):
        v = task.get(k)
        if v:
            return _stringify(v).lower().strip()
    return ""


def task_assignees(task):
    out = []
    for a in (task.get("assignments") or []):
        if isinstance(a, dict):
            n = (a.get("name") or a.get("full_name") or
                 f"{a.get('first_name','').strip()} {a.get('last_name','').strip()}".strip())
            if n:
                out.append(n)
    return out


def task_finished_by(task):
    fb = task.get("finished_by")
    if isinstance(fb, dict):
        return (fb.get("name") or
                f"{fb.get('first_name','').strip()} {fb.get('last_name','').strip()}".strip())
    return ""


def task_department(task):
    return _stringify(task.get("type_department")).strip()


def task_bill_to(task):
    """Breezeway's own owner/tenant attribution — top-level `bill_to`."""
    return _stringify(task.get("bill_to")).lower().strip()


def task_tag_names(task):
    out = []
    for t in (task.get("task_tags") or task.get("tags") or []):
        name = (t.get("name") or t.get("label") or "") if isinstance(t, dict) else t
        name = str(name).strip()
        if name:
            out.append(name)
    return out


def task_costs(task):
    """Return owner-billed cost line items: list of (amount, description, type, bill_to)."""
    out = []
    for c in (task.get("costs") or []):
        if not isinstance(c, dict):
            continue
        amt = c.get("cost")
        try:
            amt = float(amt)
        except (TypeError, ValueError):
            amt = None
        out.append((amt, _stringify(c.get("description")).strip(),
                    _stringify(c.get("type_cost")).strip(),
                    _stringify(c.get("bill_to")).lower().strip()))
    return out


def linked_reservation_id(task):
    lr = task.get("linked_reservation")
    if isinstance(lr, dict):
        return str(lr.get("external_reservation_id") or lr.get("id") or "")
    return ""


def draft_classify(title, summary):
    """DRAFT only. Returns (service_type, draft_price, partial_flag)."""
    blob = f"{title}  {summary}".strip()
    partial = bool(PARTIAL_FLAG.search(blob))
    for stype, price, rx in DRAFT_RULES:
        if rx.search(blob):
            # "partial" overrides price to regular per the operator's rule, but we
            # KEEP the flag so a human sees it.
            if partial and stype != "regular":
                return "regular(partial?)", 50, True
            return stype, price, partial
    return "unknown", 0, partial


# ── Properties + tags ────────────────────────────────────────────────────────

def fetch_properties(token):
    props, err = bw_get_paginated(PROPERTY_URL, {}, token, "properties")
    if err:
        REPORT.fail(f"Property list errored: {err} (got {len(props)} first).")
    if not props:
        REPORT.fail("Property list empty — nothing to scan."); _halt("No properties.")
    return props


def fetch_property_tags(token, pid):
    for path in (f"/public/inventory/v1/property/{pid}/tags",
                 f"/public/inventory/v1/property/{pid}"):
        data, err, status = bw_get(f"{BASE}{path}", {}, token, f"tags {pid}")
        if err:
            continue
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            tags = data.get("tags") or data.get("property_tags") or []
            if tags:
                return tags
    return []


def classify_property_tags(token, pid):
    """Return the set of billing-tag keys this property carries."""
    keys = set()
    for t in fetch_property_tags(token, pid):
        name = (t.get("name") or t.get("label") or "") if isinstance(t, dict) else t
        name = str(name).lower().strip()
        if name in BILLING_TAGS:
            keys.add(BILLING_TAGS[name])
    return keys


def property_id(p):
    return p.get("id") or p.get("property_id") or p.get("home_id")


def property_name(p):
    return (p.get("name") or p.get("property_name") or
            p.get("address") or _stringify(p.get("unit_address")) or str(property_id(p)))


def property_query_param(p):
    ref = p.get("reference_property_id")
    pid = property_id(p)
    if ref:
        return {"reference_property_id": ref}
    if pid is not None:
        return {"home_id": pid}
    return {}


# ── Task fetch per property ──────────────────────────────────────────────────

def fetch_tasks_for_property(token, p, start_d, end_d):
    qp = property_query_param(p)
    if not qp:
        return [], "no usable property id"
    drange = f"{start_d.isoformat()},{end_d.isoformat()}"
    # Pull tasks scheduled in the window. (Completion-date filtering happens later;
    # we keep BOTH dates so the operator can resolve the month-boundary quirk.)
    tasks, err = bw_get_paginated(TASK_URL, {**qp, "scheduled_date": drange}, token,
                                  f"tasks {property_name(p)}")
    return tasks, err


# ── Inspect ──────────────────────────────────────────────────────────────────

def run_inspect(token, properties, start_d, end_d, workers):
    REPORT.info("INSPECT — finding the first hot-tub task to confirm field names…")
    tagged = collect_tagged_properties(token, properties, workers)
    for p in tagged:
        tasks, err = fetch_tasks_for_property(token, p, start_d, end_d)
        if err:
            REPORT.warn(f"inspect: {property_name(p)} errored: {err}"); continue
        for t in tasks:
            if HOT_TUB_CANDIDATE.search(f"{task_title(t)}  {task_summary(t)}"):
                print("\n" + "=" * 78)
                print(f"SAMPLE HOT-TUB TASK — property: {property_name(p)}")
                print("=" * 78)
                print(json.dumps(t, indent=2, default=str))
                print("=" * 78)
                skey, sval = _first_present(t, SUMMARY_KEYS)
                ckey, cval = task_completion(t)
                print(f"Detected SUMMARY field   : {skey or 'NONE of ' + ','.join(SUMMARY_KEYS)}  -> {sval[:120]!r}")
                print(f"Detected COMPLETION field: {ckey or 'NONE'}  -> {cval!r}")
                print(f"Detected title           : {task_title(t)!r}")
                print(f"scheduled_date           : {t.get('scheduled_date')!r}")
                print(f"status                   : {task_status(t)!r}")
                print(f"assignees / finished_by  : {task_assignees(t)} / {task_finished_by(t)!r}")
                print("=" * 78)
                return
    REPORT.warn("INSPECT found no hot-tub task in the window among tagged properties.")


# ── Collect tagged properties ────────────────────────────────────────────────

def collect_tagged_properties(token, properties, workers):
    REPORT.properties_total = len(properties)
    REPORT.info(f"Classifying tags for {len(properties)} properties (workers={workers})…")
    tagged = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(classify_property_tags, token, property_id(p)): p for p in properties}
        for fut in as_completed(futs):
            p = futs[fut]
            try:
                keys = fut.result()
            except Exception as ex2:
                REPORT.warn(f"tag classify failed for {property_name(p)}: {ex2}")
                continue
            if keys:
                p["_billing_tags"] = sorted(keys)
                tagged.append(p)
    REPORT.properties_tagged = len(tagged)
    REPORT.info(f"{len(tagged)} properties carry a billing-relevant tag.")
    return tagged


# ── Full sample ──────────────────────────────────────────────────────────────

def run_sample(token, properties, start_d, end_d, workers, max_props):
    tagged = collect_tagged_properties(token, properties, workers)
    if max_props:
        REPORT.warn(f"TEST MODE: limiting to first {max_props} of {len(tagged)} tagged properties.")
        tagged = tagged[:max_props]

    rows = []
    title_counter = Counter()
    summary_counter = Counter()
    type_counter = Counter()
    tag_counter = Counter()
    billto_counter = Counter()
    cost_counter = Counter()

    def work(p):
        tasks, err = fetch_tasks_for_property(token, p, start_d, end_d)
        return p, tasks, err

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for p, tasks, err in ex.map(work, tagged):
            if err:
                REPORT.fail(f"{property_name(p)}: {err} — its tasks are MISSING from the sample.")
                continue
            REPORT.properties_scanned += 1
            for t in tasks:
                title = task_title(t)
                summary = task_summary(t)
                if not HOT_TUB_CANDIDATE.search(f"{title}  {summary}"):
                    continue
                ckey, cdate = task_completion(t)
                # Classify on name + description (summary is usually null; the real
                # service wording lives in those two — confirmed via --inspect).
                desc = _stringify(t.get("description")).strip()
                stype, price, partial = draft_classify(title, f"{summary} {desc}")
                sched = str(t.get("scheduled_date") or "")[:10]
                costs = task_costs(t)
                owner_cost = sum(a for (a, _d, _ty, bt) in costs
                                 if a is not None and (bt == "owner" or not bt))
                cost_descs = " | ".join(f"${a:g}:{d}" for (a, d, _ty, _bt) in costs if a is not None)
                tag_names = task_tag_names(t)
                row = {
                    "property":       property_name(p),
                    "property_tags":  ",".join(p.get("_billing_tags", [])),
                    "task_id":        t.get("id"),
                    "title":          title,
                    "description":    desc,
                    "summary":        summary,
                    "scheduled_date": sched,
                    "scheduled_time": str(t.get("scheduled_time") or "")[:5],
                    "completed_date": cdate,
                    "completion_field": ckey or "",
                    "status":         task_status(t),
                    "bill_to":        task_bill_to(t),
                    "task_tags":      "; ".join(tag_names),
                    "owner_cost_total": f"{owner_cost:g}" if owner_cost else "",
                    "cost_line_items": cost_descs,
                    "linked_res_id":  linked_reservation_id(t),
                    "assignees":      "; ".join(task_assignees(t)),
                    "finished_by":    task_finished_by(t),
                    "department":     task_department(t),
                    "draft_type":     stype,
                    "draft_price":    price,
                    "partial_flag":   "YES" if partial else "",
                }
                rows.append(row)
                REPORT.candidate_tasks += 1
                title_counter[title.lower().strip()] += 1
                if desc:
                    summary_counter[desc.lower().strip()[:80]] += 1
                type_counter[stype] += 1
                for nm in tag_names:
                    tag_counter[nm] += 1
                billto_counter[task_bill_to(t) or "(blank)"] += 1
                for (a, d, _ty, _bt) in costs:
                    if a is not None:
                        cost_counter[f"${a:g}"] += 1

    rows.sort(key=lambda r: (r["property"].lower(), r["scheduled_date"]))
    return rows, {
        "title": title_counter, "summary": summary_counter, "type": type_counter,
        "tag": tag_counter, "billto": billto_counter, "cost": cost_counter,
    }


# ── Output ───────────────────────────────────────────────────────────────────

CSV_FIELDS = ["property", "property_tags", "task_id", "title", "description", "summary",
              "scheduled_date", "scheduled_time", "completed_date", "completion_field",
              "status", "bill_to", "task_tags", "owner_cost_total", "cost_line_items",
              "linked_res_id", "assignees", "finished_by", "department",
              "draft_type", "draft_price", "partial_flag"]


def write_csv(path, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    return len(rows)


def write_vocab(path, counters, start_d, end_d):
    title_counter   = counters["title"]
    summary_counter = counters["summary"]
    type_counter    = counters["type"]
    tag_counter     = counters["tag"]
    billto_counter  = counters["billto"]
    cost_counter    = counters["cost"]
    L = []
    A = L.append
    A("# Hot Tub Billing — Vocabulary Sample (Phase 1)")
    A("")
    status = "✅ COMPLETE" if REPORT.is_complete else f"⚠️ PARTIAL — {len(REPORT.failures)} failure(s)"
    A(f"**Status: {status}**")
    A("")
    A(f"- Window: **{start_d.isoformat()} → {end_d.isoformat()}**")
    A(f"- Properties total / tagged / scanned: "
      f"{REPORT.properties_total} / {REPORT.properties_tagged} / {REPORT.properties_scanned}")
    A(f"- Candidate hot-tub tasks found: **{REPORT.candidate_tasks}**")
    A("")
    A("> ⚠️ `draft_type` / `draft_price` are FIRST-GUESS only. This file exists so "
      "we can correct the keyword rules against real wording before any billing logic "
      "is trusted. Nothing here is final.")
    A("")
    A("## Draft type distribution")
    A("| draft_type | count |")
    A("|---|---|")
    for stype, n in type_counter.most_common():
        A(f"| {stype} | {n} |")
    A("")
    A("## bill_to distribution (Breezeway's own owner/tenant flag)")
    A("_If owner/tenant is reliably set here, we may not need to infer it from lease logic._")
    A("")
    A("| bill_to | count |")
    A("|---|---|")
    for bt, n in billto_counter.most_common():
        A(f"| {bt} | {n} |")
    A("")
    A("## Cost line-item amounts seen (do prices already live in Breezeway?)")
    A("")
    A("| amount | count |")
    A("|---|---|")
    for amt, n in cost_counter.most_common():
        A(f"| {amt} | {n} |")
    A("")
    A("## Task tags seen (e.g. 'Arrival HT', 'Billed via PMS', 'Lease')")
    A("_These tags may classify service type / billing route far more reliably than free text._")
    A("")
    A("| count | task tag |")
    A("|---|---|")
    for tag, n in tag_counter.most_common():
        A(f"| {n} | {tag} |")
    A("")
    A("## Every distinct TITLE seen (most common first)")
    A("_Use this to map real wording → service type. Watch for typos & non-billable work orders._")
    A("")
    A("| count | title |")
    A("|---|---|")
    for title, n in title_counter.most_common():
        A(f"| {n} | {title} |")
    A("")
    A("## Distinct DESCRIPTION text seen (first 80 chars, most common first)")
    A("")
    A("| count | description (truncated) |")
    A("|---|---|")
    for s, n in summary_counter.most_common(200):
        A(f"| {n} | {s} |")
    A("")
    A("## Failures")
    if REPORT.failures:
        for m in REPORT.failures:
            A(f"- ❌ {m}")
    else:
        A("- None. 🎉")
    A("")
    A("## Warnings")
    if REPORT.warnings:
        for w in REPORT.warnings[:100]:
            A(f"- ⚠️ {w}")
        if len(REPORT.warnings) > 100:
            A(f"- …and {len(REPORT.warnings) - 100} more (see log).")
    else:
        A("- None.")
    A("")
    A("## Full log")
    A("```")
    L.extend(REPORT.log_lines)
    A("```")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(L) + "\n")


def write_json(path, rows, type_counter, start_d, end_d):
    payload = {
        "title": "Hot Tub Billing — Sample (Phase 1)",
        "status": "COMPLETE" if REPORT.is_complete else "PARTIAL",
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "window": {"start": start_d.isoformat(), "end": end_d.isoformat()},
        "properties_total": REPORT.properties_total,
        "properties_tagged": REPORT.properties_tagged,
        "properties_scanned": REPORT.properties_scanned,
        "candidate_tasks": REPORT.candidate_tasks,
        "draft_type_distribution": dict(type_counter),
        "rows": rows,
        "failures": REPORT.failures,
        "warnings_count": len(REPORT.warnings),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)


# ── Halt ─────────────────────────────────────────────────────────────────────

def _halt(reason):
    print("\n" + "!" * 78, file=sys.stderr)
    print(f"HALTED: {reason}", file=sys.stderr)
    print("This run produced NO trustworthy sample.", file=sys.stderr)
    print("!" * 78, file=sys.stderr)
    sys.exit(2)


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Hot Tub Billing data sampler — read-only.")
    ap.add_argument("--inspect", action="store_true",
                    help="Print one hot-tub task's full JSON and detected fields, then exit.")
    ap.add_argument("--days", type=int, default=120, help="Window length in days (default 120).")
    ap.add_argument("--end", default="", help="End date YYYY-MM-DD (default today).")
    ap.add_argument("--outdir", default=".", help="Output directory (default current).")
    ap.add_argument("--workers", type=int, default=12, help="Parallel property fetches (default 12).")
    ap.add_argument("--max-properties", type=int, default=0,
                    help="TESTING ONLY: scan only the first N tagged properties.")
    args = ap.parse_args()

    if args.end:
        try:
            end_d = date.fromisoformat(args.end)
        except ValueError:
            print("--end must be YYYY-MM-DD.", file=sys.stderr); sys.exit(2)
    else:
        end_d = date.today()
    start_d = end_d - timedelta(days=args.days)

    REPORT.info(f"Hot Tub Billing Sampler — window {start_d} → {end_d}")
    token = authenticate()
    properties = fetch_properties(token)

    if args.inspect:
        run_inspect(token, properties, start_d, end_d, args.workers)
        sys.exit(0 if REPORT.is_complete else 1)

    rows, counters = run_sample(
        token, properties, start_d, end_d, args.workers, args.max_properties)

    os.makedirs(args.outdir, exist_ok=True)
    csv_path   = os.path.join(args.outdir, "hot_tub_billing_sample.csv")
    vocab_path = os.path.join(args.outdir, "hot_tub_billing_vocab.md")
    json_path  = os.path.join(args.outdir, "hot_tub_billing_sample.json")

    n = write_csv(csv_path, rows)
    write_vocab(vocab_path, counters, start_d, end_d)
    write_json(json_path, rows, counters["type"], start_d, end_d)

    REPORT.info(f"Wrote {csv_path} ({n} rows), {vocab_path}, {json_path}")
    print("\n" + "=" * 78)
    if REPORT.is_complete and not args.max_properties:
        print(f"COMPLETE — {REPORT.candidate_tasks} candidate hot-tub tasks across "
              f"{REPORT.properties_scanned}/{REPORT.properties_tagged} tagged properties.")
        sys.exit(0)
    else:
        print(f"PARTIAL — {len(REPORT.failures)} failure(s). See {vocab_path}.")
        sys.exit(1)


if __name__ == "__main__":
    main()
