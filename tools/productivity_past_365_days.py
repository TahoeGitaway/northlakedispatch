#!/usr/bin/env python3
"""
Productivity Past 365 Days
==========================

Standalone, STRICTLY READ-ONLY Breezeway report: how many tasks each named team
member COMPLETED per day over the rolling last 365 days.

Purpose: see whether completed-task volume per person trended up or down.

------------------------------------------------------------------------------
NON-NEGOTIABLE RULES (enforced in code):
  1. READ-ONLY. This program issues only GET requests (plus the one auth POST).
     It NEVER calls PATCH/PUT/DELETE/POST-to-task and never mutates a Breezeway
     record. "Disarm Bear Fence" tasks are FILTERED OUT of the counts, not
     removed from Breezeway. (See `_assert_readonly` — a hard guard.)
  2. NEVER FAIL SILENTLY. Every auth error, 429, 5xx, empty/malformed page,
     unmatched/ambiguous name, or out-of-range task is logged with detail and
     reflected in the final summary. If anything failed, the report header says
     "PARTIAL — N failures" and the process exits non-zero.
  3. STOP AND ASK, DON'T GUESS. If a name resolves to zero or to multiple
     Breezeway users, the run HALTS and reports it. Names are never guessed or
     silently dropped.

------------------------------------------------------------------------------
USAGE
  Set credentials (same env vars the web app uses):
      BREEZEWAY_CLIENT_ID, BREEZEWAY_CLIENT_SECRET

  Dry inspect FIRST (mandatory sanity check before trusting a year of numbers):
      python productivity_past_365_days.py --inspect
        → authenticates, resolves the 7 names, then pulls ONE completed task and
          prints its full JSON plus the field names this program detected
          (completion timestamp / assignee field / task-name field). Confirm
          these match before doing a full run.

  Full run:
      python productivity_past_365_days.py
        → writes productivity_past_365_days.csv  (one row per person × day)
          and  productivity_past_365_days_summary.md (header, totals, failures)

  Options:
      --days N            window length (default 365)
      --delay SECONDS     polite pause between property calls (default 0.25)
      --outdir DIR        where to write the report files (default ".")
      --max-properties N  scan only the first N properties (TESTING ONLY; forces
                          a PARTIAL report since coverage is incomplete)

  Exit code: 0 = COMPLETE, non-zero = PARTIAL or HALTED. See the summary file.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
import json
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

import requests

# Make console output resilient on non-UTF-8 terminals (e.g. a Windows cp1252
# console) so a stray unicode char in a log line can never crash the run — a
# print blowing up would itself be a silent-ish failure, which this tool forbids.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# Load credentials from a local .env the same way the web app does, so this
# standalone tool authenticates with zero extra setup. Best-effort: if
# python-dotenv isn't installed, real environment variables still work.
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ── Configuration ────────────────────────────────────────────────────────────

BASE = "https://api.breezeway.io"
AUTH_URL  = f"{BASE}/public/auth/v1/"
PEOPLE_URL   = f"{BASE}/public/inventory/v1/people"
PROPERTY_URL = f"{BASE}/public/inventory/v1/property"
TASK_URL     = f"{BASE}/public/inventory/v1/task"
COMPANY_URL  = f"{BASE}/public/inventory/v1/company"

CLIENT_ID     = os.environ.get("BREEZEWAY_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("BREEZEWAY_CLIENT_SECRET", "")

# The people to include. (Javier was removed at the operator's request — he matched
# zero Breezeway users on the 2026-06-26 inspect. "Trevor" appeared twice in the
# original request and was deduped to one; if there are genuinely two Trevors this
# list is WRONG and the program will halt at name resolution on the ambiguous name.)
# Julie joined mid-cycle this year — current-window only; do NOT run --prior for her
# (she has no prior-year data, so a prior run would just show zeros).
PEOPLE = ["Andy", "Trevor", "Calder", "Jonah", "Irving", "Chris", "Julie"]

# Names the operator has DELIBERATELY chosen to skip on a run (name → reason). The
# sanctioned way to exclude a requested person: logged loudly and called out in the
# report header, never a silent drop. Empty by default.
DROPPED_NAMES: dict = {}

# Tasks whose name/template equals this (case-insensitive, trimmed) are excluded
# from the counts. They are NOT modified or removed on Breezeway.
EXCLUDE_TASK_NAME = "disarm bear fence"

# Candidate field names. The dry-inspect confirms which one Breezeway
# uses; the program logs the detected key so the numbers are interpretable.
COMPLETION_KEYS = ["finished_at", "completed_at", "date_completed",
                   "completion_date", "finished", "date_finished", "closed_at"]
NAME_KEYS       = ["name", "title", "template_name", "task_template_name",
                   "type_task", "task_type", "task_name"]

MAX_ATTEMPTS = 5          # per-request retry cap before a loud failure
BACKOFF_CAP  = 60         # seconds — ceiling on exponential backoff
PAGE_LIMIT   = 100        # Breezeway max page size

# Only ever touch the network through these verbs. A hard guard against the
# read-only rule: if any code path ever tries to mutate, it raises immediately.
_ALLOWED_METHODS = {"GET", "POST"}   # POST is used ONLY for the auth endpoint
_SESSION = requests.Session()


# ── Run report (accumulates everything for the final summary) ────────────────

class RunReport:
    def __init__(self):
        self.failures: list[str] = []   # hard problems → PARTIAL + non-zero exit
        self.warnings: list[str] = []   # notable but non-fatal observations
        self.log_lines: list[str] = []  # full chronological log
        self.properties_total = 0
        self.properties_scanned = 0
        self.tasks_seen = 0             # completed tasks returned for our people
        self.tasks_counted = 0         # distinct tasks that contributed a count
        self.person_increments = 0     # total per-person tallies (CSV cell sum)
        self.excluded_bear_fence = 0   # distinct "Disarm Bear Fence" tasks skipped
        self.completed_by_others = 0   # completed tasks finished by people outside our set
        self.no_completer = 0          # finished tasks with no `finished_by` recorded
        self.out_of_range_skipped = 0  # finished_at outside the window
        self.no_completion_date = 0    # task with no parseable completion date
        self.completion_key_used: str | None = None
        self.name_field_for_exclusion: dict[str, int] = defaultdict(int)
        self.intentionally_dropped: list[str] = []   # requested people excluded on purpose

    def fail(self, msg: str):
        line = f"[FAIL] {msg}"
        self.failures.append(msg)
        self._emit(line)

    def warn(self, msg: str):
        line = f"[WARN] {msg}"
        self.warnings.append(msg)
        self._emit(line)

    def info(self, msg: str):
        self._emit(f"[info] {msg}")

    def _emit(self, line: str):
        ts = datetime.now().strftime("%H:%M:%S")
        full = f"{ts} {line}"
        self.log_lines.append(full)
        print(full, flush=True)

    @property
    def is_complete(self) -> bool:
        return not self.failures


REPORT = RunReport()


# ── HTTP layer (read-only, with 429 / 5xx handling) ──────────────────────────

def _assert_readonly(method: str):
    if method.upper() not in _ALLOWED_METHODS:
        raise RuntimeError(
            f"READ-ONLY VIOLATION: attempted HTTP {method}. This program must "
            f"never mutate Breezeway data. Aborting."
        )


def _retry_delay_from(resp: requests.Response, attempt: int) -> float:
    """Prefer the server-provided retry timing (header, then body); otherwise
    exponential backoff (2,4,8,16,…) capped at BACKOFF_CAP."""
    # Header: standard Retry-After (seconds) or a rate-limit reset hint
    for h in ("Retry-After", "retry-after", "X-RateLimit-Reset", "x-ratelimit-reset"):
        v = resp.headers.get(h)
        if v:
            try:
                return min(BACKOFF_CAP, max(1.0, float(v)))
            except ValueError:
                pass
    # Body: look for a retry-ish field
    try:
        body = resp.json()
        if isinstance(body, dict):
            for k in ("retry_after", "retry_in", "retry_after_seconds", "wait", "retryAfter"):
                if k in body:
                    return min(BACKOFF_CAP, max(1.0, float(body[k])))
    except Exception:
        pass
    return min(BACKOFF_CAP, float(2 ** attempt))


def bw_get(url: str, params: dict, token: str, what: str) -> tuple:
    """Single GET with retry/backoff. Returns (json_or_None, error_or_'', status).

    Retries 429 (respecting server timing) and 5xx; a non-retryable status, a
    timeout, or exhausting MAX_ATTEMPTS returns a loud error string — never a
    silent empty result.
    """
    _assert_readonly("GET")
    last_status = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            resp = _SESSION.get(url, headers={"Authorization": f"JWT {token}"},
                                params=params, timeout=30)
        except requests.exceptions.Timeout:
            delay = min(BACKOFF_CAP, float(2 ** attempt))
            REPORT.warn(f"{what}: timeout (attempt {attempt}/{MAX_ATTEMPTS}); waiting {delay:.0f}s")
            time.sleep(delay)
            continue
        except Exception as ex:
            return None, f"network error: {ex}", last_status

        last_status = resp.status_code

        if resp.status_code == 200:
            try:
                return resp.json(), "", 200
            except Exception as ex:
                return None, f"200 but unparseable JSON: {ex} | body={resp.text[:200]!r}", 200

        if resp.status_code == 429:
            delay = _retry_delay_from(resp, attempt)
            REPORT.warn(f"{what}: HTTP 429 rate-limited (attempt {attempt}/{MAX_ATTEMPTS}); waiting {delay:.0f}s")
            time.sleep(delay)
            continue

        if resp.status_code >= 500:
            delay = _retry_delay_from(resp, attempt)
            REPORT.warn(f"{what}: HTTP {resp.status_code} (attempt {attempt}/{MAX_ATTEMPTS}); waiting {delay:.0f}s")
            time.sleep(delay)
            continue

        # Non-retryable (4xx other than 429)
        detail = resp.text[:300]
        try:
            detail = resp.json()
        except Exception:
            pass
        return None, f"HTTP {resp.status_code}: {detail}", resp.status_code

    return None, f"gave up after {MAX_ATTEMPTS} attempts (last status {last_status})", last_status


def bw_get_paginated(url: str, params: dict, token: str, what: str) -> tuple:
    """Walk every page until a page returns < PAGE_LIMIT records.
    Returns (all_results, error_or_'').  On any page error, stops and returns
    what it has PLUS the error (so the caller can record a partial failure)."""
    out = []
    page = 1
    while True:
        data, err, status = bw_get(url, {**params, "limit": PAGE_LIMIT, "page": page},
                                   token, f"{what} (page {page})")
        if err:
            return out, err
        if isinstance(data, dict):
            results = data.get("results", data.get("data", data.get("tasks", [])))
        elif isinstance(data, list):
            results = data
        else:
            results = None
        if results is None:
            return out, f"malformed page {page}: expected a list/results, got {type(data).__name__}"
        out.extend(results)
        if len(results) < PAGE_LIMIT:
            return out, ""
        page += 1


# ── Auth ─────────────────────────────────────────────────────────────────────

def authenticate() -> str:
    """Fetch a Breezeway access token ONCE (cached by the caller). Handles the
    1-req/min auth rate limit by respecting the server's retry timing on 429.
    Halts the program on unrecoverable auth failure."""
    _assert_readonly("POST")
    if not CLIENT_ID or not CLIENT_SECRET:
        REPORT.fail("Missing BREEZEWAY_CLIENT_ID / BREEZEWAY_CLIENT_SECRET in the environment.")
        _halt("Cannot authenticate without credentials.")
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            resp = _SESSION.post(AUTH_URL,
                                 json={"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET},
                                 timeout=30)
        except Exception as ex:
            REPORT.warn(f"auth: network error {ex} (attempt {attempt}/{MAX_ATTEMPTS})")
            time.sleep(min(BACKOFF_CAP, float(2 ** attempt)))
            continue
        if resp.status_code == 200:
            try:
                token = resp.json().get("access_token")
            except Exception as ex:
                REPORT.fail(f"auth: 200 but unparseable body: {ex}")
                _halt("Auth response could not be parsed.")
            if not token:
                REPORT.fail(f"auth: 200 but no access_token in body: {resp.text[:200]!r}")
                _halt("Auth succeeded but returned no token.")
            REPORT.info("Authenticated with Breezeway (token cached for this run).")
            return token
        if resp.status_code == 429:
            # Auth endpoint is limited to 1 request/minute — respect the timing.
            delay = _retry_delay_from(resp, attempt)
            delay = max(delay, 60.0)   # auth limit is per-minute
            REPORT.warn(f"auth: HTTP 429 (1/min limit). Waiting {delay:.0f}s (attempt {attempt}/{MAX_ATTEMPTS}).")
            time.sleep(delay)
            continue
        if resp.status_code >= 500:
            # Transient upstream/gateway error (e.g. 502/503/504) — back off and retry,
            # don't treat Breezeway's nginx hiccup as a permanent auth failure.
            delay = _retry_delay_from(resp, attempt)
            REPORT.warn(f"auth: HTTP {resp.status_code} transient gateway error "
                        f"(attempt {attempt}/{MAX_ATTEMPTS}); waiting {delay:.0f}s")
            time.sleep(delay)
            continue
        # Non-retryable 4xx (e.g. bad credentials) — fail loudly.
        REPORT.fail(f"auth: HTTP {resp.status_code}: {resp.text[:300]!r}")
        _halt("Authentication failed.")
    REPORT.fail("auth: exhausted retries.")
    _halt("Authentication failed after retries.")


# ── Company detection (only matters for cross-company partner keys) ──────────

def detect_company(token: str) -> dict:
    """Return extra params to add to every request if this is a multi-company
    (partner/enterprise) key. For a normal single-company key, returns {}.
    If MULTIPLE companies are detected we HALT and ask, rather than guess which
    company's data to report."""
    data, err, status = bw_get(COMPANY_URL, {"limit": PAGE_LIMIT, "page": 1}, token, "list companies")
    if err or status != 200:
        REPORT.info(f"Company list endpoint not usable (status {status}); assuming single-company key.")
        return {}
    if isinstance(data, dict):
        companies = data.get("results", data.get("data", []))
    elif isinstance(data, list):
        companies = data
    else:
        companies = []
    ids = sorted({str(c.get("id") or c.get("company_id")) for c in companies if isinstance(c, dict)} - {"None"})
    if len(ids) <= 1:
        REPORT.info(f"Single-company key (companies seen: {ids or 'none reported'}).")
        return {}
    REPORT.fail(f"Cross-company key detected — spans companies {ids}. "
                f"Re-run specifying which company_id to report; refusing to guess.")
    _halt("Multiple companies detected; specify company_id and re-run.")


# ── Name resolution (halts on zero / multiple matches) ───────────────────────

def fetch_people(token: str, extra: dict) -> list:
    # No status filter: this is a HISTORICAL report, so someone who was active most
    # of the year but has since left must still resolve (active-only would falsely
    # halt on them). We log the count so a human can sanity-check the roster size.
    people, err = bw_get_paginated(PEOPLE_URL, {**extra}, token, "people")
    if err:
        REPORT.fail(f"Could not load the people/users list: {err}")
        _halt("People list is required to resolve names.")
    if not people:
        REPORT.fail("People list came back empty — cannot resolve any names.")
        _halt("Empty people list.")
    REPORT.info(f"Loaded {len(people)} Breezeway users for name resolution.")
    return people


def _person_name(p: dict) -> str:
    return (p.get("name")
            or f"{p.get('first_name', '').strip()} {p.get('last_name', '').strip()}".strip()
            or p.get("email") or "").strip()


def resolve_names(people: list) -> dict:
    """Map each requested name → exactly one Breezeway user id. Halts (and prints
    every candidate) if any name has zero or multiple matches."""
    # Build a normalized index: lowercased full name → list of (id, display)
    index: dict[str, list] = defaultdict(list)
    for p in people:
        pid = p.get("id") or p.get("user_id") or p.get("assignee_id")
        disp = _person_name(p)
        if pid is None or not disp:
            continue
        index[disp.lower().strip()].append((pid, disp))

    resolved: dict[str, int] = {}
    problems = False
    REPORT.info("Resolving names → Breezeway user IDs:")
    for name in PEOPLE:
        if name in DROPPED_NAMES:
            REPORT.intentionally_dropped.append(name)
            REPORT.warn(f"   {name:<10} → EXCLUDED ON PURPOSE: {DROPPED_NAMES[name]}. "
                        f"Reported numbers will NOT include {name}.")
            continue
        key = name.lower().strip()
        # exact full-name match first
        cands = list(index.get(key, []))
        if not cands:
            # first-name / contains match (so "Andy" matches "Andy Smith")
            for disp_key, lst in index.items():
                first = disp_key.split()[0] if disp_key.split() else disp_key
                if first == key or disp_key.startswith(key + " ") or (f" {key} " in f" {disp_key} "):
                    cands.extend(lst)
            # de-dup by id
            seen = set(); uniq = []
            for pid, disp in cands:
                if pid not in seen:
                    seen.add(pid); uniq.append((pid, disp))
            cands = uniq

        if len(cands) == 1:
            pid, disp = cands[0]
            resolved[name] = int(pid)
            REPORT.info(f"   {name:<10} → id {pid}  ({disp})")
        elif len(cands) == 0:
            problems = True
            REPORT.fail(f"   {name:<10} → NO MATCH among {len(people)} Breezeway users. Halting this name.")
        else:
            problems = True
            listing = "; ".join(f"id {pid} ({disp})" for pid, disp in cands)
            REPORT.fail(f"   {name:<10} → {len(cands)} MATCHES: {listing}. Ambiguous — halting this name.")

    if problems:
        _halt("One or more names could not be resolved to exactly one user. "
              "Provide explicit user IDs (or fix the names) and re-run. No data was pulled.")
    return resolved


# ── Task field helpers (resolved against the dry inspect) ────────────────────

def _stringify(v) -> str:
    if isinstance(v, dict):
        return str(v.get("value") or v.get("name") or v.get("label") or "")
    return str(v or "")


def detect_completion_key(task: dict) -> str | None:
    for k in COMPLETION_KEYS:
        if task.get(k):
            return k
    return None


def task_name_values(task: dict) -> list:
    vals = []
    for k in NAME_KEYS:
        if k in task and task.get(k):
            vals.append((k, _stringify(task.get(k)).strip()))
    return vals


def is_bear_fence(task: dict) -> tuple:
    """(excluded?, field_that_matched). Case-insensitive, trimmed."""
    for k, v in task_name_values(task):
        if v.lower().strip() == EXCLUDE_TASK_NAME:
            return True, k
    return False, None


def task_finished_by_id(task: dict):
    """The single user who COMPLETED the task (Breezeway `finished_by`),
    or None if not recorded. This is the attribution basis for the report:
    'tasks completed per person' = who finished it, not who it was assigned to."""
    fb = task.get("finished_by")
    if isinstance(fb, dict) and fb.get("id") is not None:
        try:
            return int(fb["id"])
        except (TypeError, ValueError):
            return None
    return None


def task_assignee_ids(task: dict) -> list:
    ids = []
    for a in (task.get("assignments") or []):
        if isinstance(a, dict) and a.get("assignee_id") is not None:
            try: ids.append(int(a["assignee_id"]))
            except (TypeError, ValueError): pass
        elif isinstance(a, int):
            ids.append(a)
    for k in ("assignee_ids", "assigned_to"):
        v = task.get(k)
        if isinstance(v, list):
            for x in v:
                try: ids.append(int(x))
                except (TypeError, ValueError): pass
    return ids


def completion_date(task: dict, key: str) -> str | None:
    """Return the YYYY-MM-DD bucket from the completion timestamp, or None."""
    raw = task.get(key)
    if not raw:
        return None
    s = str(raw)
    # Fast path: ISO-ish string starting YYYY-MM-DD
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    # Fallback: parse a full datetime
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date().isoformat()
    except Exception:
        return None


# ── Property listing ─────────────────────────────────────────────────────────

def fetch_properties(token: str, extra: dict) -> list:
    # No status filter: a property deactivated since last year may still hold
    # completed tasks from the window. We surface that caveat in the summary.
    props, err = bw_get_paginated(PROPERTY_URL, {**extra}, token, "properties")
    if err:
        REPORT.fail(f"Property list errored: {err} (got {len(props)} before failing).")
    if not props:
        REPORT.fail("Property list is empty — nothing to scan.")
        _halt("No properties returned.")
    return props


def property_query_param(p: dict) -> tuple:
    """Pick the identifier param for a property's task query."""
    ref = p.get("reference_property_id")
    pid = p.get("id") or p.get("property_id") or p.get("home_id")
    if ref:
        return {"reference_property_id": ref}, f"ref={ref}"
    if pid is not None:
        return {"home_id": pid}, f"home_id={pid}"
    return {}, "NO-ID"


# ── Dry inspect ──────────────────────────────────────────────────────────────

def dry_inspect(token, extra, our_ids, finished_param, properties):
    # Field names (finished_at, assignments, name/title) are identical on ANY
    # completed task, so do NOT filter by our 6 people here — just grab the first
    # completed task in the window. That hits on the first active property instead
    # of hunting the whole portfolio (critical when Breezeway is slow). Cap the
    # scan so a flaky API can never make this run unbounded.
    INSPECT_CAP = 120
    REPORT.info(f"DRY INSPECT — grabbing the first completed task to confirm field names "
                f"(scanning up to {INSPECT_CAP} properties)…")
    for i, p in enumerate(properties[:INSPECT_CAP]):
        qp, label = property_query_param(p)
        if not qp:
            continue
        params = {**qp, **extra, "finished_at": finished_param}
        tasks, err = bw_get_paginated(TASK_URL, params, token, f"inspect tasks {label}")
        if err:
            REPORT.warn(f"inspect: property {label} errored: {err}")
            continue
        if tasks:
            t = tasks[0]
            ckey = detect_completion_key(t)
            print("\n" + "=" * 78)
            print("SAMPLE COMPLETED TASK (full JSON) — confirm the field names below match:")
            print("=" * 78)
            print(json.dumps(t, indent=2, default=str))
            print("=" * 78)
            print(f"Detected completion-date field : {ckey or 'NONE FOUND (expected one of ' + ', '.join(COMPLETION_KEYS) + ')'}")
            print(f"Detected name fields           : {task_name_values(t) or 'NONE'}")
            print(f"Detected assignee IDs          : {task_assignee_ids(t)}")
            print(f"  (our in-scope IDs            : {sorted(our_ids)})")
            print("=" * 78)
            if not ckey:
                REPORT.fail("No completion-date field found on the sample task — a full run "
                            "would not be able to bucket by day. Confirm the field name first.")
            return
        if i and i % 25 == 0:
            REPORT.info(f"inspect: scanned {i} properties, none with a completed task yet…")
    REPORT.warn("DRY INSPECT found no completed task in the window within the scanned properties.")


# ── Full tally ───────────────────────────────────────────────────────────────

def run_full(token, extra, resolved, finished_param, start_d, end_d, properties, delay):
    our_ids = set(resolved.values())
    id_to_name = {v: k for k, v in resolved.items()}

    counts: dict[int, dict[str, int]] = {uid: defaultdict(int) for uid in our_ids}
    seen_tasks: set = set()
    inspected_once = False

    REPORT.properties_total = len(properties)
    # Attribution is by `finished_by` (who COMPLETED the task), which can differ from
    # the assignee — so we deliberately do NOT filter the query by assignee_ids.
    # Instead we pull EVERY completed task per property in the window and keep only
    # those finished by our people. Heavier, but the accurate "completed by" measure.
    REPORT.info(f"Scanning {len(properties)} properties for ALL completed tasks {finished_param} "
                f"(attributing by finished_by to our {len(our_ids)} people)…")

    for i, p in enumerate(properties, 1):
        qp, label = property_query_param(p)
        if not qp:
            REPORT.warn(f"property #{i} has no usable id; skipped: {json.dumps(p)[:160]}")
            continue

        params = {**qp, **extra, "finished_at": finished_param}
        tasks, err = bw_get_paginated(TASK_URL, params, token, f"tasks {label}")
        if err:
            REPORT.fail(f"property {label}: {err} — its completed tasks are MISSING from the report.")
            # keep going so the rest of the report still builds (marked PARTIAL)
            time.sleep(delay)
            continue

        REPORT.properties_scanned += 1

        for t in tasks:
            tid = t.get("id")
            if tid is not None:
                if tid in seen_tasks:
                    continue          # de-dup: same task surfaced under two queries
                seen_tasks.add(tid)

            # One-time field confirmation, printed into the log of the real run too.
            if not inspected_once:
                inspected_once = True
                ckey0 = detect_completion_key(t)
                REPORT.completion_key_used = ckey0
                REPORT.info(f"First completed task seen → completion field = {ckey0!r}, "
                            f"name fields = {task_name_values(t)}, finished_by = {task_finished_by_id(t)}")
                if not ckey0:
                    REPORT.fail("No completion-date field on returned tasks; cannot bucket by day. "
                                "Run --inspect and confirm the field name.")

            excluded, field = is_bear_fence(t)
            if excluded:
                REPORT.excluded_bear_fence += 1
                REPORT.name_field_for_exclusion[field] += 1
                continue

            completer = task_finished_by_id(t)
            if completer is None:
                # Finished task with no recorded completer — surfaced, never guessed.
                REPORT.no_completer += 1
                continue
            if completer not in our_ids:
                REPORT.completed_by_others += 1   # finished by someone outside our 6
                continue

            ckey = REPORT.completion_key_used or detect_completion_key(t)
            day = completion_date(t, ckey) if ckey else None
            if not day:
                REPORT.no_completion_date += 1
                REPORT.warn(f"task {tid}: no parseable completion date ({ckey}={t.get(ckey)!r}); not counted.")
                continue
            if not (start_d.isoformat() <= day <= end_d.isoformat()):
                REPORT.out_of_range_skipped += 1
                continue

            REPORT.tasks_seen += 1
            REPORT.tasks_counted += 1
            counts[completer][day] += 1
            REPORT.person_increments += 1

        if i % 25 == 0:
            REPORT.info(f"  …{i}/{len(properties)} properties scanned "
                        f"({REPORT.tasks_counted} tasks counted so far)")
        time.sleep(delay)

    return counts, id_to_name


# ── Output ───────────────────────────────────────────────────────────────────

def write_csv(path, counts, id_to_name):
    rows = []
    for uid, byday in counts.items():
        for day, n in byday.items():
            if n:
                rows.append((id_to_name[uid], uid, day, n))
    rows.sort(key=lambda r: (r[0].lower(), r[2]))
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["person", "user_id", "date", "completed_tasks"])
        w.writerows(rows)
    return len(rows)


def write_summary(path, counts, id_to_name, start_d, end_d, csv_rows, args, status_word):
    lines = []
    A = lines.append
    A("# Productivity Past 365 Days")
    A("")
    if REPORT.is_complete:
        A(f"**Status: ✅ COMPLETE** — every property was scanned without error.")
    else:
        A(f"**Status: ⚠️ PARTIAL — {len(REPORT.failures)} failure(s).** "
          f"Some data is MISSING; the counts below UNDERSTATE reality. See *Failures* at the bottom.")
    A("")
    if REPORT.intentionally_dropped:
        dropped = ", ".join(f"**{n}** ({DROPPED_NAMES.get(n, 'operator decision')})"
                            for n in REPORT.intentionally_dropped)
        A(f"> ⚠️ **Requested {len(PEOPLE)} people; reporting "
          f"{len(PEOPLE) - len(REPORT.intentionally_dropped)}.** Intentionally excluded: {dropped}. "
          f"These people are NOT included in any count below — this is a deliberate operator "
          f"decision, not a data gap.")
        A("")
    A(f"- Date range covered: **{start_d.isoformat()} → {end_d.isoformat()}** "
      f"({(end_d - start_d).days} days)")
    A(f"- Run timestamp: {datetime.now().astimezone().isoformat(timespec='seconds')}")
    A(f"- Properties in account: {REPORT.properties_total}")
    A(f"- Properties scanned OK: {REPORT.properties_scanned}")
    A(f"- Completed tasks counted: {REPORT.tasks_counted}")
    A(f"- Per-person tallies (sum of CSV counts): {REPORT.person_increments}")
    A(f"- Excluded \"Disarm Bear Fence\" tasks: {REPORT.excluded_bear_fence}")
    if args.max_properties:
        A(f"- ⚠️ TEST MODE: only the first {args.max_properties} properties were scanned "
          f"→ report is intentionally PARTIAL.")
    A("")

    # Counting rules / interpretation
    A("## How to read these numbers")
    A("- **Counted by who finished it (`finished_by`).** Each finished task is credited to the "
      "single person who marked it complete — NOT who it was assigned to. So each task counts "
      "exactly once, for the person who finished it.")
    A(f"- **Completion field used:** `{REPORT.completion_key_used}` — the day bucket is the "
      "date portion of that timestamp **as Breezeway returns it**. If Breezeway returns UTC, "
      "a task finished late at night local time may land on the next day. Note this when "
      "comparing day-to-day.")
    A(f"- **Exclusion:** tasks named \"{EXCLUDE_TASK_NAME}\" (case-insensitive) are filtered "
      f"out, matched on field(s): "
      f"{dict(REPORT.name_field_for_exclusion) or '(none seen)'}. They are NOT removed from Breezeway.")
    A("- **De-dup:** each task id is counted once even if it surfaces under two property queries.")
    A("")

    # Per-user yearly totals
    A("## Per-person yearly totals")
    A("")
    A("| Person | User ID | Completed tasks (365d) | Active days |")
    A("|---|---|---|---|")
    for name in PEOPLE:
        if name in REPORT.intentionally_dropped:
            A(f"| {name} | — | EXCLUDED (operator) | — |")
            continue
        uid = None
        for u, n in id_to_name.items():
            if n == name:
                uid = u; break
        if uid is None:
            A(f"| {name} | (unresolved) | — | — |")
            continue
        byday = counts.get(uid, {})
        total = sum(byday.values())
        A(f"| {name} | {uid} | {total} | {sum(1 for v in byday.values() if v)} |")
    A("")

    # Monthly roll-up per person (directly answers the up/down trend question)
    A("## Monthly totals per person (for trend)")
    A("")
    months = sorted({day[:7] for byday in counts.values() for day in byday})
    if months:
        header = "| Person | " + " | ".join(months) + " | Total |"
        A(header)
        A("|" + "---|" * (len(months) + 2))
        for name in PEOPLE:
            if name in REPORT.intentionally_dropped:
                A(f"| {name} | " + " | ".join("—" for _ in months) + " | EXCLUDED |")
                continue
            uid = next((u for u, n in id_to_name.items() if n == name), None)
            if uid is None:
                A(f"| {name} | " + " | ".join("—" for _ in months) + " | — |")
                continue
            bymonth = defaultdict(int)
            for day, n in counts.get(uid, {}).items():
                bymonth[day[:7]] += n
            cells = " | ".join(str(bymonth.get(m, 0)) for m in months)
            A(f"| {name} | {cells} | {sum(bymonth.values())} |")
        A("")
        A("_Read left→right per row: rising numbers = more completed tasks over time, "
          "falling = fewer. Compare the first few months to the last few._")
    else:
        A("_No completed tasks found in the window for any of these people._")
    A("")

    # Diagnostics
    A("## Run diagnostics")
    A(f"- Completed tasks finished by people outside this group (not counted): {REPORT.completed_by_others}")
    A(f"- Finished tasks with no `finished_by` recorded (not counted): {REPORT.no_completer}")
    A(f"- Tasks with finished_at outside the window (skipped): {REPORT.out_of_range_skipped}")
    A(f"- Tasks with no parseable completion date (skipped): {REPORT.no_completion_date}")
    A(f"- CSV rows written: {csv_rows}")
    A("- Caveat: properties were listed without a status filter. If Breezeway's property "
      "endpoint defaults to active-only, completed tasks at since-deactivated properties "
      "could be missing — treat this as a known blind spot, not a verified-complete scan.")
    A("")

    # Failures & warnings — never rounded away
    A("## Failures")
    if REPORT.failures:
        for fmsg in REPORT.failures:
            A(f"- ❌ {fmsg}")
    else:
        A("- None. 🎉")
    A("")
    A("## Warnings")
    if REPORT.warnings:
        for w in REPORT.warnings[:200]:
            A(f"- ⚠️ {w}")
        if len(REPORT.warnings) > 200:
            A(f"- …and {len(REPORT.warnings) - 200} more (see full log).")
    else:
        A("- None.")
    A("")

    A("## Full run log")
    A("```")
    lines.extend(REPORT.log_lines)
    A("```")

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def write_json(path, counts, id_to_name, start_d, end_d, args):
    """Structured payload for the in-app page (templates/productivity.html) — the
    SAME numbers as the CSV/summary, just machine-readable so the web view can
    render the tables without re-running the (slow) scan."""
    months = sorted({day[:7] for byday in counts.values() for day in byday})
    people = []
    for name in PEOPLE:
        if name in REPORT.intentionally_dropped:
            people.append({"name": name, "excluded": True})
            continue
        uid = next((u for u, n in id_to_name.items() if n == name), None)
        byday = counts.get(uid, {}) if uid is not None else {}
        bymonth = defaultdict(int)
        for day, n in byday.items():
            bymonth[day[:7]] += n
        people.append({
            "name": name,
            "user_id": uid,
            "excluded": False,
            "yearly_total": sum(byday.values()),
            "active_days": sum(1 for v in byday.values() if v),
            "by_month": dict(bymonth),
            "by_day": dict(sorted(byday.items())),
        })
    payload = {
        "title": "Productivity Past 365 Days",
        "status": "PARTIAL" if (not REPORT.is_complete or args.max_properties) else "COMPLETE",
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "window": {"start": start_d.isoformat(), "end": end_d.isoformat(),
                   "days": (end_d - start_d).days},
        "completion_field": REPORT.completion_key_used,
        "counting_basis": "by completer (finished_by)",
        "properties_total": REPORT.properties_total,
        "properties_scanned": REPORT.properties_scanned,
        "tasks_counted": REPORT.tasks_counted,
        "person_increments": REPORT.person_increments,
        "excluded_bear_fence": REPORT.excluded_bear_fence,
        "months": months,
        "people": people,
        "failures": REPORT.failures,
        "warnings_count": len(REPORT.warnings),
        "intentionally_dropped": REPORT.intentionally_dropped,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


# ── Halt helper ──────────────────────────────────────────────────────────────

def _halt(reason: str):
    """Loud, non-zero halt. Writes a minimal summary so the failure is on record."""
    print("\n" + "!" * 78, file=sys.stderr)
    print(f"HALTED: {reason}", file=sys.stderr)
    print("This run produced NO trustworthy report. See the messages above.", file=sys.stderr)
    print("!" * 78, file=sys.stderr)
    sys.exit(2)


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Productivity Past 365 Days — read-only Breezeway report.")
    ap.add_argument("--inspect", action="store_true",
                    help="Dry inspect: print one completed task's JSON and detected fields, then exit.")
    ap.add_argument("--days", type=int, default=365, help="Window length in days (default 365).")
    ap.add_argument("--end", default="",
                    help="End date YYYY-MM-DD of the window (default: today). Window = [end - days, end]. "
                         "Use this to run a PRIOR cycle, e.g. last year's same date.")
    ap.add_argument("--prior", action="store_true",
                    help="Write to the PRIOR-cycle report files (productivity_past_365_days_prior.*) instead of "
                         "the current ones, so the page can compare the two cycles. Pair with --end.")
    ap.add_argument("--delay", type=float, default=0.25,
                    help="Polite pause between property calls, seconds (default 0.25).")
    ap.add_argument("--outdir",
                    default=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "reports"),
                    help="Directory for the report files (default <repo>/reports).")
    ap.add_argument("--max-properties", type=int, default=0,
                    help="TESTING ONLY: scan only the first N properties (forces a PARTIAL report).")
    args = ap.parse_args()

    if args.end:
        try:
            end_d = date.fromisoformat(args.end)
        except ValueError:
            print("--end must be a date in YYYY-MM-DD form.", file=sys.stderr)
            sys.exit(2)
    else:
        end_d = date.today()
    start_d = end_d - timedelta(days=args.days)
    finished_param = f"{start_d.isoformat()},{end_d.isoformat()}"

    REPORT.info(f"Productivity Past 365 Days — window {finished_param}")

    token = authenticate()
    extra = detect_company(token)
    people = fetch_people(token, extra)
    resolved = resolve_names(people)          # halts on zero/ambiguous
    our_ids = set(resolved.values())

    properties = fetch_properties(token, extra)
    if args.max_properties:
        REPORT.warn(f"TEST MODE: limiting scan to first {args.max_properties} of {len(properties)} properties.")
        properties = properties[:args.max_properties]

    if args.inspect:
        dry_inspect(token, extra, our_ids, finished_param, properties)
        # inspect is informational; exit non-zero only if a hard failure was logged
        sys.exit(0 if REPORT.is_complete else 1)

    counts, id_to_name = run_full(token, extra, resolved, finished_param,
                                  start_d, end_d, properties, args.delay)

    os.makedirs(args.outdir, exist_ok=True)
    # --prior routes to a separate file set so the two cycles don't overwrite each
    # other; the page reads both and compares them.
    suffix = "_prior" if args.prior else ""
    csv_path  = os.path.join(args.outdir, f"productivity_past_365_days{suffix}.csv")
    sum_path  = os.path.join(args.outdir, f"productivity_past_365_days{suffix}_summary.md")
    json_path = os.path.join(args.outdir, f"productivity_past_365_days{suffix}.json")

    csv_rows = write_csv(csv_path, counts, id_to_name)
    status_word = "COMPLETE" if REPORT.is_complete else "PARTIAL"
    if args.max_properties:
        status_word = "PARTIAL"
    write_summary(sum_path, counts, id_to_name, start_d, end_d, csv_rows, args, status_word)
    write_json(json_path, counts, id_to_name, start_d, end_d, args)

    REPORT.info(f"Wrote {csv_path} ({csv_rows} rows), {sum_path}, and {json_path}")
    print("\n" + "=" * 78)
    if REPORT.is_complete and not args.max_properties:
        print(f"COMPLETE — {REPORT.tasks_counted} tasks counted across "
              f"{REPORT.properties_scanned}/{REPORT.properties_total} properties.")
        sys.exit(0)
    else:
        print(f"PARTIAL — {len(REPORT.failures)} failure(s). The report says so at the top. "
              f"See {sum_path}.")
        sys.exit(1)


if __name__ == "__main__":
    main()
