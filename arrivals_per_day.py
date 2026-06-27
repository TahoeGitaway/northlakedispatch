#!/usr/bin/env python3
"""
Average Arrivals Per Day
========================

Standalone, STRICTLY READ-ONLY Breezeway report: how many guest/owner/lease
arrivals (check-ins) happen per day over a rolling window, and the average
arrivals/day across that window.

Counterpart to productivity_past_365_days.py — same auth, same safety rules,
same output shape (CSV + JSON + summary .md). The difference: this counts
RESERVATION check-ins, not completed tasks, and it pulls them in a single
date-ranged query (no per-property loop), so it is far lighter on the API.

------------------------------------------------------------------------------
NON-NEGOTIABLE RULES (enforced in code):
  1. READ-ONLY. Only GET requests (plus the one auth POST). Never PATCH/PUT/
     DELETE/POST-to-a-record. (See `_assert_readonly` — a hard guard.)
  2. NEVER FAIL SILENTLY. Every auth error, 429, 5xx, empty/malformed page, or
     unparseable date is logged and reflected in the final summary. If anything
     failed, the header says "PARTIAL — N failures" and the process exits non-zero.
  3. BLOCKS ARE NOT ARRIVALS. Internal holds/blocks are classified out of the
     arrival counts (same classification logic the web app uses), never counted
     as guest activity.

------------------------------------------------------------------------------
USAGE
  Set credentials (same env vars the web app uses):
      BREEZEWAY_CLIENT_ID, BREEZEWAY_CLIENT_SECRET

  Dry inspect FIRST (confirm the reservation field names before trusting a year):
      python arrivals_per_day.py --inspect
        → authenticates, pulls ONE reservation in the window and prints its full
          JSON plus the detected check-in-date / type fields. Confirm, then run.

  Full run:
      python arrivals_per_day.py
        → writes arrivals_per_day.csv      (one row per day: counts by type)
                 arrivals_per_day.json      (machine-readable, for a future page)
                 arrivals_per_day_summary.md (averages, monthly trend, failures)

  Options:
      --days N      window length (default 365)
      --end DATE    end of window YYYY-MM-DD (default today). Window = [end-days, end].
      --prior       write to the *_prior.* file set (pair with --end for a prior cycle).
      --outdir DIR  where to write the report files (default ".")

  Exit code: 0 = COMPLETE, non-zero = PARTIAL or HALTED. See the summary file.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta

import requests

# Resilient console output on non-UTF-8 terminals (a print blowing up would be a
# silent-ish failure, which this tool forbids).
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# Load credentials from a local .env the same way the web app does.
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ── Configuration ────────────────────────────────────────────────────────────

BASE = "https://api.breezeway.io"
AUTH_URL        = f"{BASE}/public/auth/v1/"
RESERVATION_URL = f"{BASE}/public/inventory/v1/reservation"

CLIENT_ID     = os.environ.get("BREEZEWAY_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("BREEZEWAY_CLIENT_SECRET", "")

# Arrival types we count. Blocks are excluded entirely (internal holds, not
# real guest/owner activity) — matched on the classification below.
ARRIVAL_TYPES = ["guest", "owner", "lease"]

# Internal-hold reservation/stay types — classified out of arrivals.
BLOCK_TYPES = {"block", "maintenance", "hold", "owner_block", "management_block"}

MAX_ATTEMPTS = 5          # per-request retry cap before a loud failure
BACKOFF_CAP  = 60         # seconds — ceiling on exponential backoff
PAGE_LIMIT   = 100        # Breezeway max page size

_ALLOWED_METHODS = {"GET", "POST"}   # POST is used ONLY for the auth endpoint
_SESSION = requests.Session()


# ── Run report (accumulates everything for the final summary) ────────────────

class RunReport:
    def __init__(self):
        self.failures: list[str] = []
        self.warnings: list[str] = []
        self.log_lines: list[str] = []
        self.reservations_seen = 0
        self.arrivals_counted = 0
        self.blocks_excluded = 0
        self.no_checkin_date = 0
        self.out_of_range_skipped = 0
        self.checkin_key_used: str | None = None

    def fail(self, msg: str):
        self.failures.append(msg)
        self._emit(f"[FAIL] {msg}")

    def warn(self, msg: str):
        self.warnings.append(msg)
        self._emit(f"[WARN] {msg}")

    def info(self, msg: str):
        self._emit(f"[info] {msg}")

    def _emit(self, line: str):
        full = f"{datetime.now().strftime('%H:%M:%S')} {line}"
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
    for h in ("Retry-After", "retry-after", "X-RateLimit-Reset", "x-ratelimit-reset"):
        v = resp.headers.get(h)
        if v:
            try:
                return min(BACKOFF_CAP, max(1.0, float(v)))
            except ValueError:
                pass
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
    """Single GET with retry/backoff. Returns (json_or_None, error_or_'', status)."""
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
        detail = resp.text[:300]
        try:
            detail = resp.json()
        except Exception:
            pass
        return None, f"HTTP {resp.status_code}: {detail}", resp.status_code

    return None, f"gave up after {MAX_ATTEMPTS} attempts (last status {last_status})", last_status


def bw_get_paginated(url: str, params: dict, token: str, what: str) -> tuple:
    """Walk every page until a page returns < PAGE_LIMIT records.
    Returns (all_results, error_or_'')."""
    out = []
    page = 1
    while True:
        data, err, status = bw_get(url, {**params, "limit": PAGE_LIMIT, "page": page},
                                   token, f"{what} (page {page})")
        if err:
            return out, err
        if isinstance(data, dict):
            results = data.get("results", data.get("data", []))
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
            delay = max(_retry_delay_from(resp, attempt), 60.0)   # auth is 1/min
            REPORT.warn(f"auth: HTTP 429 (1/min limit). Waiting {delay:.0f}s (attempt {attempt}/{MAX_ATTEMPTS}).")
            time.sleep(delay)
            continue
        if resp.status_code >= 500:
            delay = _retry_delay_from(resp, attempt)
            REPORT.warn(f"auth: HTTP {resp.status_code} transient gateway error "
                        f"(attempt {attempt}/{MAX_ATTEMPTS}); waiting {delay:.0f}s")
            time.sleep(delay)
            continue
        REPORT.fail(f"auth: HTTP {resp.status_code}: {resp.text[:300]!r}")
        _halt("Authentication failed.")
    REPORT.fail("auth: exhausted retries.")
    _halt("Authentication failed after retries.")


# ── Reservation classification (mirrors routes/briefing.py exactly) ──────────

def _extract_str(val) -> str:
    """Pull a lowercase machine-readable string out of whatever Breezeway sends.
    type_stay / type_reservation are dicts like {"code": "owner", "name": "Owner Stay"}."""
    if not val:
        return ""
    if isinstance(val, dict):
        return (val.get("code") or val.get("name") or
                val.get("label") or val.get("type") or "").lower().strip()
    return str(val).lower().strip()


def classify_reservation(r: dict) -> str:
    """Returns 'lease', 'owner', 'block', or 'guest'. Same priority order the
    web app uses so the numbers are consistent with the briefing/calendar."""
    ts = _extract_str(r.get("type_stay"))
    tr = _extract_str(r.get("type_reservation"))
    tag_names = [_extract_str(t) for t in (r.get("tags") or [])]

    if tr in BLOCK_TYPES or ts in BLOCK_TYPES:
        return "block"
    if ts == "owner":
        return "owner"
    if "owner next" in tag_names:
        return "owner"
    checkin  = r.get("checkin_date")  or ""
    checkout = r.get("checkout_date") or ""
    if checkin and checkout:
        try:
            if (date.fromisoformat(checkout[:10]) - date.fromisoformat(checkin[:10])).days >= 30:
                return "lease"
        except Exception:
            pass
    if ts == "lease":
        return "lease"
    return "guest"


CHECKIN_KEYS = ["checkin_date", "check_in_date", "arrival_date", "start_date", "date_checkin"]


def detect_checkin_key(r: dict) -> str | None:
    for k in CHECKIN_KEYS:
        if r.get(k):
            return k
    return None


def checkin_day(r: dict, key: str) -> str | None:
    raw = r.get(key)
    if not raw:
        return None
    s = str(raw)
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date().isoformat()
    except Exception:
        return None


# ── Fetch ────────────────────────────────────────────────────────────────────

def fetch_arrivals(token: str, start_d: date, end_d: date) -> list:
    """Single date-ranged pull of every reservation checking in within the window."""
    params = {"checkin_date_ge": start_d.isoformat(), "checkin_date_le": end_d.isoformat()}
    REPORT.info(f"Fetching reservations with check-in in [{start_d} … {end_d}] (single ranged query, paginated)…")
    reservations, err = bw_get_paginated(RESERVATION_URL, params, token, "reservations")
    if err:
        REPORT.fail(f"Reservation fetch errored: {err} (got {len(reservations)} before failing). "
                    f"Counts below UNDERSTATE reality.")
    if not reservations:
        REPORT.warn("Reservation query returned zero rows for the window.")
    REPORT.info(f"Pulled {len(reservations)} reservation rows.")
    return reservations


# ── Dry inspect ──────────────────────────────────────────────────────────────

def dry_inspect(token, start_d, end_d):
    reservations = fetch_arrivals(token, start_d, end_d)
    if not reservations:
        REPORT.warn("DRY INSPECT found no reservation in the window.")
        return
    r = reservations[0]
    ckey = detect_checkin_key(r)
    print("\n" + "=" * 78)
    print("SAMPLE RESERVATION (full JSON) — confirm the field names below match:")
    print("=" * 78)
    print(json.dumps(r, indent=2, default=str))
    print("=" * 78)
    print(f"Detected check-in-date field : {ckey or 'NONE FOUND (expected one of ' + ', '.join(CHECKIN_KEYS) + ')'}")
    print(f"type_stay                    : {r.get('type_stay')!r}")
    print(f"type_reservation             : {r.get('type_reservation')!r}")
    print(f"tags                         : {r.get('tags')!r}")
    print(f"classified_as                : {classify_reservation(r)}")
    print("=" * 78)
    if not ckey:
        REPORT.fail("No check-in-date field found on the sample reservation — a full run "
                    "could not bucket by day. Confirm the field name first.")


# ── Tally ────────────────────────────────────────────────────────────────────

def run_full(token, start_d, end_d, reservations):
    # counts[day][type] = n ; also a set of reservation ids for de-dup
    counts: dict[str, dict[str, int]] = defaultdict(lambda: {t: 0 for t in ARRIVAL_TYPES})
    seen: set = set()

    for r in reservations:
        rid = r.get("id")
        if rid is not None:
            if rid in seen:
                continue
            seen.add(rid)
        REPORT.reservations_seen += 1

        kind = classify_reservation(r)
        if kind == "block":
            REPORT.blocks_excluded += 1
            continue

        if REPORT.checkin_key_used is None:
            REPORT.checkin_key_used = detect_checkin_key(r)
            REPORT.info(f"First reservation seen → check-in field = {REPORT.checkin_key_used!r}, "
                        f"classified = {kind}")
        ckey = REPORT.checkin_key_used or detect_checkin_key(r)
        day = checkin_day(r, ckey) if ckey else None
        if not day:
            REPORT.no_checkin_date += 1
            REPORT.warn(f"reservation {rid}: no parseable check-in date ({ckey}={r.get(ckey)!r}); not counted.")
            continue
        if not (start_d.isoformat() <= day <= end_d.isoformat()):
            # The API range is inclusive, but guard anyway — never silently miscount.
            REPORT.out_of_range_skipped += 1
            continue
        if kind not in counts[day]:
            counts[day][kind] = 0
        counts[day][kind] += 1
        REPORT.arrivals_counted += 1

    return counts


# ── Output ───────────────────────────────────────────────────────────────────

def _all_days(start_d: date, end_d: date) -> list:
    out, d = [], start_d
    while d <= end_d:
        out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def write_csv(path, counts, start_d, end_d):
    days = _all_days(start_d, end_d)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "total_arrivals"] + ARRIVAL_TYPES)
        for ds in days:
            row = counts.get(ds, {})
            by = [row.get(t, 0) for t in ARRIVAL_TYPES]
            w.writerow([ds, sum(by)] + by)
    return len(days)


def _averages(counts, days):
    n_days = len(days)
    total = sum(sum(counts.get(ds, {}).values()) for ds in days)
    by_type_total = {t: sum(counts.get(ds, {}).get(t, 0) for ds in days) for t in ARRIVAL_TYPES}
    days_with = sum(1 for ds in days if sum(counts.get(ds, {}).values()) > 0)
    return {
        "total_arrivals": total,
        "window_days": n_days,
        "avg_per_calendar_day": round(total / n_days, 2) if n_days else 0,
        "days_with_arrivals": days_with,
        "avg_per_active_day": round(total / days_with, 2) if days_with else 0,
        "by_type_total": by_type_total,
        "by_type_avg_per_day": {t: round(by_type_total[t] / n_days, 2) if n_days else 0
                                for t in ARRIVAL_TYPES},
    }


def write_summary(path, counts, start_d, end_d, days):
    a = _averages(counts, days)
    L = []
    P = L.append
    P("# Average Arrivals Per Day")
    P("")
    if REPORT.is_complete:
        P("**Status: ✅ COMPLETE** — the full window was fetched without error.")
    else:
        P(f"**Status: ⚠️ PARTIAL — {len(REPORT.failures)} failure(s).** Some reservations are "
          f"MISSING; the counts below UNDERSTATE reality. See *Failures* at the bottom.")
    P("")
    P(f"- Window: **{start_d.isoformat()} → {end_d.isoformat()}** ({a['window_days']} days)")
    P(f"- Run timestamp: {datetime.now().astimezone().isoformat(timespec='seconds')}")
    P(f"- Reservations examined: {REPORT.reservations_seen}")
    P(f"- Blocks/holds excluded: {REPORT.blocks_excluded}")
    P("")
    P("## The headline number")
    P("")
    P(f"- **Average arrivals per day: {a['avg_per_calendar_day']}** "
      f"({a['total_arrivals']} arrivals ÷ {a['window_days']} calendar days)")
    P(f"- Average per *active* day (days with ≥1 arrival): {a['avg_per_active_day']} "
      f"(over {a['days_with_arrivals']} active days)")
    P("")
    P("By type (per calendar day):")
    P("")
    P("| Type | Total | Avg / day |")
    P("|---|---|---|")
    for t in ARRIVAL_TYPES:
        P(f"| {t} | {a['by_type_total'][t]} | {a['by_type_avg_per_day'][t]} |")
    P(f"| **all** | **{a['total_arrivals']}** | **{a['avg_per_calendar_day']}** |")
    P("")
    P("## How to read these numbers")
    P("- **Arrivals = reservation check-ins** in the window, classified guest/owner/lease. "
      "**Blocks and internal holds are excluded** (same logic as the app's briefing & calendar).")
    P(f"- **Check-in field used:** `{REPORT.checkin_key_used}` — the day bucket is the date "
      "portion of that value as Breezeway returns it.")
    P("- **Two averages:** *per calendar day* divides by every day in the window (the true "
      "daily rate, including zero-arrival days); *per active day* divides only by days that "
      "had at least one arrival (typical size of a busy day).")
    P("- **De-dup:** each reservation id is counted once.")
    P("")
    P("## Monthly trend (arrivals per month)")
    P("")
    bymonth = defaultdict(int)
    for ds in days:
        bymonth[ds[:7]] += sum(counts.get(ds, {}).values())
    months = sorted(bymonth)
    if months:
        P("| Month | Arrivals | Avg / day |")
        P("|---|---|---|")
        for m in months:
            mdays = sum(1 for ds in days if ds[:7] == m)
            avg = round(bymonth[m] / mdays, 2) if mdays else 0
            P(f"| {m} | {bymonth[m]} | {avg} |")
        P("")
        P("_Read top→bottom: rising = busier arrival months, falling = quieter._")
    else:
        P("_No arrivals found in the window._")
    P("")
    P("## Run diagnostics")
    P(f"- Arrivals counted: {REPORT.arrivals_counted}")
    P(f"- Reservations with no parseable check-in date (skipped): {REPORT.no_checkin_date}")
    P(f"- Reservations outside the window (skipped): {REPORT.out_of_range_skipped}")
    P("")
    P("## Failures")
    if REPORT.failures:
        for f in REPORT.failures:
            P(f"- ❌ {f}")
    else:
        P("- None. 🎉")
    P("")
    P("## Warnings")
    if REPORT.warnings:
        for w in REPORT.warnings[:200]:
            P(f"- ⚠️ {w}")
        if len(REPORT.warnings) > 200:
            P(f"- …and {len(REPORT.warnings) - 200} more (see full log).")
    else:
        P("- None.")
    P("")
    P("## Full run log")
    P("```")
    L.extend(REPORT.log_lines)
    P("```")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(L) + "\n")


def write_json(path, counts, start_d, end_d, days):
    a = _averages(counts, days)
    bymonth = defaultdict(int)
    for ds in days:
        bymonth[ds[:7]] += sum(counts.get(ds, {}).values())
    payload = {
        "title": "Average Arrivals Per Day",
        "status": "COMPLETE" if REPORT.is_complete else "PARTIAL",
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "window": {"start": start_d.isoformat(), "end": end_d.isoformat(), "days": a["window_days"]},
        "checkin_field": REPORT.checkin_key_used,
        "arrival_types": ARRIVAL_TYPES,
        "averages": a,
        "by_month": dict(sorted(bymonth.items())),
        "by_day": {ds: counts.get(ds, {t: 0 for t in ARRIVAL_TYPES}) for ds in days},
        "reservations_examined": REPORT.reservations_seen,
        "blocks_excluded": REPORT.blocks_excluded,
        "failures": REPORT.failures,
        "warnings_count": len(REPORT.warnings),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


# ── Halt helper ──────────────────────────────────────────────────────────────

def _halt(reason: str):
    print("\n" + "!" * 78, file=sys.stderr)
    print(f"HALTED: {reason}", file=sys.stderr)
    print("This run produced NO trustworthy report. See the messages above.", file=sys.stderr)
    print("!" * 78, file=sys.stderr)
    sys.exit(2)


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Average Arrivals Per Day — read-only Breezeway report.")
    ap.add_argument("--inspect", action="store_true",
                    help="Dry inspect: print one reservation's JSON and detected fields, then exit.")
    ap.add_argument("--days", type=int, default=365, help="Window length in days (default 365).")
    ap.add_argument("--end", default="",
                    help="End date YYYY-MM-DD of the window (default today). Window = [end-days, end].")
    ap.add_argument("--prior", action="store_true",
                    help="Write to the PRIOR-cycle file set (arrivals_per_day_prior.*). Pair with --end.")
    ap.add_argument("--outdir", default=".", help="Directory for the report files (default current dir).")
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

    REPORT.info(f"Average Arrivals Per Day — window {start_d.isoformat()} → {end_d.isoformat()}")

    token = authenticate()

    if args.inspect:
        dry_inspect(token, start_d, end_d)
        sys.exit(0 if REPORT.is_complete else 1)

    reservations = fetch_arrivals(token, start_d, end_d)
    counts = run_full(token, start_d, end_d, reservations)
    days = _all_days(start_d, end_d)

    os.makedirs(args.outdir, exist_ok=True)
    suffix = "_prior" if args.prior else ""
    csv_path  = os.path.join(args.outdir, f"arrivals_per_day{suffix}.csv")
    sum_path  = os.path.join(args.outdir, f"arrivals_per_day{suffix}_summary.md")
    json_path = os.path.join(args.outdir, f"arrivals_per_day{suffix}.json")

    csv_rows = write_csv(csv_path, counts, start_d, end_d)
    write_summary(sum_path, counts, start_d, end_d, days)
    write_json(json_path, counts, start_d, end_d, days)

    a = _averages(counts, days)
    REPORT.info(f"Wrote {csv_path} ({csv_rows} day-rows), {sum_path}, and {json_path}")
    print("\n" + "=" * 78)
    if REPORT.is_complete:
        print(f"COMPLETE — avg {a['avg_per_calendar_day']} arrivals/day "
              f"({a['total_arrivals']} arrivals over {a['window_days']} days).")
        sys.exit(0)
    else:
        print(f"PARTIAL — {len(REPORT.failures)} failure(s). The report says so at the top. "
              f"See {sum_path}.")
        sys.exit(1)


if __name__ == "__main__":
    main()
