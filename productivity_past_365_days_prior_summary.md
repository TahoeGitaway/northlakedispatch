# Productivity Past 365 Days

**Status: ✅ COMPLETE** — every property was scanned without error.

- Date range covered: **2024-06-25 → 2025-06-25** (365 days)
- Run timestamp: 2026-06-26T12:25:31-07:00
- Properties in account: 517
- Properties scanned OK: 517
- Completed tasks counted: 3535
- Per-person tallies (sum of CSV counts): 3535
- Excluded "Disarm Bear Fence" tasks: 1

## How to read these numbers
- **Counted by who completed it (`finished_by`).** Each finished task is credited to the single person who marked it complete — NOT who it was assigned to. So each task counts exactly once, for the person who actually did it.
- **Completion field used:** `finished_at` — the day bucket is the date portion of that timestamp **as Breezeway returns it**. If Breezeway returns UTC, a task finished late at night local time may land on the next day. Note this when comparing day-to-day.
- **Exclusion:** tasks named "disarm bear fence" (case-insensitive) are filtered out, matched on field(s): {'name': 1}. They are NOT removed from Breezeway.
- **De-dup:** each task id is counted once even if it surfaces under two property queries.

## Per-person yearly totals

| Person | User ID | Completed tasks (365d) | Active days |
|---|---|---|---|
| Andy | 250612 | 698 | 92 |
| Trevor | 250618 | 886 | 101 |
| Calder | 373683 | 0 | 0 |
| Jonah | 365226 | 0 | 0 |
| Irving | 250622 | 954 | 104 |
| Chris | 266840 | 997 | 105 |

## Monthly totals per person (for trend)

| Person | 2025-01 | 2025-02 | 2025-03 | 2025-04 | 2025-05 | 2025-06 | Total |
|---|---|---|---|---|---|---|---|
| Andy | 121 | 187 | 180 | 58 | 92 | 60 | 698 |
| Trevor | 105 | 201 | 168 | 115 | 155 | 142 | 886 |
| Calder | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| Jonah | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| Irving | 179 | 201 | 203 | 129 | 114 | 128 | 954 |
| Chris | 107 | 221 | 229 | 146 | 146 | 148 | 997 |

_Read left→right per row: rising numbers = more completed tasks over time, falling = fewer. Compare the first few months to the last few._

## Run diagnostics
- Completed tasks finished by people outside this group (not counted): 7352
- Finished tasks with no `finished_by` recorded (not counted): 3583
- Tasks with finished_at outside the window (skipped): 0
- Tasks with no parseable completion date (skipped): 0
- CSV rows written: 402
- Caveat: properties were listed without a status filter. If Breezeway's property endpoint defaults to active-only, completed tasks at since-deactivated properties could be missing — treat this as a known blind spot, not a verified-complete scan.

## Failures
- None. 🎉

## Warnings
- ⚠️ tasks ref=799394 (page 1): timeout (attempt 1/5); waiting 2s

## Full run log
```
12:05:56 [info] Productivity Past 365 Days — window 2024-06-25,2025-06-25
12:06:05 [info] Authenticated with Breezeway (token cached for this run).
12:06:05 [info] Company list endpoint not usable (status 404); assuming single-company key.
12:06:06 [info] Loaded 57 Breezeway users for name resolution.
12:06:06 [info] Resolving names → Breezeway user IDs:
12:06:06 [info]    Andy       → id 250612  (Andy Rosman)
12:06:06 [info]    Trevor     → id 250618  (Trevor Bales)
12:06:06 [info]    Calder     → id 373683  (Calder McCarron)
12:06:06 [info]    Jonah      → id 365226  (Jonah Buchanan-Caldwell)
12:06:06 [info]    Irving     → id 250622  (Irving Pantoja)
12:06:06 [info]    Chris      → id 266840  (Chris Marin)
12:06:18 [info] Scanning 517 properties for ALL completed tasks 2024-06-25,2025-06-25 (attributing by finished_by to our 6 people)…
12:07:02 [info]   …25/517 properties scanned (0 tasks counted so far)
12:07:49 [info]   …50/517 properties scanned (0 tasks counted so far)
12:08:37 [info]   …75/517 properties scanned (0 tasks counted so far)
12:09:23 [info] First completed task seen → completion field = 'finished_at', name fields = [('name', 'Departure Clean')], finished_by = 270986
12:09:34 [info]   …100/517 properties scanned (0 tasks counted so far)
12:10:14 [info]   …125/517 properties scanned (2 tasks counted so far)
12:12:03 [info]   …150/517 properties scanned (84 tasks counted so far)
12:13:41 [info]   …175/517 properties scanned (289 tasks counted so far)
12:14:25 [WARN] tasks ref=799394 (page 1): timeout (attempt 1/5); waiting 2s
12:15:54 [info]   …200/517 properties scanned (541 tasks counted so far)
12:16:36 [info]   …225/517 properties scanned (800 tasks counted so far)
12:17:12 [info]   …250/517 properties scanned (1004 tasks counted so far)
12:17:52 [info]   …275/517 properties scanned (1270 tasks counted so far)
12:18:35 [info]   …300/517 properties scanned (1495 tasks counted so far)
12:19:28 [info]   …325/517 properties scanned (1701 tasks counted so far)
12:20:07 [info]   …350/517 properties scanned (1957 tasks counted so far)
12:20:58 [info]   …375/517 properties scanned (2243 tasks counted so far)
12:22:04 [info]   …400/517 properties scanned (2509 tasks counted so far)
12:23:27 [info]   …425/517 properties scanned (2776 tasks counted so far)
12:24:07 [info]   …450/517 properties scanned (3040 tasks counted so far)
12:24:36 [info]   …475/517 properties scanned (3230 tasks counted so far)
12:25:02 [info]   …500/517 properties scanned (3399 tasks counted so far)
```
