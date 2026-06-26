# Productivity Past 365 Days

**Status: ✅ COMPLETE** — every property was scanned without error.

- Date range covered: **2025-06-26 → 2026-06-26** (365 days)
- Run timestamp: 2026-06-26T11:10:03-07:00
- Properties in account: 517
- Properties scanned OK: 517
- Completed tasks counted: 10300
- Per-person tallies (sum of CSV counts): 10300
- Excluded "Disarm Bear Fence" tasks: 223

## How to read these numbers
- **Counted by who completed it (`finished_by`).** Each finished task is credited to the single person who marked it complete — NOT who it was assigned to. So each task counts exactly once, for the person who actually did it.
- **Completion field used:** `finished_at` — the day bucket is the date portion of that timestamp **as Breezeway returns it**. If Breezeway returns UTC, a task finished late at night local time may land on the next day. Note this when comparing day-to-day.
- **Exclusion:** tasks named "disarm bear fence" (case-insensitive) are filtered out, matched on field(s): {'name': 223}. They are NOT removed from Breezeway.
- **De-dup:** each task id is counted once even if it surfaces under two property queries.

## Per-person yearly totals

| Person | User ID | Completed tasks (365d) | Active days |
|---|---|---|---|
| Andy | 250612 | 1573 | 207 |
| Trevor | 250618 | 2020 | 217 |
| Calder | 373683 | 1167 | 132 |
| Jonah | 365226 | 1178 | 155 |
| Irving | 250622 | 2035 | 228 |
| Chris | 266840 | 2327 | 233 |

## Monthly totals per person (for trend)

| Person | 2025-06 | 2025-07 | 2025-08 | 2025-09 | 2025-10 | 2025-11 | 2025-12 | 2026-01 | 2026-02 | 2026-03 | 2026-04 | 2026-05 | 2026-06 | Total |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Andy | 0 | 158 | 67 | 63 | 82 | 128 | 112 | 199 | 167 | 186 | 130 | 122 | 159 | 1573 |
| Trevor | 62 | 218 | 224 | 116 | 144 | 164 | 144 | 125 | 205 | 129 | 167 | 124 | 198 | 2020 |
| Calder | 0 | 0 | 0 | 0 | 0 | 0 | 139 | 200 | 201 | 175 | 122 | 166 | 164 | 1167 |
| Jonah | 0 | 0 | 0 | 0 | 0 | 86 | 118 | 197 | 190 | 178 | 123 | 135 | 151 | 1178 |
| Irving | 40 | 215 | 168 | 136 | 112 | 121 | 152 | 213 | 211 | 179 | 141 | 164 | 183 | 2035 |
| Chris | 58 | 255 | 175 | 189 | 162 | 152 | 217 | 224 | 156 | 183 | 161 | 187 | 208 | 2327 |

_Read left→right per row: rising numbers = more completed tasks over time, falling = fewer. Compare the first few months to the last few._

## Run diagnostics
- Completed tasks finished by people outside this group (not counted): 17049
- Finished tasks with no `finished_by` recorded (not counted): 5157
- Tasks with finished_at outside the window (skipped): 0
- Tasks with no parseable completion date (skipped): 0
- CSV rows written: 1172
- Caveat: properties were listed without a status filter. If Breezeway's property endpoint defaults to active-only, completed tasks at since-deactivated properties could be missing — treat this as a known blind spot, not a verified-complete scan.

## Failures
- None. 🎉

## Warnings
- None.

## Full run log
```
10:45:49 [info] Productivity Past 365 Days — window 2025-06-26,2026-06-26
10:45:51 [info] Authenticated with Breezeway (token cached for this run).
10:45:51 [info] Company list endpoint not usable (status 404); assuming single-company key.
10:45:52 [info] Loaded 57 Breezeway users for name resolution.
10:45:52 [info] Resolving names → Breezeway user IDs:
10:45:52 [info]    Andy       → id 250612  (Andy Rosman)
10:45:52 [info]    Trevor     → id 250618  (Trevor Bales)
10:45:52 [info]    Calder     → id 373683  (Calder McCarron)
10:45:52 [info]    Jonah      → id 365226  (Jonah Buchanan-Caldwell)
10:45:52 [info]    Irving     → id 250622  (Irving Pantoja)
10:45:52 [info]    Chris      → id 266840  (Chris Marin)
10:45:59 [info] Scanning 517 properties for ALL completed tasks 2025-06-26,2026-06-26 (attributing by finished_by to our 6 people)…
10:46:04 [info] First completed task seen → completion field = 'finished_at', name fields = [('name', 'Linens - Owner Request *Delivery on 6/29')], finished_by = 410546
10:46:21 [info]   …25/517 properties scanned (1 tasks counted so far)
10:46:55 [info]   …50/517 properties scanned (63 tasks counted so far)
10:47:46 [info]   …75/517 properties scanned (401 tasks counted so far)
10:48:33 [info]   …100/517 properties scanned (685 tasks counted so far)
10:49:20 [info]   …125/517 properties scanned (1000 tasks counted so far)
10:50:00 [info]   …150/517 properties scanned (1514 tasks counted so far)
10:50:46 [info]   …175/517 properties scanned (1892 tasks counted so far)
10:52:04 [info]   …200/517 properties scanned (2530 tasks counted so far)
10:53:34 [info]   …225/517 properties scanned (3264 tasks counted so far)
10:55:03 [info]   …250/517 properties scanned (3853 tasks counted so far)
10:56:28 [info]   …275/517 properties scanned (4610 tasks counted so far)
10:57:29 [info]   …300/517 properties scanned (5125 tasks counted so far)
10:58:31 [info]   …325/517 properties scanned (5707 tasks counted so far)
10:59:47 [info]   …350/517 properties scanned (6364 tasks counted so far)
11:01:29 [info]   …375/517 properties scanned (6988 tasks counted so far)
11:02:47 [info]   …400/517 properties scanned (7487 tasks counted so far)
11:04:53 [info]   …425/517 properties scanned (8255 tasks counted so far)
11:06:38 [info]   …450/517 properties scanned (8941 tasks counted so far)
11:07:47 [info]   …475/517 properties scanned (9372 tasks counted so far)
11:08:58 [info]   …500/517 properties scanned (9804 tasks counted so far)
```
