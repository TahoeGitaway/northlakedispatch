# Productivity Past 365 Days

**Status: ✅ COMPLETE** — every property was scanned without error.

- Date range covered: **2025-06-26 → 2026-06-26** (365 days)
- Run timestamp: 2026-06-26T13:53:07-07:00
- Properties in account: 517
- Properties scanned OK: 517
- Completed tasks counted: 11800
- Per-person tallies (sum of CSV counts): 11800
- Excluded "Disarm Bear Fence" tasks: 227

## How to read these numbers
- **Counted by who finished it (`finished_by`).** Each finished task is credited to the single person who marked it complete — NOT who it was assigned to. So each task counts exactly once, for the person who finished it.
- **Completion field used:** `finished_at` — the day bucket is the date portion of that timestamp **as Breezeway returns it**. If Breezeway returns UTC, a task finished late at night local time may land on the next day. Note this when comparing day-to-day.
- **Exclusion:** tasks named "disarm bear fence" (case-insensitive) are filtered out, matched on field(s): {'name': 227}. They are NOT removed from Breezeway.
- **De-dup:** each task id is counted once even if it surfaces under two property queries.

## Per-person yearly totals

| Person | User ID | Completed tasks (365d) | Active days |
|---|---|---|---|
| Andy | 250612 | 1575 | 207 |
| Trevor | 250618 | 2020 | 217 |
| Calder | 373683 | 1172 | 132 |
| Jonah | 365226 | 1182 | 155 |
| Irving | 250622 | 2039 | 228 |
| Chris | 266840 | 2327 | 233 |
| Julie | 354488 | 1485 | 159 |

## Monthly totals per person (for trend)

| Person | 2025-06 | 2025-07 | 2025-08 | 2025-09 | 2025-10 | 2025-11 | 2025-12 | 2026-01 | 2026-02 | 2026-03 | 2026-04 | 2026-05 | 2026-06 | Total |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Andy | 0 | 158 | 67 | 63 | 82 | 128 | 112 | 199 | 167 | 186 | 130 | 122 | 161 | 1575 |
| Trevor | 62 | 218 | 224 | 116 | 144 | 164 | 144 | 125 | 205 | 129 | 167 | 124 | 198 | 2020 |
| Calder | 0 | 0 | 0 | 0 | 0 | 0 | 139 | 200 | 201 | 175 | 122 | 166 | 169 | 1172 |
| Jonah | 0 | 0 | 0 | 0 | 0 | 86 | 118 | 197 | 190 | 178 | 123 | 135 | 155 | 1182 |
| Irving | 40 | 215 | 168 | 136 | 112 | 121 | 152 | 213 | 211 | 179 | 141 | 164 | 187 | 2039 |
| Chris | 58 | 255 | 175 | 189 | 162 | 152 | 217 | 224 | 156 | 183 | 161 | 187 | 208 | 2327 |
| Julie | 0 | 0 | 0 | 0 | 60 | 154 | 154 | 213 | 185 | 195 | 157 | 191 | 176 | 1485 |

_Read left→right per row: rising numbers = more completed tasks over time, falling = fewer. Compare the first few months to the last few._

## Run diagnostics
- Completed tasks finished by people outside this group (not counted): 15577
- Finished tasks with no `finished_by` recorded (not counted): 5162
- Tasks with finished_at outside the window (skipped): 0
- Tasks with no parseable completion date (skipped): 0
- CSV rows written: 1331
- Caveat: properties were listed without a status filter. If Breezeway's property endpoint defaults to active-only, completed tasks at since-deactivated properties could be missing — treat this as a known blind spot, not a verified-complete scan.

## Failures
- None. 🎉

## Warnings
- None.

## Full run log
```
13:34:54 [info] Productivity Past 365 Days — window 2025-06-26,2026-06-26
13:34:56 [info] Authenticated with Breezeway (token cached for this run).
13:34:56 [info] Company list endpoint not usable (status 404); assuming single-company key.
13:34:56 [info] Loaded 57 Breezeway users for name resolution.
13:34:56 [info] Resolving names → Breezeway user IDs:
13:34:56 [info]    Andy       → id 250612  (Andy Rosman)
13:34:56 [info]    Trevor     → id 250618  (Trevor Bales)
13:34:56 [info]    Calder     → id 373683  (Calder McCarron)
13:34:56 [info]    Jonah      → id 365226  (Jonah Buchanan-Caldwell)
13:34:56 [info]    Irving     → id 250622  (Irving Pantoja)
13:34:56 [info]    Chris      → id 266840  (Chris Marin)
13:34:56 [info]    Julie      → id 354488  (Julie Rohrback)
13:35:02 [info] Scanning 517 properties for ALL completed tasks 2025-06-26,2026-06-26 (attributing by finished_by to our 7 people)…
13:35:05 [info] First completed task seen → completion field = 'finished_at', name fields = [('name', 'Linens - Owner Request *Delivery on 6/29')], finished_by = 410546
13:35:22 [info]   …25/517 properties scanned (1 tasks counted so far)
13:35:54 [info]   …50/517 properties scanned (68 tasks counted so far)
13:36:33 [info]   …75/517 properties scanned (433 tasks counted so far)
13:37:19 [info]   …100/517 properties scanned (789 tasks counted so far)
13:37:53 [info]   …125/517 properties scanned (1119 tasks counted so far)
13:39:03 [info]   …150/517 properties scanned (1698 tasks counted so far)
13:39:55 [info]   …175/517 properties scanned (2113 tasks counted so far)
13:41:02 [info]   …200/517 properties scanned (2876 tasks counted so far)
13:42:10 [info]   …225/517 properties scanned (3684 tasks counted so far)
13:42:58 [info]   …250/517 properties scanned (4348 tasks counted so far)
13:44:02 [info]   …275/517 properties scanned (5229 tasks counted so far)
13:44:44 [info]   …300/517 properties scanned (5841 tasks counted so far)
13:45:24 [info]   …325/517 properties scanned (6484 tasks counted so far)
13:46:37 [info]   …350/517 properties scanned (7211 tasks counted so far)
13:47:31 [info]   …375/517 properties scanned (7969 tasks counted so far)
13:48:18 [info]   …400/517 properties scanned (8558 tasks counted so far)
13:49:22 [info]   …425/517 properties scanned (9449 tasks counted so far)
13:50:19 [info]   …450/517 properties scanned (10242 tasks counted so far)
13:51:26 [info]   …475/517 properties scanned (10752 tasks counted so far)
13:52:12 [info]   …500/517 properties scanned (11263 tasks counted so far)
```
