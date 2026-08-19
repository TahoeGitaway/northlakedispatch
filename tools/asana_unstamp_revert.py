"""
tools/asana_unstamp_revert.py — undo a bad My Bot stamping run.

My Bot's stamp_house_and_date used to sort candidates OLDEST-first and cap at 50,
so an "apply" run could rename 50 PAST-dated tasks and reset their due dates
backwards, dumping them all into Overdue. This restores them.

The old values are read from Asana's own per-task story history (`name_changed`
and `due_date_changed` stories), not guessed from the new title — so the restore
is exact.

Usage (from the repo root, with ASANA_TOKEN set in .env):

    .venv/bin/python tools/asana_unstamp_revert.py --inspect        # dump raw stories, no writes
    .venv/bin/python tools/asana_unstamp_revert.py                  # DRY RUN: show the revert plan
    .venv/bin/python tools/asana_unstamp_revert.py --hours 6        # widen the incident window
    .venv/bin/python tools/asana_unstamp_revert.py --apply          # actually restore

Always writes a JSON snapshot of current state to reports/ before any change.
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(REPO, ".env"))

BASE = "https://app.asana.com/api/1.0"
STAMP_RE = re.compile(r"^\s*\d{1,2}/\d{1,2}\s+(?:Arrival|Dept)\s+-\s+.+?\s+-\s+", re.I)


def _headers():
    tok = (os.environ.get("ASANA_TOKEN") or "").strip()
    if not tok:
        sys.exit("ASANA_TOKEN is empty. Put a Personal Access Token in .env "
                 "(Asana → My Settings → Apps → Manage Developer Apps → + New Access Token).")
    return {"Authorization": f"Bearer {tok}", "Accept": "application/json",
            "Content-Type": "application/json"}


def get(path, **params):
    r = requests.get(BASE + path, headers=_headers(), params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def put(path, data):
    r = requests.put(BASE + path, headers=_headers(), json={"data": data}, timeout=30)
    if not r.ok:
        raise RuntimeError(f"{r.status_code}: {r.text[:300]}")
    return r.json()


def fetch_my_tasks():
    ws = get("/workspaces")["data"][0]["gid"]
    utl = get("/users/me/user_task_list", workspace=ws)["data"]["gid"]
    out, offset = [], None
    while True:
        p = {"opt_fields": "name,gid,due_on,completed,modified_at", "limit": 100}
        if offset:
            p["offset"] = offset
        body = get(f"/user_task_lists/{utl}/tasks", **p)
        out += body.get("data", [])
        offset = (body.get("next_page") or {}).get("offset")
        if not offset:
            return out


def stories(gid):
    return get(f"/tasks/{gid}/stories",
               opt_fields="resource_subtype,created_at,created_by.name,type,text,"
                          "old_name,new_name,old_dates,new_dates").get("data", [])


def _parse_ts(s):
    try:
        return datetime.fromisoformat((s or "").replace("Z", "+00:00"))
    except ValueError:
        return None


def _due_from_dates(obj):
    """Pull due_on out of a story's old_dates/new_dates block."""
    if isinstance(obj, dict):
        return obj.get("due_on")
    return None


def build_revert(gid, since):
    """Return {'old_name':…, 'old_due':…, 'evidence':[…]} from stories after `since`.

    Uses the EARLIEST change in the window, so the value restored is what the task
    looked like before the bad run — not an intermediate state.
    """
    ev, old_name, old_due, saw_due = [], None, None, False
    for s in stories(gid):
        ts = _parse_ts(s.get("created_at"))
        if not ts or ts < since:
            continue
        sub = s.get("resource_subtype")
        if sub == "name_changed":
            ev.append(s)
            if old_name is None:                 # earliest wins
                old_name = s.get("old_name")
        elif sub == "due_date_changed":
            ev.append(s)
            if not saw_due:
                old_due = _due_from_dates(s.get("old_dates"))
                saw_due = True                   # None is a real value (no due date)
    return {"old_name": old_name, "old_due": old_due, "saw_due": saw_due, "evidence": ev}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=float, default=3.0,
                    help="how far back the bad run happened (default 3)")
    ap.add_argument("--apply", action="store_true", help="perform the restore")
    ap.add_argument("--inspect", action="store_true",
                    help="dump raw story JSON for the first few affected tasks and exit")
    args = ap.parse_args()

    since = datetime.now(timezone.utc) - timedelta(hours=args.hours)
    print(f"Incident window: since {since.isoformat()}  (last {args.hours}h)\n")

    tasks = fetch_my_tasks()
    touched = [t for t in tasks
               if (_parse_ts(t.get("modified_at")) or since) >= since
               and STAMP_RE.match(t.get("name") or "")]
    print(f"{len(tasks)} tasks in My Tasks; {len(touched)} stamped + modified in the window\n")
    if not touched:
        print("Nothing matches — try a wider --hours.")
        return

    if args.inspect:
        for t in touched[:3]:
            print(f"=== {t['gid']}  {t['name']}")
            for s in build_revert(t["gid"], since)["evidence"]:
                print(json.dumps(s, indent=2)[:900], "\n")
        return

    plan, unclear = [], []
    for t in touched:
        r = build_revert(t["gid"], since)
        if r["old_name"] is None and not r["saw_due"]:
            unclear.append((t, "no name/due change recorded in the window"))
            continue
        plan.append((t, r))

    snap = os.path.join(REPO, "reports",
                        f"asana_unstamp_snapshot_{datetime.now():%Y%m%d_%H%M%S}.json")
    os.makedirs(os.path.dirname(snap), exist_ok=True)
    with open(snap, "w") as f:
        json.dump([{"gid": t["gid"], "current_name": t.get("name"),
                    "current_due": t.get("due_on"),
                    "restore_name": r["old_name"], "restore_due": r["old_due"]}
                   for t, r in plan], f, indent=2)
    print(f"Snapshot written: {snap}\n")

    for i, (t, r) in enumerate(plan, 1):
        print(f"{i}. [{t['gid']}]")
        print(f"    name: {t.get('name')}")
        print(f"       -> {r['old_name'] if r['old_name'] is not None else '(unchanged)'}")
        print(f"    due:  {t.get('due_on')} -> "
              f"{r['old_due'] if r['saw_due'] else '(unchanged)'}")
    if unclear:
        print(f"\n{len(unclear)} task(s) with no recorded change — left alone:")
        for t, why in unclear:
            print(f"  - [{t['gid']}] {t.get('name')}: {why}")

    if not args.apply:
        print(f"\nDRY RUN — nothing changed. {len(plan)} task(s) would be restored.")
        print("Re-run with --apply to perform the restore.")
        return

    ok = fail = 0
    for t, r in plan:
        data = {}
        if r["old_name"] is not None:
            data["name"] = r["old_name"]
        if r["saw_due"]:
            data["due_on"] = r["old_due"]        # None clears the due date
        if not data:
            continue
        try:
            put(f"/tasks/{t['gid']}", data)
            ok += 1
        except Exception as e:
            fail += 1
            print(f"  FAILED [{t['gid']}]: {e}")
    print(f"\nRestored {ok} task(s); {fail} failed. Snapshot: {snap}")


if __name__ == "__main__":
    main()
