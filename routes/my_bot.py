"""
routes/my_bot.py — Personal AI assistant (My Bot).

Handles all My Bot routes and Asana integration independently of the Ops Bot
(routes/admin.py). Changes here cannot break the Ops Bot.
"""

import json
import os
from datetime import datetime

import requests
from flask import (Blueprint, render_template, request, jsonify, Response, stream_with_context)
from flask_login import login_required

from db import get_db, get_cursor
from routes.auth import admin_required

my_bot_bp = Blueprint("my_bot", __name__)


def _my_bot_required(f):
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        return admin_required(f)(*args, **kwargs)
    return decorated


# ── History trimming ──────────────────────────────────────────────

def _safe_trim(messages, limit):
    """Trim history to `limit` messages without orphaning tool_result blocks."""
    if len(messages) <= limit:
        return list(messages)
    trimmed = list(messages[-limit:])
    while trimmed:
        first = trimmed[0]
        content = first.get("content", "")
        role = first.get("role", "")
        is_tool_result_msg = (
            role == "user"
            and isinstance(content, list)
            and all(isinstance(b, dict) and b.get("type") == "tool_result" for b in content)
        )
        if is_tool_result_msg or role == "assistant":
            trimmed.pop(0)
        else:
            break
    return trimmed


# ── Asana helpers ─────────────────────────────────────────────────

def _asana_request(method, path, payload=None):
    token = os.environ.get("ASANA_TOKEN", "")
    if not token:
        return None, "ASANA_TOKEN not configured."
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    url = f"https://app.asana.com/api/1.0{path}"
    try:
        if method == "GET":
            resp = requests.get(url, headers=headers, params=payload, timeout=15)
        elif method == "POST":
            resp = requests.post(url, headers=headers, json=payload or {}, timeout=15)
        elif method == "PUT":
            resp = requests.put(url, headers=headers, json=payload or {}, timeout=15)
        elif method == "DELETE":
            resp = requests.delete(url, headers=headers, timeout=15)
        else:
            return None, f"Unknown method {method}"
        if not resp.ok:
            return None, f"Asana API {resp.status_code}: {resp.text[:200]}"
        return resp.json().get("data"), None
    except Exception as e:
        return None, str(e)


def _asana_fetch_all(path, params):
    """GET a paginated Asana collection and return every item across all pages."""
    token = os.environ.get("ASANA_TOKEN", "")
    if not token:
        return None, "ASANA_TOKEN not configured."
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    url = f"https://app.asana.com/api/1.0{path}"
    all_items = []
    page_params = dict(params)
    for _ in range(30):  # safety cap at 3 000 items
        try:
            resp = requests.get(url, headers=headers, params=page_params, timeout=20)
            if not resp.ok:
                return None, f"Asana API {resp.status_code}: {resp.text[:200]}"
            body = resp.json()
        except Exception as e:
            return None, str(e)
        all_items.extend(body.get("data", []))
        nxt = body.get("next_page")
        if not nxt or not nxt.get("offset"):
            break
        page_params = dict(params)
        page_params["offset"] = nxt["offset"]
    return all_items, None


_asana_workspace_cache = {"gid": None}


def _get_asana_workspace():
    if _asana_workspace_cache["gid"]:
        return _asana_workspace_cache["gid"]
    data, err = _asana_request("GET", "/workspaces")
    if err or not data:
        return None
    gid = data[0].get("gid") if isinstance(data, list) else data.get("gid")
    _asana_workspace_cache["gid"] = gid
    return gid


# ── Asana notification polling ────────────────────────────────────

def poll_asana_notifications():
    """Check Asana for new comments on tasks assigned to me since last poll.
    Called by the scheduler every 30 minutes.
    """
    token = os.environ.get("ASANA_TOKEN", "")
    if not token:
        return

    now_str = datetime.utcnow().isoformat()

    conn = get_db(); cur = get_cursor(conn)
    cur.execute("SELECT value FROM asana_poll_state WHERE key='last_checked'")
    row = cur.fetchone()
    last_checked = row["value"] if row else None
    cur.close(); conn.rollback(); conn.close()

    ws = _get_asana_workspace()
    if not ws:
        return

    tasks_data, err = _asana_request("GET", "/tasks", {
        "assignee":   "me",
        "workspace":  ws,
        "completed":  "false",
        "opt_fields": "gid,name,parent.name",
    })
    if err or not tasks_data:
        return
    tasks = tasks_data if isinstance(tasks_data, list) else []

    # Ensure parent_name column exists (safe to run every time)
    try:
        conn0 = get_db(); cur0 = get_cursor(conn0)
        cur0.execute("ALTER TABLE asana_notifications ADD COLUMN IF NOT EXISTS parent_name TEXT")
        conn0.commit(); cur0.close(); conn0.close()
    except Exception:
        pass

    new_notifications = []
    for task in tasks:
        tgid   = task.get("gid")
        tname  = task.get("name", "Unnamed task")
        parent = task.get("parent") or {}
        pname  = parent.get("name", "") if isinstance(parent, dict) else ""
        if not tgid:
            continue
        stories_data, serr = _asana_request("GET", f"/tasks/{tgid}/stories", {
            "opt_fields": "gid,type,created_at,created_by.name,text",
        })
        if serr or not stories_data:
            continue
        for story in (stories_data if isinstance(stories_data, list) else []):
            if story.get("type") != "comment":
                continue
            sgid         = story.get("gid", "")
            created_at   = story.get("created_at", "")
            commenter    = (story.get("created_by") or {}).get("name", "Someone")
            comment_text = story.get("text", "")
            if last_checked and created_at and created_at <= last_checked:
                continue
            item_key = f"{tgid}::{sgid}"
            new_notifications.append((item_key, tgid, tname, pname, sgid, commenter, comment_text, created_at))

    if new_notifications:
        conn2 = get_db(); cur2 = get_cursor(conn2)
        for (key, tgid, tname, pname, sgid, commenter, text, cat) in new_notifications:
            cur2.execute(
                """INSERT INTO asana_notifications
                   (item_key, task_gid, task_name, parent_name, story_gid, commenter, comment_text, asana_created_at, created_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (item_key) DO NOTHING""",
                (key, tgid, tname, pname, sgid, commenter, text, cat, now_str),
            )
        conn2.commit(); cur2.close(); conn2.close()

    conn3 = get_db(); cur3 = get_cursor(conn3)
    cur3.execute(
        "INSERT INTO asana_poll_state (key, value) VALUES ('last_checked', %s) "
        "ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value",
        (now_str,),
    )
    conn3.commit(); cur3.close(); conn3.close()


# ── Asana notification API routes ─────────────────────────────────

@my_bot_bp.route("/api/asana-notifications")
@login_required
@_my_bot_required
def api_asana_notifications():
    conn = get_db(); cur = get_cursor(conn)
    cur.execute(
        "SELECT item_key, task_gid, task_name, commenter, comment_text, asana_created_at "
        "FROM asana_notifications WHERE dismissed_at IS NULL AND replied_at IS NULL "
        "ORDER BY asana_created_at DESC"
    )
    notes = [dict(r) for r in cur.fetchall()]
    cur.close(); conn.rollback(); conn.close()
    return jsonify({"notifications": notes})


@my_bot_bp.route("/api/asana-notification/dismiss", methods=["POST"])
@login_required
@_my_bot_required
def api_asana_notification_dismiss():
    key = (request.get_json(force=True) or {}).get("key", "").strip()
    if not key:
        return jsonify({"error": "key required"}), 400
    conn = get_db(); cur = get_cursor(conn)
    cur.execute(
        "UPDATE asana_notifications SET dismissed_at=%s WHERE item_key=%s",
        (datetime.utcnow().isoformat(), key),
    )
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})


# ── My Bot page + chat ────────────────────────────────────────────

@my_bot_bp.route("/admin/my-bot")
@login_required
@_my_bot_required
def my_bot_page():
    return render_template("admin_my_bot.html")


@my_bot_bp.route("/admin/my-bot/chat", methods=["POST"])
@login_required
@_my_bot_required
def my_bot_chat():
    import anthropic as _anthropic
    from routes.admin import _execute_fetch_tasks_multi_standalone

    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        return jsonify({"error": "ANTHROPIC_API_KEY not configured."}), 500

    data     = request.get_json(force=True) or {}
    messages = data.get("messages", [])

    tools = [
        {
            "name": "get_my_notifications",
            "description": (
                "Read Asana comment notifications from the database — comments from others "
                "on tasks assigned to you, polled every 30 minutes. Returns unread notifications "
                "by default. Filter by property_name to see only notifications about a specific house. "
                "Each result includes the task GID so you can immediately call get_task_comments, "
                "update_asana_task, or draft_asana_comment on it."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "property_name": {
                        "type": "string",
                        "description": "Optional: only return notifications where task name contains this property name",
                    },
                    "include_dismissed": {
                        "type": "boolean",
                        "description": "If true, include dismissed notifications too (default false = unread only)",
                    },
                },
            },
        },
        {
            "name": "get_my_asana_tasks",
            "description": (
                "Fetch Asana tasks assigned to the current user. "
                "Returns task name, GID, due date, project, completion status, and notes. "
                "Use filter='incomplete' (default) to see open tasks, 'complete' for done tasks, "
                "or 'all' for everything. Optionally filter by project name."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "filter":  {"type": "string", "description": "'incomplete', 'complete', or 'all'"},
                    "project": {"type": "string", "description": "Optional project name to filter by"},
                },
            },
        },
        {
            "name": "update_asana_task",
            "description": (
                "Update an Asana task — rename it, mark complete/incomplete, change due date, or update notes. "
                "Requires task_gid from get_my_asana_tasks."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "task_gid":  {"type": "string", "description": "Asana task GID"},
                    "task_name": {"type": "string", "description": "Current task name for context"},
                    "new_name":  {"type": "string", "description": "New title/name for the task"},
                    "completed": {"type": "boolean", "description": "Set completion status"},
                    "due_on":    {"type": "string",  "description": "New due date YYYY-MM-DD"},
                    "notes":     {"type": "string",  "description": "Replace task notes/description"},
                },
                "required": ["task_gid", "task_name"],
            },
        },
        {
            "name": "get_task_comments",
            "description": (
                "Fetch comments on a SINGLE Asana task. Use get_comments_batch for 2+ tasks."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "task_gid":  {"type": "string", "description": "Asana task GID"},
                    "task_name": {"type": "string", "description": "Task name for context"},
                },
                "required": ["task_gid", "task_name"],
            },
        },
        {
            "name": "get_comments_batch",
            "description": (
                "Fetch comments on MULTIPLE Asana tasks simultaneously (in parallel). "
                "ALWAYS use this instead of calling get_task_comments repeatedly when checking "
                "comments across more than one task. Returns only tasks that have comments from "
                "others — skips tasks with no activity. Max ~20 tasks per call for reliability."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "tasks": {
                        "type": "array",
                        "description": "Tasks to check for comments",
                        "items": {
                            "type": "object",
                            "properties": {
                                "task_gid":  {"type": "string"},
                                "task_name": {"type": "string"},
                            },
                            "required": ["task_gid", "task_name"],
                        },
                    },
                },
                "required": ["tasks"],
            },
        },
        {
            "name": "draft_asana_comment",
            "description": (
                "Draft a comment to post on an Asana task. "
                "Returns the suggested comment text for user review and editing before posting. "
                "Always use this instead of posting directly — the user must confirm the text first."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "task_gid":       {"type": "string", "description": "Asana task GID"},
                    "task_name":      {"type": "string", "description": "Task name for context"},
                    "suggested_text": {"type": "string", "description": "The suggested comment text"},
                },
                "required": ["task_gid", "task_name", "suggested_text"],
            },
        },
        {
            "name": "batch_update_asana_tasks",
            "description": (
                "Update multiple Asana tasks in parallel in a single call. "
                "Use this whenever you need to update more than one task — it runs all updates "
                "simultaneously server-side and returns per-task success/failure/timeout results. "
                "NEVER call update_asana_task in a loop; use this instead."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "updates": {
                        "type": "array",
                        "description": "List of task updates to apply in parallel",
                        "items": {
                            "type": "object",
                            "properties": {
                                "task_gid":  {"type": "string", "description": "Asana task GID"},
                                "task_name": {"type": "string", "description": "Task name for context"},
                                "new_name":  {"type": "string", "description": "New title for the task"},
                                "due_on":    {"type": "string", "description": "New due date YYYY-MM-DD"},
                                "completed": {"type": "boolean"},
                                "notes":     {"type": "string"},
                            },
                            "required": ["task_gid", "task_name"],
                        },
                    },
                },
                "required": ["updates"],
            },
        },
        {
            "name": "delete_asana_task",
            "description": (
                "Permanently delete a single Asana task. This cannot be undone. "
                "Always CONFIRM_ACTION before calling this."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "task_gid":  {"type": "string", "description": "Asana task GID"},
                    "task_name": {"type": "string", "description": "Task name for confirmation context"},
                },
                "required": ["task_gid", "task_name"],
            },
        },
        {
            "name": "batch_delete_asana_tasks",
            "description": (
                "Permanently delete multiple Asana tasks in parallel. Cannot be undone. "
                "Use for 2+ deletions. Always CONFIRM_ACTION first."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "tasks": {
                        "type": "array",
                        "description": "Tasks to delete",
                        "items": {
                            "type": "object",
                            "properties": {
                                "task_gid":  {"type": "string"},
                                "task_name": {"type": "string"},
                            },
                            "required": ["task_gid", "task_name"],
                        },
                    },
                },
                "required": ["tasks"],
            },
        },
        {
            "name": "fetch_breezeway_tasks",
            "description": (
                "Fetch Breezeway task data (cleaning jobs, inspections, maintenance) "
                "for one or more properties over a date range. "
                "Use for questions like 'what tasks are scheduled at X this week' or "
                "'what cleaning jobs do I have coming up'. "
                "Pass a list of property names to fetch multiple at once (runs in parallel). "
                "Maximum date range 30 days."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "start_date":     {"type": "string", "description": "Start date YYYY-MM-DD"},
                    "end_date":       {"type": "string", "description": "End date YYYY-MM-DD"},
                    "property_names": {"type": "array", "items": {"type": "string"},
                                       "description": "One or more property names"},
                    "status":         {"type": "string", "description": "Optional: housekeeping, maintenance, inspection, complete, pending"},
                },
                "required": ["start_date", "end_date", "property_names"],
            },
        },
    ]

    def _exec_get_notifications(property_name=None, include_dismissed=False):
        conn = get_db(); cur = get_cursor(conn)
        pf = (property_name or "").strip().lower()
        if pf:
            # Match against parent_name first (reliable), fall back to task_name
            if include_dismissed:
                cur.execute(
                    "SELECT task_gid, task_name, parent_name, commenter, comment_text, asana_created_at, dismissed_at "
                    "FROM asana_notifications "
                    "WHERE (LOWER(parent_name) LIKE %s OR LOWER(task_name) LIKE %s) "
                    "ORDER BY asana_created_at DESC LIMIT 50",
                    (f"%{pf}%", f"%{pf}%"),
                )
            else:
                cur.execute(
                    "SELECT task_gid, task_name, parent_name, commenter, comment_text, asana_created_at, dismissed_at "
                    "FROM asana_notifications "
                    "WHERE dismissed_at IS NULL "
                    "AND (LOWER(parent_name) LIKE %s OR LOWER(task_name) LIKE %s) "
                    "ORDER BY asana_created_at DESC LIMIT 50",
                    (f"%{pf}%", f"%{pf}%"),
                )
        else:
            if include_dismissed:
                cur.execute(
                    "SELECT task_gid, task_name, parent_name, commenter, comment_text, asana_created_at, dismissed_at "
                    "FROM asana_notifications ORDER BY asana_created_at DESC LIMIT 50"
                )
            else:
                cur.execute(
                    "SELECT task_gid, task_name, parent_name, commenter, comment_text, asana_created_at, dismissed_at "
                    "FROM asana_notifications WHERE dismissed_at IS NULL "
                    "ORDER BY asana_created_at DESC LIMIT 50"
                )
        rows = cur.fetchall(); cur.close(); conn.rollback(); conn.close()
        if not rows:
            label = f" about '{property_name}'" if pf else ""
            return f"No {'unread ' if not include_dismissed else ''}notifications{label}."
        scope = "All" if include_dismissed else "Unread"
        prop_label = f" for '{property_name}'" if pf else ""
        lines = [f"{scope} notifications{prop_label} ({len(rows)}):"]
        for r in rows:
            prop  = r.get("parent_name") or ""
            task  = r["task_name"]
            label = f"{prop} — {task}" if prop and prop.lower() != task.lower() else task
            when  = (r.get("asana_created_at") or "")[:10]
            dismissed = " [dismissed]" if r.get("dismissed_at") else ""
            lines.append(
                f"\n📩 [{r['task_gid']}] {label}{dismissed}\n"
                f"  {r['commenter']} — {when}\n"
                f"  {r['comment_text']}"
            )
        return "\n".join(lines)

    def _exec_get_tasks(filter_val="incomplete", project_filter=None):
        ws = _get_asana_workspace()
        if not ws:
            return "Could not retrieve Asana workspace."

        utl_data, err = _asana_request("GET", "/users/me/user_task_list", {"workspace": ws})
        if err or not utl_data:
            return f"Could not get user task list: {err or 'no data'}"
        utl_gid = utl_data.get("gid") if isinstance(utl_data, dict) else None
        if not utl_gid:
            return "Could not find user task list GID."

        params = {
            "opt_fields": "name,gid,due_on,start_on,completed,notes,projects.name,parent.name,parent.gid,parent.due_on,parent.start_on",
            "limit":      100,
        }
        if filter_val == "incomplete":
            params["completed_since"] = "now"

        tasks, err = _asana_fetch_all(f"/user_task_lists/{utl_gid}/tasks", params)
        if err:
            return f"Error fetching tasks: {err}"
        if tasks is None:
            tasks = []

        if filter_val == "complete":
            tasks = [t for t in tasks if t.get("completed")]
        elif filter_val == "incomplete":
            tasks = [t for t in tasks if not t.get("completed")]

        if project_filter:
            pf = project_filter.lower()
            tasks = [t for t in tasks if any(
                pf in (p.get("name") or "").lower()
                for p in (t.get("projects") or [])
            )]

        tasks.sort(key=lambda t: (t.get("due_on") or "9999-99-99"))

        if not tasks:
            return "No tasks found."
        lines = [f"Found {len(tasks)} task(s):"]
        for t in tasks:
            projects = ", ".join(p.get("name", "") for p in (t.get("projects") or []))
            parent   = t.get("parent") or {}
            pname    = parent.get("name", "")
            pstart   = parent.get("start_on", "")
            pdue     = parent.get("due_on", "")
            status   = "✓ done" if t.get("completed") else "open"
            due      = t.get("due_on") or "no due date"
            line = f'• [{t["gid"]}] {t["name"]} | {status} | due {due} | project: {projects or "none"}'
            if pname:
                lease_dates = ""
                if pstart and pdue:
                    lease_dates = f" [{pstart} → {pdue}]"
                elif pdue:
                    lease_dates = f" [ends {pdue}]"
                line += f' | property: {pname}{lease_dates}'
            lines.append(line)
            if t.get("notes"):
                lines.append(f'  Notes: {t["notes"]}')
        return "\n".join(lines)

    def _exec_batch_update(updates):
        from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeout

        def _do_one(u):
            payload = {"data": {}}
            if u.get("new_name"):    payload["data"]["name"]      = u["new_name"]
            if u.get("due_on"):      payload["data"]["due_on"]    = u["due_on"]
            if u.get("completed") is not None:
                                     payload["data"]["completed"] = u["completed"]
            if u.get("notes") is not None:
                                     payload["data"]["notes"]     = u["notes"]
            label = u.get("new_name") or u.get("task_name", u.get("task_gid", "?"))
            if not payload["data"]:
                return label, "skipped — nothing to change"
            _, err = _asana_request("PUT", f"/tasks/{u['task_gid']}", payload)
            if err:
                return label, f"FAILED: {err}"
            return label, "✓"

        rows = []
        with ThreadPoolExecutor(max_workers=10) as ex:
            futures = {ex.submit(_do_one, u): u for u in updates}
            for fut in as_completed(futures, timeout=60):
                try:
                    label, status = fut.result(timeout=12)
                except FutureTimeout:
                    u = futures[fut]
                    label = u.get("task_name", u.get("task_gid", "?"))
                    status = "TIMED OUT — Asana did not respond in 12 s"
                except Exception as exc:
                    u = futures[fut]
                    label = u.get("task_name", u.get("task_gid", "?"))
                    status = f"ERROR: {exc}"
                rows.append((label, status))

        ok  = sum(1 for _, s in rows if s == "✓")
        bad = [(l, s) for l, s in rows if s != "✓" and not s.startswith("skipped")]
        lines = [f"Batch complete: {ok}/{len(updates)} succeeded."]
        if bad:
            lines.append(f"⚠️ {len(bad)} issue(s):")
            for l, s in bad:
                lines.append(f"  • {l}: {s}")
        lines.append("\nAll results:")
        for l, s in sorted(rows, key=lambda x: x[0]):
            lines.append(f"  {s}  {l}")
        return "\n".join(lines)

    def _exec_delete_task(task_gid, task_name):
        _, err = _asana_request("DELETE", f"/tasks/{task_gid}")
        if err:
            return f"Failed to delete '{task_name}': {err}"
        return f"'{task_name}' deleted."

    def _exec_batch_delete(tasks):
        from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeout

        def _do_one(t):
            _, err = _asana_request("DELETE", f"/tasks/{t['task_gid']}")
            label = t.get("task_name", t.get("task_gid", "?"))
            if err:
                return label, f"FAILED: {err}"
            return label, "✓ deleted"

        rows = []
        with ThreadPoolExecutor(max_workers=10) as ex:
            futures = {ex.submit(_do_one, t): t for t in tasks}
            for fut in as_completed(futures, timeout=60):
                try:
                    label, status = fut.result(timeout=12)
                except FutureTimeout:
                    t = futures[fut]
                    label = t.get("task_name", t.get("task_gid", "?"))
                    status = "TIMED OUT"
                except Exception as exc:
                    t = futures[fut]
                    label = t.get("task_name", t.get("task_gid", "?"))
                    status = f"ERROR: {exc}"
                rows.append((label, status))

        ok  = sum(1 for _, s in rows if s == "✓ deleted")
        bad = [(l, s) for l, s in rows if s != "✓ deleted"]
        lines = [f"Batch delete complete: {ok}/{len(tasks)} deleted."]
        if bad:
            lines.append(f"⚠️ {len(bad)} issue(s):")
            for l, s in bad:
                lines.append(f"  • {l}: {s}")
        return "\n".join(lines)

    def _exec_update_task(task_gid, task_name, new_name=None, completed=None, due_on=None, notes=None):
        payload = {"data": {}}
        if new_name:
            payload["data"]["name"] = new_name
        if completed is not None:
            payload["data"]["completed"] = completed
        if due_on:
            payload["data"]["due_on"] = due_on
        if notes is not None:
            payload["data"]["notes"] = notes
        if not payload["data"]:
            return "No fields to update provided."
        _, err = _asana_request("PUT", f"/tasks/{task_gid}", payload)
        if err:
            return f"Error updating task: {err}"
        label = new_name or task_name
        return f"Task '{label}' updated successfully."

    def _exec_get_comments(task_gid, task_name):
        MY_NAME = "Madeline Gall"
        task_url = f"https://app.asana.com/0/0/{task_gid}"
        stories, err = _asana_request("GET", f"/tasks/{task_gid}/stories",
                                      {"opt_fields": "type,text,created_by.name,created_at"})
        if err:
            return f"Error fetching comments for '{task_name}': {err}"
        all_comments = [s for s in (stories if isinstance(stories, list) else [])
                        if s.get("type") == "comment"]
        if not all_comments:
            return f"No comments on '{task_name}'.\nTask link: {task_url}"

        # Mark comments that are replies to the user (come after a Madeline comment)
        lines = [f"Comments on '{task_name}' — {task_url}"]
        last_was_mine = False
        shown = 0
        for c in all_comments:
            author = (c.get("created_by") or {}).get("name", "Unknown")
            when   = (c.get("created_at") or "")[:10]
            text   = (c.get("text") or "").strip()
            if author == MY_NAME:
                last_was_mine = True
                continue  # skip own comments in display
            prefix = "↩ replied to you" if last_was_mine else ""
            label  = f"[{author} — {when}]{' · ' + prefix if prefix else ''}"
            lines.append(f"\n{label}\n{text}")
            last_was_mine = False
            shown += 1

        if shown == 0:
            return (f"No comments from others on '{task_name}' "
                    f"(only your own comments exist).\nTask link: {task_url}")
        return "\n".join(lines)

    def _exec_get_comments_batch(tasks):
        """Fetch comments for multiple tasks in parallel. tasks = [{task_gid, task_name}]."""
        from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeout
        MY_NAME = "Madeline Gall"

        def _fetch_one(task_gid, task_name):
            task_url = f"https://app.asana.com/0/0/{task_gid}"
            stories, err = _asana_request("GET", f"/tasks/{task_gid}/stories",
                                          {"opt_fields": "type,text,created_by.name,created_at"})
            if err:
                return task_name, f"error: {err}", task_url
            all_comments = [s for s in (stories if isinstance(stories, list) else [])
                            if s.get("type") == "comment"]
            last_was_mine = False
            others = []
            for c in all_comments:
                author = (c.get("created_by") or {}).get("name", "Unknown")
                when   = (c.get("created_at") or "")[:10]
                text   = (c.get("text") or "").strip()
                if author == MY_NAME:
                    last_was_mine = True
                    continue
                prefix = " · ↩ replied to you" if last_was_mine else ""
                others.append(f"[{author} — {when}{prefix}]\n{text}")
                last_was_mine = False
            return task_name, others, task_url

        results = {}
        with ThreadPoolExecutor(max_workers=8) as ex:
            futures = {ex.submit(_fetch_one, t["task_gid"], t["task_name"]): t["task_name"]
                       for t in tasks}
            for fut in as_completed(futures, timeout=30):
                try:
                    name, comments, url = fut.result(timeout=8)
                    results[name] = (comments, url)
                except FutureTimeout:
                    results[futures[fut]] = (["timed out"], "")
                except Exception as ex2:
                    results[futures[fut]] = ([f"error: {ex2}"], "")

        sections = []
        has_activity = False
        for t in tasks:
            name = t["task_name"]
            comments, url = results.get(name, ([], ""))
            if isinstance(comments, str):  # error string
                sections.append(f"⚠ {name}: {comments}")
            elif comments == ["timed out"]:
                sections.append(f"⏱ {name}: timed out")
            elif not comments:
                pass  # skip tasks with no comments from others
            else:
                has_activity = True
                link = f" — {url}" if url else ""
                sections.append(f"\n📌 {name}{link}\n" + "\n\n".join(comments))

        if not has_activity:
            return f"No comments from others on any of the {len(tasks)} tasks checked."
        return "\n\n".join(sections)

    def _trunc_for_history(content, limit=800):
        if not isinstance(content, str) or len(content) <= limit:
            return content
        cut = content[:limit].rfind('\n')
        if cut < limit // 2:
            cut = limit
        return content[:cut] + "\n[…truncated — bot will re-fetch if needed]"

    def generate():
        def sse(obj):
            return f"data: {json.dumps(obj)}\n\n"

        ai_client  = _anthropic.Anthropic(api_key=key)
        trimmed    = _safe_trim(messages, 8)
        history_additions = []
        reply_text = ""
        from datetime import date as _today_cls
        today_str = _today_cls.today().isoformat()
        system_prompt = (
            f"You are a personal assistant for the admin of North Lake Dispatch, a vacation rental operations platform. "
            f"Today is {today_str}.\n\n"
            "TOOLS:\n"
            "1. NOTIFICATIONS — get_my_notifications: reads unread Asana comment notifications "
            "from the local database (instant, no API call). Filter by property_name to scope to one house. "
            "Returns task GIDs so you can immediately act (reply, update, etc.).\n"
            "2. ASANA — get_my_asana_tasks (fetch tasks), "
            "get_task_comments (comments on ONE task), "
            "get_comments_batch (comments on MULTIPLE tasks in parallel — ALWAYS use this for 2+ tasks), "
            "update_asana_task (single update), "
            "batch_update_asana_tasks (multiple updates in parallel — use this for 2+ tasks), "
            "delete_asana_task (delete one task), "
            "batch_delete_asana_tasks (delete multiple tasks in parallel — use for 2+), "
            "draft_asana_comment (suggest a comment for the user to edit and post).\n"
            "3. BREEZEWAY — fetch_breezeway_tasks (property cleaning/inspection/maintenance tasks).\n\n"
            "PROPERTY CONTEXT — when the user asks 'what do we know about [house]' or asks for a summary of a house:\n"
            "Run ALL of these in parallel (call them in one turn):\n"
            "  a) get_my_notifications(property_name='[house]') — unread comments on tasks for that house\n"
            "  b) get_my_asana_tasks — then filter results to tasks whose parent.name matches the house, "
            "then call get_comments_batch on those tasks to get full comment history\n"
            "  c) fetch_breezeway_tasks for the house (current date through +14 days) — cleaning/maintenance schedule\n"
            "Combine all results into one summary. Do not ask which source to check — check all of them.\n\n"
            "CONTEXT — understand this about how tasks are structured:\n"
            "- TASK TREE: Parent tasks = property/house names. Children tasks = work assigned to org members. "
            "Any task assigned to the user (you are talking to Madeline Gall) concerns her, "
            "whether it's an arrival task, departure task, inspection, or anything else at a property.\n"
            "- ARRIVAL SYNONYMS: 'Lease walk thru', 'Lease arrival', 'Arrival task', 'Walk thru', "
            "'Move-in inspection', 'Guest arrival' — all mean the same thing: a guest or tenant arriving.\n"
            "- DEPARTURE SYNONYMS: 'Departure task', 'Lease departure', 'Post lease inspection', "
            "'Move-out inspection', 'Guest departure', 'Checkout task' — all mean the same thing: "
            "a guest or tenant leaving. Both arrivals AND departures concern Madeline.\n"
            "- COMMENTS: When the user asks about comments across multiple tasks, use get_comments_batch "
            "with ALL relevant tasks in a single call — never loop get_task_comments one task at a time "
            "and never ask 'want me to continue with the next batch?' Just do all of them. "
            "If there are more than 20 tasks, split into two get_comments_batch calls back-to-back without asking. "
            "The tool skips tasks with no outside comments automatically, so results are concise.\n\n"
            "RULES — follow these exactly:\n"
            "- NEVER modify, paraphrase, abbreviate, or invent task names or property names. "
            "Always copy them character-for-character exactly as they appear in Asana data. "
            "If a name looks odd or unfamiliar, report it exactly as-is — do not 'correct' it.\n"
            "- DEFAULT SOURCE IS ASANA. When the user asks about tasks, always use Asana tools. "
            "Only use fetch_breezeway_tasks if the user explicitly says 'Breezeway' or asks about "
            "cleaning jobs, inspections, or maintenance schedules by property.\n"
            "- NEVER say you are doing something without immediately calling the tool. "
            "Saying 'I'll fire all updates now' and then not calling a tool is not allowed. "
            "Call the tool first, then describe what happened based on the results.\n"
            "- For 2 or more task updates: ALWAYS use batch_update_asana_tasks, never loop update_asana_task.\n"
            "- After any tool call, report the results honestly: how many succeeded, "
            "which ones timed out or failed, and the exact error message for failures.\n"
            "- Before any write (update, batch, comment): show CONFIRM_ACTION: <what you will do> "
            "and wait for the user to confirm. After confirmation, call the tool immediately.\n"
            "- Never guess task GIDs — always call get_my_asana_tasks first.\n"
            "- If get_comments_batch returns 404 errors on multiple tasks, the GIDs are stale — "
            "call get_my_asana_tasks again to get fresh GIDs, then retry. Do not tell the user "
            "the tasks are 'corrupted or deleted' unless re-fetching also fails.\n"
            "- If a tool returns an error, tell the user exactly what it says.\n"
            "- Be concise. Do not pad responses.\n"
            "- FORMATTING — when presenting lists of tasks or properties, always use this structure:\n"
            "  **[House Name]**\n"
            "    • [Month Day, Year] – [Month Day, Year]: [Task Name]\n"
            "    • [Month Day, Year] – [Month Day, Year]: [Task Name]\n"
            "  (blank line between each property)\n"
            "  Write dates as full month name + day + year (e.g. 'May 17, 2026'). "
            "  Never use YYYY-MM-DD format in displayed output. "
            "  House name is bold on its own line. Tasks are indented bullet points beneath it. "
            "  Always put a blank line between properties."
        )

        try:
            for _turn in range(6):
                turn_text    = ""
                asst_content = []
                with ai_client.messages.stream(
                    model="claude-haiku-4-5-20251001", max_tokens=1024,
                    system=system_prompt, messages=trimmed, tools=tools,
                ) as stream:
                    for chunk in stream.text_stream:
                        turn_text  += chunk
                        reply_text += chunk
                        yield sse({"type": "delta", "text": chunk})
                    final_msg = stream.get_final_message()

                if final_msg.stop_reason == "tool_use":
                    for b in final_msg.content:
                        if b.type == "tool_use":
                            asst_content.append({"type": "tool_use", "id": b.id, "name": b.name, "input": b.input})
                        elif b.type == "text":
                            asst_content.append({"type": "text", "text": b.text})
                    trimmed.append({"role": "assistant", "content": asst_content})
                    history_additions.append({"role": "assistant", "content": asst_content})

                    tool_results         = []
                    tool_results_history = []
                    for block in final_msg.content:
                        if block.type != "tool_use":
                            continue
                        if block.name == "get_my_notifications":
                            prop = block.input.get("property_name") or ""
                            label = f" for '{prop}'" if prop else ""
                            yield sse({"type": "status", "text": f"Reading notifications{label}…"})
                            result = _exec_get_notifications(
                                prop,
                                block.input.get("include_dismissed", False),
                            )
                        elif block.name == "get_task_comments":
                            yield sse({"type": "status", "text": f"Fetching comments on '{block.input.get('task_name', 'task')}'…"})
                            result = _exec_get_comments(
                                block.input.get("task_gid", ""),
                                block.input.get("task_name", ""),
                            )
                        elif block.name == "get_comments_batch":
                            tasks_in = block.input.get("tasks", [])
                            n = len(tasks_in)
                            yield sse({"type": "status", "text": f"Checking comments on {n} tasks in parallel…"})
                            result = _exec_get_comments_batch(tasks_in)
                        elif block.name == "get_my_asana_tasks":
                            yield sse({"type": "status", "text": "Fetching your Asana tasks…"})
                            result = _exec_get_tasks(
                                block.input.get("filter", "incomplete"),
                                block.input.get("project"),
                            )
                        elif block.name == "update_asana_task":
                            yield sse({"type": "status", "text": "Updating task…"})
                            result = _exec_update_task(
                                block.input.get("task_gid", ""),
                                block.input.get("task_name", ""),
                                block.input.get("new_name"),
                                block.input.get("completed"),
                                block.input.get("due_on"),
                                block.input.get("notes"),
                            )
                        elif block.name == "batch_update_asana_tasks":
                            updates = block.input.get("updates", [])
                            n = len(updates)
                            yield sse({"type": "status", "text": f"Running {n} Asana update{'s' if n != 1 else ''} in parallel…"})
                            result = _exec_batch_update(updates)
                        elif block.name == "delete_asana_task":
                            yield sse({"type": "status", "text": f"Deleting task…"})
                            result = _exec_delete_task(
                                block.input.get("task_gid", ""),
                                block.input.get("task_name", ""),
                            )
                        elif block.name == "batch_delete_asana_tasks":
                            tasks_to_del = block.input.get("tasks", [])
                            n = len(tasks_to_del)
                            yield sse({"type": "status", "text": f"Deleting {n} task{'s' if n != 1 else ''} in parallel…"})
                            result = _exec_batch_delete(tasks_to_del)
                        elif block.name == "draft_asana_comment":
                            result = (
                                f"DRAFT_COMMENT_READY\n"
                                f"task_gid={block.input.get('task_gid','')}\n"
                                f"task_name={block.input.get('task_name','')}\n"
                                f"suggested_text={block.input.get('suggested_text','')}"
                            )
                        elif block.name == "fetch_breezeway_tasks":
                            names = block.input.get("property_names") or []
                            n = len(names)
                            yield sse({"type": "status", "text": f"Fetching Breezeway tasks for {n} propert{'y' if n==1 else 'ies'}…"})
                            result = _execute_fetch_tasks_multi_standalone(
                                block.input.get("start_date", ""),
                                block.input.get("end_date", ""),
                                names,
                                block.input.get("status"),
                            )
                        else:
                            result = f"Unknown tool: {block.name}"
                        tool_results.append({
                            "type": "tool_result", "tool_use_id": block.id, "content": result,
                        })
                        # Only truncate Breezeway results — Asana results are kept full
                        # because the bot frequently needs GIDs/details in follow-up turns.
                        history_content = _trunc_for_history(result) if block.name == "fetch_breezeway_tasks" else result
                        tool_results_history.append({
                            "type": "tool_result", "tool_use_id": block.id,
                            "content": history_content,
                        })
                    trimmed.append({"role": "user", "content": tool_results})
                    history_additions.append({"role": "user", "content": tool_results_history})
                else:
                    break
            else:
                # Loop exhausted all turns still in tool_use — tell the user honestly
                yield sse({"type": "delta", "text":
                    "\n\n⚠️ Hit the turn limit before finishing — some tasks weren't checked. "
                    "Try asking about a smaller batch (e.g. 'check comments on my overdue tasks only')."})

            yield sse({"type": "done", "history_additions": history_additions})
        except Exception as e:
            yield sse({"type": "error", "text": str(e)})

    return Response(stream_with_context(generate()), mimetype="text/event-stream",
                    headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"})


@my_bot_bp.route("/admin/my-bot/post-comment", methods=["POST"])
@login_required
@_my_bot_required
def my_bot_post_comment():
    body      = request.get_json(force=True) or {}
    task_gid  = (body.get("task_gid") or "").strip()
    task_name = (body.get("task_name") or "").strip()
    text      = (body.get("text") or "").strip()
    if not task_gid or not text:
        return jsonify({"error": "task_gid and text required"}), 400
    _, err = _asana_request("POST", f"/tasks/{task_gid}/stories", {"data": {"text": text}})
    if err:
        return jsonify({"error": err}), 500
    return jsonify({"ok": True, "task_name": task_name})
