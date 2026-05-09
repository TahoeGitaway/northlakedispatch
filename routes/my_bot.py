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
        else:
            return None, f"Unknown method {method}"
        if not resp.ok:
            return None, f"Asana API {resp.status_code}: {resp.text[:200]}"
        return resp.json().get("data"), None
    except Exception as e:
        return None, str(e)


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
        "opt_fields": "gid,name",
    })
    if err or not tasks_data:
        return
    tasks = tasks_data if isinstance(tasks_data, list) else []

    new_notifications = []
    for task in tasks:
        tgid  = task.get("gid")
        tname = task.get("name", "Unnamed task")
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
            new_notifications.append((item_key, tgid, tname, sgid, commenter, comment_text, created_at))

    if new_notifications:
        conn2 = get_db(); cur2 = get_cursor(conn2)
        for (key, tgid, tname, sgid, commenter, text, cat) in new_notifications:
            cur2.execute(
                """INSERT INTO asana_notifications
                   (item_key, task_gid, task_name, story_gid, commenter, comment_text, asana_created_at, created_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (item_key) DO NOTHING""",
                (key, tgid, tname, sgid, commenter, text, cat, now_str),
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
            "opt_fields": "name,gid,due_on,completed,notes,projects.name,parent.name,parent.gid",
            "limit":      100,
        }
        if filter_val == "incomplete":
            params["completed_since"] = "now"

        data, err = _asana_request("GET", f"/user_task_lists/{utl_gid}/tasks", params)
        if err:
            return f"Error fetching tasks: {err}"
        tasks = data if isinstance(data, list) else []

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
            projects   = ", ".join(p.get("name", "") for p in (t.get("projects") or []))
            parent     = (t.get("parent") or {}).get("name", "")
            status     = "✓ done" if t.get("completed") else "open"
            due        = t.get("due_on") or "no due date"
            line = f'• [{t["gid"]}] {t["name"]} | {status} | due {due} | project: {projects or "none"}'
            if parent:
                line += f' | parent task: {parent}'
            lines.append(line)
            if t.get("notes"):
                lines.append(f'  Notes: {t["notes"]}')
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

    def generate():
        def sse(obj):
            return f"data: {json.dumps(obj)}\n\n"

        ai_client  = _anthropic.Anthropic(api_key=key)
        trimmed    = _safe_trim(messages, 20)
        history_additions = []
        reply_text = ""
        from datetime import date as _today_cls
        today_str = _today_cls.today().isoformat()
        system_prompt = (
            f"You are a personal assistant for the admin of North Lake Dispatch, a vacation rental operations platform. "
            f"Today is {today_str}.\n"
            "You have two data sources:\n"
            "1. ASANA — the user's personal task list. Use get_my_asana_tasks to look up tasks, "
            "update_asana_task to update them (always CONFIRM_ACTION first), "
            "and draft_asana_comment to suggest a comment the user edits before posting.\n"
            "2. BREEZEWAY — property operations tasks (cleaning, inspections, maintenance). "
            "Use fetch_breezeway_tasks with property names and a date range.\n"
            "- Be concise and direct. Use bullet points.\n"
            "- Never guess task GIDs — always call get_my_asana_tasks first to find them.\n"
            "- For write actions (update, comment), always show CONFIRM_ACTION: <description> first."
        )

        try:
            for _turn in range(6):
                turn_text    = ""
                asst_content = []
                with ai_client.messages.stream(
                    model="claude-sonnet-4-6", max_tokens=1500,
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

                    tool_results = []
                    for block in final_msg.content:
                        if block.type != "tool_use":
                            continue
                        if block.name == "get_my_asana_tasks":
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
                    tool_msg = {"role": "user", "content": tool_results}
                    trimmed.append(tool_msg)
                    history_additions.append(tool_msg)
                else:
                    break

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
