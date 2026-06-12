"""
routes/quick_complete.py — Quick-complete recurring Asana tasks for Madeline.

Visible and accessible only to mgalldev@gmail.com.
Finds the two named recurring tasks in Facilities Maintenance & Projects
and marks them complete with one button click.

Endpoints:
  GET  /my/quick-complete      — page
  POST /my/quick-complete/run  — find + complete both tasks (JSON)
"""

import os
import requests

from flask import Blueprint, render_template, jsonify
from flask_login import login_required, current_user

quick_complete_bp = Blueprint("quick_complete", __name__)

MADELINE_EMAIL = "mgalldev@gmail.com"
TARGET_PROJECT = "Facilities Maintenance & Projects"
TARGET_TASKS   = [
    "Task Title / Dates",
    "Review new Post Rental Inspections",
]


def _madeline_required(f):
    from functools import wraps
    from flask import redirect, url_for
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated or current_user.email != MADELINE_EMAIL:
            return redirect(url_for("dispatch.home"))
        return f(*args, **kwargs)
    return decorated


def _asana_get(path, params=None):
    token = os.environ.get("ASANA_TOKEN", "")
    if not token:
        return None, "ASANA_TOKEN not configured."
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    url = f"https://app.asana.com/api/1.0{path}"
    try:
        resp = requests.get(url, headers=headers, params=params or {}, timeout=15)
        if not resp.ok:
            return None, f"Asana {resp.status_code}: {resp.text[:200]}"
        return resp.json().get("data"), None
    except Exception as e:
        return None, str(e)


def _asana_put(path, payload):
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
        resp = requests.put(url, headers=headers, json=payload, timeout=15)
        if not resp.ok:
            return None, f"Asana {resp.status_code}: {resp.text[:200]}"
        return resp.json().get("data"), None
    except Exception as e:
        return None, str(e)


def _find_and_complete(task_name: str, all_tasks: list) -> dict:
    """Find task_name in all_tasks (already filtered to TARGET_PROJECT) and mark complete."""
    name_lower = task_name.strip().lower()
    match = None
    for t in all_tasks:
        if t.get("name", "").strip().lower() == name_lower:
            match = t
            break
    # Fallback: substring match
    if not match:
        for t in all_tasks:
            if name_lower in t.get("name", "").strip().lower():
                match = t
                break

    if not match:
        return {"task": task_name, "success": False,
                "detail": f"Not found in '{TARGET_PROJECT}' — may already be complete or not yet assigned."}

    _, err = _asana_put(f"/tasks/{match['gid']}", {"data": {"completed": True}})
    if err:
        return {"task": task_name, "success": False, "detail": err}
    return {"task": task_name, "success": True,
            "detail": f"✓ Marked complete"}


@quick_complete_bp.route("/my/quick-complete")
@login_required
@_madeline_required
def quick_complete_page():
    return render_template("quick_complete.html")


@quick_complete_bp.route("/my/quick-complete/run", methods=["POST"])
@login_required
@_madeline_required
def quick_complete_run():
    # Get workspace
    ws_data, err = _asana_get("/workspaces", {"limit": 1})
    if err or not ws_data:
        return jsonify({"error": f"Could not get workspace: {err}"})
    ws = ws_data[0].get("gid") if isinstance(ws_data, list) else ws_data.get("gid", "")

    # Get user task list GID
    utl_data, err = _asana_get("/users/me/user_task_list", {"workspace": ws})
    if err or not utl_data:
        return jsonify({"error": f"Could not get task list: {err}"})
    utl_gid = utl_data.get("gid") if isinstance(utl_data, dict) else None
    if not utl_gid:
        return jsonify({"error": "Could not find user task list GID."})

    # Fetch all incomplete tasks in TARGET_PROJECT
    all_tasks = []
    page_params = {
        "opt_fields": "name,gid,completed,projects.name",
        "completed_since": "now",
        "limit": 100,
    }
    for _ in range(10):
        data, err = _asana_get(f"/user_task_lists/{utl_gid}/tasks", page_params)
        if err or not data:
            break
        all_tasks.extend(data if isinstance(data, list) else [])
        break  # single page is enough for name-matching

    # Filter to TARGET_PROJECT
    project_tasks = [
        t for t in all_tasks
        if any(TARGET_PROJECT.lower() in (p.get("name") or "").lower()
               for p in (t.get("projects") or []))
    ]

    results = [_find_and_complete(name, project_tasks) for name in TARGET_TASKS]
    return jsonify({"results": results, "all_ok": all(r["success"] for r in results)})
