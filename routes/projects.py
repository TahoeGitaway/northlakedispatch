"""
routes/projects.py — Multi-property project tracking (e.g. Fire Extinguisher).
Completely separate from the daily routing system.
"""

import csv
import difflib
import io
import json
import math
import random
from datetime import datetime

from flask import (Blueprint, Response, jsonify, redirect,
                   render_template, request, url_for)
from flask_login import current_user, login_required

from db import get_cursor, get_db

projects_bp = Blueprint("projects", __name__, url_prefix="/projects")

CLUSTER_COLORS = [
    "#6366f1", "#f59e0b", "#10b981", "#ef4444",
    "#3b82f6", "#8b5cf6", "#ec4899", "#14b8a6",
    "#f97316", "#84cc16",
]


# ── Helpers ───────────────────────────────────────────────────────

def _load_db_properties():
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        'SELECT id, "Property Name", "Unit Address", "Latitude", "Longitude" '
        'FROM properties WHERE "Latitude" IS NOT NULL AND "Longitude" IS NOT NULL'
    )
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows


def _match_name(name, db_props):
    """Return (match_type, matched_row). match_type: 'exact', 'fuzzy', or None."""
    name_clean = name.strip()
    name_lower = name_clean.lower()

    for row in db_props:
        if row["Property Name"].lower().strip() == name_lower:
            return "exact", dict(row)

    db_lowers = [r["Property Name"].lower().strip() for r in db_props]
    close = difflib.get_close_matches(name_lower, db_lowers, n=1, cutoff=0.55)
    if close:
        for row in db_props:
            if row["Property Name"].lower().strip() == close[0]:
                score = difflib.SequenceMatcher(None, name_lower, close[0]).ratio()
                result = dict(row)
                result["_score"] = round(score, 2)
                result["_input"] = name_clean
                return "fuzzy", result

    return None, {"_input": name_clean}


def _kmeans(points, k, max_iters=50):
    """K-means on [(lat, lng), ...]. Returns list of integer cluster labels."""
    n = len(points)
    if n == 0:
        return []
    k = max(1, min(k, n))
    if k == 1:
        return [0] * n

    rng = random.Random(42)
    centroids = list(rng.sample(points, k))
    labels = [0] * n

    for _ in range(max_iters):
        new_labels = [
            min(range(k), key=lambda ki, p=p: math.hypot(
                p[0] - centroids[ki][0], p[1] - centroids[ki][1]))
            for p in points
        ]
        if new_labels == labels:
            break
        labels = new_labels
        for ki in range(k):
            pts = [points[i] for i in range(n) if labels[i] == ki]
            if pts:
                centroids[ki] = (
                    sum(p[0] for p in pts) / len(pts),
                    sum(p[1] for p in pts) / len(pts),
                )

    return labels


def _get_project(project_id):
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("SELECT * FROM projects WHERE id = %s", (project_id,))
    row = cur.fetchone()
    cur.close(); conn.close()
    return row


# ── List & Create ─────────────────────────────────────────────────

@projects_bp.route("/", methods=["GET", "POST"])
@login_required
def list_projects():
    conn = get_db()
    cur  = get_cursor(conn)

    if request.method == "POST":
        name = request.form.get("name", "").strip()
        desc = request.form.get("description", "").strip()
        if name:
            cur.execute(
                "INSERT INTO projects (name, description, created_by, created_at) "
                "VALUES (%s, %s, %s, %s)",
                (name, desc, current_user.id, datetime.utcnow().isoformat())
            )
            conn.commit()
        cur.close(); conn.close()
        return redirect(url_for("projects.list_projects"))

    cur.execute("""
        SELECT p.id, p.name, p.description, p.status, p.created_at,
               u.name AS creator_name,
               COUNT(DISTINCT pp.id)  AS total_props,
               COUNT(DISTINCT tc.id)  AS completed_props
        FROM projects p
        LEFT JOIN users u ON p.created_by = u.id
        LEFT JOIN project_properties pp ON pp.project_id = p.id
        LEFT JOIN task_completions tc ON tc.project_property_id = pp.id
        GROUP BY p.id, u.name
        ORDER BY p.created_at DESC
    """)
    projects = cur.fetchall()
    cur.close(); conn.close()
    return render_template("projects_list.html", projects=projects)


# ── Project Detail ────────────────────────────────────────────────

@projects_bp.route("/<int:project_id>")
@login_required
def project_detail(project_id):
    conn = get_db()
    cur  = get_cursor(conn)

    cur.execute("SELECT * FROM projects WHERE id = %s", (project_id,))
    project = cur.fetchone()
    if not project:
        cur.close(); conn.close()
        return "Project not found", 404

    cur.execute("""
        SELECT pp.id, pp.property_name, pp.address, pp.lat, pp.lng,
               tc.id           AS completion_id,
               tc.completed_at,
               tc.comment,
               u.name          AS completed_by_name
        FROM project_properties pp
        LEFT JOIN LATERAL (
            SELECT id, completed_at, comment, completed_by
            FROM task_completions
            WHERE project_property_id = pp.id
            ORDER BY completed_at DESC LIMIT 1
        ) tc ON true
        LEFT JOIN users u ON u.id = tc.completed_by
        WHERE pp.project_id = %s
        ORDER BY pp.property_name
    """, (project_id,))
    props = [dict(r) for r in cur.fetchall()]

    cur.close(); conn.close()

    total     = len(props)
    completed = sum(1 for p in props if p["completion_id"])

    route_size = max(5, min(50, int(request.args.get("size", 15))))

    # K-means clusters for pending properties
    pending = [p for p in props if not p["completion_id"] and p["lat"] and p["lng"]]
    if pending:
        k      = max(1, math.ceil(len(pending) / route_size))
        labels = _kmeans([(p["lat"], p["lng"]) for p in pending], k)
        label_map = {pending[i]["id"]: labels[i] for i in range(len(pending))}
    else:
        label_map = {}

    for p in props:
        p["cluster"] = label_map.get(p["id"])

    return render_template("project_detail.html",
        project    = project,
        props      = props,
        props_json = json.dumps(props),
        total      = total,
        completed  = completed,
        colors     = json.dumps(CLUSTER_COLORS),
        route_size = route_size,
    )


# ── Planner ───────────────────────────────────────────────────────

@projects_bp.route("/<int:project_id>/planner")
@login_required
def planner(project_id):
    project = _get_project(project_id)
    if not project:
        return "Project not found", 404
    return render_template("project_planner.html", project=project)


@projects_bp.route("/<int:project_id>/planner/match", methods=["POST"])
@login_required
def planner_match(project_id):
    data  = request.get_json(force=True)
    names = [n.strip() for n in (data.get("names") or []) if n.strip()]
    if not names:
        return jsonify({"results": []})

    db_props = _load_db_properties()
    results  = []
    for name in names:
        mtype, row = _match_name(name, db_props)
        if mtype == "exact":
            results.append({
                "type":    "exact",
                "input":   name,
                "name":    row["Property Name"],
                "address": row["Unit Address"] or "",
                "lat":     row["Latitude"],
                "lng":     row["Longitude"],
            })
        elif mtype == "fuzzy":
            results.append({
                "type":    "fuzzy",
                "input":   row["_input"],
                "name":    row["Property Name"],
                "address": row["Unit Address"] or "",
                "lat":     row["Latitude"],
                "lng":     row["Longitude"],
                "score":   row["_score"],
            })
        else:
            results.append({"type": "none", "input": name})

    return jsonify({"results": results})


@projects_bp.route("/<int:project_id>/planner/add", methods=["POST"])
@login_required
def planner_add(project_id):
    project = _get_project(project_id)
    if not project:
        return jsonify({"error": "Not found"}), 404

    data  = request.get_json(force=True)
    props = data.get("properties", [])

    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        "SELECT property_name FROM project_properties WHERE project_id = %s",
        (project_id,)
    )
    existing = {r["property_name"].lower() for r in cur.fetchall()}

    added = 0
    for p in props:
        name = (p.get("name") or "").strip()
        if not name or name.lower() in existing:
            continue
        cur.execute(
            """INSERT INTO project_properties
               (project_id, property_name, address, lat, lng, added_at, added_by)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (project_id, name, p.get("address", ""),
             p.get("lat"), p.get("lng"),
             datetime.utcnow().isoformat(), current_user.id)
        )
        existing.add(name.lower())
        added += 1

    conn.commit()
    cur.close(); conn.close()
    return jsonify({"added": added})


# ── Task Completion ───────────────────────────────────────────────

@projects_bp.route("/<int:project_id>/properties/<int:prop_id>/complete",
                   methods=["POST"])
@login_required
def complete_property(project_id, prop_id):
    data    = request.get_json(force=True)
    comment = (data.get("comment") or "").strip()
    now     = datetime.utcnow().isoformat()

    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        """INSERT INTO task_completions
           (project_property_id, completed_by, completed_at, comment)
           VALUES (%s, %s, %s, %s)""",
        (prop_id, current_user.id, now, comment)
    )
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"success": True, "completed_by": current_user.name,
                    "completed_at": now})


@projects_bp.route("/<int:project_id>/properties/<int:prop_id>/uncomplete",
                   methods=["POST"])
@login_required
def uncomplete_property(project_id, prop_id):
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        "DELETE FROM task_completions WHERE project_property_id = %s",
        (prop_id,)
    )
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"success": True})


# ── Delete ───────────────────────────────────────────────────────

@projects_bp.route("/<int:project_id>/delete", methods=["POST"])
@login_required
def delete_project(project_id):
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute("DELETE FROM projects WHERE id = %s", (project_id,))
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"success": True})


@projects_bp.route("/<int:project_id>/properties/<int:prop_id>/delete",
                   methods=["POST"])
@login_required
def delete_property(project_id, prop_id):
    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        "DELETE FROM project_properties WHERE id = %s AND project_id = %s",
        (prop_id, project_id)
    )
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"success": True})


# ── CSV Report ────────────────────────────────────────────────────

@projects_bp.route("/<int:project_id>/report.csv")
@login_required
def report_csv(project_id):
    conn = get_db()
    cur  = get_cursor(conn)

    cur.execute("SELECT name FROM projects WHERE id = %s", (project_id,))
    project = cur.fetchone()
    if not project:
        cur.close(); conn.close()
        return "Not found", 404

    cur.execute("""
        SELECT pp.property_name, pp.address,
               CASE WHEN tc.id IS NOT NULL THEN 'Complete' ELSE 'Pending' END AS status,
               u.name  AS completed_by,
               tc.completed_at,
               tc.comment
        FROM project_properties pp
        LEFT JOIN LATERAL (
            SELECT id, completed_at, comment, completed_by
            FROM task_completions
            WHERE project_property_id = pp.id
            ORDER BY completed_at DESC LIMIT 1
        ) tc ON true
        LEFT JOIN users u ON u.id = tc.completed_by
        WHERE pp.project_id = %s
        ORDER BY status, pp.property_name
    """, (project_id,))
    rows = cur.fetchall()
    cur.close(); conn.close()

    buf = io.StringIO()
    w   = csv.writer(buf)
    w.writerow(["Property", "Address", "Status",
                "Completed By", "Completed At", "Comment"])
    for r in rows:
        w.writerow([
            r["property_name"], r["address"], r["status"],
            r["completed_by"] or "", r["completed_at"] or "", r["comment"] or "",
        ])

    fname = project["name"].replace(" ", "_") + "_report.csv"
    return Response(buf.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename={fname}"})
