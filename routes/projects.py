"""
routes/projects.py — Multi-property project tracking (e.g. Fire Extinguisher).
Completely separate from the daily routing system.
"""

import csv
import difflib
import io
import json
import math
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
    """Return (match_type, data). match_type: 'exact', 'fuzzy', or None.
    For 'fuzzy', data is a list of up to 3 candidate dicts ordered by score."""
    name_clean = name.strip()
    name_lower = name_clean.lower()

    for row in db_props:
        if row["Property Name"].lower().strip() == name_lower:
            return "exact", dict(row)

    db_lowers = [r["Property Name"].lower().strip() for r in db_props]
    close = difflib.get_close_matches(name_lower, db_lowers, n=3, cutoff=0.55)
    if close:
        candidates = []
        for match_lower in close:
            for row in db_props:
                if row["Property Name"].lower().strip() == match_lower:
                    score = difflib.SequenceMatcher(None, name_lower, match_lower).ratio()
                    result = dict(row)
                    result["_score"] = round(score, 2)
                    result["_input"] = name_clean
                    candidates.append(result)
                    break
        return "fuzzy", candidates

    return None, {"_input": name_clean}


def _route_groups(props, service_min=15, day_min=540, speed_kmh=35):
    """
    Greedy nearest-neighbor grouping that fills each route up to day_min minutes.
    Returns (labels, route_minutes, route_sequences) where:
      labels[i]            = route index for props[i]
      route_minutes[group] = total estimated minutes for that route
      route_sequences[group] = [prop_indices] in nearest-neighbor visit order
    """
    n = len(props)
    if n == 0:
        return [], {}, {}

    def drive_min(a, b):
        lat1, lon1 = math.radians(a["lat"]), math.radians(a["lng"])
        lat2, lon2 = math.radians(b["lat"]), math.radians(b["lng"])
        dlat, dlon = lat2 - lat1, lon2 - lon1
        h = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2
        km = 2 * 6371 * math.asin(math.sqrt(min(1.0, h)))
        return (km / speed_kmh) * 60

    labels          = [-1] * n
    route_minutes   = {}
    route_sequences = {}
    unvisited       = list(range(n))
    avg_lat = sum(props[i]["lat"] for i in unvisited) / n
    avg_lng = sum(props[i]["lng"] for i in unvisited) / n
    unvisited.sort(key=lambda i: (props[i]["lat"] - avg_lat)**2 + (props[i]["lng"] - avg_lng)**2)

    group = 0
    while unvisited:
        start = unvisited.pop(0)
        labels[start] = group
        sequence = [start]
        elapsed  = service_min
        current  = start

        while unvisited:
            best_pos, best_drive = None, float("inf")
            for pos, idx in enumerate(unvisited):
                d = drive_min(props[current], props[idx])
                if d < best_drive:
                    best_drive, best_pos = d, pos

            if best_pos is None:
                break
            if elapsed + best_drive + service_min > day_min:
                break

            elapsed += best_drive + service_min
            idx = unvisited.pop(best_pos)
            labels[idx] = group
            sequence.append(idx)
            current = idx

        route_sequences[group] = sequence
        route_minutes[group]   = round(elapsed)
        group += 1

    return labels, route_minutes, route_sequences


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
               tc.task_type,
               u.name          AS completed_by_name
        FROM project_properties pp
        LEFT JOIN LATERAL (
            SELECT id, completed_at, comment, completed_by, task_type
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

    # Greedy nearest-neighbor day-filling grouping
    pending = [p for p in props if not p["completion_id"] and p["lat"] and p["lng"]]
    if pending:
        labels, route_minutes, route_sequences = _route_groups(pending)
        label_map  = {pending[i]["id"]: labels[i] for i in range(len(pending))}
        nn_order_map = {
            pending[prop_idx]["id"]: pos
            for seq in route_sequences.values()
            for pos, prop_idx in enumerate(seq)
        }
        route_meta = {g: {"minutes": m} for g, m in route_minutes.items()}
        for g, meta in route_meta.items():
            meta["stops"] = sum(1 for lbl in labels if lbl == g)
    else:
        label_map    = {}
        nn_order_map = {}
        route_meta   = {}

    for p in props:
        p["cluster"]  = label_map.get(p["id"])
        p["nn_order"] = nn_order_map.get(p["id"], 9999)

    return render_template("project_detail.html",
        project    = project,
        props      = props,
        props_json = json.dumps(props),
        total      = total,
        completed  = completed,
        colors     = json.dumps(CLUSTER_COLORS),
        route_meta = json.dumps(route_meta),
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
                "type":       "fuzzy",
                "input":      row[0]["_input"],
                "candidates": [
                    {
                        "name":    c["Property Name"],
                        "address": c["Unit Address"] or "",
                        "lat":     c["Latitude"],
                        "lng":     c["Longitude"],
                        "score":   c["_score"],
                    }
                    for c in row
                ],
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

VALID_TASK_TYPES = {
    "departure_clean", "arrival_inspect", "owner_turnover",
    "deep_clean", "mid_stay", "maintenance",
}

TASK_TYPE_LABELS = {
    "departure_clean": "Departure Clean",
    "arrival_inspect": "Arrival Inspection",
    "owner_turnover":  "Owner Turnover",
    "deep_clean":      "Deep Clean",
    "mid_stay":        "Mid-Stay Check",
    "maintenance":     "Maintenance",
}

@projects_bp.route("/<int:project_id>/properties/<int:prop_id>/complete",
                   methods=["POST"])
@login_required
def complete_property(project_id, prop_id):
    data      = request.get_json(force=True)
    comment   = (data.get("comment") or "").strip()
    task_type = (data.get("task_type") or "departure_clean").strip()
    if task_type not in VALID_TASK_TYPES:
        task_type = "departure_clean"
    now = datetime.utcnow().isoformat()

    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        """INSERT INTO task_completions
           (project_property_id, completed_by, completed_at, comment, task_type)
           VALUES (%s, %s, %s, %s, %s)""",
        (prop_id, current_user.id, now, comment, task_type)
    )
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"success": True, "completed_by": current_user.name,
                    "completed_at": now, "task_type": task_type,
                    "task_type_label": TASK_TYPE_LABELS.get(task_type, task_type)})


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


# ── Properties by ID (for route optimizer pre-load) ──────────────

@projects_bp.route("/properties")
@login_required
def get_properties_by_ids():
    ids_str = request.args.get("ids", "")
    try:
        ids = [int(i) for i in ids_str.split(",") if i.strip()]
    except ValueError:
        return jsonify({"properties": []}), 400
    if not ids:
        return jsonify({"properties": []})

    conn = get_db()
    cur  = get_cursor(conn)
    cur.execute(
        """SELECT p.id, p.property_name AS name, p.address, p.lat, p.lng
           FROM project_properties p
           JOIN unnest(%s::int[]) WITH ORDINALITY AS ord(id, pos) ON p.id = ord.id
           ORDER BY ord.pos""",
        (ids,)
    )
    props = [dict(r) for r in cur.fetchall()]
    cur.close(); conn.close()
    return jsonify({"properties": props})


# ── Tech Task View (mobile) ───────────────────────────────────────

@projects_bp.route("/<int:project_id>/tasks")
@login_required
def project_tasks(project_id):
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
               tc.task_type,
               u.name          AS completed_by_name
        FROM project_properties pp
        LEFT JOIN LATERAL (
            SELECT id, completed_at, comment, completed_by, task_type
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

    pending = [p for p in props if not p["completion_id"] and p["lat"] and p["lng"]]
    if pending:
        labels, *_ = _route_groups(pending)
        label_map = {pending[i]["id"]: labels[i] for i in range(len(pending))}
    else:
        label_map = {}

    for p in props:
        p["cluster"] = label_map.get(p["id"])

    return render_template("project_tasks.html",
        project   = project,
        props_json= json.dumps(props),
        total     = total,
        completed = completed,
        colors    = json.dumps(CLUSTER_COLORS),
    )


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
               tc.task_type,
               u.name  AS completed_by,
               tc.completed_at,
               tc.comment
        FROM project_properties pp
        LEFT JOIN LATERAL (
            SELECT id, completed_at, comment, completed_by, task_type
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
    w.writerow(["Property", "Address", "Status", "Task Type",
                "Completed By", "Completed At", "Comment"])
    for r in rows:
        label = TASK_TYPE_LABELS.get(r["task_type"] or "", r["task_type"] or "")
        w.writerow([
            r["property_name"], r["address"], r["status"], label,
            r["completed_by"] or "", r["completed_at"] or "", r["comment"] or "",
        ])

    fname = project["name"].replace(" ", "_") + "_report.csv"
    return Response(buf.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename={fname}"})


@projects_bp.route("/<int:project_id>/debug")
@login_required
def project_debug(project_id):
    """Admin-only JSON dump of all task completions for a project."""
    if not current_user.is_admin:
        return jsonify({"error": "Admin only"}), 403

    conn = get_db()
    cur  = get_cursor(conn)

    cur.execute("SELECT id, name, status, created_at FROM projects WHERE id = %s", (project_id,))
    project = cur.fetchone()
    if not project:
        cur.close(); conn.close()
        return jsonify({"error": "Project not found"}), 404

    cur.execute("""
        SELECT pp.id, pp.property_name, pp.address, pp.lat, pp.lng, pp.added_at,
               u_add.name AS added_by_name,
               tc.id AS completion_id, tc.completed_at, tc.comment, tc.task_type,
               u_comp.name AS completed_by_name
        FROM project_properties pp
        LEFT JOIN users u_add ON u_add.id = pp.added_by
        LEFT JOIN LATERAL (
            SELECT id, completed_at, comment, completed_by, task_type
            FROM task_completions
            WHERE project_property_id = pp.id
            ORDER BY completed_at DESC LIMIT 1
        ) tc ON true
        LEFT JOIN users u_comp ON u_comp.id = tc.completed_by
        WHERE pp.project_id = %s
        ORDER BY pp.property_name
    """, (project_id,))
    props = [dict(r) for r in cur.fetchall()]

    cur.execute("""
        SELECT tc.id, tc.project_property_id, tc.completed_at, tc.comment,
               tc.task_type, u.name AS completed_by_name, pp.property_name
        FROM task_completions tc
        JOIN project_properties pp ON pp.id = tc.project_property_id
        LEFT JOIN users u ON u.id = tc.completed_by
        WHERE pp.project_id = %s
        ORDER BY tc.completed_at DESC
    """, (project_id,))
    completions = [dict(r) for r in cur.fetchall()]

    cur.close(); conn.close()

    total    = len(props)
    done     = sum(1 for p in props if p["completion_id"])
    return jsonify({
        "project":     dict(project),
        "summary":     {"total": total, "completed": done, "pending": total - done},
        "properties":  props,
        "all_completions": completions,
    })
