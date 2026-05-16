"""
routes/ops_bot.py — Ops Bot (TG Operations AI assistant).

All Ops Bot routes live here, separate from admin.py.
Admin-only: every endpoint is protected by @admin_required.

Endpoints:
  GET  /admin/chatbot                          — page
  POST /admin/chatbot/chat                     — SSE streaming chat
  POST /admin/chatbot/session/save
  GET  /admin/chatbot/sessions
  GET  /admin/chatbot/session/<id>
  DELETE /admin/chatbot/session/<id>
  POST /admin/chatbot/save-flag
"""

import json
import os
import time as _time
from datetime import datetime, timedelta

from flask import (Blueprint, render_template, request, jsonify,
                   Response, stream_with_context)
from flask_login import login_required, current_user

from db import get_db, get_cursor
from routes.auth import admin_required

ops_bot_bp = Blueprint("ops_bot", __name__)

# Breezeway context cache — avoids re-fetching on every chat turn
_bw_ctx_cache: dict = {}
_BW_CTX_TTL = 10 * 60  # 10 minutes


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


@ops_bot_bp.route("/admin/chatbot")
@login_required
@admin_required
def chatbot_page():
    return render_template("admin_chatbot.html")


@ops_bot_bp.route("/admin/chatbot/chat", methods=["POST"])
@login_required
@admin_required
def chatbot_chat():
    import anthropic
    from routes.briefing import (
        _fetch_todays_routes, _fetch_bw_reservations,
        _fetch_bw_endpoint, _get_breezeway_token, _classify_reservation,
        _get_property_name, _get_property_address, _extract_str,
        _get_live_property_cache, _get_live_ref_cache,
    )

    data     = request.get_json(force=True)
    messages = data.get("messages", [])
    dates    = data.get("dates", [])
    images   = data.get("images", [])

    if not messages:
        return jsonify({"error": "No message provided."}), 400

    ALLOWED_IMAGE_TYPES = {"image/jpeg", "image/png", "image/gif", "image/webp"}
    if images:
        safe_images = [
            img for img in images
            if isinstance(img, dict)
            and img.get("media_type") in ALLOWED_IMAGE_TYPES
            and isinstance(img.get("data"), str)
            and len(img["data"]) < 20 * 1024 * 1024
        ]
        if safe_images and messages and messages[-1].get("role") == "user":
            last = messages[-1]
            existing_content = last.get("content", "")
            if isinstance(existing_content, str):
                content_blocks = [{"type": "text", "text": existing_content}] if existing_content else []
            else:
                content_blocks = list(existing_content)
            image_blocks = [
                {"type": "image", "source": {"type": "base64",
                                             "media_type": img["media_type"],
                                             "data": img["data"]}}
                for img in safe_images
            ]
            messages[-1] = {"role": "user", "content": image_blocks + content_blocks}

    today_str  = datetime.utcnow().strftime("%Y-%m-%d")
    user_name  = current_user.name
    session_id = data.get("session_id", "")

    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        return jsonify({"error": "ANTHROPIC_API_KEY not configured."}), 500

    tools = [
        {
            "name": "fetch_reservation_data",
            "description": (
                "Fetch Breezeway reservation data (arrivals, departures, routes) for a date range. "
                "Use this whenever the user asks about dates not already in the loaded context — "
                "e.g. 'next week', 'this Friday', 'next month', 'June', 'this summer'. "
                "Resolve relative references using today's date before calling. "
                "Maximum range is 30 days per call."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "start_date": {"type": "string", "description": "Start date YYYY-MM-DD"},
                    "end_date":   {"type": "string", "description": "End date YYYY-MM-DD (inclusive, max 30 days after start)"},
                },
                "required": ["start_date", "end_date"],
            },
        },
        {
            "name": "fetch_task_data",
            "description": (
                "Fetch Breezeway task data for a SINGLE property. "
                "Use fetch_tasks_multi instead when asking about 2 or more properties at once — "
                "it fetches all of them in parallel and is much faster. "
                "Use this only when fetching exactly one property. "
                "Maximum date range is 30 days per call. "
                "Use status='housekeeping' for cleaning tasks, 'maintenance' for maintenance, 'inspection' for inspections."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "start_date":    {"type": "string", "description": "Start date YYYY-MM-DD"},
                    "end_date":      {"type": "string", "description": "End date YYYY-MM-DD (max 30 days after start)"},
                    "property_name": {"type": "string", "description": "Required: property name (partial match ok)."},
                    "status":        {"type": "string", "description": "Optional: 'housekeeping', 'maintenance', 'inspection', 'safety', 'complete', 'pending', or 'in_progress'."},
                },
                "required": ["start_date", "end_date", "property_name"],
            },
        },
        {
            "name": "fetch_tasks_multi",
            "description": (
                "Fetch Breezeway task data for MULTIPLE properties simultaneously (in parallel). "
                "ALWAYS use this instead of multiple fetch_task_data calls when the user asks about "
                "2 or more properties. It runs all fetches concurrently so results come back in seconds "
                "regardless of how many properties are requested. "
                "Maximum date range is 30 days per call."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "start_date":      {"type": "string", "description": "Start date YYYY-MM-DD"},
                    "end_date":        {"type": "string", "description": "End date YYYY-MM-DD (max 30 days after start)"},
                    "property_names":  {"type": "array", "items": {"type": "string"},
                                        "description": "List of property names to fetch tasks for simultaneously."},
                    "status":          {"type": "string", "description": "Optional: 'housekeeping', 'maintenance', 'inspection', 'safety', 'complete', 'pending', or 'in_progress'."},
                },
                "required": ["start_date", "end_date", "property_names"],
            },
        },
        {
            "name": "list_properties",
            "description": (
                "Return the full list of active property names from Breezeway. "
                "Call this whenever the user asks about ALL properties, or when you need "
                "property names to pass into fetch_task_data or fetch_tasks_multi."
            ),
            "input_schema": {"type": "object", "properties": {}},
        },
    ]

    def _execute_fetch(start_str, end_str):
        from datetime import date as _date2
        from collections import defaultdict
        try:
            s = _date2.fromisoformat(start_str)
            e = _date2.fromisoformat(end_str)
        except ValueError:
            return "Error: invalid date format. Use YYYY-MM-DD."
        if (e - s).days > 30:
            e = s + timedelta(days=30)
            end_str = e.isoformat()
        if e < s:
            return "Error: end_date must be on or after start_date."

        tok = _get_breezeway_token()
        if not tok:
            return "Breezeway not configured — cannot fetch reservation data."
        try:
            cis  = _fetch_bw_reservations(tok, {"checkin_date_ge":  start_str, "checkin_date_le":  end_str})
            cos  = _fetch_bw_reservations(tok, {"checkout_date_ge": start_str, "checkout_date_le": end_str})
        except Exception as ex:
            return f"Error fetching data: {ex}"

        ci_by = defaultdict(list)
        co_by = defaultdict(list)
        for r in cis:
            d = (r.get("checkin_date")  or "")[:10]
            if start_str <= d <= end_str:
                ci_by[d].append(r)
        for r in cos:
            d = (r.get("checkout_date") or "")[:10]
            if start_str <= d <= end_str:
                co_by[d].append(r)

        all_days = sorted(set(list(ci_by.keys()) + list(co_by.keys())))
        lines = [f"Data for {start_str} through {end_str}:"]
        if not all_days:
            lines.append("No arrivals or departures found in this period.")
        for d in all_days:
            lines.append(f"\n--- {d} ---")
            try:
                rts = _fetch_todays_routes(d)
                for r in rts:
                    stops = [x for x in json.loads(r["stops_json"] or "[]") if not x.get("isLunch")]
                    ln = f"  Route: \"{r['name']}\""
                    if r["assigned_to"]: ln += f" → {r['assigned_to']}"
                    ln += f" ({len(stops)} stops)"
                    lines.append(ln)
            except Exception:
                pass
            for r in ci_by.get(d, []):
                kind = _classify_reservation(r)
                prop = _get_property_name(r.get("property_id"))
                co_d = (r.get("checkout_date") or "")[:10]
                ci_d = (r.get("checkin_date")  or "")[:10]
                nights = ""
                if ci_d and co_d:
                    try:
                        n = (_date2.fromisoformat(co_d) - _date2.fromisoformat(ci_d)).days
                        nights = f", {n} nights"
                    except Exception:
                        pass
                lines.append(f"  ARRIVAL  [{kind.upper()}] {prop} (out {co_d}{nights})")
            for r in co_by.get(d, []):
                kind = _classify_reservation(r)
                prop = _get_property_name(r.get("property_id"))
                ci_d = (r.get("checkin_date")  or "")[:10]
                co_d = (r.get("checkout_date") or "")[:10]
                nights = ""
                if ci_d and co_d:
                    try:
                        n = (_date2.fromisoformat(co_d) - _date2.fromisoformat(ci_d)).days
                        nights = f", {n} nights"
                    except Exception:
                        pass
                lines.append(f"  DEPARTURE [{kind.upper()}] {prop} (in since {ci_d}{nights})")
        return "\n".join(lines)

    def _execute_fetch_tasks(start_str, end_str, property_name_filter=None, status_filter=None):
        from datetime import date as _date2
        import difflib
        try:
            s = _date2.fromisoformat(start_str)
            e = _date2.fromisoformat(end_str)
        except ValueError:
            return "Error: invalid date format. Use YYYY-MM-DD."
        if (e - s).days > 30:
            e = s + timedelta(days=30)
            end_str = e.isoformat()

        tok = _get_breezeway_token()
        if not tok:
            return "Breezeway not configured."

        params = {"scheduled_date": f"{start_str},{end_str}"}

        if not property_name_filter:
            return ("A property name is required to fetch task data — "
                    "the Breezeway task API does not support global queries. "
                    "Please ask the user which property they want to check.")

        _property_cache = _get_live_property_cache()
        name_lower = property_name_filter.lower().strip()
        rev = {v.lower(): k for k, v in _property_cache.items() if isinstance(v, str)}
        if name_lower in rev:
            pid = rev[name_lower]
            matched_prop_name = property_name_filter
        else:
            prefix_m  = [k for k in rev if k.startswith(name_lower)]
            substr_m  = [k for k in rev if name_lower in k]
            query_words = set(name_lower.split())
            word_m    = [k for k in rev if query_words and query_words.issubset(set(k.split()))]
            fuzzy_m   = (difflib.get_close_matches(name_lower, rev.keys(), n=3, cutoff=0.6) or
                         difflib.get_close_matches(name_lower, rev.keys(), n=3, cutoff=0.4))
            reverse_m = [k for k in rev if len(k) > 4 and k in name_lower]
            matches = prefix_m or substr_m or word_m or fuzzy_m or reverse_m
            if len(matches) > 1:
                matches = sorted(matches, key=len)
            pid = rev[matches[0]] if matches else None
            matched_prop_name = _property_cache.get(pid, property_name_filter) if pid else None

        if not pid:
            cache_size = len(_property_cache)
            if cache_size == 0:
                return (f"Property cache is empty — Breezeway property list could not be loaded. "
                        f"Cannot look up tasks for '{property_name_filter}'.")
            candidates = difflib.get_close_matches(name_lower, rev.keys(), n=5, cutoff=0.3)
            candidate_str = (", ".join(f'"{_property_cache[rev[c]]}"' for c in candidates)
                             if candidates else "none found")
            all_names_sample = sorted(_property_cache.values())[:30]
            return (f"Could not find a property matching '{property_name_filter}' "
                    f"({cache_size} properties in cache). "
                    f"Closest matches: {candidate_str}. "
                    f"All cached property names: {all_names_sample}. "
                    f"Retry using the exact name from that list.")

        matched_prop_name = matched_prop_name or property_name_filter

        dept_map = {"housekeeping": "housekeeping", "cleaning": "housekeeping",
                    "maintenance": "maintenance", "inspection": "inspection", "safety": "safety"}
        dept_filter = dept_map.get((status_filter or "").lower())
        if dept_filter:
            params["type_department"] = dept_filter

        ref_id = _get_live_ref_cache().get(pid)
        prop_params_to_try = []
        if ref_id:
            prop_params_to_try.append(("reference_property_id", ref_id))
        prop_params_to_try.extend([("property_id", pid), ("home_id", pid)])

        tasks, error = [], "property not found"
        for prop_key, prop_val in prop_params_to_try:
            t, e, status_code = _fetch_bw_endpoint(tok, "/public/inventory/v1/task/", {**params, prop_key: prop_val})
            if status_code == 200:
                tasks, error = t, ""
                break
            if "403" in e or "access" in e.lower():
                return "Task data requires elevated API access on your Breezeway plan."
            error = e

        if error and not tasks:
            return f"Could not fetch tasks: {error}"

        def _task_status(t):
            for key in ("type_task_status", "status", "state"):
                v = t.get(key)
                if v is None:
                    continue
                if isinstance(v, str):
                    return v.lower()
                if isinstance(v, dict):
                    s = v.get("value") or v.get("name") or v.get("label") or ""
                    if s:
                        return str(s).lower()
            return "unknown"

        if status_filter and not dept_filter:
            tasks = [t for t in tasks if _task_status(t) == status_filter.lower()]

        if not tasks:
            prop_label   = f" at {matched_prop_name or property_name_filter}" if property_name_filter else ""
            status_label = f" ({status_filter})" if status_filter else ""
            return f"No tasks found{prop_label}{status_label} between {start_str} and {end_str}."

        lines = [f"Tasks for {start_str} through {end_str}"]
        if matched_prop_name or property_name_filter:
            lines[0] += f" — {matched_prop_name or property_name_filter}"
        lines.append(f"({len(tasks)} task{'s' if len(tasks) != 1 else ''} found)\n")

        by_status = {}
        for t in tasks:
            st = _task_status(t)
            by_status.setdefault(st, []).append(t)

        status_order = ["complete", "in_progress", "pending", "blocked", "cancelled", "unknown"]
        for st in status_order + [k for k in by_status if k not in status_order]:
            group = by_status.get(st)
            if not group:
                continue
            lines.append(f"── {st.upper()} ({len(group)}) ──")
            for t in group:
                def _sf(v):
                    if isinstance(v, str): return v
                    if isinstance(v, dict): return v.get("value") or v.get("name") or v.get("label") or ""
                    return ""
                title    = (_sf(t.get("title")) or _sf(t.get("name")) or _sf(t.get("type_department")) or "Untitled")
                dept     = _sf(t.get("type_department"))
                home_id  = t.get("home_id") or t.get("property_id")
                prop_name = _get_property_name(home_id) if home_id else (t.get("property_name") or "")
                raw_assignments = t.get("assignments") or []
                if isinstance(raw_assignments, list) and raw_assignments:
                    names = []
                    for a in raw_assignments:
                        if isinstance(a, dict):
                            n = (a.get("name") or a.get("full_name") or
                                 (a.get("first_name", "") + " " + a.get("last_name", "")).strip())
                            if n:
                                names.append(n)
                        elif a:
                            names.append(str(a))
                    assignee = ", ".join(names)
                else:
                    assignee = ""

                def _fmt_dt(raw):
                    if not raw:
                        return ""
                    s = str(raw)
                    date_part = s[:10]
                    time_part = ""
                    if len(s) > 10:
                        t_raw = s[11:16]
                        if t_raw:
                            try:
                                h, m = int(t_raw[:2]), int(t_raw[3:5])
                                suffix = "AM" if h < 12 else "PM"
                                h12 = h % 12 or 12
                                time_part = f" {h12}:{m:02d} {suffix}"
                            except Exception:
                                time_part = f" {t_raw}"
                    return date_part + time_part

                sched_date = t.get("scheduled_date") or ""
                sched_time = t.get("scheduled_time") or ""
                if sched_date and sched_time:
                    sched = _fmt_dt(f"{sched_date}T{sched_time}")
                else:
                    sched = _fmt_dt(sched_date or t.get("start_time") or t.get("scheduled_start") or "")
                finished = _fmt_dt(t.get("finished_at") or t.get("completed_at") or "")
                notes    = (t.get("notes") or t.get("description") or "")[:120]

                line = f"  • {title}"
                if dept:       line += f" [{dept}]"
                if prop_name:  line += f" — {prop_name}"
                if sched:      line += f" | scheduled {sched}"
                if finished:   line += f" | done {finished}"
                if assignee:   line += f" | assigned: {assignee}"
                if notes:      line += f"\n    {notes}"
                lines.append(line)
            lines.append("")

        return "\n".join(lines)

    def _execute_fetch_tasks_multi(start_str, end_str, property_names, status_filter=None):
        from concurrent.futures import ThreadPoolExecutor, as_completed
        if not property_names:
            return "No property names provided."
        results = {}
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {
                executor.submit(_execute_fetch_tasks, start_str, end_str, name, status_filter): name
                for name in property_names
            }
            for future in as_completed(futures):
                name = futures[future]
                try:
                    results[name] = future.result()
                except Exception as ex:
                    results[name] = f"Error fetching tasks: {ex}"
        return "\n\n".join(
            f"=== {n} ===\n{results.get(n, 'No data returned.')}" for n in property_names
        )

    def generate():
        def sse(obj):
            return f"data: {json.dumps(obj)}\n\n"

        yield sse({"type": "status", "text": "Loading schedule data…"})

        capped_dates = sorted(dates if dates else [today_str])[:7]
        min_date     = capped_dates[0]
        max_date     = capped_dates[-1]
        date_set     = set(capped_dates)

        cache_key = f"{min_date}:{max_date}"
        cached    = _bw_ctx_cache.get(cache_key)
        if cached and _time.time() - cached[0] < _BW_CTX_TTL:
            all_checkins, all_checkouts = cached[1], cached[2]
        else:
            try:
                token = _get_breezeway_token()
                if token:
                    all_checkins  = _fetch_bw_reservations(token, {
                        "checkin_date_ge": min_date, "checkin_date_le": max_date,
                    })
                    all_checkouts = _fetch_bw_reservations(token, {
                        "checkout_date_ge": min_date, "checkout_date_le": max_date,
                    })
                else:
                    all_checkins = all_checkouts = []
            except Exception:
                all_checkins = all_checkouts = []
            _bw_ctx_cache[cache_key] = (_time.time(), all_checkins, all_checkouts)

        checkins_by_date  = {}
        checkouts_by_date = {}
        for r in all_checkins:
            d = (r.get("checkin_date") or "")[:10]
            if d in date_set:
                checkins_by_date.setdefault(d, []).append(r)
        for r in all_checkouts:
            d = (r.get("checkout_date") or "")[:10]
            if d in date_set:
                checkouts_by_date.setdefault(d, []).append(r)

        context_blocks  = []
        context_summary = []
        for date_str in capped_dates:
            try:
                routes    = _fetch_todays_routes(date_str)
                checkins  = checkins_by_date.get(date_str, [])
                checkouts = checkouts_by_date.get(date_str, [])
                block = [f"\n=== {date_str} ==="]
                if routes:
                    block.append(f"Saved routes ({len(routes)}):")
                    for r in routes:
                        stops = [s for s in json.loads(r["stops_json"] or "[]") if not s.get("isLunch")]
                        line  = f"  - \"{r['name']}\""
                        if r["assigned_to"]: line += f" → {r['assigned_to']}"
                        line += f": {len(stops)} stop{'s' if len(stops) != 1 else ''}"
                        if (r.get("notes") or "").strip(): line += f". Notes: {r['notes'].strip()}"
                        block.append(line)
                else:
                    block.append("No routes saved for this date.")
                if checkins:
                    block.append(f"Arrivals ({len(checkins)}):")
                    for r in checkins:
                        kind     = _classify_reservation(r)
                        pid      = r.get("property_id")
                        prop     = _get_property_name(pid)
                        addr     = _get_property_address(pid)
                        t        = r.get("checkin_time", "")
                        checkout = (r.get("checkout_date") or "")[:10]
                        checkin  = (r.get("checkin_date")  or "")[:10]
                        tag_names = [_extract_str(tg) for tg in (r.get("tags") or [])]
                        nights = ""
                        if checkin and checkout:
                            try:
                                from datetime import date as _date
                                n = (_date.fromisoformat(checkout) - _date.fromisoformat(checkin)).days
                                nights = f", {n} nights"
                            except Exception:
                                pass
                        line = f"  - [{kind.upper()}] {prop}" + (f" — {addr}" if addr else "")
                        line += f" (checkin {checkin}, checkout {checkout}{nights})"
                        if t:         line += f" at {t[:5]}"
                        if tag_names: line += f" [tags: {', '.join(tag_names)}]"
                        block.append(line)
                else:
                    block.append("No arrivals this date.")
                if checkouts:
                    block.append(f"Departures ({len(checkouts)}):")
                    for r in checkouts:
                        kind     = _classify_reservation(r)
                        pid      = r.get("property_id")
                        prop     = _get_property_name(pid)
                        addr     = _get_property_address(pid)
                        t        = r.get("checkout_time", "")
                        checkout = (r.get("checkout_date") or "")[:10]
                        checkin  = (r.get("checkin_date")  or "")[:10]
                        tag_names = [_extract_str(tg) for tg in (r.get("tags") or [])]
                        nights = ""
                        if checkin and checkout:
                            try:
                                from datetime import date as _date
                                n = (_date.fromisoformat(checkout) - _date.fromisoformat(checkin)).days
                                nights = f", {n} nights"
                            except Exception:
                                pass
                        line = f"  - [{kind.upper()}] {prop}" + (f" — {addr}" if addr else "")
                        line += f" (checkin {checkin}, checkout {checkout}{nights})"
                        if t:         line += f" by {t[:5]}"
                        if tag_names: line += f" [tags: {', '.join(tag_names)}]"
                        block.append(line)
                else:
                    block.append("No departures this date.")
                context_blocks.append("\n".join(block))
                context_summary.append({"date": date_str, "routes": len(routes),
                                        "arrivals": len(checkins), "departures": len(checkouts)})
            except Exception as e:
                context_blocks.append(f"\n=== {date_str} ===\nData load error: {e}")
                context_summary.append({"date": date_str, "error": str(e)})

        try:
            conn = get_db(); cur = get_cursor(conn)
            cur.execute("SELECT title, category, body FROM chatbot_knowledge WHERE is_active = 1 ORDER BY category, title")
            knowledge_rows = cur.fetchall()
            cur.close(); conn.rollback(); conn.close()
        except Exception:
            knowledge_rows = []

        if knowledge_rows:
            kb_lines = ["=== COMPANY KNOWLEDGE BASE ===",
                        "The following policies and SOPs are from Tahoe Getaways. "
                        "Use them to answer questions accurately.\n"]
            for row in knowledge_rows:
                kb_lines.append(f"[{row['category']}: {row['title']}]")
                kb_lines.append(row["body"].strip())
                kb_lines.append("")
            knowledge_section = "\n".join(kb_lines) + "\n\n"
        else:
            knowledge_section = ""

        system_prompt = (
            f"You are the TG Operations Bot for Tahoe Getaways, a vacation rental company in Lake Tahoe. "
            f"You are talking to {user_name}. Today's date is {today_str}.\n\n"
            "HOW TO ANSWER:\n"
            "- For questions about specific properties, reservations, schedules, or company SOPs: "
            "use the knowledge base and loaded Breezeway data below as your primary source.\n"
            "- For general property management, hospitality, or operations questions: use your own knowledge "
            "to give a helpful, practical answer — you don't need to restrict yourself to the provided context.\n"
            "- If asked about a specific property or reservation that isn't in the loaded data, say so clearly "
            "and suggest the staff member check Breezeway or Streamline directly.\n"
            "- Be concise and direct. Use bullet points for multi-part answers.\n"
            "- When staff asks you to take a write action (save a note, flag a property, mark something complete), "
            "respond with a line starting exactly with 'CONFIRM_ACTION:' followed by a short description. "
            "Do not consider the action done until confirmed.\n"
            "- SCOPE BEFORE FETCHING: Before calling any fetch tool for a date range longer than 7 days, "
            "confirm the exact range with the user unless they stated it explicitly. "
            "For ranges ≤7 days or when the user named specific dates, fetch immediately without asking.\n"
            "- PROPERTY SCOPE: If the user asks about tasks at all properties or doesn't name one, "
            "call list_properties first to get the full list, then use fetch_tasks_multi — "
            "do NOT ask the user to name the properties.\n"
            "- TOOL USAGE: When the user asks about tasks at 2+ properties, ALWAYS use fetch_tasks_multi "
            "(not multiple fetch_task_data calls) — it fetches all properties in parallel in one shot.\n"
            "- TASK FILTERING — CRITICAL: This is an OPERATIONS bot, not a housekeeping bot. "
            "NEVER pass status='housekeeping' to any fetch tool and NEVER show housekeeping tasks "
            "(deep cleans, departure cleans, mid-stays, linen changes, turnovers) unless the user "
            "explicitly asks about cleaning or housekeeping. "
            "When the user asks about tasks without specifying a type, default to status='maintenance'. "
            "Carpet cleans, inspections, repairs, and safety checks are maintenance — always use "
            "status='maintenance' for those. If results still contain housekeeping entries, skip them.\n"
            "- TOOL ACCURACY: fetch_task_data and fetch_tasks_multi return ALL tasks from Breezeway for the given property "
            "and date range. Filter out housekeeping before presenting results (see above). "
            "If tasks are missing after filtering, say so clearly. "
            "Always report full task title, scheduled date/time, status, and assignee for every task shown. "
            "If a field is blank in the data, say 'not listed' rather than claiming the API can't provide it.\n\n"
            + knowledge_section
            + "RESERVATION TYPES:\n"
            "  GUEST = paying guest stay\n"
            "  OWNER = owner stay or owner-booked reservation\n"
            "  LEASE = paying guest stay of 30+ days (long-term rental — still a paying guest, not an owner)\n"
            "  BLOCK = maintenance hold or owner block — no guests, property unavailable\n\n"
            "POST RENTAL INSPECTION (PRI):\n"
            "Required when a short-term GUEST (<30 days) checks out AND the next reservation at that "
            "property is OWNER or BLOCK. Also required if no upcoming reservation within 60 days (vacancy PRI).\n"
            "Flagged in Breezeway by adding 'owner next' tag to the incoming OWNER/BLOCK booking.\n\n"
            "LOADED DATA (routes + arrivals + departures for selected dates):\n"
            + "\n".join(context_blocks)
            + "\n\nYou also have a tool — fetch_reservation_data — to look up Breezeway data "
            "for any other date range the user asks about."
        )

        def _trunc_for_history(content, limit=800):
            if not isinstance(content, str) or len(content) <= limit:
                return content
            cut = content[:limit].rfind('\n')
            if cut < limit // 2:
                cut = limit
            return content[:cut] + "\n[…truncated — bot will re-fetch if needed]"

        ai_client         = anthropic.Anthropic(api_key=key)
        trimmed           = _safe_trim(messages, 12)
        history_additions = []
        reply_text        = ""

        try:
            for _turn in range(6):
                turn_text    = ""
                asst_content = []

                with ai_client.messages.stream(
                    model      = "claude-haiku-4-5-20251001",
                    max_tokens = 1500,
                    system     = system_prompt,
                    messages   = trimmed,
                    tools      = tools,
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
                        else:
                            asst_content.append({"type": b.type})
                    trimmed.append({"role": "assistant", "content": asst_content})
                    history_additions.append({"role": "assistant", "content": asst_content})

                    tool_results         = []
                    tool_results_history = []
                    tool_blocks          = [b for b in final_msg.content if b.type == "tool_use"]
                    tool_total           = len(tool_blocks)
                    tool_idx             = 0
                    for block in final_msg.content:
                        if block.type == "tool_use":
                            tool_idx += 1
                            counter  = f" ({tool_idx}/{tool_total})" if tool_total > 1 else ""
                            if block.name == "fetch_reservation_data":
                                yield sse({"type": "status", "text": f"Fetching reservation data{counter}…"})
                                result = _execute_fetch(
                                    block.input.get("start_date", ""),
                                    block.input.get("end_date",   ""),
                                )
                            elif block.name == "fetch_task_data":
                                prop = block.input.get("property_name") or ""
                                prop_label = f" for {prop}" if prop else ""
                                yield sse({"type": "status", "text": f"Fetching tasks{prop_label}{counter}…"})
                                result = _execute_fetch_tasks(
                                    block.input.get("start_date", ""),
                                    block.input.get("end_date",   ""),
                                    prop or None,
                                    block.input.get("status"),
                                )
                            elif block.name == "fetch_tasks_multi":
                                names = block.input.get("property_names") or []
                                n = len(names)
                                yield sse({"type": "status", "text": f"Fetching tasks for {n} properties simultaneously…"})
                                result = _execute_fetch_tasks_multi(
                                    block.input.get("start_date", ""),
                                    block.input.get("end_date",   ""),
                                    names,
                                    block.input.get("status"),
                                )
                            elif block.name == "list_properties":
                                yield sse({"type": "status", "text": "Loading property list from Breezeway…"})
                                from routes.briefing import _ensure_property_cache, _get_live_property_cache
                                _ensure_property_cache()
                                cache = _get_live_property_cache()
                                if cache:
                                    names_list = sorted(cache.values())
                                    result = f"{len(names_list)} active properties:\n" + "\n".join(f"- {n}" for n in names_list)
                                else:
                                    result = "Property list unavailable — Breezeway may not be configured."
                            else:
                                result = f"Unknown tool: {block.name}"
                            tool_results.append({
                                "type":        "tool_result",
                                "tool_use_id": block.id,
                                "content":     result,
                            })
                            tool_results_history.append({
                                "type":        "tool_result",
                                "tool_use_id": block.id,
                                "content":     _trunc_for_history(result),
                            })
                    trimmed.append({"role": "user", "content": tool_results})
                    history_additions.append({"role": "user", "content": tool_results_history})
                else:
                    break

            try:
                user_msg = messages[-1]["content"] if messages else ""
                conn_log = get_db()
                cur_log  = get_cursor(conn_log)
                cur_log.execute(
                    "INSERT INTO bot_interactions "
                    "(user_id, session_id, query, response, dates_loaded, created_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (current_user.id, data.get("session_id", ""),
                     user_msg, reply_text, json.dumps(dates),
                     datetime.utcnow().isoformat()),
                )
                conn_log.commit()
                cur_log.close(); conn_log.close()
            except Exception:
                pass

            yield sse({
                "type":              "done",
                "history_additions": history_additions,
                "context_summary":   context_summary,
                "kb_count":          len(knowledge_rows),
            })
        except Exception as e:
            yield sse({"type": "error", "text": str(e)})

    return Response(
        stream_with_context(generate()),
        mimetype = "text/event-stream",
        headers  = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@ops_bot_bp.route("/admin/chatbot/session/save", methods=["POST"])
@login_required
@admin_required
def chatbot_session_save():
    data       = request.get_json(force=True)
    session_id = (data.get("session_id") or "").strip()
    messages   = data.get("messages", [])
    if not session_id or not messages:
        return jsonify({"error": "session_id and messages required"}), 400

    def _strip_images(msgs):
        out = []
        for m in msgs:
            content = m.get("content")
            if isinstance(content, list):
                stripped = []
                for block in content:
                    if isinstance(block, dict) and block.get("type") == "image":
                        stripped.append({"type": "image", "source": {"type": "placeholder"}})
                    else:
                        stripped.append(block)
                out.append({**m, "content": stripped})
            else:
                out.append(m)
        return out

    safe_messages = _strip_images(messages)
    title = ""
    for m in safe_messages:
        if m.get("role") == "user":
            c = m.get("content")
            if isinstance(c, str):
                title = c[:80]
            elif isinstance(c, list):
                for b in c:
                    if isinstance(b, dict) and b.get("type") == "text":
                        title = (b.get("text") or "")[:80]
                        break
            if title:
                break

    now = datetime.utcnow().isoformat()
    conn = get_db(); cur = get_cursor(conn)
    try:
        cur.execute("""
            INSERT INTO chatbot_sessions (user_id, session_id, title, messages_json, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (session_id) DO UPDATE
              SET messages_json = EXCLUDED.messages_json,
                  title         = EXCLUDED.title,
                  updated_at    = EXCLUDED.updated_at
        """, (current_user.id, session_id, title, json.dumps(safe_messages), now, now))
        conn.commit()
    finally:
        conn.rollback(); cur.close(); conn.close()
    return jsonify({"success": True})


@ops_bot_bp.route("/admin/chatbot/sessions", methods=["GET"])
@login_required
@admin_required
def chatbot_sessions_list():
    conn = get_db(); cur = get_cursor(conn)
    try:
        cur.execute("""
            SELECT session_id, title, updated_at
            FROM chatbot_sessions
            WHERE user_id = %s
            ORDER BY updated_at DESC
            LIMIT 30
        """, (current_user.id,))
        rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.rollback(); cur.close(); conn.close()
    return jsonify({"sessions": rows})


@ops_bot_bp.route("/admin/chatbot/session/<session_id>", methods=["GET"])
@login_required
@admin_required
def chatbot_session_load(session_id):
    conn = get_db(); cur = get_cursor(conn)
    try:
        cur.execute("""
            SELECT messages_json FROM chatbot_sessions
            WHERE session_id = %s AND user_id = %s
        """, (session_id, current_user.id))
        row = cur.fetchone()
    finally:
        conn.rollback(); cur.close(); conn.close()
    if not row:
        return jsonify({"error": "Not found"}), 404
    try:
        messages = json.loads(row["messages_json"])
    except Exception:
        messages = []
    return jsonify({"messages": messages})


@ops_bot_bp.route("/admin/chatbot/session/<session_id>", methods=["DELETE"])
@login_required
@admin_required
def chatbot_session_delete(session_id):
    conn = get_db(); cur = get_cursor(conn)
    try:
        cur.execute("DELETE FROM chatbot_sessions WHERE session_id = %s AND user_id = %s",
                    (session_id, current_user.id))
        conn.commit()
    finally:
        conn.rollback(); cur.close(); conn.close()
    return jsonify({"success": True})


@ops_bot_bp.route("/admin/chatbot/save-flag", methods=["POST"])
@login_required
@admin_required
def chatbot_save_flag():
    data        = request.get_json(force=True)
    description = (data.get("description") or "").strip()
    date_str    = (data.get("date") or "").strip()
    if not description or not date_str:
        return jsonify({"error": "description and date required"}), 400

    note_line = f"[Bot flag — {current_user.name}: {description}]"
    now       = datetime.utcnow().isoformat()
    conn      = get_db()
    cur       = get_cursor(conn)
    cur.execute("SELECT note_text FROM briefing_notes WHERE note_date = %s", (date_str,))
    row = cur.fetchone()
    if row:
        new_text = (row["note_text"] or "").rstrip() + "\n" + note_line
        cur.execute(
            "UPDATE briefing_notes SET note_text=%s, updated_by=%s, updated_at=%s WHERE note_date=%s",
            (new_text, current_user.id, now, date_str),
        )
    else:
        cur.execute(
            "INSERT INTO briefing_notes (note_date, note_text, updated_by, updated_at) VALUES (%s,%s,%s,%s)",
            (date_str, note_line, current_user.id, now),
        )
    conn.commit()
    cur.close(); conn.close()

    try:
        conn_log = get_db()
        cur_log  = get_cursor(conn_log)
        cur_log.execute(
            "UPDATE bot_interactions SET action_taken=%s "
            "WHERE id = (SELECT id FROM bot_interactions WHERE user_id=%s ORDER BY id DESC LIMIT 1)",
            (f"Saved flag: {description}", current_user.id),
        )
        conn_log.commit()
        cur_log.close(); conn_log.close()
    except Exception:
        pass

    return jsonify({"success": True})
