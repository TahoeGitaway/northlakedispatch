"""
routes/bw_probe.py — Breezeway API capability probe (admin-only, READ-ONLY).

Answers one question: can Breezeway return tasks for a DATE across the whole
company in a single query, or does it really require one call per property?

Every full-day scan in this app fans out ~442 per-property calls, which is what
trips the rate limiter (HTTP 429). If a company-wide task query works on this
plan, those 442 calls collapse to one paginated query and the throttling
problem disappears rather than needing to be paced around.

Also reports any rate-limit headers Breezeway returns, so the real request
budget stops being guesswork.

Standalone blueprint. Makes a small, bounded number of GETs (one page each,
never paginating) so running it cannot itself worsen throttling. Writes nothing.
"""

import time

import requests
from flask import Blueprint, jsonify, request
from flask_login import login_required

from routes.auth import admin_required

bw_probe_bp = Blueprint("bw_probe", __name__)

# Task endpoint paths seen across this codebase, most likely first.
_CANDIDATE_PATHS = [
    "/public/inventory/v1/task",
    "/public/work/v2/task",
    "/public/work/v1/task",
    "/public/v1/task",
]

# Date-filter conventions to try. Breezeway's docs and this codebase disagree
# about which is correct, so try each and let the API decide.
def _date_param_sets(d: str) -> list:
    return [
        {"label": "scheduled_date=D,D",             "params": {"scheduled_date": f"{d},{d}"}},
        {"label": "start_date/end_date",            "params": {"start_date": d, "end_date": d}},
        {"label": "scheduled_date_from/_to",        "params": {"scheduled_date_from": d, "scheduled_date_to": d}},
        {"label": "scheduled_date=D",               "params": {"scheduled_date": d}},
        {"label": "date=D",                         "params": {"date": d}},
    ]

# Headers worth surfacing — different gateways use different names, so match loosely.
_RATE_HEADER_HINTS = ("ratelimit", "rate-limit", "retry-after", "x-rate", "throttle")


def _interesting_headers(h) -> dict:
    return {k: v for k, v in h.items()
            if any(hint in k.lower() for hint in _RATE_HEADER_HINTS)}


def _probe_once(token: str, path: str, params: dict, timeout: float = 20.0) -> dict:
    """ONE request, ONE page. Never paginates — this is a capability check, not a
    data pull, and it must not add meaningful load to an already-throttled API."""
    t0 = time.time()
    try:
        resp = requests.get(
            f"https://api.breezeway.io{path}",
            headers={"Authorization": f"JWT {token}"},
            params={**params, "limit": 100, "page": 1},
            timeout=timeout,
        )
    except requests.exceptions.Timeout:
        return {"status": None, "error": "timed out after 20 s", "elapsed_s": round(time.time() - t0, 2)}
    except Exception as ex:
        return {"status": None, "error": str(ex), "elapsed_s": round(time.time() - t0, 2)}

    out = {
        "status":    resp.status_code,
        "elapsed_s": round(time.time() - t0, 2),
        "rate_limit_headers": _interesting_headers(resp.headers),
    }
    if not resp.ok:
        try:
            out["error"] = resp.json()
        except Exception:
            out["error"] = resp.text[:300]
        return out

    try:
        data = resp.json()
    except Exception:
        out["error"] = "200 but body was not JSON"
        return out

    rows = (data.get("results", data.get("data", data.get("tasks", []))) or []) \
           if isinstance(data, dict) else (data or [])
    # The decisive signal: a company-wide query returns tasks spanning MANY
    # properties. One property's worth of rows means the filter was ignored or
    # silently scoped, which would make this useless as a replacement.
    home_ids = {str(r.get("home_id") or r.get("property_id")) for r in rows
                if isinstance(r, dict) and (r.get("home_id") or r.get("property_id"))}
    out.update({
        "rows_first_page":     len(rows),
        "distinct_properties": len(home_ids),
        "looks_company_wide":  len(home_ids) > 1,
        "total_hint":          (data.get("count") or data.get("total")) if isinstance(data, dict) else None,
        "sample_dates":        sorted({(r.get("scheduled_date") or "")[:10] for r in rows
                                       if isinstance(r, dict) and r.get("scheduled_date")})[:5],
    })
    return out


def _wait_for_clear(token: str, day: str, budget_s: float = 10.0) -> dict:
    """Breezeway exposes NO rate-limit headers, so the only way to learn how long
    the limiter stays hot is to measure it. Poll one cheap request until it stops
    returning 429, and report how long that took.

    Budget is deliberately small: this app already loses long requests to a gateway
    timeout ("upstream error"), so the whole pre-flight — wait plus the final
    request — must stay well inside it. A 429 comes back fast, so a short per-call
    timeout costs nothing here."""
    t0, waited, tries = time.time(), [], 0
    while time.time() - t0 < budget_s:
        tries += 1
        res = _probe_once(token, _CANDIDATE_PATHS[0], {"scheduled_date": f"{day},{day}"},
                          timeout=6.0)
        if res.get("status") != 429:
            return {"cleared": True, "seconds_until_clear": round(time.time() - t0, 1),
                    "polls": tries, "first_ok_result": res}
        nap = min(4.0, 1.5 * tries)          # gentle ramp; never hammer a hot limiter
        waited.append(nap)
        time.sleep(nap)
    return {"cleared": False, "waited_s": round(time.time() - t0, 1), "polls": tries,
            "note": "Still 429 after the wait budget — the limiter stays hot for "
                    "longer than this request can safely block. Retry when the app "
                    "has been idle for a few minutes."}


@bw_probe_bp.route("/admin/bw-probe")
@login_required
@admin_required
def bw_probe():
    """Probe whether a company-wide task-by-date query is supported.

    GET /admin/bw-probe?date=YYYY-MM-DD   (defaults to today)
    Returns JSON. Read-only; makes at most len(paths) * len(param_sets) requests,
    one page each, plus a short pre-flight wait if the limiter is currently hot.
    """
    from routes.briefing import _get_breezeway_token
    from datetime import date as _date

    token = _get_breezeway_token()
    if not token:
        return jsonify({"error": "Breezeway is not configured (no token)."}), 502

    day = (request.args.get("date") or _date.today().isoformat())[:10]

    # Pre-flight: a probe run started while throttled reports a false "unsupported",
    # so wait for a clear window first — and record how long that took, since with
    # no rate-limit headers that recovery time is the only signal we have.
    preflight = _wait_for_clear(token, day)
    if not preflight.get("cleared"):
        return jsonify({
            "date": day,
            "verdict": {"supported": None,
                        "reason": "Breezeway stayed rate-limited for the whole pre-flight "
                                  "wait, so the capability question is still unanswered."},
            "preflight": preflight,
            "rate_limit_headers_seen": {},
            "attempts": [],
        })

    attempts = []
    verdict  = None
    for path in _CANDIDATE_PATHS:
        for ps in _date_param_sets(day):
            res = _probe_once(token, path, ps["params"])
            attempts.append({"path": path, "convention": ps["label"], **res})
            # First result that returns many properties' tasks answers the question.
            if res.get("looks_company_wide"):
                verdict = {
                    "supported": True,
                    "path": path,
                    "convention": ps["label"],
                    "params": ps["params"],
                    "rows_first_page": res.get("rows_first_page"),
                    "distinct_properties": res.get("distinct_properties"),
                }
                break
            # A 429 mid-probe means the API is currently throttled — stop rather
            # than adding load, and say so plainly instead of reporting a false no.
            if res.get("status") == 429:
                verdict = {"supported": None,
                           "reason": "Breezeway returned 429 during the probe — "
                                     "currently rate-limited, so this run is inconclusive. "
                                     "Retry in a few minutes."}
                break
        if verdict:
            break

    if verdict is None:
        verdict = {"supported": False,
                   "reason": "No path/convention returned tasks spanning multiple properties. "
                             "The per-property fan-out appears genuinely required."}

    # Surface every rate-limit header seen, whatever the verdict — knowing the real
    # budget is useful even if the bulk query is unsupported.
    seen_headers = {}
    for a in attempts:
        seen_headers.update(a.get("rate_limit_headers") or {})

    return jsonify({
        "date": day,
        "verdict": verdict,
        "preflight": preflight,
        # Empty here is itself a finding: Breezeway advertises no budget, so any
        # pacing has to be derived by measurement rather than read off a header.
        "rate_limit_headers_seen": seen_headers,
        "attempts": attempts,
    })


@bw_probe_bp.route("/admin/bw-gate")
@login_required
@admin_required
def bw_gate_status():
    """Live state of the process-wide Breezeway rate gate.

    GET /admin/bw-gate          — current pacing, cooldown and counters
    GET /admin/bw-gate?reset=1  — clear learned state (diagnostics only)

    Breezeway publishes no rate-limit headers, so the gate learns the budget by
    watching 429s. This makes what it learned visible instead of guesswork.
    """
    from routes.bw_ratelimit import gate

    if request.args.get("reset"):
        gate.reset()
        return jsonify({"reset": True, "gate": gate.snapshot()})

    snap = gate.snapshot()
    c    = snap["counters"]
    total = c["ok"] + c["throttled_429"]
    snap["throttle_rate"] = f"{(100.0 * c['throttled_429'] / total):.1f}%" if total else "n/a"
    snap["reading"] = (
        "interval_s 0 means the gate has seen no throttling and is not pacing. "
        "A rising interval_s means it is backing off; a high gave_up_waiting count "
        "means requests are being refused locally to protect the gateway timeout."
    )
    return jsonify(snap)


# ── Batching probe ────────────────────────────────────────────────
# The capability probe showed reference_property_id is mandatory, so a
# company-wide query is out. But it also showed scheduled_date must be a
# COMMA-SEPARATED PAIR ("Length must be 2"), i.e. this API does use delimited
# multi-value params. If reference_property_id accepts a list too, 442 calls
# become ~9 at 50 ids each — which solves the rate limiting outright rather
# than pacing around it. That is worth ruling in or out before building a
# throttle.

def _batch_shapes(ref_ids: list, day: str) -> list:
    """Ways an API might accept several property ids in one call. requests encodes
    a list value as repeated ?k=v1&k=v2, which is the other common convention."""
    csv_ids = ",".join(ref_ids)
    return [
        {"label": "reference_property_id=a,b,c (comma)",   "params": {"reference_property_id": csv_ids}},
        {"label": "reference_property_id repeated",        "params": {"reference_property_id": ref_ids}},
        {"label": "reference_property_ids=a,b,c (plural)", "params": {"reference_property_ids": csv_ids}},
        {"label": "property_id=a,b,c",                     "params": {"property_id": csv_ids}},
        {"label": "home_id=a,b,c",                         "params": {"home_id": csv_ids}},
    ]


@bw_probe_bp.route("/admin/bw-probe-batch")
@login_required
@admin_required
def bw_probe_batch():
    """Can one call cover several properties?

    GET /admin/bw-probe-batch?date=YYYY-MM-DD&n=5
    Read-only. Sends at most len(shapes) + 1 requests, one page each.
    """
    from routes.briefing import (_get_breezeway_token, _ensure_property_cache,
                                 _get_live_ref_cache)
    from datetime import date as _date

    token = _get_breezeway_token()
    if not token:
        return jsonify({"error": "Breezeway is not configured (no token)."}), 502

    day = (request.args.get("date") or _date.today().isoformat())[:10]
    try:
        n = max(2, min(20, int(request.args.get("n") or 5)))
    except (TypeError, ValueError):
        n = 5

    _ensure_property_cache()
    ref_ids = [r for r in (_get_live_ref_cache() or {}).values() if r][:n]
    if len(ref_ids) < 2:
        return jsonify({"error": "Need at least 2 reference property ids in the cache."}), 502

    date_params = {"scheduled_date": f"{day},{day}"}

    # Baseline: ONE property, so we can tell "batching worked" from "the filter was
    # ignored and it returned everything" — both look like a big number otherwise.
    base = _probe_once(token, _CANDIDATE_PATHS[0],
                       {**date_params, "reference_property_id": ref_ids[0]}, timeout=15.0)

    results, winner = [], None
    for shape in _batch_shapes(ref_ids, day):
        res = _probe_once(token, _CANDIDATE_PATHS[0], {**date_params, **shape["params"]},
                          timeout=15.0)
        entry = {"shape": shape["label"], **res}
        # Success = accepted AND covering more than the one baseline property.
        # A 200 alone proves nothing: an ignored filter also returns 200.
        if res.get("status") == 200 and (res.get("distinct_properties") or 0) > 1:
            entry["verdict"] = "ACCEPTED — covers multiple properties"
            winner = winner or {"shape": shape["label"], "params_form": shape["label"],
                                "distinct_properties": res.get("distinct_properties"),
                                "rows_first_page": res.get("rows_first_page"),
                                "ids_sent": len(ref_ids)}
        elif res.get("status") == 200:
            entry["verdict"] = ("200 but only one property — the extra ids were likely "
                                "ignored rather than honoured")
        elif res.get("status") == 429:
            entry["verdict"] = "429 — rate limited, inconclusive for this shape"
        else:
            entry["verdict"] = "rejected"
        results.append(entry)
        if winner:
            break

    return jsonify({
        "date": day,
        "ids_tested": len(ref_ids),
        "baseline_single_property": {
            "status": base.get("status"),
            "rows_first_page": base.get("rows_first_page"),
            "distinct_properties": base.get("distinct_properties"),
        },
        "verdict": (
            {"batching_supported": True, **winner} if winner else
            {"batching_supported": False,
             "reason": "No tested shape returned tasks for more than one property. "
                       "Note this covers the common conventions, not every possible "
                       "one — Breezeway support could still confirm a documented form."}
        ),
        "attempts": results,
    })
