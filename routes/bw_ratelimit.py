"""
routes/bw_ratelimit.py — process-wide pacing for the Breezeway API.

Why this exists
---------------
Every full-day view fans out ~442 per-property task calls (Breezeway requires
reference_property_id; there is no company-wide task query — verified via
/admin/bw-probe). Seven such fan-outs exist across the app, spread over 25
thread pools with 8–32 workers each, and none of them knew the others existed.
Each also retried failures 3× with 0.3/0.6/0.9 s backoff, so a throttled sweep
turned 242 failures into ~726 extra requests fired at an API that was already
refusing them. The retries were feeding the failure.

Breezeway publishes NO rate-limit headers — no Retry-After, no quota (also
verified by the probe). So the budget cannot be read; it has to be discovered.

What this does
--------------
A single gate that every Breezeway request passes through, giving the app one
place that owns "how fast are we allowed to ask":

  * Paces requests to a minimum interval, shared across ALL threads and pools.
  * On a 429, backs off multiplicatively and pauses EVERY thread, not just the
    one that was refused — 16 workers independently retrying into the same wall
    is what produced the storm.
  * Relaxes gradually after sustained success, so a brief throttle doesn't
    permanently cripple throughput (AIMD, as in TCP congestion control).
  * Refuses to block longer than MAX_GATE_WAIT. This app already loses long
    requests to a gateway timeout, and a request that hangs is worse than a
    property reported as failed — the UI now names failures accurately, so
    failing fast degrades honestly instead of hanging.

Deliberately in-process. The deployment runs a single Gunicorn worker, so
process-wide is effectively global. If that ever changes to multiple workers,
this needs to move to Redis or the limits become per-worker.
"""

import random
import threading
import time

# Pacing bounds, set from measured behaviour rather than guessed.
#
# First production run of this gate: 12 real 429s out of ~212 requests sent
# (~5.7%), which the previous constants escalated into 714 locally-shed requests
# and a scan that reported 241 false failures. Two errors caused that:
#
#   1. Doubling the interval per 429 pinned it at the 1s cap after 12 of them.
#      Interval is GLOBAL spacing, so 442 properties x 1s = 442 seconds — with an
#      8s acquire deadline almost every request had to be shed. The ceiling has
#      to be small enough that a full sweep still fits: 442 x 0.04 ~= 18s.
#   2. Recovery required consecutive successes, but a shed request never reaches
#      the API and so never reports one. Once escalated it could never come back
#      down (ok_streak stayed 0). Recovery must also depend on elapsed quiet time.
#
# So: additive increase in small steps, a ceiling a real sweep can live with, and
# decay driven by time rather than only by success.
_STEP          = 0.01   # interval added per 429 (additive increase)
_MAX_INTERVAL  = 0.04   # ceiling: ~25 req/s, keeps a 442-property sweep near 18s
_COOL_BASE     = 0.4    # first pause after a 429
_COOL_MAX      = 2.0    # cap — a 20s global pause cannot fit inside one scan
_QUIET_DECAY_S = 3.0    # no 429 for this long → step the interval back down
_RELAX_AFTER   = 10     # consecutive successes also step it down
_MAX_GATE_WAIT = 10.0   # never block a request longer than this

# Status used when THIS APP declines to send, rather than Breezeway refusing.
# Reporting both as 429 makes the UI blame Breezeway for our own shedding, which
# is the same class of mistake the failure-cause work set out to fix. Chosen in
# the 5xx range deliberately: existing callers already retry `status >= 500`, so
# local shedding stays retryable without touching a single call site.
LOCAL_THROTTLE_STATUS = 598


class _BreezewayGate:
    def __init__(self):
        self._lock          = threading.Lock()
        self._next_slot     = 0.0   # monotonic time the next request may start
        self._interval      = 0.0   # current spacing; 0 = unthrottled
        self._cool_until    = 0.0   # hard pause for every thread after a 429
        self._ok_streak     = 0
        self._penalty       = 0.0   # current 429 pause length
        self._last_429      = 0.0   # monotonic; drives time-based decay
        # Counters, for the diagnostics endpoint.
        self._n_429         = 0
        self._n_ok          = 0
        self._n_gave_up     = 0
        self._total_waited  = 0.0

    def acquire(self) -> bool:
        """Wait for permission to send. Returns False if permission could not be
        obtained within MAX_GATE_WAIT — the caller should treat that as a
        throttled failure rather than blocking and risking a gateway timeout."""
        deadline = time.monotonic() + _MAX_GATE_WAIT
        while True:
            with self._lock:
                now  = time.monotonic()
                self._decay_locked(now)
                wait = max(self._cool_until - now, self._next_slot - now, 0.0)
                if wait <= 0.0:
                    # Claim this slot before releasing the lock, so concurrent
                    # threads queue behind us instead of all claiming "now".
                    self._next_slot = now + self._interval
                    return True
                if now + wait > deadline:
                    self._n_gave_up += 1
                    return False
            # Jitter so released threads don't all fire on the same tick.
            nap = min(wait, deadline - time.monotonic())
            if nap <= 0:
                with self._lock:
                    self._n_gave_up += 1
                return False
            time.sleep(nap + random.uniform(0, 0.05))
            with self._lock:
                self._total_waited += nap

    def _decay_locked(self, now: float) -> None:
        """Step the interval back down after a quiet spell. Caller holds the lock.

        Recovery cannot depend on successes alone: when the gate is shedding, most
        requests never reach the API and so never report one. The first production
        run pinned itself at maximum backoff with ok_streak stuck at 0 for exactly
        that reason. Elapsed quiet time is the signal that always arrives."""
        if self._interval <= 0.0 or self._last_429 <= 0.0:
            return
        quiet = now - self._last_429
        if quiet < _QUIET_DECAY_S:
            return
        steps = int(quiet // _QUIET_DECAY_S)
        self._interval = max(0.0, self._interval - _STEP * steps)
        self._penalty  = max(0.0, self._penalty - _COOL_BASE * steps)
        # Consume the elapsed time so the next decay needs another quiet spell.
        self._last_429 = now

    def on_response(self, status) -> None:
        """Feed a result back so the gate can adapt."""
        with self._lock:
            now = time.monotonic()
            if status == 429:
                self._n_429    += 1
                self._ok_streak = 0
                self._last_429  = now
                # Additive increase. Doubling reached the ceiling after a dozen
                # 429s and shed the entire sweep; small steps track the real limit
                # instead of overshooting it.
                self._interval = min(_MAX_INTERVAL, self._interval + _STEP)
                self._penalty  = min(_COOL_MAX,
                                     self._penalty + _COOL_BASE if self._penalty else _COOL_BASE)
                # Jitter the release so the fleet doesn't resume in lockstep.
                self._cool_until = now + self._penalty * random.uniform(0.85, 1.15)
                return

            if status is not None and 200 <= status < 400:
                self._n_ok      += 1
                self._ok_streak += 1
                # Sustained success steps it down too — belt and braces alongside
                # the time-based decay above.
                if self._ok_streak >= _RELAX_AFTER and self._interval > 0:
                    self._ok_streak = 0
                    self._interval  = max(0.0, self._interval - _STEP)
                    self._penalty   = max(0.0, self._penalty - _COOL_BASE)

    def snapshot(self) -> dict:
        with self._lock:
            now = time.monotonic()
            return {
                "interval_s":        round(self._interval, 3),
                "cooling_down":      self._cool_until > now,
                "cooldown_left_s":   round(max(0.0, self._cool_until - now), 2),
                "current_penalty_s": round(self._penalty, 2),
                "ok_streak":         self._ok_streak,
                "counters": {
                    "ok": self._n_ok, "throttled_429": self._n_429,
                    "gave_up_waiting": self._n_gave_up,
                    "total_waited_s": round(self._total_waited, 1),
                },
            }

    def reset(self) -> None:
        """Clear learned state — for diagnostics only."""
        with self._lock:
            self._next_slot = self._interval = self._cool_until = 0.0
            self._ok_streak = 0
            self._penalty   = 0.0
            self._last_429  = 0.0


gate = _BreezewayGate()
