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
verified by the probe), so for a long time the budget had to be discovered by
running into it. It is now CONFIRMED out of band: 200 requests per minute.

That changes the gate's job. Discovering a limit reactively means only ever
learning about it from 429s already received — every lesson costs a failed
request, and the gate can only tighten after damage is done. With the number
known, the correct spacing can be applied from the first request of the day and
429s become the exception rather than the feedback channel.

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
# Those constants were tuned against a limit nobody knew. Now that it is confirmed
# at 200 req/min, the old ceiling turns out to have been the whole problem:
#
#   _MAX_INTERVAL = 0.04s  ->  25 req/s  ->  1500/min  ->  7.5x the real limit.
#
# The gate could not comply even at maximum backoff. Every knob below it was
# therefore tuning the shape of a curve that never reached a legal rate, which is
# why sweeps kept ending in a wall of 429s no matter how the escalation behaved.
#
# The last observed import corroborates the figure exactly: 197 properties loaded
# and 35 refused inside the 45s budget = 232 requests sent, ~309/min attempted
# against a 200/min ceiling.
#
# So the gate now PACES rather than discovers. Spacing is set from the known limit
# up front and 429s are treated as an anomaly (another client on the same quota,
# a burst that crossed a window edge) rather than as the primary signal.
_LIMIT_PER_MIN = 200    # CONFIRMED with Breezeway, not inferred.
# Run at ~90% of quota. Sitting exactly on the limit means any jitter, clock skew,
# or fixed-window edge tips over it, and the cost of a 429 (a global cooldown that
# stalls every thread) is far higher than the cost of the 10% headroom.
_HEADROOM      = 0.90
_BASE_INTERVAL = 60.0 / (_LIMIT_PER_MIN * _HEADROOM)   # 0.333s -> 180 req/min
_STEP          = 0.05   # interval added per 429; sized against a 0.33s baseline,
                        # where the old 0.01 step was noise
_MAX_INTERVAL  = 0.60   # room to back off BELOW the baseline rate (~100/min) when
                        # something else is sharing the quota
_COOL_BASE     = 0.4    # first pause after a 429
_COOL_MAX      = 2.0    # cap — a 20s global pause cannot fit inside one scan
_QUIET_DECAY_S = 3.0    # no 429 for this long → step back down toward the baseline
_RELAX_AFTER   = 10     # consecutive successes also step it down
# A request waits behind at most one pool's worth of peers, not the whole sweep:
# the executor holds the backlog and only its workers are ever in the gate. The
# widest pool in the app is 32 (briefing), so worst-case queuing is 32 x 0.33 =
# ~10.6s — just over the old 10s deadline, which would have misreported ordinary
# queuing as this app shedding. 15s clears it with margin and stays far below both
# the 45s import budget and the 300s gateway kill.
_MAX_GATE_WAIT = 15.0   # never block a request longer than this

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
        # Start AT the known-safe spacing instead of unthrottled. Starting at 0 meant
        # every process began each day by bursting until Breezeway pushed back — the
        # first ~200 requests went out with no spacing at all, and the 429s that
        # followed were self-inflicted.
        self._interval      = _BASE_INTERVAL
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
        throttled failure rather than blocking and risking a gateway timeout.

        Slots are RESERVED in arrival order, then waited out. The previous version
        re-contended for the same slot on every wake: a thread slept until
        _next_slot, took the lock, usually found another thread had already claimed
        it, and slept again with no ordering between them. Nothing bounded how many
        times one caller could lose that race. Measured at 16 workers, worst-case
        waiting ran to 115-149 slot-widths against a pool only 16 deep — at 0.33s
        spacing that is a 38-50s wait on a 15s deadline, so the gate would have shed
        as "held back by this app" requests that were simply queued unfairly.

        Reserving up front makes the wait deterministic: a caller waits for the
        callers already ahead of it and nothing else, so the deepest wait is one
        pool's width x the interval."""
        start    = time.monotonic()
        deadline = start + _MAX_GATE_WAIT
        with self._lock:
            now = time.monotonic()
            self._decay_locked(now)
            # This caller's own slot. No one else can be granted it, so the wait
            # below is a plain sleep rather than a race.
            slot = max(self._next_slot, self._cool_until, now)
            if slot > deadline:
                # Shed BEFORE sleeping. Waiting out most of the deadline only to
                # fail wastes the very capacity the deadline exists to protect.
                self._n_gave_up += 1
                return False
            self._next_slot = slot + self._interval

        # Wait for the reserved slot, re-checking only the global cooldown — a 429
        # landing after the reservation must still hold this thread back.
        while True:
            with self._lock:
                now  = time.monotonic()
                wait = max(slot - now, self._cool_until - now)
                if wait <= 0.0:
                    self._total_waited += now - start
                    return True
                if now + wait > deadline:
                    self._n_gave_up += 1
                    self._total_waited += now - start
                    return False
            # Jitter so threads released together by a cooldown don't fire in lockstep.
            time.sleep(min(wait, max(0.0, deadline - time.monotonic())) + random.uniform(0, 0.05))

    def _decay_locked(self, now: float) -> None:
        """Step the interval back down after a quiet spell. Caller holds the lock.

        Recovery cannot depend on successes alone: when the gate is shedding, most
        requests never reach the API and so never report one. The first production
        run pinned itself at maximum backoff with ok_streak stuck at 0 for exactly
        that reason. Elapsed quiet time is the signal that always arrives.

        Decay stops at _BASE_INTERVAL, never 0. Below the baseline the gate is over
        the published limit by construction, so 'recovered' has a floor."""
        if self._interval <= _BASE_INTERVAL or self._last_429 <= 0.0:
            return
        quiet = now - self._last_429
        if quiet < _QUIET_DECAY_S:
            return
        steps = int(quiet // _QUIET_DECAY_S)
        self._interval = max(_BASE_INTERVAL, self._interval - _STEP * steps)
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
                # Push the reservation line past the cooldown too. Slots are handed
                # out ahead of time now, so without this the next arrivals would
                # reserve slots that fall INSIDE the pause and then all come due the
                # instant it lifts — re-bursting into an API that just refused us.
                self._next_slot = max(self._next_slot, self._cool_until)
                return

            if status is not None and 200 <= status < 400:
                self._n_ok      += 1
                self._ok_streak += 1
                # Sustained success steps it down too — belt and braces alongside
                # the time-based decay above.
                if self._ok_streak >= _RELAX_AFTER and self._interval > _BASE_INTERVAL:
                    self._ok_streak = 0
                    self._interval  = max(_BASE_INTERVAL, self._interval - _STEP)
                    self._penalty   = max(0.0, self._penalty - _COOL_BASE)

    def snapshot(self) -> dict:
        with self._lock:
            now = time.monotonic()
            return {
                "interval_s":        round(self._interval, 3),
                # The spacing in the units the limit is quoted in, so the diagnostics
                # page can be read against Breezeway's 200/min without arithmetic.
                "limit_per_min":     _LIMIT_PER_MIN,
                "effective_per_min": round(60.0 / self._interval, 1) if self._interval > 0 else None,
                "baseline_per_min":  round(60.0 / _BASE_INTERVAL, 1),
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
        """Clear learned state — for diagnostics only. Returns to the baseline
        spacing, not to unthrottled: the 200/min limit is a fact about Breezeway,
        not something this gate learned and can forget."""
        with self._lock:
            self._next_slot = self._cool_until = 0.0
            self._interval  = _BASE_INTERVAL
            self._ok_streak = 0
            self._penalty   = 0.0
            self._last_429  = 0.0


gate = _BreezewayGate()
