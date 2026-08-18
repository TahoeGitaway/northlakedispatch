# --threads: the app served ONE request at a time before this. Gunicorn's default
# sync worker handles a single request per process, and with --workers unset that
# is one request for the whole app — so opening six route windows queued them
# serially, each waiting out the one before it. That, not just the rate limit, is
# why the second and third tab appeared to do nothing. It also breaks the sweep
# sharing outright: a waiter cannot be served while the owner holds the only
# thread, so it can never receive the result it is waiting for.
#
# --workers stays at 1 ON PURPOSE. The Breezeway rate gate, the day cache and the
# in-flight sweep registry are all in-process (routes/bw_ratelimit.py says so).
# A second worker would give each its own copy: two gates pacing 180/min each
# against a 200/min limit, and two sweeps that cannot see one another. Threads
# share process state; workers do not.
#
# --timeout: 120 was below the work. A full day is ~148s of sweeping at 3 req/s,
# so every complete sweep was being killed by gunicorn before it could answer —
# the request simply died and the window showed nothing. 300 matches the sweep
# budget (200s) plus reservations and post-processing, with room to spare.
web: gunicorn app:app --bind 0.0.0.0:$PORT --workers 1 --threads 8 --timeout 300
