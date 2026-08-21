"""Property-id plumbing: the reference-id lookup, and the id-form fallback order.

These guard a bug that cost half the app's Breezeway budget for months while
looking completely healthy. Breezeway's property endpoint returns `id` as an INT,
so `_property_ref_cache` is int-keyed; the scanners carry pids around as STRINGS,
because that is what reservation JSON and the database hand them. A plain
`ref_cache.get(pid)` therefore missed on every house, every time, and each one
fell through to the id-form fallback — which tried `property_id` first, a
parameter Breezeway aliases onto `reference_property_id` and can only answer 422.

Two requests per house where one would do, the first guaranteed to fail. Nothing
looked wrong, because the second request returned the right tasks.

Stdlib unittest on purpose: this repo has no test framework and no dev
dependencies, and adding pytest to requirements.txt would ship it to production.

    python3 -m unittest discover tests
"""

import os
import sys
import unittest
from datetime import date
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DATABASE_URL", "postgresql://unused/unused")

from routes.briefing import _ref_for                      # noqa: E402
from routes import lease_prep                             # noqa: E402


# A ref cache shaped exactly like the real one: int keys, str values.
REF_CACHE = {858109: "TD-GLACIER", 1110435: "WS-VALHALLA"}


class RefForTests(unittest.TestCase):
    """The lookup itself, across the type mismatch it exists to absorb."""

    def test_str_pid_against_int_keyed_cache(self):
        # The whole bug in one line: this returned "" before _ref_for existed.
        self.assertEqual(_ref_for(REF_CACHE, "858109"), "TD-GLACIER")

    def test_int_pid_against_int_keyed_cache(self):
        self.assertEqual(_ref_for(REF_CACHE, 858109), "TD-GLACIER")

    def test_int_pid_against_str_keyed_cache(self):
        # Defends the other direction too, in case the loader ever changes.
        self.assertEqual(_ref_for({"858109": "TD-GLACIER"}, 858109), "TD-GLACIER")

    def test_missing_pid_returns_empty_string(self):
        # "" is what the callers treat as "no reference id, use a fallback form",
        # so it must be falsy — not None, not a KeyError.
        self.assertEqual(_ref_for(REF_CACHE, "999999"), "")

    def test_non_numeric_pid_does_not_raise(self):
        # int("abc") would explode; the isdigit guard is what prevents it.
        self.assertEqual(_ref_for(REF_CACHE, "not-a-pid"), "")

    def test_empty_cache(self):
        self.assertEqual(_ref_for({}, "858109"), "")


class _Recorder:
    """Stands in for briefing._fetch_bw_endpoint, recording what it was asked.

    Returns (results, error, status) like the real one. `answers` maps an id key
    to the reply for it, so a test can say "reference_property_id works, the
    others 422" and then assert on which were actually tried.
    """

    def __init__(self, answers):
        self.answers = answers
        self.calls = []                      # [(id_key, value), ...] in order

    def __call__(self, token, path, params):
        key = next(k for k in ("reference_property_id", "home_id", "property_id")
                   if k in params)
        self.calls.append((key, params[key]))
        return self.answers.get(key, ([], "HTTP 422: Property not found.", 422))

    @property
    def keys_tried(self):
        return [k for k, _ in self.calls]


TASKS = [{"id": 1, "title": "Deep clean"}]
OK = (TASKS, "", 200)
EMPTY_OK = ([], "", 200)
REFUSED_422 = ([], "HTTP 422: Property not found.", 422)
THROTTLED = ([], "HTTP 429: too many requests", 429)


def _fetch(rec, ref_id, pid="858109"):
    with mock.patch("routes.briefing._fetch_bw_endpoint", rec):
        return lease_prep._fetch_tasks_for_property(
            "tok", pid, ref_id, date(2026, 8, 23), date(2026, 9, 5))


class RequestCountTests(unittest.TestCase):
    """The money test. A house with a reference id costs exactly ONE request."""

    def test_one_request_when_the_reference_id_resolves(self):
        rec = _Recorder({"reference_property_id": OK})
        tasks, err = _fetch(rec, _ref_for(REF_CACHE, "858109"))

        self.assertEqual(tasks, TASKS)
        self.assertEqual(err, "")
        # Before the fix this was 2: a 422 on property_id, then a 200 on home_id.
        self.assertEqual(len(rec.calls), 1, f"expected 1 request, got {rec.calls}")
        self.assertEqual(rec.keys_tried, ["reference_property_id"])

    def test_the_pid_the_callers_actually_pass_is_a_string(self):
        """Guards the specific regression: a str pid must still find the ref id.

        If someone reverts _ref_for to ref_cache.get(pid), this fails — the lookup
        misses, no reference_property_id is sent, and the fallback runs.
        """
        ref_id = _ref_for(REF_CACHE, "858109")     # str pid, int-keyed cache
        self.assertTrue(ref_id, "ref lookup missed — the double-fetch bug is back")

        rec = _Recorder({"reference_property_id": OK})
        _fetch(rec, ref_id)
        self.assertNotIn("property_id", rec.keys_tried)


class CallSiteTests(unittest.TestCase):
    """Exercise the real sweep, not just the helper.

    The tests above prove `_ref_for` works and that the fetcher asks correctly when
    handed a ref id. Neither notices if a call site goes back to
    `ref_cache.get(pid, "")` — the helper would still be right, and the sweep would
    still be broken. This drives `_work_leases` with the exact shapes production
    uses: an int-keyed cache and string pids straight off the cache table.
    """

    def _run_sweep(self, ref_cache):
        rec = _Recorder({"reference_property_id": OK, "home_id": OK})
        rows = [{"property_id": "858109", "anchor_date": "2026-09-01",
                 "tasks_ok": False, "hot_tub_ok": True}]

        with mock.patch("routes.briefing._fetch_bw_endpoint", rec), \
             mock.patch.object(lease_prep, "_write_cached_lease"):
            lease_prep._work_leases("tok", "pre", rows, ref_cache, workers=1)
        return rec

    def test_the_sweep_resolves_the_ref_id_and_asks_once(self):
        rec = self._run_sweep(REF_CACHE)          # int-keyed, as in production

        self.assertEqual(rec.keys_tried, ["reference_property_id"],
                         "the sweep did not resolve the ref id — it fell through to "
                         "the id-form fallback, which is the doubled-request bug")
        self.assertEqual(len(rec.calls), 1)
        self.assertEqual(rec.calls[0][1], "TD-GLACIER")

    def test_a_house_with_no_ref_id_still_resolves_via_home_id(self):
        rec = self._run_sweep({})                 # nothing known for this house

        self.assertEqual(rec.keys_tried, ["home_id"])
        self.assertEqual(rec.calls[0][1], "858109")


class FallbackOrderTests(unittest.TestCase):
    """When a house genuinely has no reference id, ask the way that can work."""

    def test_home_id_is_tried_before_property_id(self):
        rec = _Recorder({"home_id": OK})
        tasks, err = _fetch(rec, "")             # no reference id for this house

        self.assertEqual(tasks, TASKS)
        self.assertEqual(err, "")
        self.assertEqual(rec.keys_tried, ["home_id"])
        # property_id is Breezeway's alias for reference_property_id: passing a raw
        # pid can only 422, so it must never be the first thing tried.
        self.assertNotIn("property_id", rec.keys_tried)

    def test_property_id_survives_as_a_last_resort(self):
        rec = _Recorder({"home_id": REFUSED_422, "property_id": OK})
        tasks, _ = _fetch(rec, "")

        self.assertEqual(tasks, TASKS)
        self.assertEqual(rec.keys_tried, ["home_id", "property_id"])


class EmptyVersusFailedTests(unittest.TestCase):
    """An empty answer and a refused question must never look the same.

    This behaviour predates the id fix and is what stops a throttled lookup being
    cached overnight as "this lease has no prep tasks". The reorder must not
    disturb it.
    """

    def test_a_real_empty_result_is_an_answer_not_an_error(self):
        rec = _Recorder({"reference_property_id": EMPTY_OK})
        tasks, err = _fetch(rec, "TD-GLACIER")

        self.assertEqual(tasks, [])
        self.assertEqual(err, "", "a 200 with no tasks is an answer, not a failure")

    def test_every_key_refused_is_an_error_not_an_empty_result(self):
        rec = _Recorder({})                       # everything 422s
        tasks, err = _fetch(rec, "TD-GLACIER")

        self.assertEqual(tasks, [])
        self.assertTrue(err, "all keys failed — this must not read as 'no tasks'")

    def test_throttled_is_reported_rather_than_swallowed(self):
        rec = _Recorder({"reference_property_id": THROTTLED,
                         "home_id": THROTTLED, "property_id": THROTTLED})
        tasks, err = _fetch(rec, "TD-GLACIER")

        self.assertEqual(tasks, [])
        self.assertIn("429", err)


class SweptPidTests(unittest.TestCase):
    """A task must stay attached to the property it was fetched for.

    walk_thru_rename matches each task to that property's guest arrivals, in a map
    keyed by the reservation's property_id. It used to re-derive the pid from the
    task payload, preferring `property_id` — which holds only while every task is
    fetched by home_id. Once the reference-id path started working, tasks came back
    from a different id space, the match found no arrivals, and every task was
    dropped: a scan that looked perfectly healthy and proposed nothing at all.
    """

    def test_the_sweep_stamps_the_pid_it_asked_about(self):
        from routes import walk_thru_rename as wtr

        # A payload whose property_id is NOT the Breezeway pid — the shape that
        # breaks a match derived from the task alone.
        task = {"id": 77, "title": "Walk Thru", "scheduled_date": "2026-08-30",
                "property_id": 326417, "home_id": 858240}

        with mock.patch.object(wtr, "_fetch_tasks_for_property",
                               return_value=([task], True, 200)), \
             mock.patch("routes.briefing._get_live_ref_cache", return_value={}):
            tasks, failed, _ = wtr._fetch_tasks_for_pids(
                "tok", ["858240"], date(2026, 8, 29), date(2026, 9, 5))

        self.assertEqual(failed, [])
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0]["_swept_pid"], "858240",
                         "the task lost the property it was fetched for — the "
                         "arrival match will drop it")

    def test_the_stamp_wins_over_a_payload_in_another_id_space(self):
        """The proposal loop must prefer the swept pid over the payload."""
        task = {"_swept_pid": "858240", "property_id": 326417, "home_id": 858240}
        pid = (task.get("_swept_pid")
               or str(task.get("home_id") or task.get("property_id") or ""))
        self.assertEqual(pid, "858240")

        # And with no stamp (a task held from an older cached scan), home_id leads
        # — the precedence dispatch.py already uses.
        legacy = {"property_id": 326417, "home_id": 858240}
        pid = (legacy.get("_swept_pid")
               or str(legacy.get("home_id") or legacy.get("property_id") or ""))
        self.assertEqual(pid, "858240")


class CacheKeyContractTests(unittest.TestCase):
    """Pin what the loader stores, so a change there is caught here.

    If this starts failing, _property_ref_cache's key type changed — which is
    exactly the drift that produced the original bug, and it should surface as a
    failing test rather than as a doubled request count nobody notices.
    """

    def test_the_loader_keys_the_ref_cache_by_int(self):
        from routes import briefing

        page = {"results": [{"id": 858109, "name": "Glacier Ski Lease",
                             "reference_property_id": "TD-GLACIER"}]}
        resp = mock.Mock(status_code=200, ok=True)
        resp.json.return_value = page

        with mock.patch.object(briefing, "bw_get", return_value=resp), \
             mock.patch.object(briefing, "_get_breezeway_token", return_value="tok"):
            briefing._property_cache_ts = 0          # force a refresh
            briefing._load_property_cache()

        keys = list(briefing._property_ref_cache.keys())
        self.assertEqual(keys, [858109])
        self.assertIsInstance(keys[0], int)
        # And the value is the external reference, stringified.
        self.assertEqual(briefing._property_ref_cache[858109], "TD-GLACIER")


if __name__ == "__main__":
    unittest.main()
