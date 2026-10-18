import copy
from datetime import datetime, timedelta, timezone
import unittest

from src.dds import DdsError
from src.dds.scd2 import build_history


CREATED = datetime(2023, 1, 1, tzinfo=timezone.utc)


def issue(state="open", closed_at=None):
    return {"issue_id": 10, "repository_id": 20, "created_at": CREATED,
            "source_state": state, "source_closed_at": closed_at}


def event(identifier, kind, seconds):
    return {"event_id": identifier, "issue_id": 10, "repository_id": 20,
            "event_type": kind, "github_created_at": CREATED + timedelta(seconds=seconds)}


class HistoryTests(unittest.TestCase):
    def assert_code(self, code, selected_issue, selected_events):
        with self.assertRaises(DdsError) as raised:
            build_history(selected_issue, selected_events)
        self.assertEqual(code, raised.exception.problem["code"])

    def test_no_events_has_one_open_interval(self):
        history = build_history(issue(), [])
        self.assertEqual(history, {"events": [], "warnings": [], "versions": [
            {"status": "open", "valid_from": CREATED, "valid_to": None,
             "is_current": True, "source_event_id": None}]})

    def test_close_reopen_close_is_sorted_and_does_not_mutate_input(self):
        events = [event(3, "closed", 30), event(1, "closed", 10), event(2, "reopened", 20)]
        before = copy.deepcopy(events)
        history = build_history(issue("closed", CREATED + timedelta(seconds=30)), events)
        self.assertEqual(events, before)
        self.assertEqual(["open", "closed", "open", "closed"],
                         [version["status"] for version in history["versions"]])
        self.assertEqual([1, 2, 3], [item["event_id"] for item in history["events"]])
        self.assertTrue(all(item["is_status_transition"] for item in history["events"]))
        self.assertEqual(history, build_history(issue("closed", CREATED + timedelta(seconds=30)),
                                               list(reversed(events))))
        for left, right in zip(history["versions"], history["versions"][1:]):
            self.assertEqual(left["valid_to"], right["valid_from"])
            self.assertLess(left["valid_from"], left["valid_to"])
            self.assertFalse(left["is_current"])
        self.assertTrue(history["versions"][-1]["is_current"])

    def test_late_and_changed_event_rebuild_history(self):
        original = [event(3, "closed", 30)]
        self.assertEqual(2, len(build_history(issue("closed"), original)["versions"]))
        late = original + [event(1, "closed", 10), event(2, "reopened", 20)]
        self.assertEqual(4, len(build_history(issue("closed"), late)["versions"]))
        changed = [event(1, "closed", 5), event(2, "reopened", 20), event(3, "closed", 30)]
        self.assertEqual(CREATED + timedelta(seconds=5),
                         build_history(issue("closed"), changed)["versions"][1]["valid_from"])

    def test_duplicate_close_keeps_original_interval_and_marks_noop(self):
        history = build_history(issue("closed", CREATED + timedelta(seconds=20)),
                                [event(1, "closed", 10), event(2, "closed", 20)])
        self.assertEqual(2, len(history["versions"]))
        self.assertEqual(CREATED + timedelta(seconds=10), history["versions"][-1]["valid_from"])
        self.assertFalse(history["events"][-1]["is_status_transition"])
        self.assertEqual("closed", history["events"][-1]["status_before"])
        self.assertEqual(["repeated_status"], [warning["code"] for warning in history["warnings"]])

    def test_reopen_without_known_closure_blocks_instead_of_staying_open(self):
        for events in ([event(1, "reopened", 10)],
                       [event(1, "reopened", 10), event(2, "closed", 20)],
                       [event(1, "labeled", 5), event(2, "reopened", 10)]):
            with self.subTest(events=[item["event_type"] for item in events]):
                state = "closed" if events[-1]["event_type"] == "closed" else "open"
                with self.assertRaises(DdsError) as raised:
                    build_history(issue(state), events)
                self.assertEqual("reopened_without_close", raised.exception.problem["code"])
                self.assertEqual(events[-1 if state == "open" else 0]["event_id"],
                                 raised.exception.problem["details"]["event_id"])

    def test_conversion_is_a_known_closure_before_reopen(self):
        history = build_history(issue(), [event(1, "converted_to_discussion", 10),
                                          event(2, "reopened", 20)])
        self.assertEqual(["open", "closed", "open"], [version["status"] for version in history["versions"]])

    def test_repeated_reopen_after_confirmed_cycle_does_not_split_open_interval(self):
        history = build_history(issue(), [event(1, "closed", 10), event(2, "reopened", 20),
                                          event(3, "reopened", 30)])
        self.assertEqual(3, len(history["versions"]))
        self.assertFalse(history["events"][-1]["is_status_transition"])
        self.assertEqual(["repeated_status"], [warning["code"] for warning in history["warnings"]])

    def test_same_time_close_reopen_preserves_transitions_without_closed_interval(self):
        history = build_history(issue(), [event(2, "reopened", 10), event(1, "closed", 10)])
        self.assertEqual(1, len(history["versions"]))
        self.assertEqual([True, True], [item["is_status_transition"] for item in history["events"]])
        self.assertEqual(["open", "closed"], [item["status_before"] for item in history["events"]])
        self.assertEqual("simultaneous_events", history["warnings"][0]["code"])

    def test_same_time_final_status_uses_last_effective_transition(self):
        history = build_history(issue("closed"), [event(3, "closed", 10),
                                event(1, "closed", 10), event(2, "reopened", 10)])
        self.assertEqual(2, len(history["versions"]))
        self.assertEqual(3, history["versions"][-1]["source_event_id"])

    def test_transition_at_creation_has_no_zero_length_open_interval(self):
        history = build_history(issue("closed", CREATED), [event(1, "closed", 0)])
        self.assertEqual(1, len(history["versions"]))
        self.assertEqual("closed", history["versions"][0]["status"])
        self.assertEqual(CREATED, history["versions"][0]["valid_from"])

    def test_creation_close_reopen_retains_initial_interval(self):
        history = build_history(issue(), [event(1, "closed", 0), event(2, "reopened", 0)])
        self.assertEqual(1, len(history["versions"]))
        self.assertIsNone(history["versions"][0]["source_event_id"])

    def test_conversion_closes_and_preserves_event_type(self):
        history = build_history(issue("closed"), [event(1, "converted_to_discussion", 10)])
        self.assertEqual("closed", history["versions"][-1]["status"])
        self.assertEqual("converted_to_discussion", history["events"][0]["event_type"])

    def test_unknown_event_preserves_payload_and_has_no_status(self):
        selected_event = {**event(1, "future_event_type", 10), "extra": "preserved"}
        history = build_history(issue(), [selected_event])
        self.assertEqual("preserved", history["events"][0]["extra"])
        self.assertIsNone(history["events"][0]["status_before"])
        self.assertIsNone(history["events"][0]["status_after"])
        self.assertFalse(history["events"][0]["is_status_transition"])

    def test_closed_at_discrepancies_are_warnings(self):
        missing = build_history(issue("closed"), [event(1, "closed", 10)])
        self.assertEqual("closed_at_missing", missing["warnings"][-1]["code"])
        mismatch = build_history(issue("closed", CREATED + timedelta(seconds=11)),
                                 [event(1, "closed", 10)])
        self.assertEqual("closed_at_mismatch", mismatch["warnings"][-1]["code"])

    def test_invalid_source_state_and_dates_are_blocking(self):
        scenarios = [
            ("invalid_source_state", issue("unknown"), []),
            ("invalid_identifier", {**issue(), "issue_id": True}, []),
            ("invalid_timestamp", {**issue(), "created_at": CREATED.replace(tzinfo=None)}, []),
            ("invalid_timestamp", issue("closed", "yesterday"), []),
            ("closed_before_creation", issue("closed", CREATED - timedelta(seconds=1)), []),
            ("open_issue_has_closed_at", issue("open", CREATED), []),
            ("source_state_mismatch", issue("closed"), []),
            ("source_state_mismatch", issue(), [event(1, "closed", 10)]),
        ]
        for code, selected_issue, selected_events in scenarios:
            with self.subTest(code=code, issue=selected_issue):
                self.assert_code(code, selected_issue, selected_events)

    def test_invalid_events_are_blocking(self):
        scenarios = [
            ("invalid_event_identifier", [{**event(1, "closed", 10), "event_id": 0}]),
            ("duplicate_event_identifier", [event(1, "closed", 10), event(1, "closed", 20)]),
            ("event_parent_mismatch", [{**event(1, "closed", 10), "issue_id": 11}]),
            ("invalid_event_type", [{**event(1, "closed", 10), "event_type": None}]),
            ("invalid_timestamp", [{**event(1, "closed", 10), "github_created_at": None}]),
            ("event_before_creation", [event(1, "closed", -1)]),
        ]
        for code, events in scenarios:
            with self.subTest(code=code):
                self.assert_code(code, issue(), events)

    def test_timezone_offsets_are_compared_as_instants(self):
        offset = timezone(timedelta(hours=3))
        selected_event = event(1, "closed", 10)
        selected_event["github_created_at"] = selected_event["github_created_at"].astimezone(offset)
        history = build_history(issue("closed", CREATED + timedelta(seconds=10)), [selected_event])
        self.assertEqual(CREATED + timedelta(seconds=10), history["versions"][-1]["valid_from"])
        self.assertEqual([], history["warnings"])


if __name__ == "__main__":
    unittest.main()
