"""Deterministic status reconstruction, independent of database or driver."""

from datetime import datetime, timezone
from itertools import groupby

from . import DdsError


STATUS_TARGETS = {
    "closed": "closed",
    "converted_to_discussion": "closed",
    "reopened": "open",
}


def build_history(issue: dict, events: list[dict]) -> dict:
    """Annotate actual transitions and build positive, half-open intervals.

    Events at the same instant are applied by GitHub ID before the final status
    is materialized. Thus a close/reopen pair is visible in events even when it
    does not occupy an interval of positive duration. A reopening without any
    earlier known closure proves the event history incomplete: closure is never
    synthesized from closed_at, updated_at or the reopening itself.
    """
    issue_id = issue.get("issue_id")
    repository_id = issue.get("repository_id")

    def fail(code, **details):
        raise DdsError(code, issue_id=issue_id, repository_id=repository_id,
                       details=details)

    def valid_id(value):
        return isinstance(value, int) and not isinstance(value, bool) and value > 0

    def timestamp(value, field, event_id=None):
        if not isinstance(value, datetime) or value.utcoffset() is None:
            fail("invalid_timestamp", field=field, event_id=event_id)
        return value.astimezone(timezone.utc)

    if not valid_id(issue_id) or not valid_id(repository_id):
        fail("invalid_identifier")
    created_at = timestamp(issue.get("created_at"), "created_at")
    source_state = issue.get("source_state")
    if source_state not in ("open", "closed"):
        fail("invalid_source_state")
    closed_at = issue.get("source_closed_at")
    if closed_at is not None:
        closed_at = timestamp(closed_at, "source_closed_at")
        if closed_at < created_at:
            fail("closed_before_creation")
        if source_state == "open":
            fail("open_issue_has_closed_at")

    warnings = []

    def warn(code, **details):
        warnings.append({"severity": "warning", "code": code,
                         "issue_id": issue_id, "repository_id": repository_id,
                         "details": details})

    ordered = []
    seen = set()
    for original in events:
        event = dict(original)
        event_id = event.get("event_id")
        if not valid_id(event_id):
            fail("invalid_event_identifier")
        if event_id in seen:
            fail("duplicate_event_identifier", event_id=event_id)
        seen.add(event_id)
        if (event.get("issue_id") != issue_id
                or event.get("repository_id") != repository_id):
            fail("event_parent_mismatch", event_id=event_id)
        event_type = event.get("event_type")
        if not isinstance(event_type, str) or not event_type:
            fail("invalid_event_type", event_id=event_id)
        event["github_created_at"] = timestamp(
            event.get("github_created_at"), "github_created_at", event_id)
        if event["github_created_at"] < created_at:
            fail("event_before_creation", event_id=event_id)
        ordered.append(event)
    ordered.sort(key=lambda event: (event["github_created_at"], event["event_id"]))

    state = "open"
    latest_closure = None
    versions = [{"status": state, "valid_from": created_at, "valid_to": None,
                 "is_current": True, "source_event_id": None}]
    for occurred_at, grouped in groupby(ordered, key=lambda event: event["github_created_at"]):
        group = list(grouped)
        if len(group) > 1:
            warn("simultaneous_events", event_ids=[event["event_id"] for event in group])
        final_transition_id = None
        for event in group:
            target = STATUS_TARGETS.get(event["event_type"])
            event.update(status_before=None, status_after=None, is_status_transition=False)
            if target is None:
                continue
            if target == "open" and latest_closure is None:
                fail("reopened_without_close", event_id=event["event_id"])
            event["status_before"] = state
            event["status_after"] = target
            event["is_status_transition"] = target != state
            if target == "closed":
                latest_closure = occurred_at
            if target == state:
                warn("repeated_status", event_id=event["event_id"], status=state)
            else:
                final_transition_id = event["event_id"]
            state = target
        if state == versions[-1]["status"]:
            continue
        version = {"status": state, "valid_from": occurred_at, "valid_to": None,
                   "is_current": True, "source_event_id": final_transition_id}
        if occurred_at == versions[-1]["valid_from"]:
            versions[-1] = version
        else:
            versions[-1].update(valid_to=occurred_at, is_current=False)
            versions.append(version)

    if state != source_state:
        fail("source_state_mismatch", reconstructed_state=state, source_state=source_state)
    if source_state == "closed":
        if closed_at is None:
            warn("closed_at_missing")
        elif closed_at != latest_closure:
            warn("closed_at_mismatch", source_closed_at=closed_at.isoformat(),
                 latest_closure=latest_closure.isoformat() if latest_closure else None)
    return {"events": ordered, "versions": versions, "warnings": warnings}
