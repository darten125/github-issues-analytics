"""DDS transformation of confirmed GitHub Issues STG observations."""

RULES_VERSION = "2"


class DdsError(ValueError):
    """A publish-blocking quality issue, safe to expose without raw payloads."""

    def __init__(self, code, *, issue_id=None, repository_id=None, details=None):
        self.problem = {"code": code, "severity": "error", "issue_id": issue_id,
                        "repository_id": repository_id, "details": details or {}}
        super().__init__(f"{code} (issue_id={issue_id}, repository_id={repository_id})")


def load_dds(batch_ids):
    from .loader import load_dds as load
    return load(batch_ids)


def preview_dds(batch_ids):
    from .loader import preview_dds as preview
    return preview(batch_ids)


def validate_dds(*, expected_batch_ids=None):
    from .loader import validate_dds as validate
    return validate(expected_batch_ids=expected_batch_ids)
