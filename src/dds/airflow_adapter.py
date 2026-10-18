"""Callable boundary for the STG owner's future downstream Airflow task."""

from collections.abc import Mapping
from uuid import UUID

from src.stg.config import REPOSITORIES

from . import DdsError
from .loader import load_dds
from .source import _time


def run_from_stg_summary(summary: Mapping) -> dict:
    if (not isinstance(summary, Mapping) or summary.get("status") != "succeeded"
            or summary.get("full_load") is not True):
        raise DdsError("invalid_stg_summary")
    batches = summary.get("batches")
    if not isinstance(batches, list) or not 1 <= len(batches) <= 3:
        raise DdsError("invalid_stg_summary")
    names = []
    batch_ids = []
    boundaries = []
    for batch in batches:
        if not isinstance(batch, Mapping) or batch.get("repository") not in REPOSITORIES:
            raise DdsError("invalid_stg_summary")
        names.append(batch["repository"])
        batch_ids.append(batch.get("batch_id"))
        # The recorded load parameter travels with the batch; the core rechecks it in PostgreSQL.
        if not isinstance(batch.get("created_since"), str):
            raise DdsError("invalid_stg_summary")
        boundaries.append(_time(batch["created_since"], "summary.created_since"))
    if len(set(names)) != len(names):
        raise DdsError("duplicate_repository")
    try:
        expected = {str(UUID(str(batch_id))): name for batch_id, name in zip(batch_ids, names)}
    except (ValueError, AttributeError, TypeError):
        raise DdsError("invalid_batch_id") from None
    if len(expected) != len(batch_ids):
        raise DdsError("duplicate_batch_id")
    # The core rechecks all statuses and repository identities in PostgreSQL.
    return load_dds(list(expected), expected_repositories=expected,
                    expected_created_since=dict(zip(expected, boundaries)))
