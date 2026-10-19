"""Airflow entrypoint after a successful full DDS publication."""

from collections.abc import Mapping

from . import CdmError, EXPECTED_REPOSITORIES
from .loader import load_cdm


def run_after_dds(result: Mapping) -> dict:
    if not isinstance(result, Mapping) or result.get("status") != "succeeded":
        raise CdmError("invalid_dds_result")
    if not result.get("coverage_complete") or not result.get("ready_for_cdm"):
        raise CdmError("dds_not_ready")
    repositories = result.get("repositories")
    if (not isinstance(repositories, list) or len(repositories) != 3
            or {item.get("repository") for item in repositories
                if isinstance(item, Mapping)} != set(EXPECTED_REPOSITORIES)):
        raise CdmError("invalid_dds_result")
    return load_cdm(expected_repositories=repositories)
