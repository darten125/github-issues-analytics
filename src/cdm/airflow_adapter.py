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
    if not isinstance(repositories, list) or not 1 <= len(repositories) <= 3:
        raise CdmError("invalid_dds_result")
    if any(not isinstance(item, Mapping)
           or not isinstance(item.get("repository"), str)
           or not item.get("batch_id") or not item.get("run_id")
           for item in repositories):
        raise CdmError("invalid_dds_result")
    names = [item["repository"] for item in repositories]
    if len(set(names)) != len(names) or not set(names) <= set(EXPECTED_REPOSITORIES):
        raise CdmError("invalid_dds_result")
    return load_cdm(expected_repositories=repositories)
