"""Small, read-only GitHub REST client for the STG loader."""

from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit

import requests

LOGGER = logging.getLogger(__name__)
API_ROOT = "https://api.github.com"
NEXT_LINK = re.compile(r'<([^>]+)>\s*;\s*rel="next"')


class GitHubApiError(RuntimeError):
    """An API failure with a message that never includes credentials or payloads."""


class GitHubClient:
    def __init__(self, token: str, *, session=None, sleep=time.sleep, max_attempts=3, max_wait=120):
        if not token:
            raise ValueError("GITHUB_TOKEN is required for the STG loader")
        self.session = session or requests.Session()
        self.session.headers.update({
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "github-issues-analytics-stg",
        })
        self.sleep = sleep
        self.max_attempts = max_attempts
        self.max_wait = max_wait

    @staticmethod
    def _safe_url(url: str) -> str:
        parsed = urlsplit(url)
        if parsed.scheme != "https" or parsed.netloc != "api.github.com":
            raise GitHubApiError("GitHub pagination returned an untrusted URL")
        return url

    @staticmethod
    def _next_link(header: str | None) -> str | None:
        if not header:
            return None
        match = NEXT_LINK.search(header)
        return match.group(1) if match else None

    def _delay(self, response, attempt: int) -> float:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return max(0.0, float(retry_after))
            except ValueError:
                try:
                    target = parsedate_to_datetime(retry_after)
                    return max(0.0, (target - datetime.now(timezone.utc)).total_seconds())
                except (TypeError, ValueError, OverflowError):
                    pass
        if response.headers.get("X-RateLimit-Remaining") == "0":
            try:
                return max(0.0, float(response.headers["X-RateLimit-Reset"]) - time.time() + 1)
            except (KeyError, ValueError):
                pass
        return float(2 ** (attempt - 1))

    @staticmethod
    def _is_rate_limited(response) -> bool:
        if response.status_code == 429:
            return True
        if response.status_code != 403:
            return False
        if response.headers.get("X-RateLimit-Remaining") == "0" or "Retry-After" in response.headers:
            return True
        try:
            message = str(response.json().get("message", "")).lower()
        except (ValueError, AttributeError):
            return False
        return "secondary rate limit" in message or "abuse detection" in message

    def _get(self, url: str, params=None):
        self._safe_url(url)
        path = urlsplit(url).path
        for attempt in range(1, self.max_attempts + 1):
            try:
                response = self.session.request("GET", url, params=params, timeout=(5, 30))
            except requests.RequestException as exc:
                if attempt == self.max_attempts:
                    raise GitHubApiError(f"Network error on GET {path} after {attempt} attempts") from exc
                delay = float(2 ** (attempt - 1))
                LOGGER.warning("Network error on GET %s; retrying in %.0f s", path, delay)
                self.sleep(delay)
                continue
            if response.status_code == 200:
                try:
                    return response.json(), response.headers
                except ValueError as exc:
                    raise GitHubApiError(f"Invalid JSON from GET {path}") from exc
            rate_limited = self._is_rate_limited(response)
            transient = rate_limited or 500 <= response.status_code <= 599
            if not transient or attempt == self.max_attempts:
                raise GitHubApiError(f"GitHub GET {path} failed with HTTP {response.status_code}")
            delay = self._delay(response, attempt)
            if delay > self.max_wait:
                raise GitHubApiError(
                    f"GitHub rate limit for GET {path} requires waiting over {self.max_wait} s; retry later"
                )
            LOGGER.warning("GitHub GET %s returned HTTP %s; retrying in %.0f s", path, response.status_code, delay)
            self.sleep(delay)
        raise AssertionError("unreachable")

    def get_repository(self, full_name: str) -> dict:
        data, _ = self._get(f"{API_ROOT}/repos/{full_name}")
        if not isinstance(data, dict):
            raise GitHubApiError("Repository response is not an object")
        return data

    def _pages(self, url: str, params: dict):
        visited = set()
        while url:
            if url in visited:
                raise GitHubApiError("GitHub pagination repeated a page URL")
            visited.add(url)
            data, headers = self._get(url, params=params)
            if not isinstance(data, list):
                raise GitHubApiError("List response is not an array")
            yield data
            url = self._next_link(headers.get("Link"))
            if url:
                self._safe_url(url)
            params = None

    def iter_issues(self, full_name: str):
        return self._pages(
            f"{API_ROOT}/repos/{full_name}/issues",
            {"state": "all", "sort": "created", "direction": "desc", "per_page": 100},
        )

    def iter_issue_events(self, full_name: str, issue_number: int):
        return self._pages(
            f"{API_ROOT}/repos/{full_name}/issues/{issue_number}/events",
            {"per_page": 100},
        )
