import unittest
from unittest.mock import patch

import requests

from src.stg.github_client import GitHubClient, GitHubApiError


class Response:
    def __init__(self, status, data, headers=None):
        self.status_code = status
        self._data = data
        self.headers = headers or {}

    def json(self):
        return self._data


class Session:
    def __init__(self, responses):
        self.headers = {}
        self.responses = iter(responses)
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        result = next(self.responses)
        if isinstance(result, Exception):
            raise result
        return result


class GitHubClientTests(unittest.TestCase):
    def test_pagination_uses_link_and_authorization(self):
        session = Session([
            Response(200, [{"id": 1}], {"Link": '<https://api.github.com/repos/a/b/issues?page=2>; rel="next"'}),
            Response(200, [{"id": 2}]),
        ])
        client = GitHubClient("test-token", session=session, sleep=lambda _: None)
        pages = list(client.iter_issues("a/b"))
        self.assertEqual(pages, [[{"id": 1}], [{"id": 2}]])
        self.assertEqual(session.headers["Authorization"], "Bearer test-token")
        self.assertEqual(session.calls[0][2]["params"]["state"], "all")
        self.assertEqual(session.calls[0][2]["params"]["per_page"], 100)
        self.assertEqual(session.calls[1][1], "https://api.github.com/repos/a/b/issues?page=2")
        self.assertIsNone(session.calls[1][2]["params"])

    def test_retries_network_error_and_server_error_with_bound(self):
        delays = []
        session = Session([requests.Timeout(), Response(503, {}), Response(200, {"id": 1})])
        client = GitHubClient("token", session=session, sleep=delays.append)
        self.assertEqual(client.get_repository("a/b")["id"], 1)
        self.assertEqual(len(session.calls), 3)
        self.assertEqual(delays, [1, 2])

    def test_permission_403_does_not_retry(self):
        session = Session([Response(403, {"message": "Resource not accessible by personal access token"})])
        client = GitHubClient("token", session=session, sleep=lambda _: None)
        with self.assertRaises(GitHubApiError):
            client.get_repository("a/b")
        self.assertEqual(len(session.calls), 1)

    def test_rate_limit_uses_retry_after(self):
        delays = []
        session = Session([Response(429, {}, {"Retry-After": "3"}), Response(200, {"id": 1})])
        client = GitHubClient("token", session=session, sleep=delays.append)
        self.assertEqual(client.get_repository("a/b")["id"], 1)
        self.assertEqual(delays, [3])

    def test_rejects_untrusted_pagination_host_before_sending_token(self):
        session = Session([Response(200, [{"id": 1}], {"Link": '<https://evil.example/issues?page=2>; rel="next"'})])
        client = GitHubClient("token", session=session, sleep=lambda _: None)
        with self.assertRaises(GitHubApiError):
            list(client.iter_issues("a/b"))
        self.assertEqual(len(session.calls), 1)

    def test_repeated_next_link_fails_instead_of_looping(self):
        repeated = '<https://api.github.com/repos/a/b/issues?page=2>; rel="next"'
        session = Session([Response(200, [], {"Link": repeated}), Response(200, [], {"Link": repeated})])
        client = GitHubClient("token", session=session, sleep=lambda _: None)
        with self.assertRaises(GitHubApiError):
            list(client.iter_issues("a/b"))
        self.assertEqual(len(session.calls), 2)


if __name__ == "__main__":
    unittest.main()
