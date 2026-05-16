"""Tests for the HPE OAuth API client retry behavior."""

import json
import logging
from typing import Any

import pytest
from requests import Response
from requests.exceptions import RequestException

from models import HPEOAuth2Client


class _FakeOAuthSession:
    """Minimal OAuth session fake for exercising HPEOAuth2Client retries."""

    def __init__(self, outcomes: list[Response | RequestException]) -> None:
        self._outcomes = outcomes
        self.requests: list[tuple[str, str]] = []

    def fetch_token(self, token_url: str, client_secret: str) -> dict[str, Any]:
        """Return a static token response."""
        return {"access_token": "token", "expires_in": 3600, "token_url": token_url, "client_secret": client_secret}

    def request(self, method: str, url: str, **kwargs: Any) -> Response:
        """Return or raise the next configured outcome."""
        self.requests.append((method, url))
        outcome = self._outcomes.pop(0)
        if isinstance(outcome, RequestException):
            raise outcome
        return outcome

    def close(self) -> None:
        """Close the fake session."""


def _response(status_code: int, payload: dict[str, Any]) -> Response:
    """Build a typed JSON response."""
    response = Response()
    response.status_code = status_code
    response.url = "https://api.example.com/resource"
    response._content = json.dumps(payload).encode("utf-8")
    response.headers["Content-Type"] = "application/json"
    return response


def _client(fake_oauth: _FakeOAuthSession, retry_attempts: int = 2) -> HPEOAuth2Client:
    """Build an HPE client using a fake OAuth session."""
    client = HPEOAuth2Client(
        token_url="https://auth.example.com/oauth2/token",
        base_url="https://api.example.com",
        client_id="00000000-0000-4000-8000-000000000000",
        client_secret="secret",
        retry_attempts=retry_attempts,
        retry_min_seconds=1,
        retry_max_seconds=1,
    )
    client._oauth = fake_oauth
    return client


def test_request_retries_retryable_response_and_logs_warning(caplog: pytest.LogCaptureFixture) -> None:
    """A retryable HTTP response should be retried and logged before the next attempt."""
    fake_oauth = _FakeOAuthSession(
        outcomes=[
            _response(500, {"error": "temporary"}),
            _response(200, {"response": {"status": "SUCCESS"}, "items": []}),
        ]
    )
    client = _client(fake_oauth=fake_oauth)

    with caplog.at_level(logging.WARNING, logger="AFIRA.HPEOAuth2Client"):
        response = client.get("/resource")

    assert response.status_code == 200
    assert fake_oauth.requests == [
        ("GET", "https://api.example.com/resource"),
        ("GET", "https://api.example.com/resource"),
    ]
    assert "returned HTTP 500" in caplog.text
    assert "Retrying attempt 2/2" in caplog.text


def test_request_logs_exhausted_retryable_response(caplog: pytest.LogCaptureFixture) -> None:
    """The final retryable response should be logged when all attempts are exhausted."""
    fake_oauth = _FakeOAuthSession(
        outcomes=[
            _response(500, {"error": "temporary"}),
            _response(503, {"error": "still failing"}),
        ]
    )
    client = _client(fake_oauth=fake_oauth)

    with caplog.at_level(logging.WARNING, logger="AFIRA.HPEOAuth2Client"):
        response = client.get("/resource")

    assert response.status_code == 503
    assert len(fake_oauth.requests) == 2
    assert "returned HTTP 500" in caplog.text
    assert "still returned HTTP 503 after 2/2 attempts" in caplog.text


def test_request_retries_request_exception_and_logs_warning(caplog: pytest.LogCaptureFixture) -> None:
    """A requests transport exception should be retried and logged."""
    fake_oauth = _FakeOAuthSession(
        outcomes=[
            RequestException("connection dropped"),
            _response(200, {"response": {"status": "SUCCESS"}, "items": []}),
        ]
    )
    client = _client(fake_oauth=fake_oauth)

    with caplog.at_level(logging.WARNING, logger="AFIRA.HPEOAuth2Client"):
        response = client.get("/resource")

    assert response.status_code == 200
    assert len(fake_oauth.requests) == 2
    assert "failed with RequestException: connection dropped" in caplog.text
    assert "Retrying attempt 2/2" in caplog.text


def test_request_logs_exhausted_request_exception(caplog: pytest.LogCaptureFixture) -> None:
    """The final transport exception should be logged when all attempts fail."""
    fake_oauth = _FakeOAuthSession(
        outcomes=[
            RequestException("connection dropped"),
            RequestException("still down"),
        ]
    )
    client = _client(fake_oauth=fake_oauth)

    with caplog.at_level(logging.WARNING, logger="AFIRA.HPEOAuth2Client"):
        with pytest.raises(RequestException, match="still down"):
            client.get("/resource")

    assert len(fake_oauth.requests) == 2
    assert "failed with RequestException: connection dropped" in caplog.text
    assert "failed after 2/2 attempts with RequestException: still down" in caplog.text
