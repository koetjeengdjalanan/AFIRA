"""Tests for the Fortigate API client retry behavior."""

import json
import logging
from typing import Any
from unittest.mock import patch

import pytest
from requests import Response
from requests.exceptions import RequestException

from models import FortigateClient


class _FakeSendRequest:
    """Minimal fake for FortigateClient._send_request to control retry scenarios."""

    def __init__(self, outcomes: list[Response | RequestException]) -> None:
        self._outcomes = outcomes
        self.calls: list[tuple[str, str]] = []

    def __call__(self, method: str, url: str, **kwargs: Any) -> Response:
        """Return or raise the next configured outcome."""
        self.calls.append((method, url))
        outcome = self._outcomes.pop(0)
        if isinstance(outcome, RequestException):
            raise outcome
        return outcome


def _response(status_code: int, payload: dict[str, Any]) -> Response:
    """Build a typed JSON response."""
    r = Response()
    r.status_code = status_code
    r.url = "https://192.168.1.1/api/v2/monitor/system/status"
    r._content = json.dumps(payload).encode("utf-8")
    r.headers["Content-Type"] = "application/json"
    return r


def _client(retry_attempts: int = 2) -> FortigateClient:
    """Build a Fortigate client configured for testing."""
    return FortigateClient(
        base_url="https://192.168.1.1",
        api_token="test-api-token",
        retry_attempts=retry_attempts,
        retry_min_seconds=1,
        retry_max_seconds=1,
    )


def test_request_retries_retryable_response_and_logs_warning(caplog: pytest.LogCaptureFixture) -> None:
    """A retryable HTTP response should be retried and logged before the next attempt."""
    fake = _FakeSendRequest(
        outcomes=[
            _response(500, {"error": "temporary"}),
            _response(200, {"status": "SUCCESS", "results": []}),
        ]
    )
    client = _client()

    with patch.object(FortigateClient, "_send_request", new=fake):
        with caplog.at_level(logging.WARNING, logger="AFIRA.FortigateClient"):
            response = client.get("/api/v2/monitor/system/status")

    assert response.status_code == 200
    assert fake.calls == [
        ("GET", "https://192.168.1.1/api/v2/monitor/system/status"),
        ("GET", "https://192.168.1.1/api/v2/monitor/system/status"),
    ]
    assert "returned HTTP 500" in caplog.text
    assert "Retrying attempt 2/2" in caplog.text


def test_request_logs_exhausted_retryable_response(caplog: pytest.LogCaptureFixture) -> None:
    """The final retryable response should be logged when all attempts are exhausted."""
    fake = _FakeSendRequest(
        outcomes=[
            _response(500, {"error": "temporary"}),
            _response(503, {"error": "still failing"}),
        ]
    )
    client = _client()

    with patch.object(FortigateClient, "_send_request", new=fake):
        with caplog.at_level(logging.WARNING, logger="AFIRA.FortigateClient"):
            response = client.get("/api/v2/monitor/system/status")

    assert response.status_code == 503
    assert len(fake.calls) == 2
    assert "returned HTTP 500" in caplog.text
    assert "still returned HTTP 503 after 2/2 attempts" in caplog.text


def test_request_retries_request_exception_and_logs_warning(caplog: pytest.LogCaptureFixture) -> None:
    """A requests transport exception should be retried and logged."""
    fake = _FakeSendRequest(
        outcomes=[
            RequestException("connection dropped"),
            _response(200, {"status": "SUCCESS", "results": []}),
        ]
    )
    client = _client()

    with patch.object(FortigateClient, "_send_request", new=fake):
        with caplog.at_level(logging.WARNING, logger="AFIRA.FortigateClient"):
            response = client.get("/api/v2/monitor/system/status")

    assert response.status_code == 200
    assert len(fake.calls) == 2
    assert "failed with RequestException: connection dropped" in caplog.text
    assert "Retrying attempt 2/2" in caplog.text


def test_request_logs_exhausted_request_exception(caplog: pytest.LogCaptureFixture) -> None:
    """The final transport exception should be logged and re-raised when all attempts fail."""
    fake = _FakeSendRequest(
        outcomes=[
            RequestException("connection dropped"),
            RequestException("still down"),
        ]
    )
    client = _client()

    with patch.object(FortigateClient, "_send_request", new=fake):
        with caplog.at_level(logging.WARNING, logger="AFIRA.FortigateClient"):
            with pytest.raises(RequestException, match="still down"):
                client.get("/api/v2/monitor/system/status")

    assert len(fake.calls) == 2
    assert "failed with RequestException: connection dropped" in caplog.text
    assert "failed after 2/2 attempts with RequestException: still down" in caplog.text


def test_request_adds_authorization_header() -> None:
    """The Authorization header must carry the Bearer token for every request."""
    captured_headers: dict[str, str] = {}
    ok_response = _response(200, {"status": "SUCCESS"})

    def capture(method: str, url: str, **kwargs: Any) -> Response:
        captured_headers.update(kwargs.get("headers", {}))
        return ok_response

    client = _client()
    with patch.object(FortigateClient, "_send_request", new=staticmethod(capture)):
        client.get("/api/v2/monitor/system/status")

    assert captured_headers.get("Authorization") == "Bearer test-api-token"


def test_request_builds_correct_url() -> None:
    """The absolute URL must be built from base_url and the endpoint."""
    captured_urls: list[str] = []
    ok_response = _response(200, {"status": "SUCCESS"})

    def capture(method: str, url: str, **kwargs: Any) -> Response:
        captured_urls.append(url)
        return ok_response

    client = _client()
    with patch.object(FortigateClient, "_send_request", new=staticmethod(capture)):
        client.get("/api/v2/monitor/system/status")

    assert captured_urls == ["https://192.168.1.1/api/v2/monitor/system/status"]


def test_get_sends_get_method() -> None:
    """client.get() should issue a GET request."""
    captured_methods: list[str] = []
    ok_response = _response(200, {"status": "SUCCESS"})

    def capture(method: str, url: str, **kwargs: Any) -> Response:
        captured_methods.append(method)
        return ok_response
    
    client = _client()
    with patch.object(FortigateClient, "_send_request", new=staticmethod(capture)):
        client.get("/api/v2/monitor/system/status")

    assert captured_methods == ["GET"]


def test_post_sends_post_method() -> None:
    """client.post() should issue a POST request."""
    captured_methods: list[str] = []
    ok_response = _response(200, {"status": "SUCCESS"})

    def capture(method: str, url: str, **kwargs: Any) -> Response:
        captured_methods.append(method)
        return ok_response

    client = _client()
    with patch.object(FortigateClient, "_send_request", new=staticmethod(capture)):
        client.post("/api/v2/monitor/system/status")

    assert captured_methods == ["POST"]


def test_fortigate_error_status_triggers_retry(caplog: pytest.LogCaptureFixture) -> None:
    """A non-SUCCESS Fortigate body status should trigger a retry."""
    fake = _FakeSendRequest(
        outcomes=[
            _response(200, {"status": "error", "message": "device not ready"}),
            _response(200, {"status": "SUCCESS", "results": []}),
        ]
    )
    client = _client()

    with patch.object(FortigateClient, "_send_request", new=fake):
        with caplog.at_level(logging.WARNING, logger="AFIRA.FortigateClient"):
            response = client.get("/api/v2/monitor/system/status")

    assert response.status_code == 200
    assert len(fake.calls) == 2
    assert "Fortigate response status ERROR" in caplog.text
    assert "Retrying attempt 2/2" in caplog.text


def test_non_retryable_response_is_not_retried() -> None:
    """A successful 200 response must not trigger any retry."""
    fake = _FakeSendRequest(
        outcomes=[_response(200, {"status": "SUCCESS", "results": []})]
    )
    client = _client()

    with patch.object(FortigateClient, "_send_request", new=fake):
        response = client.get("/api/v2/monitor/system/status")

    assert response.status_code == 200
    assert len(fake.calls) == 1


def test_retry_response_reason_for_retryable_status_codes() -> None:
    """HTTP status codes 408, 409, 425, 429, and 5xx should produce a retry reason."""
    for code in [408, 409, 425, 429, 500, 502, 503, 504]:
        r = _response(code, {})
        reason = FortigateClient._retry_response_reason(r)
        assert reason is not None, f"Expected HTTP {code} to be retryable"
        assert str(code) in reason


def test_retry_response_reason_for_non_retryable_status_codes() -> None:
    """HTTP status codes 200, 201, 400, 401, 403, 404 must not produce a retry reason."""
    for code in [200, 201, 400, 401, 403, 404]:
        r = _response(code, {"status": "SUCCESS"})
        assert FortigateClient._retry_response_reason(r) is None, f"Expected HTTP {code} to NOT be retryable"
