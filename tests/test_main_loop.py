"""Tests for AFIRA container loop behavior."""

import threading
from pathlib import Path
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock

import pytest

import main
from models import EnvironmentsVariables


def _env(loop_sleep_seconds: int) -> EnvironmentsVariables:
    """Return a minimal typed environment object for loop-only tests."""
    return cast(EnvironmentsVariables, SimpleNamespace(loop_sleep_seconds=loop_sleep_seconds))


def _run_env() -> EnvironmentsVariables:
    """Return a minimal typed environment object for run_once tests."""
    return cast(
        EnvironmentsVariables,
        SimpleNamespace(
            loop_sleep_seconds=5,
            debug_mode=False,
            influxdb=SimpleNamespace(),
        ),
    )


def test_successful_cycle_sleeps_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """A successful iteration should sleep before the next cycle."""
    shutdown_event = threading.Event()
    run_once_mock: Mock = Mock(return_value=3)
    monkeypatch.setattr(main, "run_once", run_once_mock)

    def wait_once(timeout: float | None = None) -> bool:
        shutdown_event.set()
        return True

    wait_mock: Mock = Mock(side_effect=wait_once)
    monkeypatch.setattr(shutdown_event, "wait", wait_mock)

    main.run_forever(env_vars=_env(loop_sleep_seconds=5), shutdown_event=shutdown_event)

    run_once_mock.assert_called_once()
    wait_mock.assert_called_once_with(timeout=5)


def test_failed_cycle_logs_sleeps_and_continues(monkeypatch: pytest.MonkeyPatch) -> None:
    """A failed iteration should not stop the container loop."""
    shutdown_event = threading.Event()
    wait_timeouts: list[float | None] = []
    run_once_mock: Mock = Mock(side_effect=[RuntimeError("temporary failure"), 9])
    monkeypatch.setattr(main, "run_once", run_once_mock)

    def wait_until_second_cycle(timeout: float | None = None) -> bool:
        wait_timeouts.append(timeout)
        if len(wait_timeouts) == 2:
            shutdown_event.set()
            return True
        return False

    monkeypatch.setattr(shutdown_event, "wait", Mock(side_effect=wait_until_second_cycle))

    main.run_forever(env_vars=_env(loop_sleep_seconds=11), shutdown_event=shutdown_event)

    assert run_once_mock.call_count == 2
    assert wait_timeouts == [11, 11]


def test_shutdown_event_exits_without_starting_cycle(monkeypatch: pytest.MonkeyPatch) -> None:
    """A pre-set shutdown event should prevent any collection cycle from starting."""
    shutdown_event = threading.Event()
    shutdown_event.set()
    run_once_mock: Mock = Mock()
    wait_mock: Mock = Mock()
    monkeypatch.setattr(main, "run_once", run_once_mock)
    monkeypatch.setattr(shutdown_event, "wait", wait_mock)

    main.run_forever(env_vars=_env(loop_sleep_seconds=5), shutdown_event=shutdown_event)

    run_once_mock.assert_not_called()
    wait_mock.assert_not_called()


def test_loop_sleep_seconds_reads_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """AFIRA_LOOP_SLEEP_SECONDS should configure the typed environment model."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("AFIRA_LOOP_SLEEP_SECONDS", "42")

    env_vars = EnvironmentsVariables()

    assert env_vars.loop_sleep_seconds == 42
    assert env_vars.logging.log_file_path.is_file()


def test_run_once_skips_aruba_when_new_central_credentials_are_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """run_once should skip Aruba when the new_central credential block is missing."""
    monkeypatch.setattr(main, "retrieve_creds", Mock(return_value={"fortigate": {"base_url": "https://x", "api_token": "token"}}))
    monkeypatch.setattr(main, "test_db_setup", Mock())
    aruba_mock: Mock = Mock(return_value=[])
    fortigate_mock: Mock = Mock(return_value=[])
    monkeypatch.setattr(main, "_run_aruba_fetcher", aruba_mock)
    monkeypatch.setattr(main, "_run_fortigate_fetcher", fortigate_mock)
    monkeypatch.setattr(main, "store_points", Mock(return_value=None))
    monkeypatch.setattr(main.asyncio, "run", Mock(return_value=None))

    result = main.run_once(env_vars=_run_env())

    assert result == 0
    aruba_mock.assert_not_called()
    fortigate_mock.assert_called_once()


def test_run_once_skips_fortigate_when_fortigate_credentials_are_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """run_once should skip Fortigate when the fortigate credential block is missing."""
    monkeypatch.setattr(
        main,
        "retrieve_creds",
        Mock(return_value={
            "new_central": {
                "token_url": "https://example.com/oauth2/token",
                "base_url": "https://example.com",
                "client_id": "00000000-0000-0000-0000-000000000000",
                "client_secret": "secret",
            },
        }),
    )
    monkeypatch.setattr(main, "test_db_setup", Mock())
    aruba_mock: Mock = Mock(return_value=[])
    fortigate_mock: Mock = Mock(return_value=[])
    monkeypatch.setattr(main, "_run_aruba_fetcher", aruba_mock)
    monkeypatch.setattr(main, "_run_fortigate_fetcher", fortigate_mock)
    monkeypatch.setattr(main, "store_points", Mock(return_value=None))
    monkeypatch.setattr(main.asyncio, "run", Mock(return_value=None))

    result = main.run_once(env_vars=_run_env())

    assert result == 0
    aruba_mock.assert_called_once()
    fortigate_mock.assert_not_called()
