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
