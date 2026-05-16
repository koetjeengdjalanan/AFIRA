"""Tests for shared runtime environment access."""

from pathlib import Path

import pytest

from config.runtime import get_environment, initialize_environment, reset_environment_for_tests
from models import EnvironmentsVariables


def test_runtime_environment_is_initialized_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The shared runtime environment should be created once and reused."""
    monkeypatch.chdir(tmp_path)
    reset_environment_for_tests()
    first_environment = EnvironmentsVariables(loop_sleep_seconds=3, compatibility_mode=False, verbose=False)
    second_environment = EnvironmentsVariables(loop_sleep_seconds=9, compatibility_mode=False, verbose=False)

    try:
        assert initialize_environment(env_vars=first_environment) is first_environment
        assert initialize_environment(env_vars=second_environment) is first_environment
        assert get_environment() is first_environment
    finally:
        reset_environment_for_tests()


def test_initialize_environment_loads_dotenv_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Startup initialization should load dotenv values into the typed environment object."""
    monkeypatch.chdir(tmp_path)
    reset_environment_for_tests()
    for key in (
        "DEBUG_MODE",
        "LOG_FILE_PATH",
        "LOG_INFLUXDB_ENABLED",
        "LOG_INFLUXDB_LEVEL",
        "INFLUXDB_URL",
        "INFLUXDB_CONNECTION_POOL_MAXSIZE",
    ):
        monkeypatch.delenv(key, raising=False)

    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            (
                "DEBUG_MODE=True",
                "LOG_FILE_PATH=./runtime.log",
                "LOG_INFLUXDB_LEVEL=WARNING",
                "INFLUXDB_URL=http://dotenv-influxdb:8086",
                "INFLUXDB_CONNECTION_POOL_MAXSIZE=15",
            )
        ),
        encoding="utf-8",
    )

    try:
        env_vars = initialize_environment(env_file=env_file)

        assert env_vars.debug_mode is True
        assert env_vars.logging.log_file_path == (tmp_path / "runtime.log").absolute()
        assert env_vars.logging.log_influxdb_enabled is True
        assert env_vars.logging.log_influxdb_level == "WARNING"
        assert env_vars.influxdb.url == "http://dotenv-influxdb:8086"
        assert env_vars.influxdb.connection_pool_maxsize == 15
    finally:
        reset_environment_for_tests()
