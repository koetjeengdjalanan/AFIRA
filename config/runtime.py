"""Shared runtime environment access for AFIRA."""

from pathlib import Path

from dotenv import load_dotenv

from models import EnvironmentsVariables

_ENVIRONMENT: EnvironmentsVariables | None = None


def initialize_environment(
    env_file: Path | None = None,
    env_vars: EnvironmentsVariables | None = None,
) -> EnvironmentsVariables:
    """Initialize and return the single runtime environment object."""
    global _ENVIRONMENT

    if _ENVIRONMENT is not None:
        return _ENVIRONMENT

    if env_vars is None:
        _ = load_dotenv(dotenv_path=env_file)
        env_vars = EnvironmentsVariables()

    _ENVIRONMENT = env_vars
    return _ENVIRONMENT


def get_environment() -> EnvironmentsVariables:
    """Return the initialized runtime environment object."""
    if _ENVIRONMENT is None:
        raise RuntimeError("AFIRA environment has not been initialized. Call initialize_environment() at startup.")
    return _ENVIRONMENT


def reset_environment_for_tests() -> None:
    """Reset the runtime environment singleton for isolated tests."""
    global _ENVIRONMENT

    _ENVIRONMENT = None
