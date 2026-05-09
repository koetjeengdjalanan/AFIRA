"""Pytest configuration for script-style AFIRA modules."""

import sys
from pathlib import Path

PROJECT_ROOT: Path = Path(__file__).resolve().parents[1]
PROJECT_ROOT_PATH: str = str(PROJECT_ROOT)

if PROJECT_ROOT_PATH not in sys.path:
    sys.path.insert(0, PROJECT_ROOT_PATH)
