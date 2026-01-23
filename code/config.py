"""
Configuration module for wrestling-stats.

Loads governing body settings from environment variables (via .env file).
Use these values throughout the codebase instead of hardcoding organization-specific values.

Environment Variables:
    GOVERNING_BODY_ID: Numeric ID for TrackWrestling's gbId parameter (default: 38)
    GOVERNING_BODY_ACRONYM: Short identifier for DB names, etc. (default: NYSPHSAA)
    GOVERNING_BODY_NAME: Full display name (default: New York State Public High School Athletic Association)
"""

from __future__ import annotations

import os
from pathlib import Path

# Attempt to load .env from project root
try:
    from dotenv import load_dotenv
    # Find project root (parent of 'code' directory)
    _project_root = Path(__file__).parent.parent
    _env_path = _project_root / ".env"
    if _env_path.exists():
        load_dotenv(_env_path)
except ImportError:
    # python-dotenv not installed; rely on environment variables being set externally
    pass


# ----- Governing Body Configuration -----

# Numeric ID used in TrackWrestling's gbId query parameter
GOVERNING_BODY_ID: int = int(os.getenv("GOVERNING_BODY_ID", "230728132"))

# Short acronym for database filenames (lowercase used in paths)
GOVERNING_BODY_ACRONYM: str = os.getenv("GOVERNING_BODY_ACRONYM", "NVWF")

# Full display name
GOVERNING_BODY_NAME: str = os.getenv(
    "GOVERNING_BODY_NAME",
    "Northern Virginia Wrestling Federation"
)


# ----- Derived Values -----

def get_db_filename() -> str:
    """Return the database filename based on the governing body acronym."""
    return f"trackwrestling_{GOVERNING_BODY_ACRONYM.lower()}.db"


def get_db_path() -> Path:
    """Return the full path to the database file."""
    project_root = Path(__file__).parent.parent
    return project_root / "output" / get_db_filename()


# Convenience alias for backwards compatibility
DB_PATH = get_db_path()

