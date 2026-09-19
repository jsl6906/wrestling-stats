"""
Configuration module for wrestling-stats.

Loads governing body and database settings from environment variables (via .env file).

Environment Variables:
    GOVERNING_BODY_ID: Numeric ID for TrackWrestling's gbId parameter
    GOVERNING_BODY_ACRONYM: Short identifier; lowercased as the gov_body discriminator in the DB
    GOVERNING_BODY_NAME: Full display name
    PGHOST / PGDATABASE / PGUSER: Azure Database for PostgreSQL connection settings (Entra ID auth)
    PG_SCHEMA: Schema holding all wrestling tables (default: trackwrestling)
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

# Short acronym; lowercase form is the gov_body key in every table
GOVERNING_BODY_ACRONYM: str = os.getenv("GOVERNING_BODY_ACRONYM", "NVWF")

# Full display name
GOVERNING_BODY_NAME: str = os.getenv(
    "GOVERNING_BODY_NAME",
    "Northern Virginia Wrestling Federation"
)

GOV_BODY: str = GOVERNING_BODY_ACRONYM.lower()


# ----- PostgreSQL Configuration -----

PGHOST: str = os.getenv("PGHOST", "jsl6906.postgres.database.azure.com")
PGDATABASE: str = os.getenv("PGDATABASE", "personal_storage")
# Entra principal name (UPN locally, app registration name in CI)
PGUSER: str = os.getenv("PGUSER", "")
PG_SCHEMA: str = os.getenv("PG_SCHEMA", "trackwrestling")


