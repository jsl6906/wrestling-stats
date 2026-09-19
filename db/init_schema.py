"""
Apply db/schema.sql (idempotent). Safe to run as the CI service principal.

Run with uv:
    uv run db/init_schema.py
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "code"))

from db import ensure_schema, get_pg_connection  # noqa: E402


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    conn = get_pg_connection()
    try:
        ensure_schema(conn)
        logging.getLogger("init_schema").info("Schema is up to date")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
