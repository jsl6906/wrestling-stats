"""
DuckDB UI Launcher
Attaches the Postgres `trackwrestling` schema (Entra ID auth) as `pg` and launches the DuckDB web UI.
Requires `az login` and internet access for the web UI.

Run with uv:
    uv run db/start_duckb_ui.py              # read-only
    uv run db/start_duckb_ui.py --write      # allow writes
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "code"))

from config import PGDATABASE, PGHOST, PG_SCHEMA  # noqa: E402
from db import duckdb_attach_postgres  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description="Launch the DuckDB web UI attached to Postgres")
    ap.add_argument("--write", action="store_true", help="Attach read-write instead of read-only")
    args = ap.parse_args()

    print(f"Attaching {PGHOST}/{PGDATABASE} schema {PG_SCHEMA} as 'pg' ({'read-write' if args.write else 'read-only'})")
    try:
        conn = duckdb_attach_postgres(read_only=not args.write)
        print("Query tables as pg.<table>, e.g. SELECT gov_body, COUNT(*) FROM pg.matches GROUP BY 1;")
        print("\nStarting DuckDB UI... The web interface will open in your browser.")
        print("Press Enter to stop the UI and exit...")
        conn.execute("CALL start_ui();")
        input()
        conn.close()
        print("Connection closed.")
    except Exception as e:
        print(f"Error: {e}")
        print("Make sure you are logged in with `az login` and your IP is allowed by the server firewall.")


if __name__ == "__main__":
    main()
