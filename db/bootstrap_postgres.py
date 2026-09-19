"""
One-time (idempotent) Postgres bootstrap. Must be run by a Microsoft Entra admin of the server.

- Creates the Entra-backed Postgres role for the GitHub Actions service principal
- Creates the application schema and grants the CI role access to it (incl. future objects)
- Applies db/schema.sql

Run with uv (after `az login`):
    uv run db/bootstrap_postgres.py --ci-principal wrestling-stats-github
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "code"))

from psycopg import sql  # noqa: E402

from config import PGDATABASE, PG_SCHEMA  # noqa: E402
from db import ensure_schema, get_pg_connection  # noqa: E402


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    log = logging.getLogger("bootstrap")

    ap = argparse.ArgumentParser(description="Bootstrap Postgres schema and CI role (Entra admin only)")
    ap.add_argument("--ci-principal", required=True, help="Entra app registration display name used by GitHub Actions")
    args = ap.parse_args()
    role = args.ci_principal

    # pgaadauth_* functions exist only in the maintenance DB; roles are cluster-wide
    admin = get_pg_connection(dbname="postgres")
    try:
        exists = admin.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", [role]).fetchone()
        if exists:
            log.info("Role %s already exists", role)
        else:
            log.info("Creating Entra principal role %s", role)
            admin.execute("SELECT * FROM pgaadauth_create_principal(%s, false, false)", [role])
    finally:
        admin.close()

    conn = get_pg_connection()
    try:
        log.info("Ensuring schema %s", PG_SCHEMA)
        conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(PG_SCHEMA)))

        r = sql.Identifier(role)
        s = sql.Identifier(PG_SCHEMA)
        for stmt in (
            sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(sql.Identifier(PGDATABASE), r),
            sql.SQL("GRANT USAGE, CREATE ON SCHEMA {} TO {}").format(s, r),
            sql.SQL("GRANT ALL ON ALL TABLES IN SCHEMA {} TO {}").format(s, r),
            sql.SQL("GRANT ALL ON ALL SEQUENCES IN SCHEMA {} TO {}").format(s, r),
            sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT ALL ON TABLES TO {}").format(s, r),
            sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT ALL ON SEQUENCES TO {}").format(s, r),
        ):
            conn.execute(stmt)

        log.info("Applying db/schema.sql")
        ensure_schema(conn)
        # Tables created above by the admin need explicit grants (default privileges only cover future objects of this role)
        conn.execute(sql.SQL("GRANT ALL ON ALL TABLES IN SCHEMA {} TO {}").format(s, r))
        conn.execute(sql.SQL("GRANT ALL ON ALL SEQUENCES IN SCHEMA {} TO {}").format(s, r))
        log.info("Bootstrap complete")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
