"""
PostgreSQL connectivity for wrestling-stats using Microsoft Entra ID authentication.

- Locally: DefaultAzureCredential (az login / VS Code / env vars).
- GitHub Actions: ClientAssertionCredential using the job's OIDC token, so each process
  mints a fresh token regardless of how long the job has been running.

Writes use psycopg; reads may use DuckDB with the postgres extension via duckdb_attach_postgres().
"""

from __future__ import annotations

import base64
import json
import logging
import os
from pathlib import Path
from typing import Optional

import psycopg
from azure.core.credentials import AccessToken, TokenCredential

try:
    from .config import PGHOST, PGDATABASE, PGUSER, PG_SCHEMA, GOV_BODY, GOVERNING_BODY_ID, GOVERNING_BODY_ACRONYM, GOVERNING_BODY_NAME
except ImportError:
    from config import PGHOST, PGDATABASE, PGUSER, PG_SCHEMA, GOV_BODY, GOVERNING_BODY_ID, GOVERNING_BODY_ACRONYM, GOVERNING_BODY_NAME

log = logging.getLogger(__name__)
logging.getLogger("azure").setLevel(logging.WARNING)

PG_TOKEN_SCOPE = "https://ossrdbms-aad.database.windows.net/.default"
GITHUB_OIDC_AUDIENCE = "api://AzureADTokenExchange"
SCHEMA_SQL_PATH = Path(__file__).parent.parent / "db" / "schema.sql"

_credential: Optional[TokenCredential] = None


def _github_oidc_assertion() -> str:
    import httpx

    url = os.environ["ACTIONS_ID_TOKEN_REQUEST_URL"]
    bearer = os.environ["ACTIONS_ID_TOKEN_REQUEST_TOKEN"]
    resp = httpx.get(
        url,
        params={"audience": GITHUB_OIDC_AUDIENCE},
        headers={"Authorization": f"bearer {bearer}", "Accept": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["value"]


def get_credential() -> TokenCredential:
    global _credential
    if _credential is not None:
        return _credential

    if os.getenv("ACTIONS_ID_TOKEN_REQUEST_URL"):
        from azure.identity import ClientAssertionCredential

        _credential = ClientAssertionCredential(
            tenant_id=os.environ["AZURE_TENANT_ID"],
            client_id=os.environ["AZURE_CLIENT_ID"],
            func=_github_oidc_assertion,
        )
    else:
        from azure.identity import DefaultAzureCredential

        # No managed identity here; skipping IMDS avoids a multi-second probe timeout
        _credential = DefaultAzureCredential(
            exclude_interactive_browser_credential=True,
            exclude_managed_identity_credential=True,
        )
    return _credential


def get_pg_token() -> AccessToken:
    return get_credential().get_token(PG_TOKEN_SCOPE)


def _token_claims(token: str) -> dict:
    payload = token.split(".")[1]
    payload += "=" * (-len(payload) % 4)
    return json.loads(base64.urlsafe_b64decode(payload))


def get_pg_user(token: Optional[str] = None) -> str:
    """PGUSER if set; otherwise derive the Entra principal name from the token."""
    if PGUSER:
        return PGUSER
    if token is None:
        token = get_pg_token().token
    claims = _token_claims(token)
    user = claims.get("upn") or claims.get("preferred_username") or claims.get("unique_name")
    if not user:
        raise RuntimeError("PGUSER not set and token has no upn/preferred_username claim")
    return user


def get_conninfo(token: Optional[str] = None, dbname: str = PGDATABASE) -> str:
    if token is None:
        token = get_pg_token().token
    user = get_pg_user(token)
    return f"host={PGHOST} dbname={dbname} user={user} password={token} sslmode=require"


def get_pg_connection(autocommit: bool = True, dbname: str = PGDATABASE) -> psycopg.Connection:
    conn = psycopg.connect(get_conninfo(dbname=dbname), autocommit=autocommit, options=f"-c search_path={PG_SCHEMA}")
    return conn


def duckdb_attach_postgres(read_only: bool = True, alias: str = "pg"):
    """In-memory DuckDB with the Postgres schema attached as `alias` (tables as alias.table)."""
    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres;")
    ro = ", READ_ONLY" if read_only else ""
    # ATTACH does not accept bound parameters; conninfo is host/user/JWT (no quotes)
    conninfo = get_conninfo().replace("'", "''")
    con.execute(f"ATTACH '{conninfo}' AS {alias} (TYPE postgres, SCHEMA '{PG_SCHEMA}'{ro})")
    return con


def export_parquet_to_stdout(select_sql: str, params: list) -> None:
    """Run a DuckDB SELECT against the attached Postgres schema (alias `pg`) and stream Parquet to stdout."""
    import sys
    import tempfile
    from pathlib import Path as _Path

    con = duckdb_attach_postgres(read_only=True)
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
    tmp.close()  # DuckDB needs to open it for writing
    try:
        # Bound params inside COPY (...) return 0 rows with the postgres scanner; materialize first
        con.execute(f"CREATE TEMP TABLE _export AS {select_sql}", params)
        con.execute("COPY _export TO ? (FORMAT 'parquet')", [tmp.name])
        sys.stdout.buffer.write(_Path(tmp.name).read_bytes())
        sys.stdout.flush()
    finally:
        con.close()
        try:
            os.unlink(tmp.name)
        except OSError:
            pass


def ensure_schema(conn: psycopg.Connection) -> None:
    conn.execute(SCHEMA_SQL_PATH.read_text(encoding="utf-8"))


def upsert_governing_body(conn: psycopg.Connection) -> None:
    conn.execute(
        """--sql
        INSERT INTO governing_bodies (gov_body, tw_gb_id, acronym, name)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (gov_body) DO UPDATE SET
            tw_gb_id = EXCLUDED.tw_gb_id,
            acronym = EXCLUDED.acronym,
            name = EXCLUDED.name
        """,
        [GOV_BODY, GOVERNING_BODY_ID, GOVERNING_BODY_ACRONYM, GOVERNING_BODY_NAME],
    )
