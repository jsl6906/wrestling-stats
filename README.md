# Wrestling Stats

Scrape wrestling tournament data from TrackWrestling, parse match results, and calculate Elo ratings.
Data is stored in Azure Database for PostgreSQL (schema `trackwrestling`) using Microsoft Entra ID authentication; there are no database passwords.

## Setup

1. Install dependencies: `uv sync`
2. Sign in to Azure: `az login` (your account must be allowed on the Postgres server firewall)
3. Configure: copy `.env.example` to `.env` and set the governing body variables.
4. First-time only (Entra admin): `npm run bootstrap-db` creates the schema, tables and the CI role.

## Configuration

Set in `.env`:
- `GOVERNING_BODY_ID`: TrackWrestling's gbId (e.g., 38 for NYSPHSAA)
- `GOVERNING_BODY_ACRONYM`: Short identifier (e.g., NYSPHSAA); its lowercase form is the `gov_body` key in every table
- `GOVERNING_BODY_NAME`: Full name
- `PGHOST`, `PGDATABASE`: Postgres server and database
- `PGUSER`: Entra principal name (optional locally; derived from your token)

Known governing bodies live in `governing_bodies.json`; it drives the CI loop and the Observable site's routes.
Add a new entry there to onboard another governing body.

## Usage

- Scrape: `uv run code/scrape_tournaments.py --lookback-weeks 2`
- Parse: `uv run code/parse_round_html.py`
- Calculate Elo: `uv run code/calculate_elo.py`
- Explore data: `npm run db-ui` (DuckDB web UI with the Postgres schema attached as `pg`)
- Site: `npm run dev` / `npm run build` (Observable Framework; loaders query Postgres through DuckDB)

## CI/CD

`.github/workflows/deploy.yml` authenticates to Azure with GitHub OIDC (app registration `wrestling-stats-github`),
opens a temporary firewall rule for the runner, runs scrape → parse → Elo for every governing body, builds the site, and deploys to GitHub Pages.

Repository secrets: `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_SUBSCRIPTION_ID`.
Repository variables: `PGHOST`, `PGDATABASE`, `PGUSER`, `AZURE_PG_RESOURCE_GROUP`, `AZURE_PG_SERVER_NAME`.

## Project Structure

- `code/config.py`: Configuration
- `code/db.py`: Entra token acquisition, psycopg connection, DuckDB↔Postgres attach
- `code/scrape_tournaments.py`: Scrape tournaments
- `code/parse_round_html.py`: Parse matches
- `code/calculate_elo.py`: Elo ratings
- `db/schema.sql`: Idempotent DDL (`db/init_schema.py` applies it)
- `db/bootstrap_postgres.py`: One-time role/schema/grant bootstrap (Entra admin)
- `observable-framework-app/`: Static site
