# Wrestling Stats

Scrape wrestling tournament data from TrackWrestling, parse match results, and calculate Elo ratings.
Data is stored in Azure Database for PostgreSQL (schema `trackwrestling`) using Microsoft Entra ID authentication; there are no database passwords.

## Setup

1. Install dependencies with Python 3.13 (see [`.python-version`](./.python-version)) and Node.js 22 (the CI runtime): `uv sync --locked` and `npm ci --prefix observable-framework-app`.
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
Add a new entry there to onboard another governing body. An optional `usaw_results` object (`name_pattern`, `since`)
also imports that body's events from the USA Wrestling results site (USA Bracketing, shown on [TheMat.com/results](https://www.themat.com/results)).

## Usage

- Scrape: `uv run code/scrape_tournaments.py --lookback-weeks 2`
- Parse: `uv run code/parse_round_html.py`
- Import USA Wrestling results: `uv run code/scrape_usaw_results.py --name-pattern '^NVWF\b' --since 2026-07-01`
  - Set `GOVERNING_BODY_ID`, `GOVERNING_BODY_ACRONYM`, and `GOVERNING_BODY_NAME` for the target body before importing, including with `--event-id`; event IDs do not choose a governing body.
  - New events are stored as `usaw-<id>`. Events already loaded (by ID, or by the same name and date from TrackWrestling) skip bout imports so Elo never double-counts bouts.
  - Each event's final bracket placements (age group, weight, place, wrestler, team) are stored in `placements`. If TrackWrestling already holds the same-name/date event, placements are saved under that existing event ID without modifying bouts or Elo. Missing placements are re-checked within the discovery window (or pass `--event-id`), since placements are often posted after results.
  - Placement age-group and weight labels are preserved from USA Wrestling. Their combined `weight_class` matches USA Wrestling bouts, but TrackWrestling may use different division labels; do not assume an exact weight-class join across sources.
  - `--event-id 19988 --dry-run` parses specific events without writing and compares them with the matching TrackWrestling event.
  - `--replace` re-imports events already loaded from USA Wrestling; then run `calculate_elo.py --recalculate`.
  - `--supersede-incomplete` replaces a TrackWrestling event when USA Wrestling reproduces every stored bout exactly and has additional bouts (e.g. rounds TrackWrestling never published); then run `calculate_elo.py --recalculate`. The TrackWrestling scraper skips events whose name and date match a USA Wrestling import, and `parse_round_html.py --reparse` keeps imported bouts.
  - `--supersede` (with `--event-id`) replaces the same-name/date TrackWrestling event even when bouts differ, for events reviewed by hand (e.g. NYSPHSAA `--event-id 18029 --event-id 18308 --event-id 18321`).
  - Events with a different name but the same date (±1 day) that share at least half their winner/loser pairs with an existing event are skipped, never superseded.
  - `--replaced-flag-file PATH` appends a line whenever existing matches are replaced; CI uses it to run `calculate_elo.py --recalculate` instead of an incremental update.
- Calculate Elo: `uv run code/calculate_elo.py`
- Explore data: `npm run db-ui` (DuckDB web UI with the Postgres schema attached as `pg`)
- Site: `npm run dev` / `npm run build` (Observable Framework; loaders query Postgres through DuckDB)

## CI/CD

`.github/workflows/deploy.yml` authenticates to Azure with GitHub OIDC (app registration `wrestling-stats-github`),
opens a temporary firewall rule for the runner, runs scrape → parse → USA Wrestling import (when configured; currently NVWF `^NVWF` and VA_USA `VAWA`/`VA USA` events, last 8 weeks, with `--supersede-incomplete`) → Elo for every governing body, builds the site, and deploys to GitHub Pages. Elo is fully recalculated for a body when the import replaced existing matches.

Repository secrets: `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_SUBSCRIPTION_ID`.
Repository variables: `PGHOST`, `PGDATABASE`, `PGUSER`, `AZURE_PG_RESOURCE_GROUP`, `AZURE_PG_SERVER_NAME`.

## Dependency maintenance

Dependabot opens weekly update PRs for Python (`uv`), the Observable app (`npm`), and GitHub Actions. CI installs from the committed lockfiles, pins actions to release commits, and pins the runner to Ubuntu 26.04 so runner upgrades can be tested separately with Playwright.

For manual refreshes, run `uv lock --upgrade` and `npm update --prefix observable-framework-app`, then review the lockfile diffs and run `uv run --locked python test/test_parse_matches.py` and `uv run --locked python test/test_usaw_results.py`. Test DuckDB and pandas major upgrades separately with the PostgreSQL-backed site loaders before changing their version bounds.

## Project Structure

- `code/config.py`: Configuration
- `code/db.py`: Entra token acquisition, psycopg connection, DuckDB↔Postgres attach
- `code/scrape_tournaments.py`: Scrape tournaments
- `code/parse_round_html.py`: Parse matches
- `code/scrape_usaw_results.py`: Import USA Wrestling (USA Bracketing) results and placements
- `code/calculate_elo.py`: Elo ratings
- `db/schema.sql`: Idempotent DDL (`db/init_schema.py` applies it)
- `db/bootstrap_postgres.py`: One-time role/schema/grant bootstrap (Entra admin)
- `observable-framework-app/`: Static site
