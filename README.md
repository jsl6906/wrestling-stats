# Wrestling Stats

Scrape wrestling tournament data from TrackWrestling, parse match results, and calculate Elo ratings.

## Setup

1. Install dependencies with [uv](https://docs.astral.sh/uv/):
   ```bash
   uv sync
   ```

2. Configure your governing body (organization) by copying `.env.example` to `.env` and editing:
   ```bash
   cp .env.example .env
   ```

## Configuration

The project uses environment variables to configure which wrestling organization to scrape. Set these in a `.env` file at the project root:

| Variable | Description | Example Values |
|----------|-------------|----------------|
| `GOVERNING_BODY_ID` | TrackWrestling's numeric `gbId` parameter | `38` (NYSPHSAA)<br>`230728132` (NVWF)<br>`52` (VHSL)<br>`253734046` (VA_USA) |
| `GOVERNING_BODY_ACRONYM` | Short identifier used in database filenames | `NYSPHSAA`<br>`NVWF`<br>`VHSL`<br>`VA_USA` |
| `GOVERNING_BODY_NAME` | Full display name | `New York State Public High School Athletic Association`<br>`Northern Virginia Wrestling Federation`<br>`Virginia High School League`<br>`Virginia USA Wrestling` |

The database will be created at `output/trackwrestling_{acronym}.db` (lowercase).

## Usage

### Scrape Tournaments

```bash
uv run python -m code.scrape_tournaments --max-tournaments 50 --show
```

### Parse Match Data

```bash
uv run python code/parse_round_html.py
```

### Calculate Elo Ratings

```bash
uv run python code/calculate_elo.py
```

## Project Structure

- `code/config.py` - Centralized configuration loaded from `.env`
- `code/scrape_tournaments.py` - Scrape tournament list and round HTML
- `code/parse_round_html.py` - Parse HTML into structured match data
- `code/calculate_elo.py` - Compute Elo ratings for wrestlers
- `output/` - DuckDB database files
