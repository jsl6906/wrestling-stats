# Wrestling Stats

Scrape wrestling tournament data from TrackWrestling, parse match results, and calculate Elo ratings.

## Setup

1. Install dependencies: `uv sync`
2. Configure: Copy `.env.example` to `.env` and set governing body variables.

## Configuration

Set in `.env`:
- `GOVERNING_BODY_ID`: TrackWrestling's gbId (e.g., 38 for NYSPHSAA)
- `GOVERNING_BODY_ACRONYM`: Short identifier (e.g., NYSPHSAA)
- `GOVERNING_BODY_NAME`: Full name

Database: `output/trackwrestling_{acronym}.db`

## Usage

- Scrape: `uv run python -m code.scrape_tournaments --max-tournaments 50 --show`
- Parse: `uv run python code/parse_round_html.py`
- Calculate Elo: `uv run python code/calculate_elo.py`

## Project Structure

- `code/config.py`: Configuration
- `code/scrape_tournaments.py`: Scrape tournaments
- `code/parse_round_html.py`: Parse matches
- `code/calculate_elo.py`: Elo ratings
- `output/`: Databases
