"""
Observable Framework data loader for individual_stats/[gov_body] dynamic route.
Loads wrestlers data for a specific gov_body parameter from Postgres via DuckDB.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "code"))

from db import export_parquet_to_stdout  # noqa: E402


def main() -> None:
	logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stderr)
	log = logging.getLogger("wrestlers_loader")

	parser = argparse.ArgumentParser(description="Load wrestlers data for a specific gov_body")
	parser.add_argument("--gov_body", required=True, help="The governing body identifier (e.g., nvwf)")
	args = parser.parse_args()
	gov_body = args.gov_body

	log.info("Loading wrestlers for gov_body: %s", gov_body)

	sql = """--sql
	  SELECT
	    w.name,
	    w.matches_played,
	    w.wins,
	    w.losses,
	    w.wins_fall,
	    w.losses_fall,
	    w.current_elo,
	    w.best_elo,
	    w.last_team,
	    w.last_opponent_name,
	    w.last_adjustment,
	    w.opponent_avg_elo,
	    w.last_updated,
	    -- ISO string projections
	    CAST(w.best_date AS VARCHAR) AS best_date,
	    CAST(w.last_start_date AS VARCHAR) AS last_start_date,
	    CAST(w.last_updated AS VARCHAR) AS last_updated_iso
	  FROM pg.wrestlers w
	  WHERE w.gov_body = ?
	  ORDER BY w.name
	"""

	export_parquet_to_stdout(sql, [gov_body])
	log.info("Parquet file streamed to stdout successfully")


if __name__ == "__main__":
	main()
