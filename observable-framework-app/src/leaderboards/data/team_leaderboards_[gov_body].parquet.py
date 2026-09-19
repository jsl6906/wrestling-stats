"""
Observable Framework data loader for leaderboards/[gov_body] dynamic route.
Loads team leaderboard data for a specific gov_body parameter from Postgres via DuckDB.
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
	log = logging.getLogger("team_leaderboards_loader")

	parser = argparse.ArgumentParser(description="Load team leaderboards for a specific gov_body")
	parser.add_argument("--gov_body", required=True, help="The governing body identifier (e.g., vhsl)")
	args = parser.parse_args()
	gov_body = args.gov_body

	log.info("Loading team leaderboards for gov_body: %s", gov_body)

	sql = """--sql
	  WITH match_data AS (
	    SELECT
	      wh.team,
	      wh.start_date,
	      wh.role,
	      wh.decision_type,
	      wh.decision_type_code,
	      -- Calculate season: Sept 1 to Aug 31
	      CASE 
	        WHEN MONTH(wh.start_date) >= 9 
	        THEN CAST(YEAR(wh.start_date) AS VARCHAR) || '-' || CAST(YEAR(wh.start_date) + 1 AS VARCHAR)
	        ELSE CAST(YEAR(wh.start_date) - 1 AS VARCHAR) || '-' || CAST(YEAR(wh.start_date) AS VARCHAR)
	      END AS season
	    FROM pg.wrestler_history wh
	    WHERE wh.gov_body = ?
	      AND wh.start_date IS NOT NULL
	      AND wh.bye = FALSE
	      AND wh.team IS NOT NULL
	  ),
	  team_stats AS (
	    SELECT
	      team,
	      season,
	      COUNT(*) as matches_played,
	      SUM(CASE WHEN role IN ('W', 'winner') THEN 1 ELSE 0 END) as wins,
	      SUM(CASE WHEN role IN ('W', 'winner') AND (LOWER(decision_type) LIKE '%fall%' OR decision_type_code IN ('FALL', 'PIN')) THEN 1 ELSE 0 END) as wins_fall,
	      SUM(CASE WHEN role IN ('L', 'loser') THEN 1 ELSE 0 END) as losses
	    FROM match_data
	    GROUP BY team, season
	  )
	  SELECT
	    team,
	    season,
	    matches_played,
	    wins,
	    losses,
	    wins_fall,
	    CASE 
	      WHEN matches_played > 0 THEN CAST(wins AS DOUBLE) / CAST(matches_played AS DOUBLE) * 100
	      ELSE 0
	    END as win_pct,
	    CASE 
	      WHEN wins > 0 THEN CAST(wins_fall AS DOUBLE) / CAST(wins AS DOUBLE) * 100
	      ELSE 0
	    END as fall_pct
	  FROM team_stats
	  WHERE matches_played > 0
	  ORDER BY season DESC, matches_played DESC
	"""

	export_parquet_to_stdout(sql, [gov_body])
	log.info("Parquet file streamed to stdout successfully")


if __name__ == "__main__":
	main()
