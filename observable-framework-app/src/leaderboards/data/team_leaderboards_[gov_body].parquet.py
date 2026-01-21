"""
Observable Framework data loader for leaderboards/[gov_body] dynamic route.
Loads team leaderboard data for a specific gov_body parameter.
"""

from __future__ import annotations

import os
import sys
import tempfile
import logging
import argparse
import duckdb
from pathlib import Path


def main() -> None:
	logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stderr)
	log = logging.getLogger("team_leaderboards_loader")

	# Parse command-line arguments
	parser = argparse.ArgumentParser(description="Load team leaderboards for a specific gov_body")
	parser.add_argument("--gov_body", required=True, help="The governing body identifier (e.g., vhsl)")
	args = parser.parse_args()
	
	gov_body = args.gov_body
	
	log.info("Loading team leaderboards for gov_body: %s", gov_body)

	# Find the output directory relative to this script
	script_dir = os.path.dirname(os.path.abspath(__file__))
	output_dir = os.path.normpath(os.path.join(script_dir, "..", "..", "..", "..", "output"))
	
	# Construct the database file path
	db_file = Path(output_dir) / f"trackwrestling_{gov_body}.db"
	
	if not db_file.exists():
		log.error("Database file not found: %s", db_file)
		sys.exit(1)
	
	log.info("Found database: %s", db_file)

	# Query to calculate team leaderboards with season support
	sql = """--sql
	COPY (
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
	    FROM wrestler_history wh
	    WHERE wh.start_date IS NOT NULL
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
	) TO ? (FORMAT 'parquet')
	"""

	parquet_tmp = None
	parquet_tmp_name: str | None = None
	con = None
	try:
		con = duckdb.connect(str(db_file))
		
		parquet_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
		parquet_tmp_name = parquet_tmp.name
		parquet_tmp.close()  # DuckDB needs to open it for writing

		con.execute(sql, [parquet_tmp_name])
		
		# Stream parquet file to stdout
		with open(parquet_tmp_name, "rb") as f:
			sys.stdout.buffer.write(f.read())
			sys.stdout.flush()
		
		log.info("Parquet file streamed to stdout successfully")
		
	except Exception as e:
		log.error("Failed to export team leaderboards: %s", e)
		sys.exit(1)
	finally:
		try:
			if parquet_tmp_name:
				os.unlink(parquet_tmp_name)
		except Exception:
			pass
		try:
			if con:
				con.close()
		except Exception:
			pass


if __name__ == "__main__":
	main()
