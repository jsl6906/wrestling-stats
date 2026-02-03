"""
Observable Framework data loader for leaderboards/[gov_body] dynamic route.
Loads individual wrestler leaderboard data for a specific gov_body parameter.
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
	log = logging.getLogger("individual_leaderboards_loader")

	# Parse command-line arguments
	parser = argparse.ArgumentParser(description="Load individual leaderboards for a specific gov_body")
	parser.add_argument("--gov_body", required=True, help="The governing body identifier (e.g., vhsl)")
	args = parser.parse_args()
	
	gov_body = args.gov_body
	
	log.info("Loading individual leaderboards for gov_body: %s", gov_body)

	# Find the output directory relative to this script
	script_dir = os.path.dirname(os.path.abspath(__file__))
	output_dir = os.path.normpath(os.path.join(script_dir, "..", "..", "..", "..", "output"))
	
	# Construct the database file path
	db_file = Path(output_dir) / f"trackwrestling_{gov_body}.db"
	
	if not db_file.exists():
		log.warning("Database file not found: %s. Returning empty dataset.", db_file)
		# Output an empty parquet file
		import pyarrow as pa
		import pyarrow.parquet as pq
		schema = pa.schema([
			('name', pa.string()),
			('team', pa.string()),
			('season', pa.string()),
			('matches_played', pa.int64()),
			('wins', pa.int64()),
			('losses', pa.int64()),
			('wins_fall', pa.int64()),
			('highest_elo', pa.float64()),
			('current_elo', pa.float64()),
			('current_elo', pa.float64()),
			('biggest_upset_win', pa.float64()),
			('upset_event_id', pa.int64()),
			('upset_date', pa.string()),
			('upset_tournament_name', pa.string()),
			('upset_opponent_name', pa.string()),
			('upset_opponent_team', pa.string()),
			('upset_result', pa.string()),
			('win_pct', pa.float64()),
			('fall_pct', pa.float64())
		])
		empty_table = pa.table({field.name: pa.array([], type=field.type) for field in schema})
		pq.write_table(empty_table, sys.stdout.buffer)
		sys.stdout.flush()
		return
	
	log.info("Found database: %s", db_file)

	# Query to calculate individual leaderboards with season support
	sql = """--sql
	COPY (
	  WITH match_data AS (
	    SELECT
	      wh.name,
	      wh.team,
	      wh.start_date,
	      wh.event_id,
	      wh.role,
	      wh.decision_type,
	      wh.decision_type_code,
	      wh.pre_elo,
	      wh.post_elo,
	      wh.adjustment,
	      -- Calculate season: Sept 1 to Aug 31
	      CASE 
	        WHEN MONTH(wh.start_date) >= 9 
	        THEN CAST(YEAR(wh.start_date) AS VARCHAR) || '-' || CAST(YEAR(wh.start_date) + 1 AS VARCHAR)
	        ELSE CAST(YEAR(wh.start_date) - 1 AS VARCHAR) || '-' || CAST(YEAR(wh.start_date) AS VARCHAR)
	      END AS season
	    FROM wrestler_history wh
	    WHERE wh.start_date IS NOT NULL
	      AND wh.bye = FALSE
	  ),
	  wrestler_stats AS (
	    SELECT
	      md.name,
	      md.team,
	      md.season,
	      COUNT(*) as matches_played,
	      SUM(CASE WHEN md.role IN ('W', 'winner') THEN 1 ELSE 0 END) as wins,
	      SUM(CASE WHEN md.role IN ('W', 'winner') AND (LOWER(md.decision_type) LIKE '%fall%' OR md.decision_type_code IN ('FALL', 'PIN')) THEN 1 ELSE 0 END) as wins_fall,
	      SUM(CASE WHEN md.role IN ('L', 'loser') THEN 1 ELSE 0 END) as losses,
	      MAX(md.post_elo) as highest_elo,
	      MAX(CASE WHEN md.role IN ('W', 'winner') AND md.adjustment > 0 THEN md.adjustment ELSE NULL END) as biggest_upset_win
	    FROM match_data md
	    GROUP BY md.name, md.team, md.season
	  ),
	  upset_details AS (
	    SELECT
	      wh.name,
	      wh.team,
	      CASE 
	        WHEN MONTH(wh.start_date) >= 9 
	        THEN CAST(YEAR(wh.start_date) AS VARCHAR) || '-' || CAST(YEAR(wh.start_date) + 1 AS VARCHAR)
	        ELSE CAST(YEAR(wh.start_date) - 1 AS VARCHAR) || '-' || CAST(YEAR(wh.start_date) AS VARCHAR)
	      END AS season,
	      wh.event_id,
	      wh.start_date,
	      wh.decision_type,
	      wh.opponent_name,
	      wh.opponent_team,
	      wh.adjustment,
	      ROW_NUMBER() OVER (PARTITION BY wh.name, wh.team, 
	        CASE 
	          WHEN MONTH(wh.start_date) >= 9 
	          THEN CAST(YEAR(wh.start_date) AS VARCHAR) || '-' || CAST(YEAR(wh.start_date) + 1 AS VARCHAR)
	          ELSE CAST(YEAR(wh.start_date) - 1 AS VARCHAR) || '-' || CAST(YEAR(wh.start_date) AS VARCHAR)
	        END
	        ORDER BY wh.adjustment DESC) as rn
	    FROM wrestler_history wh
	    WHERE wh.role IN ('W', 'winner') 
	      AND wh.adjustment > 0
	      AND wh.bye = FALSE
	  )
	  SELECT
	    ws.name,
	    ws.team,
	    ws.season,
	    ws.matches_played,
	    ws.wins,
	    ws.losses,
	    ws.wins_fall,
	    ws.highest_elo,
	    w.current_elo,
	    ws.biggest_upset_win,
	    ud.event_id as upset_event_id,
	    CAST(ud.start_date AS VARCHAR) as upset_date,
	    t.name as upset_tournament_name,
	    ud.opponent_name as upset_opponent_name,
	    ud.opponent_team as upset_opponent_team,
	    ud.decision_type as upset_result,
	    CASE 
	      WHEN ws.matches_played > 0 THEN CAST(ws.wins AS DOUBLE) / CAST(ws.matches_played AS DOUBLE) * 100
	      ELSE 0
	    END as win_pct,
	    CASE 
	      WHEN ws.wins > 0 THEN CAST(ws.wins_fall AS DOUBLE) / CAST(ws.wins AS DOUBLE) * 100
	      ELSE 0
	    END as fall_pct
	  FROM wrestler_stats ws
	  LEFT JOIN wrestlers w ON ws.name = w.name
	  LEFT JOIN upset_details ud ON ws.name = ud.name AND ws.team = ud.team AND ws.season = ud.season AND ud.rn = 1
	  LEFT JOIN tournaments t ON ud.event_id = t.event_id
	  WHERE ws.matches_played > 0
	  ORDER BY ws.season DESC, ws.matches_played DESC
	) TO ? (FORMAT 'parquet')
	"""

	parquet_tmp = None
	parquet_tmp_name: str | None = None
	con = None
	try:
		con = duckdb.connect(str(db_file), read_only=True)
		
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
		# Check if error is due to missing table
		if "wrestler_history" in str(e) or "tournaments" in str(e):
			log.warning("Required tables not found: %s. Returning empty dataset.", e)
			# Output empty parquet
			import pyarrow as pa
			import pyarrow.parquet as pq
			schema = pa.schema([
				('name', pa.string()),
				('team', pa.string()),
				('season', pa.string()),
				('matches_played', pa.int64()),
				('wins', pa.int64()),
				('losses', pa.int64()),
				('wins_fall', pa.int64()),
				('highest_elo', pa.float64()),
				('biggest_upset_win', pa.float64()),
				('upset_event_id', pa.int64()),
				('upset_date', pa.string()),
				('upset_tournament_name', pa.string()),
				('upset_opponent_name', pa.string()),
				('upset_opponent_team', pa.string()),
				('upset_result', pa.string()),
				('win_pct', pa.float64()),
				('fall_pct', pa.float64())
			])
			empty_table = pa.table({field.name: pa.array([], type=field.type) for field in schema})
			pq.write_table(empty_table, sys.stdout.buffer)
			sys.stdout.flush()
		else:
			log.error("Failed to export individual leaderboards: %s", e)
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
