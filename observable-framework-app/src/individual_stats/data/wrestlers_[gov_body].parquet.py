"""
Observable Framework data loader for individual_stats/[gov_body] dynamic route.
Loads wrestlers data for a specific gov_body parameter. 
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
	log = logging.getLogger("wrestlers_loader")

	# Parse command-line arguments
	parser = argparse.ArgumentParser(description="Load wrestlers data for a specific gov_body")
	parser.add_argument("--gov_body", required=True, help="The governing body identifier (e.g., nvwf)")
	args = parser.parse_args()
	
	gov_body = args.gov_body
	
	log.info("Loading wrestlers for gov_body: %s", gov_body)

	# Find the output directory relative to this script
	script_dir = os.path.dirname(os.path.abspath(__file__))
	output_dir = os.path.normpath(os.path.join(script_dir, "..", "..", "..", "..", "output"))
	
	# Construct the database file path
	db_file = Path(output_dir) / f"trackwrestling_{gov_body}.db"
	
	if not db_file.exists():
		log.error("Database file not found: %s", db_file)
		sys.exit(1)
	
	log.info("Found database: %s", db_file)

	# Query to materialize wrestlers
	sql = """--sql
	COPY (
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
	  FROM wrestlers w
	  ORDER BY w.name
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
		log.error("Failed to export wrestlers: %s", e)
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
