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
		log.warning("Database file not found: %s. Returning empty dataset.", db_file)
		# Output an empty parquet file
		import pyarrow as pa
		import pyarrow.parquet as pq
		schema = pa.schema([
			('name', pa.string()),
			('matches_played', pa.int64()),
			('wins', pa.int64()),
			('losses', pa.int64()),
			('wins_fall', pa.int64()),
			('losses_fall', pa.int64()),
			('current_elo', pa.float64()),
			('best_elo', pa.float64()),
			('last_team', pa.string()),
			('last_opponent_name', pa.string()),
			('last_adjustment', pa.float64()),
			('opponent_avg_elo', pa.float64()),
			('last_updated', pa.timestamp('us')),
			('best_date', pa.string()),
			('last_start_date', pa.string()),
			('last_updated_iso', pa.string())
		])
		empty_table = pa.table({field.name: pa.array([], type=field.type) for field in schema})
		pq.write_table(empty_table, sys.stdout.buffer)
		sys.stdout.flush()
		return
	
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
		if "wrestlers" in str(e).lower():
			log.warning("Required tables not found: %s. Returning empty dataset.", e)
			# Output empty parquet
			import pyarrow as pa
			import pyarrow.parquet as pq
			schema = pa.schema([
				('name', pa.string()),
				('matches_played', pa.int64()),
				('wins', pa.int64()),
				('losses', pa.int64()),
				('wins_fall', pa.int64()),
				('losses_fall', pa.int64()),
				('current_elo', pa.float64()),
				('best_elo', pa.float64()),
				('last_team', pa.string()),
				('last_opponent_name', pa.string()),
				('last_adjustment', pa.float64()),
				('opponent_avg_elo', pa.float64()),
				('last_updated', pa.timestamp('us')),
				('best_date', pa.string()),
				('last_start_date', pa.string()),
				('last_updated_iso', pa.string())
			])
			empty_table = pa.table({field.name: pa.array([], type=field.type) for field in schema})
			pq.write_table(empty_table, sys.stdout.buffer)
			sys.stdout.flush()
		else:
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
