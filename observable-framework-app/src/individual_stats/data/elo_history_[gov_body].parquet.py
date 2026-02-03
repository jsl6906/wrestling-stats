"""
Observable Framework data loader for individual_stats/[gov_body] dynamic route.
Loads elo_history data for a specific gov_body parameter. 
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
	log = logging.getLogger("elo_history_loader")

	# Parse command-line arguments
	parser = argparse.ArgumentParser(description="Load elo_history data for a specific gov_body")
	parser.add_argument("--gov_body", required=True, help="The governing body identifier (e.g., nvwf)")
	args = parser.parse_args()
	
	gov_body = args.gov_body
	
	log.info("Loading elo_history for gov_body: %s", gov_body)

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
			('event_id', pa.int64()),
			('tournament_name', pa.string()),
			('round_label', pa.string()),
			('round_detail', pa.string()),
			('team', pa.string()),
			('role', pa.string()),
			('weight_class', pa.string()),
			('start_date', pa.timestamp('us')),
			('opponent_name', pa.string()),
			('opponent_team', pa.string()),
			('opponent_pre_elo', pa.float64()),
			('opponent_post_elo', pa.float64()),
			('decision_type', pa.string()),
			('decision_type_code', pa.string()),
			('bye', pa.bool_()),
			('bye', pa.bool_()),
			('pre_elo', pa.float64()),
			('post_elo', pa.float64()),
			('adjustment', pa.float64()),
			('expected_score', pa.float64()),
			('margin', pa.int64()),
			('fall_seconds', pa.int64()),
			('last_updated', pa.timestamp('us')),
			('elo_sequence', pa.int64()),
			('start_date_iso', pa.string()),
			('last_updated_iso', pa.string())
		])
		empty_table = pa.table({field.name: pa.array([], type=field.type) for field in schema})
		pq.write_table(empty_table, sys.stdout.buffer)
		sys.stdout.flush()
		return
	
	log.info("Found database: %s", db_file)

	# Query to materialize wrestler_history
	sql = """--sql
	COPY (
	  SELECT
	    wh.name,
	    wh.event_id,
	    t.name AS tournament_name,
        tr.label AS round_label, wh.round_detail,
		wh.team, wh.role,
		wh.weight_class, wh.start_date,
		wh.opponent_name, wh.opponent_team, wh.opponent_pre_elo, wh.opponent_post_elo,
		wh.decision_type, wh.decision_type_code,
		wh.bye,
		wh.pre_elo, wh.post_elo, wh.adjustment, wh.expected_score,
		wh.margin, wh.fall_seconds,
		wh.last_updated,
		wh.elo_sequence,
		-- ISO string projections to ease JSON consumption in front-ends
		CAST(wh.start_date AS VARCHAR) AS start_date_iso,
		CAST(wh.last_updated AS VARCHAR) AS last_updated_iso
	  FROM wrestler_history wh
	  LEFT JOIN tournaments t ON t.event_id = wh.event_id
	  LEFT JOIN tournament_rounds tr ON tr.event_id = wh.event_id AND tr.round_id = wh.round_id
	  ORDER BY wh.start_date NULLS LAST, wh.event_id, wh.round_order NULLS LAST, wh.elo_sequence NULLS LAST, wh.match_rowid, wh.role
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
		if "wrestler_history" in str(e) or "tournaments" in str(e) or "tournament_rounds" in str(e):
			log.warning("Required tables not found: %s. Returning empty dataset.", e)
			# Output empty parquet
			import pyarrow as pa
			import pyarrow.parquet as pq
			schema = pa.schema([
				('name', pa.string()),
				('event_id', pa.int64()),
				('tournament_name', pa.string()),
				('round_label', pa.string()),
				('round_detail', pa.string()),
				('team', pa.string()),
				('role', pa.string()),
				('weight_class', pa.string()),
				('start_date', pa.timestamp('us')),
				('opponent_name', pa.string()),
				('opponent_team', pa.string()),
				('opponent_pre_elo', pa.float64()),
				('opponent_post_elo', pa.float64()),
				('decision_type', pa.string()),
				('decision_type_code', pa.string()),
				('pre_elo', pa.float64()),
				('post_elo', pa.float64()),
				('adjustment', pa.float64()),
				('expected_score', pa.float64()),
				('margin', pa.int64()),
				('fall_seconds', pa.int64()),
				('last_updated', pa.timestamp('us')),
				('elo_sequence', pa.int64()),
				('start_date_iso', pa.string()),
				('last_updated_iso', pa.string())
			])
			empty_table = pa.table({field.name: pa.array([], type=field.type) for field in schema})
			pq.write_table(empty_table, sys.stdout.buffer)
			sys.stdout.flush()
		else:
			log.error("Failed to export wrestler_history: %s", e)
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
