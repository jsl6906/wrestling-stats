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
		log.error("Database file not found: %s", db_file)
		sys.exit(1)
	
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
		wh.pre_elo, wh.post_elo, wh.adjustment, wh.expected_score,
		wh.margin, wh.fall_seconds,
		wh.last_updated,
		wh.elo_sequence,
		-- ISO string projections to ease JSON consumption in front-ends
		CAST(wh.start_date AS VARCHAR) AS start_date_iso,
		CAST(wh.last_updated AS VARCHAR) AS last_updated_iso
	  FROM wrestler_history wh
	  JOIN tournaments t ON t.event_id = wh.event_id
	  LEFT JOIN tournament_rounds tr ON tr.event_id = wh.event_id AND tr.round_id = wh.round_id
	  ORDER BY wh.start_date NULLS LAST, wh.event_id, wh.round_order NULLS LAST, wh.elo_sequence NULLS LAST, wh.match_rowid, wh.role
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
