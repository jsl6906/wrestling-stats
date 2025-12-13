"""
Observable Framework data loader (Python): emits a Parquet file of wrestler_history.

This script connects to DuckDB at output/scrape.db, queries wrestler_history,
and writes the Parquet bytes to stdout (so the final built asset is elo_history.parquet).

Notes:
- We write Parquet via DuckDB COPY to a temporary file, then stream the file to stdout.
- Keep ALL logging on stderr to avoid corrupting the Parquet binary on stdout. 
"""

from __future__ import annotations

import os
import sys
import tempfile
import logging
import duckdb


def main() -> None:
	logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stderr)
	log = logging.getLogger("elo_history_loader")

	# Allow override via env var if desired; default to project path
	db_path = os.environ.get("WRESTLING_STATS_DB", os.path.join("..", "..", "..", "output", "trackwrestling.db"))
	# Resolve relative to this script location
	script_dir = os.path.dirname(os.path.abspath(__file__))
	db_abspath = os.path.normpath(os.path.join(script_dir, db_path))

	# Query to materialize wrestler_history; adjust ordering as needed for frontend
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

	try:
		con = duckdb.connect(db_abspath)
	except Exception as e:
		log.error("Failed to connect DuckDB at %s: %s", db_abspath, e)
		sys.exit(1)

	tmp = None
	tmp_name: str | None = None
	try:
		tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
		tmp_name = tmp.name
		tmp.close()  # DuckDB needs to open it for writing

		con.execute(sql, [tmp_name])

		with open(tmp_name, "rb") as f:
			# Stream Parquet to stdout (Observable Framework captures this as elo_history.parquet)
			sys.stdout.buffer.write(f.read())
			sys.stdout.flush()
	except Exception as e:
		log.error("Failed to export wrestler_history to Parquet: %s", e)
		sys.exit(1)
	finally:
		try:
			if tmp_name:
				os.unlink(tmp_name)
		except Exception:
			# Non-fatal if cleanup fails
			pass
		try:
			con.close()
		except Exception:
			pass


if __name__ == "__main__":
	main()

