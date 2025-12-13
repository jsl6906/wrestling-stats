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
	log = logging.getLogger("wrestlers_loader")

	# Allow override via env var if desired; default to project path
	db_path = os.environ.get("WRESTLING_STATS_DB", os.path.join("..", "..", "..", "output", "trackwrestling.db"))
	# Resolve relative to this script location
	script_dir = os.path.dirname(os.path.abspath(__file__))
	db_abspath = os.path.normpath(os.path.join(script_dir, db_path))

	# Query to materialize wrestler_history; adjust ordering as needed for frontend
	sql = """--sql
	COPY (
	  SELECT
	    *
	  FROM wrestlers
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
		log.error("Failed to export to Parquet: %s", e)
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

