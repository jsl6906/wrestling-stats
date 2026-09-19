"""
Observable Framework data loader for individual_stats/[gov_body] dynamic route.
Loads elo_history data for a specific gov_body parameter from Postgres via DuckDB.
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
	log = logging.getLogger("elo_history_loader")

	parser = argparse.ArgumentParser(description="Load elo_history data for a specific gov_body")
	parser.add_argument("--gov_body", required=True, help="The governing body identifier (e.g., nvwf)")
	args = parser.parse_args()
	gov_body = args.gov_body

	log.info("Loading elo_history for gov_body: %s", gov_body)

	sql = """--sql
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
	  FROM pg.wrestler_history wh
	  LEFT JOIN pg.tournaments t ON t.gov_body = wh.gov_body AND t.event_id = wh.event_id
	  LEFT JOIN pg.tournament_rounds tr ON tr.gov_body = wh.gov_body AND tr.event_id = wh.event_id AND tr.round_id = wh.round_id
	  WHERE wh.gov_body = ?
	  ORDER BY wh.start_date NULLS LAST, wh.event_id, wh.round_order NULLS LAST, wh.elo_sequence NULLS LAST, wh.match_id, wh.role
	"""

	export_parquet_to_stdout(sql, [gov_body])
	log.info("Parquet file streamed to stdout successfully")


if __name__ == "__main__":
	main()
