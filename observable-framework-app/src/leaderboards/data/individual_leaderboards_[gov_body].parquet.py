"""
Observable Framework data loader for leaderboards/[gov_body] dynamic route.
Loads individual wrestler leaderboard data for a specific gov_body parameter from Postgres via DuckDB.
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
	log = logging.getLogger("individual_leaderboards_loader")

	parser = argparse.ArgumentParser(description="Load individual leaderboards for a specific gov_body")
	parser.add_argument("--gov_body", required=True, help="The governing body identifier (e.g., vhsl)")
	args = parser.parse_args()
	gov_body = args.gov_body

	log.info("Loading individual leaderboards for gov_body: %s", gov_body)

	sql = """--sql
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
	      wh.opponent_name,
	      wh.opponent_team,
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
	      md.name,
	      md.team,
	      md.season,
	      md.event_id,
	      md.start_date,
	      md.decision_type,
	      md.opponent_name,
	      md.opponent_team,
	      md.adjustment,
	      ROW_NUMBER() OVER (PARTITION BY md.name, md.team, md.season ORDER BY md.adjustment DESC) as rn
	    FROM match_data md
	    WHERE md.role IN ('W', 'winner') 
	      AND md.adjustment > 0
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
	  LEFT JOIN pg.wrestlers w ON w.gov_body = ? AND ws.name = w.name
	  LEFT JOIN upset_details ud ON ws.name = ud.name AND ws.team = ud.team AND ws.season = ud.season AND ud.rn = 1
	  LEFT JOIN pg.tournaments t ON t.gov_body = ? AND ud.event_id = t.event_id
	  WHERE ws.matches_played > 0
	  ORDER BY ws.season DESC, ws.matches_played DESC
	"""

	export_parquet_to_stdout(sql, [gov_body, gov_body, gov_body])
	log.info("Parquet file streamed to stdout successfully")


if __name__ == "__main__":
	main()
