"""
Observable Framework data loader for the home page: headline stats per governing body.
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "code"))

from db import duckdb_attach_postgres  # noqa: E402
from site_queries import MIN_SEASON_MATCHES, season_label_sql  # noqa: E402

GOV_BODIES_PATH = Path(__file__).resolve().parents[3] / "governing_bodies.json"


def main() -> None:
	logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stderr)
	gov_bodies = [gb["acronym"].lower() for gb in json.loads(GOV_BODIES_PATH.read_text(encoding="utf-8"))]

	con = duckdb_attach_postgres(read_only=True)
	try:
		con.execute(
			f"""--sql
			CREATE TEMP TABLE h AS
			SELECT gov_body, name, team, role, event_id, start_date AS d, post_elo,
				{season_label_sql('start_date')} AS season,
				row_number() OVER (PARTITION BY gov_body ORDER BY start_date, elo_sequence NULLS LAST, match_id, role) AS ord
			FROM pg.wrestler_history
			WHERE gov_body IN (SELECT unnest(?)) AND name IS NOT NULL AND start_date IS NOT NULL
				AND NOT coalesce(bye, FALSE) AND post_elo IS NOT NULL
			""",
			[gov_bodies],
		)
		totals = con.execute(
			"""--sql
			WITH latest AS (SELECT gov_body, max(season) AS season FROM h GROUP BY gov_body)
			SELECT h.gov_body,
				count(DISTINCT h.name) AS wrestlers,
				count(*) FILTER (WHERE h.role = 'W') AS matches,
				count(DISTINCT h.event_id) AS tournaments,
				CAST(min(h.d) AS VARCHAR) AS first_date,
				CAST(max(h.d) AS VARCHAR) AS last_date,
				any_value(l.season) AS season,
				count(DISTINCT h.team) FILTER (WHERE h.season = l.season) AS season_teams,
				count(DISTINCT h.name) FILTER (WHERE h.season = l.season) AS season_wrestlers
			FROM h JOIN latest l USING (gov_body)
			GROUP BY h.gov_body
			"""
		).fetchall()
		cols = ["gov_body", "wrestlers", "matches", "tournaments", "first_date", "last_date", "season", "season_teams", "season_wrestlers"]
		summary = {r[0]: dict(zip(cols, r)) for r in totals}

		leaders = con.execute(
			f"""--sql
			WITH latest AS (SELECT gov_body, max(season) AS season FROM h GROUP BY gov_body),
			s AS (
				SELECT h.gov_body, h.name, arg_max(h.team, h.ord) AS team, arg_max(h.post_elo, h.ord) AS elo, count(*) AS n
				FROM h JOIN latest l ON l.gov_body = h.gov_body AND l.season = h.season
				GROUP BY h.gov_body, h.name
			)
			SELECT gov_body, name, team, round(elo) AS elo
			FROM s WHERE n >= {MIN_SEASON_MATCHES}
			QUALIFY row_number() OVER (PARTITION BY gov_body ORDER BY elo DESC) <= 3
			ORDER BY gov_body, elo DESC
			"""
		).fetchall()
		for gb, name, team, elo in leaders:
			summary[gb].setdefault("leaders", []).append({"name": name, "team": team, "elo": int(elo)})
	finally:
		con.close()

	sys.stdout.write(json.dumps(summary))


if __name__ == "__main__":
	main()
