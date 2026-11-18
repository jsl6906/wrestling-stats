"""
Observable Framework archive loader: one zip per governing body with every dataset the
Rankings, Wrestler and Head-to-Head pages need. Members are extracted on demand, so each
page only downloads what it references:

- wrestlers.parquet         directory used by the search box (one row per wrestler)
- history_NN.parquet        non-bye matches for wrestlers in shard NN (see site_queries.shard_of)
- ranks_NN.parquet          weekly overall/weight-class rank snapshots for wrestlers in shard NN
- seasons.parquet           one row per wrestler per season (record, start/end/peak Elo, 30-day Elo)
- bands.parquet             weekly Elo percentiles of ranked wrestlers (chart context)
- trend.parquet             weekly ranks for anyone in a season's top 25 (bump chart)
- placements.parquet        USA Wrestling bracket placements
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
import tempfile
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "code"))

import pandas as pd  # noqa: E402

from db import duckdb_attach_postgres  # noqa: E402
from site_queries import (  # noqa: E402
	MIN_SEASON_MATCHES,
	SHARD_COUNT,
	is_fall_sql,
	season_label_sql,
	shard_of,
	weight_key_sql,
	weight_num_sql,
)

log = logging.getLogger("gb_archive_loader")


def build(con, gov_body: str, out_dir: Path) -> list[Path]:
	con.execute(
		f"""--sql
		CREATE TEMP TABLE m AS
		SELECT
			wh.name, wh.team, wh.role, wh.start_date AS d, wh.event_id,
			t.name AS tournament,
			coalesce(tr.label, wh.round_detail) AS round,
			wh.weight_class,
			{weight_key_sql('wh.weight_class')} AS w_key,
			{weight_num_sql('wh.weight_class')} AS w_num,
			wh.opponent_name, wh.opponent_team, wh.opponent_pre_elo,
			wh.pre_elo, wh.post_elo,
			wh.decision_type, wh.decision_type_code,
			{is_fall_sql('wh.decision_type', 'wh.decision_type_code')} AS is_fall,
			wh.margin, wh.fall_seconds,
			{season_label_sql('wh.start_date')} AS season,
			row_number() OVER (ORDER BY wh.start_date, wh.elo_sequence NULLS LAST, wh.match_id, wh.role) AS ord
		FROM pg.wrestler_history wh
		LEFT JOIN pg.tournaments t ON t.gov_body = wh.gov_body AND t.event_id = wh.event_id
		LEFT JOIN pg.tournament_rounds tr ON tr.gov_body = wh.gov_body AND tr.event_id = wh.event_id AND tr.round_id = wh.round_id
		WHERE wh.gov_body = ? AND wh.name IS NOT NULL AND wh.start_date IS NOT NULL
			AND NOT coalesce(wh.bye, FALSE) AND wh.post_elo IS NOT NULL
		""",
		[gov_body],
	)
	n = con.execute("SELECT count(*) FROM m").fetchone()[0]
	log.info("Loaded %s match rows", n)

	names = [r[0] for r in con.execute("SELECT DISTINCT name FROM m").fetchall()]
	shardmap = pd.DataFrame({"name": names, "shard": [shard_of(x) for x in names]})
	con.register("shardmap_df", shardmap)
	con.execute("CREATE TEMP TABLE shardmap AS SELECT * FROM shardmap_df")

	# Last state of each wrestler per day (several matches can happen on one day)
	con.execute(
		"""--sql
		CREATE TEMP TABLE daily AS
		WITH per_day AS (
			SELECT name, season, d,
				arg_max(post_elo, ord) AS elo,
				arg_max(w_key, ord) FILTER (WHERE w_key IS NOT NULL) AS day_w_key,
				count(*) AS n
			FROM m GROUP BY name, season, d
		)
		SELECT name, season, d, elo,
			last_value(day_w_key IGNORE NULLS) OVER w AS w_key,
			sum(n) OVER w AS season_n
		FROM per_day
		WINDOW w AS (PARTITION BY name, season ORDER BY d ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
		"""
	)

	# Weekly (Sunday) snapshots from each season's first match through its last
	con.execute(
		"""--sql
		CREATE TEMP TABLE weeks AS
		WITH b AS (SELECT season, min(d) AS smin, max(d) AS smax FROM m GROUP BY season)
		SELECT season, CAST(unnest(generate_series(
			CAST(smin + to_days(CAST((7 - isodow(smin)) % 7 AS INTEGER)) AS TIMESTAMP),
			CAST(smax + to_days(CAST((7 - isodow(smax)) % 7 AS INTEGER)) AS TIMESTAMP),
			INTERVAL 7 DAY)) AS DATE) AS wk
		FROM b
		"""
	)
	con.execute(
		f"""--sql
		CREATE TEMP TABLE snaps AS
		WITH people AS (SELECT DISTINCT season, name FROM daily),
		as_of AS (
			SELECT w.season, w.wk, p.name, x.elo, x.w_key, x.season_n
			FROM weeks w
			JOIN people p ON p.season = w.season
			ASOF JOIN daily x ON x.name = p.name AND x.season = w.season AND w.wk >= x.d
		)
		SELECT season, wk, name, elo, w_key,
			CAST(rank() OVER (PARTITION BY season, wk ORDER BY elo DESC) AS INTEGER) AS rank,
			CAST(count(*) OVER (PARTITION BY season, wk) AS INTEGER) AS n,
			CAST(CASE WHEN w_key IS NOT NULL THEN rank() OVER (PARTITION BY season, wk, w_key ORDER BY elo DESC) END AS INTEGER) AS w_rank,
			CAST(CASE WHEN w_key IS NOT NULL THEN count(*) OVER (PARTITION BY season, wk, w_key) END AS INTEGER) AS w_n
		FROM as_of
		WHERE season_n >= {MIN_SEASON_MATCHES}
		"""
	)

	files: list[Path] = []

	def copy(name: str, sql: str, params: list | None = None) -> None:
		path = out_dir / name
		# COPY can't bind parameters with the postgres scanner; materialize first (see db.export_parquet_to_stdout)
		con.execute(f"CREATE OR REPLACE TEMP TABLE _out AS {sql}", params or [])
		con.execute(f"COPY _out TO '{path.as_posix()}' (FORMAT parquet, COMPRESSION zstd)")
		files.append(path)

	for k in range(SHARD_COUNT):
		copy(
			f"history_{k:02d}.parquet",
			f"""--sql
			SELECT m.name, CAST(d AS VARCHAR) AS date, CAST(ord AS INTEGER) AS ord, season,
				event_id, tournament, round, weight_class AS weight, w_key, team,
				opponent_name AS opp, opponent_team AS opp_team,
				CAST(round(opponent_pre_elo) AS INTEGER) AS opp_elo,
				role AS result, decision_type AS dec, decision_type_code AS dec_code, is_fall,
				CAST(margin AS INTEGER) AS margin, CAST(fall_seconds AS INTEGER) AS fall_s,
				round(pre_elo, 1) AS pre, round(post_elo, 1) AS post
			FROM m JOIN shardmap s ON s.name = m.name
			WHERE s.shard = {k}
			ORDER BY m.name, ord
			""",
		)
		copy(
			f"ranks_{k:02d}.parquet",
			f"""--sql
			SELECT x.name, season, CAST(wk AS VARCHAR) AS wk, CAST(round(elo) AS INTEGER) AS elo,
				rank, n, w_key, w_rank, w_n
			FROM snaps x JOIN shardmap s ON s.name = x.name
			WHERE s.shard = {k}
			ORDER BY x.name, wk
			""",
		)

	copy(
		"seasons.parquet",
		"""--sql
		WITH s AS (
			SELECT season, name,
				CAST(count(*) AS INTEGER) AS matches,
				CAST(count(*) FILTER (WHERE role = 'W') AS INTEGER) AS wins,
				CAST(count(*) FILTER (WHERE role = 'L') AS INTEGER) AS losses,
				CAST(count(*) FILTER (WHERE role = 'W' AND is_fall) AS INTEGER) AS falls,
				arg_min(pre_elo, ord) AS start_elo,
				arg_max(post_elo, ord) AS end_elo,
				max(post_elo) AS peak_elo,
				max(d) AS last_d,
				arg_max(team, ord) FILTER (WHERE team IS NOT NULL) AS team,
				arg_max(w_key, ord) FILTER (WHERE w_key IS NOT NULL) AS w_key,
				arg_max(w_num, ord) FILTER (WHERE w_num IS NOT NULL) AS w_num
			FROM m GROUP BY season, name
		),
		cut AS (SELECT season, CAST(max(d) - INTERVAL 30 DAY AS DATE) AS cutoff FROM m GROUP BY season),
		s2 AS (SELECT s.*, c.cutoff FROM s JOIN cut c USING (season)),
		-- Elo as of 30 days before the season's latest match (may come from an earlier season)
		career AS (SELECT name, d, arg_max(post_elo, ord) AS elo FROM m GROUP BY name, d),
		e30 AS (
			SELECT s2.season, s2.name, x.elo AS elo_30
			FROM s2 ASOF JOIN career x ON x.name = s2.name AND s2.cutoff >= x.d
		),
		m30 AS (
			SELECT m.season, m.name, CAST(count(*) AS INTEGER) AS m30
			FROM m JOIN cut c USING (season) WHERE m.d > c.cutoff GROUP BY m.season, m.name
		)
		SELECT s2.season, s2.name, s2.team, s2.w_key, CAST(s2.w_num AS INTEGER) AS w_num,
			s2.matches, s2.wins, s2.losses, s2.falls,
			round(s2.start_elo, 1) AS start_elo, round(s2.end_elo, 1) AS end_elo, round(s2.peak_elo, 1) AS peak_elo,
			round(e30.elo_30, 1) AS elo_30, coalesce(m30.m30, 0) AS m30,
			CAST(s2.last_d AS VARCHAR) AS last_date
		FROM s2
		LEFT JOIN e30 ON e30.season = s2.season AND e30.name = s2.name
		LEFT JOIN m30 ON m30.season = s2.season AND m30.name = s2.name
		ORDER BY s2.season DESC, s2.end_elo DESC
		""",
	)

	copy(
		"bands.parquet",
		"""--sql
		SELECT season, CAST(wk AS VARCHAR) AS wk,
			CAST(count(*) AS INTEGER) AS n,
			round(quantile_cont(elo, 0.10), 1) AS p10,
			round(quantile_cont(elo, 0.25), 1) AS p25,
			round(quantile_cont(elo, 0.50), 1) AS p50,
			round(quantile_cont(elo, 0.75), 1) AS p75,
			round(quantile_cont(elo, 0.90), 1) AS p90,
			round(quantile_cont(elo, 0.99), 1) AS p99
		FROM snaps GROUP BY season, wk ORDER BY wk
		""",
	)

	copy(
		"trend.parquet",
		"""--sql
		WITH top AS (SELECT DISTINCT season, name FROM snaps WHERE rank <= 25)
		SELECT x.season, CAST(x.wk AS VARCHAR) AS wk, x.name, x.rank, CAST(round(x.elo) AS INTEGER) AS elo
		FROM snaps x JOIN top USING (season, name)
		ORDER BY x.season, x.wk, x.rank
		""",
	)

	copy(
		"wrestlers.parquet",
		"""--sql
		WITH last_w AS (
			SELECT name, arg_max(w_key, ord) FILTER (WHERE w_key IS NOT NULL) AS w_key FROM m GROUP BY name
		)
		SELECT w.name, w.last_team AS team, l.w_key,
			round(w.current_elo, 1) AS elo, round(w.best_elo, 1) AS best_elo, CAST(w.best_date AS VARCHAR) AS best_date,
			CAST(w.matches_played AS INTEGER) AS matches, CAST(w.wins AS INTEGER) AS wins, CAST(w.losses AS INTEGER) AS losses,
			CAST(w.wins_fall AS INTEGER) AS wins_fall, CAST(w.losses_fall AS INTEGER) AS losses_fall,
			round(w.opponent_avg_elo, 1) AS opp_avg_elo, CAST(w.last_start_date AS VARCHAR) AS last_date
		FROM pg.wrestlers w
		LEFT JOIN last_w l ON l.name = w.name
		WHERE w.gov_body = ? AND coalesce(w.matches_played, 0) > 0
		ORDER BY w.name
		""",
		[gov_body],
	)

	copy(
		"placements.parquet",
		"""--sql
		SELECT p.name, p.team, p.event_id, t.name AS tournament, CAST(t.start_date AS VARCHAR) AS date,
			p.weight_class, CAST(p.place AS INTEGER) AS place
		FROM pg.placements p
		LEFT JOIN pg.tournaments t ON t.gov_body = p.gov_body AND t.event_id = p.event_id
		WHERE p.gov_body = ? AND p.name IS NOT NULL AND p.place IS NOT NULL
		ORDER BY p.name, t.start_date
		""",
		[gov_body],
	)
	return files


def main() -> None:
	logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stderr)
	parser = argparse.ArgumentParser(description="Build the per-governing-body data archive")
	parser.add_argument("--gov_body", required=True, help="Governing body identifier (e.g., vhsl)")
	gov_body = parser.parse_args().gov_body

	con = duckdb_attach_postgres(read_only=True)
	buf = io.BytesIO()
	try:
		with tempfile.TemporaryDirectory() as tmp:
			files = build(con, gov_body, Path(tmp))
			# Parquet members are already compressed
			with zipfile.ZipFile(buf, "w", zipfile.ZIP_STORED) as zf:
				for f in files:
					zf.write(f, f.name)
	finally:
		con.close()

	sys.stdout.buffer.write(buf.getvalue())
	sys.stdout.flush()
	log.info("Wrote archive for %s (%.1f MB)", gov_body, len(buf.getvalue()) / 1e6)


if __name__ == "__main__":
	main()
