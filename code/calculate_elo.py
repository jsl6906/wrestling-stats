"""
Compute Elo ratings for wrestlers across all matches, in tournament date order, and
write results back onto each match row.

Inputs (DuckDB tables expected):
- tournaments(event_id, start_date, name, ...)
- matches(event_id, round_id, weight_class, raw_match_results,
		  round_detail, winner_name, winner_team, loser_name, loser_team,
		  decision_type, decision_type_code, winner_points, loser_points, fall_time, bye,
		  ...)

Outputs (added columns on matches if missing):
- winner_elo_after, winner_elo_adjustment
- loser_elo_after, loser_elo_adjustment

Notes:
- Initial Elo for unseen wrestler: 1000
- K-factor: 32 base; modifiers by decision type:
  - fall/forfeit/default: K*1.25
  - tech fall (TF)/major (MD)/SV-1/OT: K*1.10
  - decision (Dec): K*1.00
  - bye: ignored (no Elo change)
- Match ordering: tournament.start_date asc, then event_id, then an approximate round order
  parsed from round_detail (Quarterfinal, Semifinal, Final, Consolation, etc). We default unknowns to mid-range.

Run:
  uv run python code/calculate_elo.py
"""

from __future__ import annotations

import logging
from typing import Dict, Optional, Any, List, Tuple

import duckdb
try:
	from tqdm.auto import tqdm  # type: ignore
except Exception:
	def tqdm(iterable, total=None, desc=None):  # type: ignore
		return iterable

def progress(iterable, total=None, desc: str | None = None):
	"""Wrapper around tqdm to make sure bar is enabled in terminals."""
	try:
		return tqdm(iterable, total=total, desc=desc)
	except Exception:
		return iterable


def ensure_matches_elo_columns(conn: duckdb.DuckDBPyConnection) -> None:
	# Add ELO columns if not present
	cols = set(
		r[0]
		for r in conn.execute(
			"""--sql
			SELECT column_name FROM information_schema.columns WHERE table_name = 'matches'
			"""
		).fetchall()
	)
	alters = []
	def add(col: str, typ: str):
		if col not in cols:
			alters.append(f"ALTER TABLE matches ADD COLUMN {col} {typ}")
	add("winner_elo_after", "DOUBLE")
	add("winner_elo_adjustment", "DOUBLE")
	add("loser_elo_after", "DOUBLE")
	add("loser_elo_adjustment", "DOUBLE")
	# Detailed metadata
	add("elo_computed_at", "TIMESTAMP")
	add("elo_sequence", "BIGINT")
	add("winner_elo_before", "DOUBLE")
	add("loser_elo_before", "DOUBLE")
	add("expected_winner", "DOUBLE")
	add("expected_loser", "DOUBLE")
	add("k_applied", "DOUBLE")
	add("k_type_mult", "DOUBLE")
	add("k_mov_mult", "DOUBLE")
	add("k_quick_mult", "DOUBLE")
	add("margin", "INTEGER")
	add("fall_seconds", "INTEGER")
	add("round_order", "INTEGER")
	add("winner_prev_matches", "INTEGER")
	add("loser_prev_matches", "INTEGER")
	if alters:
		conn.execute(";\n".join(["--sql"] + alters))


def ensure_wrestlers_table(conn: duckdb.DuckDBPyConnection) -> None:
	conn.execute(
		"""--sql
		CREATE TABLE IF NOT EXISTS wrestlers (
			name TEXT PRIMARY KEY,
			current_elo DOUBLE,
			matches_played INTEGER,
			last_event_id TEXT,
			last_start_date DATE,
			last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			last_opponent_name TEXT,
			last_adjustment DOUBLE,
			last_team TEXT,
			best_elo DOUBLE,
			best_date DATE,
			-- Summary stats
			wins INTEGER,
			wins_fall INTEGER,
			losses INTEGER,
			losses_fall INTEGER,
			dqs INTEGER,
			opponent_elo_sum DOUBLE,
			opponent_elo_count INTEGER,
			opponent_avg_elo DOUBLE
		);
		"""
	)
	# Backfill any missing columns for existing DBs
	cols = set(
		r[0]
		for r in conn.execute(
			"""--sql
			SELECT column_name FROM information_schema.columns WHERE table_name = 'wrestlers'
			"""
		).fetchall()
	)
	expect = [
		("wins", "INTEGER"),
		("wins_fall", "INTEGER"),
		("losses", "INTEGER"),
		("losses_fall", "INTEGER"),
		("dqs", "INTEGER"),
		("opponent_elo_sum", "DOUBLE"),
		("opponent_elo_count", "INTEGER"),
		("opponent_avg_elo", "DOUBLE"),
	]
	for name, typ in expect:
		if name not in cols:
			conn.execute(f"""--sql
			ALTER TABLE wrestlers ADD COLUMN {name} {typ}
			""")


def ensure_wrestler_history_table(conn: duckdb.DuckDBPyConnection) -> None:
	conn.execute(
		"""--sql
		CREATE TABLE IF NOT EXISTS wrestler_history (
			match_rowid BIGINT,
			role TEXT, -- 'W' or 'L'
			name TEXT,
			team TEXT,
			event_id TEXT,
			round_id TEXT,
			weight_class TEXT,
			start_date DATE,
			opponent_name TEXT,
			opponent_team TEXT,
			opponent_pre_elo DOUBLE,
			opponent_post_elo DOUBLE,
			pre_elo DOUBLE,
			post_elo DOUBLE,
			adjustment DOUBLE,
			expected_score DOUBLE,
			k_applied DOUBLE,
			k_type_mult DOUBLE,
			k_mov_mult DOUBLE,
			k_quick_mult DOUBLE,
			decision_type TEXT,
			decision_type_code TEXT,
			margin INTEGER,
			fall_seconds INTEGER,
			round_detail TEXT,
			round_order INTEGER,
			bye BOOLEAN,
			elo_sequence BIGINT,
			last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (match_rowid, role)
		);
		"""
	)
	# Backfill any missing columns for existing DBs
	cols = set(
		r[0]
		for r in conn.execute(
			"""--sql
			SELECT column_name FROM information_schema.columns WHERE table_name = 'wrestler_history'
			"""
		).fetchall()
	)
	expect = [
		("match_rowid", "BIGINT"),
		("role", "TEXT"),
		("name", "TEXT"),
		("team", "TEXT"),
		("event_id", "TEXT"),
		("round_id", "TEXT"),
		("weight_class", "TEXT"),
		("start_date", "DATE"),
		("opponent_name", "TEXT"),
		("opponent_team", "TEXT"),
		("opponent_pre_elo", "DOUBLE"),
		("opponent_post_elo", "DOUBLE"),
		("pre_elo", "DOUBLE"),
		("post_elo", "DOUBLE"),
		("adjustment", "DOUBLE"),
		("expected_score", "DOUBLE"),
		("k_applied", "DOUBLE"),
		("k_type_mult", "DOUBLE"),
		("k_mov_mult", "DOUBLE"),
		("k_quick_mult", "DOUBLE"),
		("decision_type", "TEXT"),
		("decision_type_code", "TEXT"),
		("margin", "INTEGER"),
		("fall_seconds", "INTEGER"),
		("round_detail", "TEXT"),
		("round_order", "INTEGER"),
		("bye", "BOOLEAN"),
		("elo_sequence", "BIGINT"),
		("last_updated", "TIMESTAMP"),
	]
	for name, typ in expect:
		if name not in cols:
			conn.execute(f"""--sql
			ALTER TABLE wrestler_history ADD COLUMN {name} {typ}
			""")


ROUND_ORDER = {
	# Common labels to sort within a tournament
	# Lower number means earlier in event
	"pigtail": 5,
	"prelim": 10,
	"r1": 20,
	"r2": 30,
	"r3": 40,
	"r4": 50,
	"r5": 60,
	"r6": 70,
	"quarter": 80,
	"quarterfinal": 80,
	"quarters": 80,
	"consolation": 85,
	"semifinal": 90,
	"semi": 90,
	"semis": 90,
	"final": 100,
	"championship": 100,
	"placement": 110,
}


def round_sort_key(round_detail: Optional[str]) -> int:
	if not round_detail:
		return 50
	s = round_detail.strip().lower()
	# direct match
	for k, v in ROUND_ORDER.items():
		if k in s:
			return v
	# explicit R#
	import re
	m = re.search(r"\br(\d+)\b", s)
	if m:
		try:
			n = int(m.group(1))
			return 10 + n * 10
		except Exception:
			pass
	return 60


def expected_score(ra: float, rb: float) -> float:
	return 1.0 / (1.0 + 10 ** ((rb - ra) / 400.0))


def _parse_fall_time_to_seconds(fall_time: Optional[str]) -> Optional[int]:
	if not fall_time:
		return None
	try:
		if ":" in fall_time:
			mm, ss = fall_time.strip().split(":", 1)
			return int(mm) * 60 + int(ss)
		# If only seconds provided
		return int(fall_time)
	except Exception:
		return None


def k_components(
	decision_type: Optional[str],
	decision_code: Optional[str],
	winner_points: Optional[int],
	loser_points: Optional[int],
	fall_time: Optional[str],
) -> Tuple[float, float, float, float, Optional[int], Optional[int]]:
	base_k = 32.0
	dt = (decision_type or "").lower()
	dc = (decision_code or "").upper()

	# Byes do not change ratings
	if dt == "bye":
		return 0.0, 1.0, 1.0, 1.0, None, None

	# Margin of victory factor for decisions
	margin = None
	if winner_points is not None and loser_points is not None:
		try:
			margin = max(0, int(winner_points) - int(loser_points))
		except Exception:
			margin = None

	# Default multipliers
	type_mult = 1.0
	mov_mult = 1.0

	# Tech/Major/SV/OT get a slight base boost
	if "tech" in dt or dc.startswith("TF") or "major" in dt or dc.startswith("MD") or dc.startswith("SV") or dc in ("OT", "UTB"):
		type_mult = 1.10
		if margin is not None:
			# 3% per point, capped at +35%
			mov_mult = 1.0 + min(0.35, 0.03 * margin)
	# Regular decisions
	elif "dec" in dt or dc == "DEC" or "decision" in dt:
		type_mult = 1.00
		if margin is not None:
			# 3% per point, capped at +30%
			mov_mult = 1.0 + min(0.30, 0.03 * margin)
	# Falls, forfeits, defaults: treat as big wins; earlier time -> bigger boost
	if "fall" in dt or dc in ("FALL", "PIN", "FF", "FOR", "DEF"):
		# Base big-win multiplier
		# Add a quickness component: map fall time in [0, FALL_REF_SEC] to [1.50, 1.25]
		FALL_REF_SEC = 180  # reference period length (3 minutes) for scaling
		sec = _parse_fall_time_to_seconds(fall_time)
		quick_mult = 1.25
		if sec is not None:
			x = max(0.0, min(1.0, 1.0 - (sec / float(FALL_REF_SEC))))
			quick_mult = 1.25 + 0.25 * x  # in [1.25, 1.50]
		# For falls, ignore mov_mult and type_mult; use quick_mult
		return base_k * quick_mult, 1.0, 1.0, quick_mult, margin, sec

	return base_k * type_mult * mov_mult, type_mult, mov_mult, 1.0, margin, None


def k_factor(decision_type: Optional[str], decision_code: Optional[str], winner_points: Optional[int], loser_points: Optional[int], fall_time: Optional[str]) -> float:
	k, *_ = k_components(decision_type, decision_code, winner_points, loser_points, fall_time)
	return k


def fetch_matches_ordered(conn: duckdb.DuckDBPyConnection) -> List[Tuple[Any, ...]]:
	rows = conn.execute(
		"""--sql
	 SELECT m.rowid, m.event_id, m.round_id, m.weight_class, m.winner_name, m.loser_name,
		 m.decision_type, m.decision_type_code, m.round_detail,
		 m.winner_points, m.loser_points, m.fall_time,
		 m.winner_team, m.loser_team,
			   t.start_date
		FROM matches m
		JOIN tournaments t ON t.event_id = m.event_id
		WHERE COALESCE(m.bye, FALSE) = FALSE AND m.winner_name IS NOT NULL AND m.loser_name IS NOT NULL
		ORDER BY t.start_date NULLS LAST, m.event_id
		"""
	).fetchall()
	# Sort within event explicitly by round order
	def _key(r: Tuple[Any, ...]):
		(rowid, event_id, round_id, weight_class, wname, lname, d_type, d_code, rdetail, wpts, lpts, ftime, wteam, lteam, start_date) = r
		sd = start_date or "9999-12-31"
		return (sd, event_id, round_sort_key(rdetail))
	rows.sort(key=_key)
	return rows


def run() -> None:
	logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
	log = logging.getLogger(__name__)

	conn = duckdb.connect("output/scrape.db")
	ensure_matches_elo_columns(conn)
	ensure_wrestlers_table(conn)
	ensure_wrestler_history_table(conn)

	# Ensure idempotent reruns: clear history so we can reinsert cleanly
	conn.execute("""--sql
	DELETE FROM wrestler_history
	""")

	rows = fetch_matches_ordered(conn)
	log.info("matches to process: %s", len(rows))

	# In-memory trackers
	rating: Dict[str, float] = {}
	played: Dict[str, int] = {}
	best_elo: Dict[str, float] = {}
	wins: Dict[str, int] = {}
	wins_fall: Dict[str, int] = {}
	losses: Dict[str, int] = {}
	losses_fall: Dict[str, int] = {}
	dqs: Dict[str, int] = {}
	opp_sum: Dict[str, float] = {}
	opp_cnt: Dict[str, int] = {}
	seq: int = 0

	def _vals(name: str, team: Optional[str], opp_name: Optional[str], last_adj: float) -> list:
		w = wins.get(name, 0)
		wf = wins_fall.get(name, 0)
		losses_cnt = losses.get(name, 0)
		lf = losses_fall.get(name, 0)
		dqv = dqs.get(name, 0)
		os = opp_sum.get(name, 0.0)
		oc = opp_cnt.get(name, 0)
		oavg = (os / oc) if oc > 0 else None
		return [name, rating.get(name, 1000.0), played.get(name, 0), event_id, start_date,
				opp_name, last_adj, team, best_elo.get(name, rating.get(name, 1000.0)), start_date,
				w, wf, losses_cnt, lf, dqv, os, oc, oavg]

	def _vals2(name: str, team: Optional[str], opp_name: Optional[str], last_adj: float) -> list:
		# wrapper; identical shape to _vals
		return _vals(name, team, opp_name, last_adj)

	for (rowid, event_id, round_id, weight_class, wname, lname, d_type, d_code, rdetail, wpts, lpts, ftime, wteam, lteam, start_date) in progress(rows, total=len(rows), desc="Elo matches"):
		seq += 1
		# Initialize ratings if new
		ra = rating.get(wname, 1000.0)
		rb = rating.get(lname, 1000.0)
		# Expected and K
		ea = expected_score(ra, rb)
		k, t_mult, m_mult, q_mult, margin, fsec = k_components(d_type, d_code, wpts, lpts, ftime)
		rd_order = round_sort_key(rdetail)
		if k <= 0.0:
			# No change (bye or ignored)
			conn.execute(
				"""--sql
				UPDATE matches SET winner_elo_after = ?, winner_elo_adjustment = ?,
								   loser_elo_after = ?, loser_elo_adjustment = ?,
								   elo_computed_at = now(),
								   elo_sequence = ?,
								   winner_elo_before = ?, loser_elo_before = ?,
								   expected_winner = ?, expected_loser = ?,
								   k_applied = ?, k_type_mult = ?, k_mov_mult = ?, k_quick_mult = ?,
								   margin = ?, fall_seconds = ?, round_order = ?,
								   winner_prev_matches = ?, loser_prev_matches = ?
				WHERE rowid = ?
				""",
				[ra, 0.0, rb, 0.0,
				 seq, ra, rb, ea, 1.0 - ea, k, t_mult, m_mult, q_mult,
				 margin if margin is not None else None, fsec if fsec is not None else None, rd_order,
				 played.get(wname, 0), played.get(lname, 0),
				 rowid],
			)
			# Update wrestlers table with unchanged ratings
			wp = played.get(wname, 0) + 1
			lp = played.get(lname, 0) + 1
			played[wname] = wp
			played[lname] = lp
			rating[wname] = ra
			rating[lname] = rb
			prev_best_w = best_elo.get(wname, 1000.0)
			prev_best_l = best_elo.get(lname, 1000.0)
			if ra > prev_best_w:
				best_elo[wname] = ra
			if rb > prev_best_l:
				best_elo[lname] = rb
			# Persist wrestlers (winner and loser); avoid aliasing target table in ON CONFLICT
			conn.execute(
				"""--sql
				INSERT INTO wrestlers (name, current_elo, matches_played, last_event_id, last_start_date,
										 last_opponent_name, last_adjustment, last_team, best_elo, best_date,
										 wins, wins_fall, losses, losses_fall, dqs, opponent_elo_sum, opponent_elo_count, opponent_avg_elo)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				ON CONFLICT (name) DO UPDATE SET
					current_elo = EXCLUDED.current_elo,
					matches_played = EXCLUDED.matches_played,
					last_event_id = EXCLUDED.last_event_id,
					last_start_date = EXCLUDED.last_start_date,
					last_updated = now(),
					last_opponent_name = EXCLUDED.last_opponent_name,
					last_adjustment = EXCLUDED.last_adjustment,
					last_team = EXCLUDED.last_team,
					best_elo = GREATEST(COALESCE(best_elo, EXCLUDED.best_elo), EXCLUDED.best_elo),
					best_date = CASE WHEN EXCLUDED.best_elo >= COALESCE(best_elo, EXCLUDED.best_elo) THEN EXCLUDED.best_date ELSE best_date END,
					wins = EXCLUDED.wins,
					wins_fall = EXCLUDED.wins_fall,
					losses = EXCLUDED.losses,
					losses_fall = EXCLUDED.losses_fall,
					dqs = EXCLUDED.dqs,
					opponent_elo_sum = EXCLUDED.opponent_elo_sum,
					opponent_elo_count = EXCLUDED.opponent_elo_count,
					opponent_avg_elo = EXCLUDED.opponent_avg_elo
				""",
				_vals(wname, wteam, lname, 0.0)
			)
			conn.execute(
				"""--sql
				INSERT INTO wrestlers (name, current_elo, matches_played, last_event_id, last_start_date,
										 last_opponent_name, last_adjustment, last_team, best_elo, best_date,
										 wins, wins_fall, losses, losses_fall, dqs, opponent_elo_sum, opponent_elo_count, opponent_avg_elo)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				ON CONFLICT (name) DO UPDATE SET
					current_elo = EXCLUDED.current_elo,
					matches_played = EXCLUDED.matches_played,
					last_event_id = EXCLUDED.last_event_id,
					last_start_date = EXCLUDED.last_start_date,
					last_updated = now(),
					last_opponent_name = EXCLUDED.last_opponent_name,
					last_adjustment = EXCLUDED.last_adjustment,
					last_team = EXCLUDED.last_team,
					best_elo = GREATEST(COALESCE(best_elo, EXCLUDED.best_elo), EXCLUDED.best_elo),
					best_date = CASE WHEN EXCLUDED.best_elo >= COALESCE(best_elo, EXCLUDED.best_elo) THEN EXCLUDED.best_date ELSE best_date END,
					wins = EXCLUDED.wins,
					wins_fall = EXCLUDED.wins_fall,
					losses = EXCLUDED.losses,
					losses_fall = EXCLUDED.losses_fall,
					dqs = EXCLUDED.dqs,
					opponent_elo_sum = EXCLUDED.opponent_elo_sum,
					opponent_elo_count = EXCLUDED.opponent_elo_count,
					opponent_avg_elo = EXCLUDED.opponent_avg_elo
				""",
				_vals(lname, lteam, wname, 0.0)
			)
			# Insert wrestler_history rows (bye)
			conn.execute(
				"""--sql
				INSERT INTO wrestler_history (
					match_rowid, role, name, team, event_id, round_id, weight_class, start_date,
						opponent_name, opponent_team, opponent_pre_elo, opponent_post_elo, pre_elo, post_elo, adjustment, expected_score,
					k_applied, k_type_mult, k_mov_mult, k_quick_mult,
					decision_type, decision_type_code, margin, fall_seconds,
					round_detail, round_order, bye, elo_sequence
					) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				""",
					[rowid, 'W', wname, wteam, event_id, round_id, weight_class, start_date,
						lname, lteam, rb, rb, ra, ra, 0.0, ea,
					k, t_mult, m_mult, q_mult,
					d_type, d_code, margin if margin is not None else None, fsec if fsec is not None else None,
					rdetail, rd_order, True, seq]
			)
			conn.execute(
				"""--sql
					INSERT INTO wrestler_history (
					match_rowid, role, name, team, event_id, round_id, weight_class, start_date,
						opponent_name, opponent_team, opponent_pre_elo, opponent_post_elo, pre_elo, post_elo, adjustment, expected_score,
					k_applied, k_type_mult, k_mov_mult, k_quick_mult,
					decision_type, decision_type_code, margin, fall_seconds,
					round_detail, round_order, bye, elo_sequence
					) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				""",
					[rowid, 'L', lname, lteam, event_id, round_id, weight_class, start_date,
						wname, wteam, ra, ra, rb, rb, 0.0, 1.0 - ea,
					k, t_mult, m_mult, q_mult,
					d_type, d_code, margin if margin is not None else None, fsec if fsec is not None else None,
					rdetail, rd_order, True, seq]
			)
			continue
		# Winner scored 1, loser 0
		delta_a = k * (1.0 - ea)
		delta_b = -delta_a
		ra2 = ra + delta_a
		rb2 = rb + delta_b
		# Update map
		rating[wname] = ra2
		rating[lname] = rb2
		played[wname] = played.get(wname, 0) + 1
		played[lname] = played.get(lname, 0) + 1
		# Winner/loser summaries
		is_fall_win = ("fall" in (d_type or "").lower()) or (d_code or "").upper() in ("FALL", "PIN")
		is_fall_loss = is_fall_win  # same event implies fall for both
		is_dq = ("disq" in (d_type or "").lower()) or ((d_code or "").upper() == "DQ")
		wins[wname] = wins.get(wname, 0) + 1
		if is_fall_win:
			wins_fall[wname] = wins_fall.get(wname, 0) + 1
		losses[lname] = losses.get(lname, 0) + 1
		if is_fall_loss:
			losses_fall[lname] = losses_fall.get(lname, 0) + 1
		if is_dq:
			dqs[wname] = dqs.get(wname, 0) + 1
			dqs[lname] = dqs.get(lname, 0) + 1
		# Opponent strength accumulators (use pre-match elo)
		opp_sum[wname] = opp_sum.get(wname, 0.0) + rb
		opp_cnt[wname] = opp_cnt.get(wname, 0) + 1
		opp_sum[lname] = opp_sum.get(lname, 0.0) + ra
		opp_cnt[lname] = opp_cnt.get(lname, 0) + 1
		if ra2 > best_elo.get(wname, 1000.0):
			best_elo[wname] = ra2
		if rb2 > best_elo.get(lname, 1000.0):
			best_elo[lname] = rb2
		# Persist per-match
		conn.execute(
			"""--sql
			UPDATE matches SET winner_elo_after = ?, winner_elo_adjustment = ?,
							   loser_elo_after = ?, loser_elo_adjustment = ?,
							   elo_computed_at = now(),
							   elo_sequence = ?,
							   winner_elo_before = ?, loser_elo_before = ?,
							   expected_winner = ?, expected_loser = ?,
							   k_applied = ?, k_type_mult = ?, k_mov_mult = ?, k_quick_mult = ?,
							   margin = ?, fall_seconds = ?, round_order = ?,
							   winner_prev_matches = ?, loser_prev_matches = ?
			WHERE rowid = ?
			""",
			[ra2, delta_a, rb2, delta_b,
			 seq, ra, rb, ea, 1.0 - ea, k, t_mult, m_mult, q_mult,
			 margin if margin is not None else None, fsec if fsec is not None else None, rd_order,
			 played.get(wname, 0) - 1, played.get(lname, 0) - 1,
			 rowid],
		)
		# Persist wrestlers (winner) with summary stats
		conn.execute(
			"""--sql
			INSERT INTO wrestlers (
				name, current_elo, matches_played, last_event_id, last_start_date,
				last_opponent_name, last_adjustment, last_team, best_elo, best_date,
				wins, wins_fall, losses, losses_fall, dqs,
				opponent_elo_sum, opponent_elo_count, opponent_avg_elo
			)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT (name) DO UPDATE SET
				current_elo = EXCLUDED.current_elo,
				matches_played = EXCLUDED.matches_played,
				last_event_id = EXCLUDED.last_event_id,
				last_start_date = EXCLUDED.last_start_date,
				last_updated = now(),
				last_opponent_name = EXCLUDED.last_opponent_name,
				last_adjustment = EXCLUDED.last_adjustment,
				last_team = EXCLUDED.last_team,
				best_elo = GREATEST(COALESCE(best_elo, EXCLUDED.best_elo), EXCLUDED.best_elo),
				best_date = CASE WHEN EXCLUDED.best_elo >= COALESCE(best_elo, EXCLUDED.best_elo) THEN EXCLUDED.best_date ELSE best_date END,
				wins = EXCLUDED.wins,
				wins_fall = EXCLUDED.wins_fall,
				losses = EXCLUDED.losses,
				losses_fall = EXCLUDED.losses_fall,
				dqs = EXCLUDED.dqs,
				opponent_elo_sum = EXCLUDED.opponent_elo_sum,
				opponent_elo_count = EXCLUDED.opponent_elo_count,
				opponent_avg_elo = EXCLUDED.opponent_avg_elo
			""",
			_vals(wname, wteam, lname, float(delta_a))
		)
		# Persist wrestlers (loser) with summary stats
		conn.execute(
			"""--sql
			INSERT INTO wrestlers (
				name, current_elo, matches_played, last_event_id, last_start_date,
				last_opponent_name, last_adjustment, last_team, best_elo, best_date,
				wins, wins_fall, losses, losses_fall, dqs,
				opponent_elo_sum, opponent_elo_count, opponent_avg_elo
			)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT (name) DO UPDATE SET
				current_elo = EXCLUDED.current_elo,
				matches_played = EXCLUDED.matches_played,
				last_event_id = EXCLUDED.last_event_id,
				last_start_date = EXCLUDED.last_start_date,
				last_updated = now(),
				last_opponent_name = EXCLUDED.last_opponent_name,
				last_adjustment = EXCLUDED.last_adjustment,
				last_team = EXCLUDED.last_team,
				best_elo = GREATEST(COALESCE(best_elo, EXCLUDED.best_elo), EXCLUDED.best_elo),
				best_date = CASE WHEN EXCLUDED.best_elo >= COALESCE(best_elo, EXCLUDED.best_elo) THEN EXCLUDED.best_date ELSE best_date END,
				wins = EXCLUDED.wins,
				wins_fall = EXCLUDED.wins_fall,
				losses = EXCLUDED.losses,
				losses_fall = EXCLUDED.losses_fall,
				dqs = EXCLUDED.dqs,
				opponent_elo_sum = EXCLUDED.opponent_elo_sum,
				opponent_elo_count = EXCLUDED.opponent_elo_count,
				opponent_avg_elo = EXCLUDED.opponent_avg_elo
			""",
			_vals(lname, lteam, wname, float(delta_b))
		)
		# Insert wrestler_history rows (normal match)
		conn.execute(
			"""--sql
			INSERT INTO wrestler_history (
				match_rowid, role, name, team, event_id, round_id, weight_class, start_date,
				opponent_name, opponent_team, opponent_pre_elo, opponent_post_elo, pre_elo, post_elo, adjustment, expected_score,
				k_applied, k_type_mult, k_mov_mult, k_quick_mult,
				decision_type, decision_type_code, margin, fall_seconds,
				round_detail, round_order, bye, elo_sequence
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			""",
			[rowid, 'W', wname, wteam, event_id, round_id, weight_class, start_date,
				lname, lteam, rb, rb2, ra, ra2, float(delta_a), ea,
				k, t_mult, m_mult, q_mult,
				d_type, d_code, margin if margin is not None else None, fsec if fsec is not None else None,
				rdetail, rd_order, False, seq]
		)
		conn.execute(
			"""--sql
			INSERT INTO wrestler_history (
				match_rowid, role, name, team, event_id, round_id, weight_class, start_date,
				opponent_name, opponent_team, opponent_pre_elo, opponent_post_elo, pre_elo, post_elo, adjustment, expected_score,
				k_applied, k_type_mult, k_mov_mult, k_quick_mult,
				decision_type, decision_type_code, margin, fall_seconds,
				round_detail, round_order, bye, elo_sequence
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			""",
			[rowid, 'L', lname, lteam, event_id, round_id, weight_class, start_date,
				wname, wteam, ra, ra2, rb, rb2, float(delta_b), 1.0 - ea,
				k, t_mult, m_mult, q_mult,
				d_type, d_code, margin if margin is not None else None, fsec if fsec is not None else None,
				rdetail, rd_order, False, seq]
		)

	log.info("Elo calculation complete. Wrestlers rated: %s", len(rating))


if __name__ == "__main__":
	run()

