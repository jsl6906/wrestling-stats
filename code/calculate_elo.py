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
- Close-loss credit: an underdog can receive partial credit on a close loss (small point margin or SV/OT),
	which slightly reduces the winner's gain and can yield a small Elo increase for the underdog.
- Cooldown: Ratings converge toward baseline (1000) during periods of inactivity to prevent stale ratings.
  High ratings decay downward, low ratings recover upward. Default: 1% convergence per 90 days.
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
	from .config import get_db_path
except ImportError:
	from config import get_db_path

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


# Cooldown configuration
COOLDOWN_ENABLED = True
COOLDOWN_DAYS_THRESHOLD = 90  # Apply cooldown after 90+ days of inactivity
COOLDOWN_RATE_PER_DAY = 0.01 / 90  # 1% convergence over 90 days = ~0.011% per day
COOLDOWN_MIN_RATING = 800  # Don't decay below this rating
COOLDOWN_BASELINE = 1000  # Converge towards this baseline rating


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
	add("k_expected_mult", "DOUBLE")
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
			k_expected_mult DOUBLE,
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
		("k_expected_mult", "DOUBLE"),
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
	base_k = 48.0  # Increased from 32 for more aggressive rating changes
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

	# Tech/Major/SV/OT get a bigger base boost
	if "tech" in dt or dc.startswith("TF") or "major" in dt or dc.startswith("MD") or dc.startswith("SV") or dc in ("OT", "UTB"):
		type_mult = 1.30  # Increased from 1.10
		if margin is not None:
			# 5% per point, capped at +60% (more aggressive)
			mov_mult = 1.0 + min(0.60, 0.05 * margin)
	# Regular decisions
	elif "dec" in dt or dc == "DEC" or "decision" in dt:
		type_mult = 1.00
		if margin is not None:
			# 4% per point, capped at +50% (more aggressive)
			mov_mult = 1.0 + min(0.50, 0.04 * margin)
	# Falls, forfeits, defaults: treat as big wins; earlier time -> bigger boost
	if "fall" in dt or dc in ("FALL", "PIN", "FF", "FOR", "DEF"):
		# Much higher multiplier for falls - reward dominance
		# Add a quickness component: map fall time in [0, FALL_REF_SEC] to [2.25, 1.75]
		FALL_REF_SEC = 180  # reference period length (3 minutes) for scaling
		sec = _parse_fall_time_to_seconds(fall_time)
		quick_mult = 1.75  # Increased base from 1.25
		if sec is not None:
			x = max(0.0, min(1.0, 1.0 - (sec / float(FALL_REF_SEC))))
			quick_mult = 1.75 + 0.50 * x  # in [1.75, 2.25] - much higher range
		# For falls, ignore mov_mult and type_mult; use quick_mult
		return base_k * quick_mult, 1.0, 1.0, quick_mult, margin, sec

	return base_k * type_mult * mov_mult, type_mult, mov_mult, 1.0, margin, None


def k_factor(decision_type: Optional[str], decision_code: Optional[str], winner_points: Optional[int], loser_points: Optional[int], fall_time: Optional[str]) -> float:
	k, *_ = k_components(decision_type, decision_code, winner_points, loser_points, fall_time)
	return k


def close_loss_bonus_for_loser(
	loser_pre: float,
	winner_pre: float,
	decision_type: Optional[str],
	decision_code: Optional[str],
	margin: Optional[int],
) -> float:
	"""Return a small [0, 0.25] partial credit for the losing wrestler when:
	- The loser was the underdog (lower pre-match Elo), and
	- The match was close (small point margin) or went to SV/OT.

	This credit is applied symmetrically (winner loses same amount of actual score), preserving zero-sum Elo.
	"""
	dt = (decision_type or "").lower()
	dc = (decision_code or "").upper()
	# No bonus on falls/forfeits/defaults/tech falls
	if ("fall" in dt) or (dc in ("FALL", "PIN", "FF", "FOR", "DEF", "TF")):
		return 0.0
	# Only if loser was underdog
	gap = max(0.0, winner_pre - loser_pre)
	if gap <= 0:
		return 0.0
	# Closeness factor: SV/OT -> treat as maximum closeness; else use margin if available
	is_overtime = dc in ("SV-1", "SV1", "OT", "UTB") or ("sudden victory" in dt) or ("overtime" in dt)
	if is_overtime:
		close_factor = 1.0
	elif margin is not None:
		# Linear drop-off: margin 0-2 -> 1..0; clip to [0,1]
		close_factor = max(0.0, min(1.0, (2.0 - float(margin)) / 2.0))
	else:
		close_factor = 0.0
	if close_factor <= 0.0:
		return 0.0
	# Normalize rating gap to ~[0,1] over 400 Elo range
	gap_factor = max(0.0, min(1.0, gap / 400.0))
	# Base scale: up to 0.25 actual-score points (quite modest)
	bonus = 0.25 * close_factor * gap_factor
	return float(max(0.0, min(0.25, bonus)))


def apply_cooldown(
	current_rating: float,
	last_match_date: Any,
	current_date: Any,
	baseline_rating: float = COOLDOWN_BASELINE
) -> float:
	"""Apply rating cooldown for periods of inactivity.
	
	Ratings converge toward baseline (1000) during inactivity:
	- High ratings (>1000) decay downward toward 1000
	- Low ratings (<1000) recover upward toward 1000
	- Ratings at/near baseline remain stable
	
	Args:
		current_rating: Current Elo rating
		last_match_date: Date of wrestler's last match
		current_date: Current tournament date
		baseline_rating: Rating to converge towards (default 1000)
		
	Returns:
		Adjusted rating after cooldown convergence
	"""
	if not COOLDOWN_ENABLED:
		return current_rating
		
	if not last_match_date or not current_date:
		return current_rating
		
	try:
		# Parse dates
		if isinstance(last_match_date, str):
			from datetime import datetime
			last_dt = datetime.strptime(last_match_date, '%Y-%m-%d').date()
		else:
			last_dt = last_match_date
			
		if isinstance(current_date, str):
			from datetime import datetime
			current_dt = datetime.strptime(current_date, '%Y-%m-%d').date()
		else:
			current_dt = current_date
			
		# Calculate days since last match
		days_inactive = (current_dt - last_dt).days
		
		# Only apply cooldown after threshold
		if days_inactive <= COOLDOWN_DAYS_THRESHOLD:
			return current_rating
			
		# Calculate convergence toward baseline
		excess_days = days_inactive - COOLDOWN_DAYS_THRESHOLD
		convergence_factor = 1.0 - (COOLDOWN_RATE_PER_DAY * excess_days)
		convergence_factor = max(0.0, min(1.0, convergence_factor))  # Clamp to [0,1]
		
		# Apply convergence toward baseline (works for both high and low ratings)
		converged_rating = baseline_rating + (current_rating - baseline_rating) * convergence_factor
		
		# Respect minimum rating floor for extreme cases
		converged_rating = max(COOLDOWN_MIN_RATING, converged_rating)
		
		return float(converged_rating)
		
	except Exception:
		# If date parsing fails, return original rating
		return current_rating


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

	conn = duckdb.connect(str(get_db_path()))
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
	best_date_map: Dict[str, Any] = {}
	last_match_date: Dict[str, Any] = {}  # Track last match date for cooldown
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
		# Use the tracked best_date when best_elo was achieved; initialize to this match date if missing
		bdate = best_date_map.get(name, start_date)
		return [name, rating.get(name, 1000.0), played.get(name, 0), event_id, start_date,
				opp_name, last_adj, team, best_elo.get(name, rating.get(name, 1000.0)), bdate,
				w, wf, losses_cnt, lf, dqv, os, oc, oavg]

	def _vals2(name: str, team: Optional[str], opp_name: Optional[str], last_adj: float) -> list:
		# wrapper; identical shape to _vals
		return _vals(name, team, opp_name, last_adj)

	for (rowid, event_id, round_id, weight_class, wname, lname, d_type, d_code, rdetail, wpts, lpts, ftime, wteam, lteam, start_date) in progress(rows, total=len(rows), desc="Elo matches"):
		seq += 1
		# Do not initialize best_elo to baseline; only record post-match maxima
		
		# Apply cooldown for periods of inactivity
		if COOLDOWN_ENABLED:
			for name in [wname, lname]:
				if name in rating and name in last_match_date:
					cooled_rating = apply_cooldown(
						rating[name], 
						last_match_date[name], 
						start_date
					)
					if cooled_rating != rating[name]:
						log.debug(f"Cooldown applied to {name}: {rating[name]:.1f} -> {cooled_rating:.1f}")
						rating[name] = cooled_rating
		
		# Initialize ratings if new
		ra = rating.get(wname, 1000.0)
		rb = rating.get(lname, 1000.0)
		# Expected and K
		ea = expected_score(ra, rb)
		k, t_mult, m_mult, q_mult, margin, fsec = k_components(d_type, d_code, wpts, lpts, ftime)
		# Aggressive multiplier based on how expected the outcome is
		# Big upsets (low ea for winner) get MUCH larger swings
		# Expected outcomes (high ea) get reduced K to prevent runaway leaders
		k_expected_mult = 1.0
		if ea >= 0.80:
			# Linear from 0.80..1.00 -> 1.0..0.3 (reduce even more for heavy favorites)
			k_expected_mult = max(0.3, 1.0 - 3.5 * (ea - 0.80))
		elif ea <= 0.25:
			# Linear from 0.25..0.0 -> 1.0..2.5 (HUGE boost for big upsets)
			k_expected_mult = min(2.5, 1.0 + 6.0 * (0.25 - ea))
		elif ea <= 0.40:
			# Linear from 0.40..0.25 -> 1.0..1.0 (moderate boost for upsets)
			k_expected_mult = 1.0 + 1.5 * (0.40 - ea)
		# Apply expected multiplier
		k_adj = k * k_expected_mult
		rd_order = round_sort_key(rdetail)
		if k_adj <= 0.0:
			# No change (bye or ignored)
			conn.execute(
				"""--sql
				UPDATE matches SET winner_elo_after = ?, winner_elo_adjustment = ?,
								   loser_elo_after = ?, loser_elo_adjustment = ?,
								   elo_computed_at = now(),
								   elo_sequence = ?,
								   winner_elo_before = ?, loser_elo_before = ?,
								   expected_winner = ?, expected_loser = ?,
								 k_applied = ?, k_type_mult = ?, k_expected_mult = ?, k_mov_mult = ?, k_quick_mult = ?,
								   margin = ?, fall_seconds = ?, round_order = ?,
								   winner_prev_matches = ?, loser_prev_matches = ?
				WHERE rowid = ?
				""",
				[ra, 0.0, rb, 0.0,
				 seq, ra, rb, ea, 1.0 - ea, k_adj, t_mult, k_expected_mult, m_mult, q_mult,
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
			# Update last match dates for bye tracking
			last_match_date[wname] = start_date
			last_match_date[lname] = start_date
			prev_best_w = best_elo.get(wname, float("-inf"))
			prev_best_l = best_elo.get(lname, float("-inf"))
			if ra > prev_best_w:
				best_elo[wname] = ra
				best_date_map[wname] = start_date
			if rb > prev_best_l:
				best_elo[lname] = rb
				best_date_map[lname] = start_date
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
					best_elo = EXCLUDED.best_elo,
					best_date = EXCLUDED.best_date,
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
					best_elo = EXCLUDED.best_elo,
					best_date = EXCLUDED.best_date,
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
					k_applied, k_type_mult, k_expected_mult, k_mov_mult, k_quick_mult,
					decision_type, decision_type_code, margin, fall_seconds,
					round_detail, round_order, bye, elo_sequence
					) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				""",
					[rowid, 'W', wname, wteam, event_id, round_id, weight_class, start_date,
						lname, lteam, rb, rb, ra, ra, 0.0, ea,
					 k_adj, t_mult, k_expected_mult, m_mult, q_mult,
					d_type, d_code, margin if margin is not None else None, fsec if fsec is not None else None,
					rdetail, rd_order, True, seq]
			)
			conn.execute(
				"""--sql
					INSERT INTO wrestler_history (
					match_rowid, role, name, team, event_id, round_id, weight_class, start_date,
						opponent_name, opponent_team, opponent_pre_elo, opponent_post_elo, pre_elo, post_elo, adjustment, expected_score,
					k_applied, k_type_mult, k_expected_mult, k_mov_mult, k_quick_mult,
					decision_type, decision_type_code, margin, fall_seconds,
					round_detail, round_order, bye, elo_sequence
					) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				""",
					[rowid, 'L', lname, lteam, event_id, round_id, weight_class, start_date,
						wname, wteam, ra, ra, rb, rb, 0.0, 1.0 - ea,
					k_adj, t_mult, k_expected_mult, m_mult, q_mult,
					d_type, d_code, margin if margin is not None else None, fsec if fsec is not None else None,
					rdetail, rd_order, True, seq]
			)
			continue
		# Winner scored 1, loser 0, with optional close-loss credit to the underdog loser.
		# Compute margin and possible bonus for the loser
		# Note: margin was computed in k_components; recompute here to avoid refactor coupling
		margin_val = None
		if wpts is not None and lpts is not None:
			try:
				margin_val = max(0, int(wpts) - int(lpts))
			except Exception:
				margin_val = None
		bonus = close_loss_bonus_for_loser(
			loser_pre=rb,
			winner_pre=ra,
			decision_type=d_type,
			decision_code=d_code,
			margin=margin_val,
		)
		# Reduce close-loss bonus impact since we have more aggressive upset multipliers
		bonus = bonus * 0.5  # Cut close-loss bonus in half
		s_w = 1.0 - bonus
		s_l = 0.0 + bonus
		delta_a = k_adj * (s_w - ea)
		delta_b = k_adj * (s_l - (1.0 - ea))
		ra2 = ra + delta_a
		rb2 = rb + delta_b
		# Update map
		rating[wname] = ra2
		rating[lname] = rb2
		played[wname] = played.get(wname, 0) + 1
		played[lname] = played.get(lname, 0) + 1
		# Update last match dates for cooldown tracking
		last_match_date[wname] = start_date
		last_match_date[lname] = start_date
		# Winner/loser summaries
		is_fall_match = ("fall" in (d_type or "").lower()) or (d_code or "").upper() in ("FALL", "PIN")
		is_dq = ("disq" in (d_type or "").lower()) or ((d_code or "").upper() == "DQ")
		wins[wname] = wins.get(wname, 0) + 1
		if is_fall_match:
			wins_fall[wname] = wins_fall.get(wname, 0) + 1
		losses[lname] = losses.get(lname, 0) + 1
		if is_fall_match:
			losses_fall[lname] = losses_fall.get(lname, 0) + 1
		if is_dq:
			dqs[wname] = dqs.get(wname, 0) + 1
			dqs[lname] = dqs.get(lname, 0) + 1
		# Opponent strength accumulators (use pre-match elo)
		opp_sum[wname] = opp_sum.get(wname, 0.0) + rb
		opp_cnt[wname] = opp_cnt.get(wname, 0) + 1
		opp_sum[lname] = opp_sum.get(lname, 0.0) + ra
		opp_cnt[lname] = opp_cnt.get(lname, 0) + 1
		if ra2 > best_elo.get(wname, float("-inf")):
			best_elo[wname] = ra2
			best_date_map[wname] = start_date
		if rb2 > best_elo.get(lname, float("-inf")):
			best_elo[lname] = rb2
			best_date_map[lname] = start_date
		# Persist per-match
		conn.execute(
			"""--sql
			UPDATE matches SET winner_elo_after = ?, winner_elo_adjustment = ?,
							   loser_elo_after = ?, loser_elo_adjustment = ?,
							   elo_computed_at = now(),
							   elo_sequence = ?,
							   winner_elo_before = ?, loser_elo_before = ?,
							   expected_winner = ?, expected_loser = ?,
						 k_applied = ?, k_type_mult = ?, k_expected_mult = ?, k_mov_mult = ?, k_quick_mult = ?,
							   margin = ?, fall_seconds = ?, round_order = ?,
							   winner_prev_matches = ?, loser_prev_matches = ?
			WHERE rowid = ?
			""",
			[ra2, delta_a, rb2, delta_b,
			 seq, ra, rb, ea, 1.0 - ea, k_adj, t_mult, k_expected_mult, m_mult, q_mult,
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
				best_elo = EXCLUDED.best_elo,
				best_date = EXCLUDED.best_date,
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
				best_elo = EXCLUDED.best_elo,
				best_date = EXCLUDED.best_date,
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
				k_applied, k_type_mult, k_expected_mult, k_mov_mult, k_quick_mult,
				decision_type, decision_type_code, margin, fall_seconds,
				round_detail, round_order, bye, elo_sequence
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			""",
			[rowid, 'W', wname, wteam, event_id, round_id, weight_class, start_date,
				lname, lteam, rb, rb2, ra, ra2, float(delta_a), ea,
				 k_adj, t_mult, k_expected_mult, m_mult, q_mult,
				d_type, d_code, margin if margin is not None else None, fsec if fsec is not None else None,
				rdetail, rd_order, False, seq]
		)
		conn.execute(
			"""--sql
			INSERT INTO wrestler_history (
				match_rowid, role, name, team, event_id, round_id, weight_class, start_date,
					opponent_name, opponent_team, opponent_pre_elo, opponent_post_elo, pre_elo, post_elo, adjustment, expected_score,
				k_applied, k_type_mult, k_expected_mult, k_mov_mult, k_quick_mult,
				decision_type, decision_type_code, margin, fall_seconds,
				round_detail, round_order, bye, elo_sequence
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			""",
			[rowid, 'L', lname, lteam, event_id, round_id, weight_class, start_date,
				wname, wteam, ra, ra2, rb, rb2, float(delta_b), 1.0 - ea,
				k_adj, t_mult, k_expected_mult, m_mult, q_mult,
				d_type, d_code, margin if margin is not None else None, fsec if fsec is not None else None,
				rdetail, rd_order, False, seq]
		)

	log.info("Elo calculation complete. Wrestlers rated: %s", len(rating))


if __name__ == "__main__":
	run()

