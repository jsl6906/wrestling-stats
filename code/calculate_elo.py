"""
Compute Elo ratings for wrestlers across all matches, in tournament date order, and
write results back onto each match row.

Inputs (Postgres tables, scoped by gov_body):
- tournaments(gov_body, event_id, start_date, name, ...)
- matches(match_id, gov_body, event_id, round_id, weight_class, raw_match_results,
		  round_detail, winner_name, winner_team, loser_name, loser_team,
		  decision_type, decision_type_code, winner_points, loser_points, fall_time, bye,
		  ...)

Outputs (Elo columns on matches, plus wrestlers and wrestler_history rows):
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
  uv run code/calculate_elo.py                # Incremental: process only new matches
  uv run code/calculate_elo.py --recalculate  # Full recalculation: delete history and reprocess all
"""

from __future__ import annotations

import argparse
import logging
from datetime import date as _date
from typing import Dict, Optional, Any, List, Tuple

import psycopg

try:
	from .config import GOV_BODY
	from .db import get_pg_connection, ensure_schema
except ImportError:
	from config import GOV_BODY
	from db import get_pg_connection, ensure_schema

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

# Rows buffered before flushing to Postgres (remote round-trips dominate runtime)
WRITE_BATCH_SIZE = 1000


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


def delete_all_elo_data(conn: psycopg.Connection, log: logging.Logger) -> Tuple[int, int, int]:
	"""Delete all Elo data for this governing body for full recalculation.
	
	Returns tuple of (wrestlers_deleted, history_deleted, matches_cleared).
	"""
	history_count = conn.execute("""--sql
		DELETE FROM wrestler_history WHERE gov_body = %s
	""", [GOV_BODY]).rowcount

	wrestler_count = conn.execute("""--sql
		DELETE FROM wrestlers WHERE gov_body = %s
	""", [GOV_BODY]).rowcount

	matches_with_elo = conn.execute("""--sql
		UPDATE matches SET 
			winner_elo_after = NULL,
			winner_elo_adjustment = NULL,
			loser_elo_after = NULL,
			loser_elo_adjustment = NULL,
			elo_computed_at = NULL,
			elo_sequence = NULL,
			winner_elo_before = NULL,
			loser_elo_before = NULL,
			expected_winner = NULL,
			expected_loser = NULL,
			k_applied = NULL,
			k_type_mult = NULL,
			k_expected_mult = NULL,
			k_mov_mult = NULL,
			k_quick_mult = NULL,
			margin = NULL,
			fall_seconds = NULL,
			round_order = NULL,
			winner_prev_matches = NULL,
			loser_prev_matches = NULL
		WHERE gov_body = %s AND elo_computed_at IS NOT NULL
	""", [GOV_BODY]).rowcount

	log.info("Deleted Elo data: %d wrestlers, %d history records, %d matches cleared", 
			 wrestler_count, history_count, matches_with_elo)
	conn.commit()
	return (wrestler_count, history_count, matches_with_elo)


def load_existing_wrestlers(conn: psycopg.Connection) -> Dict[str, Dict[str, Any]]:
	"""Load existing wrestler data from the database for incremental processing.
	
	Returns a dict keyed by wrestler name with current stats.
	"""
	rows = conn.execute(
		"""--sql
		SELECT name, current_elo, matches_played, last_start_date, best_elo, best_date,
			   wins, wins_fall, losses, losses_fall, dqs, opponent_elo_sum, opponent_elo_count,
			   last_event_id, last_opponent_name, last_adjustment, last_team
		FROM wrestlers
		WHERE gov_body = %s
		""",
		[GOV_BODY],
	).fetchall()
	
	wrestlers = {}
	for row in rows:
		(name, current_elo, matches_played, last_start_date, best_elo, best_date,
		 wins, wins_fall, losses, losses_fall, dqs, opp_sum, opp_cnt,
		 last_event_id, last_opponent_name, last_adjustment, last_team) = row
		wrestlers[name] = {
			"rating": current_elo or 1000.0,
			"played": matches_played or 0,
			"last_match_date": last_start_date,
			"best_elo": best_elo or current_elo or 1000.0,
			"best_date": best_date or last_start_date,
			"wins": wins or 0,
			"wins_fall": wins_fall or 0,
			"losses": losses or 0,
			"losses_fall": losses_fall or 0,
			"dqs": dqs or 0,
			"opp_sum": opp_sum or 0.0,
			"opp_cnt": opp_cnt or 0,
			"last_event_id": last_event_id,
			"last_opponent_name": last_opponent_name,
			"last_adjustment": last_adjustment or 0.0,
			"last_team": last_team,
		}
	return wrestlers


def fetch_matches_ordered(conn: psycopg.Connection, incremental: bool = True) -> List[Tuple[Any, ...]]:
	"""Fetch matches to process, ordered by date and round.
	
	Args:
		conn: Postgres connection
		incremental: If True, only fetch unprocessed matches (elo_computed_at IS NULL).
					 If False, fetch all matches.
	"""
	incremental_filter = "AND m.elo_computed_at IS NULL" if incremental else ""
	rows = conn.execute(
		f"""--sql
		SELECT m.match_id, m.event_id, m.round_id, m.weight_class, m.winner_name, m.loser_name,
			m.decision_type, m.decision_type_code, m.round_detail,
			m.winner_points, m.loser_points, m.fall_time,
			m.winner_team, m.loser_team,
			t.start_date
		FROM matches m
		JOIN tournaments t ON t.gov_body = m.gov_body AND t.event_id = m.event_id
		WHERE m.gov_body = %s
		  AND COALESCE(m.bye, FALSE) = FALSE 
		  AND m.winner_name IS NOT NULL 
		  AND m.loser_name IS NOT NULL
		  {incremental_filter}
		ORDER BY t.start_date NULLS LAST, m.event_id, m.match_id
		""",
		[GOV_BODY],
	).fetchall()
	# Sort within event explicitly by round order
	def _key(r: Tuple[Any, ...]):
		(match_id, event_id, round_id, weight_class, wname, lname, d_type, d_code, rdetail, wpts, lpts, ftime, wteam, lteam, start_date) = r
		sd = start_date or _date.max
		return (sd, event_id, round_sort_key(rdetail))
	rows.sort(key=_key)
	return rows


def check_for_out_of_sequence_matches(conn: psycopg.Connection, rows: List[Tuple[Any, ...]], log: logging.Logger) -> None:
	"""Check if any new matches to be processed fall within the date range of already-processed matches.
	
	This warns the user if newly parsed matches are "out of sequence" - e.g., a match from 2 years ago
	that was just scraped but falls chronologically in the middle of matches that already have Elo computed.
	"""
	if not rows:
		return
	
	# Get the date range of matches that already have ELO computed
	result = conn.execute("""--sql
		SELECT MIN(t.start_date) as min_date, MAX(t.start_date) as max_date
		FROM matches m
		JOIN tournaments t ON t.gov_body = m.gov_body AND t.event_id = m.event_id
		WHERE m.gov_body = %s AND m.elo_computed_at IS NOT NULL
	""", [GOV_BODY]).fetchone()
	
	if not result or result[0] is None:
		# No matches have been processed yet, nothing to check
		return
	
	existing_min_date, existing_max_date = result
	
	# Extract dates from the new matches to be processed
	new_match_dates = []
	for row in rows:
		# start_date is the last element (index -1) in the row tuple
		start_date = row[-1]
		if start_date:
			new_match_dates.append((start_date, row[1], row[2]))  # date, event_id, round_id
	
	if not new_match_dates:
		return
	
	# Find matches that fall within the existing date range (out of sequence)
	out_of_sequence = []
	for date, event_id, round_id in new_match_dates:
		if existing_min_date <= date <= existing_max_date:
			out_of_sequence.append((date, event_id, round_id))
	
	if out_of_sequence:
		# Group by date for clearer warning
		from collections import defaultdict
		by_date = defaultdict(list)
		for date, event_id, round_id in out_of_sequence:
			by_date[date].append((event_id, round_id))
		
		log.warning("=" * 80)
		log.warning("OUT-OF-SEQUENCE MATCHES DETECTED!")
		log.warning("Found %d newly parsed match(es) that fall within the date range of already-processed matches.", len(out_of_sequence))
		log.warning("Existing match date range: %s to %s", existing_min_date, existing_max_date)
		log.warning("")
		log.warning("Out-of-sequence matches by date:")
		for date in sorted(by_date.keys()):
			events = set(event_id for event_id, _ in by_date[date])
			log.warning("  %s: %d match(es) from %d event(s)", date, len(by_date[date]), len(events))
			for event_id, round_id in sorted(by_date[date])[:5]:  # Show first 5
				log.warning("    - Event: %s, Round: %s", event_id, round_id)
			if len(by_date[date]) > 5:
				log.warning("    ... and %d more", len(by_date[date]) - 5)
		log.warning("")
		log.warning("These matches may affect ELO ratings chronologically.")
		log.warning("Consider running with --recalculate to recompute all ELO ratings from scratch.")
		log.warning("=" * 80)


def run(recalculate: bool = False) -> None:
	logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
	log = logging.getLogger(__name__)

	conn = get_pg_connection(autocommit=False)
	ensure_schema(conn)
	conn.commit()

	if recalculate:
		# Full recalculation: delete all Elo data and start from scratch
		log.info("Full recalculation mode: deleting existing Elo data")
		delete_all_elo_data(conn, log)
		log.info("Elo data deleted, starting full recalculation")

	rows = fetch_matches_ordered(conn, incremental=not recalculate)
	log.info("matches to process: %s", len(rows))
	
	if not rows:
		log.info("No matches to process")
		conn.close()
		return
	
	# Check for out-of-sequence matches in incremental mode
	if not recalculate:
		check_for_out_of_sequence_matches(conn, rows, log)

	# In-memory trackers
	last_event: Dict[str, Any] = {}
	last_opp: Dict[str, Optional[str]] = {}
	last_adj: Dict[str, float] = {}
	last_team: Dict[str, Optional[str]] = {}
	touched: set[str] = set()
	if recalculate:
		# Start from scratch
		rating: Dict[str, float] = {}
		played: Dict[str, int] = {}
		best_elo: Dict[str, float] = {}
		best_date_map: Dict[str, Any] = {}
		last_match_date: Dict[str, Any] = {}
		wins: Dict[str, int] = {}
		wins_fall: Dict[str, int] = {}
		losses: Dict[str, int] = {}
		losses_fall: Dict[str, int] = {}
		dqs: Dict[str, int] = {}
		opp_sum: Dict[str, float] = {}
		opp_cnt: Dict[str, int] = {}
		seq: int = 0
	else:
		# Load existing wrestler data for incremental update
		log.info("Incremental mode: loading existing wrestler data")
		wrestlers = load_existing_wrestlers(conn)
		rating: Dict[str, float] = {name: w["rating"] for name, w in wrestlers.items()}
		played: Dict[str, int] = {name: w["played"] for name, w in wrestlers.items()}
		best_elo: Dict[str, float] = {name: w["best_elo"] for name, w in wrestlers.items()}
		best_date_map: Dict[str, Any] = {name: w["best_date"] for name, w in wrestlers.items()}
		last_match_date: Dict[str, Any] = {name: w["last_match_date"] for name, w in wrestlers.items()}
		wins: Dict[str, int] = {name: w["wins"] for name, w in wrestlers.items()}
		wins_fall: Dict[str, int] = {name: w["wins_fall"] for name, w in wrestlers.items()}
		losses: Dict[str, int] = {name: w["losses"] for name, w in wrestlers.items()}
		losses_fall: Dict[str, int] = {name: w["losses_fall"] for name, w in wrestlers.items()}
		dqs: Dict[str, int] = {name: w["dqs"] for name, w in wrestlers.items()}
		opp_sum: Dict[str, float] = {name: w["opp_sum"] for name, w in wrestlers.items()}
		opp_cnt: Dict[str, int] = {name: w["opp_cnt"] for name, w in wrestlers.items()}
		last_event = {name: w["last_event_id"] for name, w in wrestlers.items()}
		last_opp = {name: w["last_opponent_name"] for name, w in wrestlers.items()}
		last_adj = {name: w["last_adjustment"] for name, w in wrestlers.items()}
		last_team = {name: w["last_team"] for name, w in wrestlers.items()}
		# Get the highest elo_sequence to continue from
		max_seq_result = conn.execute("""--sql
			SELECT COALESCE(MAX(elo_sequence), 0) FROM matches WHERE gov_body = %s
		""", [GOV_BODY]).fetchone()
		seq: int = max_seq_result[0] if max_seq_result else 0
		log.info("Loaded %d wrestlers, starting from sequence %d", len(wrestlers), seq)

	MATCH_UPDATE_SQL = """--sql
		UPDATE matches SET winner_elo_after = %s, winner_elo_adjustment = %s,
						   loser_elo_after = %s, loser_elo_adjustment = %s,
						   elo_computed_at = now(),
						   elo_sequence = %s,
						   winner_elo_before = %s, loser_elo_before = %s,
						   expected_winner = %s, expected_loser = %s,
						   k_applied = %s, k_type_mult = %s, k_expected_mult = %s, k_mov_mult = %s, k_quick_mult = %s,
						   margin = %s, fall_seconds = %s, round_order = %s,
						   winner_prev_matches = %s, loser_prev_matches = %s
		WHERE match_id = %s
	"""
	HISTORY_INSERT_SQL = """--sql
		INSERT INTO wrestler_history (
			match_id, role, gov_body, name, team, event_id, round_id, weight_class, start_date,
			opponent_name, opponent_team, opponent_pre_elo, opponent_post_elo, pre_elo, post_elo, adjustment, expected_score,
			k_applied, k_type_mult, k_expected_mult, k_mov_mult, k_quick_mult,
			decision_type, decision_type_code, margin, fall_seconds,
			round_detail, round_order, bye, elo_sequence
		) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
	"""
	WRESTLER_UPSERT_SQL = """--sql
		INSERT INTO wrestlers (
			gov_body, name, current_elo, matches_played, last_event_id, last_start_date,
			last_opponent_name, last_adjustment, last_team, best_elo, best_date,
			wins, wins_fall, losses, losses_fall, dqs,
			opponent_elo_sum, opponent_elo_count, opponent_avg_elo
		)
		VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
		ON CONFLICT (gov_body, name) DO UPDATE SET
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
	"""

	match_updates: List[list] = []
	history_rows: List[list] = []

	def _flush() -> None:
		if match_updates:
			with conn.cursor() as cur:
				cur.executemany(MATCH_UPDATE_SQL, match_updates)
			match_updates.clear()
		if history_rows:
			with conn.cursor() as cur:
				cur.executemany(HISTORY_INSERT_SQL, history_rows)
			history_rows.clear()
		conn.commit()

	def _record_last(name: str, team: Optional[str], opp_name: Optional[str], adj: float, event_id: str) -> None:
		last_event[name] = event_id
		last_opp[name] = opp_name
		last_adj[name] = adj
		last_team[name] = team
		touched.add(name)

	def _vals(name: str) -> list:
		w = wins.get(name, 0)
		wf = wins_fall.get(name, 0)
		losses_cnt = losses.get(name, 0)
		lf = losses_fall.get(name, 0)
		dqv = dqs.get(name, 0)
		os = opp_sum.get(name, 0.0)
		oc = opp_cnt.get(name, 0)
		oavg = (os / oc) if oc > 0 else None
		lmd = last_match_date.get(name)
		# Use the tracked best_date when best_elo was achieved; initialize to last match date if missing
		bdate = best_date_map.get(name, lmd)
		return [GOV_BODY, name, rating.get(name, 1000.0), played.get(name, 0), last_event.get(name), lmd,
				last_opp.get(name), last_adj.get(name, 0.0), last_team.get(name),
				best_elo.get(name, rating.get(name, 1000.0)), bdate,
				w, wf, losses_cnt, lf, dqv, os, oc, oavg]

	for (match_id, event_id, round_id, weight_class, wname, lname, d_type, d_code, rdetail, wpts, lpts, ftime, wteam, lteam, start_date) in progress(rows, total=len(rows), desc="Elo matches"):
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
			match_updates.append(
				[ra, 0.0, rb, 0.0,
				 seq, ra, rb, ea, 1.0 - ea, k_adj, t_mult, k_expected_mult, m_mult, q_mult,
				 margin if margin is not None else None, fsec if fsec is not None else None, rd_order,
				 played.get(wname, 0), played.get(lname, 0),
				 match_id],
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
			_record_last(wname, wteam, lname, 0.0, event_id)
			_record_last(lname, lteam, wname, 0.0, event_id)
			# wrestler_history rows (bye)
			history_rows.append(
					[match_id, 'W', GOV_BODY, wname, wteam, event_id, round_id, weight_class, start_date,
						lname, lteam, rb, rb, ra, ra, 0.0, ea,
					 k_adj, t_mult, k_expected_mult, m_mult, q_mult,
					d_type, d_code, margin if margin is not None else None, fsec if fsec is not None else None,
					rdetail, rd_order, True, seq]
			)
			history_rows.append(
					[match_id, 'L', GOV_BODY, lname, lteam, event_id, round_id, weight_class, start_date,
						wname, wteam, ra, ra, rb, rb, 0.0, 1.0 - ea,
					k_adj, t_mult, k_expected_mult, m_mult, q_mult,
					d_type, d_code, margin if margin is not None else None, fsec if fsec is not None else None,
					rdetail, rd_order, True, seq]
			)
			if len(match_updates) >= WRITE_BATCH_SIZE:
				_flush()
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
		_record_last(wname, wteam, lname, float(delta_a), event_id)
		_record_last(lname, lteam, wname, float(delta_b), event_id)
		# Buffer per-match writes
		match_updates.append(
			[ra2, delta_a, rb2, delta_b,
			 seq, ra, rb, ea, 1.0 - ea, k_adj, t_mult, k_expected_mult, m_mult, q_mult,
			 margin if margin is not None else None, fsec if fsec is not None else None, rd_order,
			 played.get(wname, 0) - 1, played.get(lname, 0) - 1,
			 match_id],
		)
		history_rows.append(
			[match_id, 'W', GOV_BODY, wname, wteam, event_id, round_id, weight_class, start_date,
				lname, lteam, rb, rb2, ra, ra2, float(delta_a), ea,
				 k_adj, t_mult, k_expected_mult, m_mult, q_mult,
				d_type, d_code, margin if margin is not None else None, fsec if fsec is not None else None,
				rdetail, rd_order, False, seq]
		)
		history_rows.append(
			[match_id, 'L', GOV_BODY, lname, lteam, event_id, round_id, weight_class, start_date,
				wname, wteam, ra, ra2, rb, rb2, float(delta_b), 1.0 - ea,
				k_adj, t_mult, k_expected_mult, m_mult, q_mult,
				d_type, d_code, margin if margin is not None else None, fsec if fsec is not None else None,
				rdetail, rd_order, False, seq]
		)
		if len(match_updates) >= WRITE_BATCH_SIZE:
			_flush()

	_flush()

	# Persist final state for every wrestler touched in this run
	wrestler_rows = [_vals(name) for name in sorted(touched)]
	if wrestler_rows:
		with conn.cursor() as cur:
			cur.executemany(WRESTLER_UPSERT_SQL, wrestler_rows)
	conn.commit()
	conn.close()

	log.info("Elo calculation complete. Wrestlers rated: %s (updated %d)", len(rating), len(wrestler_rows))


if __name__ == "__main__":
	parser = argparse.ArgumentParser(
		description="Compute Elo ratings for wrestlers across all matches."
	)
	parser.add_argument(
		"--recalculate",
		action="store_true",
		help="Full recalculation: delete all Elo history and reprocess all matches from scratch"
	)
	args = parser.parse_args()
	run(recalculate=args.recalculate)

