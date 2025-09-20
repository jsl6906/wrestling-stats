"""
Scrape TrackWrestling NVWF tournaments and save Round Results HTML per round.

Design (per approach.md):
- Iterate tournament result pages
- For each tournament: open modal, click Enter Event, go to Round Results
- Iterate rounds (excluding All Rounds), click Go, save HTML as raw_results/<date>_<name>/<round>.html
- Use DuckDB to keep lightweight metadata tables (tournaments, rounds, pages)

Notes:
- TrackWrestling uses a session (twSessionId + TIM). We stay within a single browser context
  to preserve cookies while iterating a given run. Saved HTML is raw content for offline parsing.
- This script only scrapes; parsing is handled separately in `extract_results.py`.

CLI examples (run with uv):
- uv run python -m code.scrape_raw_data --season 2024-2025 --max-tournaments 50
- uv run python -m code.scrape_raw_data --resume
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
import time
import importlib.util
from pathlib import Path
from dataclasses import dataclass
from datetime import date
from typing import List, Optional, Tuple

import duckdb
try:
	# When run as a module inside a package
	from .shared_trackwrestling import scrape_rounds_for_event
except Exception:
	# Fallback when running as a script
	_shared_path = Path(__file__).parent / 'shared_trackwrestling.py'
	_spec = importlib.util.spec_from_file_location('shared_trackwrestling', str(_shared_path))
	if _spec and _spec.loader:
		_mod = importlib.util.module_from_spec(_spec)
		sys.modules[_spec.name] = _mod  # type: ignore[index]
		_spec.loader.exec_module(_mod)  # type: ignore[attr-defined]
		scrape_rounds_for_event = _mod.scrape_rounds_for_event  # type: ignore[attr-defined]
	else:
		raise RuntimeError('Failed to load shared_trackwrestling')

# We import Playwright lazily in main() to allow syntax checks without it installed.

BASE_SEARCH_URL = (
	"https://www.trackwrestling.com/Login.jsp?tName=NVWF&state=&sDate=&eDate=&lastName=&firstName="
	"&teamName=&sfvString=&city=&gbId=&camps=false"
)


# ---- Data models ----
@dataclass
class Tournament:
	event_id: str
	name: str
	year: Optional[int]
	index_on_page: int


## Removed Round dataclass while focusing on tournaments only


# ---- DuckDB helpers ----
def ensure_db(conn: duckdb.DuckDBPyConnection) -> None:
	conn.execute(
		"""--sql
		CREATE TABLE IF NOT EXISTS tournaments (
			event_id TEXT PRIMARY KEY,
			name TEXT,
			year INTEGER,
			start_date DATE,
			end_date DATE,
			address TEXT,
			venue TEXT,
			street TEXT,
			city TEXT,
			state TEXT,
			postal_code TEXT,
			first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
		"""
	)
	# Dropped round/pages tables for now while focusing on tournament list


def upsert_tournament(conn: duckdb.DuckDBPyConnection, t: Tournament) -> None:
	conn.execute(
		"""--sql
		INSERT INTO tournaments AS t (event_id, name, year)
		VALUES (?, ?, ?)
		ON CONFLICT (event_id) DO UPDATE SET
			name = EXCLUDED.name,
			year = EXCLUDED.year
		""",
		[t.event_id, t.name, t.year],
	)


def upsert_tournament_details(
	conn: duckdb.DuckDBPyConnection,
	*,
	event_id: str,
	name: Optional[str] = None,
	year: Optional[int] = None,
	start_date: Optional[str] = None,
	end_date: Optional[str] = None,
	address: Optional[str] = None,
	venue: Optional[str] = None,
	street: Optional[str] = None,
	city: Optional[str] = None,
	state: Optional[str] = None,
	postal_code: Optional[str] = None,
) -> None:
	# Minimal upsert of event_id, then update any provided fields
	conn.execute(
		"""--sql
		INSERT INTO tournaments (event_id)
		VALUES (?)
		ON CONFLICT (event_id) DO NOTHING
		""",
		[event_id],
	)
	assignments = []
	params: List[object] = []
	if name is not None:
		assignments.append("name = ?")
		params.append(name)
	if year is not None:
		assignments.append("year = ?")
		params.append(year)
	if start_date is not None:
		assignments.append("start_date = ?")
		params.append(start_date)
	if end_date is not None:
		assignments.append("end_date = ?")
		params.append(end_date)
	if address is not None:
		assignments.append("address = ?")
		params.append(address)
	if venue is not None:
		assignments.append("venue = ?")
		params.append(venue)
	if street is not None:
		assignments.append("street = ?")
		params.append(street)
	if city is not None:
		assignments.append("city = ?")
		params.append(city)
	if state is not None:
		assignments.append("state = ?")
		params.append(state)
	if postal_code is not None:
		assignments.append("postal_code = ?")
		params.append(postal_code)
	if not assignments:
		return
	params.append(event_id)
	conn.execute(
		f"""--sql
		UPDATE tournaments
		SET {', '.join(assignments)}
		WHERE event_id = ?
		""",
		params,
	)


## Removed upsert_round and record_page helpers for now


# ---- Utility helpers ----
def safe_dir_name(name: str) -> str:
	# Replace characters not allowed in Windows filenames
	return re.sub(r"[^\w\-\. ]+", "_", name).strip()


def extract_event_id(js_call: str) -> Optional[str]:
	# Format: javascript:eventSelected(877838132,'2025 NVWF Sample Scramble',2, '', 0);
	m = re.search(r"eventSelected\((\d+),", js_call)
	return m.group(1) if m else None


def extract_event_name(js_call: str) -> Optional[str]:
	m = re.search(r"eventSelected\(\d+,\'(.*?)\'", js_call)
	return m.group(1) if m else None


def event_year_from_name(name: str) -> Optional[int]:
	m = re.search(r"(20\d{2})", name)
	return int(m.group(1)) if m else None
def parse_date_range(text: str) -> Tuple[Optional[str], Optional[str]]:
	# Normalize whitespace
	t = re.sub(r"\s+", " ", (text or "").strip())
	if not t:
		return None, None
	# Patterns to try:
	# 1) MM/DD - MM/DD/YYYY
	m = re.match(r"^(\d{2}/\d{2})\s*-\s*(\d{2}/\d{2}/(\d{4}))$", t)
	if m:
		mmdd_start = m.group(1)
		end_full = m.group(2)
		year = int(m.group(3))
		start_date = f"{year}-{mmdd_start[0:2]}-{mmdd_start[3:5]}"
		end_parts = end_full.split("/")
		end_date = f"{end_parts[2]}-{end_parts[0]}-{end_parts[1]}"
		return start_date, end_date
	# 2) MM/DD/YYYY - MM/DD/YYYY
	m = re.match(r"^(\d{2}/\d{2}/\d{4})\s*-\s*(\d{2}/\d{2}/\d{4})$", t)
	if m:
		s_parts = m.group(1).split("/")
		e_parts = m.group(2).split("/")
		return f"{s_parts[2]}-{s_parts[0]}-{s_parts[1]}", f"{e_parts[2]}-{e_parts[0]}-{e_parts[1]}"
	# 3) MM/DD/YYYY (single day)
	m = re.match(r"^(\d{2}/\d{2}/\d{4})$", t)
	if m:
		parts = m.group(1).split("/")
		iso = f"{parts[2]}-{parts[0]}-{parts[1]}"
		return iso, iso
	# Fallback None
	return None, None


def parse_address_lines(text: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
	"""Split address span into venue, street, city, state, postal. Input may contain line breaks."""
	if not text:
		return None, None, None, None, None
	lines = [ln.strip() for ln in text.replace("\r", "").split("\n") if ln.strip()]
	# If <br> are not converted to newlines, attempt to split on <br>
	if len(lines) <= 1 and "<br>" in text:
		lines = [ln.strip() for ln in text.split("<br>") if ln.strip()]
	venue = lines[0] if len(lines) > 0 else None
	street = lines[1] if len(lines) > 1 else None
	city = state = postal = None
	if len(lines) > 2:
		m = re.match(r"^([^,]+),\s*([A-Z]{2})\s*(\d{5})?$", lines[2])
		if m:
			city, state, postal = m.group(1), m.group(2), m.group(3)
		else:
			city = lines[2]
	return venue, street, city, state, postal


# ---- Scraper core ----
def run_scraper(args: argparse.Namespace) -> None:
	# Delay import to allow linting without playwright installed
	from playwright.sync_api import sync_playwright

	out_root = Path("Raw Results")
	out_root.mkdir(parents=True, exist_ok=True)

	# Open local DuckDB file next to outputs
	db_path = Path("output") / "scrape.db"
	db_path.parent.mkdir(parents=True, exist_ok=True)
	conn = duckdb.connect(str(db_path))
	ensure_db(conn)

	with sync_playwright() as p:
		browser = p.chromium.launch(headless=not args.show)
		context = browser.new_context()
		page = context.new_page()

		page.goto(BASE_SEARCH_URL, wait_until="domcontentloaded")

		tournaments_scraped = 0
		page_index = 0
		# Overall summary counters for this run
		overall_events = 0
		overall_events_succeeded = 0
		overall_events_skipped = 0
		overall_rounds_discovered = 0
		overall_round_rows_added = 0
		overall_html_saved = 0

		def find_tournament_frame():
			# Try to find the frame that contains the tournament list
			for fr in page.frames:
				try:
					cnt = fr.locator('[href*="eventSelected("], [onclick*="eventSelected("]').count()
					if cnt and cnt > 0:
						return fr
				except Exception:
					continue
			return None

		def wait_for_tournament_frame(timeout_ms: int = 8000):
			start = time.time()
			while (time.time() - start) * 1000 < timeout_ms:
				fr = find_tournament_frame()
				if fr is not None:
					return fr
				time.sleep(0.25)
			return None

		def return_to_list(max_steps: int = 5) -> bool:
			"""Navigate back until the tournaments frame is visible again."""
			for _ in range(max_steps):
				try:
					fr = find_tournament_frame()
					if fr is not None:
						return True
					page.go_back(wait_until="domcontentloaded")
					time.sleep(0.3)
				except Exception:
					pass
			# Fallback: hard goto
			try:
				page.goto(BASE_SEARCH_URL, wait_until="domcontentloaded")
				return wait_for_tournament_frame() is not None
			except Exception:
				return False

		def parse_tournament_list(list_frame) -> List[dict]:
			"""Return list of dicts with tournament metadata from ul.tournament-ul."""
			results: List[dict] = []
			ul = list_frame.locator('ul.tournament-ul')
			if ul.count() == 0:
				return results
			items = ul.locator('> li').element_handles()
			for li in items:
				anchor = li.query_selector('a[href*="eventSelected("], [onclick*="eventSelected("]')
				if not anchor:
					continue
				href = anchor.get_attribute('href') or ''
				onclick = anchor.get_attribute('onclick') or ''
				js = href or onclick
				event_id = extract_event_id(js or '')
				if not event_id:
					continue
				divs = li.query_selector_all('div')
				name = None
				start_iso = end_iso = None
				if len(divs) >= 2:
					spans = divs[1].query_selector_all('a span')
					if len(spans) >= 1:
						name = (spans[0].text_content() or '').strip()
					if len(spans) >= 2:
						date_text = (spans[1].text_content() or '').strip()
						start_iso, end_iso = parse_date_range(date_text)
				address_text = venue = street = city = state = postal = None
				if len(divs) >= 3:
					addr_span = divs[2].query_selector('td:nth-of-type(2) > span')
					if addr_span:
						address_text = (addr_span.inner_text() or '').strip()
						venue, street, city, state, postal = parse_address_lines(address_text)
				year = None
				if name:
					year = event_year_from_name(name)
				if year is None and start_iso:
					try:
						year = int(start_iso.split('-')[0])
					except Exception:
						year = None
				results.append({
					"event_id": event_id,
					"name": name,
					"year": year,
					"start_date": start_iso,
					"end_date": end_iso,
					"address": address_text,
					"venue": venue,
					"street": street,
					"city": city,
					"state": state,
					"postal_code": postal,
				})
			return results

		logger = logging.getLogger(__name__)
		while True:
			page_index += 1
			list_frame = wait_for_tournament_frame()
			if list_frame is None:
				break

			# Parse tournament listings and upsert details
			try:
				t_list = parse_tournament_list(list_frame)
				logger.info("[page %s] tournaments found: %s", page_index, len(t_list))
				# Upsert details for all visible tournaments on this page (keeps DB fresh)
				for tmeta in t_list:
					upsert_tournament_details(
						conn,
						event_id=tmeta["event_id"],
						name=tmeta["name"],
						year=tmeta["year"],
						start_date=tmeta["start_date"],
						end_date=tmeta["end_date"],
						address=tmeta["address"],
						venue=tmeta["venue"],
						street=tmeta["street"],
						city=tmeta["city"],
						state=tmeta["state"],
						postal_code=tmeta["postal_code"],
					)

				# Helper checks
				def _is_future_or_today(t: dict) -> bool:
					start_iso = t.get("start_date")
					if not start_iso:
						return False
					try:
						return date.fromisoformat(start_iso) >= date.today()
					except Exception:
						return False

				def _needs_scrape(eid: str) -> bool:
					# If rounds table missing or no rows, needs scrape
					try:
						row = conn.execute("""--sql
							SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'tournament_rounds'
						""").fetchone()
						exists = bool(row and row[0] and row[0] > 0)
					except Exception:
						exists = False
					if not exists:
						return True
					try:
						row = conn.execute("""--sql
							SELECT COUNT(*) FROM tournament_rounds WHERE event_id = ?
						""", [eid]).fetchone()
						total = row[0] if row else 0
						if total == 0:
							return True
						row = conn.execute("""--sql
							SELECT COUNT(*) FROM tournament_rounds WHERE event_id = ? AND raw_html IS NULL
						""", [eid]).fetchone()
						missing = row[0] if row else 0
						return missing > 0
					except Exception:
						return True

				# Build eligible list honoring date and rounds state
				eligible: List[dict] = []
				for tmeta in t_list:
					if not tmeta.get("event_id"):
						continue
					if _is_future_or_today(tmeta):
						logger.info("[skip][date] %s | name=%s | start_date=%s >= today", tmeta["event_id"], tmeta.get("name"), tmeta.get("start_date"))
						continue
					if not _needs_scrape(tmeta["event_id"]):
						logger.info("[skip][complete] %s | name=%s | rounds complete (raw_html present)", tmeta["event_id"], tmeta.get("name"))
						continue
					eligible.append(tmeta)

				# Honor max-tournaments across pages based on eligible only
				to_process = eligible
				if args.max_tournaments is not None:
					remaining = max(0, args.max_tournaments - tournaments_scraped)
					to_process = eligible[:remaining]

				# After upserting, scrape rounds for eligible subset
				for tmeta in to_process:
					eid = tmeta.get("event_id")
					if not eid:
						continue
					# Summary before/after counters per event
					try:
						row = conn.execute("""--sql
							SELECT COUNT(*) FROM tournament_rounds WHERE event_id = ?
						""", [eid]).fetchone()
						before_total = row[0] if row else 0
						row = conn.execute("""--sql
							SELECT COUNT(*) FROM tournament_rounds WHERE event_id = ? AND raw_html IS NOT NULL
						""", [eid]).fetchone()
						before_html = row[0] if row else 0
					except Exception:
						before_total = 0
						before_html = 0
					res = scrape_rounds_for_event(page, conn, eid)
					overall_events += 1
					if res >= 0:
						# Compute deltas
						try:
							row = conn.execute("""--sql
								SELECT COUNT(*) FROM tournament_rounds WHERE event_id = ?
							""", [eid]).fetchone()
							after_total = row[0] if row else before_total
							row = conn.execute("""--sql
								SELECT COUNT(*) FROM tournament_rounds WHERE event_id = ? AND raw_html IS NOT NULL
							""", [eid]).fetchone()
							after_html = row[0] if row else before_html
						except Exception:
							after_total = before_total
							after_html = before_html
						delta_total = max(0, after_total - before_total)
						delta_html = max(0, after_html - before_html)
						overall_events_succeeded += 1
						overall_rounds_discovered += res
						overall_round_rows_added += delta_total
						overall_html_saved += delta_html
						logger.info("[summary][event] %s | name=%s | rounds_discovered=%s | rows_added=%s | html_saved=%s",
													    eid, tmeta.get("name"), res, delta_total, delta_html)
					else:
						overall_events_skipped += 1
						logger.warning("[summary][event] %s | name=%s | skipped code=%s",
						   eid, tmeta.get("name"), res)
					# Return to the list to keep context
					try:
						# Best-effort: go back until list is visible
						for _ in range(3):
							fr = find_tournament_frame()
							if fr is not None:
								break
							page.go_back(wait_until="domcontentloaded")
							time.sleep(0.2)
					except Exception:
						pass
				# Count processed (attempted) tournaments to honor max-tournaments if set
				tournaments_scraped += len(to_process)
			except Exception:
				pass

			# No rounds/results clickthrough while focusing on tournaments only

			if args.max_tournaments and tournaments_scraped >= args.max_tournaments:
				break

			# Next page of tournaments: click and wait for list content to change
			list_frame = wait_for_tournament_frame()
			if list_frame is None:
				break
			ul = list_frame.query_selector('ul.tournament-ul')
			before_html = ul.inner_html() if ul else None
			# Also capture first/last event_id on page to detect change
			def page_event_ids(fr):
				ids = []
				for a in fr.locator('a[href*="eventSelected("], [onclick*="eventSelected("]').element_handles():
					js = (a.get_attribute('href') or a.get_attribute('onclick') or '')
					eid = extract_event_id(js or '')
					if eid:
						ids.append(eid)
				return ids
			before_ids = page_event_ids(list_frame)
			logger.debug("[page %s] before_ids: %s...%s", page_index, before_ids[:3], before_ids[-3:] if len(before_ids)>3 else before_ids)
			# Debug: enumerate candidate next buttons
			try:
				next_candidates = list_frame.locator('a[href="javascript:nextTournaments()"], [onclick^="nextTournaments"]').element_handles()
				logger.debug("[page %s] next candidates: %s", page_index, len(next_candidates))
				for idx, nh in enumerate(next_candidates):
					try:
						cls = nh.get_attribute('class') or ''
						aria = nh.get_attribute('aria-disabled') or ''
						vis = nh.evaluate("el => !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length)")
						logger.debug("  - cand[%s] class='%s' aria-disabled='%s' visible=%s", idx, cls, aria, vis)
					except Exception:
						pass
			except Exception:
				pass
			advanced = False
			curr_ids = []
			try:
				# Prefer explicit next button structure the site uses
				next_link = list_frame.locator('a[href="javascript:nextTournaments()"]:has(i.icon-arrow_r.dgNext)').first
				if next_link.count() == 0:
					# Fallbacks
					next_link = list_frame.locator('a[href="javascript:nextTournaments()"]').first
					if next_link.count() == 0:
						next_link = list_frame.locator('[onclick^="nextTournaments"]').first
				logger.debug("[page %s] next_link count: %s", page_index, next_link.count())
				next_all = list_frame.locator('a[href="javascript:nextTournaments()"], [onclick^="nextTournaments"]').element_handles()
				# Choose the last visible candidate as the likely bottom pager
				clicked = False
				if next_all:
					for idx in range(len(next_all) - 1, -1, -1):
						nh = next_all[idx]
						try:
							visible = nh.evaluate("el => !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length)")
							if not visible:
								continue
							cls = nh.get_attribute('class') or ''
							aria = nh.get_attribute('aria-disabled') or ''
							if 'disabled' in cls or aria.lower() == 'true':
								continue
							logger.debug("[page %s] clicking next candidate index=%s class='%s' aria='%s'", page_index, idx, cls, aria)
							nh.scroll_into_view_if_needed()
							nh.click()
							clicked = True
							break
						except Exception:
							continue
				if not clicked and next_link.count() > 0:
					# Some sites disable the button on last page; skip if aria-disabled or class contains disabled
					is_disabled = False
					try:
						cls = next_link.get_attribute('class') or ''
						aria = next_link.get_attribute('aria-disabled') or ''
						is_disabled = 'disabled' in cls or aria.lower() == 'true'
					except Exception:
						pass
					if is_disabled:
						advanced = False
					else:
						logger.debug("[page %s] clicking next_link directly", page_index)
						next_link.scroll_into_view_if_needed()
						next_link.click()
				else:
					# Try evaluating JS directly in frame
					try:
						logger.debug("[page %s] evaluating nextTournaments() in frame", page_index)
						list_frame.evaluate("nextTournaments()")
					except Exception:
						pass
				# Small delay to allow the AJAX request to start
				time.sleep(0.5)
				# Wait for list content or event_ids to change (client-side update)
				start = time.time()
				while (time.time() - start) < 10.0:
					list_frame = wait_for_tournament_frame()
					if list_frame is None:
						break
					ul2 = list_frame.query_selector('ul.tournament-ul')
					curr_ids = page_event_ids(list_frame)
					if ul and ul2:
						curr = ul2.inner_html()
						if (before_html is not None and curr != before_html) or (curr_ids and curr_ids != before_ids):
							advanced = True
							break
					time.sleep(0.25)
				# end debug block
			except Exception:
				advanced = False
			logger.debug("[page %s] after_ids: %s...%s", page_index, curr_ids[:3], curr_ids[-3:] if len(curr_ids)>3 else curr_ids)
			# Minimal logging about pagination
			if advanced:
				logger.info("[page %s] next page detected", page_index)
			else:
				logger.info("[page %s] no next page; stopping", page_index)
				break

		# Overall summary for the run
		logger.info("[summary][overall] events_processed=%s | succeeded=%s | skipped=%s | rounds_discovered=%s | rows_added=%s | html_saved=%s",
		            overall_events, overall_events_succeeded, overall_events_skipped,
		            overall_rounds_discovered, overall_round_rows_added, overall_html_saved)
		browser.close()


def build_argparser() -> argparse.ArgumentParser:
	p = argparse.ArgumentParser(description="Scrape NVWF tournaments list into DuckDB (tournaments only)")
	p.add_argument("--season", help="Optional season filter like 2024-2025 (currently informational)")
	p.add_argument("--max-tournaments", type=int, default=None, help="Limit number of tournaments to scrape")
	p.add_argument("--show", action="store_true", help="Run browser headed to observe scraping")
	p.add_argument("--resume", action="store_true", help="Reserved for future resume logic")
	p.add_argument(
		"--log-level",
		default="INFO",
		choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
		help="Logging level (default: INFO)",
	)
	return p


def main(argv: Optional[List[str]] = None) -> int:
	parser = build_argparser()
	args = parser.parse_args(argv)
	# Configure root logging once based on CLI
	logging.basicConfig(
		level=getattr(logging, (args.log_level or "INFO").upper(), logging.INFO),
		format="%(asctime)s %(levelname)s %(name)s: %(message)s",
	)
	run_scraper(args)
	return 0


if __name__ == "__main__":
	# If no CLI args are provided, run with sensible defaults (no CLI required)
	if len(sys.argv) <= 1:
		default_args = argparse.Namespace(season=None, max_tournaments=10, show=True, resume=False, log_level="DEBUG")
		logging.basicConfig(
			level=getattr(logging, (default_args.log_level or "INFO").upper(), logging.INFO),
			format="%(asctime)s %(levelname)s %(name)s: %(message)s",
		)
		run_scraper(default_args)
	else:
		raise SystemExit(main())

