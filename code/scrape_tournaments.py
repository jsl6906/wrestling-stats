"""
Scrape TrackWrestling tournaments and save Round Results HTML per round.

Design:
- Use fast HTTP-based discovery to get tournament list (no browser needed)
- For each tournament: use VerifyPassword.jsp to establish session, navigate to Round Results
- Iterate rounds (excluding All Rounds), click Go, save parsed data to DuckDB

Configuration (via .env):
- GOVERNING_BODY_ID: TrackWrestling gbId parameter (e.g., 38 for VHSL)
- GOVERNING_BODY_ACRONYM: Used in database filename (e.g., trackwrestling_vhsl.db)
- GOVERNING_BODY_NAME: Full display name

Notes:
- Tournament discovery uses direct HTTP requests (~200ms for full listing)
- Session establishment uses VerifyPassword.jsp (viewer access, no credentials needed)
- Round scraping uses Playwright (requires JavaScript rendering)
- This script only scrapes; parsing is handled separately in `parse_round_html.py`.

CLI examples (run with uv):
- uv run python -m code.scrape_tournaments --start-date 01/01/2024 --end-date 12/31/2024 --max-tournaments 50
- uv run python -m code.scrape_tournaments --start-date 09/01/2024 --end-date 06/30/2025
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import re
import time
from dataclasses import dataclass
from datetime import date, timedelta
from typing import List, Optional, Tuple

import duckdb
import httpx
from bs4 import BeautifulSoup

# Import from package modules
try:
    from .shared_trackwrestling import ensure_rounds_table, parse_rounds
    from .config import get_db_path, GOVERNING_BODY_ACRONYM, GOVERNING_BODY_ID
except ImportError:
    # Fallback for direct script execution
    from shared_trackwrestling import ensure_rounds_table, parse_rounds
    from config import get_db_path, GOVERNING_BODY_ACRONYM, GOVERNING_BODY_ID


logger = logging.getLogger(__name__)

# ============================================================================
# Constants
# ============================================================================

BASE_URL = "https://www.trackwrestling.com"
GENERIC_SESSION_ID = "zyxwvutsrq"  # Generic session ID for public viewer access

# Map event_type to URL path segment
TOURNAMENT_TYPE_PATHS = {
    1: "predefinedtournaments",
    2: "opentournaments",
    3: "teamtournaments",
    4: "freestyletournaments",
    5: "seasontournaments",
}

# Map event_type to human-readable names
TOURNAMENT_TYPE_NAMES = {
    1: "Predefined Tournament",
    2: "Open Tournament",
    3: "Team Tournament",
    4: "Freestyle Tournament",
    5: "Season Tournament",
}

# Tournaments to exclude from scraping (add event_ids here as needed)
# Example reasons: incomplete data, parsing issues, test events, etc.
EXCLUDED_TOURNAMENT_IDS = [
    # Add event IDs here as strings, e.g.:
    # "12345",  # Reason: incomplete bracket
    # "67890",  # Reason: test tournament
]


def _get_timestamp() -> str:
    """Generate TIM parameter (milliseconds since epoch)."""
    return str(int(time.time() * 1000))


# ============================================================================
# Data Models
# ============================================================================

@dataclass
class Tournament:
    """Tournament information from HTTP discovery."""
    event_id: str
    name: str
    event_type: int  # 1-5, maps to TOURNAMENT_TYPE_PATHS
    start_date: Optional[str] = None  # ISO format YYYY-MM-DD
    end_date: Optional[str] = None
    venue_name: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None

    @property
    def event_type_path(self) -> str:
        """Get URL path segment for this tournament type."""
        return TOURNAMENT_TYPE_PATHS.get(self.event_type, "opentournaments")

    @property
    def event_type_name(self) -> str:
        """Get human-readable name for this tournament type."""
        return TOURNAMENT_TYPE_NAMES.get(self.event_type, "Unknown")


# ============================================================================
# DuckDB Helpers
# ============================================================================

def ensure_db(conn: duckdb.DuckDBPyConnection) -> None:
    """Ensure the tournaments table exists."""
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
            event_type_id INTEGER,
            event_type_name TEXT,
            first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    # Backfill: add event_type columns if missing (for existing databases)
    cols = set(
        r[0]
        for r in conn.execute(
            """--sql
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'tournaments'
            """
        ).fetchall()
    )
    if "event_type_id" not in cols:
        conn.execute("""--sql
        ALTER TABLE tournaments ADD COLUMN event_type_id INTEGER
        """)
    if "event_type_name" not in cols:
        conn.execute("""--sql
        ALTER TABLE tournaments ADD COLUMN event_type_name TEXT
        """)


def upsert_tournament(
    conn: duckdb.DuckDBPyConnection,
    *,
    event_id: str,
    name: Optional[str] = None,
    year: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    venue: Optional[str] = None,
    city: Optional[str] = None,
    state: Optional[str] = None,
    event_type_id: Optional[int] = None,
    event_type_name: Optional[str] = None,
) -> None:
    """Insert or update a tournament record."""
    conn.execute(
        """--sql
        INSERT INTO tournaments (event_id, name, year, start_date, end_date, venue, city, state, event_type_id, event_type_name)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (event_id) DO UPDATE SET
            name = COALESCE(EXCLUDED.name, tournaments.name),
            year = COALESCE(EXCLUDED.year, tournaments.year),
            start_date = COALESCE(EXCLUDED.start_date, tournaments.start_date),
            end_date = COALESCE(EXCLUDED.end_date, tournaments.end_date),
            venue = COALESCE(EXCLUDED.venue, tournaments.venue),
            city = COALESCE(EXCLUDED.city, tournaments.city),
            state = COALESCE(EXCLUDED.state, tournaments.state),
            event_type_id = COALESCE(EXCLUDED.event_type_id, tournaments.event_type_id),
            event_type_name = COALESCE(EXCLUDED.event_type_name, tournaments.event_type_name)
        """,
        [event_id, name, year, start_date, end_date, venue, city, state, event_type_id, event_type_name],
    )


def cleanup_orphaned_tournaments(conn: duckdb.DuckDBPyConnection) -> int:
    """
    Delete tournaments that have no rounds or no matches.
    Returns number of tournaments deleted.
    """
    # Check which tables exist
    existing_tables = set(
        r[0] for r in conn.execute("SHOW TABLES").fetchall()
    )
    has_rounds_table = "tournament_rounds" in existing_tables
    has_matches_table = "matches" in existing_tables

    # Find tournaments with no rounds (only if tournament_rounds table exists)
    no_rounds = []
    if has_rounds_table:
        no_rounds = conn.execute(
            """--sql
            SELECT t.event_id, t.name
            FROM tournaments t
            LEFT JOIN tournament_rounds tr ON t.event_id = tr.event_id
            WHERE tr.event_id IS NULL
            """
        ).fetchall()

    # Find tournaments with no matches (only if matches table exists)
    no_matches = []
    if has_matches_table and has_rounds_table:
        no_matches = conn.execute(
            """--sql
            SELECT DISTINCT t.event_id, t.name
            FROM tournaments t
            LEFT JOIN matches m ON t.event_id = m.event_id
            WHERE m.event_id IS NULL
              AND t.event_id NOT IN (
                  SELECT t2.event_id FROM tournaments t2
                  LEFT JOIN tournament_rounds tr2 ON t2.event_id = tr2.event_id
                  WHERE tr2.event_id IS NULL
              )
            """
        ).fetchall()

    total_deleted = 0

    if no_rounds:
        logger.info("Deleting %d tournaments with no rounds...", len(no_rounds))
        for event_id, name in no_rounds:
            logger.debug("  - %s: %s", event_id, name)
        conn.execute(
            """--sql
            DELETE FROM tournaments
            WHERE event_id IN (
                SELECT t.event_id
                FROM tournaments t
                LEFT JOIN tournament_rounds tr ON t.event_id = tr.event_id
                WHERE tr.event_id IS NULL
            )
            """
        )
        total_deleted += len(no_rounds)

    if no_matches:
        logger.info("Deleting %d tournaments with no matches...", len(no_matches))
        for event_id, name in no_matches:
            logger.debug("  - %s: %s", event_id, name)
        conn.execute(
            """--sql
            DELETE FROM tournaments
            WHERE event_id IN (
                SELECT DISTINCT t.event_id
                FROM tournaments t
                LEFT JOIN matches m ON t.event_id = m.event_id
                WHERE m.event_id IS NULL
            )
            """
        )
        total_deleted += len(no_matches)

    if total_deleted > 0:
        logger.info("Cleanup complete: deleted %d orphaned tournaments", total_deleted)
    else:
        logger.info("No orphaned tournaments found")

    return total_deleted


# ============================================================================
# HTTP Tournament Discovery
# ============================================================================

def _parse_date_range(text: str) -> Tuple[Optional[str], Optional[str]]:
    """Parse date range text to ISO format dates."""
    if not text:
        return None, None

    text = re.sub(r"\s+", " ", text.strip())

    # MM/DD - MM/DD/YYYY
    m = re.match(r"^(\d{1,2}/\d{1,2})\s*-\s*(\d{1,2}/\d{1,2}/(\d{4}))$", text)
    if m:
        year = m.group(3)
        start_parts = m.group(1).split("/")
        end_parts = m.group(2).split("/")
        start = f"{year}-{start_parts[0].zfill(2)}-{start_parts[1].zfill(2)}"
        end = f"{end_parts[2]}-{end_parts[0].zfill(2)}-{end_parts[1].zfill(2)}"
        return start, end

    # MM/DD/YYYY - MM/DD/YYYY
    m = re.match(r"^(\d{1,2}/\d{1,2}/\d{4})\s*-\s*(\d{1,2}/\d{1,2}/\d{4})$", text)
    if m:
        s = m.group(1).split("/")
        e = m.group(2).split("/")
        return (
            f"{s[2]}-{s[0].zfill(2)}-{s[1].zfill(2)}",
            f"{e[2]}-{e[0].zfill(2)}-{e[1].zfill(2)}",
        )

    # Single date MM/DD/YYYY
    m = re.match(r"^(\d{1,2}/\d{1,2}/\d{4})$", text)
    if m:
        parts = m.group(1).split("/")
        iso = f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
        return iso, iso

    return None, None


def _parse_venue(text: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Parse venue text into name, city, state."""
    if not text:
        return None, None, None

    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
    venue_name = lines[0] if lines else None
    city = state = None

    if len(lines) > 1:
        m = re.match(r"^([^,]+),\s*([A-Z]{2})", lines[-1])
        if m:
            city, state = m.group(1).strip(), m.group(2)

    return venue_name, city, state


def _parse_tournament_item(li) -> Optional[Tournament]:
    """Parse a single tournament list item from BeautifulSoup."""
    # Find anchor with eventSelected call
    anchor = li.select_one('a[href*="eventSelected"], a[onclick*="eventSelected"]')
    if not anchor:
        return None

    href = anchor.get("href", "") or anchor.get("onclick", "")

    # Extract: eventSelected(eventId, 'name', eventType, ...)
    match = re.search(r"eventSelected\((\d+),\s*'([^']*)',\s*(\d+)", href)
    if not match:
        return None

    event_id = match.group(1)
    name = match.group(2)
    event_type = int(match.group(3))

    # Parse date
    start_date = end_date = None
    date_span = li.select_one("div:nth-child(2) span:nth-child(2)")
    if date_span:
        start_date, end_date = _parse_date_range(date_span.text.strip())

    # Parse venue
    venue_name = city = state = None
    venue_span = li.select_one("div:nth-child(3) span")
    if venue_span:
        venue_text = venue_span.get_text(separator="\n")
        venue_name, city, state = _parse_venue(venue_text)

    return Tournament(
        event_id=event_id,
        name=name,
        event_type=event_type,
        start_date=start_date,
        end_date=end_date,
        venue_name=venue_name,
        city=city,
        state=state,
    )


def _parse_tournament_list(html: str) -> List[Tournament]:
    """Parse tournament list from HTML response."""
    soup = BeautifulSoup(html, "html.parser")
    tournaments = []

    for li in soup.select(".tournament-ul > li"):
        try:
            tournament = _parse_tournament_item(li)
            if tournament:
                tournaments.append(tournament)
        except Exception as e:
            logger.debug("Error parsing tournament item: %s", e)
            continue

    return tournaments


def _parse_pagination_info(html: str) -> Tuple[int, int, int]:
    """
    Parse pagination info from HTML response.
    
    Looks for pattern like "1 - 30 of 160" in dataGridNextPrev div.
    
    Returns:
        Tuple of (start_index, end_index, total_count)
        Returns (0, 0, 0) if no pagination info found.
    """
    soup = BeautifulSoup(html, "html.parser")
    
    # Look for the pagination div
    pagination_div = soup.select_one(".dataGridNextPrev")
    if not pagination_div:
        return (0, 0, 0)
    
    # Find the span with "X - Y of Z" pattern
    for span in pagination_div.find_all("span"):
        text = span.get_text(strip=True)
        # Match pattern like "1 - 30 of 160"
        match = re.match(r"(\d+)\s*-\s*(\d+)\s+of\s+(\d+)", text)
        if match:
            start_idx = int(match.group(1))
            end_idx = int(match.group(2))
            total = int(match.group(3))
            return (start_idx, end_idx, total)
    
    return (0, 0, 0)


async def discover_tournaments_async(
    start_date: str,
    end_date: str,
    governing_body_id: int = GOVERNING_BODY_ID,
) -> List[Tournament]:
    """
    Discover tournaments using fast HTTP requests with pagination support.

    Args:
        start_date: Start date in MM/DD/YYYY format
        end_date: End date in MM/DD/YYYY format
        governing_body_id: TrackWrestling governing body ID

    Returns:
        List of Tournament objects
    """
    url = f"{BASE_URL}/Login.jsp"
    all_tournaments: List[Tournament] = []
    page_index = 0  # TrackWrestling uses 0-based page index
    # TrackWrestling returns ~30 results per page

    async with httpx.AsyncClient(
        timeout=30.0,
        follow_redirects=True,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    ) as client:
        while True:
            params = {
                "TIM": _get_timestamp(),
                "twSessionId": GENERIC_SESSION_ID,
                "gbId": str(governing_body_id),
                "sDate": start_date,
                "eDate": end_date,
                "tournamentIndex": str(page_index),
                "tName": "",
                "state": "",
                "lastName": "",
                "firstName": "",
                "teamName": "",
                "sfvString": "",
                "city": "",
                "camps": "false",
            }

            try:
                response = await client.get(url, params=params)
                response.raise_for_status()
                
                # Parse tournaments from this page
                page_tournaments = _parse_tournament_list(response.text)
                
                # Parse pagination info
                start_idx, end_idx, total_count = _parse_pagination_info(response.text)
                
                if page_tournaments:
                    all_tournaments.extend(page_tournaments)
                    logger.debug(
                        "Page %d: found %d tournaments (showing %d-%d of %d)",
                        page_index, len(page_tournaments), start_idx, end_idx, total_count
                    )
                
                # Check if there are more pages
                # If no pagination info found, or we've reached the last page, stop
                if total_count == 0 or end_idx >= total_count or not page_tournaments:
                    break
                
                # Move to next page
                page_index += 1
                
                # Safety limit to prevent infinite loops
                if page_index > 100:
                    logger.warning("Reached page limit (100), stopping pagination")
                    break
                    
            except httpx.HTTPStatusError as e:
                logger.error("HTTP error discovering tournaments (page %d): %s", page_index, e)
                break
            except Exception as e:
                logger.error("Error discovering tournaments (page %d): %s", page_index, e)
                break

    # Deduplicate by event_id (in case of any overlap)
    seen_ids = set()
    unique_tournaments = []
    for t in all_tournaments:
        if t.event_id not in seen_ids:
            seen_ids.add(t.event_id)
            unique_tournaments.append(t)

    logger.info(
        "Discovered %d tournaments across %d pages (gbId=%s, dates=%s to %s)",
        len(unique_tournaments), page_index + 1, governing_body_id, start_date, end_date
    )
    return unique_tournaments


def discover_tournaments(start_date: str, end_date: str) -> List[Tournament]:
    """Synchronous wrapper for tournament discovery."""
    return asyncio.run(discover_tournaments_async(start_date, end_date))


# ============================================================================
# URL Building
# ============================================================================

def build_session_urls(event_id: str, event_type: int) -> Tuple[str, str]:
    """
    Build URLs to access a tournament's Round Results page.

    Returns:
        Tuple of (verify_password_url, round_results_url)

    The VerifyPassword.jsp call establishes the tournament session (viewer access),
    then RoundResults.jsp can be accessed directly.
    """
    type_path = TOURNAMENT_TYPE_PATHS.get(event_type, "opentournaments")
    timestamp = _get_timestamp()

    # VerifyPassword.jsp establishes tournament session (viewer login, no credentials)
    verify_url = (
        f"{BASE_URL}/{type_path}/VerifyPassword.jsp"
        f"?TIM={timestamp}&twSessionId={GENERIC_SESSION_ID}&tournamentId={event_id}"
        f"&userType=viewer&userName=&password="
    )

    # RoundResults page
    round_results_url = (
        f"{BASE_URL}/{type_path}/RoundResults.jsp"
        f"?TIM={timestamp}&twSessionId={GENERIC_SESSION_ID}&tournamentId={event_id}"
        f"&displayFormatBox=1"
    )

    return verify_url, round_results_url


def parse_dual_meet_charts(page) -> List[Tuple[str, str, str]]:
    """
    Parse dual meet chart/pool selection links from the page.
    
    Some dual meets have an intermediate page where you select a chart/pool
    before getting to the bout selector.
    
    Returns list of (chart_id, chart_name, chart_href) tuples, e.g.:
    [("265342132", "Getem Services Black & Blue", "DualMeetWizard.jsp?..."), ...]
    """
    import re
    
    charts: List[Tuple[str, str, str]] = []
    
    # Look for ul.top-links with DualMeetWizard.jsp links
    for fr in [page] + list(page.frames):
        try:
            # Check for the chart selection page pattern
            chart_links = fr.locator('ul.top-links a[href*="DualMeetWizard.jsp"]')
            count = chart_links.count()
            
            if count > 0:
                logger.debug("Found %d chart links in frame", count)
                for i in range(count):
                    try:
                        link = chart_links.nth(i)
                        href = link.get_attribute("href") or ""
                        name = (link.inner_text() or "").strip()
                        
                        # Extract chartId from href
                        chart_id_match = re.search(r'chartId=(\d+)', href)
                        chart_id = chart_id_match.group(1) if chart_id_match else f"chart_{i}"
                        
                        if name:
                            charts.append((chart_id, name, href))
                    except Exception:
                        continue
                
                if charts:
                    return charts
        except Exception:
            continue
    
    return charts


def parse_dual_meet_bouts(page) -> List[Tuple[str, str]]:
    """
    Parse dual meet bout options from the boutNumberBox selector.
    
    Returns list of (bout_value, bout_label) tuples, e.g.:
    [("N.1", "1.  Woodgrove vs Dominion"), ("N.2", "2.  McLean vs Briar Woods"), ...]
    """
    from playwright.sync_api import TimeoutError as PWTimeout

    def _extract_from_select(sel_loc) -> List[Tuple[str, str]]:
        options = sel_loc.locator("option[value]")
        cnt = options.count()
        out: List[Tuple[str, str]] = []
        for i in range(cnt):
            value = options.nth(i).get_attribute("value") or ""
            if not value or value == "":
                continue
            label = (options.nth(i).inner_text() or "").strip()
            # Skip placeholder options
            if "select" in label.lower():
                continue
            out.append((value, label))
        return out

    # Check frames first (most common location for dual meets)
    for fr in page.frames:
        try:
            sel = fr.locator("select#boutNumberBox")
            # Quick check if it exists before waiting
            if sel.count() > 0:
                bouts = _extract_from_select(sel)
                logger.debug("boutNumberBox found in frame; options=%s", len(bouts))
                return bouts
        except Exception:
            continue

    # Try on the main page (less common, but check anyway)
    try:
        select = page.locator("select#boutNumberBox")
        # Quick check first
        if select.count() > 0:
            bouts = _extract_from_select(select)
            logger.debug("boutNumberBox found on page; options=%s", len(bouts))
            return bouts
    except Exception:
        pass

    # If not found immediately, wait a bit and try again (page might still be loading)
    try:
        select = page.locator("select#boutNumberBox")
        select.wait_for(timeout=2000)
        bouts = _extract_from_select(select)
        logger.debug("boutNumberBox found on page after wait; options=%s", len(bouts))
        return bouts
    except PWTimeout:
        pass
    except Exception:
        pass

    # Final attempt: check frames with wait
    for fr in page.frames:
        try:
            sel = fr.locator("select#boutNumberBox")
            sel.wait_for(timeout=1500)
            bouts = _extract_from_select(sel)
            logger.debug("boutNumberBox found in frame after wait; options=%s", len(bouts))
            return bouts
        except Exception:
            continue

    return []


# ============================================================================
# Utility Helpers
# ============================================================================

def event_year_from_name(name: str) -> Optional[int]:
    """Extract year from tournament name."""
    m = re.search(r"(20\d{2})", name)
    return int(m.group(1)) if m else None


# ============================================================================
# Main Scraper
# ============================================================================

def run_scraper(args: argparse.Namespace) -> None:
    """
    Main scraper function using HTTP discovery + Playwright for rounds.

    1. Discover tournaments via fast HTTP requests
    2. Filter to eligible events (past events without complete rounds)
    3. For each event: establish session via VerifyPassword.jsp, scrape rounds
    """
    from datetime import datetime
    from playwright.sync_api import sync_playwright

    start_time = time.time()

    # 1. Discover tournaments via HTTP
    logger.info("=" * 80)
    logger.info("Starting tournament discovery...")
    logger.info("  Date range: %s to %s", args.start_date, args.end_date)
    logger.info("=" * 80)

    discovered = discover_tournaments(args.start_date, args.end_date)
    discovery_time = time.time() - start_time
    logger.info("Discovered %d tournaments in %.2fs", len(discovered), discovery_time)

    if not discovered:
        logger.warning("No tournaments found for date range")
        return

    # 2. Open DuckDB and ensure tables exist
    db_path = get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db = duckdb.connect(str(db_path))
    ensure_db(db)
    ensure_rounds_table(db)

    # Cleanup orphaned tournaments
    logger.info("=" * 80)
    logger.info("Pre-scrape cleanup: removing orphaned tournaments")
    logger.info("=" * 80)
    cleanup_orphaned_tournaments(db)

    # Upsert all discovered tournaments
    for t in discovered:
        year = event_year_from_name(t.name) if t.name else None
        if year is None and t.start_date:
            try:
                year = int(t.start_date.split("-")[0])
            except (ValueError, IndexError):
                pass

        upsert_tournament(
            db,
            event_id=t.event_id,
            name=t.name,
            year=year,
            start_date=t.start_date,
            end_date=t.end_date,
            venue=t.venue_name,
            city=t.city,
            state=t.state,
            event_type_id=t.event_type,
            event_type_name=t.event_type_name,
        )

    logger.info("Upserted %d tournament records", len(discovered))

    # 3. Determine which tournaments need round scraping
    today = date.today()
    eligible_events: List[Tournament] = []

    for t in discovered:
        # Skip excluded tournaments
        if t.event_id in EXCLUDED_TOURNAMENT_IDS:
            logger.debug("Skipping excluded event %s (%s)", t.event_id, t.name)
            continue
        
        # Skip future events
        if t.start_date:
            try:
                event_start = datetime.strptime(t.start_date, "%Y-%m-%d").date()
                if event_start > today:
                    logger.debug("Skipping future event %s (%s) - starts %s",
                                t.event_id, t.name, t.start_date)
                    continue
            except ValueError:
                pass

        # Check if we already have rounds for this event
        existing = db.execute(
            """--sql
            SELECT COUNT(*) FROM tournament_rounds WHERE event_id = ?
            """,
            [t.event_id],
        ).fetchone()

        if existing and existing[0] > 0:
            logger.debug("Skipping event %s - already has %d rounds", t.event_id, existing[0])
            continue

        eligible_events.append(t)

    logger.info("=" * 80)
    logger.info("%d tournaments eligible for round scraping", len(eligible_events))
    logger.info("=" * 80)

    if args.max_tournaments:
        eligible_events = eligible_events[: args.max_tournaments]
        logger.info("Limited to %d tournaments (--max-tournaments)", len(eligible_events))

    if not eligible_events:
        logger.info("No tournaments need round scraping")
        db.close()
        return

    # 4. Scrape rounds using Playwright
    overall_events = 0
    overall_succeeded = 0
    overall_skipped = 0

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=not args.show)
        page = browser.new_page()

        for t in eligible_events:
            overall_events += 1
            logger.info(
                "[event %d/%d] Processing %s: %s (type=%d)",
                overall_events, len(eligible_events), t.event_id, t.name, t.event_type
            )

            try:
                # Reset page state between tournaments to prevent navigation conflicts
                try:
                    page.goto("about:blank", wait_until="domcontentloaded", timeout=5000)
                except Exception:
                    pass

                # Build URLs for session establishment
                verify_url, round_results_url = build_session_urls(t.event_id, t.event_type)

                # Step 1: Establish session via VerifyPassword.jsp
                logger.debug("Establishing session: %s", verify_url)
                page.goto(verify_url, wait_until="load", timeout=20000)
                # Wait for any redirects to settle
                try:
                    page.wait_for_load_state("networkidle", timeout=5000)
                except Exception:
                    pass

                # Step 2: Navigate to RoundResults
                logger.debug("Loading round results: %s", round_results_url)
                page.goto(round_results_url, wait_until="load", timeout=15000)
                # Wait for any redirects to settle (team tournaments redirect to MainFrame.jsp)
                try:
                    page.wait_for_load_state("networkidle", timeout=5000)
                except Exception:
                    pass

                # Check for round selector (standard tournaments)
                round_selector_found = False
                is_dual_meet = False
                for fr in [page] + list(page.frames):
                    try:
                        if fr.locator("select#roundIdBox").count() > 0:
                            round_selector_found = True
                            break
                    except Exception:
                        continue

                # Try alternative tournament types if needed
                if not round_selector_found:
                    logger.debug("Round selector not found, trying alternative types...")
                    for alt_type, alt_path in TOURNAMENT_TYPE_PATHS.items():
                        if alt_type == t.event_type:
                            continue

                        alt_verify = (
                            f"{BASE_URL}/{alt_path}/VerifyPassword.jsp"
                            f"?TIM={_get_timestamp()}&twSessionId={GENERIC_SESSION_ID}"
                            f"&tournamentId={t.event_id}&userType=viewer&userName=&password="
                        )
                        alt_results = (
                            f"{BASE_URL}/{alt_path}/RoundResults.jsp"
                            f"?TIM={_get_timestamp()}&twSessionId={GENERIC_SESSION_ID}"
                            f"&tournamentId={t.event_id}&displayFormatBox=1"
                        )

                        try:
                            page.goto(alt_verify, wait_until="load", timeout=10000)
                            try:
                                page.wait_for_load_state("networkidle", timeout=3000)
                            except Exception:
                                pass
                            page.goto(alt_results, wait_until="load", timeout=10000)
                            try:
                                page.wait_for_load_state("networkidle", timeout=3000)
                            except Exception:
                                pass

                            for fr in [page] + list(page.frames):
                                try:
                                    if fr.locator("select#roundIdBox").count() > 0:
                                        round_selector_found = True
                                        round_results_url = alt_results
                                        logger.info("Found round selector with path: %s", alt_path)
                                        break
                                except Exception:
                                    continue

                            if round_selector_found:
                                break
                        except Exception as e:
                            logger.debug("Failed with %s: %s", alt_path, e)
                            continue

                # If still no round selector, try Dual Meet Results (for team tournaments)
                # Team tournaments use a different UI - we need to click navigation links
                if not round_selector_found:
                    logger.debug("No round selector found, trying to find Dual Meet navigation...")
                    
                    # First, make sure we're in the tournament context
                    # Navigate to the main tournament page first
                    type_path = TOURNAMENT_TYPE_PATHS.get(t.event_type, "teamtournaments")
                    main_url = (
                        f"{BASE_URL}/{type_path}/MainFrame.jsp"
                        f"?TIM={_get_timestamp()}&twSessionId={GENERIC_SESSION_ID}"
                        f"&tournamentId={t.event_id}"
                    )
                    try:
                        page.goto(main_url, wait_until="load", timeout=15000)
                        try:
                            page.wait_for_load_state("networkidle", timeout=5000)
                        except Exception:
                            pass
                    except Exception as e:
                        logger.debug("Failed to load main frame: %s", e)
                    
                    # Look for "Results" menu first, then "Dual Meets" or similar links
                    dual_link_found = False
                    for fr in [page] + list(page.frames):
                        try:
                            # First try clicking Results menu/link
                            results_link = fr.locator('a:has-text("Results")').first
                            if results_link.count() > 0:
                                logger.debug("Clicking Results link")
                                results_link.click(timeout=3000)
                                time.sleep(0.2)
                        except Exception:
                            pass
                        
                        # Look for dual meet navigation links
                        for link_text in ['Dual Meets', 'Dual Meet', 'Match Results', 'Duals']:
                            try:
                                link = fr.locator(f'a:has-text("{link_text}")').first
                                if link.count() > 0 and link.is_visible():
                                    logger.debug("Found dual meet link: %s", link_text)
                                    link.click(timeout=5000)
                                    try:
                                        page.wait_for_load_state("networkidle", timeout=5000)
                                    except Exception:
                                        pass
                                    dual_link_found = True
                                    break
                            except Exception as e:
                                logger.debug("Error with link '%s': %s", link_text, str(e)[:50])
                                continue
                        if dual_link_found:
                            break

                    # Check for bout selector in all frames
                    for fr in [page] + list(page.frames):
                        try:
                            if fr.locator("select#boutNumberBox").count() > 0:
                                is_dual_meet = True
                                logger.info("Found dual meet bout selector for %s", t.event_id)
                                break
                        except Exception:
                            continue
                    
                    # If no bout selector, check for chart selection page
                    if not is_dual_meet:
                        charts = parse_dual_meet_charts(page)
                        if charts:
                            is_dual_meet = True
                            logger.info("Found dual meet chart selection page with %d charts for %s", 
                                       len(charts), t.event_id)

                if not round_selector_found and not is_dual_meet:
                    logger.warning("[event] %s | no round/bout selector found", t.event_id)
                    overall_skipped += 1
                    continue

                # Handle dual meet tournaments (boutNumberBox)
                if is_dual_meet:
                    # Check for chart selection page first
                    charts = parse_dual_meet_charts(page)
                    
                    # Store the starting URL for potential re-navigation
                    starting_dual_url = page.url
                    logger.debug("Starting dual meet URL: %s", starting_dual_url)
                    
                    # If there are charts, iterate through each one
                    # If no charts, treat as a single "default" chart
                    chart_list = charts if charts else [("default", "Default", "")]
                    
                    saved_count = 0
                    for chart_id, chart_name, chart_href in chart_list:
                        try:
                            # If there's a chart href, we need to click/navigate to it
                            if chart_href:
                                logger.info("Navigating to chart: %s (%s)", chart_name, chart_id)
                                
                                # Find and click the chart link
                                chart_link_clicked = False
                                for fr in [page] + list(page.frames):
                                    try:
                                        link = fr.locator(f'a[href*="chartId={chart_id}"]').first
                                        if link.count() > 0:
                                            link.click(timeout=5000)
                                            try:
                                                page.wait_for_load_state("networkidle", timeout=3000)
                                            except Exception:
                                                pass
                                            chart_link_clicked = True
                                            break
                                    except Exception as e:
                                        logger.debug("Error clicking chart link in frame: %s", e)
                                        continue
                                
                                if not chart_link_clicked:
                                    logger.warning("Could not click chart link for %s", chart_name)
                                    continue
                            
                            # Now look for bouts within this chart
                            bouts = parse_dual_meet_bouts(page)
                            if not bouts:
                                logger.debug("No bouts found for chart %s", chart_name)
                                # If using charts, go back to chart selection for next chart
                                if charts:
                                    page.goto(starting_dual_url, wait_until="load", timeout=15000)
                                    try:
                                        page.wait_for_load_state("networkidle", timeout=3000)
                                    except Exception:
                                        pass
                                    time.sleep(1.0)
                                continue
                            
                            # Store the current chart URL for re-navigation between bouts
                            current_chart_url = page.url
                            logger.debug("Chart URL for re-navigation: %s", current_chart_url)
                            
                            for bout_id, bout_label in bouts:
                                try:
                                    # Create unique round_id including chart context
                                    if charts:
                                        unique_round_id = f"chart_{chart_id}_{bout_id}"
                                        full_label = f"{chart_name} - {bout_label}"
                                    else:
                                        unique_round_id = bout_id
                                        full_label = bout_label
                                    
                                    # Find bout selector frame
                                    bout_frame = None
                                    for fr in [page] + list(page.frames):
                                        try:
                                            if fr.locator("select#boutNumberBox").count() > 0:
                                                bout_frame = fr
                                                break
                                        except Exception:
                                            continue

                                    if not bout_frame:
                                        # Try re-navigating to chart page
                                        if current_chart_url:
                                            page.goto(current_chart_url, wait_until="load", timeout=15000)
                                            try:
                                                page.wait_for_load_state("networkidle", timeout=3000)
                                            except Exception:
                                                pass
                                            # Re-find frame
                                            for fr in [page] + list(page.frames):
                                                try:
                                                    if fr.locator("select#boutNumberBox").count() > 0:
                                                        bout_frame = fr
                                                        break
                                                except Exception:
                                                    continue
                                        
                                    if not bout_frame:
                                        logger.debug("Could not find bout frame for %s", bout_id)
                                        continue

                                    # Select bout
                                    bout_frame.locator("select#boutNumberBox").select_option(value=bout_id)
                                    time.sleep(0.2)

                                    # Wait for the PageFrame iframe to appear and load its content
                                    # The iframe starts with src="" and gets populated dynamically
                                    raw_html = None
                                    try:
                                        # First, wait for network to be idle after selection
                                        try:
                                            page.wait_for_load_state("networkidle", timeout=3000)
                                        except Exception:
                                            pass
                                        
                                        # Wait for iframe#PageFrame to exist and have a non-empty src
                                        detail_frame = None
                                        max_attempts = 10
                                        
                                        for attempt in range(max_attempts):
                                            # Look for iframe with id="PageFrame" or name="PageFrame"
                                            for fr in page.frames:
                                                try:
                                                    # Check if this frame has the right name/id
                                                    if hasattr(fr, 'name') and fr.name == 'PageFrame':
                                                        frame_url = fr.url
                                                        # Make sure it has actual content (not empty src)
                                                        if frame_url and frame_url != 'about:blank' and 'DualMeetDetail.jsp' in frame_url:
                                                            detail_frame = fr
                                                            logger.debug("Found PageFrame with content: %s", frame_url[:100])
                                                            break
                                                except Exception:
                                                    continue
                                            
                                            # Also check by looking for frames with DualMeetDetail.jsp
                                            if not detail_frame:
                                                for fr in page.frames:
                                                    try:
                                                        frame_url = fr.url
                                                        if 'DualMeetDetail.jsp' in frame_url:
                                                            detail_frame = fr
                                                            logger.debug("Found DualMeetDetail frame: %s", frame_url[:100])
                                                            break
                                                    except Exception:
                                                        continue
                                            
                                            if detail_frame:
                                                break
                                            
                                            # Wait shorter, more frequent checks
                                            if attempt < max_attempts - 1:
                                                wait_time = 0.2 + (attempt * 0.1)
                                                logger.debug("PageFrame not ready on attempt %d, waiting %.1fs...", attempt + 1, wait_time)
                                                time.sleep(wait_time)
                                        
                                        if detail_frame:
                                            # Wait for the frame's content to be ready
                                            try:
                                                # Wait for table or section with results
                                                detail_frame.wait_for_selector("table, section.tw-list", timeout=5000)
                                            except Exception as e:
                                                logger.debug("Timeout waiting for content in detail frame: %s", e)
                                            
                                            # Try to get just the relevant table/section, not the whole page
                                            # Look for section.tw-list first (match results section)
                                            section_loc = detail_frame.locator("section.tw-list").first
                                            if section_loc.count() > 0:
                                                raw_html = section_loc.evaluate("el => el.outerHTML")
                                                logger.debug("Captured section.tw-list HTML (%d chars)", len(raw_html))
                                            else:
                                                # Try table with class tw-table
                                                table_loc = detail_frame.locator("table.tw-table").first
                                                if table_loc.count() > 0:
                                                    raw_html = table_loc.evaluate("el => el.outerHTML")
                                                    logger.debug("Captured table.tw-table HTML (%d chars)", len(raw_html))
                                                else:
                                                    # Fallback: get any table with content
                                                    tables = detail_frame.locator("table[width]")
                                                    if tables.count() > 0:
                                                        # Find the largest table (likely has the data)
                                                        for i in range(tables.count()):
                                                            try:
                                                                html = tables.nth(i).evaluate("el => el.outerHTML")
                                                                if len(html) > 500:  # Has substantial content
                                                                    raw_html = html
                                                                    logger.debug("Captured table[width] HTML (%d chars)", len(raw_html))
                                                                    break
                                                            except Exception:
                                                                continue
                                            
                                            # Final fallback: get entire frame content
                                            if not raw_html or len(raw_html) < 100:
                                                raw_html = detail_frame.content()
                                                logger.debug("Captured full frame content as fallback (%d chars)", len(raw_html))
                                        else:
                                            logger.warning("PageFrame not found after %d attempts for bout %s", max_attempts, bout_id)
                                    except Exception as e:
                                        logger.debug("Error capturing detail HTML for bout %s: %s", bout_id, e)
                                        raw_html = None

                                    if raw_html:
                                        db.execute(
                                            """--sql
                                            INSERT INTO tournament_rounds (event_id, round_id, label, raw_html)
                                            VALUES (?, ?, ?, ?)
                                            ON CONFLICT (event_id, round_id) DO UPDATE SET
                                                label = EXCLUDED.label,
                                                raw_html = EXCLUDED.raw_html
                                            """,
                                            [t.event_id, unique_round_id, full_label, raw_html],
                                        )
                                        saved_count += 1
                                        logger.debug("Saved dual meet bout %s: %s", unique_round_id, full_label)

                                except Exception as e:
                                    logger.debug("Error saving bout %s: %s", bout_id, e)
                                    continue
                            
                            # If using charts, go back to chart selection for next chart
                            if charts:
                                page.goto(starting_dual_url, wait_until="load", timeout=15000)
                                try:
                                    page.wait_for_load_state("networkidle", timeout=3000)
                                except Exception:
                                    pass
                                
                        except Exception as e:
                            logger.debug("Error processing chart %s: %s", chart_name, e)
                            continue

                    if saved_count > 0:
                        overall_succeeded += 1
                        logger.info(
                            "[event] %s | %s | succeeded (saved %d bouts)",
                            t.event_id, t.name, saved_count
                        )
                    else:
                        overall_skipped += 1
                        logger.warning("[event] %s | %s | no bouts saved", t.event_id, t.name)
                    continue  # Move to next tournament

                # Parse rounds from selector (standard tournament flow)
                rounds = parse_rounds(page)
                if not rounds:
                    logger.warning("[event] %s | no rounds found", t.event_id)
                    overall_skipped += 1
                    continue

                # Scrape each round
                saved_count = 0
                for rid, label in rounds:
                    # Skip "All Rounds" aggregate
                    if (label or "").strip().lower() == "all rounds" or rid in (None, "", "0"):
                        continue

                    try:
                        # Re-navigate for each round to maintain page state
                        page.goto(round_results_url, wait_until="load", timeout=15000)
                        try:
                            page.wait_for_load_state("networkidle", timeout=3000)
                        except Exception:
                            pass

                        # Find round selector frame
                        rounds_frame = None
                        for fr in [page] + list(page.frames):
                            try:
                                if fr.locator("select#roundIdBox").count() > 0:
                                    rounds_frame = fr
                                    break
                            except Exception:
                                continue

                        if not rounds_frame:
                            continue

                        # Select round and click Go
                        rounds_frame.locator("select#roundIdBox").select_option(value=rid)
                        time.sleep(0.1)

                        go_btn = rounds_frame.locator(
                            'input[type="button"][value="Go"][onclick*="viewSchedule"], '
                            'input[type="button"][value="Go"]'
                        ).first
                        if go_btn.count() > 0:
                            go_btn.click()
                            # Wait for content to load
                            try:
                                page.wait_for_load_state("networkidle", timeout=2000)
                            except Exception:
                                pass

                        # Get HTML content
                        raw_html = None
                        for fr in [page] + list(page.frames):
                            try:
                                results_loc = fr.locator(
                                    "#resultsTable, #bracketsTable, #results, "
                                    "div.results, table.results"
                                ).first
                                if results_loc.count() > 0:
                                    raw_html = results_loc.inner_html()
                                    break
                            except Exception:
                                continue

                        if not raw_html:
                            raw_html = page.content()

                        # Save to database
                        db.execute(
                            """--sql
                            INSERT INTO tournament_rounds (event_id, round_id, label, raw_html)
                            VALUES (?, ?, ?, ?)
                            ON CONFLICT (event_id, round_id) DO UPDATE SET
                                label = EXCLUDED.label,
                                raw_html = EXCLUDED.raw_html
                            """,
                            [t.event_id, rid, label, raw_html],
                        )
                        saved_count += 1
                        logger.debug("Saved round %s: %s", rid, label)

                    except Exception as e:
                        logger.debug("Error saving round %s: %s", rid, e)
                        continue

                if saved_count > 0:
                    overall_succeeded += 1
                    logger.info(
                        "[event] %s | %s | succeeded (saved %d rounds)",
                        t.event_id, t.name, saved_count
                    )
                else:
                    overall_skipped += 1
                    logger.warning("[event] %s | %s | no rounds saved", t.event_id, t.name)

            except Exception as e:
                overall_skipped += 1
                logger.error("[event] %s | %s | error: %s", t.event_id, t.name, e)

        browser.close()

    db.close()

    elapsed = time.time() - start_time
    logger.info("=" * 80)
    logger.info(
        "[summary] Completed in %.2fs | events=%d | succeeded=%d | skipped=%d",
        elapsed, overall_events, overall_succeeded, overall_skipped
    )
    logger.info("=" * 80)


# ============================================================================
# CLI
# ============================================================================

def build_argparser() -> argparse.ArgumentParser:
    """Build command-line argument parser."""
    yesterday = date.today() - timedelta(days=1)
    one_year_ago = yesterday - timedelta(days=365)
    default_start = one_year_ago.strftime("%m/%d/%Y")
    default_end = yesterday.strftime("%m/%d/%Y")

    p = argparse.ArgumentParser(
        description=f"Scrape {GOVERNING_BODY_ACRONYM} tournaments from TrackWrestling"
    )
    p.add_argument(
        "--start-date",
        default=default_start,
        help=f"Start date in MM/DD/YYYY format (default: {default_start})",
    )
    p.add_argument(
        "--end-date",
        default=default_end,
        help=f"End date in MM/DD/YYYY format (default: {default_end})",
    )
    p.add_argument(
        "--max-tournaments",
        type=int,
        default=None,
        help="Limit number of tournaments to scrape",
    )
    p.add_argument(
        "--show",
        action="store_true",
        help="Run browser headed to observe scraping",
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level (default: INFO)",
    )
    return p


def main(argv: Optional[List[str]] = None) -> int:
    """Main entry point."""
    parser = build_argparser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    run_scraper(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

