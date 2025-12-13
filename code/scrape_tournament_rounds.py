"""
Scrape rounds for tournaments that do not yet have any rounds recorded,
select each round on the Round Results page, click Go (viewSchedule),
and store the resulting page's HTML into tournament_rounds.raw_html.

Usage:
- Direct run (no CLI): uv run python code/scrape_tournament_rounds.py
- Optional flags: --show, --log-level
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

# Add project root to Python path so we can import from code package
sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb
from code.shared_trackwrestling import (
    BASE_SEARCH_URL,
    ensure_rounds_table,
    open_event_by_id,
    enter_event,
    goto_round_results,
    parse_rounds,
    upsert_round,
)

BASE_SEARCH_URL = BASE_SEARCH_URL


@dataclass
class TournamentRow:
    event_id: str
    name: str


def ensure_db(conn: duckdb.DuckDBPyConnection) -> None:
    # Ensure base tournaments table (schema compatible with scrape_tournaments)
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
    # Ensure tournament_rounds table (shared helper will also add raw_html column if missing)
    conn.execute(
        """--sql
        CREATE TABLE IF NOT EXISTS tournament_rounds (
            event_id TEXT,
            round_id TEXT,
            label TEXT,
            first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (event_id, round_id)
        );
        """
    )


def fetch_events_without_rounds(conn: duckdb.DuckDBPyConnection) -> List[TournamentRow]:
    rows = conn.execute(
        """--sql
        SELECT t.event_id, COALESCE(t.name, '') AS name
        FROM tournaments t
        LEFT JOIN tournament_rounds r
        ON t.event_id = r.event_id
        GROUP BY t.event_id, t.name
        HAVING COUNT(r.round_id) = 0
        ORDER BY t.event_id
        """
    ).fetchall()
    return [TournamentRow(event_id=r[0], name=r[1]) for r in rows]


# Optional utility retained (not currently used here)

def extract_event_id(js_call: str) -> Optional[str]:
    m = re.search(r"eventSelected\((\d+),", js_call)
    return m.group(1) if m else None


def run_scraper(args: argparse.Namespace) -> None:
    from playwright.sync_api import sync_playwright

    logger = logging.getLogger(__name__)

    # DB
    db_path = Path("output") / "trackwrestling.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    ensure_db(conn)

    # Select tournaments missing rounds
    to_process = fetch_events_without_rounds(conn)
    if not to_process:
        logger.info("No tournaments without rounds found.")
        return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not args.show)
        context = browser.new_context()
        page = context.new_page()
        page.goto(BASE_SEARCH_URL, wait_until="domcontentloaded")

        from code.shared_trackwrestling import (
            wait_for_tournament_frame,
            return_to_list,
        )

        # Helper: pick up a snapshot of the current results HTML
        def get_results_snapshot() -> str:
            try:
                # Prefer known containers inside frames first
                for fr in page.frames:
                    try:
                        loc = fr.locator("#resultsTable, #bracketsTable, #results, div.results, table.results").first
                        if loc.count() > 0:
                            return loc.inner_html()
                    except Exception:
                        continue
                # Fallback to page content
                return page.content()
            except Exception:
                return ""

        for row in to_process:
            event_id = row.event_id
            name = row.name

            list_frame = wait_for_tournament_frame(page)
            if list_frame is None:
                logger.warning("[skip] tournaments list not available for event %s", event_id)
                continue

            # Open event
            if not open_event_by_id(page, event_id):
                logger.warning("[warn] could not locate event %s in list; skipping", event_id)
                continue

            # Enter Event
            if not enter_event(page):
                logger.warning("[warn] Enter Event not clickable for %s", event_id)
                return_to_list(page)
                continue

            # Navigate to Round Results
            if not goto_round_results(page):
                logger.warning("[warn] could not open Round Results for %s", event_id)
                return_to_list(page)
                continue

            # Ensure schema and discover rounds
            ensure_rounds_table(conn)
            rounds = parse_rounds(page)
            logger.info("%s %s: found %s rounds", event_id, name, len(rounds))

            # Iterate each round: select option, click Go/viewSchedule, capture HTML
            for rid, label in rounds:
                try:
                    # Skip aggregate options
                    if label.strip().lower() == "all rounds" or rid in ("", "0"):
                        continue

                    # Find the frame containing the round selector and Go button
                    rounds_frame = None
                    select_loc = None
                    btn_go = None
                    for fr in [page] + list(page.frames):
                        try:
                            sel = fr.locator("select#roundIdBox")
                            if sel.count() > 0:
                                rounds_frame = fr
                                select_loc = sel
                                # Prefer the exact Go button with viewSchedule()
                                btn_go = fr.locator(
                                    'input[type="button"][value="Go"][onclick*="viewSchedule"], '
                                    'input[type="button"][value="Go"], '
                                    'button:has-text("Go")'
                                ).first
                                break
                        except Exception:
                            continue
                    if rounds_frame is None or select_loc is None:
                        logger.warning("could not find rounds selector for %s; skipping round %s", event_id, label)
                        continue

                    before_html = get_results_snapshot()

                    # Select the round value
                    try:
                        select_loc.select_option(value=rid)
                        logger.debug("selected round %s in event %s", label, event_id)
                    except Exception:
                        logger.warning("failed to select round %s for %s", label, event_id)
                        continue

                    # Click Go or evaluate viewSchedule()
                    triggered = False
                    try:
                        if btn_go and btn_go.count() > 0:
                            btn_go.click()
                            triggered = True
                            logger.debug("clicked Go for %s - %s", event_id, label)
                    except Exception:
                        pass
                    if not triggered:
                        try:
                            rounds_frame.evaluate("viewSchedule()")
                            triggered = True
                            logger.debug("invoked viewSchedule() via JS for %s - %s", event_id, label)
                        except Exception:
                            logger.warning("could not trigger Go/viewSchedule for %s - %s", event_id, label)

                    # Wait for content change
                    try:
                        page.wait_for_timeout(300)
                    except Exception:
                        pass
                    for _ in range(40):  # ~10 seconds
                        try:
                            after_html = get_results_snapshot()
                            if after_html and after_html != before_html:
                                break
                        except Exception:
                            pass
                        try:
                            page.wait_for_timeout(250)
                        except Exception:
                            pass

                    # Small stabilization wait to ensure the DOM has finished rendering
                    try:
                        page.wait_for_timeout(400)
                    except Exception:
                        pass
                    # Capture raw HTML from the most relevant container; fallback to full page content
                    raw_html = None
                    try:
                        target = None
                        for fr in page.frames:
                            try:
                                if fr.locator("#bracketsTable, #resultsTable, #results, div.results, table.results").count() > 0:
                                    target = fr
                                    break
                            except Exception:
                                continue
                        if target is None:
                            target = page
                        loc = target.locator("#resultsTable, #bracketsTable, #results, div.results, table.results").first
                        try:
                            if loc.count() > 0:
                                target.wait_for_timeout(150)
                        except Exception:
                            pass
                        if loc.count() > 0:
                            raw_html = loc.inner_html()
                        else:
                            raw_html = target.content()
                    except Exception:
                        raw_html = None

                    # Upsert round with raw_html
                    upsert_round(conn, event_id, rid, label, raw_html)
                    logger.debug("%s %s -> round %s stored (html=%s)", event_id, name, label, "yes" if raw_html else "no")
                except Exception as e:
                    logger.warning("failed capturing round %s for %s: %s", label, event_id, e)

            # Return to list
            return_to_list(page)

        browser.close()


def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Scrape Round IDs and raw HTML for tournaments missing rounds")
    p.add_argument("--show", action="store_true", help="Run browser headed to observe scraping")
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
    logging.basicConfig(
        level=getattr(logging, (args.log_level or "INFO").upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    run_scraper(args)
    return 0


if __name__ == "__main__":
    if len(sys.argv) <= 1:
        default_args = argparse.Namespace(show=True, log_level="DEBUG")
        logging.basicConfig(
            level=getattr(logging, (default_args.log_level or "INFO").upper(), logging.INFO),
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )
        run_scraper(default_args)
    else:
        raise SystemExit(main())
