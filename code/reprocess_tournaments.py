"""
Re-process existing tournaments with updated parsing logic.

This script resets the parsed_ok flag and clears matches for specific tournaments,
allowing parse_round_html.py to re-process them with updated parsing logic.

Usage:
    # Re-process all tournaments
    uv run python code/reprocess_tournaments.py --all

    # Re-process specific event IDs
    uv run python code/reprocess_tournaments.py --event-ids 12345 67890

    # Re-process tournaments from a date range
    uv run python code/reprocess_tournaments.py --start-date 2024-01-01 --end-date 2024-12-31

    # Preview what would be re-processed (dry run)
    uv run python code/reprocess_tournaments.py --all --dry-run
"""

from __future__ import annotations

import argparse
import logging
from typing import List, Optional

import duckdb

try:
    from .config import get_db_path
except ImportError:
    from config import get_db_path


def reset_tournaments(
    conn: duckdb.DuckDBPyConnection,
    event_ids: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    all_tournaments: bool = False,
    dry_run: bool = False,
) -> int:
    """
    Reset parsed_ok flag and delete matches for specified tournaments.
    
    Returns the number of tournaments affected.
    """
    logger = logging.getLogger(__name__)
    
    # Build WHERE clause based on filters
    where_clauses = []
    params: List[object] = []
    
    if event_ids:
        placeholders = ",".join("?" * len(event_ids))
        where_clauses.append(f"event_id IN ({placeholders})")
        params.extend(event_ids)
    
    if start_date:
        where_clauses.append("event_id IN (SELECT event_id FROM tournaments WHERE start_date >= ?)")
        params.append(start_date)
    
    if end_date:
        where_clauses.append("event_id IN (SELECT event_id FROM tournaments WHERE start_date <= ?)")
        params.append(end_date)
    
    where_sql = ""
    if where_clauses and not all_tournaments:
        where_sql = "WHERE " + " AND ".join(where_clauses)
    elif not all_tournaments and not where_clauses:
        logger.error("No filter specified. Use --all, --event-ids, or date range.")
        return 0
    
    # Preview what will be affected
    preview_query = f"""--sql
        SELECT 
            tr.event_id,
            t.name,
            t.start_date,
            COUNT(DISTINCT tr.round_id) as rounds,
            COUNT(m.event_id) as matches
        FROM tournament_rounds tr
        LEFT JOIN tournaments t ON tr.event_id = t.event_id
        LEFT JOIN matches m ON tr.event_id = m.event_id AND tr.round_id = m.round_id
        {where_sql}
        GROUP BY tr.event_id, t.name, t.start_date
        ORDER BY t.start_date DESC, t.name
    """
    
    affected = conn.execute(preview_query, params).fetchall()
    
    if not affected:
        logger.info("No tournaments found matching criteria.")
        return 0
    
    logger.info("=" * 80)
    logger.info("Tournaments to be re-processed:")
    logger.info("=" * 80)
    total_matches = 0
    for event_id, name, start_date, rounds, matches in affected:
        logger.info(f"  {event_id} | {name} | {start_date} | {rounds} rounds | {matches} matches")
        total_matches += matches
    logger.info("=" * 80)
    logger.info(f"Total: {len(affected)} tournaments, {total_matches} matches to be deleted")
    logger.info("=" * 80)
    
    if dry_run:
        logger.info("[DRY RUN] No changes made. Remove --dry-run to execute.")
        return len(affected)
    
    # Execute the reset
    # 1. Delete matches
    delete_matches_query = f"""--sql
        DELETE FROM matches
        {where_sql}
    """
    deleted = conn.execute(delete_matches_query, params)
    logger.info(f"Deleted {deleted.fetchone() if deleted else 0} match rows")
    
    # 2. Reset parsed_ok flag
    reset_flag_query = f"""--sql
        UPDATE tournament_rounds
        SET parsed_ok = FALSE
        {where_sql}
    """
    updated = conn.execute(reset_flag_query, params)
    logger.info(f"Reset parsed_ok flag for {updated.fetchone() if updated else 0} round rows")
    
    logger.info("=" * 80)
    logger.info(f"Ready to re-process {len(affected)} tournaments")
    logger.info("Run: uv run python code/parse_round_html.py")
    logger.info("=" * 80)
    
    return len(affected)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Reset tournaments for re-processing with updated parsing logic",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Re-process all tournaments (default when run directly)
  uv run python code/reprocess_tournaments.py

  # Re-process all tournaments with explicit flag
  uv run python code/reprocess_tournaments.py --all

  # Re-process specific tournaments
  uv run python code/reprocess_tournaments.py --event-ids 12345 67890

  # Re-process tournaments from 2024
  uv run python code/reprocess_tournaments.py --start-date 2024-01-01 --end-date 2024-12-31

  # Preview changes without executing
  uv run python code/reprocess_tournaments.py --dry-run
        """
    )
    
    parser.add_argument(
        "--all",
        action="store_true",
        help="Re-process ALL tournaments (use with caution)"
    )
    parser.add_argument(
        "--event-ids",
        nargs="+",
        help="Specific event IDs to re-process"
    )
    parser.add_argument(
        "--start-date",
        help="Re-process tournaments from this date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end-date",
        help="Re-process tournaments up to this date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without executing"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)"
    )
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    
    # Default to --all if no filters specified
    if not any([args.all, args.event_ids, args.start_date, args.end_date]):
        args.all = True
        logging.getLogger(__name__).info("No filters specified, defaulting to --all tournaments")
    
    # Connect to database
    conn = duckdb.connect(str(get_db_path()))
    
    # Execute reset
    count = reset_tournaments(
        conn,
        event_ids=args.event_ids,
        start_date=args.start_date,
        end_date=args.end_date,
        all_tournaments=args.all,
        dry_run=args.dry_run,
    )
    
    conn.close()
    return 0 if count > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
