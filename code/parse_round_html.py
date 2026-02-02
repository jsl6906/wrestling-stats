"""
Parse saved Round Results HTML from tournament_rounds.raw_html into a matches table.

- Iterates tournament_rounds where raw_html IS NOT NULL and parsed_ok is NULL/False
- For each row, parses the HTML DOM looking for <section class="tw-list">
    - Under this section, there will be a sequence of <h2> and <ul> elements.
    - Each <h2> text becomes weight_class for subsequent <ul> siblings until next <h2>.
        - For each <ul> under a given weight class, each <li> inside the <ul> is one match.
            We save the inner HTML of the <li> (without the <li> wrapper) as raw_match_results,
            one row per <li>, with event_id, round_id, weight_class.
- Marks tournament_rounds.parsed_ok = TRUE after successful extraction for that row.

Run with uv:
    uv run code/parse_round_html.py
    uv run code/parse_round_html.py --reparse  # Re-parse all rounds, deleting existing matches
"""

from __future__ import annotations

import argparse
import logging
from typing import List, Optional, Dict, Any, Tuple
import re

from bs4 import BeautifulSoup, Tag
import duckdb
from tqdm import tqdm

try:
    from .config import get_db_path
except ImportError:
    from config import get_db_path


def ensure_schema(conn: duckdb.DuckDBPyConnection) -> None:
    # Ensure matches table exists with structured fields; add missing columns if table already exists
    conn.execute(
        """--sql
        CREATE TABLE IF NOT EXISTS matches (
            event_id TEXT,
            round_id TEXT,
            weight_class TEXT,
            raw_match_results TEXT,
            round_detail TEXT,
            winner_name TEXT,
            winner_team TEXT,
            decision_type TEXT,
            loser_name TEXT,
            loser_team TEXT,
            decision_type_code TEXT,
            winner_points INTEGER,
            loser_points INTEGER,
            fall_time TEXT,
            bye BOOLEAN,
            first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # Backfill: add any missing columns for older databases
    cols = set(
        r[0]
        for r in conn.execute(
            """--sql
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'matches'
            """
        ).fetchall()
    )
    expected: list[tuple[str, str]] = [
        ("event_id", "TEXT"),
        ("round_id", "TEXT"),
        ("weight_class", "TEXT"),
        ("raw_match_results", "TEXT"),
        ("round_detail", "TEXT"),
        ("winner_name", "TEXT"),
        ("winner_team", "TEXT"),
        ("decision_type", "TEXT"),
        ("loser_name", "TEXT"),
        ("loser_team", "TEXT"),
        ("decision_type_code", "TEXT"),
        ("winner_points", "INTEGER"),
        ("loser_points", "INTEGER"),
        ("fall_time", "TEXT"),
        ("bye", "BOOLEAN"),
        ("first_seen", "TIMESTAMP"),
    ]
    for name, typ in expected:
        if name not in cols:
            conn.execute(f"""--sql
            ALTER TABLE matches ADD COLUMN {name} {typ}
            """)

    # tournament_rounds is created with full schema by the scraper shared module


def fetch_unparsed_round_html(conn: duckdb.DuckDBPyConnection, reparse: bool = False) -> List[tuple]:
    """Fetch round HTML to parse. If reparse=True, includes already-parsed rounds."""
    if reparse:
        rows = conn.execute(
            """--sql
            SELECT event_id, round_id, label, raw_html
            FROM tournament_rounds
            WHERE raw_html IS NOT NULL
            ORDER BY event_id, round_id
            """
        ).fetchall()
    else:
        rows = conn.execute(
            """--sql
            SELECT event_id, round_id, label, raw_html
            FROM tournament_rounds
            WHERE raw_html IS NOT NULL AND COALESCE(parsed_ok, FALSE) = FALSE
            ORDER BY event_id, round_id
            """
        ).fetchall()
    return rows


def insert_match(conn: duckdb.DuckDBPyConnection, row: Dict[str, Any]) -> None:
    """Insert one parsed match row into matches."""
    conn.execute(
        """--sql
        INSERT INTO matches (
            event_id, round_id, weight_class, raw_match_results,
            round_detail, winner_name, winner_team, decision_type,
            loser_name, loser_team, decision_type_code,
            winner_points, loser_points, fall_time, bye
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            row.get("event_id"), row.get("round_id"), row.get("weight_class"), row.get("raw_match_results"),
            row.get("round_detail"), row.get("winner_name"), row.get("winner_team"), row.get("decision_type"),
            row.get("loser_name"), row.get("loser_team"), row.get("decision_type_code"),
            row.get("winner_points"), row.get("loser_points"), row.get("fall_time"), row.get("bye"),
        ],
    )


def delete_all_matches(conn: duckdb.DuckDBPyConnection) -> tuple[int, int]:
    """Delete all matches and wrestler_history. Returns (matches_deleted, history_deleted)."""
    # Count before deleting
    history_result = conn.execute("""--sql
        SELECT COUNT(*) FROM wrestler_history
    """).fetchone()
    history_count = history_result[0] if history_result else 0
    
    matches_result = conn.execute("""--sql
        SELECT COUNT(*) FROM matches
    """).fetchone()
    matches_count = matches_result[0] if matches_result else 0
    
    # Delete all wrestler_history
    conn.execute("""--sql
        DELETE FROM wrestler_history
    """)
    
    # Delete all matches
    conn.execute("""--sql
        DELETE FROM matches
    """)
    
    return matches_count, history_count


def delete_matches_for_round(conn: duckdb.DuckDBPyConnection, event_id: str, round_id: str) -> int:
    """Delete all existing matches and wrestler_history for a given round. Returns count of deleted matches."""
    # First delete wrestler_history records that reference matches from this round
    # wrestler_history has match_rowid that references matches.rowid
    conn.execute(
        """--sql
        DELETE FROM wrestler_history
        WHERE match_rowid IN (
            SELECT rowid FROM matches
            WHERE event_id = ? AND round_id = ?
        )
        """,
        [event_id, round_id],
    )
    
    # Then delete the matches themselves and count deleted rows
    deleted_rows = conn.execute(
        """--sql
        DELETE FROM matches
        WHERE event_id = ? AND round_id = ?
        """,
        [event_id, round_id],
    ).fetchall()
    # DuckDB DELETE returns the number of rows deleted
    return len(deleted_rows) if deleted_rows else 0


def mark_parsed_ok(conn: duckdb.DuckDBPyConnection, event_id: str, round_id: str) -> None:
    conn.execute(
        """--sql
        UPDATE tournament_rounds
        SET parsed_ok = TRUE
        WHERE event_id = ? AND round_id = ?
        """,
        [event_id, round_id],
    )


def parse_round_html(raw_html: str) -> List[tuple]:
    """Return a list of tuples: (weight_class, raw_li_html) for each match.
    
    Supports two formats:
    1. Standard tournament format: section.tw-list with <h2> weight classes and <ul><li> matches
    2. Dual meet format: table.tw-table with <tr> rows containing weight class and match data
    """
    soup = BeautifulSoup(raw_html or "", "html.parser")
    results: List[tuple] = []
    
    # Try dual meet table format first (table.tw-table)
    table = soup.select_one("table.tw-table")
    if table:
        # Dual meet format: each <tr> contains weight class in first <td>, match in second <td>
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) >= 2:
                # First cell typically contains weight class link
                weight_cell = cells[0]
                match_cell = cells[1]
                
                # Extract weight class (from link text or cell text)
                weight_link = weight_cell.find("a")
                if weight_link:
                    weight_class = (weight_link.get_text(strip=True) or "").strip()
                else:
                    weight_class = (weight_cell.get_text(strip=True) or "").strip()
                
                # Skip header rows, empty weight classes, and penalty/administrative entries
                # Penalty entries have keywords like: unsportsmanlike, misconduct, correction, etc.
                if not weight_class or weight_class.lower() in ["&nbsp;", "", "match summary"]:
                    continue
                
                # Skip penalty/administrative rows (these contain point deductions, not actual matches)
                penalty_keywords = [
                    "unsportsmanlike", "misconduct", "correction", "bench", 
                    "unsport", "penalty", "deduction"
                ]
                if any(keyword in weight_class.lower() for keyword in penalty_keywords):
                    continue
                
                # Get match summary HTML (inner HTML of the match cell)
                match_html = match_cell.decode_contents()
                if match_html and match_html.strip():
                    results.append((weight_class, match_html))
        
        if results:  # If we found dual meet data, return it
            return results
    
    # Fall back to standard tournament format (section.tw-list)
    section = soup.select_one("section.tw-list, section[class~=tw-list]")
    if not section:
        return results
    
    current_weight: Optional[str] = None
    for child in section.children:
        if not isinstance(child, Tag):
            continue
        tag = (child.name or "").lower()
        if tag == "h2":
            current_weight = (child.get_text(" ", strip=True) or "").strip()
        elif tag == "ul" and current_weight:
            # Each <li> under this <ul> is a match
            lis = child.find_all("li", recursive=False)
            if not lis:
                # Some pages may not use strict structure; fallback to all lis
                lis = child.find_all("li")
            for li in lis:
                # Save inner HTML only (strip the <li> wrapper)
                if isinstance(li, Tag):
                    inner = li.decode_contents()
                else:
                    inner = str(li)
                results.append((current_weight, inner))
        # ignore other tags for now
    return results


def _normalize_text(s: str) -> str:
    return " ".join((s or "").replace("\xa0", " ").split())


def _fix_known_issues(s: str) -> str:
    """Apply targeted cleanup rules to raw input text before parsing.
    Start with specific substitutions; extend as new issues are found.
    """
    try:
        # Normalize 'Keyvon (kj) Riley' -> 'Keyvon Riley' (case-insensitive, flexible spacing)
        s = re.sub(r"Keyvon\s*\(\s*kj\s*\)\s*Riley", "Keyvon Riley", s, flags=re.I)
    except Exception:
        pass
    return s


# -----------------------------
# Configurable conversions
# -----------------------------
# Add name and team corrections here. Each entry is (regex_pattern, replacement), case-insensitive.
# Example: fix nicknames in parentheses or common misspellings.
NAME_CONVERSIONS_RAW: List[Tuple[str, str]] = [
    (r"\bKeyvon\s*\(\s*kj\s*\)\s*Riley\b", "Keyvon Riley"),
    # Normalize Blaise McNeil variants (McNeil/McNeill/Mcneil) to 'Blaise McNeil'
    (r"\bBlaise\s+McNeil{1,2}\b", "Blaise McNeil"),
    # Normalize Mateo/Matteo Corsini variants to 'Matteo Corsini'
    (r"\bMat{1,2}eo\s+Corsini\b", "Matteo Corsini"),
    # Normalize Carter Van Dyk/Van-dyk variants to 'Carter Van-Dyk'
    (r"\bCarter\s+Van[\s-]?[Dd]yk\b", "Carter Van-Dyk"),
    # Normalize Chaley Pai-Bedell/Pia-Bedell variants to 'Chaley Pai-Bedell'
    (r"\bChaley\s+P[ia]{2}-Bedell\b", "Chaley Pai-Bedell"),
    # Remove "(correct)" annotation from names
    (r"\bBlake\s+Rosenbaum\s+\(correct\)\b", "Blake Rosenbaum"),
    # Remove forfeit/bye suffixes from names (e.g., "John Doe-Forfeit" -> "John Doe")
    (r"-\s*(?:Forfeit|Bye|DFF|DDQ|Unknown|Forfiet)\b", ""),
    # Remove any digits present in names (e.g., 'John 2 Doe' -> 'John Doe')
    (r"\d+", ""),
]

TEAM_CONVERSIONS_RAW: List[Tuple[str, str]] = [
    (r"\bAlexandria\s+Junior\s+Titans\b", "Alexandria"),
    (r"\bAnnandale\s+Mat\s+Rats\b", "Annandale"),
    (r"\bBraddock\s+Wrestling\s+Club\b", "Braddock"),
    (r"\bE9\b", "E9 Wrestling"),
    (r"\bE9\s*Wrestling\b|\bE9Wrestling\b", "E9 Wrestling"),
    (r"\bFauquier\s+Wrestling\b", "Fauquier"),
    (r"\bFort\s*Belvoir\b|\bFortBelvoir\b", "Fort Belvoir"),
    (r"\bFranconia\s+Wrestling\s+Club\b", "Franconia"),
    (r"\bGunston\s+Wrestling\s+Club\b", "Gunston"),
    (r"\bHerndon\s*Hawks\b|\bHerndonHawks\b", "Herndon Hawks"),
    (r"\bKing\s*George\b|\bKingGeorge\b", "King George"),
    (r"\bKing\s+George\s+Wrestling\s+Club\b", "King George"),
    (r"\bAlexandria\b", "Alexandria Junior Titans"),  # note: later rule overrides earlier mapping
    (r"\bMcLean\s+Lions?\s+Wrestling\b", "McLean"),
    (r"\bMount\s+Vernon\s+Youth\s+Wrestling\b", "Mt Vernon"),
    (r"\bMount\s*Vernon\b|\bMountVernon\b", "Mt Vernon"),
    (r"\bPit\s*Bull\b|\bPitBull\b", "Pit Bull"),
    (r"\bPrince\s+William\s+County\s+Wrestling\s+Club\b", "Prince William"),
    (r"\bPrince\s+William\s+Wrestling\s+Club\b", "Prince William"),
    (r"\bPrinceWilliam\b", "Prince William"),
    (r"\bRangers\b", "Ranger Wrestling Club"),
    (r"\bScanlan\s+Wrestling\s+Academy\b", "Scanlan"),
    (r"\bScanlon\s+Wrestling\b", "Scanlan"),
    (r"\bSmyrna\s+Wrestling\b", "Smyrna"),
    (r"\bSouth\s+County\s+Athletic\s+Association\b", "South County"),
    (r"\bSouthCounty\b", "South County"),
    (r"\bVienna\s+Youth\s+Inc\b", "Vienna"),
    (r"\bVikings?\s+Wrestling\s+Club\b", "Vikings"),
    (r"\bWild\s*Buffalos\b|\bWildBuffalos\b", "Wild Buffalos"),
]

NAME_CONVERSIONS = [(re.compile(pat, re.IGNORECASE), repl) for pat, repl in NAME_CONVERSIONS_RAW]
TEAM_CONVERSIONS = [(re.compile(pat, re.IGNORECASE), repl) for pat, repl in TEAM_CONVERSIONS_RAW]


def _apply_conversions(value: Optional[str], conversions: List[Tuple[re.Pattern[str], str]]) -> Optional[str]:
    if not value:
        return value
    out = value
    for rx, repl in conversions:
        out = rx.sub(repl, out)
    return out.strip()


def _extract_team_with_parens(text: str, start_pos: int) -> tuple[str, int]:
    """Extract team name that may contain nested parentheses.
    
    Args:
        text: The full text to search
        start_pos: Position of the opening '(' for the team name
    
    Returns:
        Tuple of (team_name, end_position) where end_position is after the closing ')'
    """
    if start_pos >= len(text) or text[start_pos] != '(':
        return "", start_pos
    
    depth = 0
    pos = start_pos
    
    while pos < len(text):
        if text[pos] == '(':
            depth += 1
        elif text[pos] == ')':
            depth -= 1
            if depth == 0:
                # Found the matching closing paren
                team_name = text[start_pos + 1:pos]
                return team_name, pos + 1
        pos += 1
    
    # No matching closing paren found
    return "", start_pos


def _parse_wrestler_team(text: str) -> tuple[Optional[str], Optional[str], int]:
    """Parse 'Name (Team) [record]' pattern, handling nested parens in team names.
    
    The team is always the LAST set of parentheses BEFORE any record,
    which allows names with parentheticals like "Bilegt (Billy) Arslan (Mclean)" to be parsed correctly.
    Stops searching after finding a record to avoid confusing decision codes with team names.
    
    Returns:
        Tuple of (wrestler_name, team_name, end_position)
    """
    import re as _re
    
    # Find the LAST opening paren that has a matching closing paren
    # But stop if we encounter a record (e.g., "17-21") after a parenthetical group
    last_team_start = -1
    last_team_end = -1
    
    # Scan through the text to find all balanced parenthetical groups
    pos = 0
    while pos < len(text):
        if text[pos] == '(':
            # Try to extract this parenthetical group
            team_candidate, end_pos = _extract_team_with_parens(text, pos)
            if end_pos > pos:  # Successfully extracted
                last_team_start = pos
                last_team_end = end_pos
                pos = end_pos
                
                # Check if there's a record after this parenthetical
                remaining = text[pos:].lstrip()
                record_match = _re.match(r'^\d+-\d+', remaining)
                if record_match:
                    # Found a record - this is definitely the team, stop searching
                    break
            else:
                pos += 1
        else:
            pos += 1
    
    if last_team_start == -1:
        # No parentheses found
        return None, None, 0
    
    # Extract the team name from the last parenthetical
    wrestler_name = text[:last_team_start].strip()
    team_name = text[last_team_start + 1:last_team_end - 1]
    end_pos = last_team_end
    
    # Skip optional record (e.g., "17-21") after the team
    remaining = text[end_pos:].lstrip()
    record_match = _re.match(r'^\d+-\d+\s*', remaining)
    if record_match:
        end_pos += len(text[end_pos:]) - len(remaining) + len(record_match.group(0))
    
    return wrestler_name, team_name, end_pos


def _parse_wrestler_team_first(text: str) -> tuple[Optional[str], Optional[str], int]:
    """Parse 'Name (Team)' pattern, intelligently finding the team parenthesis.
    
    This is used for the "over" format where decision codes may have parentheses after the team.
    Handles nicknames in parentheses like "John (Peyton) Cherkaur (Gloucester HS)".
    
    Strategy:
    1. If a parenthesis is followed by a record (e.g., "17-21"), it's the team
    2. Otherwise, prefer parentheses that look like team names (contain "HS", multiple words, etc.)
    3. Skip parentheses that look like nicknames (single word, short, mid-name)
    
    Returns:
        Tuple of (wrestler_name, team_name, end_position)
    """
    import re as _re
    
    # Collect all parenthetical groups with their positions
    candidates = []
    pos = 0
    while pos < len(text):
        if text[pos] == '(':
            # Try to extract this parenthetical group
            content, end_pos = _extract_team_with_parens(text, pos)
            if end_pos > pos:  # Successfully extracted
                # Check if followed by a record
                remaining_after = text[end_pos:].lstrip()
                has_record = bool(_re.match(r'^\d+-\d+', remaining_after))
                
                # Calculate heuristic score for being a team name
                score = 0
                content_lower = content.lower()
                
                # Strong indicators it's a team name
                if has_record:
                    score += 100  # Record after = definitely the team
                if ' hs' in content_lower or content_lower.endswith('hs'):
                    score += 50
                if 'high school' in content_lower:
                    score += 50
                if ' ' in content.strip():  # Multiple words
                    score += 30
                if len(content) > 10:  # Longer content
                    score += 20
                
                # Indicators it's a nickname
                if ' ' not in content.strip():  # Single word
                    score -= 30
                if len(content) <= 8:  # Short content
                    score -= 20
                if pos > 0 and pos < len(text) - end_pos:  # Mid-text (not at start or end)
                    name_before = text[:pos].strip()
                    if name_before and not name_before.endswith(')'):  # Name continues after
                        score -= 25
                
                candidates.append((score, pos, end_pos, content))
                pos = end_pos
            else:
                pos += 1
        else:
            pos += 1
    
    if not candidates:
        # No parentheses found
        return None, None, 0
    
    # Take the candidate with the highest score (most likely to be team)
    candidates.sort(reverse=True)  # Sort by score descending
    score, team_start, team_end, team_name = candidates[0]
    
    # Extract wrestler name (everything before the team)
    wrestler_name = text[:team_start].strip()
    end_pos = team_end
    
    # Skip optional record (e.g., "17-21") after the team
    remaining = text[end_pos:].lstrip()
    record_match = _re.match(r'^\d+-\d+\s*', remaining)
    if record_match:
        end_pos += len(text[end_pos:]) - len(remaining) + len(record_match.group(0))
    
    return wrestler_name, team_name, end_pos


def _normalize_person_name(name: Optional[str]) -> Optional[str]:
    if not name:
        return name
    # First apply title case to handle lowercase names like "anthony gleeson"
    # This converts to "Anthony Gleeson"
    name_titled = name.title()
    # Then apply conversions which will fix special cases like "Mcneil" -> "McNeil"
    out = _apply_conversions(name_titled, NAME_CONVERSIONS)
    if out is None:
        return out
    # Collapse multiple spaces created by removals
    return " ".join(out.split())


def _normalize_team_name(team: Optional[str]) -> Optional[str]:
    return _apply_conversions(team, TEAM_CONVERSIONS)


def _apply_name_team_conversions(out: Dict[str, Any]) -> Dict[str, Any]:
    # Check for empty or invalid loser names (e.g., "()", empty string, or only whitespace)
    loser_name = out.get("loser_name", "")
    if loser_name and isinstance(loser_name, str):
        loser_name_clean = loser_name.strip()
        # Treat as forfeit/bye if loser name is empty, "()", or similar invalid values
        if not loser_name_clean or loser_name_clean in ["()", "[]", "{}"]:
            out["decision_type"] = "forfeit"
            out["decision_type_code"] = "For."
            out["bye"] = False  # Forfeit still counts for Elo (winner gets credit)
            out["loser_name"] = None
            out["loser_team"] = None
            out["winner_points"] = None
            out["loser_points"] = None
            out["fall_time"] = None
            return out
    
    # Check if either participant is "Unknown (Unattached)" - treat as a bye
    winner_is_unknown = (out.get("winner_name") == "Unknown" and out.get("winner_team") == "Unattached")
    loser_is_unknown = (out.get("loser_name") == "Unknown" and out.get("loser_team") == "Unattached")
    
    if winner_is_unknown or loser_is_unknown:
        out["decision_type"] = "bye"
        out["decision_type_code"] = "Bye"
        out["bye"] = True
        # Keep the real wrestler, clear the unknown one
        if winner_is_unknown:
            out["winner_name"] = out.get("loser_name")
            out["winner_team"] = out.get("loser_team")
            out["loser_name"] = None
            out["loser_team"] = None
        else:
            out["loser_name"] = None
            out["loser_team"] = None
        # Clear scoring details since it's a bye
        out["winner_points"] = None
        out["loser_points"] = None
        out["fall_time"] = None
        return out
    
    # Check if both participants are "Forfeit" (double forfeit) - already marked as bye but ensure names are cleared
    winner_name = out.get("winner_name", "")
    loser_name = out.get("loser_name", "")
    if (winner_name and "forfeit" in winner_name.lower() and 
        loser_name and "forfeit" in loser_name.lower()):
        out["decision_type"] = "bye"
        out["decision_type_code"] = "DFF"
        out["bye"] = True
        out["winner_name"] = None
        out["winner_team"] = None
        out["loser_name"] = None
        out["loser_team"] = None
        out["winner_points"] = None
        out["loser_points"] = None
        out["fall_time"] = None
        return out
    
    # Normalize winner/loser names and teams via configured conversions
    if out.get("winner_name"):
        out["winner_name"] = _normalize_person_name(out["winner_name"])  # type: ignore[arg-type]
    if out.get("loser_name"):
        out["loser_name"] = _normalize_person_name(out["loser_name"])  # type: ignore[arg-type]
    if out.get("winner_team"):
        out["winner_team"] = _normalize_team_name(out["winner_team"])  # type: ignore[arg-type]
    if out.get("loser_team"):
        out["loser_team"] = _normalize_team_name(out["loser_team"])  # type: ignore[arg-type]
    return out


def parse_match_text(raw_text: str) -> Dict[str, Any]:
    """Parse a single match text line into structured fields.
    Returns keys: round_detail, winner_name, winner_team, decision_type,
    loser_name, loser_team, decision_type_code, winner_points, loser_points, fall_time, bye.
    """
    import re as _re

    # First, fix known data issues, then normalize whitespace
    text = _fix_known_issues(raw_text)
    text = _normalize_text(text)
    out: Dict[str, Any] = {
        "round_detail": None,
        "winner_name": None,
        "winner_team": None,
        "decision_type": None,
        "loser_name": None,
        "loser_team": None,
        "decision_type_code": None,
        "winner_points": None,
        "loser_points": None,
        "fall_time": None,
        "bye": False,
    }

    # Skip dual meet score summary rows (just team scores, no match data)
    # These appear as simple numbers like "72.0", "30.0", or adjustments like "-1.0", "-3.0", "-7.0"
    if _re.match(r'^-?\d+\.?\d*$', text.strip()):
        out["bye"] = True
        out["decision_type"] = "bye"
        out["decision_type_code"] = "SCORE"
        return out

    # Extract round detail prefix if present
    # Only split on " - " if it appears to be a round prefix (contains "round", "place", "champ", etc.)
    # This avoids splitting on hyphens in names like "Sampson - Johnson"
    if " - " in text:
        potential_rd, potential_rest = text.split(" - ", 1)
        # Check if the potential round detail looks like an actual round prefix
        rd_lower = potential_rd.lower()
        if any(keyword in rd_lower for keyword in ["round", "place", "champ", "semi", "quarter", "final", "cons", "prelim"]):
            out["round_detail"] = potential_rd.strip()
            rest = potential_rest
        else:
            rest = text
    else:
        rest = text

    # Double forfeit case (standalone text)
    if rest.strip().lower() == "double forfeit":
        out["decision_type"] = "bye"
        out["decision_type_code"] = "DFF"
        out["bye"] = True
        return out

    # DFF (double forfeit) or DDQ (double disqualification) case: "A (Team) and B (Team) DFF/DDQ"
    if "dff" in rest.lower() or "ddq" in rest.lower():
        m = _re.search(r"^(?P<a>.+?) \((?P<ateam>.*?)\)(?:\s+\d+-\d+)?\s+and\s+(?P<b>.+?) \((?P<bteam>.*?)\)(?:\s+\d+-\d+)?\s+(?:\((?P<code>DFF|DDQ)\)|(?P<code2>DFF|DDQ))$", rest, _re.I)
        if m:
            # Store both participants; treat as a bye to skip Elo
            out["winner_name"] = m.group("a").strip()
            out["winner_team"] = m.group("ateam").strip()
            out["loser_name"] = m.group("b").strip()
            out["loser_team"] = m.group("bteam").strip()
            out["decision_type"] = "bye"
            out["decision_type_code"] = (m.group("code") or m.group("code2")).upper()
            out["bye"] = True
            return _apply_name_team_conversions(out)

    # Bye case
    if "received a bye" in rest.lower():
        m = _re.search(r"^(?P<win>.+?) \((?P<wteam>.*?)\)(?:\s+\d+-\d+)?\s+received a bye", rest, _re.I)
        if m:
            out["winner_name"] = m.group("win").strip()
            out["winner_team"] = m.group("wteam").strip()
        out["decision_type"] = "bye"
        out["decision_type_code"] = "Bye"
        out["bye"] = True
        return _apply_name_team_conversions(out)

    # "X vs Y" format (no decision yet, treat as bye)
    m_vs = _re.search(r"^(?P<a>.+?)\s+\((?P<ateam>.*?)\)(?:\s+\d+-\d+)?\s+vs\s+(?P<b>.+?)\s+\((?P<bteam>.*?)\)(?:\s+\d+-\d+)?", rest, _re.I)
    if m_vs:
        out["winner_name"] = m_vs.group("a").strip()
        out["winner_team"] = m_vs.group("ateam").strip()
        out["loser_name"] = m_vs.group("b").strip()
        out["loser_team"] = m_vs.group("bteam").strip()
        out["decision_type"] = "bye"
        out["decision_type_code"] = "Bye"
        out["bye"] = True
        return _apply_name_team_conversions(out)

    # "Won in <code> by <dtype>" combined format (e.g., "won in SV-1 by fall over")
    # This handles cases like "Jax Engh (Team) won in SV-1 by fall over Nathan Taylor (Team) (SV-1 (Fall) 6:30)"
    # Also handles: "won in TB-3 by riding time over ... (TB-3 (RT) 2-2)"
    # The team name should not include trailing content - use [^)]+ to stop at first )
    m_in_by = _re.search(
        r"^(?P<win>.+?)\s+\((?P<wteam>.*?)\)(?:\s+\d+-\d+)?\s+won in\s+(?P<code>[A-Za-z0-9-]+)\s+by\s+(?P<dtype>.+?)\s+over\s+"
        r"(?P<lose>.+?)\s+\((?P<lteam>[^)]+)\)(?:\s+\d+-\d+)?\s*"
        r"(?:\((?P<dcode_paren>.+?)(?:\s+\((?P<dtype_paren>[^)]+)\))?\s+(?:(?P<ftime_paren>\d+:\d+)|(?P<score_paren>\d+-\d+))\)|(?P<score>\d+-\d+))?$",
        rest,
        _re.I,
    )
    if m_in_by:
        out["winner_name"] = m_in_by.group("win").strip()
        out["winner_team"] = m_in_by.group("wteam").strip()
        out["loser_name"] = m_in_by.group("lose").strip()
        out["loser_team"] = m_in_by.group("lteam").strip()
        
        # The code is in the "won in" part (e.g., SV-1)
        out["decision_type_code"] = m_in_by.group("code").strip()
        
        # The decision type is from "by <dtype>" part (e.g., "fall")
        dtype = m_in_by.group("dtype").strip().lower()
        if "fall" in dtype:
            out["decision_type"] = "fall"
        elif "sudden victory" in dtype:
            out["decision_type"] = "sudden victory"
        elif "overtime" in dtype:
            out["decision_type"] = "overtime"
        else:
            out["decision_type"] = dtype
        
        # Extract fall time if present
        ftime = m_in_by.group("ftime_paren")
        if ftime:
            out["fall_time"] = ftime
        
        # Extract score if present (either from paren or standalone)
        score = m_in_by.group("score_paren") or m_in_by.group("score")
        if score:
            try:
                wp, lp = score.split("-")
                out["winner_points"] = int(wp)
                out["loser_points"] = int(lp)
            except Exception:
                pass
        
        return _apply_name_team_conversions(out)

    # "Won in <type>" cases (e.g., sudden victory - 1, double overtime)
    # Handles both formats: "SV-1 16-14" and "(SV-1 16-14)" and "(2-OT 7-5)"
    m_in = _re.search(
        r"^(?P<win>.+?)\s+\((?P<wteam>.*?)\)(?:\s+\d+-\d+)?\s+won in\s+(?P<dtype>.+?)\s+over\s+"
        r"(?P<lose>.+?)\s+\((?P<lteam>.*?)\)(?:\s+\d+-\d+)?\s+"
        r"(?:\((?P<dcode_paren>[A-Za-z0-9][A-Za-z0-9. -]*?)(?:\s+(?P<ftime_paren>\d+:\d+))?(?:\s+(?P<score_paren>\d+-\d+))?\)"
        r"|(?P<dcode>(?![0-9]+-[0-9]+)[A-Za-z0-9-]+)(?:\s+\((?P<dnote>[^)]+)\))?(?:\s+(?P<score>\d+-\d+)|\s+(?P<ftime>\d+:\d+))?)$",
        rest,
        _re.I,
    )
    if m_in:
        out["winner_name"] = m_in.group("win").strip()
        out["winner_team"] = m_in.group("wteam").strip()
        dtype = m_in.group("dtype").strip().lower()
        # Normalize: if contains 'by fall' or 'fall', treat as fall for Elo; else detect sudden victory.
        if "by fall" in dtype or dtype.startswith("fall") or " fall" in dtype:
            out["decision_type"] = "fall"
        elif "sudden victory" in dtype:
            out["decision_type"] = "sudden victory"
        elif "overtime" in dtype or "over time" in dtype:
            out["decision_type"] = "overtime"
        else:
            out["decision_type"] = dtype
        out["loser_name"] = m_in.group("lose").strip()
        out["loser_team"] = m_in.group("lteam").strip()
        
        # Handle both formats: (Code score) and Code score
        if m_in.group("dcode_paren"):
            out["decision_type_code"] = m_in.group("dcode_paren").strip()
            score = m_in.group("score_paren")
            ftime = m_in.group("ftime_paren")
        else:
            out["decision_type_code"] = m_in.group("dcode").strip()
            score = m_in.group("score")
            ftime = m_in.group("ftime")
        
        if ftime and (out.get("decision_type") or "").startswith("fall"):
            out["fall_time"] = ftime
        elif score:
            try:
                wp, lp = score.split("-")
                out["winner_points"] = int(wp)
                out["loser_points"] = int(lp)
            except Exception:
                pass
        return _apply_name_team_conversions(out)

    # "Won by" format with manual parsing to handle nicknames in parentheses
    # Try manual parsing first for "won by" format to handle names like "Bilegt (Billy) Arslan (Mclean)"
    if " won by " in rest.lower() and " over " in rest.lower():
        won_by_pos = rest.lower().find(" won by ")
        if won_by_pos > 0:
            # Parse winner (use LAST parens for team to handle nicknames)
            winner_text = rest[:won_by_pos]
            winner_name, winner_team, _ = _parse_wrestler_team(winner_text)
            
            if winner_name:
                # Find "over" after "won by"
                after_won_by = rest[won_by_pos + 8:]  # Skip " won by "
                over_pos_in_remainder = after_won_by.lower().find(" over ")
                
                if over_pos_in_remainder > 0:
                    # Extract decision type between "won by" and "over"
                    decision_type = after_won_by[:over_pos_in_remainder].strip()
                    
                    # Parse loser (use LAST parens for team to handle nicknames)
                    after_over = after_won_by[over_pos_in_remainder + 6:].lstrip()  # Skip " over "
                    loser_name, loser_team, loser_end = _parse_wrestler_team(after_over)
                    
                    if loser_name:
                        # Parse decision code and score/time from remainder
                        remaining = after_over[loser_end:].lstrip()
                        
                        # Handle both parenthetical and non-parenthetical codes
                        # Patterns: (Code time (score)), (Code time score), (Code score), Code score, Code time
                        code_match = _re.match(
                            r'^(?:\((?P<dcode_paren>[A-Za-z0-9][A-Za-z0-9. -]*?)(?:\s+(?P<ftime_paren>\d+:\d+))?(?:\s+\((?P<score_paren_nested>\d+-\d+)\)|(?:\s+(?P<score_paren>\d+-\d+)))?\)|(?P<dcode>(?![0-9]+-[0-9]+)[A-Za-z0-9-]+)(?:\s+\((?P<dnote>[^)]+)\))?(?:\s+(?P<score>\d+-\d+)|\s+(?P<ftime>\d+:\d+))?)$',
                            remaining
                        )
                        
                        if code_match or not remaining:  # Match or no code at all
                            out["winner_name"] = winner_name
                            out["winner_team"] = winner_team or ""
                            out["decision_type"] = decision_type.lower()
                            out["loser_name"] = loser_name
                            out["loser_team"] = loser_team or ""
                            
                            if code_match:
                                # Handle multiple formats
                                if code_match.group("dcode_paren"):
                                    out["decision_type_code"] = code_match.group("dcode_paren").strip()
                                    score = code_match.group("score_paren_nested") or code_match.group("score_paren")
                                    ftime = code_match.group("ftime_paren")
                                else:
                                    out["decision_type_code"] = code_match.group("dcode").strip() if code_match.group("dcode") else None
                                    score = code_match.group("score")
                                    ftime = code_match.group("ftime")
                                
                                if ftime and out["decision_type"].startswith("fall"):
                                    out["fall_time"] = ftime
                                elif ftime and "tech" in out["decision_type"]:
                                    out["fall_time"] = ftime
                                if score:
                                    try:
                                        wp, lp = score.split("-")
                                        out["winner_points"] = int(wp)
                                        out["loser_points"] = int(lp)
                                    except Exception:
                                        pass
                            
                            return _apply_name_team_conversions(out)

    # Special case: forfeit with empty loser name - "won by forfeit over () FF"
    m_forfeit_empty = _re.search(
        r"^(?P<win>.+?)\s+\((?P<wteam>.*?)\)(?:\s+\d+-\d+)?\s+won by\s+(?P<dtype>forfeit)\s+over\s+\(\)\s*(?P<dcode>[A-Za-z0-9.]+)?$",
        rest,
        _re.I,
    )
    if m_forfeit_empty:
        out["winner_name"] = m_forfeit_empty.group("win").strip()
        out["winner_team"] = m_forfeit_empty.group("wteam").strip()
        out["decision_type"] = "forfeit"
        out["decision_type_code"] = m_forfeit_empty.group("dcode").strip() if m_forfeit_empty.group("dcode") else "For."
        out["loser_name"] = None
        out["loser_team"] = None
        return _apply_name_team_conversions(out)

    # Normal or fall cases - "won by <decision>"
    # Winner (Team) won by <decision_type> over Loser (Team) <CODE> <score|time>
    # Also handles format: Winner (Team) 3-2 won by fall over Loser (Team) 0-2 (Fall 0:21)
    # Also handles nested format: (TF-1.5 5:20 (16-0))
    # Also handles codes with spaces: (M. For.)
    # Also handles codes starting with numbers: (2-OT 7-5)
    # Note: Parenthetical codes must be checked first to avoid matching record "0-2" as decision code
    m = _re.search(
        r"^(?P<win>.+?)\s+\((?P<wteam>.*?)\)(?:\s+\d+-\d+)?\s+won by\s+(?P<dtype>.+?)\s+over\s+"
        r"(?P<lose>.+?)\s+\((?P<lteam>.*?)\)(?:\s+\d+-\d+)?\s+"
        r"(?:\((?P<dcode_paren>[A-Za-z0-9][A-Za-z0-9. -]*?)(?:\s+(?P<ftime_paren>\d+:\d+))?(?:\s+\((?P<score_paren_nested>\d+-\d+)\)|(?:\s+(?P<score_paren>\d+-\d+)))?\)"
        r"|(?P<dcode>(?![0-9]+-[0-9]+)[A-Za-z0-9-]+)(?:\s+\((?P<dnote>[^)]+)\))?(?:\s+(?P<score>\d+-\d+)|\s+(?P<ftime>\d+:\d+))?)$",
        rest,
        _re.I,
    )
    if m:
        out["winner_name"] = m.group("win").strip()
        out["winner_team"] = m.group("wteam").strip()
        out["decision_type"] = m.group("dtype").strip().lower()
        out["loser_name"] = m.group("lose").strip()
        out["loser_team"] = m.group("lteam").strip()
        
        # Handle multiple formats: (Code time (score)), (Code time), (Code score), or Code time
        if m.group("dcode_paren"):
            out["decision_type_code"] = m.group("dcode_paren").strip()
            score = m.group("score_paren_nested") or m.group("score_paren")
            ftime = m.group("ftime_paren")
        else:
            out["decision_type_code"] = m.group("dcode").strip() if m.group("dcode") else None
            score = m.group("score")
            ftime = m.group("ftime")
        
        if ftime and out["decision_type"].startswith("fall"):
            out["fall_time"] = ftime
        elif ftime and "tech" in out["decision_type"]:
            # Tech fall can also have a time
            out["fall_time"] = ftime
        if score:
            try:
                wp, lp = score.split("-")
                out["winner_points"] = int(wp)
                out["loser_points"] = int(lp)
            except Exception:
                pass
        return _apply_name_team_conversions(out)

    # Dual meet simplified format: Winner (Team) over Loser (Team) <Decision> <score|time>
    # This format omits "won by" and just uses "over"
    # Try manual parsing first to handle nested parentheses in team names
    if " over " in rest.lower():
        over_pos = rest.lower().find(" over ")
        if over_pos > 0:
            # Parse winner (use FIRST parens since decision code might have parens too)
            winner_text = rest[:over_pos]
            winner_name, winner_team, _ = _parse_wrestler_team_first(winner_text)
            
            if winner_name:
                # Parse loser (use FIRST parens to avoid confusion with decision code parens)
                after_over = rest[over_pos + 6:].lstrip()  # Skip " over "
                loser_name, loser_team, loser_end = _parse_wrestler_team_first(after_over)
                
                if loser_name:
                    # Parse decision code and score/time
                    # Handles formats like: "TB-2 (Fall) 0:00", "Fall 3:34", "Dec 5-3", etc.
                    remaining = after_over[loser_end:].lstrip()
                    
                    # Try to extract decision code with optional parenthetical details and score/time
                    # Pattern: Code (Details) time/score OR Code time/score
                    code_match = _re.match(r'^([A-Za-z0-9-]+)(?:\s+\(([^)]+)\))?(?:\s+(\d+-\d+|\d+:\d+))?', remaining)
                    if code_match:
                        out["winner_name"] = winner_name
                        out["winner_team"] = winner_team or ""
                        out["loser_name"] = loser_name
                        out["loser_team"] = loser_team or ""
                        out["decision_type_code"] = code_match.group(1).strip()
                        
                        # If there's a parenthetical detail (like "Fall" in "TB-2 (Fall)"), use it for decision_type
                        paren_detail = code_match.group(2)
                        
                        # Infer decision_type from parenthetical detail or code
                        code_up = out["decision_type_code"].upper()
                        if paren_detail:
                            paren_up = paren_detail.upper()
                            if "FALL" in paren_up:
                                out["decision_type"] = "fall"
                            elif "DEC" in paren_up:
                                out["decision_type"] = "decision"
                            elif "MD" in paren_up or "MAJ" in paren_up:
                                out["decision_type"] = "major decision"
                            elif "TF" in paren_up or "TECH" in paren_up:
                                out["decision_type"] = "tech fall"
                            else:
                                out["decision_type"] = paren_detail.lower()
                        elif code_up in ("SV-1", "SV1"):
                            out["decision_type"] = "sudden victory"
                        elif "FALL" in code_up or code_up == "PIN":
                            out["decision_type"] = "fall"
                        elif code_up in ("MD", "MAJ"):
                            out["decision_type"] = "major decision"
                        elif code_up == "TF":
                            out["decision_type"] = "tech fall"
                        elif code_up in ("DEC", "D"):
                            out["decision_type"] = "decision"
                        elif code_up == "FORF":
                            out["decision_type"] = "forfeit"
                        elif code_up in ("OT", "TB-1", "TB-2", "UTB"):
                            out["decision_type"] = "overtime"
                        
                        # Capture score or fall time
                        if code_match.group(3):
                            if ':' in code_match.group(3):
                                # It's a time
                                if (out.get("decision_type") or "").startswith("fall"):
                                    out["fall_time"] = code_match.group(3)
                            else:
                                # It's a score
                                try:
                                    wp, lp = code_match.group(3).split("-")
                                    out["winner_points"] = int(wp)
                                    out["loser_points"] = int(lp)
                                except Exception:
                                    pass
                        
                        return _apply_name_team_conversions(out)
    
    # Fallback to regex for simple cases without nested parens
    m_simple = _re.search(
        r"^(?P<win>.+?)\s+\((?P<wteam>.*?)\)(?:\s+\d+-\d+)?\s+over\s+"
        r"(?P<lose>.+?)\s+\((?P<lteam>.*?)\)(?:\s+\d+-\d+)?\s+"
        r"(?P<dcode>[A-Za-z0-9-]+)(?:\s+(?P<score>\d+-\d+)|\s+(?P<ftime>\d+:\d+))?",
        rest,
        _re.I,
    )
    if m_simple:
        out["winner_name"] = m_simple.group("win").strip()
        out["winner_team"] = m_simple.group("wteam").strip()
        out["loser_name"] = m_simple.group("lose").strip()
        out["loser_team"] = m_simple.group("lteam").strip()
        out["decision_type_code"] = m_simple.group("dcode").strip()
        
        # Infer decision_type from code
        code_up = out["decision_type_code"].upper()
        if code_up in ("SV-1", "SV1"):
            out["decision_type"] = "sudden victory"
        elif "FALL" in code_up or code_up == "PIN":
            out["decision_type"] = "fall"
        elif code_up in ("MD", "MAJ"):
            out["decision_type"] = "major decision"
        elif code_up == "TF":
            out["decision_type"] = "tech fall"
        elif code_up in ("DEC", "D"):
            out["decision_type"] = "decision"
        elif code_up == "FORF":
            out["decision_type"] = "forfeit"
        elif code_up in ("OT", "TB-1", "UTB"):
            out["decision_type"] = "overtime"
        
        # Capture score or fall time
        score = m_simple.group("score")
        ftime = m_simple.group("ftime")
        if ftime and (out.get("decision_type") or "").startswith("fall"):
            out["fall_time"] = ftime
        elif score:
            try:
                wp, lp = score.split("-")
                out["winner_points"] = int(wp)
                out["loser_points"] = int(lp)
            except Exception:
                pass
        return _apply_name_team_conversions(out)

    # Variant: Winner (Team) won over Loser (Team) <CODE> <score|time>
    # Some entries omit the explicit decision phrase; we still capture code and numbers.
    # Try manual parsing first to handle nested parentheses in team names
    if " won over " in rest.lower():
        won_over_pos = rest.lower().find(" won over ")
        if won_over_pos > 0:
            # Parse winner (use FIRST parens to avoid confusion with decision code parens)
            winner_text = rest[:won_over_pos]
            winner_name, winner_team, _ = _parse_wrestler_team_first(winner_text)
            
            if winner_name:
                # Parse loser (use FIRST parens to avoid confusion with decision code parens)
                after_won_over = rest[won_over_pos + 10:].lstrip()  # Skip " won over "
                loser_name, loser_team, loser_end = _parse_wrestler_team_first(after_won_over)
                
                if loser_name:
                    # Parse decision code and score/time
                    remaining = after_won_over[loser_end:].lstrip()
                    
                    # Try to extract decision code and optional score/time
                    code_match = _re.match(r'^(\S+)(?:\s+(\d+-\d+|\d+:\d+))?', remaining)
                    if code_match:
                        out["winner_name"] = winner_name
                        out["winner_team"] = winner_team or ""
                        out["loser_name"] = loser_name
                        out["loser_team"] = loser_team or ""
                        out["decision_type_code"] = code_match.group(1).strip()
                        
                        # Infer a decision_type from common codes when possible
                        code_up = out["decision_type_code"].upper()
                        if code_up in ("SV-1", "SV1"):
                            out["decision_type"] = "sudden victory"
                        elif code_up in ("MD", "TF", "OT", "UTB"):
                            # We'll map these broadly; detailed type can still be read from code
                            mapping = {"MD": "major decision", "TF": "tech fall", "OT": "overtime", "UTB": "ultimate tiebreaker"}
                            out["decision_type"] = mapping.get(code_up, None)
                        elif code_up in ("FALL", "PIN"):
                            out["decision_type"] = "fall"
                        elif code_up == "DEC":
                            out["decision_type"] = "decision"
                        
                        # Capture score or fall time
                        if code_match.group(2):
                            if ':' in code_match.group(2):
                                # It's a time
                                if (out.get("decision_type") or "").startswith("fall"):
                                    out["fall_time"] = code_match.group(2)
                            else:
                                # It's a score
                                try:
                                    wp, lp = code_match.group(2).split("-")
                                    out["winner_points"] = int(wp)
                                    out["loser_points"] = int(lp)
                                except Exception:
                                    pass
                        
                        return _apply_name_team_conversions(out)
    
    # Fallback to regex for simple cases without nested parens
    m_over = _re.search(
        r"^(?P<win>.+?)\s+\((?P<wteam>.*?)\)(?:\s+\d+-\d+)?\s+won over\s+"
        r"(?P<lose>.+?)\s+\((?P<lteam>.*?)\)(?:\s+\d+-\d+)?\s+"
        r"(?P<dcode>\S+)(?:\s+(?P<score>\d+-\d+)|\s+(?P<ftime>\d+:\d+))?",
        rest,
        _re.I,
    )
    if m_over:
        out["winner_name"] = m_over.group("win").strip()
        out["winner_team"] = m_over.group("wteam").strip()
        out["loser_name"] = m_over.group("lose").strip()
        out["loser_team"] = m_over.group("lteam").strip()
        out["decision_type_code"] = m_over.group("dcode").strip()
        # Infer a decision_type from common codes when possible
        code_up = out["decision_type_code"].upper()
        if code_up in ("SV-1", "SV1"):
            out["decision_type"] = "sudden victory"
        elif code_up in ("MD", "TF", "OT", "UTB"):
            # We'll map these broadly; detailed type can still be read from code
            mapping = {"MD": "major decision", "TF": "tech fall", "OT": "overtime", "UTB": "ultimate tiebreaker"}
            out["decision_type"] = mapping.get(code_up, None)
        elif code_up in ("FALL", "PIN"):
            out["decision_type"] = "fall"
        elif code_up == "DEC":
            out["decision_type"] = "decision"
        # Capture score or fall time
        score = m_over.group("score")
        ftime = m_over.group("ftime")
        if ftime and (out.get("decision_type") or "").startswith("fall"):
            out["fall_time"] = ftime
        elif score:
            try:
                wp, lp = score.split("-")
                out["winner_points"] = int(wp)
                out["loser_points"] = int(lp)
            except Exception:
                pass
        return _apply_name_team_conversions(out)

    # Fallback minimal parse without code/score
    m2 = _re.search(r"^(?P<win>.+?)\s+\((?P<wteam>.*?)\)(?:\s+\d+-\d+)?\s+won by\s+(?P<dtype>.+?)\s+over\s+(?P<lose>.+?)\s+\((?P<lteam>.*?)\)(?:\s+\d+-\d+)?", rest, _re.I)
    if m2:
        out["winner_name"] = m2.group("win").strip()
        out["winner_team"] = m2.group("wteam").strip()
        out["decision_type"] = m2.group("dtype").strip().lower()
        out["loser_name"] = m2.group("lose").strip()
        out["loser_team"] = m2.group("lteam").strip()
        return _apply_name_team_conversions(out)

    return _apply_name_team_conversions(out)


def run(reparse: bool = False) -> None:
    # Custom logging handler that uses tqdm.write to avoid interfering with progress bar
    class TqdmLoggingHandler(logging.Handler):
        def emit(self, record):
            try:
                msg = self.format(record)
                tqdm.write(msg)
            except Exception:
                self.handleError(record)
    
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = TqdmLoggingHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    logger.addHandler(handler)

    conn = duckdb.connect(str(get_db_path()))
    ensure_schema(conn)

    rows = fetch_unparsed_round_html(conn, reparse=reparse)
    if not rows:
        logger.info("No unparsed round HTML found.")
        conn.close()
        return

    # If reparse is enabled, delete all matches and history once at the start for efficiency
    if reparse:
        matches_deleted, history_deleted = delete_all_matches(conn)
        logger.info("Deleted %d matches and %d wrestler_history records for reparse", matches_deleted, history_deleted)
        conn.commit()

    logger.info("Parsing %d rounds...", len(rows))
    
    for event_id, round_id, label, raw_html in tqdm(rows, desc="Parsing rounds", unit="round"):
        try:
            items: List[Tuple[str, str]] = parse_round_html(raw_html)
            saved = 0
            for weight_class, raw_li in items:
                # Extract plain text for structured parsing
                txt = _normalize_text(BeautifulSoup(raw_li, "html.parser").get_text(" "))
                fields = parse_match_text(txt)
                row = {
                    "event_id": event_id,
                    "round_id": round_id,
                    "weight_class": weight_class,
                    "raw_match_results": raw_li,
                    **fields,
                }
                insert_match(conn, row)
                saved += 1
            mark_parsed_ok(conn, event_id, round_id)
            conn.commit()  # Explicitly commit after each round
        except Exception as e:
            tqdm.write(f"WARNING: failed to parse event={event_id} round={round_id}: {e}")
            try:
                conn.rollback()  # Rollback on error if transaction is active
            except Exception:
                pass  # Ignore if no transaction is active
    
    conn.commit()  # Final commit
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Parse saved Round Results HTML from tournament_rounds.raw_html into a matches table."
    )
    parser.add_argument(
        "--reparse",
        action="store_true",
        help="Re-parse existing matches: delete all matches for each round before re-parsing the raw HTML"
    )
    args = parser.parse_args()
    run(reparse=args.reparse)
