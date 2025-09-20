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
    uv run python code/parse_round_html.py
"""

from __future__ import annotations

import logging
from typing import List, Optional, Dict, Any, Tuple
import re

from bs4 import BeautifulSoup, Tag
import duckdb


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


def fetch_unparsed_round_html(conn: duckdb.DuckDBPyConnection) -> List[tuple]:
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
    """Return a list of tuples: (weight_class, raw_li_html) for each match LI under tw-list.
    Parsing strategy: linear scan of section.tw-list children, track current <h2> as weight_class,
    and capture each <li> inside following <ul> elements as separate matches until the next <h2>.
    """
    soup = BeautifulSoup(raw_html or "", "html.parser")
    section = soup.select_one("section.tw-list, section[class~=tw-list]")
    results: List[tuple] = []
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
]

TEAM_CONVERSIONS_RAW: List[Tuple[str, str]] = [
    # (r"\bFt\.?\s*Belvoir\b", "Fort Belvoir"),
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


def _normalize_person_name(name: Optional[str]) -> Optional[str]:
    return _apply_conversions(name, NAME_CONVERSIONS)


def _normalize_team_name(team: Optional[str]) -> Optional[str]:
    return _apply_conversions(team, TEAM_CONVERSIONS)


def _apply_name_team_conversions(out: Dict[str, Any]) -> Dict[str, Any]:
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

    # Extract round detail prefix if present
    if " - " in text:
        rd, rest = text.split(" - ", 1)
        out["round_detail"] = rd.strip()
    else:
        rest = text

    # DFF (double forfeit) case: "A (Team) and B (Team) DFF"
    if "dff" in rest.lower():
        m = _re.search(r"^(?P<a>.+) \((?P<ateam>[^)]*)\)\s+and\s+(?P<b>.+) \((?P<bteam>[^)]*)\)\s+DFF\b", rest, _re.I)
        if m:
            # Store both participants; treat as a bye to skip Elo
            out["winner_name"] = m.group("a").strip()
            out["winner_team"] = m.group("ateam").strip()
            out["loser_name"] = m.group("b").strip()
            out["loser_team"] = m.group("bteam").strip()
            out["decision_type"] = "bye"
            out["decision_type_code"] = "DFF"
            out["bye"] = True
            return _apply_name_team_conversions(out)

    # Bye case
    if "received a bye" in rest.lower():
        m = _re.search(r"^(?P<win>.+) \((?P<wteam>[^)]*)\)\s+received a bye", rest, _re.I)
        if m:
            out["winner_name"] = m.group("win").strip()
            out["winner_team"] = m.group("wteam").strip()
        out["decision_type"] = "bye"
        out["decision_type_code"] = "Bye"
        out["bye"] = True
        return _apply_name_team_conversions(out)

    # "Won in <type>" cases (e.g., sudden victory - 1)
    m_in = _re.search(
        r"^(?P<win>(?:[^()]|\([^)]*\))+?)\s+\((?P<wteam>[^)]*)\)\s+won in\s+(?P<dtype>.+?)\s+over\s+"
        r"(?P<lose>(?:[^()]|\([^)]*\))+?)\s+\((?P<lteam>[^)]*)\)\s+"
        r"(?P<dcode>[A-Za-z0-9-]+)(?:\s+\((?P<dnote>[^)]+)\))?(?:\s+(?P<score>\d+-\d+)|\s+(?P<ftime>\d+:\d+))?",
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
        else:
            out["decision_type"] = dtype
        out["loser_name"] = m_in.group("lose").strip()
        out["loser_team"] = m_in.group("lteam").strip()
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

    # Normal or fall cases
    # Winner (Team) won by <decision_type> over Loser (Team) <CODE> <score|time>
    m = _re.search(
        r"^(?P<win>(?:[^()]|\([^)]*\))+?)\s+\((?P<wteam>[^)]*)\)\s+won by\s+(?P<dtype>.+?)\s+over\s+"
        r"(?P<lose>(?:[^()]|\([^)]*\))+?)\s+\((?P<lteam>[^)]*)\)\s+"
        r"(?P<dcode>[A-Za-z0-9-]+)(?:\s+\((?P<dnote>[^)]+)\))?(?:\s+(?P<score>\d+-\d+)|\s+(?P<ftime>\d+:\d+))?",
        rest,
        _re.I,
    )
    if m:
        out["winner_name"] = m.group("win").strip()
        out["winner_team"] = m.group("wteam").strip()
        out["decision_type"] = m.group("dtype").strip().lower()
        out["loser_name"] = m.group("lose").strip()
        out["loser_team"] = m.group("lteam").strip()
        out["decision_type_code"] = m.group("dcode").strip()
        score = m.group("score")
        ftime = m.group("ftime")
        if ftime and out["decision_type"].startswith("fall"):
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
    m_over = _re.search(
        r"^(?P<win>(?:[^()]|\([^)]*\))+?)\s+\((?P<wteam>[^)]*)\)\s+won over\s+"
        r"(?P<lose>(?:[^()]|\([^)]*\))+?)\s+\((?P<lteam>[^)]*)\)\s+"
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
    m2 = _re.search(r"^(?P<win>.+) \((?P<wteam>[^)]*)\)\s+won by\s+(?P<dtype>.+?)\s+over\s+(?P<lose>.+) \((?P<lteam>[^)]*)\)", rest, _re.I)
    if m2:
        out["winner_name"] = m2.group("win").strip()
        out["winner_team"] = m2.group("wteam").strip()
        out["decision_type"] = m2.group("dtype").strip().lower()
        out["loser_name"] = m2.group("lose").strip()
        out["loser_team"] = m2.group("lteam").strip()
        return _apply_name_team_conversions(out)

    return _apply_name_team_conversions(out)


def run() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logger = logging.getLogger(__name__)

    conn = duckdb.connect("output/scrape.db")
    ensure_schema(conn)

    rows = fetch_unparsed_round_html(conn)
    if not rows:
        logger.info("No unparsed round HTML found.")
        return

    for event_id, round_id, label, raw_html in rows:
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
            logger.info("parsed event=%s round=%s label=%s -> matches=%s", event_id, round_id, label, saved)
        except Exception as e:
            logger.warning("failed to parse event=%s round=%s: %s", event_id, round_id, e)


if __name__ == "__main__":
    run()
