"""
Import bout results from the USA Wrestling results site (USA Bracketing events).

TheMat.com/results embeds https://www.usawmembership.com/usaw_results, which serves
server-rendered HTML (no browser automation needed):
- Index:  /usaw_results?page=N            (newest first; Event Name, Event Date)
- Event:  /usaw_results/{id}/results?page=N  (Age Group, Weight, Round, Result)
- Event:  /usaw_results/{id}/placements?page=N  (Age Group, Place, Weight, Name)

Each result row is stored directly as one row in `matches` (no tournament_rounds HTML),
using event_id "usaw-{id}" so it cannot collide with TrackWrestling event IDs.
Placements are stored in `placements` using USA Wrestling's age-group and weight labels.
When a same-name/date event already exists from TrackWrestling, placements are attached
to that event ID without changing its bouts or Elo. Its weight_class labels may differ.
Events without placements are re-checked within the discovery window (or with --event-id).

Bout imports are skipped when:
- they were already imported (pass --replace to re-import specific --event-id values), or
- a tournament with the same name and date already exists from another source
  (e.g. TrackWrestling history), so Elo never counts the same bouts twice.
  With --supersede-incomplete, that other copy is replaced instead when USA Wrestling
  reproduces every stored bout exactly and has additional bouts.

Run with uv:
    uv run code/scrape_usaw_results.py --name-pattern '^NVWF\\b' --since 2026-07-01
    uv run code/scrape_usaw_results.py --event-id 19988 --dry-run   # parse + compare, no writes
    uv run code/scrape_usaw_results.py --event-id 19980 --supersede-incomplete
"""

from __future__ import annotations

import argparse
import logging
import re
import time
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterator, List, Optional

import httpx
import psycopg
from bs4 import BeautifulSoup

try:
    from .config import GOV_BODY
    from .db import ensure_schema, get_pg_connection, upsert_governing_body
    from .parse_round_html import (
        _apply_name_team_conversions,
        _normalize_person_name,
        _normalize_team_name,
        _normalize_text,
        insert_match,
    )
except ImportError:
    from config import GOV_BODY
    from db import ensure_schema, get_pg_connection, upsert_governing_body
    from parse_round_html import (
        _apply_name_team_conversions,
        _normalize_person_name,
        _normalize_team_name,
        _normalize_text,
        insert_match,
    )


BASE_URL = "https://www.usawmembership.com/usaw_results"
EVENT_ID_PREFIX = "usaw-"
USER_AGENT = "wrestling-stats (+https://github.com/jsl6906/wrestling-stats)"
REQUEST_DELAY_SECONDS = 0.3
MAX_INDEX_PAGES = 1000

log = logging.getLogger(__name__)

# Team names may contain one level of nested parentheses, e.g. "Club (VA)".
_TEAM = r"[^()]*(?:\([^()]*\)[^()]*)*"
_CODE = (
    r"Fall|F|Pin|Dec|Maj|MD|TF|SV-\d+|TB-\d+|\d+-OT|UTB|OT|FF|MFFL|MFF|For\.?|Forf\.?|"
    r"Inj\.?|Def\.?|DQ|DFF|DDQ|NC"
)
_RESULT_RE = re.compile(
    rf"^(?P<win>.+?)\s+\((?P<wteam>{_TEAM})\)\s+"
    rf"(?P<code>{_CODE})"
    r"(?:\s+(?P<num>\d+-\d+|\d+:\d+))?"
    r"(?:\s+\((?P<time>\d+:\d+)\))?"
    rf"\s+(?P<lose>.+?)\s+\((?P<lteam>{_TEAM})\)$",
    re.I,
)
# Some events (e.g. NYSPHSAA) wrap the result in parentheses:
# "Winner (Club) (TF 15-0 3:12) Loser (Club)", "(TF-1.5 4:00 (17-2))", "(For.)", "(Inj. [time])".
_PAREN_CODE = (
    r"(?:TF|SV|TB)-\d+(?:\.\d+)?|Fall|Pin|Dec|Maj|MD|TF|UTB|OT|FF|MFF|M\.\s?For\.|For\.|"
    r"Inj\.|Def\.|DQ|DFF|DDQ|NC"
)
_PAREN_RESULT_RE = re.compile(
    rf"^(?P<win>.+?)\s+\((?P<wteam>{_TEAM})\)\s+"
    rf"\((?P<code>{_PAREN_CODE})"
    r"(?:\s+(?P<num>\d+-\d+|\d+:\d+|\[time\]))?"
    r"(?:\s+(?P<time>\d+:\d+))?"
    r"(?:\s+\((?P<score>\d+-\d+)\))?\)"
    rf"\s+(?P<lose>.+?)\s+\((?P<lteam>{_TEAM})\)$",
    re.I,
)
# Parenthesized codes TrackWrestling stores verbatim rather than normalized.
_VERBATIM_CODES = {"FOR.": ("forfeit", "For."), "M. FOR.": ("medical forfeit", "M. For."), "M.FOR.": ("medical forfeit", "M. For.")}

# Codes and decision types mirror what parse_round_html.py stores for TrackWrestling.
_CODE_MAP = {
    "FALL": ("fall", "Fall"),
    "F": ("fall", "Fall"),
    "PIN": ("fall", "Fall"),
    "DEC": ("decision", "Dec"),
    "MAJ": ("major decision", "Maj"),
    "MD": ("major decision", "Maj"),
    "TF": ("tech fall", "TF"),
    "UTB": ("the ultimate tie breaker", "UTB"),
    "OT": ("overtime", "OT"),
    "FF": ("forfeit", "FF"),
    "FOR": ("forfeit", "FF"),
    "FORF": ("forfeit", "FF"),
    "MFF": ("medical forfeit", "MFF"),
    "MFFL": ("medical forfeit", "MFFL"),
    "INJ": ("injury default", "Inj."),
    "DEF": ("default", "Def."),
    "DQ": ("disqualification", "DQ"),
    "DFF": ("double forfeit", "DFF"),
    "DDQ": ("double disqualification", "DDQ"),
    "NC": ("no contest", "NC"),
}
# Results that do not represent a contested bout and must not affect Elo.
_NON_BOUT_CODES = {"DFF", "DDQ", "NC"}


def parse_usaw_result(text: str) -> Dict[str, Any]:
    """Parse 'Winner (Club) <CODE> [score|time] Loser (Club)' into match fields.

    Unrecognized text returns empty participant fields (Elo skips rows without names).
    """
    out: Dict[str, Any] = {
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
    text = _normalize_text(text or "")
    m = _RESULT_RE.match(text) or _PAREN_RESULT_RE.match(text)
    if not m:
        return out

    raw_code = m.group("code")
    key = raw_code.upper().rstrip(".")
    if raw_code.upper() in _VERBATIM_CODES:
        decision_type, code = _VERBATIM_CODES[raw_code.upper()]
    elif key.startswith("TF-"):
        decision_type, code = "tech fall", key
    elif key.startswith("SV-"):
        decision_type, code = "sudden victory", key
    elif key.startswith("TB-"):
        decision_type, code = f"tie breaker - {key[3:].split('.')[0]}", key
    elif key.endswith("-OT"):
        decision_type, code = "overtime", key
    else:
        decision_type, code = _CODE_MAP[key]

    out.update(
        winner_name=m.group("win").strip(),
        winner_team=m.group("wteam").strip(),
        loser_name=m.group("lose").strip(),
        loser_team=m.group("lteam").strip(),
        decision_type=decision_type,
        decision_type_code=code,
    )
    num = m.group("num")
    score = m.groupdict().get("score")
    if num and "-" in num:
        score = num
    elif num and ":" in num:
        # A leading time is the fall/stoppage time; a time after a score (TF 15-0 3:12) is not stored.
        out["fall_time"] = num
    if score:
        wp, lp = score.split("-")
        out["winner_points"], out["loser_points"] = int(wp), int(lp)

    out = _apply_name_team_conversions(out)
    if code in _NON_BOUT_CODES:
        out["bye"] = True
    return out


@dataclass
class UsawEvent:
    usaw_id: int
    name: str
    start_date: date

    @property
    def event_id(self) -> str:
        return f"{EVENT_ID_PREFIX}{self.usaw_id}"


class UsawClient:
    def __init__(self, delay: float = REQUEST_DELAY_SECONDS) -> None:
        self._client = httpx.Client(
            timeout=30,
            headers={"User-Agent": USER_AGENT},
            follow_redirects=True,
            transport=httpx.HTTPTransport(retries=3),
        )
        self._delay = delay

    def close(self) -> None:
        self._client.close()

    def _get(self, url: str, page: int) -> BeautifulSoup:
        resp = self._client.get(url, params={"page": page})
        resp.raise_for_status()
        time.sleep(self._delay)
        return BeautifulSoup(resp.text, "html.parser")

    def iter_index(self) -> Iterator[UsawEvent]:
        """Yield events from the results index, newest first."""
        for page in range(1, MAX_INDEX_PAGES + 1):
            soup = self._get(BASE_URL, page)
            events = list(_parse_index(soup))
            if not events:
                return
            yield from events

    def fetch_event(self, usaw_id: int) -> tuple[Optional[str], List[List[str]]]:
        """Return (title, rows) for every results page of an event (Age Group, Weight, Round, Result)."""
        return self._fetch_table(usaw_id, "results")

    def fetch_placements(self, usaw_id: int) -> List[List[str]]:
        """Return rows for every placements page of an event (Age Group, Place, Weight, Name)."""
        return self._fetch_table(usaw_id, "placements")[1]

    def _fetch_table(self, usaw_id: int, kind: str) -> tuple[Optional[str], List[List[str]]]:
        url = f"{BASE_URL}/{usaw_id}/{kind}"
        title: Optional[str] = None
        rows: List[List[str]] = []
        page = 1
        while True:
            soup = self._get(url, page)
            if title is None:
                h3 = soup.find("h3")
                title = h3.get_text(" ", strip=True) if h3 else None
            for tr in soup.select("table tbody tr"):
                cells = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
                if len(cells) == 4:
                    rows.append(cells)
            if not soup.find("a", href=re.compile(rf"[?&]page={page + 1}\b")):
                return title, rows
            page += 1


def _parse_index(soup: BeautifulSoup) -> Iterator[UsawEvent]:
    for link in soup.find_all("a", href=re.compile(r"/usaw_results/\d+/results$")):
        tr = link.find_parent("tr")
        cells = tr.find_all("td") if tr else []
        if len(cells) < 2:
            continue
        try:
            start = datetime.strptime(cells[1].get_text(strip=True), "%m/%d/%Y").date()
        except ValueError:
            continue
        usaw_id = int(re.search(r"/usaw_results/(\d+)/", link["href"]).group(1))
        yield UsawEvent(usaw_id, cells[0].get_text(" ", strip=True), start)


def _parse_title(title: Optional[str]) -> tuple[Optional[str], Optional[date]]:
    """Split 'Event Name - MM/DD/YYYY Results' into name and date."""
    if not title:
        return None, None
    m = re.match(r"^(?P<name>.+?)\s+-\s+(?P<date>\d{2}/\d{2}/\d{4})\b", title)
    if not m:
        return title, None
    return m.group("name"), datetime.strptime(m.group("date"), "%m/%d/%Y").date()


def build_match_rows(event_id: str, rows: List[List[str]]) -> List[Dict[str, Any]]:
    out = []
    for age_group, weight, round_label, result in rows:
        if not round_label:
            # Older events fold the round into the weight cell: "120 Champ. Round 2".
            m = re.match(r"^(\S+)\s+(\D.*)$", weight or "")
            if m:
                weight, round_label = m.group(1), m.group(2)
        out.append(
            {
                "event_id": event_id,
                "round_id": round_label or "Results",
                "weight_class": " ".join(p for p in (age_group, weight) if p),
                "raw_match_results": result,
                "round_detail": round_label or None,
                **parse_usaw_result(result),
            }
        )
    return out


_PLACEMENT_NAME_RE = re.compile(rf"^(?P<name>.+?)\s+\((?P<team>{_TEAM})\)$")


def parse_placement_name(text: str) -> tuple[Optional[str], Optional[str]]:
    """Split 'Name (Team)' into a normalized (name, team), matching how bout participants are stored."""
    text = _normalize_text(text or "")
    if not text:
        return None, None
    m = _PLACEMENT_NAME_RE.match(text)
    name, team = (m.group("name"), m.group("team").strip() or None) if m else (text, None)
    return _normalize_person_name(name), _normalize_team_name(team)


def build_placement_rows(event_id: str, rows: List[List[str]]) -> List[Dict[str, Any]]:
    out = []
    for age_group, place, weight, raw_name in rows:
        name, team = parse_placement_name(raw_name)
        place = place.strip()
        out.append(
            {
                "event_id": event_id,
                "age_group": age_group or None,
                "weight": weight or None,
                "weight_class": " ".join(p for p in (age_group, weight) if p) or None,
                "place": int(place) if place.isdigit() else None,
                "name": name,
                "team": team,
                "raw_placement": raw_name,
            }
        )
    out.sort(key=lambda p: (p["weight_class"] or "", p["place"] is None, p["place"] or 0))
    return out


def _norm_event_name(name: str) -> str:
    return re.sub(r"\s+", " ", name).strip().lower()


def find_existing_event(conn: psycopg.Connection, event: UsawEvent) -> Optional[str]:
    """Return an event_id already holding this event (same ID, or same name/date from another source)."""
    row = conn.execute(
        """--sql
        SELECT t.event_id
        FROM tournaments t
        WHERE t.gov_body = %s
          AND (t.event_id = %s OR (t.start_date = %s AND lower(regexp_replace(trim(t.name), '\\s+', ' ', 'g')) = %s))
          AND EXISTS (SELECT 1 FROM matches m WHERE m.gov_body = t.gov_body AND m.event_id = t.event_id)
        ORDER BY (t.event_id = %s) DESC
        LIMIT 1
        """,
        [GOV_BODY, event.event_id, event.start_date, _norm_event_name(event.name), event.event_id],
    ).fetchone()
    return row[0] if row else None


def find_overlapping_event(
    conn: psycopg.Connection, event: UsawEvent, matches: List[Dict[str, Any]]
) -> Optional[str]:
    """Return a same-date event from another source sharing most bouts, even if its name differs."""
    pairs = {(m["winner_name"], m["loser_name"]) for m in matches if m.get("winner_name") and m.get("loser_name")}
    if not pairs:
        return None
    rows = conn.execute(
        """--sql
        SELECT m.event_id, m.winner_name, m.loser_name
        FROM matches m JOIN tournaments t ON t.gov_body = m.gov_body AND t.event_id = m.event_id
        WHERE m.gov_body = %s AND m.event_id <> %s
          AND t.start_date BETWEEN %s AND %s
          AND m.winner_name IS NOT NULL AND m.loser_name IS NOT NULL
        """,
        [GOV_BODY, event.event_id, event.start_date - timedelta(days=1), event.start_date + timedelta(days=1)],
    ).fetchall()
    shared = Counter(eid for eid, w, l in rows if (w, l) in pairs)
    if not shared:
        return None
    event_id, count = shared.most_common(1)[0]
    return event_id if count >= 0.5 * len(pairs) else None


def save_event(
    conn: psycopg.Connection,
    event: UsawEvent,
    matches: List[Dict[str, Any]],
    supersede_event_id: Optional[str] = None,
    placements: Optional[List[Dict[str, Any]]] = None,
) -> None:
    """Replace this event's tournament, rounds, matches and placements in a single transaction.

    supersede_event_id removes an incomplete copy of the event loaded from another source.
    The TrackWrestling scraper skips events whose name and date match a USA Wrestling import,
    so a superseded event is not scraped again.
    """
    with conn.transaction():
        if supersede_event_id:
            # placements cascade from tournaments
            for table in ("matches", "tournament_rounds", "tournaments"):
                conn.execute(
                    f"DELETE FROM {table} WHERE gov_body = %s AND event_id = %s",
                    [GOV_BODY, supersede_event_id],
                )
        conn.execute(
            """--sql
            INSERT INTO tournaments (gov_body, event_id, name, year, start_date, end_date, event_type_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (gov_body, event_id) DO UPDATE SET
                name = EXCLUDED.name,
                year = EXCLUDED.year,
                start_date = EXCLUDED.start_date,
                end_date = EXCLUDED.end_date,
                event_type_name = EXCLUDED.event_type_name
            """,
            [GOV_BODY, event.event_id, event.name, event.start_date.year, event.start_date,
             event.start_date, "USA Wrestling Results"],
        )
        conn.execute("DELETE FROM matches WHERE gov_body = %s AND event_id = %s", [GOV_BODY, event.event_id])
        conn.execute("DELETE FROM tournament_rounds WHERE gov_body = %s AND event_id = %s", [GOV_BODY, event.event_id])
        # Rounds without raw_html keep the tournament out of the TrackWrestling orphan cleanup
        # and are never picked up by parse_round_html.py.
        for round_id in dict.fromkeys(m["round_id"] for m in matches):
            conn.execute(
                """--sql
                INSERT INTO tournament_rounds (gov_body, event_id, round_id, label, raw_html, parsed_ok)
                VALUES (%s, %s, %s, %s, NULL, TRUE)
                """,
                [GOV_BODY, event.event_id, round_id, round_id],
            )
        for row in matches:
            insert_match(conn, row)
        save_placements(conn, event.event_id, placements or [])


def save_placements(conn: psycopg.Connection, event_id: str, placements: List[Dict[str, Any]]) -> None:
    """Replace an event's placements (the event's tournament row must already exist)."""
    with conn.transaction():
        conn.execute("DELETE FROM placements WHERE gov_body = %s AND event_id = %s", [GOV_BODY, event_id])
        with conn.cursor() as cur:
            cur.executemany(
                """--sql
                INSERT INTO placements (gov_body, event_id, age_group, weight, weight_class, place, name, team, raw_placement)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    [GOV_BODY, event_id, p["age_group"], p["weight"], p["weight_class"], p["place"],
                     p["name"], p["team"], p["raw_placement"]]
                    for p in placements
                ],
            )


def has_placements(conn: psycopg.Connection, event_id: str) -> bool:
    # A dry run does not apply the schema, so the table may not exist yet.
    if conn.execute("SELECT to_regclass('placements') IS NULL").fetchone()[0]:
        return False
    return conn.execute(
        "SELECT EXISTS (SELECT 1 FROM placements WHERE gov_body = %s AND event_id = %s)",
        [GOV_BODY, event_id],
    ).fetchone()[0]


def _log_placements(placements: List[Dict[str, Any]]) -> int:
    """Log a placements summary; return the number of rows without a numeric place or name."""
    bad = sum(1 for p in placements if p["place"] is None or not p["name"])
    brackets = len({p["weight_class"] for p in placements})
    log.info("  placements: %d in %d brackets, %d unparsed", len(placements), brackets, bad)
    return bad


_COMPARE_FIELDS = (
    "winner_name", "winner_team", "loser_name", "loser_team", "decision_type",
    "decision_type_code", "winner_points", "loser_points", "fall_time",
)


def compare_with_existing(
    conn: psycopg.Connection, existing_event_id: str, matches: List[Dict[str, Any]]
) -> tuple[int, int]:
    """Compare parsed bouts with an existing event; log counts only.

    Returns (bouts only in the existing event, bouts only in the parsed results).
    """
    cols = ", ".join(("weight_class", "round_detail") + _COMPARE_FIELDS)
    db_rows = conn.execute(
        f"""--sql
        SELECT {cols} FROM matches
        WHERE gov_body = %s AND event_id = %s AND COALESCE(bye, FALSE) = FALSE
        """,
        [GOV_BODY, existing_event_id],
    ).fetchall()
    keys = ("weight_class", "round_detail") + _COMPARE_FIELDS
    # Age-group labels differ between sources ("Varsity 120" vs "High School Varsity Girls 120"),
    # so compare on the trailing weight token only.
    def _key(row: tuple) -> tuple:
        weight = (row[0] or "").split()
        return (weight[-1] if weight else None,) + tuple(row[1:])

    existing = Counter(_key(tuple(r)) for r in db_rows)
    parsed = Counter(_key(tuple(m.get(k) for k in keys)) for m in matches if not m.get("bye"))
    only_existing = existing - parsed
    only_parsed = parsed - existing

    log.info(
        "  compare vs %s: existing=%d parsed=%d identical=%d only_existing=%d only_parsed=%d",
        existing_event_id, sum(existing.values()), sum(parsed.values()),
        sum((existing & parsed).values()), sum(only_existing.values()), sum(only_parsed.values()),
    )
    # Pair leftovers by bout position to show which fields differ, without printing names.
    by_pos = {(r[0], r[1]): r for r in only_existing.elements()}
    field_diffs: Counter = Counter()
    for r in only_parsed.elements():
        other = by_pos.get((r[0], r[1]))
        if other is None:
            field_diffs["<unpaired>"] += 1
            continue
        for i, field in enumerate(_COMPARE_FIELDS, start=2):
            if r[i] != other[i]:
                field_diffs[field] += 1
    if field_diffs:
        log.info("  differing fields: %s", dict(field_diffs))
    return sum(only_existing.values()), sum(only_parsed.values())


def backfill_placements(
    client: UsawClient,
    conn: psycopg.Connection,
    ev: UsawEvent,
    dry_run: bool,
    target_event_id: Optional[str] = None,
) -> int:
    """Load missing placements under an existing event ID, without changing its bouts or Elo."""
    event_id = target_event_id or ev.event_id
    if has_placements(conn, event_id):
        log.info("  placements already loaded")
        return 0
    placements = build_placement_rows(event_id, client.fetch_placements(ev.usaw_id))
    if not placements:
        log.info("  no placements posted yet")
        return 0
    bad = _log_placements(placements)
    if dry_run:
        log.info("  dry run: not writing")
    else:
        save_placements(conn, event_id, placements)
        log.info("  saved %d placements under %s", len(placements), event_id)
    return 1 if bad else 0


def run(
    *,
    name_pattern: Optional[str],
    since: Optional[date],
    event_ids: List[int],
    dry_run: bool,
    replace: bool,
    supersede_incomplete: bool = False,
    supersede: bool = False,
    replaced_flag_file: Optional[str] = None,
) -> int:
    if supersede and not event_ids:
        raise SystemExit("--supersede requires explicit --event-id values.")
    if not event_ids and not name_pattern:
        raise SystemExit("Provide --name-pattern for discovery or one or more --event-id values.")

    client = UsawClient()
    conn = get_pg_connection(autocommit=True)
    failures = 0
    try:
        if not dry_run:
            ensure_schema(conn)
            upsert_governing_body(conn)

        events: List[UsawEvent] = []
        # Already-imported events that have no placements stored yet (placements are often posted later).
        placement_backfill: List[tuple[UsawEvent, str]] = []
        if event_ids:
            events = [UsawEvent(usaw_id, "", date.min) for usaw_id in event_ids]
        else:
            pattern = re.compile(name_pattern, re.I)
            for ev in client.iter_index():
                if since and ev.start_date < since:
                    break
                if not pattern.search(ev.name) or ev.start_date > date.today():
                    continue
                existing = find_existing_event(conn, ev)
                # Events loaded from another source are still fetched when comparing or superseding.
                fetch_other_source = dry_run or supersede_incomplete
                if existing and not replace and (existing == ev.event_id or not fetch_other_source):
                    log.info("%s %s (%s): already loaded as %s", ev.event_id, ev.name, ev.start_date, existing)
                    if not has_placements(conn, existing):
                        placement_backfill.append((ev, existing))
                    continue
                events.append(ev)
        log.info(
            "Found %d USA Wrestling event(s) to process, %d needing placements only",
            len(events), len(placement_backfill),
        )

        for ev, existing in placement_backfill:
            log.info("%s %s (%s): fetching placements", ev.event_id, ev.name, ev.start_date)
            failures += backfill_placements(client, conn, ev, dry_run, target_event_id=existing)

        for ev in events:
            title, rows = client.fetch_event(ev.usaw_id)
            name, start = _parse_title(title)
            ev.name = ev.name or name or f"USAW event {ev.usaw_id}"
            if ev.start_date == date.min:
                if start is None:
                    log.warning("Skipping %s: could not determine event date", ev.event_id)
                    failures += 1
                    continue
                ev.start_date = start

            if not rows:
                log.info("%s %s (%s): no results posted yet", ev.event_id, ev.name, ev.start_date)
                continue

            matches = build_match_rows(ev.event_id, rows)
            unparsed = sum(1 for m in matches if not m["winner_name"] and not m["bye"])
            codes = Counter(m["decision_type_code"] for m in matches)
            log.info(
                "%s %s (%s): %d bouts, %d unparsed, codes=%s",
                ev.event_id, ev.name, ev.start_date, len(matches), unparsed, dict(codes.most_common()),
            )
            if unparsed:
                failures += 1
                for m in matches:
                    if not m["winner_name"] and not m["bye"]:
                        log.warning("  unparsed result format: %s", re.sub(r"[A-Za-z]+", "w", m["raw_match_results"]))

            existing = find_existing_event(conn, ev)
            if not existing:
                overlap = find_overlapping_event(conn, ev, matches)
                if overlap:
                    # Never supersede here: the TrackWrestling duplicate check matches on name/date only.
                    log.info("  skipping: bouts overlap existing event %s (different name)", overlap)
                    continue
            supersede_id: Optional[str] = None
            if existing and existing != ev.event_id:
                only_existing, only_parsed = compare_with_existing(conn, existing, matches)
                # Only replace when every stored bout is reproduced exactly and USA Wrestling has more.
                if supersede_incomplete and not unparsed and only_existing == 0 and only_parsed > 0:
                    supersede_id = existing
                    log.info("  %s is missing %d bout(s); superseding it", existing, only_parsed)
                elif supersede and not unparsed:
                    supersede_id = existing
                    log.info("  superseding %s as requested", existing)
                else:
                    log.info("  skipping bouts: already loaded as %s", existing)
                    failures += backfill_placements(client, conn, ev, dry_run, target_event_id=existing)
                    continue
            elif existing and not replace:
                log.info("  skipping bouts: already imported (use --replace to re-import)")
                failures += backfill_placements(client, conn, ev, dry_run)
                continue

            placements = build_placement_rows(ev.event_id, client.fetch_placements(ev.usaw_id))
            if _log_placements(placements):
                failures += 1
            if dry_run:
                log.info("  dry run: not writing")
                continue

            save_event(conn, ev, matches, supersede_event_id=supersede_id, placements=placements)
            log.info("  saved %d bouts, %d placements", len(matches), len(placements))
            if existing:
                log.warning("  replaced existing matches; run calculate_elo.py --recalculate")
                if replaced_flag_file:
                    with open(replaced_flag_file, "a", encoding="utf-8") as fh:
                        fh.write(f"{ev.event_id} replaced {existing}\n")
    finally:
        conn.close()
        client.close()
    return failures


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def main() -> None:
    parser = argparse.ArgumentParser(description="Import USA Wrestling (USA Bracketing) results into matches.")
    parser.add_argument("--name-pattern", help="Regex matched against event names in the results index")
    parser.add_argument("--since", type=_parse_date, help="Only events on/after this date (YYYY-MM-DD)")
    parser.add_argument("--lookback-weeks", type=int, help="Only events within this many weeks (combined with --since)")
    parser.add_argument("--event-id", type=int, action="append", default=[], help="USA Wrestling results event ID")
    parser.add_argument("--dry-run", action="store_true", help="Parse and compare without writing")
    parser.add_argument("--replace", action="store_true", help="Re-import events already imported from USA Wrestling")
    parser.add_argument(
        "--supersede-incomplete",
        action="store_true",
        help="Replace an event loaded from another source when USA Wrestling has all of its bouts plus more",
    )
    parser.add_argument(
        "--supersede",
        action="store_true",
        help="With --event-id: replace a same-name/date event from another source even if bouts differ",
    )
    parser.add_argument(
        "--replaced-flag-file",
        help="Append a line to this file whenever existing matches are replaced (Elo needs --recalculate)",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    since = args.since
    if args.lookback_weeks is not None:
        lookback = date.today() - timedelta(weeks=args.lookback_weeks)
        since = max(since, lookback) if since else lookback

    failures = run(
        name_pattern=args.name_pattern,
        since=since,
        event_ids=args.event_id,
        dry_run=args.dry_run,
        replace=args.replace,
        supersede_incomplete=args.supersede_incomplete,
        supersede=args.supersede,
        replaced_flag_file=args.replaced_flag_file,
    )
    if failures:
        log.warning("%d event(s) had unparsed rows or errors", failures)


if __name__ == "__main__":
    main()
