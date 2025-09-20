from __future__ import annotations

import re
import logging
from urllib.parse import urlparse, parse_qs, urlencode
import time
from typing import List, Optional, Tuple, Any

import duckdb

# Shared constants
BASE_SEARCH_URL = (
    "https://www.trackwrestling.com/Login.jsp?tName=NVWF&state=&sDate=&eDate=&lastName=&firstName="
    "&teamName=&sfvString=&city=&gbId=&camps=false"
)

# Module logger
logger = logging.getLogger(__name__)


def extract_event_id(js_call: str) -> Optional[str]:
    m = re.search(r"eventSelected\((\d+),", js_call)
    return m.group(1) if m else None


def ensure_rounds_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """--sql
        CREATE TABLE IF NOT EXISTS tournament_rounds (
            event_id TEXT,
            round_id TEXT,
            label TEXT,
            raw_html TEXT,
            parsed_ok BOOLEAN,
            first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (event_id, round_id)
        );
        """
    )


def upsert_round(conn: duckdb.DuckDBPyConnection, event_id: str, round_id: str, label: str, raw_html: Optional[str] = None) -> None:
    # Insert or update label and optionally raw_html
    if raw_html is None:
        conn.execute(
            """--sql
            INSERT INTO tournament_rounds AS tr (event_id, round_id, label)
            VALUES (?, ?, ?)
            ON CONFLICT (event_id, round_id) DO UPDATE SET
                label = EXCLUDED.label
            """,
            [event_id, round_id, label],
        )
    else:
        conn.execute(
            """--sql
            INSERT INTO tournament_rounds AS tr (event_id, round_id, label, raw_html)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (event_id, round_id) DO UPDATE SET
                label = EXCLUDED.label,
                raw_html = EXCLUDED.raw_html
            """,
            [event_id, round_id, label, raw_html],
        )


# Playwright helpers (all use sync API objects passed in)
def find_tournament_frame(page: Any) -> Optional[Any]:
    for fr in page.frames:
        try:
            cnt = fr.locator('[href*="eventSelected("], [onclick*="eventSelected("]').count()
            if cnt and cnt > 0:
                return fr
        except Exception:
            continue
    return None


def wait_for_tournament_frame(page: Any, timeout_ms: int = 8000) -> Optional[Any]:
    start = time.time()
    while (time.time() - start) * 1000 < timeout_ms:
        fr = find_tournament_frame(page)
        if fr is not None:
            return fr
        time.sleep(0.25)
    return None


def page_event_ids(frame: Any) -> List[str]:
    ids: List[str] = []
    for a in frame.locator('[href*="eventSelected("], [onclick*="eventSelected("]').element_handles():
        js = (a.get_attribute('href') or a.get_attribute('onclick') or '')
        eid = extract_event_id(js or '')
        if eid:
            ids.append(eid)
    return ids


def click_next(frame: Any, page: Any) -> bool:
    ul = frame.query_selector('ul.tournament-ul')
    before_html = ul.inner_html() if ul else None
    before_ids = page_event_ids(frame)
    advanced = False
    try:
        next_all = frame.locator('a[href="javascript:nextTournaments()"], [onclick^="nextTournaments"]').element_handles()
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
                    nh.scroll_into_view_if_needed()
                    nh.click()
                    clicked = True
                    break
                except Exception:
                    continue
        if not clicked:
            # Try invoking nextTournaments in frame, then on the main page
            tried = False
            try:
                frame.evaluate("nextTournaments()")
                tried = True
            except Exception:
                pass
            if not tried:
                try:
                    page.evaluate("nextTournaments()")
                except Exception:
                    pass
        time.sleep(0.5)
        start = time.time()
        while (time.time() - start) < 10.0:
            fr = wait_for_tournament_frame(page)
            if fr is None:
                break
            ul2 = fr.query_selector('ul.tournament-ul')
            curr_ids = page_event_ids(fr)
            if ul and ul2:
                curr = ul2.inner_html()
                if (before_html is not None and curr != before_html) or (curr_ids and curr_ids != before_ids):
                    advanced = True
                    break
            time.sleep(0.25)
    except Exception:
        advanced = False
    # As a last resort, if we failed to detect advancement but the list vanished, try reloading the list URL
    if not advanced:
        try:
            fr = find_tournament_frame(page)
            if fr is None:
                page.goto(BASE_SEARCH_URL, wait_until="domcontentloaded")
                return wait_for_tournament_frame(page) is not None
        except Exception:
            pass
    return advanced


def return_to_list(page: Any, timeout_ms: int = 8000) -> bool:
    for _ in range(5):
        try:
            fr = find_tournament_frame(page)
            if fr is not None:
                return True
            page.go_back(wait_until="domcontentloaded")
            time.sleep(0.3)
        except Exception:
            pass
    try:
        page.goto(BASE_SEARCH_URL, wait_until="domcontentloaded")
        return wait_for_tournament_frame(page, timeout_ms) is not None
    except Exception:
        return False


def open_event_by_id(page: Any, event_id: str) -> bool:
    try:
        logger.debug("locating event %s from tournaments list; current url=%s", event_id, getattr(page, 'url', None))
    except Exception:
        pass
    seen_pages = 0
    while True:
        list_frame = wait_for_tournament_frame(page)
        if list_frame is None:
            logger.debug("no tournaments frame; attempting to return to list before searching for %s", event_id)
            try:
                if not return_to_list(page):
                    logger.warning("tournaments frame not found while searching for %s", event_id)
                    return False
                # After returning to list, try again
                list_frame = wait_for_tournament_frame(page)
                if list_frame is None:
                    logger.warning("still no tournaments frame after return; %s", event_id)
                    return False
            except Exception:
                logger.warning("exception trying to return to list for %s", event_id)
                return False
        anchor = list_frame.locator(
            f'a[href*="eventSelected({event_id},"], [onclick*="eventSelected({event_id},"]'
        ).first
        cnt = 0
        try:
            cnt = anchor.count()
        except Exception:
            cnt = 0
        if cnt > 0:
            try:
                logger.debug("found event %s anchor; clicking", event_id)
                anchor.scroll_into_view_if_needed()
                anchor.click()
                logger.debug("clicked event %s; new url=%s", event_id, getattr(page, 'url', None))
                return True
            except Exception:
                logger.warning("failed clicking event %s anchor", event_id)
                return False
        logger.debug("event %s not on this page; attempting nextTournaments()", event_id)
        if not click_next(list_frame, page):
            logger.info("nextTournaments() did not advance while searching for %s", event_id)
            return False
        seen_pages += 1
        if seen_pages > 200:
            logger.warning("exceeded page search limit for %s", event_id)
            return False


def enter_event(page: Any) -> bool:
    from playwright.sync_api import TimeoutError as PWTimeout

    try:
        logger.debug("attempting 'Enter Event' by role button; url=%s", getattr(page, 'url', None))
        page.get_by_role("button", name="Enter Event").click(timeout=5000)
        logger.debug("entered event via button; url=%s", getattr(page, 'url', None))
        return True
    except PWTimeout:
        try:
            logger.debug("attempting 'Enter Event' by input[value=Enter Event]")
            page.locator('input[type="button"][value="Enter Event"]').first.click(timeout=5000)
            logger.debug("entered event via input button; url=%s", getattr(page, 'url', None))
            return True
        except Exception:
            logger.warning("failed to click 'Enter Event'")
            return False


def goto_round_results(page: Any) -> bool:
    # 1) Look for direct anchor to RoundResults.jsp on the page
    try:
        link = page.locator('a[href*="RoundResults.jsp"]').first
        cnt = link.count()
        logger.debug("RoundResults.jsp anchor on page count=%s", cnt)
        if cnt > 0:
            href = link.get_attribute('href')
            if href:
                try:
                    logger.debug("navigating to RoundResults via href: %s", href)
                    page.goto(href, wait_until="domcontentloaded", timeout=8000)
                    logger.debug("navigated to RoundResults; url=%s", getattr(page, 'url', None))
                    return True
                except Exception:
                    pass
            try:
                logger.debug("clicking RoundResults anchor")
                link.click(timeout=4000)
                logger.debug("clicked RoundResults anchor; url=%s", getattr(page, 'url', None))
                return True
            except Exception:
                pass
    except Exception:
        pass
    # 2) Search frames for anchor and navigate using its href (preserves session params)
    try:
        for fr in page.frames:
            try:
                l2 = fr.locator('a[href*="RoundResults.jsp"]').first
                c2 = l2.count()
                if c2 > 0:
                    href = l2.get_attribute('href')
                    if href:
                        try:
                            logger.debug("navigating (frame) to RoundResults via href: %s", href)
                            page.goto(href, wait_until="domcontentloaded", timeout=8000)
                            logger.debug("navigated to Round Results (frame href); url=%s", getattr(page, 'url', None))
                            return True
                        except Exception:
                            pass
                    try:
                        logger.debug("clicking RoundResults anchor in frame")
                        l2.click(timeout=4000)
                        logger.debug("clicked RoundResults in frame; url=%s", getattr(page, 'url', None))
                        return True
                    except Exception:
                        continue
            except Exception:
                continue
    except Exception:
        pass
    # 3) Try link text by role/text as a last attempt
    try:
        logger.debug("trying Round Results by role link text")
        page.get_by_role("link", name=re.compile("Round Results", re.I)).first.click(timeout=4000)
        logger.debug("clicked Round Results by role; url=%s", getattr(page, 'url', None))
        return True
    except Exception:
        pass
    # 4) Session-aware fallback: construct RoundResults URL with TIM and twSessionId
    def _extract_from_url(u: str) -> Tuple[Optional[str], Optional[str]]:
        try:
            q = parse_qs(urlparse(u).query)
            sid = (q.get("twSessionId", [None])[0])
            tim = (q.get("TIM", [None])[0])
            return tim, sid
        except Exception:
            return None, None

    def _extract_from_string(s: str) -> Tuple[Optional[str], Optional[str]]:
        try:
            sid_m = re.search(r"twSessionId=([A-Za-z0-9]+)", s)
            tim_m = re.search(r"TIM=(\d+)", s)
            sid = sid_m.group(1) if sid_m else None
            tim = tim_m.group(1) if tim_m else None
            return tim, sid
        except Exception:
            return None, None

    def _collect_session(page_obj: Any) -> Tuple[Optional[str], Optional[str]]:
        # 1) Try current page URL
        tim, sid = _extract_from_url(getattr(page_obj, 'url', '') or '')
        if tim and sid:
            return tim, sid
        # 2) Try any anchors on page containing session params
        try:
            for a in page_obj.locator('a[href*="twSessionId="]').element_handles():
                href = a.get_attribute('href') or ''
                tim, sid = _extract_from_string(href)
                if tim and sid:
                    return tim, sid
        except Exception:
            pass
        # 3) Try frames
        try:
            for fr in page_obj.frames:
                try:
                    tim, sid = _extract_from_url(getattr(fr, 'url', '') or '')
                    if tim and sid:
                        return tim, sid
                except Exception:
                    pass
                try:
                    for a in fr.locator('a[href*="twSessionId="]').element_handles():
                        href = a.get_attribute('href') or ''
                        tim, sid = _extract_from_string(href)
                        if tim and sid:
                            return tim, sid
                except Exception:
                    pass
        except Exception:
            pass
        # 4) Try cookies
        try:
            cookies = page_obj.context.cookies()
            sid = None
            for c in cookies:
                if c.get('name') == 'twSessionId':
                    sid = c.get('value')
                    break
            # If we have the SID but not TIM, sometimes TIM is a timestamp; try page location as last resort
            if sid and not tim:
                tim, _ = _extract_from_url(getattr(page_obj, 'url', '') or '')
            return tim, sid
        except Exception:
            return None, None

    tim, sid = _collect_session(page)
    if tim and sid:
        try:
            params = {"displayFormatBox": "1", "TIM": tim, "twSessionId": sid}
            target = "https://www.trackwrestling.com/opentournaments/RoundResults.jsp?" + urlencode(params)
            logger.debug("navigating to session RoundResults URL: %s", target)
            page.goto(target, wait_until="domcontentloaded", timeout=8000)
            logger.debug("navigated to RoundResults with session; url=%s", getattr(page, 'url', None))
            return True
        except Exception:
            logger.warning("failed navigating to session RoundResults URL")
            return False

    logger.info("unable to determine session params for Round Results navigation")
    return False


def ensure_round_results_view(page: Any) -> bool:
    """Ensure we're back on the Round Results selection (with select#roundIdBox visible)."""
    # If already visible, done
    try:
        for fr in [page] + list(page.frames):
            try:
                if fr.locator("select#roundIdBox").count() > 0:
                    return True
            except Exception:
                continue
    except Exception:
        pass
    # Try 'Back' controls
    try:
        for fr in [page] + list(page.frames):
            try:
                back = fr.locator('input[type="button"][value="Back"], button:has-text("Back"), a:has-text("Back"), a:has-text("Round Results")').first
                if back.count() > 0:
                    back.click()
                    break
            except Exception:
                continue
    except Exception:
        pass
    # Try native back
    try:
        page.go_back(wait_until="domcontentloaded")
    except Exception:
        pass
    # Finally, navigate explicitly
    try:
        if goto_round_results(page):
            return True
    except Exception:
        pass
    # Check again
    try:
        for fr in [page] + list(page.frames):
            try:
                if fr.locator("select#roundIdBox").count() > 0:
                    return True
            except Exception:
                continue
    except Exception:
        pass
    return False


def parse_rounds(page: Any) -> List[Tuple[str, str]]:
    from playwright.sync_api import TimeoutError as PWTimeout

    # Helper to extract from a select locator
    def _extract_from_select(sel_loc) -> List[Tuple[str, str]]:
        options = sel_loc.locator("option[value]")
        cnt = options.count()
        out: List[Tuple[str, str]] = []
        for i in range(cnt):
            value = options.nth(i).get_attribute("value") or ""
            if not value:
                continue
            label = (options.nth(i).inner_text() or "").strip()
            out.append((value, label))
        return out

    # 1) Try on the page
    try:
        select = page.locator("select#roundIdBox")
        select.wait_for(timeout=4000)
        rounds = _extract_from_select(select)
        logger.debug("rounds select found on page; options=%s", len(rounds))
        return rounds
    except PWTimeout:
        pass
    # 2) Look through frames
    for fr in page.frames:
        try:
            sel = fr.locator("select#roundIdBox")
            if sel.count() > 0:
                try:
                    sel.wait_for(timeout=3000)
                except Exception:
                    pass
                rounds = _extract_from_select(sel)
                logger.debug("rounds select found in frame; options=%s", len(rounds))
                return rounds
        except Exception:
            continue
    return []


def scrape_rounds_for_event(page: Any, conn: duckdb.DuckDBPyConnection, event_id: str) -> int:
    logger.info("BEGIN scrape_rounds_for_event event_id=%s url=%s", event_id, getattr(page, 'url', None))
    if not open_event_by_id(page, event_id):
        logger.warning("FAIL open_event_by_id for %s", event_id)
        return -1
    if not enter_event(page):
        logger.warning("FAIL enter_event for %s", event_id)
        return -2
    if not goto_round_results(page):
        logger.warning("FAIL goto_round_results for %s", event_id)
        return -3
    ensure_rounds_table(conn)

    # Helper: snapshot current results HTML
    def get_results_snapshot() -> str:
        try:
            for fr in page.frames:
                try:
                    loc = fr.locator("#resultsTable, #bracketsTable, #results, div.results, table.results").first
                    if loc.count() > 0:
                        return loc.inner_html()
                except Exception:
                    continue
            return page.content()
        except Exception:
            return ""

    rounds = parse_rounds(page)
    saved = 0
    for rid, label in rounds:
        try:
            # Skip aggregate/empty
            if (label or "").strip().lower() == "all rounds" or rid in (None, "", "0"):
                continue
            # Find the frame with selector and Go
            rounds_frame = None
            select_loc = None
            btn_go = None
            for fr in [page] + list(page.frames):
                try:
                    sel = fr.locator("select#roundIdBox")
                    if sel.count() > 0:
                        rounds_frame = fr
                        select_loc = sel
                        btn_go = fr.locator(
                            'input[type="button"][value="Go"][onclick*="viewSchedule"], '
                            'input[type="button"][value="Go"], '
                            'button:has-text("Go")'
                        ).first
                        break
                except Exception:
                    continue
            if rounds_frame is None or select_loc is None:
                logger.warning("no round selector found for event %s; skipping round %s", event_id, label)
                # Still persist label-only for visibility
                upsert_round(conn, event_id, rid, label)
                continue

            before_html = get_results_snapshot()
            # Select round
            try:
                select_loc.select_option(value=rid)
                logger.debug("selected round %s (%s)", label, rid)
            except Exception:
                logger.warning("failed to select round %s (%s) for %s", label, rid, event_id)
                upsert_round(conn, event_id, rid, label)
                continue
            # Trigger viewSchedule via Go or JS
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

            # Wait for change
            try:
                page.wait_for_timeout(300)
            except Exception:
                pass
            for _ in range(40):
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

            # Small stabilization wait to ensure content is fully rendered
            try:
                page.wait_for_timeout(400)
            except Exception:
                pass
            # Capture HTML (full frame/page content)
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
                # Prefer saving full HTML to ensure #pageContent is present
                # Brief wait to allow final paint
                try:
                    target.wait_for_timeout(150)
                except Exception:
                    pass
                raw_html = target.content()
            except Exception:
                raw_html = None
            # Validate presence of <div id="pageContent"> in captured HTML before saving.
            # If missing, retry once after a longer wait and re-capture.
            try:
                def _has_page_content(html: Optional[str]) -> bool:
                    return isinstance(html, str) and re.search(r'<div[^>]+id=["\']pageContent["\']', html, re.I) is not None

                ok = _has_page_content(raw_html)
                if not ok:
                    logger.debug("pageContent missing; retrying capture after extra wait for %s - %s", event_id, label)
                    try:
                        page.wait_for_timeout(800)
                    except Exception:
                        pass
                    # Re-capture
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
                        raw_html_retry = target.content()
                        if _has_page_content(raw_html_retry):
                            raw_html = raw_html_retry
                            ok = True
                    except Exception:
                        pass

                if ok:
                    upsert_round(conn, event_id, rid, label, raw_html)
                else:
                    logger.warning("skipping raw_html save: #pageContent not found in captured HTML for %s - %s", event_id, label)
                    upsert_round(conn, event_id, rid, label)
            except Exception as e:
                logger.warning("validation error when checking #pageContent for %s - %s: %s", event_id, label, e)
                upsert_round(conn, event_id, rid, label)
            saved += 1
            # After saving a round, ensure we're back at the Round Results selection
            try:
                ensure_round_results_view(page)
            except Exception:
                pass
        except Exception as e:
            logger.warning("failed round capture %s (%s) for %s: %s", label, rid, event_id, e)
            # at least persist label
            try:
                upsert_round(conn, event_id, rid, label)
            except Exception:
                pass

    logger.info("DONE scrape_rounds_for_event event_id=%s rounds=%s saved=%s url=%s", event_id, len(rounds), saved, getattr(page, 'url', None))
    return len(rounds)
