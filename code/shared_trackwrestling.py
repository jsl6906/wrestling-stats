"""
Shared utilities for TrackWrestling scraping.

This module provides:
- Database helpers for tournament rounds table
- HTML validation utilities
- Playwright helpers for round scraping (round selection, navigation within events)

Note: Tournament discovery is now handled via HTTP requests in scrape_tournaments.py.
This module focuses on the Playwright-based round scraping workflow.
"""

from __future__ import annotations

import re
import logging
from urllib.parse import urlparse, parse_qs, urlencode
import time
from typing import List, Optional, Tuple, Any

import duckdb

# Module logger
logger = logging.getLogger(__name__)


# ============================================================================
# HTML Validation
# ============================================================================

def validate_round_html(html: Optional[str], event_id: str, label: str) -> Tuple[bool, str]:
    """
    Validate captured round HTML to detect incomplete page loads.
    
    Returns (is_valid, reason):
    - (True, "ok") if HTML appears valid
    - (False, reason) if HTML is invalid/incomplete
    """
    if not html or not isinstance(html, str):
        return False, "empty or non-string HTML"
    
    # Check minimum length (incomplete pages are usually very short)
    if len(html) < 1000:
        return False, f"HTML too short ({len(html)} bytes)"
    
    # Check for required content structures
    has_page_content = bool(re.search(r'<div[^>]+id=["\']pageContent["\']', html, re.I))
    has_tw_list = bool(re.search(r'<section[^>]+class=["\'][^"\']*(tw-list|tw\-list)[^"\'\/]*["\']', html, re.I))
    has_results_table = bool(re.search(r'<(table|div)[^>]+id=["\']?(resultsTable|bracketsTable|results)["\']?', html, re.I))
    
    # Check for cookie consent/error pages (Osano cookie manager)
    # Only reject if it ONLY has osano content and no actual page content
    if 'osano-cm-window' in html and not (has_page_content or has_tw_list or has_results_table):
        return False, "appears to be cookie consent page only"
    
    # At least one content structure should be present
    if not (has_page_content or has_tw_list or has_results_table):
        return False, "missing expected content structures (pageContent, tw-list, or results table)"
    
    # Check for common error messages
    error_patterns = [
        r'page\s+not\s+found',
        r'error\s+occurred',
        r'access\s+denied',
        r'session\s+expired',
        r'invalid\s+request',
    ]
    for pattern in error_patterns:
        if re.search(pattern, html, re.I):
            return False, f"contains error message: {pattern}"
    
    # Additional validation: if we have tw-list, check if it has actual content
    if has_tw_list:
        # Extract the tw-list section and check if it has weight classes (h2) or matches (li)
        tw_list_match = re.search(r'<section[^>]+class=["\'][^"\']*(tw-list|tw\-list)[^"\'\/]*["\'][^>]*>(.*?)</section>', html, re.I | re.S)
        if tw_list_match:
            section_content = tw_list_match.group(1)
            has_h2 = '<h2' in section_content
            has_li = '<li' in section_content
            if not (has_h2 or has_li):
                return False, "tw-list section exists but appears empty"
    
    return True, "ok"


# ============================================================================
# Database Helpers
# ============================================================================

def ensure_rounds_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Ensure the tournament_rounds table exists."""
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


def upsert_round(
    conn: duckdb.DuckDBPyConnection,
    event_id: str,
    round_id: str,
    label: str,
    raw_html: Optional[str] = None,
    validation_failed: bool = False
) -> None:
    """
    Insert or update a tournament round record.
    
    Args:
        conn: DuckDB connection
        event_id: Tournament event ID
        round_id: Round identifier
        label: Human-readable round label
        raw_html: Optional captured HTML content
        validation_failed: If True, sets parsed_ok = FALSE to prevent parsing attempts
    """
    if raw_html is None:
        conn.execute(
            """--sql
            INSERT INTO tournament_rounds AS tr (event_id, round_id, label, parsed_ok)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (event_id, round_id) DO UPDATE SET
                label = EXCLUDED.label,
                parsed_ok = COALESCE(EXCLUDED.parsed_ok, tr.parsed_ok)
            """,
            [event_id, round_id, label, False if validation_failed else None],
        )
    else:
        parsed_ok_value = False if validation_failed else None
        conn.execute(
            """--sql
            INSERT INTO tournament_rounds AS tr (event_id, round_id, label, raw_html, parsed_ok)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (event_id, round_id) DO UPDATE SET
                label = EXCLUDED.label,
                raw_html = EXCLUDED.raw_html,
                parsed_ok = COALESCE(EXCLUDED.parsed_ok, tr.parsed_ok)
            """,
            [event_id, round_id, label, raw_html, parsed_ok_value],
        )


# ============================================================================
# Playwright Helpers - Modal Management
# ============================================================================

def close_any_modals(page: Any) -> None:
    """Close any open modals that might block interactions."""
    try:
        # Check for close button in any frame or main page
        close_button = page.locator('i.icon-close[onclick="hideModal()"]').first
        if close_button.count() > 0 and close_button.is_visible():
            logger.debug("Closing open modal")
            close_button.click()
            time.sleep(0.1)
    except Exception:
        pass
    # Also try calling hideModal() directly
    try:
        page.evaluate("if (typeof hideModal === 'function') hideModal();")
    except Exception:
        pass


# ============================================================================
# Playwright Helpers - Tournament Type Detection
# ============================================================================

def detect_tournament_type(page: Any) -> Optional[str]:
    """Detect tournament type from current page URL and content."""
    try:
        current_url = getattr(page, 'url', '') or ''
        
        # Check URL path first
        if '/teamtournaments/' in current_url:
            return 'teamtournaments'
        elif '/predefinedtournaments/' in current_url:
            return 'predefinedtournaments'
        elif '/opentournaments/' in current_url:
            return 'opentournaments'
        
        # Check all frames for tournament type indicators
        for fr in [page] + list(page.frames):
            try:
                frame_url = getattr(fr, 'url', '') or ''
                if '/teamtournaments/' in frame_url:
                    return 'teamtournaments'
                elif '/predefinedtournaments/' in frame_url:
                    return 'predefinedtournaments'
                elif '/opentournaments/' in frame_url:
                    return 'opentournaments'
                
                # Check for tournament type in page content
                content = fr.content()
                if 'teamtournaments' in content:
                    return 'teamtournaments'
                elif 'predefinedtournaments' in content:
                    return 'predefinedtournaments'
            except Exception:
                continue
        
        # Default to opentournaments if no specific type detected
        return 'opentournaments'
    except Exception:
        return None


# ============================================================================
# Playwright Helpers - Round Results Navigation
# ============================================================================

def goto_round_results(page: Any) -> bool:
    """
    Navigate to the Round Results page within an event.
    
    Tries multiple strategies:
    1. Direct anchor click
    2. Frame anchor search
    3. Role-based link text
    4. Session-aware URL construction
    """
    # 1) Look for direct anchor to RoundResults.jsp on the page
    try:
        link = page.locator('a[href*="RoundResults.jsp"]').first
        cnt = link.count()
        logger.debug("RoundResults.jsp anchor on page count=%s", cnt)
        if cnt > 0:
            href = link.get_attribute('href')
            logger.debug("found RoundResults.jsp href: %s", href)
            if href:
                try:
                    logger.debug("navigating to RoundResults via href: %s", href)
                    page.goto(href, wait_until="domcontentloaded", timeout=8000)
                    logger.debug("navigated to RoundResults; url=%s", getattr(page, 'url', None))
                    
                    # Verify we got the round selector
                    for fr in [page] + list(page.frames):
                        try:
                            if fr.locator("select#roundIdBox").count() > 0:
                                logger.debug("verified round selector present")
                                return True
                        except Exception:
                            continue
                    logger.debug("navigated but no round selector found")
                except Exception as e:
                    logger.debug("navigation via href failed: %s", e)
            try:
                logger.debug("clicking RoundResults anchor")
                link.click(timeout=4000)
                logger.debug("clicked RoundResults anchor; url=%s", getattr(page, 'url', None))
                
                # Verify round selector
                for fr in [page] + list(page.frames):
                    try:
                        if fr.locator("select#roundIdBox").count() > 0:
                            return True
                    except Exception:
                        continue
            except Exception as e:
                logger.debug("clicking RoundResults failed: %s", e)
    except Exception as e:
        logger.debug("direct anchor lookup failed: %s", e)
    
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
        logger.debug("collected session params: TIM=%s, twSessionId=%s...", tim, sid[:10] if sid else None)
        
        # Detect tournament type from current page
        detected_type = detect_tournament_type(page)
        logger.debug("detected tournament type: %s", detected_type)
        
        # Build path priority based on detected type
        if detected_type == 'teamtournaments':
            paths_to_try = ['/teamtournaments/', '/predefinedtournaments/', '/opentournaments/']
        elif detected_type == 'predefinedtournaments':
            paths_to_try = ['/predefinedtournaments/', '/teamtournaments/', '/opentournaments/']
        else:
            paths_to_try = ['/opentournaments/', '/teamtournaments/', '/predefinedtournaments/']
        
        for path_type in paths_to_try:
            try:
                params = {"displayFormatBox": "1", "TIM": tim, "twSessionId": sid}
                target = f"https://www.trackwrestling.com{path_type}RoundResults.jsp?" + urlencode(params)
                logger.debug("navigating to session RoundResults URL: %s", target)
                page.goto(target, wait_until="domcontentloaded", timeout=10000)
                logger.debug("navigated to RoundResults with session; url=%s", getattr(page, 'url', None))
                
                # Check if we got a valid page with round selector
                time.sleep(0.4)
                has_rounds = False
                for fr in [page] + list(page.frames):
                    try:
                        if fr.locator("select#roundIdBox").count() > 0:
                            has_rounds = True
                            logger.debug("found round selector in %s", "main page" if fr == page else "frame")
                            break
                    except Exception:
                        continue
                
                if has_rounds:
                    logger.debug("SUCCESS: found round selector with path type: %s", path_type)
                    return True
                else:
                    logger.debug("no round selector found with path type: %s, trying next", path_type)
            except Exception as e:
                logger.debug("failed with path type %s: %s", path_type, e)
                continue
        
        logger.warning("failed navigating to session RoundResults URL with all path types (TIM=%s)", tim)
    else:
        logger.warning("unable to determine session params (TIM=%s, sid=%s) for Round Results navigation", tim, sid)
    
    # Final fallback: try to find Round Results link from current page menu/navigation
    logger.debug("attempting final fallback: searching for Round Results in navigation menu")
    try:
        # Look for navigation menus or tabs
        for fr in [page] + list(page.frames):
            try:
                # Try various selectors for Round Results links
                selectors = [
                    'a:has-text("Round Results")',
                    'a:has-text("Rounds")',
                    'a[href*="RoundResults"]',
                    'li:has-text("Round Results") a',
                    'nav a:has-text("Round Results")',
                    '.menu a:has-text("Round Results")',
                    '.navigation a:has-text("Round Results")',
                ]
                
                for selector in selectors:
                    try:
                        link = fr.locator(selector).first
                        if link.count() > 0 and link.is_visible():
                            logger.debug("found Round Results link with selector: %s", selector)
                            link.click(timeout=5000)
                            time.sleep(0.3)
                            
                            # Verify round selector appeared
                            for check_fr in [page] + list(page.frames):
                                try:
                                    if check_fr.locator("select#roundIdBox").count() > 0:
                                        logger.debug("SUCCESS: final fallback worked")
                                        return True
                                except Exception:
                                    continue
                    except Exception:
                        continue
            except Exception:
                continue
    except Exception as e:
        logger.debug("final fallback failed: %s", e)
    
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


# ============================================================================
# Playwright Helpers - Round Parsing
# ============================================================================

def parse_rounds(page: Any) -> List[Tuple[str, str]]:
    """
    Parse available rounds from the round selector dropdown.
    
    Returns list of (round_id, label) tuples.
    """
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
