"""Test parsing dual meet format."""
import duckdb
from code.config import get_db_path
from code.parse_round_html import parse_round_html, parse_match_text
from bs4 import BeautifulSoup

conn = duckdb.connect(str(get_db_path()))

# Get one unparsed dual meet record
result = conn.execute("""
    SELECT event_id, round_id, label, raw_html
    FROM tournament_rounds
    WHERE (parsed_ok IS NULL OR parsed_ok = FALSE)
    AND raw_html IS NOT NULL
    AND round_id LIKE 'chart_%'
    LIMIT 1
""").fetchone()

if result:
    event_id, round_id, label, raw_html = result
    print(f"\n=== Testing Dual Meet Parsing ===")
    print(f"Event: {event_id}, Round: {round_id}, Label: {label}\n")
    
    # Parse the HTML
    matches = parse_round_html(raw_html)
    print(f"Found {len(matches)} matches\n")
    
    # Show first 3 matches
    for i, (weight_class, raw_match) in enumerate(matches[:3], 1):
        print(f"Match {i} - Weight: {weight_class}")
        print(f"Raw HTML: {raw_match[:200]}...")
        
        # Extract text
        soup = BeautifulSoup(raw_match, "html.parser")
        text = soup.get_text(" ", strip=True)
        print(f"Extracted Text: {text}")
        
        # Parse the text
        parsed = parse_match_text(text)
        print(f"Parsed:")
        print(f"  Winner: {parsed.get('winner_name')} ({parsed.get('winner_team')})")
        print(f"  Loser: {parsed.get('loser_name')} ({parsed.get('loser_team')})")
        print(f"  Decision: {parsed.get('decision_type')} / {parsed.get('decision_type_code')}")
        if parsed.get('fall_time'):
            print(f"  Fall Time: {parsed.get('fall_time')}")
        if parsed.get('winner_points') is not None:
            print(f"  Score: {parsed.get('winner_points')}-{parsed.get('loser_points')}")
        print()
else:
    print("No unparsed dual meet records found")

conn.close()
