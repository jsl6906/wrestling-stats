"""Test parsing dual meet format with fall times."""
import duckdb
from code.config import get_db_path
from code.parse_round_html import parse_round_html, parse_match_text
from bs4 import BeautifulSoup

conn = duckdb.connect(str(get_db_path()))

# Get a dual meet record
result = conn.execute("""
    SELECT event_id, round_id, label, raw_html
    FROM tournament_rounds
    WHERE round_id = 'chart_259115132_N.2'
    LIMIT 1
""").fetchone()

if result:
    event_id, round_id, label, raw_html = result
    print(f"\n=== Testing Dual Meet Parsing with Falls ===")
    print(f"Event: {event_id}, Round: {round_id}, Label: {label}\n")
    
    # Parse the HTML
    matches = parse_round_html(raw_html)
    print(f"Found {len(matches)} matches\n")
    
    # Show first 5 matches
    for i, (weight_class, raw_match) in enumerate(matches[:5], 1):
        # Extract text
        soup = BeautifulSoup(raw_match, "html.parser")
        text = soup.get_text(" ", strip=True)
        
        # Parse the text
        parsed = parse_match_text(text)
        
        print(f"Match {i} - {weight_class}lbs")
        print(f"  {parsed.get('winner_name')} ({parsed.get('winner_team')})")
        print(f"  over {parsed.get('loser_name')} ({parsed.get('loser_team')})")
        print(f"  {parsed.get('decision_type_code')}", end="")
        if parsed.get('fall_time'):
            print(f" {parsed.get('fall_time')}", end="")
        if parsed.get('winner_points') is not None:
            print(f" {parsed.get('winner_points')}-{parsed.get('loser_points')}", end="")
        print(f" [{parsed.get('decision_type')}]")
        print()
else:
    print("Record not found")

conn.close()
