"""Reset and retest specific records."""
import duckdb
from code.config import get_db_path
from code.parse_round_html import parse_match_text

conn = duckdb.connect(str(get_db_path()))

# Reset specific problematic records
conn.execute("DELETE FROM matches WHERE round_id = 'chart_259115132_N.11'")
conn.execute("UPDATE tournament_rounds SET parsed_ok = FALSE WHERE round_id = 'chart_259115132_N.11'")
conn.commit()

print("Reset test records\n")

# Test the problem cases directly
test_cases = [
    "72.0",
    "30.0", 
    "Easton Rossell (Broad Run) over Xavier Goldberg (Rock Ridge) Maj 12-1",
    "Double Forfeit",
]

print("=== Testing edge cases ===\n")
for text in test_cases:
    result = parse_match_text(text)
    print(f"Input: {text}")
    print(f"  Decision: {result.get('decision_type')} / {result.get('decision_type_code')}")
    print(f"  Winner: {result.get('winner_name')} ({result.get('winner_team')})")
    print(f"  Loser: {result.get('loser_name')} ({result.get('loser_team')})")
    if result.get('winner_points'):
        print(f"  Score: {result.get('winner_points')}-{result.get('loser_points')}")
    print(f"  Bye: {result.get('bye')}")
    print()

conn.close()
