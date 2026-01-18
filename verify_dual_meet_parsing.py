"""Verify dual meet matches were parsed correctly."""
import duckdb
from code.config import get_db_path

conn = duckdb.connect(str(get_db_path()))

# Check total matches parsed
result = conn.execute("""
    SELECT COUNT(*) as total_matches
    FROM matches
    WHERE round_id LIKE 'chart_%'
""").fetchone()

print(f"\n=== Dual Meet Parsing Results ===")
print(f"Total dual meet matches parsed: {result[0]}\n")

# Check distribution of decision types
print("Decision type distribution:")
result = conn.execute("""
    SELECT decision_type, COUNT(*) as count
    FROM matches
    WHERE round_id LIKE 'chart_%'
    GROUP BY decision_type
    ORDER BY count DESC
""").fetchall()

for decision_type, count in result:
    print(f"  {decision_type or 'NULL'}: {count}")

# Show a few sample matches
print("\n=== Sample Dual Meet Matches ===")
result = conn.execute("""
    SELECT 
        event_id,
        round_id,
        weight_class,
        winner_name,
        winner_team,
        loser_name,
        loser_team,
        decision_type,
        decision_type_code,
        fall_time,
        winner_points,
        loser_points
    FROM matches
    WHERE round_id LIKE 'chart_%'
    AND decision_type IS NOT NULL
    LIMIT 10
""").fetchall()

for row in result:
    event_id, round_id, wc, winner, w_team, loser, l_team, dtype, dcode, ftime, wp, lp = row
    print(f"\n{wc}lbs: {winner} ({w_team}) over {loser} ({l_team})")
    print(f"  Decision: {dtype} ({dcode})", end="")
    if ftime:
        print(f" - {ftime}", end="")
    if wp is not None:
        print(f" - {wp}-{lp}", end="")
    print()

conn.close()
