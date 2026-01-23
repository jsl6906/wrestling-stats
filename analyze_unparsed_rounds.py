"""Analyze rounds that don't have matches to understand parsing issues."""

import duckdb
from pathlib import Path
from bs4 import BeautifulSoup

# Connect to the database
db_path = Path('output/trackwrestling_vhsl.db')
conn = duckdb.connect(str(db_path))

# Query for rounds with no matches
query = """--sql
SELECT 
    t.event_id,
    t.name as tournament_name,
    t.start_date,
    tr.label as round_label,
    tr.raw_html,
    tr.parsed_ok,
    LENGTH(tr.raw_html) as html_length
FROM tournament_rounds tr
LEFT JOIN tournaments t ON tr.event_id = t.event_id
LEFT JOIN matches m ON tr.round_id = m.round_id
WHERE m.round_id IS NULL
ORDER BY t.start_date DESC
LIMIT 30
"""

results = conn.execute(query).fetchall()

print(f"\n{'='*80}")
print(f"Found {len(results)} rounds with no matches")
print(f"{'='*80}\n")

# Analyze the HTML structure
for i, row in enumerate(results):
    event_id, tournament_name, start_date, round_label, raw_html, parsed_ok, html_length = row
    
    print(f"\n{i+1}. Tournament: {tournament_name}")
    print(f"   Event ID: {event_id}")
    print(f"   Round: {round_label}")
    print(f"   Date: {start_date}")
    print(f"   parsed_ok: {parsed_ok}")
    print(f"   HTML Length: {html_length}")
    
    if raw_html:
        soup = BeautifulSoup(raw_html, 'html.parser')
        
        # Check for standard tournament format (section.tw-list)
        section = soup.select_one("section.tw-list, section[class~=tw-list]")
        if section:
            print(f"   ✓ Found section.tw-list")
            # Count h2 and ul elements
            h2s = section.find_all("h2")
            uls = section.find_all("ul")
            print(f"   - {len(h2s)} weight classes (h2)")
            print(f"   - {len(uls)} match lists (ul)")
            if uls:
                total_lis = sum(len(ul.find_all("li", recursive=False)) for ul in uls)
                print(f"   - {total_lis} total matches (li)")
        else:
            print(f"   ✗ No section.tw-list found")
        
        # Check for dual meet format (table.tw-table)
        table = soup.select_one("table.tw-table")
        if table:
            print(f"   ✓ Found table.tw-table")
            rows = table.find_all("tr")
            print(f"   - {len(rows)} table rows")
            # Sample first few rows
            for j, tr in enumerate(rows[:3]):
                cells = tr.find_all("td")
                if cells:
                    print(f"   - Row {j+1}: {len(cells)} cells")
                    if len(cells) >= 2:
                        print(f"     Cell 0: {cells[0].get_text(strip=True)[:50]}")
                        print(f"     Cell 1: {cells[1].get_text(strip=True)[:50]}")
        else:
            print(f"   ✗ No table.tw-table found")
        
        # Check what's actually in the HTML
        text_content = soup.get_text(strip=True)[:200]
        if text_content:
            print(f"   First 200 chars of text: {text_content}")
        else:
            print(f"   ⚠ No text content found")
        
        # Show a sample of the raw HTML
        print(f"\n   Sample HTML (first 500 chars):")
        print(f"   {raw_html[:500]}")
    else:
        print(f"   ⚠ No raw_html content")
    
    if i >= 9:  # Limit to first 10 for detailed analysis
        print(f"\n... and {len(results) - 10} more rounds")
        break

conn.close()
