"""
Simple DuckDB UI Launcher
Connects to the local database and launches the DuckDB web UI
Requires DuckDB to be installed, and internet access for the web UI.
"""

import duckdb
from pathlib import Path

def main():
    # Find all .db files from output/ directory
    output_dir = Path("output")
    
    if not output_dir.exists() or not output_dir.is_dir():
        print("output/ directory not found.")
        return
    
    db_files = list(output_dir.glob("*.db"))
    
    if not db_files:
        print("No .db files found in output/ directory.")
        return
    
    # Use the first .db file as the main connection
    main_db = db_files[0]
    print(f"Connecting to database: {main_db}")
    
    try:
        # Connect to DuckDB
        conn = duckdb.connect(str(main_db))
        
        # Attach all other .db files from output/ directory
        other_db_files = [f for f in db_files if f != main_db]
        if other_db_files:
            print(f"\nAttaching {len(other_db_files)} additional database file(s):")
            for db_file in other_db_files:
                # Use the file name (without extension) as the database alias
                db_name = db_file.stem
                db_file_path = str(db_file)
                try:
                    conn.execute(f"ATTACH '{db_file_path}' AS {db_name};")
                    print(f"  ✓ Attached: {db_file.name} as '{db_name}'")
                except Exception as e:
                    print(f"  ✗ Failed to attach {db_file.name}: {e}")
        else:
            print("\nNo additional .db files found in output/ directory.")
                
        print("\n🚀 Starting DuckDB UI...")
        print("The web interface will open in your browser.")
        print("\nPress Enter to stop the UI and exit...")
        
        # Start the DuckDB web UI
        conn.execute("CALL start_ui();")
        
        # Wait for user input
        input()
        
        # Close connection
        conn.close()
        print("Database connection closed.")
        
    except Exception as e:
        print(f"Error: {e}")
        print("Make sure DuckDB is installed: uv add duckdb")

if __name__ == "__main__":
    main()