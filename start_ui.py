"""
Simple DuckDB UI Launcher
Connects to the local database and launches the DuckDB web UI
"""

import duckdb
import os


def main():
    db_path = "output/trackwrestling.db"
    
    # Check if database exists
    if not os.path.exists(db_path):
        print(f"Database file '{db_path}' not found.")
        return
    
    print(f"Connecting to database: {db_path}")
    
    try:
        # Connect to DuckDB
        conn = duckdb.connect(db_path)
                
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