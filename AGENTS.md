# AI Coding Agent Instructions

## uv for package management, virtual environments, and running scripts

This project uses **uv** for Python dependency management and virtual environment handling. **Never use `pip` directly** - always use `uv` commands instead. When trying to test implementation use `uv run <script>` to run scripts within the virtual environment. Use `uv add <packagename>` to add new dependencies.

## Duckdb for data access and querying

This project utilizes duckdb as the primary mechanism for accessing and querying data. duckdb methods should always be used first when attempting to query data, including local duckdb files, CSV files, postgreSQL database, and other data sources where it is possible.

## SQL Formatting

When writing code with multiline SQL statements, use triple quotes for the SQL string and prefix the SQL with a comment `--sql` to indicate that it is SQL code. This helps with readability and ensures proper formatting. Example:

```python
# Example of multiline SQL statement
sql_query = """--sql
SELECT * FROM my_table WHERE condition = true;
"""
```

## DBML for database schema documentation

This project utilizes the Database Markup Language DBML format for documenting database tables and
relationships. Any updates to data tables, column names, etc., should be updated in the .dbml file.
