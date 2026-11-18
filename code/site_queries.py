"""
Shared SQL building blocks for the Observable site's data loaders.

Everything here runs in DuckDB against tables materialized from Postgres, so seasons,
weight-class normalization, and ranking eligibility stay consistent across loaders and
with the front-end (see observable-framework-app/src/components/format.js).
"""

from __future__ import annotations

# Match history is split into this many files per governing body so a wrestler page only
# downloads the shard that holds that wrestler. Must match SHARD_COUNT in format.js.
SHARD_COUNT = 32

# A wrestler is ranked in a season once they have wrestled this many (non-bye) matches in it.
# Must match MIN_SEASON_MATCHES in format.js.
MIN_SEASON_MATCHES = 3


def shard_of(name: str) -> int:
	"""Sum of Unicode code points mod SHARD_COUNT (mirrors shardOf() in format.js)."""
	return sum(ord(c) for c in name) % SHARD_COUNT


def season_start_year_sql(date_col: str) -> str:
	"""Seasons run Sept 1 – Aug 31; returns the starting calendar year."""
	return f"(CASE WHEN month({date_col}) >= 9 THEN year({date_col}) ELSE year({date_col}) - 1 END)"


def season_label_sql(date_col: str) -> str:
	y = season_start_year_sql(date_col)
	return f"(CAST({y} AS VARCHAR) || '-' || CAST({y} + 1 AS VARCHAR))"


def is_fall_sql(decision_col: str = "decision_type", code_col: str = "decision_type_code") -> str:
	return f"(lower(coalesce({decision_col}, '')) LIKE '%fall%' OR upper(coalesce({code_col}, '')) IN ('FALL', 'PIN'))"


def weight_num_sql(col: str = "weight_class") -> str:
	"""Trailing 2–3 digit weight (e.g. 'Varsity 138', '12U (Born 2013-2014) 78'); HWT -> 285.

	Ranges such as 'Scramble 63-69' have no single weight and yield NULL.
	"""
	return f"""(CASE
		WHEN {col} IS NULL THEN NULL
		WHEN regexp_matches({col}, '\\d{{2,3}}\\s*-\\s*\\d{{2,3}}\\s*$') THEN NULL
		WHEN regexp_matches({col}, '\\d{{2,3}}\\s*(lbs?\\.?)?\\s*$')
			THEN TRY_CAST(regexp_extract({col}, '(\\d{{2,3}})\\s*(lbs?\\.?)?\\s*$', 1) AS INTEGER)
		WHEN regexp_matches({col}, '(?i)\\b(hwt|heavy)') THEN 285
		ELSE NULL
	END)"""


def weight_key_sql(col: str = "weight_class") -> str:
	"""Normalized weight class label used for weight rankings.

	High-school style labels collapse to the number ('JV 138' and 'Varsity 138' -> '138');
	youth labels keep their age group ('12U (Born 2013-2014) 78' -> '12U 78').
	"""
	age = f"regexp_extract({col}, '(?i)\\b(\\d{{1,2}}U|bantam|intermediate|novice|schoolboy|schoolgirl|cadet|junior|midget)\\b', 1)"
	num = weight_num_sql(col)
	return f"""(CASE
		WHEN {num} IS NULL THEN NULL
		WHEN {age} = '' THEN CAST({num} AS VARCHAR)
		WHEN regexp_matches({age}, '^\\d') THEN upper({age}) || ' ' || CAST({num} AS VARCHAR)
		ELSE upper(left({age}, 1)) || lower(substr({age}, 2)) || ' ' || CAST({num} AS VARCHAR)
	END)"""
