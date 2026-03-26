"""
Tests for Iceberg partition parsing logic in python_utils.sql.
Validates parsing of SHOW CREATE TABLE output for both Hive-style
and hidden (Iceberg) partition transforms.
@akmalsoliev: Yes this was created using Claude Code for TDD
"""

import re
import pytest


def parse_partitions(show_ddl):
    """Current implementation from python_utils.sql."""
    match = re.search(r'PARTITIONED BY \(([^)]*)\)', show_ddl)
    current_partitions = (
        set(p.strip() for p in match.group(1).split(',')) if match else set()
    )
    return current_partitions


def _ddl(partition_clause):
    """Helper to build a minimal DDL string."""
    if not partition_clause:
        return "CREATE TABLE t (id INT) USING iceberg"
    return f"CREATE TABLE t (id INT) USING iceberg PARTITIONED BY ({partition_clause})"


# --- Parsing ---

@pytest.mark.parametrize("clause, expected", [
    # Simple (Hive-style) - should work
    ("event_date", {"event_date"}),
    ("event_date, region", {"event_date", "region"}),
    # Hidden transforms - currently broken
    ("hours(ts)", {"hours(ts)"}),
    ("days(ts)", {"days(ts)"}),
    ("months(ts)", {"months(ts)"}),
    ("years(ts)", {"years(ts)"}),
    ("bucket(16, id)", {"bucket(16, id)"}),
    ("truncate(10, city)", {"truncate(10, city)"}),
    # Mixed
    ("hours(ts), truncate(10, city), region", {"hours(ts)", "truncate(10, city)", "region"}),
    ("hours(ts), bucket(16, id), truncate(10, city)", {"hours(ts)", "bucket(16, id)", "truncate(10, city)"}),
])
def test_parse_partitions(clause, expected):
    assert parse_partitions(_ddl(clause)) == expected


def test_no_partition_clause():
    assert parse_partitions(_ddl(None)) == set()


def test_empty_partition_clause():
    assert parse_partitions(_ddl("")) == set()


# --- Case normalization ---

@pytest.mark.parametrize("clause, expected", [
    ("HOURS(ts)", {"hours(ts)"}),
    ("Hours(ts), BUCKET(16, id), Region", {"hours(ts)", "bucket(16, id)", "region"}),
])
def test_case_normalization(clause, expected):
    assert parse_partitions(_ddl(clause)) == expected


# --- Partition evolution (add/drop) ---

@pytest.mark.parametrize("current_clause, desired, exp_drop, exp_add", [
    # No change
    ("event_date", ["event_date"], set(), set()),
    ("hours(ts)", ["hours(ts)"], set(), set()),
    # Add field
    ("event_date", ["event_date", "region"], set(), {"region"}),
    # Drop field
    ("event_date, region", ["event_date"], {"region"}, set()),
    # Evolve simple -> hidden
    ("ts", ["hours(ts)"], {"ts"}, {"hours(ts)"}),
    # Change transform
    ("hours(ts)", ["days(ts)"], {"hours(ts)"}, {"days(ts)"}),
    # No partition -> partitioned
    (None, ["bucket(16, id)"], set(), {"bucket(16, id)"}),
    # Partitioned -> no partition
    ("hours(ts)", [], {"hours(ts)"}, set()),
])
def test_partition_evolution(current_clause, desired, exp_drop, exp_add):
    current = parse_partitions(_ddl(current_clause))
    desired_set = set(f.strip().lower() for f in desired)
    assert current - desired_set == exp_drop
    assert desired_set - current == exp_add
