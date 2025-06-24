#!/usr/bin/env python3

import re
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pandas as pd
import pytest
import sys

# ────────────────────────────────────────────────────────────────────────────────
#  Fixtures – tiny in‑memory samples that exercise each logical branch
# ────────────────────────────────────────────────────────────────────────────────

@pytest.fixture()
def sample_mapping():
    """A *minimal* slice of mapping_catalogue.yml turned into a dict."""
    return {
        "columns": {
            "gene_id": {
                "regex": r"^ENSG\d{11}(?:\.\d+)?$",
                "target_table": "Genes",
                "target_column": "gene_id",
                "transforms": ["strip_version"],
            },
            "stimulus": {
                "regex": r".*",  # catch‑all
                "target_table": "Stimuli",
                "target_column": "label",
                "transforms": ["lowercase_ascii"],
            },
        }
    }


@pytest.fixture()
def sample_records():
    """Three fake lines coming out of *extract()* – each is a dict."""
    return [
        {"column": "gene_id", "value": "ENSG00000123456.17"},
        {"column": "gene_id", "value": "ENSG00000123456"},  # duplicate after strip_version
        {"column": "stimulus", "value": "  Butyrate "},
    ]


# ────────────────────────────────────────────────────────────────────────────────
#  Unit tests for *preprocessing* helper functions
# ────────────────────────────────────────────────────────────────────────────────

# Add the project root to sys.path for absolute imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etl.harmonise import harmonise
import etl.utils.preprocessing as pre

def test_strip_version():
    print("Testing strip_version()")
    
    value = pre.strip_version("ENSG00000000003.5")
    print(f"Result: {value}, Expected: {'ENSG00000000003'}")
    assert value == "ENSG00000000003"

    value = pre.strip_version("ENSG00000000003")
    print(f"Result: {value}, Expected: {'ENSG00000000003'}")
    assert value == "ENSG00000000003"
    
    # non‑ensembl stays intact
    value = pre.strip_version("BRCA1")
    print(f"Result: {value}, Expected: {'BRCA1'}")
    assert value == "BRCA1"

    # None / blank → None
    value = pre.strip_version(None)
    print(f"Result: {value}, Expected: {None}")
    assert value is None
    
    value = pre.strip_version("   ")
    print(f"Result: {value}, Expected: {None}")
    assert value is None


def test_lowercase_ascii():
    print("Testing lowercase_ascii()")
    
    value = pre.lowercase_ascii("  β‑Alanine  ")
    print(f"Result: {value}, Expected: {'beta-alanine'}")
    assert value == "beta-alanine"

    value = pre.lowercase_ascii("")
    print(f"Result: {value}, Expected: {None}")
    assert value is None


def test_split_commas():
    print("Testing split_commas()")
    
    value = pre.split_commas("A, B ,C,, ")
    print(f"Result: {value}, Expected: {['A', 'B', 'C']}")
    assert value == ["A", "B", "C"]

    value = pre.split_commas(None)
    print(f"Result: {value}, Expected: {[]}")
    assert value == []


# ────────────────────────────────────────────────────────────────────────────────
#  Unit tests for *harmonise()*
# ────────────────────────────────────────────────────────────────────────────────


def test_harmonise_dedup(sample_records, sample_mapping):
    print("Testing harmonise() deduplication")
    
    # Harmonise the sample records using the provided mapping
    print("Harmonising sample records...")
    print(f"Sample records: {sample_records}")
    print(f"Sample mapping: {sample_mapping}")
    print("Calling harmonise()...")
    from etl.harmonise import harmonise  # noqa: E402  – imported after fixtures
    staging = harmonise(sample_records, sample_mapping)
    print("Harmonisation complete. Checking results...")
    print(f"Staging data: {staging}")
        
    # The version suffix should be stripped and duplicates collapsed
    print("Checking deduplication of gene_id...")
    value = staging[("Genes", "gene_id")]
    print(f"Result: {value}, Expected: {{'ENSG00000123456'}}")
    assert value == {"ENSG00000123456"}
    
    print("Checking lower-casing and trimming of stimulus label...")
    value = staging[("Stimuli", "label")]
    print(f"Result: {value}, Expected: {{'butyrate'}}")
    assert value == {"butyrate"}
    
    print("All checks passed!")

# ────────────────────────────────────────────────────────────────────────────────
#  Unit & regression tests for *load()*
# ────────────────────────────────────────────────────────────────────────────────

# from etl.load import load  # noqa: E402  – imported late on purpose


# class DummyCursor(SimpleNamespace):
#     def __init__(self):
#         self.sql_log = []

#     def execute(self, sql):
#         self.sql_log.append(sql)
#         self.last_sql = sql



# class DummyConn(SimpleNamespace):
#     def __enter__(self):
#         return self.cursor

#     def __exit__(self, exc_type, exc, tb):
#         return False  # propagate exceptions


# class DummyEngine(SimpleNamespace):
#     def begin(self):
#         return self.connection


# @pytest.fixture()
# def dummy_engine(monkeypatch):
#     """Patch *sqlalchemy.create_engine* → Dummy objects so no real DB is touched."""

#     cursor = DummyCursor()
#     connection = DummyConn(cursor=cursor)
#     engine = DummyEngine(connection=connection)

#     with monkeypatch.context() as m:
#         m.setitem(sys.modules, "sqlalchemy", mock.MagicMock())
#         import sqlalchemy  # type: ignore  # noqa: F401

#         # # Mock the create_engine function to return our dummy engine
#         # sqlalchemy.create_engine = mock.MagicMock()
        
#         sqlalchemy.create_engine.return_value = engine  # type: ignore
#         print("Dummy engine created with mock connection and cursor.")
#         yield engine, cursor  # hand back for assertions
        
#         print("Dummy engine fixture teardown complete.")

import types, sys
from unittest import mock
import pytest

# class DummyCursor:
#     def __init__(self):
#         self.sql_log = []
#     def execute(self, sql):
#         self.sql_log.append(sql)

# class DummyConn:
#     def __init__(self, cursor):
#         self.cursor = cursor
#     def __enter__(self):  return self.cursor
#     def __exit__(self, exc, val, tb): return False

# class DummyEngine:
#     def __init__(self, cursor):
#         self.cursor = cursor
#     def begin(self): return DummyConn(self.cursor)

# @pytest.fixture()
# def dummy_engine(monkeypatch):
#     cursor  = DummyCursor()
#     engine  = DummyEngine(cursor)

#     # ---------- build a minimal fake "sqlalchemy" module ----------
#     fake_sa              = types.ModuleType("sqlalchemy")
#     fake_sa.__version__  = "2.0.0"
#     fake_sa.create_engine = lambda *a, **kw: engine
#     # anything else your code touches can be added here

#     monkeypatch.setitem(sys.modules, "sqlalchemy", fake_sa)
#     yield engine, cursor


# @pytest.mark.usefixtures("dummy_engine")
# def test_load_upsert(monkeypatch, sample_records, sample_mapping, dummy_engine):
#     engine, cursor = dummy_engine
#     print("Testing load() with upsert functionality...")
    
#     # import pandas as pd
#     # Patch to_sql BEFORE any call to it!
#     # def fake_to_sql(self, name, conn, *args, **kwargs):
#     #     fake_to_sql.called = True
#     #     assert name == "#temp"
#     #     assert kwargs.get("if_exists", None) == "replace"
#     # fake_to_sql.called = False
#     # monkeypatch.setattr(pd.DataFrame, "to_sql", fake_to_sql, raising=True)
#     from etl.load import load
    
#     print("Patching pandas.DataFrame.to_sql with a fake implementation...")
    
#     print("Calling load() with sample records and mapping...")
#     print(f"Engine: {engine}, Cursor: {cursor}\n")
    
#     print("Harmonising sample records...")
#     print("Sample records:", sample_records)
#     print("Sample mapping:", sample_mapping)
    
#     # Harmonise the sample records using the provided mapping
#     # This will generate the staging data ready for loading
#     print("Calling harmonise()...")
#     from etl.harmonise import harmonise
#     staging = harmonise(sample_records, sample_mapping)
#     # Convert defaultdict to DataFrame
#     # staging = pd.DataFrame([
#     #     {"table": k[0], "column": k[1], "value": v}
#     #     for k, values in staging.items()
#     #     for v in values
#     # ])

#     print("Harmonisation complete. Staging data prepared for loading.")
#     print("Loading staging data into MySQL database...")
#     # with engine.begin() as conn:
#     #     staging.to_sql("#temp", conn, index=False, if_exists="replace")
#     load(staging, "mysql://user:pass@localhost/fakeDB")

#     # assert fake_to_sql.called is True
#     assert any(re.search(r"INSERT INTO Genes \(gene_id\)", s) for s in cursor.sql_log)
#     assert any("ON DUPLICATE KEY UPDATE" in s for s in cursor.sql_log)

#     print("All assertions passed. Load test completed successfully.")



class DummyCursor:
    def __init__(self):
        self.sql_log = []

    def execute(self, sql):
        self.sql_log.append(sql)
        self.last_sql = sql

class DummyConn:
    def __init__(self, cursor):
        self.cursor = cursor

    def __enter__(self):
        return self.cursor

    def __exit__(self, exc_type, exc_val, tb):
        return False  # propagate exceptions

class DummyEngine:
    def __init__(self, cursor):
        self.cursor = cursor

    def begin(self):
        return DummyConn(self.cursor)

# ----------------------------------------
# Fixture to patch sqlalchemy.create_engine
# ----------------------------------------

@pytest.fixture()
def dummy_engine(monkeypatch):
    # Create dummy SQL engine
    cursor = DummyCursor()
    engine = DummyEngine(cursor)

    # Build fake sqlalchemy module
    fake_sqlalchemy = types.ModuleType("sqlalchemy")
    fake_sqlalchemy.__version__ = "2.0.0"
    fake_sqlalchemy.create_engine = lambda *a, **k: engine

    # Inject fake sqlalchemy before load() imports it
    monkeypatch.setitem(sys.modules, "sqlalchemy", fake_sqlalchemy)

    yield engine, cursor
    
@pytest.mark.usefixtures("dummy_engine")
def test_load_upsert(monkeypatch, sample_records, sample_mapping, dummy_engine):
    engine, cursor = dummy_engine

    print("Testing load() with upsert functionality...")

    from etl.harmonise import harmonise  # noqa: E402  – imported after fixtures
    from etl.load import load  # noqa: E402  – imported after fixtures
    
    staging = harmonise(sample_records, sample_mapping)
    print("Harmonised staging =", staging)

    # Call real load() with a fake MySQL URI
    load(staging, "mysql://user:pass@localhost/fakeDB")

    # Assertions
    assert any("INSERT INTO Genes" in sql for sql in cursor.sql_log)
    assert any("ON DUPLICATE KEY UPDATE" in sql for sql in cursor.sql_log)
# ────────────────────────────────────────────────────────────────────────────────

    # staging = harmonise(sample_records, sample_mapping)

    # load(staging, "mysql://user:pass@localhost/fakeDB")

    # # 1) DataFrame.to_sql was called
    # assert fake_to_sql.called is True

    # # 2) The generated SQL contains the expected pieces
    # assert re.search(r"INSERT INTO Genes \(gene_id\)", cursor.last_sql)
    # assert "ON DUPLICATE KEY UPDATE" in cursor.last_sql


# ────────────────────────────────────────────────────────────────────────────────
#  Regression test: *harmonise()* round‑trip stability
#    – If someone changes the regex or transforms, this will flag it.
# ────────────────────────────────────────────────────────────────────────────────

@pytest.mark.regression
def test_harmonise_snapshot(sample_records, sample_mapping):
    """Compare against a frozen snapshot stored in *tests/snapshots*.
    Use *pytest ‑‑snapshot‑update* to refresh if the behaviour is *intentionally* changed.
    """
    from syrupy.assertion import SnapshotAssertion  # type: ignore – optional plugin

    staging = harmonise(sample_records, sample_mapping)
    assert staging == SnapshotAssertion("harmonise_minimal")


# ────────────────────────────────────────────────────────────────────────────────
#  Smoke test for *get_remote_dataset.download_geo_supp_file* (offline)
# ────────────────────────────────────────────────────────────────────────────────

from etl.utils.get_remote_dataset import download_geo_supp_file  # noqa: E402


def test_download_geo_bad_gse(capsys):
    # Malformed accession should *not* try to touch the network
    print("Testing download_geo_supp_file with invalid GEO Series ID...")
    print("Calling download_geo_supp_file with 'BAD123' and 'foo.txt'...")
    
    download_geo_supp_file("BAD123", "foo.txt")
    print("download_geo_supp_file called. Capturing output...")
    captured = capsys.readouterr()
    print("Captured output:", captured.out)
    print("Checking if output contains 'Invalid GEO Series ID'...")
    # Check if the output contains the expected error message
    value = "Invalid GEO Series ID" in captured.out
    print(f"Output contains 'Invalid GEO Series ID': {value}")
    assert value is True
    print("Test completed successfully. Invalid GEO Series ID handled correctly.")
