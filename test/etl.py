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
                "regex": r"^ENSG\\d{11}(?:\\.\\d+)?$",
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

import etl.utils.preprocessing as pre

def test_strip_version():
    assert pre.strip_version("ENSG00000000003.5") == "ENSG00000000003"
    assert pre.strip_version("ENSG00000000003") == "ENSG00000000003"
    # non‑ensembl stays intact
    assert pre.strip_version("BRCA1") == "BRCA1"
    # None / blank → None
    assert pre.strip_version(None) is None
    assert pre.strip_version("   ") is None


def test_lowercase_ascii():
    assert pre.lowercase_ascii("  β‑Alanine  ") == "beta‑alanine"
    assert pre.lowercase_ascii("") is None


def test_split_commas():
    assert pre.split_commas("A, B ,C,, ") == ["A", "B", "C"]
    assert pre.split_commas(None) == []


# ────────────────────────────────────────────────────────────────────────────────
#  Unit tests for *harmonise()*
# ────────────────────────────────────────────────────────────────────────────────

from etl.harmonise import harmonise  # noqa: E402  – imported after fixtures


def test_harmonise_dedup(sample_records, sample_mapping):
    staging = harmonise(sample_records, sample_mapping)
    # The version suffix should be stripped and duplicates collapsed
    assert staging[("Genes", "gene_id")] == {"ENSG00000123456"}
    # lower‑casing & trimming should have been applied
    assert staging[("Stimuli", "label")] == {"butyrate"}


# ────────────────────────────────────────────────────────────────────────────────
#  Unit & regression tests for *load()*
# ────────────────────────────────────────────────────────────────────────────────

from etl.load import load  # noqa: E402  – imported late on purpose


class DummyCursor(SimpleNamespace):
    """Mimics *sqlalchemy.engine.Connection* just enough for the test."""

    def execute(self, sql):
        # store the last executed SQL for assertion
        self.last_sql = sql


class DummyConn(SimpleNamespace):
    def __enter__(self):
        return self.cursor

    def __exit__(self, exc_type, exc, tb):
        return False  # propagate exceptions


class DummyEngine(SimpleNamespace):
    def begin(self):
        return self.connection


@pytest.fixture()
def dummy_engine(monkeypatch):
    """Patch *sqlalchemy.create_engine* → Dummy objects so no real DB is touched."""

    cursor = DummyCursor()
    connection = DummyConn(cursor=cursor)
    engine = DummyEngine(connection=connection)

    with monkeypatch.context() as m:
        m.setitem(sys.modules, "sqlalchemy", mock.MagicMock())
        import sqlalchemy  # type: ignore  # noqa: F401

        sqlalchemy.create_engine.return_value = engine  # type: ignore
        yield engine, cursor  # hand back for assertions


@pytest.mark.usefixtures("dummy_engine")
def test_load_upsert(monkeypatch, sample_records, sample_mapping, dummy_engine):
    engine, cursor = dummy_engine

    # Need a real pandas.DataFrame.to_sql – monkeypatch it with a stub that records calls
    def fake_to_sql(name, conn, index, if_exists):  # noqa: D401 – tiny helper
        fake_to_sql.called = True
        assert name == "#temp"
        assert if_exists == "replace"

    fake_to_sql.called = False

    monkeypatch.setattr(pd.DataFrame, "to_sql", fake_to_sql, raising=True)

    staging = harmonise(sample_records, sample_mapping)

    load(staging, "mysql://user:pass@localhost/fakeDB")

    # 1) DataFrame.to_sql was called
    assert fake_to_sql.called is True

    # 2) The generated SQL contains the expected pieces
    assert re.search(r"INSERT INTO Genes \(gene_id\)", cursor.last_sql)
    assert "ON DUPLICATE KEY UPDATE" in cursor.last_sql


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

from get_remote_dataset import download_geo_supp_file  # noqa: E402


def test_download_geo_bad_gse(capsys):
    # Malformed accession should *not* try to touch the network
    download_geo_supp_file("BAD123", "foo.txt")
    captured = capsys.readouterr()
    assert "Invalid GEO Series ID" in captured.out
