#!/usr/bin/env python3

from etl.harmonise import enrich_ontology_term, fetch_from_ols
from etl.utils.preprocessing import lowercase_ascii

print(enrich_ontology_term("CL:0000057"))
print("\n")
print(enrich_ontology_term("HsapDv:0000087"))
print("\n")
print(enrich_ontology_term("http://purl.obolibrary.org/obo/MONDO_0005148"))
print("\n")
print(fetch_from_ols("CL_0000057"))
print("\n")

text = "β-alanine"
normalized_text = lowercase_ascii(text)
print("Normalized text:", normalized_text)
print("\n\n")





# ______________________________________________________________________________________



import sys
import os
import sqlalchemy
from sqlalchemy import text
from main import engine, tunnel

# If one doesn't want to run in bash export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}" from root folder, uncomment the next
# PROJECT_ROOT = os.path.abspath(
#     os.path.join(__file__, os.pardir, os.pardir, os.pardir)
# )
# if PROJECT_ROOT not in sys.path:
#     sys.path.insert(0, PROJECT_ROOT)


def test_connection():
    try:
        with engine.connect() as conn:
            val = conn.execute(text("SELECT 1")).scalar()
        if val != 1:
            raise RuntimeError(f"Expected 1, got {val}")
        print("[✔] Connection test passed")
        return 1
    except Exception as e:
        print(f"[✖] Connection test failed: {e}")
        return 0
    
def test_core_tables():
    try:
        inspector = sqlalchemy.inspect(engine)
        existing = set(inspector.get_table_names())
        expected = {
            "Stimuli",
            "Microbes",
            "Genes",
            "Studies",
            "Samples",
            "ExpressionStats"
        }
        missing = expected - existing
        if missing:
            raise RuntimeError(f"Missing tables: {missing}")
        print(f"[✔] Core tables exist: {', '.join(sorted(expected))}")
        return 1
    except Exception as e:
        print(f"[✖] Table existence test failed: {e}")
        return 0

def test_temp_table():
    try:
        with engine.begin() as conn:
            # create a temp table, insert a value, read it back
            conn.execute(text("CREATE TEMPORARY TABLE tmp_test (id INT)"))
            conn.execute(text("INSERT INTO tmp_test (id) VALUES (42)"))
            val = conn.execute(text("SELECT id FROM tmp_test")).scalar()
        if val != 42:
            raise RuntimeError(f"Expected 42, got {val}")

        print("[✔] Temporary table test passed")
        return 1
    except Exception as e:
        print(f"[✖] Temporary table test failed: {e}")
        return 0

if __name__ == "__main__":
    print("Running DB smoke tests…")
    counter=0
    try:
        counter+=test_connection()
        counter+=test_core_tables()
        counter+=test_temp_table()
    except AssertionError as e:
        print(f"[✖] Test failed: {e}")
    finally:
        tunnel.stop()
        print("✅ All tests passed, tunnel closed.") if counter == 3 else print(f"{counter}/3 tests passed, tunnel closed.")
        sys.exit(1)
