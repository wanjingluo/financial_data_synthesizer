import sqlite3
from pathlib import Path

from financial_data_synthesizer.exporters import export_sqlite
from financial_data_synthesizer.parsers import load_schema_sql
from financial_data_synthesizer.synthesis import GenerationConfig, SyntheticDataGenerator

ROOT = Path(__file__).resolve().parents[1]


def test_referential_integrity_sqlite(tmp_path: Path):
    schema = load_schema_sql(ROOT / "data" / "sample_schema.sql")
    cfg = GenerationConfig(seed=7, default_rows=50, per_table_rows={"transactions": 200})
    gen = SyntheticDataGenerator(schema, cfg)
    tables = gen.generate_tables()
    db = tmp_path / "t.db"
    export_sqlite(schema, tables, db)
    conn = sqlite3.connect(str(db))
    try:
        cur = conn.execute("PRAGMA foreign_key_check")
        bad = cur.fetchall()
        assert bad == []
        c1 = conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
        c2 = conn.execute("SELECT COUNT(*) FROM accounts").fetchone()[0]
        c3 = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
        assert c1 == 50 and c2 == 50 and c3 == 200
    finally:
        conn.close()


def test_json_columns_are_strings():
    from financial_data_synthesizer.schema_from_scenario import schema_for_scenario

    schema = schema_for_scenario("crm")
    gen = SyntheticDataGenerator(schema, GenerationConfig(seed=1, default_rows=5))
    tables = gen.generate_tables()
    prof = tables["customers"][0]["profile_json"]
    assert isinstance(prof, str) and prof.startswith("{")


def test_relationship_managers_full_name_looks_like_person():
    from financial_data_synthesizer.schema_from_scenario import schema_for_scenario

    schema = schema_for_scenario("crm")
    gen = SyntheticDataGenerator(schema, GenerationConfig(seed=101, default_rows=4))
    tables = gen.generate_tables()
    for row in tables["relationship_managers"]:
        fn = row["full_name"]
        assert isinstance(fn, str) and " " in fn.strip() and len(fn) > 2
