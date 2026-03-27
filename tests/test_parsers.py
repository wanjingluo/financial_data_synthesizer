from pathlib import Path

from financial_data_synthesizer.parsers import load_schema_json, load_schema_sql
from financial_data_synthesizer.topology import table_generation_order

ROOT = Path(__file__).resolve().parents[1]


def test_parse_sample_sql():
    schema = load_schema_sql(ROOT / "data" / "sample_schema.sql")
    names = [t.name for t in schema.tables]
    assert names == ["customers", "accounts", "transactions"]
    acct = schema.table_by_name("accounts")
    assert acct
    cid = acct.column_by_name("customer_id")
    assert cid and cid.fk_ref_table == "customers"


def test_parse_full_json():
    schema = load_schema_json(ROOT / "data" / "sample_schema_full.json")
    order = table_generation_order(schema)
    assert order[0] == "customers"
    assert order[-1] == "transactions"


def test_topology_simple_chain():
    from financial_data_synthesizer.models import Column, ColumnKind, DataSchema, Table

    s = DataSchema(
        tables=[
            Table("a", [Column("id", ColumnKind.STRING, is_primary_key=True)]),
            Table(
                "b",
                [
                    Column("id", ColumnKind.STRING, is_primary_key=True),
                    Column("aid", ColumnKind.STRING, fk_ref_table="a", fk_ref_column="id"),
                ],
            ),
        ]
    )
    assert len(table_generation_order(s)) == 2
