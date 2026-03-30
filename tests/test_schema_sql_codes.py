from pathlib import Path

from financial_data_synthesizer.parsers import load_schema_sql
from financial_data_synthesizer.synthesis import GenerationConfig, SyntheticDataGenerator

ROOT = Path(__file__).resolve().parents[1]
_CODES = {"USD", "EUR", "GBP", "JPY", "SGD"}
_AT = {"checking", "savings", "investment", "credit"}


def test_ddl_text_columns_currency_and_account_type_are_codes_not_gibberish():
    schema = load_schema_sql(ROOT / "data" / "sample_schema.sql")
    gen = SyntheticDataGenerator(schema, GenerationConfig(seed=3, default_rows=30))
    tables = gen.generate_tables()
    for r in tables["transactions"][:20]:
        assert r["currency"] in _CODES
    for r in tables["accounts"][:20]:
        assert r["account_type"] in _AT
