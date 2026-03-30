import json

from financial_data_synthesizer.business_rules import apply_business_rules
from financial_data_synthesizer.schema_from_scenario import schema_for_scenario
from financial_data_synthesizer.synthesis import GenerationConfig, SyntheticDataGenerator


def test_crm_business_rules_embed_entity_and_lifecycle():
    schema = schema_for_scenario("crm")
    gen = SyntheticDataGenerator(schema, GenerationConfig(seed=7, default_rows=20))
    tables = gen.generate_tables()
    out = apply_business_rules("crm", tables, seed=7)
    prof = json.loads(out["customers"][0]["profile_json"])
    assert "entity_type" in prof and "lifecycle_stage" in prof
    assert out["accounts"][0]["account_type"] in ("checking", "savings", "investment", "credit")


def test_no_op_without_matching_scenario():
    assert apply_business_rules(None, {"a": []}, seed=1) == {"a": []}
