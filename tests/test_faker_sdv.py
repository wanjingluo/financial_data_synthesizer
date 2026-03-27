import pytest

from financial_data_synthesizer.schema_from_scenario import schema_for_scenario
from financial_data_synthesizer.synthesis import GenerationConfig, SyntheticDataGenerator


def test_faker_enriches_name():
    pytest.importorskip("faker")
    schema = schema_for_scenario("crm")
    gen = SyntheticDataGenerator(schema, GenerationConfig(seed=99, default_rows=5, use_faker=True))
    tables = gen.generate_tables()
    name = tables["customers"][0]["name"]
    assert isinstance(name, str) and len(name) > 3 and " " in name


@pytest.mark.slow
def test_sdv_bootstrap_smoke():
    pytest.importorskip("sdv")
    pytest.importorskip("faker")
    from financial_data_synthesizer.sdv_integration import run_sdv_from_training_frames

    schema = schema_for_scenario("crm")
    gen = SyntheticDataGenerator(schema, GenerationConfig(seed=1, default_rows=40, use_faker=False))
    base = gen.generate_tables()
    import pandas as pd

    dfs = {k: pd.DataFrame(v) for k, v in base.items()}
    out = run_sdv_from_training_frames(dfs, seed=2, scale=1.1)
    assert "customers" in out and len(out["customers"]) >= 40
