from __future__ import annotations

from .models import DataSchema
from .scenario_templates import SCENARIOS


def schema_for_scenario(name: str) -> DataSchema:
    """
    Requirement 1 — generate a schema from a named business scenario.

    Supported: crm, trading, credit_risk, banking, client_coverage (aligned with Example Data Scenarios).
    """
    key = name.strip().lower().replace(" ", "_").replace("-", "_")
    aliases = {
        "scenario_1": "crm",
        "scenario1": "crm",
        "crm_system": "crm",
        "scenario_2": "trading",
        "scenario2": "trading",
        "trading_platform": "trading",
        "scenario_3": "credit_risk",
        "scenario3": "credit_risk",
        "credit": "credit_risk",
        "bank": "banking",
        "retail_banking": "banking",
        "rm_kyc": "client_coverage",
        "primary_rm": "client_coverage",
        "wealth": "client_coverage",
    }
    key = aliases.get(key, key)
    if key not in SCENARIOS:
        available = ", ".join(sorted(SCENARIOS))
        raise ValueError(f"Unknown scenario {name!r}. Choose one of: {available}")
    return SCENARIOS[key]
