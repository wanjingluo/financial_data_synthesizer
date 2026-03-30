from __future__ import annotations

from typing import Any, Callable

from . import credit_risk_rules, crm_rules, trading_rules

RuleFn = Callable[[dict[str, list[dict[str, Any]]], int], dict[str, list[dict[str, Any]]]]

_RULES: dict[str, RuleFn] = {
    "crm": crm_rules.apply_crm_rules,
    "trading": trading_rules.apply_trading_rules,
    "credit_risk": credit_risk_rules.apply_credit_risk_rules,
}


def normalize_scenario_key(name: str) -> str:
    k = name.strip().lower().replace(" ", "_").replace("-", "_")
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
    }
    return aliases.get(k, k)


def apply_business_rules(
    scenario: str | None,
    tables: dict[str, list[dict[str, Any]]],
    seed: int,
) -> dict[str, list[dict[str, Any]]]:
    """
    Apply scenario-specific business rules in-process (deterministic given seed).
    No-op if scenario is None or unknown.
    """
    if not scenario:
        return tables
    key = normalize_scenario_key(scenario)
    fn = _RULES.get(key)
    if not fn:
        return tables
    return fn(tables, seed)
