"""Scenario business-rule engine: entity identity, lifecycle, cross-field logic."""

from __future__ import annotations

from .engine import apply_business_rules, normalize_scenario_key

__all__ = ["apply_business_rules", "normalize_scenario_key"]
