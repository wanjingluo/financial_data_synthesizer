from __future__ import annotations

import json
import random
from typing import Any


def _j(obj: dict) -> str:
    return json.dumps(obj, ensure_ascii=False)


def _parse_j(s: Any) -> dict[str, Any]:
    if isinstance(s, dict):
        return dict(s)
    if not s or not isinstance(s, str):
        return {}
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        return {}


def apply_crm_rules(tables: dict[str, list[dict[str, Any]]], seed: int) -> dict[str, list[dict[str, Any]]]:
    """
    CRM: entity identity (retail vs corporate), customer lifecycle, cross-field
    dependencies (tier/balance/account_type), product line in metadata.
    """
    rng = random.Random(seed)
    customers = tables.get("customers", [])
    if not customers:
        return tables

    # Build per-customer business context
    by_cid: dict[str, dict[str, Any]] = {}
    for row in customers:
        cid = row["customer_id"]
        entity_type = rng.choices(["retail", "corporate"], weights=[0.72, 0.28])[0]
        lifecycle = rng.choices(
            ["onboarding", "active", "growth", "at_risk", "dormant"],
            weights=[0.08, 0.48, 0.22, 0.14, 0.08],
        )[0]
        if entity_type == "corporate":
            tier = rng.choices(["standard", "gold", "private"], weights=[0.3, 0.45, 0.25])[0]
        else:
            tier = rng.choices(["standard", "gold", "private"], weights=[0.7, 0.22, 0.08])[0]

        kyc = "pending" if lifecycle == "onboarding" else rng.choices(
            ["verified", "pending", "expired"],
            weights=[0.88, 0.08, 0.04],
        )[0]

        prof = _parse_j(row.get("profile_json"))
        prof.update(
            {
                "entity_type": entity_type,
                "lifecycle_stage": lifecycle,
                "tier": tier,
                "kyc_status": kyc,
                "segment": "business" if entity_type == "corporate" else "retail",
            }
        )
        row["profile_json"] = _j(prof)
        by_cid[cid] = {
            "entity_type": entity_type,
            "lifecycle": lifecycle,
            "tier": tier,
            "kyc": kyc,
        }

    # Accounts: product mix + balance scale by entity + lifecycle
    acct_by_id: dict[str, dict[str, Any]] = {}
    for row in tables.get("accounts", []):
        cid = row.get("customer_id")
        c = by_cid.get(cid, {})
        entity = c.get("entity_type", "retail")
        life = c.get("lifecycle", "active")
        tier = c.get("tier", "standard")

        if entity == "corporate":
            weights = [0.15, 0.12, 0.48, 0.25]
        else:
            weights = [0.48, 0.32, 0.15, 0.05]
        row["account_type"] = rng.choices(
            ["checking", "savings", "investment", "credit"],
            weights=weights,
        )[0]

        bal = float(row.get("balance") or 0)
        if entity == "corporate":
            bal *= rng.uniform(1.8, 5.0)
        if tier == "private":
            bal *= rng.uniform(1.2, 2.5)
        if tier == "gold":
            bal *= rng.uniform(1.05, 1.6)
        if life == "at_risk":
            bal *= rng.uniform(0.25, 0.65)
        elif life == "dormant":
            bal *= rng.uniform(0.05, 0.35)
        elif life == "growth":
            bal *= rng.uniform(1.05, 1.35)

        row["balance"] = round(max(0.0, bal), 2)

        meta = _parse_j(row.get("metadata_json"))
        meta.update(
            {
                "product_line": "commercial" if entity == "corporate" else "retail",
                "fee_tier": tier,
                "lifecycle_aligned": life,
            }
        )
        row["metadata_json"] = _j(meta)
        acct_by_id[row["account_id"]] = {**c, "balance": row["balance"], "account_type": row["account_type"]}

    # Transactions: amount vs account balance + lifecycle; currency mix
    for row in tables.get("transactions", []):
        aid = row.get("account_id")
        a = acct_by_id.get(aid, {})
        bal = float(a.get("balance") or 1.0)
        life = a.get("lifecycle", "active")
        frac = rng.uniform(0.001, 0.09) if life != "at_risk" else rng.uniform(0.03, 0.2)
        amt = max(1.0, bal * frac)
        if life == "at_risk":
            amt *= rng.uniform(1.1, 2.2)
        row["amount"] = round(amt, 2)

        if a.get("entity_type") == "corporate":
            row["currency"] = rng.choices(
                ["USD", "EUR", "GBP", "SGD", "JPY"],
                weights=[0.45, 0.2, 0.12, 0.13, 0.1],
            )[0]
        else:
            row["currency"] = rng.choices(
                ["USD", "EUR", "GBP", "JPY", "SGD"],
                weights=[0.55, 0.15, 0.1, 0.12, 0.08],
            )[0]

        det = _parse_j(row.get("details_json"))
        det.update(
            {
                "posting_reason": rng.choice(["purchase", "transfer", "fee", "interest", "payment"]),
                "risk_flag": life == "at_risk",
            }
        )
        row["details_json"] = _j(det)

    # Interactions: channel mix by lifecycle + entity
    for row in tables.get("customer_interactions", []):
        cid = row.get("customer_id")
        c = by_cid.get(cid, {})
        life = c.get("lifecycle", "active")
        ent = c.get("entity_type", "retail")
        if life == "at_risk":
            row["channel"] = rng.choices(
                ["branch", "phone", "chat", "email", "app"],
                weights=[0.15, 0.35, 0.2, 0.1, 0.2],
            )[0]
        elif ent == "corporate":
            row["channel"] = rng.choices(
                ["branch", "phone", "chat", "email", "app"],
                weights=[0.25, 0.25, 0.1, 0.3, 0.1],
            )[0]
        else:
            row["channel"] = rng.choices(
                ["branch", "phone", "chat", "email", "app"],
                weights=[0.15, 0.1, 0.15, 0.15, 0.45],
            )[0]
        summ = _parse_j(row.get("summary_json"))
        summ.update(
            {
                "intent": rng.choice(["service", "complaint", "sales", "collections"]),
                "priority": "high" if life == "at_risk" else "normal",
            }
        )
        row["summary_json"] = _j(summ)

    return tables
