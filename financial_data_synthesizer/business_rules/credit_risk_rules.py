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


def apply_credit_risk_rules(tables: dict[str, list[dict[str, Any]]], seed: int) -> dict[str, list[dict[str, Any]]]:
    """
    Credit risk: income vs loan principal (DTI-style), repayment status vs risk snapshot,
    regional product policy.
    """
    rng = random.Random(seed + 17)

    for row in tables.get("borrowers", []):
        inc = float(row.get("annual_income") or 50000)
        emp = int(row.get("employment_years") or 1)
        reg = row.get("region", "other")
        prof = _parse_j(row.get("profile_json"))
        prof.update(
            {
                "employment_stability": "high" if emp >= 5 else "medium" if emp >= 2 else "low",
                "regulatory_region": reg,
            }
        )
        row["profile_json"] = _j(prof)
        # Income floor by region (simplified policy)
        if reg in ("NE", "SE") and inc < 40000:
            row["annual_income"] = round(inc * rng.uniform(1.05, 1.25), 2)

    borrowers = {r["borrower_id"]: r for r in tables.get("borrowers", [])}
    loans_by_id: dict[str, dict[str, Any]] = {}
    for row in tables.get("loan_contracts", []):
        bid = row.get("borrower_id")
        b = borrowers.get(bid, {})
        inc = float(b.get("annual_income") or 50000)
        principal = float(row.get("principal") or 10000)
        # Debt-to-income style cap (very rough)
        max_p = max(5000.0, inc * rng.uniform(2.0, 4.5))
        principal = min(principal, max_p)
        rate = float(row.get("rate") or 0.05)
        if principal > inc * 3:
            rate += rng.uniform(0.005, 0.02)
        row["principal"] = round(principal, 2)
        row["rate"] = round(min(0.25, max(0.02, rate)), 6)

        cj = _parse_j(row.get("contract_json"))
        cj.update(
            {
                "product_family": rng.choice(["term_loan", "revolver", "installment"]),
                "underwriting_band": "prime" if inc > 80000 else "near_prime" if inc > 45000 else "subprime",
            }
        )
        row["contract_json"] = _j(cj)
        loans_by_id[row["loan_id"]] = {**row, "borrower_income": inc}

    for row in tables.get("repayment_history", []):
        lid = row.get("loan_id")
        lc = loans_by_id.get(lid, {})
        principal = float(lc.get("principal") or 1.0)
        paid = float(row.get("paid_amount") or 0)
        paid = min(paid, principal * rng.uniform(0.05, 1.1))
        row["paid_amount"] = round(max(0.0, paid), 2)

        st = row.get("status", "on_time")
        if st == "missed" and rng.random() < 0.4:
            row["status"] = "late"
        st2 = row.get("status", "on_time")
        dj = _parse_j(row.get("details_json"))
        dj.update({"collections_stage": 1 if st2 in ("late", "missed") else 0})
        row["details_json"] = _j(dj)

    for row in tables.get("risk_indicators", []):
        lid = row.get("loan_id")
        lc = loans_by_id.get(lid, {})
        principal = float(lc.get("principal") or 1.0)
        inc = float(lc.get("borrower_income") or 50000)
        pti = principal / max(inc, 1.0)
        pd = float(row.get("pd") or 0.05)
        pd = min(0.99, max(0.001, pd + pti * 0.08))
        lgd = float(row.get("lgd") or 0.35)
        lgd = min(0.85, max(0.05, lgd + rng.uniform(-0.05, 0.05)))
        row["pd"] = round(pd, 6)
        row["lgd"] = round(lgd, 6)

        fj = _parse_j(row.get("features_json"))
        fj.update({"pti": round(pti, 4), "model_version": "v1-rules"})
        row["features_json"] = _j(fj)

    return tables
