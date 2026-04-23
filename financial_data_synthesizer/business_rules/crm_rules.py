from __future__ import annotations

import json
import os
import random
import re
import uuid
from datetime import date, datetime, timedelta, timezone
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


def _as_of_date() -> date:
    raw = os.environ.get("FDS_AS_OF_DATE", "").strip()
    if raw:
        try:
            y, m, d = (int(x) for x in raw[:10].split("-"))
            return date(y, m, d)
        except ValueError:
            pass
    return datetime.now(timezone.utc).date()


def _parse_kyc_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date() if value.tzinfo else value.replace(tzinfo=timezone.utc).date()
    s = str(value).strip()[:10]
    if len(s) < 10 or s[4] != "-" or s[7] != "-":
        return None
    try:
        y, m, d = int(s[0:4]), int(s[5:7]), int(s[8:10])
        return date(y, m, d)
    except ValueError:
        return None


def _compute_is_servicing_eligible(
    kyc_status: str,
    kyc_expiry: date | None,
    sanctions_status: str,
    as_of: date,
) -> bool:
    if (kyc_status or "").strip() != "Active":
        return False
    if (sanctions_status or "").strip() != "Clear":
        return False
    if kyc_expiry is None:
        return False
    return kyc_expiry >= as_of


def _servicing_restriction_flags(
    eligible: bool,
    kyc_status: str,
    kyc_expiry: date | None,
    sanctions_status: str,
    as_of: date,
) -> dict[str, Any]:
    if eligible:
        return {
            "new_products_blocked": False,
            "limit_changes_blocked": False,
            "deal_close_blocked": False,
        }
    return {
        "new_products_blocked": True,
        "limit_changes_blocked": True,
        "deal_close_blocked": True,
        "reasons": {
            "kyc_not_active": (kyc_status or "") != "Active",
            "kyc_expired_or_missing": kyc_expiry is None or kyc_expiry < as_of,
            "sanctions_not_clear": (sanctions_status or "") != "Clear",
        },
    }


def _apply_client_coverage_kyc(
    tables: dict[str, list[dict[str, Any]]], seed: int
) -> None:
    """
    (client_coverage scenario) Primary RM + optional segmented sharing; is_servicing_eligible.
    """
    rng = random.Random(seed + 131)
    as_of = _as_of_date()
    rms = tables.get("relationship_managers", [])
    rm_ids = [str(r.get("rm_id", "")) for r in rms if r.get("rm_id")]
    if not rm_ids:
        return

    accounts = tables.get("client_accounts", [])
    assignments: list[dict[str, Any]] = []
    seq = 0

    for acc in accounts:
        aid = acc.get("serving_account_id")
        if not aid:
            continue
        kyc_st = (acc.get("kyc_status") or "").strip() or "Pending"
        kyc_exp = _parse_kyc_date(acc.get("kyc_expiry_date"))
        san = (acc.get("sanctions_status") or "").strip() or "Under_Review"
        st = (acc.get("account_status") or "").strip() or "Dormant"

        eligible = _compute_is_servicing_eligible(kyc_st, kyc_exp, san, as_of)
        acc["is_servicing_eligible"] = eligible
        acc["kyc_status"] = kyc_st
        acc["sanctions_status"] = san
        acc["account_status"] = st

        meta = _parse_j(acc.get("account_metadata_json"))
        meta["is_servicing_eligible_computed"] = eligible
        meta["as_of_date_utc"] = as_of.isoformat()
        meta["servicing_restriction"] = _servicing_restriction_flags(
            eligible, kyc_st, kyc_exp, san, as_of
        )
        if not eligible:
            meta["account_servicing_note"] = (
                "Restricted for new products, limit changes, and deal close until KYC/sanctions criteria met."
            )
        acc["account_metadata_json"] = _j(meta)

        if st != "Active":
            acc["coverage_owner_id"] = None
            continue

        primary_rm = rng.choice(rm_ids)
        acc["coverage_owner_id"] = primary_rm

        seq += 1
        assignments.append(
            {
                "coverage_assignment_id": f"cov_{seq:08d}_{uuid.uuid4().hex[:8]}",
                "serving_account_id": aid,
                "rm_id": primary_rm,
                "assignment_type": "Primary",
                "coverage_scope": "Full",
                "region_code": "",
                "product_line": "",
                "details_json": _j(
                    {
                        "role": "Primary_RM",
                        "single_primary_enforced": True,
                    }
                ),
            }
        )

        if rng.random() < 0.25:
            sec = rng.choice([x for x in rm_ids if x != primary_rm] or rm_ids)
            seq += 1
            assignments.append(
                {
                    "coverage_assignment_id": f"cov_{seq:08d}_{uuid.uuid4().hex[:8]}",
                    "serving_account_id": aid,
                    "rm_id": sec,
                    "assignment_type": "Product_Shared",
                    "coverage_scope": "Product_Line",
                    "region_code": "",
                    "product_line": rng.choice(["Equities", "FX", "Lending", "Derivatives"]),
                    "details_json": _j(
                        {
                            "segmented_ownership": True,
                            "shared_coverage_exception": "product_line",
                        }
                    ),
                }
            )
        if rng.random() < 0.12:
            reg = rng.choice([x for x in rm_ids if x != primary_rm] or rm_ids)
            seq += 1
            assignments.append(
                {
                    "coverage_assignment_id": f"cov_{seq:08d}_{uuid.uuid4().hex[:8]}",
                    "serving_account_id": aid,
                    "rm_id": reg,
                    "assignment_type": "Regional_Shared",
                    "coverage_scope": "Region",
                    "region_code": rng.choice(["AMER", "EMEA", "APAC"]),
                    "product_line": "",
                    "details_json": _j(
                        {
                            "segmented_ownership": True,
                            "shared_coverage_exception": "region",
                        }
                    ),
                }
            )

    tables["coverage_assignments"] = assignments


# --- Banking scenario: ordered account status history (per account_id chain) ---

_BANKING_TRANSITION_WEIGHTS: dict[str, list[tuple[str, float]]] = {
    "opened": [("active", 1.0)],
    "active": [("active", 0.55), ("frozen", 0.28), ("closed", 0.17)],
    "frozen": [("frozen", 0.2), ("active", 0.55), ("closed", 0.25)],
    "closed": [("closed", 1.0)],
}


def _parse_banking_ts(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not value:
        return datetime.now(timezone.utc)
    s = str(value).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _sample_banking_next_status(rng: random.Random, current: str) -> str:
    choices = _BANKING_TRANSITION_WEIGHTS.get(current, [("closed", 1.0)])
    states, weights = zip(*choices)
    return rng.choices(list(states), weights=list(weights), k=1)[0]


def _safe_banking_event_suffix(account_id: str, idx: int) -> str:
    alnum = re.sub(r"[^a-zA-Z0-9]+", "_", account_id).strip("_") or "acct"
    return f"{alnum}_{idx}"


def _apply_banking_status_history(tables: dict[str, list[dict[str, Any]]], seed: int) -> None:
    """
    (banking scenario) Rebuild account_status_history: time-ordered state chain per account_id;
    set bank_accounts.current_status to latest.
    """
    rng = random.Random(seed + 41)
    accounts = tables.get("bank_accounts", [])
    if not accounts:
        return

    accounts_sorted = sorted(accounts, key=lambda r: str(r.get("account_id", "")))
    history_out: list[dict[str, Any]] = []
    event_seq = 0

    for acc in accounts_sorted:
        aid = acc.get("account_id")
        if not aid:
            continue
        base = _parse_banking_ts(acc.get("opened_at"))
        n_events = rng.randint(2, 10)
        cursor = base
        prev: str | None = None
        for i in range(n_events):
            if i == 0:
                status = "opened"
            else:
                assert prev is not None
                status = _sample_banking_next_status(rng, prev)
            t = cursor
            if i < n_events - 1:
                cursor += timedelta(hours=rng.randint(1, 120), minutes=rng.randint(0, 59))
            event_seq += 1
            eid = f"evt_{event_seq:08d}_{_safe_banking_event_suffix(str(aid), i)}"
            det = _parse_j(acc.get("metadata_json"))
            det.update({"event_index": i, "previous_status": prev})
            history_out.append(
                {
                    "event_id": eid,
                    "account_id": aid,
                    "event_time": t.isoformat(),
                    "status": status,
                    "details_json": _j(det),
                }
            )
            prev = status

        last = prev or "opened"
        acc["current_status"] = last
        meta = _parse_j(acc.get("metadata_json"))
        meta.update({"status_chain_terminal": last == "closed"})
        acc["metadata_json"] = _j(meta)

    tables["account_status_history"] = history_out


def apply_crm_rules(tables: dict[str, list[dict[str, Any]]], seed: int) -> dict[str, list[dict[str, Any]]]:
    """
    CRM: customers/accounts/transactions/interactions; optional client_coverage + KYC;
    optional banking status history when `bank_accounts` is present.
    """
    customers = tables.get("customers", [])
    if customers:
        rng = random.Random(seed)
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

            prof = _parse_j(row.get("profile_json"))
            prof.pop("kyc_status", None)
            prof.update(
                {
                    "entity_type": entity_type,
                    "lifecycle_stage": lifecycle,
                    "tier": tier,
                    "segment": "business" if entity_type == "corporate" else "retail",
                }
            )
            row["profile_json"] = _j(prof)
            by_cid[cid] = {
                "entity_type": entity_type,
                "lifecycle": lifecycle,
                "tier": tier,
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

    if tables.get("client_accounts") and tables.get("relationship_managers"):
        _apply_client_coverage_kyc(tables, seed)

    if tables.get("bank_accounts"):
        _apply_banking_status_history(tables, seed)

    return tables
