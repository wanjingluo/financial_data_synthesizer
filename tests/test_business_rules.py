import json
import os
from collections import defaultdict

from financial_data_synthesizer.business_rules import apply_business_rules
from financial_data_synthesizer.business_rules.crm_rules import apply_crm_rules
from financial_data_synthesizer.schema_from_scenario import schema_for_scenario
from financial_data_synthesizer.synthesis import GenerationConfig, SyntheticDataGenerator

_ALLOWED_BANKING = {
    ("opened", "active"),
    ("active", "active"),
    ("active", "frozen"),
    ("active", "closed"),
    ("frozen", "frozen"),
    ("frozen", "active"),
    ("frozen", "closed"),
    ("closed", "closed"),
}


def test_crm_business_rules_embed_entity_and_lifecycle():
    schema = schema_for_scenario("crm")
    gen = SyntheticDataGenerator(schema, GenerationConfig(seed=7, default_rows=20))
    tables = gen.generate_tables()
    out = apply_business_rules("crm", tables, seed=7)
    prof = json.loads(out["customers"][0]["profile_json"])
    assert "entity_type" in prof and "lifecycle_stage" in prof
    assert out["accounts"][0]["account_type"] in ("checking", "savings", "investment", "credit")
    assert "relationship_managers" in out and "client_accounts" in out and "coverage_assignments" in out
    assert out["client_accounts"] and "serving_account_id" in out["client_accounts"][0]


def test_no_op_without_matching_scenario():
    assert apply_business_rules(None, {"a": []}, seed=1) == {"a": []}


def test_banking_status_chain_per_account_id():
    schema = schema_for_scenario("banking")
    gen = SyntheticDataGenerator(schema, GenerationConfig(seed=11, default_rows=15))
    tables = gen.generate_tables()
    out = apply_business_rules("banking", tables, seed=11)

    by_acct: dict[str, list[dict]] = defaultdict(list)
    for row in out["account_status_history"]:
        by_acct[str(row["account_id"])].append(row)

    for aid, rows in by_acct.items():
        rows.sort(key=lambda r: r["event_time"])
        assert rows[0]["status"] == "opened"
        for a, b in zip(rows, rows[1:]):
            assert (a["status"], b["status"]) in _ALLOWED_BANKING
        acc = next(x for x in out["bank_accounts"] if str(x["account_id"]) == aid)
        assert acc["current_status"] == rows[-1]["status"]


def test_client_coverage_primary_rm_and_kyc_eligibility():
    os.environ["FDS_AS_OF_DATE"] = "2026-06-01"
    try:
        schema = schema_for_scenario("client_coverage")
        gen = SyntheticDataGenerator(schema, GenerationConfig(seed=3, default_rows=40))
        tables = gen.generate_tables()
        out = apply_business_rules("client_coverage", tables, seed=3)
    finally:
        del os.environ["FDS_AS_OF_DATE"]

    as_of = __import__("datetime").date(2026, 6, 1)
    for acc in out["client_accounts"]:
        st = acc["account_status"]
        kyc = acc["kyc_status"]
        kexp = acc.get("kyc_expiry_date") or ""
        san = acc["sanctions_status"]
        from datetime import date as _date

        s = str(kexp).strip()[:10]
        y, m, d = int(s[0:4]), int(s[5:7]), int(s[8:10])
        kd = _date(y, m, d)
        expect = kyc == "Active" and san == "Clear" and kd >= as_of
        assert acc["is_servicing_eligible"] is expect
        if st == "Active":
            assert acc.get("coverage_owner_id")
            prim = [
                x
                for x in out["coverage_assignments"]
                if x["serving_account_id"] == acc["serving_account_id"] and x["assignment_type"] == "Primary"
            ]
            assert len(prim) == 1
            assert prim[0]["rm_id"] == acc["coverage_owner_id"]
        else:
            assert acc.get("coverage_owner_id") in (None, "")


def test_client_coverage_kyc_minimal():
    os.environ["FDS_AS_OF_DATE"] = "2025-01-10"
    try:
        tables = {
            "relationship_managers": [
                {
                    "rm_id": "rm_x",
                    "full_name": "A",
                    "home_region": "AMER",
                    "product_focus": "All",
                    "metadata_json": "{}",
                }
            ],
            "customers": [
                {
                    "customer_id": "c1",
                    "name": "Co",
                    "age": 40,
                    "country": "US",
                    "profile_json": "{}",
                }
            ],
            "client_accounts": [
                {
                    "serving_account_id": "a1",
                    "customer_id": "c1",
                    "account_status": "Active",
                    "kyc_status": "Active",
                    "kyc_expiry_date": "2025-12-01T00:00:00+00:00",
                    "sanctions_status": "Clear",
                    "coverage_owner_id": "rm_x",
                    "is_servicing_eligible": False,
                    "account_metadata_json": "{}",
                }
            ],
            "coverage_assignments": [],
        }
        out = apply_crm_rules(tables, seed=1)
    finally:
        del os.environ["FDS_AS_OF_DATE"]

    assert out["client_accounts"][0]["is_servicing_eligible"] is True
    ca = [x for x in out["coverage_assignments"] if x["serving_account_id"] == "a1" and x["assignment_type"] == "Primary"]
    assert len(ca) == 1
