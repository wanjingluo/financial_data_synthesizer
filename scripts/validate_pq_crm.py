#!/usr/bin/env python3
"""
Read CRM Parquet exports and validate:
  (A) Primary RM uniqueness and Active → coverage_owner_id; one Primary per active serving line.
  (B) is_servicing_eligible vs KYC / expiry / sanctions (as of today or FDS_AS_OF_DATE).

Usage:
  python scripts/validate_pq_crm.py out/pq0423
  python scripts/validate_pq_crm.py "C:/path/to/out/pq_0423"
  set FDS_AS_OF_DATE=2026-04-17   # optional, else calendar today (UTC)
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd


def _as_of_date() -> date:
    raw = os.environ.get("FDS_AS_OF_DATE", "").strip()
    if raw and len(raw) >= 10:
        try:
            y, m, d = (int(x) for x in raw[:10].split("-"))
            return date(y, m, d)
        except ValueError:
            pass
    return datetime.now(timezone.utc).date()


def _parse_kyc_date(value) -> date | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, pd.Timestamp):
        return value.date()
    s = str(value).strip()[:10]
    if len(s) < 10 or s[4] != "-" or s[7] != "-":
        return None
    try:
        y, m, d = int(s[0:4]), int(s[5:7]), int(s[8:10])
        return date(y, m, d)
    except ValueError:
        return None


def expected_servicing_eligible(
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


def run_validation(pq_dir: Path) -> int:
    as_of = _as_of_date()
    print(f"As-of date for KYC: {as_of.isoformat()}")

    ca_path = pq_dir / "client_accounts.parquet"
    cov_path = pq_dir / "coverage_assignments.parquet"
    for p in (ca_path, cov_path):
        if not p.is_file():
            print(f"ERROR: missing file: {p}", file=sys.stderr)
            return 2

    ca = pd.read_parquet(ca_path)
    cov = pd.read_parquet(cov_path)

    # ---- (A) Primary RM + Active coverage ----
    active = ca[ca["account_status"].astype(str).str.strip() == "Active"]
    a_errors: list[str] = []

    for _, row in active.iterrows():
        cid = row.get("coverage_owner_id")
        if cid is None or (isinstance(cid, float) and pd.isna(cid)) or str(cid).strip() == "":
            a_errors.append(
                f"serving_account_id={row.get('serving_account_id')}: Active but coverage_owner_id is null/empty"
            )

    if "serving_account_id" in cov.columns and "assignment_type" in cov.columns:
        prim = cov[cov["assignment_type"].astype(str).str.strip() == "Primary"].copy()
        prim["serving_account_id"] = prim["serving_account_id"].astype(str)
        primary_counts = prim.groupby("serving_account_id", dropna=False).size()
        for _, row in active.iterrows():
            sid = str(row["serving_account_id"])
            n = int(primary_counts.get(sid, 0)) if sid in primary_counts.index else 0
            if n != 1:
                a_errors.append(
                    f"serving_account_id={sid}: expected exactly one Primary in coverage_assignments, got {n}"
                )

    print("\n--- (A) Primary RM + Active coverage ---")
    if a_errors:
        print("FAILED:")
        for e in a_errors[:50]:
            print(f"  - {e}")
        if len(a_errors) > 50:
            print(f"  ... and {len(a_errors) - 50} more")
    else:
        print("OK: Active rows have non-null coverage_owner_id; each has exactly one Primary assignment.")

    # ---- (B) is_servicing_eligible ----
    b_errors: list[str] = []
    for _, row in ca.iterrows():
        kyc = str(row.get("kyc_status", "") or "")
        san = str(row.get("sanctions_status", "") or "")
        kexp = _parse_kyc_date(row.get("kyc_expiry_date"))
        col = row.get("is_servicing_eligible")
        if isinstance(col, (float, int)):
            if isinstance(col, float) and pd.isna(col):
                actual = None
            else:
                actual = bool(col) if not isinstance(col, float) else bool(int(col))
        else:
            actual = col if col is not None else None

        exp = expected_servicing_eligible(kyc, kexp, san, as_of)
        if actual is None:
            b_errors.append(f"serving_account_id={row.get('serving_account_id')}: is_servicing_eligible is null")
            continue
        if bool(actual) != exp:
            b_errors.append(
                f"serving_account_id={row.get('serving_account_id')}: is_servicing_eligible={actual} but expected {exp} "
                f"(kyc={kyc!r}, expiry={kexp}, sanctions={san!r})"
            )

    # Restriction note (metadata): optional check
    print("\n--- (B) KYC-driven is_servicing_eligible ---")
    if b_errors:
        print("FAILED:")
        for e in b_errors[:50]:
            print(f"  - {e}")
        if len(b_errors) > 50:
            print(f"  ... and {len(b_errors) - 50} more")
    else:
        print(
            "OK: is_servicing_eligible matches (kyc_status=='Active' AND kyc_expiry>=as_of AND sanctions=='Clear')."
        )

    if a_errors or b_errors:
        return 1
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="Validate CRM Parquet export (rules A & B).")
    p.add_argument("pq_dir", type=Path, help="Directory containing client_accounts.parquet and coverage_assignments.parquet")
    args = p.parse_args()
    if not args.pq_dir.is_dir():
        print(f"ERROR: not a directory: {args.pq_dir}", file=sys.stderr)
        return 2
    return run_validation(args.pq_dir)


if __name__ == "__main__":
    raise SystemExit(main())
