from __future__ import annotations

import json
import random
from typing import Any


def _pick(rng: random.Random, seq: list[Any]) -> Any:
    return rng.choice(seq)


def synthetic_json_for_column(
    column_name: str,
    table_name: str,
    rng: random.Random,
) -> str:
    """Produce realistic nested JSON as a string for SQLite/Parquet storage."""
    name_l = column_name.lower()
    t_l = table_name.lower()

    if "profile" in name_l or ("customer" in t_l and "json" in name_l):
        obj = {
            "tier": _pick(rng, ["standard", "gold", "private"]),
            "segment": _pick(rng, ["retail", "affluent", "business"]),
            "risk_score": round(rng.betavariate(2, 5) * 100, 2),
            "preferences": rng.sample(
                ["email", "sms", "app_push", "phone"],
                k=rng.randint(1, 3),
            ),
        }
    elif "metadata" in name_l:
        obj = {
            "branch_code": f"BR{rng.randint(1, 999):03d}",
            "channel": _pick(rng, ["branch", "online", "mobile"]),
            "fee_waiver": rng.random() < 0.15,
            "tags": rng.sample(["new", "vip", "fraud_watch", "tax_exempt"], k=rng.randint(0, 2)),
        }
    elif "summary" in name_l and "json" in name_l:
        obj = {
            "topic": _pick(rng, ["onboarding", "complaint", "product", "billing"]),
            "sentiment": _pick(rng, ["positive", "neutral", "negative"]),
            "duration_sec": rng.randint(30, 3600),
            "resolution": _pick(rng, ["resolved", "escalated", "pending"]),
        }
    elif "details" in name_l or "interaction" in t_l:
        obj = {
            "merchant": _pick(rng, ["Amazon", "Shell", "Local Cafe", "Wire Transfer"]),
            "mcc": str(rng.randint(5411, 5999)),
            "fx_rate": round(rng.lognormvariate(0, 0.05), 6) if rng.random() < 0.3 else None,
            "notes": _pick(rng, ["", "recurring", "disputed", "fee applied"]),
        }
    elif "xml" in name_l:
        root = "record"
        obj = {
            "_xml_hint": f"<{root}><id>{rng.randint(1, 10**9)}</id></{root}>",
            "payload": {"a": rng.randint(0, 100)},
        }
    else:
        obj = {
            "generated": True,
            "idx": rng.randint(0, 10**9),
            "attrs": {"k": _pick(rng, ["x", "y", "z"])},
        }
    return json.dumps(obj, ensure_ascii=False)
