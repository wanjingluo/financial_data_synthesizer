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


def apply_trading_rules(tables: dict[str, list[dict[str, Any]]], seed: int) -> dict[str, list[dict[str, Any]]]:
    """
    Trading: portfolio base currency vs order/execution; instrument asset_class vs
    order size and execution spread; cross-field product logic.
    """
    rng = random.Random(seed + 11)
    inst = {r["instrument_id"]: r for r in tables.get("instruments", [])}
    pf = {r["portfolio_id"]: r for r in tables.get("portfolios", [])}

    for row in tables.get("trade_orders", []):
        iid = row.get("instrument_id")
        pid = row.get("portfolio_id")
        ins = inst.get(iid, {})
        port = pf.get(pid, {})
        ac = ins.get("asset_class", "equity")
        base_ccy = port.get("base_currency") or "USD"

        if ac == "equity":
            row["quantity"] = round(max(1.0, float(row.get("quantity") or 0) or rng.uniform(10, 500)), 2)
            row["limit_price"] = round(max(0.01, float(row.get("limit_price") or 0) or rng.uniform(5, 500)), 4)
        elif ac == "fx":
            row["quantity"] = round(rng.uniform(10_000, 2_000_000), 2)
            row["limit_price"] = round(rng.uniform(0.5, 2.0), 6)
        elif ac == "bond":
            row["quantity"] = round(rng.uniform(50_000, 5_000_000), 2)
            row["limit_price"] = round(rng.uniform(0.5, 1.2), 4)
        else:
            row["quantity"] = round(max(1.0, float(row.get("quantity") or 1)), 2)
            row["limit_price"] = round(max(0.01, float(row.get("limit_price") or 1)), 4)

        # Side vs portfolio risk posture (simplified)
        row["side"] = rng.choice(["buy", "sell"]) if ac != "commodity" else rng.choices(
            ["buy", "sell"], weights=[0.55, 0.45]
        )[0]

        aj = _parse_j(row.get("attrs_json"))
        aj.update(
            {
                "routing": "dark_pool" if rng.random() < 0.12 else "lit",
                "venue_ccy": base_ccy,
            }
        )
        row["attrs_json"] = _j(aj)

    # Executions: fill near price, qty <= order
    orders = {r["order_id"]: r for r in tables.get("trade_orders", [])}
    for row in tables.get("trade_executions", []):
        oid = row.get("order_id")
        o = orders.get(oid, {})
        lp = float(o.get("limit_price") or 1.0)
        row["fill_price"] = round(lp * rng.uniform(0.999, 1.001), 6)
        oq = float(o.get("quantity") or 1.0)
        row["fill_qty"] = round(min(oq, oq * rng.uniform(0.2, 1.0)), 4)

        dj = _parse_j(row.get("details_json"))
        dj.update({"slippage_bps": round(rng.uniform(0.5, 8.0), 2)})
        row["details_json"] = _j(dj)

    return tables
