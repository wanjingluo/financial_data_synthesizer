from __future__ import annotations

import math
import random
import string
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterator

from .json_values import synthetic_json_for_column
from .models import ColumnKind, DataSchema, Table
from .topology import table_generation_order


def _id_string(rng: random.Random, prefix: str) -> str:
    return f"{prefix}_{uuid.UUID(int=rng.getrandbits(128)).hex[:12]}"


def _skewed_amount(rng: random.Random, mean: float = 120.0, sigma: float = 1.2) -> float:
    """Log-normal style amounts (positive, right-skewed)."""
    return round(math.exp(rng.normalvariate(math.log(mean), sigma)) * (0.5 + rng.random()), 2)


def _ticker_symbol(rng: random.Random) -> str:
    """Exchange-style tickers: mostly recognizable-style symbols, not random mixed-case noise."""
    pool = (
        "AAPL",
        "MSFT",
        "GOOGL",
        "AMZN",
        "META",
        "JPM",
        "BAC",
        "XOM",
        "SPY",
        "QQQ",
        "EURUSD",
        "GBPUSD",
        "USDJPY",
        "GLD",
        "USO",
        "TSLA",
        "NVDA",
        "V",
    )
    if rng.random() < 0.65:
        return rng.choice(pool)
    k = rng.randint(3, 5)
    return "".join(rng.choices(string.ascii_uppercase, k=k))


def _age(rng: random.Random) -> int:
    v = int(rng.normalvariate(42, 14))
    return max(18, min(95, v))


def _weighted_choice(rng: random.Random, choices: list[str], weights: list[float]) -> str:
    r = rng.random() * sum(weights)
    acc = 0.0
    for c, w in zip(choices, weights):
        acc += w
        if r <= acc:
            return c
    return choices[-1]


@dataclass
class GenerationConfig:
    seed: int = 42
    default_rows: int = 1000
    per_table_rows: dict[str, int] | None = None
    batch_size: int = 50_000
    use_faker: bool = False


class SyntheticDataGenerator:
    """Generates referentially consistent rows with plausible distributions."""

    def __init__(self, schema: DataSchema, config: GenerationConfig | None = None):
        self.schema = schema
        self.config = config or GenerationConfig()
        self.rng = random.Random(self.config.seed)
        self._pk_values: dict[str, list[Any]] = defaultdict(list)
        self._categorical_defaults = self._build_categorical_defaults()
        self._faker = None
        if self.config.use_faker:
            from .faker_bridge import FakerBridge

            self._faker = FakerBridge(self.config.seed)
        # Lazy Faker for `full_name` when `--use-faker` is off (optional dep `faker`)
        self._lazy_name_faker: Any = None

    def _name_faker_instance(self) -> Any:
        """One shared Faker for realistic `full_name`; False if `faker` is not installed."""
        if self._lazy_name_faker is not None:
            return self._lazy_name_faker
        try:
            from faker import Faker

            f = Faker("en_US")
            f.seed_instance(self.config.seed)
            self._lazy_name_faker = f
            return f
        except ImportError:
            self._lazy_name_faker = False
            return False

    def _build_categorical_defaults(self) -> dict[tuple[str, str], list[str]]:
        d: dict[tuple[str, str], list[str]] = {}
        for t in self.schema.tables:
            for c in t.columns:
                if c.kind == ColumnKind.CATEGORICAL and c.categorical_values:
                    d[(t.name, c.name)] = c.categorical_values
        return d

    def _categorical(self, table: str, col: str, rng: random.Random) -> str:
        key = (table, col)
        if key in self._categorical_defaults:
            return rng.choice(self._categorical_defaults[key])
        name = col.lower()
        if "country" in name:
            return _weighted_choice(
                rng,
                ["US", "GB", "DE", "FR", "JP", "SG", "AU", "CA"],
                [0.35, 0.12, 0.1, 0.08, 0.1, 0.08, 0.09, 0.08],
            )
        if "account_type" in name or "type" in name:
            return _weighted_choice(
                rng,
                ["checking", "savings", "investment", "credit"],
                [0.4, 0.3, 0.2, 0.1],
            )
        if "currency" in name:
            return _weighted_choice(rng, ["USD", "EUR", "GBP", "JPY", "SGD"], [0.5, 0.2, 0.1, 0.1, 0.1])
        if "side" in name:
            return rng.choice(["buy", "sell"])
        if "status" in name:
            return _weighted_choice(
                rng,
                ["pending", "filled", "cancelled", "rejected"],
                [0.15, 0.7, 0.1, 0.05],
            )
        return f"cat_{rng.randint(1, 20)}"

    def _rows_for_table(self, name: str) -> int:
        if self.config.per_table_rows and name in self.config.per_table_rows:
            return self.config.per_table_rows[name]
        return self.config.default_rows

    def _value_for_column(
        self,
        table: Table,
        col_name: str,
        col,
        row_index: int,
    ) -> Any:
        rng = self.rng
        tname = table.name
        if col.fk_ref_table and col.fk_ref_column:
            parents = self._pk_values.get(col.fk_ref_table, [])
            if not parents:
                raise ValueError(f"Missing parent PKs for {tname}.{col_name} -> {col.fk_ref_table}")
            return rng.choice(parents)

        if self._faker:
            fv = self._faker.maybe_value(tname, col)
            if fv is not None:
                return fv

        if col.is_primary_key:
            if "uuid" in col.name.lower() or col.kind == ColumnKind.STRING:
                return _id_string(rng, col.name.split("_")[0] if "_" in col.name else "id")
            return row_index + 1

        k = col.kind
        if k in (ColumnKind.INTEGER,):
            if "age" in col.name.lower():
                return _age(rng)
            return rng.randint(1, 10**6)
        if k in (ColumnKind.FLOAT, ColumnKind.NUMERIC):
            if "balance" in col.name.lower() or "exposure" in col.name.lower():
                return round(max(0, rng.lognormvariate(8, 1.2)), 2)
            if "amount" in col.name.lower() or "notional" in col.name.lower():
                return _skewed_amount(rng)
            return round(rng.normalvariate(1000, 200), 2)
        if k == ColumnKind.CATEGORICAL:
            return self._categorical(tname, col.name, rng)
        if k == ColumnKind.JSON:
            return synthetic_json_for_column(col.name, tname, rng)
        if k == ColumnKind.TIMESTAMP:
            base = datetime.now(timezone.utc) - timedelta(days=rng.randint(0, 730))
            base -= timedelta(seconds=rng.randint(0, 86400))
            return base.isoformat()
        if k == ColumnKind.BOOLEAN:
            return rng.random() < 0.5
        if k == ColumnKind.STRING:
            nl = col.name.lower()
            if nl == "full_name":
                f_inst = self._name_faker_instance()
                if f_inst:
                    return f_inst.name()
                return f"{rng.choice(['Alex', 'Sam', 'Jordan', 'Casey'])} {rng.choice(['Lee', 'Pat', 'Ng', 'Wu'])}"
            if nl == "name":
                return f"{rng.choice(['Alex','Sam','Jordan','Casey'])} {rng.choice(['Lee','Pat','Ng','Wu'])}"
            # DDL often uses TEXT for codes (currency, country, account_type). Do not use random letters.
            if "ticker" in nl or nl in ("symbol",) or nl.endswith("_symbol"):
                return _ticker_symbol(rng)
            if (
                "currency" in nl
                or "country" in nl
                or "account_type" in nl
                or nl in ("channel", "status", "side", "region", "account_type")
            ):
                return self._categorical(tname, col.name, rng)
            return "".join(rng.choices(string.ascii_letters, k=rng.randint(6, 14)))

        return None

    def _register_primary_keys(self, table: Table, rows: list[dict[str, Any]]) -> None:
        pk_cols = [c.name for c in table.columns if c.is_primary_key]
        if not pk_cols:
            return
        if len(pk_cols) == 1:
            self._pk_values[table.name] = [r[pk_cols[0]] for r in rows]
        else:
            self._pk_values[table.name] = [tuple(r[c] for c in pk_cols) for r in rows]

    def generate_tables(self) -> dict[str, list[dict[str, Any]]]:
        order = table_generation_order(self.schema)
        out: dict[str, list[dict[str, Any]]] = {}
        for tname in order:
            table = self.schema.table_by_name(tname)
            if not table:
                continue
            n = self._rows_for_table(tname)
            rows: list[dict[str, Any]] = []
            for i in range(n):
                row: dict[str, Any] = {}
                for col in table.columns:
                    row[col.name] = self._value_for_column(table, col.name, col, i)
                rows.append(row)
            self._register_primary_keys(table, rows)
            out[tname] = rows
        return out

    def iter_batches(
        self,
        table: Table,
        total_rows: int,
        value_fn: Callable[[Table, str, Any, int], Any] | None = None,
    ) -> Iterator[list[dict[str, Any]]]:
        """Stream batches for large outputs (caller must supply PK/FK state)."""
        fn = value_fn or (lambda tbl, cn, c, i: self._value_for_column(tbl, cn, c, i))
        bs = max(1, self.config.batch_size)
        done = 0
        while done < total_rows:
            chunk_n = min(bs, total_rows - done)
            batch: list[dict[str, Any]] = []
            for i in range(chunk_n):
                idx = done + i
                row = {}
                for col in table.columns:
                    row[col.name] = fn(table, col.name, col, idx)
                batch.append(row)
            done += chunk_n
            yield batch
