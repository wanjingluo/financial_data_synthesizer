from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from .exporters import export_parquet_dir, export_sqlite
from .parsers import load_schema_json, load_schema_sql
from .schema_from_scenario import schema_for_scenario
from .sdv_integration import load_tables_from_sqlite, run_sdv_from_training_frames
from .synthesis import GenerationConfig, SyntheticDataGenerator


def _parse_rows(s: str | None) -> dict[str, int] | None:
    if not s:
        return None
    out: dict[str, int] = {}
    for part in s.split(","):
        part = part.strip()
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        out[k.strip()] = int(v.strip())
    return out or None


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Financial Data Synthesizer")
    sub = p.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("generate", help="Generate synthetic data from a schema file")
    g.add_argument("--schema-sql", type=Path, help="Path to SQLite-style DDL")
    g.add_argument("--schema-json", type=Path, help="Path to internal JSON schema")
    g.add_argument("--scenario", help="Built-in scenario: crm | trading | credit_risk")
    g.add_argument("--rows", type=int, default=1000, help="Default rows per table (base generator / SDV bootstrap)")
    g.add_argument(
        "--rows-map",
        help="Override per table, e.g. customers=500,accounts=2000,transactions=8000",
    )
    g.add_argument("--seed", type=int, default=42)
    g.add_argument("--use-faker", action="store_true", help="Use Faker for names, contacts, categories, etc.")
    g.add_argument(
        "--sdv-train-sqlite",
        type=Path,
        help="Fit SDV HMA on this SQLite DB, then sample (requires: pip install sdv)",
    )
    g.add_argument(
        "--sdv-bootstrap",
        action="store_true",
        help="First generate with the base engine, then fit SDV HMA on that sample and resample",
    )
    g.add_argument(
        "--sdv-scale",
        type=float,
        default=1.0,
        help="SDV sample scale vs training row counts (HMA multiplies each table by ~scale)",
    )
    g.add_argument("--sqlite", type=Path, help="Write SQLite database to this path")
    g.add_argument("--parquet-dir", type=Path, help="Write one Parquet file per table")

    args = p.parse_args(argv)

    if args.cmd != "generate":
        return 2

    if args.sdv_train_sqlite and args.sdv_bootstrap:
        p.error("Use either --sdv-train-sqlite or --sdv-bootstrap, not both")

    if args.scenario:
        schema = schema_for_scenario(args.scenario)
    elif args.schema_json:
        schema = load_schema_json(args.schema_json)
    elif args.schema_sql:
        schema = load_schema_sql(args.schema_sql)
    else:
        p.error("Provide --scenario, --schema-json, or --schema-sql")

    cfg = GenerationConfig(
        seed=args.seed,
        default_rows=args.rows,
        per_table_rows=_parse_rows(args.rows_map),
        use_faker=args.use_faker,
    )

    use_sdv = bool(args.sdv_train_sqlite or args.sdv_bootstrap)
    if use_sdv:
        if args.sdv_train_sqlite:
            dfs = load_tables_from_sqlite(args.sdv_train_sqlite)
        else:
            gen0 = SyntheticDataGenerator(schema, cfg)
            base = gen0.generate_tables()
            dfs = {k: pd.DataFrame(v) for k, v in base.items()}
        try:
            tables = run_sdv_from_training_frames(dfs, seed=args.seed, scale=args.sdv_scale)
        except ImportError as e:
            p.error(f"SDV not available: pip install 'financial-data-synthesizer[sdv]' ({e})")
    else:
        gen = SyntheticDataGenerator(schema, cfg)
        tables = gen.generate_tables()

    if args.sqlite:
        export_sqlite(schema, tables, args.sqlite)
    if args.parquet_dir:
        export_parquet_dir(tables, args.parquet_dir)

    if not args.sqlite and not args.parquet_dir:
        for k, v in tables.items():
            print(f"{k}: {len(v)} rows")
    else:
        for k, v in tables.items():
            print(f"wrote {k}: {len(v)} rows")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
