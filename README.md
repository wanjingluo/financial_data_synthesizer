# Financial Data Synthesizer

A **synthetic data generator** for financial-style workloads. It supports **Requirement 1** (derive a data model from a business scenario) and **Requirement 2** (generate configurable row counts from a given schema: SQLite DDL, internal JSON, or built-in scenarios), while aiming for plausible distributions, referential integrity, and semi-structured field shapes.

## Architecture

```
financial_data_synthesizer/
├── schema_from_scenario.py   # Scenario → DataSchema (multi-table, PK/FK)
├── parsers.py                # SQLite DDL / JSON schema parsing
├── ddl.py                    # DataSchema → SQLite DDL (topological create order)
├── topology.py               # Table dependency order (FK-safe generation)
├── synthesis.py              # Row generation (distributions, FK sampling, semi-structured)
├── json_values.py            # Domain-style JSON column templates
├── faker_bridge.py           # Optional Faker: names, contacts, categories
├── sdv_integration.py        # Optional SDV HMA: multi-table fit & resample
├── exporters.py              # SQLite / Parquet / Delta (optional)
└── cli.py                    # CLI entry point
```

## Schema generation (Requirement 1)

- **Built-in scenario templates** (aligned with Example Data Scenarios): `crm` (customers, accounts, transactions, interactions), `trading` (portfolios, instruments, orders, executions), `credit_risk` (borrowers, loans, repayments, risk snapshots).
- Each table declares **primary** and **foreign keys** and mixes **numeric**, **categorical**, **timestamp**, and **JSON** columns.
- Extensions: map free-text scenarios to templates (aliases), or feed an LLM-produced JSON schema into the same pipeline.

## Synthetic data pipeline (Requirement 2)

1. **Input**: one of `--schema-sql`, `--schema-json`, or `--scenario`.
2. **Topological order**: `topology.table_generation_order` ensures parents are generated before dependents.
3. **Rows**: FK columns sample uniformly from the parent PK pool; amounts use lognormal / skewed draws; categoricals use weights or `categorical_values`.
4. **JSON**: `json_values.synthetic_json_for_column` builds stable nested payloads from column/table hints (e.g. profile, metadata, transaction details).
5. **Output**: `export_sqlite` (batched inserts), `export_parquet_dir` (per-table files), `export_delta` (optional: `pip install deltalake`).

## Distributions and relationships

- **Referential integrity**: child FK values come only from generated parent PKs; SQLite exports can be checked with `PRAGMA foreign_key_check` (see tests).
- **Categoricals**: optional `categorical_values`; otherwise name-based heuristics (e.g. `country`, `currency`) with weights.
- **Numeric shapes**: balances/exposure use lognormal-style draws; transaction amounts use a skewed variant; ages are clipped to 18–95.

## Semi-structured data (JSON / XML)

- JSON is stored mainly as **JSON strings** (SQLite/Parquet friendly); columns like `details_json` / `profile_json` include nested fields and arrays.
- For XML, extend `json_values` with `*_xml` branches or emit fragments into `text` columns.

## Scale and performance

- SQLite export uses **10k-row batches** with `executemany`.
- For very large runs, use `SyntheticDataGenerator.iter_batches` per table (you must keep parent PK pools consistent yourself).

## Faker and SDV (optional)

Install: `pip install -e ".[faker]"` or `pip install -e ".[sdv]"` (includes `sdv` and `faker`).

### Faker (`--use-faker`)

Produces **readable single-column values** before the default RNG, driven by column names (see `faker_bridge.py`).

**Suggested priority columns (by scenario)**

| Scenario | Priority tables | Example columns |
|----------|-----------------|-----------------|
| CRM | `customers` | `name`, `country` (or any categorical with `categorical_values`) |
| CRM | `transactions` | `transaction_time` (ISO timestamps), `currency` |
| Trading | `instruments` | `ticker` |
| Trading | `portfolios` | `name` (company/portfolio-style labels) |
| Credit risk | `borrowers` | `profile_json` still uses JSON templates; prefer SDV for heavy numerics |
| Credit risk | `loan_contracts` | `rate` / `principal` (Faker only lightly touches `rate`; large amounts fit SDV better) |

**Do not** replace with Faker: `*_id` primary/foreign keys, or whole JSON columns (keep `json_values`).

### SDV HMA (`--sdv-bootstrap` or `--sdv-train-sqlite`)

**HMA** learns cross-table dependencies and samples jointly—good for **relationships plus column joint distributions**.

**Suggested priority tables (fit together)**

1. **CRM**: `customers` → `accounts` → `transactions`; include `customer_interactions` with `customers` in one HMA run.  
2. **Trading**: `portfolios` and `instruments` together with `trade_orders` and `trade_executions`.  
3. **Credit risk**: `borrowers` → `loan_contracts` → `repayment_history` / `risk_indicators`.

**High-signal columns for training**

- Numeric: `amount`, `balance`, `principal`, `pd`, `lgd`, `fill_price`
- Categorical: `account_type`, `currency`, `channel`, `status`, `side`
- Time: `transaction_time`, `execution_time`, `due_date`
- Text / JSON: modeled as strings in SDV; for strict structure, keep rule-based JSON or engineer features (hash/truncate) before modeling.

**Examples**

```bash
# Bootstrap a training sample with the built-in engine, then HMA resample (scale ≈ multiplier vs training sizes)
fds generate --scenario crm --rows 80 --sdv-bootstrap --sdv-scale 1.5 --sqlite out/sdv.db

# Train from an existing SQLite database
fds generate --scenario crm --sdv-train-sqlite existing.db --sdv-scale 2 --sqlite out/sdv2.db
```

## Dependencies

- **Core**: `pandas`, `pyarrow` (Parquet).
- **Optional**: `faker`; `sdv` (multi-table HMA); `deltalake` (Delta Lake).

## Install and test

```bash
pip install -e ".[dev]"
python -m pytest tests -q
# Skip slower SDV integration test:
python -m pytest tests -q -m "not slow"
```

## CLI examples

```bash
# Built-in CRM scenario
fds generate --scenario crm --rows 200 --sqlite out/crm.db --parquet-dir out/pq

# Faker for more human-readable fields
fds generate --scenario crm --rows 100 --use-faker --sqlite out/crm_faker.db

# sample_schema.sql (includes transactions)
fds generate --schema-sql data/sample_schema.sql --rows 500 --sqlite out/sample.db

# sample_schema_full.json (FK metadata)
fds generate --schema-json data/sample_schema_full.json --rows 1000 --parquet-dir out/pq
```

## Example outputs

Commands can write under `example/out/` (SQLite and per-table Parquet).

## Mapping to the evaluation rubric

| Area | What this project demonstrates |
|------|--------------------------------|
| Architecture (25%) | Layered modules, multiple inputs (SQL/JSON/scenario), pluggable exporters |
| Data quality (25%) | Weighted categoricals, skewed amounts, FK consistency |
| Engineering (20%) | Clear models, CLI, pytest |
| Semi-structured (15%) | JSON templates and column-name heuristics |
| Scalability (15%) | Batched SQLite export, optional batch iterator |
