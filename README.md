# Financial Data Synthesizer

A **synthetic data generator** for financial-style workloads. It supports derive a data model from a business scenario and generate configurable row counts from a given schema: SQLite DDL, internal JSON, or built-in scenarios, while aiming for plausible distributions, referential integrity, and semi-structured field shapes.

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
├── business_rules/           # engine.py + trading/credit_risk; CRM/banking/RM-KYC in crm_rules.py
├── exporters.py              # SQLite / Parquet / Delta (optional)
└── cli.py                    # CLI entry point
```

## Schema generation

- **Built-in scenario templates** (aligned with Example Data Scenarios): `crm` — full **unified** retail + relationship dataset: `customers`, `accounts`, `transactions`, `customer_interactions`, `relationship_managers`, `client_accounts` (KYC + Primary RM; PK `serving_account_id`, FK `customer_id` → `customers`), `coverage_assignments` (FK `serving_account_id` → `client_accounts`). The same schema is also registered as **`client_coverage`** (alias). Other scenarios: `trading` (portfolios, instruments, orders, executions), `credit_risk` (borrowers, loans, repayments, risk snapshots), `banking` (customers, `bank_accounts`, `account_status_history` chain).
- Each table declares **primary** and **foreign keys** and mixes **numeric**, **categorical**, **timestamp**, and **JSON** columns.
- Extensions: map free-text scenarios to templates (aliases), or feed an LLM-produced JSON schema into the same pipeline.

## Synthetic data pipeline

1. **Input**: one of `--schema-sql`, `--schema-json`, or `--scenario`.
2. **Topological order**: `topology.table_generation_order` ensures parents are generated before dependents.
3. **Rows**: FK columns sample uniformly from the parent PK pool; amounts use lognormal / skewed draws; categoricals use weights or `categorical_values`.
4. **JSON**: `json_values.synthetic_json_for_column` builds stable nested payloads from column/table hints (e.g. profile, metadata, transaction details).
5. **Output** (only if you pass the matching CLI flags): `export_sqlite` (batched inserts), `export_parquet_dir` (per-table files), `export_delta` (optional: `pip install deltalake`). See **CLI output** below.

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

## Faker and SDV

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
fds generate --scenario crm --sdv-train-sqlite out/sdv.db --sdv-scale 2 --sqlite out/sdv2.db
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

## CLI output (`--sqlite` and `--parquet-dir`)

- **`--sqlite PATH`**: optional. If omitted, **no `.db` file is written**—data exists only in memory for that run.
- **`--parquet-dir DIR`**: optional. If omitted, **no Parquet files are written**—previous files in that folder are **not** updated.
- The two flags are **independent**: use **both** in one command if you want a SQLite database **and** per-table Parquet under `DIR`. Passing only one writes only that format.
- If **neither** `--sqlite` nor `--parquet-dir` is given, the CLI **only prints** each table’s row count to the terminal (**nothing is saved** to disk).

Parent directories (e.g. `out/`) are created automatically when writing.

- With **`--scenario`**, the **business rule engine** runs before export unless **`--no-business-rules`** is set (see [Business rule engine](#business-rule-engine-built-in-scenarios) at the end of this file).

## CLI examples

```bash
# Built-in CRM scenario — SQLite + Parquet
fds generate --scenario crm --rows 200 --sqlite out/crm.db --parquet-dir out/pq

# Faker — same: add --parquet-dir if you want Parquet updated (SQLite alone does not write Parquet)
fds generate --scenario crm --rows 100 --use-faker --sqlite out/crm_faker.db --parquet-dir out/pq

# sample_schema.sql (includes transactions) + CRM business rules (optional)
fds generate --schema-sql data/sample_schema.sql --business-rules-as crm --rows 500 --sqlite out/sample.db --parquet-dir out/pq_schema

# sample_schema_full.json (FK metadata)
fds generate --schema-json data/sample_schema_full.json --rows 1000 --parquet-dir out/pq

# No files — only print row counts to stdout
fds generate --scenario crm --rows 50

# Banking (ordered status history) — separate scenario
fds generate --scenario banking --rows 100 --seed 42 --sqlite out/banking.db
# `client_coverage` is the same schema as `crm` (alias); use `crm` for “all user tables” in one run
fds generate --scenario crm --rows 200 --sqlite out/crm.db --parquet-dir out/pq
```

## Example outputs

With `--sqlite` / `--parquet-dir`, outputs go to the paths you pass (e.g. `out/*.db` and `out/pq/*.parquet`).

---

## Business rule engine (built-in scenarios)

When you pass **`--scenario`**, a **deterministic rule engine** runs after base generation (and after SDV, if used). It rewrites / enriches fields so rows reflect **entity identity**, **lifecycle**, **cross-field**, **product**, and (where defined) **coverage / KYC** logic—without changing primary/foreign keys (referential integrity is preserved).

**CLI**

- **Default**: rules **on** when you use **`--scenario crm|trading|credit_risk|banking|client_coverage`**.
- **`--business-rules-as crm|trading|credit_risk|banking|client_coverage`**: apply the same engine when using **`--schema-sql` or `--schema-json`** (no `--scenario`). Example: `--schema-sql data/sample_schema.sql --business-rules-as crm`.
- **`--no-business-rules`**: skip the engine and keep raw generator (± Faker) output only.

**Where it lives**

- `financial_data_synthesizer/business_rules/engine.py` — dispatches by scenario.
- `crm_rules.py` — **CRM** (including **Primary RM** + `is_servicing_eligible` when `client_accounts` + `relationship_managers` are present; always true for `--scenario crm` / `client_coverage`), and **banking** (status history when `bank_accounts` is present); trading/credit are separate.
- `trading_rules.py`, `credit_risk_rules.py` — trading and credit-risk-only logic.

**Implementation vs. checking exports**

- **Enforcing rules in-process:** `cli.py` calls `apply_business_rules(...)` after `generate_tables` (unless `--no-business-rules`). CRM / RM+KYC logic is implemented in `business_rules/crm_rules.py` (e.g. `_apply_client_coverage_kyc`, `_compute_is_servicing_eligible`), not as DB CHECK constraints.
- **Re-validating Parquet (or any export) after the fact:** `scripts/validate_pq_crm.py` reads `client_accounts.parquet` and `coverage_assignments.parquet` and checks the same Primary-RM and `is_servicing_eligible` predicates. **`tests/test_business_rules.py`** covers the engine on in-memory dicts. **`notebooks/pq_crm_quicklook.ipynb`** is for ad-hoc Pandas checks.

### CRM (`crm`) — includes retail + RM / KYC (same as alias `client_coverage`)

| Theme | Rules (summary) |
|--------|-------------------|
| **Entity identity** | Each customer gets `entity_type` ∈ {retail, corporate} and `segment` in `profile_json`; corporate skews toward higher tiers. |
| **Customer lifecycle** | `lifecycle_stage` ∈ {onboarding, active, growth, at_risk, dormant}; drives balance scaling and interaction channel mix. (KYC for servicing is on `client_accounts`, not in `profile_json`.) |
| **Cross-field** | `account_type` and `balance` depend on entity + lifecycle + tier; `metadata_json` carries `product_line` (commercial vs retail) and `fee_tier`. |
| **Transactions** | `amount` scales with account balance and lifecycle (e.g. stress in at_risk); `currency` mix differs for corporate vs retail; `details_json` adds posting/risk flags. |
| **Interactions** | `channel` distribution shifts by lifecycle (e.g. more phone when at_risk); `summary_json` adds intent/priority. |
| **Primary RM** | `client_accounts` + `coverage_assignments`: for `account_status = Active`, exactly one **Primary** row; `coverage_owner_id` matches. Optional `Product_Shared` / `Regional_Shared`. **IDs:** `serving_account_id` (serving/relationship line) vs `accounts.account_id` (retail product accounts). |
| **Servicing eligibility** | `is_servicing_eligible` on `client_accounts` from KYC + `kyc_expiry_date` + `sanctions_status` (as-of: env **`FDS_AS_OF_DATE`** or UTC today). |

### Banking (`banking`) — also implemented in `crm_rules.py`

| Theme | Rules (summary) |
|--------|-------------------|
| **Status history** | `account_status_history` is **rebuilt** with a time-ordered Markov chain per `account_id` (first event `opened`; each next status depends on the previous). |
| **Current state** | `bank_accounts.current_status` = last event; `metadata_json` flags terminal `closed` when applicable. |

### Trading (`trading`)

| Theme | Rules (summary) |
|--------|-------------------|
| **Product / instrument** | `quantity` and `limit_price` ranges depend on `asset_class` (equity vs fx vs bond vs commodity). |
| **Portfolio** | `attrs_json` includes routing hint; execution currency context uses portfolio `base_currency`. |
| **Executions** | `fill_price` stays near order `limit_price`; `fill_qty` ≤ order quantity; `details_json` adds slippage_bps. |

### Credit risk (`credit_risk`)

| Theme | Rules (summary) |
|--------|-------------------|
| **Borrower** | `profile_json` adds employment stability; mild regional income floor for selected regions. |
| **Loan contracts** | Principal capped vs annual income (rough DTI-style); rate nudges up when leverage is high; `contract_json` adds product_family and underwriting_band. |
| **Repayment** | Paid amount bounded by principal; status may move missed→late; collections_stage in `details_json`. |
| **Risk indicators** | `pd` / `lgd` adjusted with payment-to-income proxy; `features_json` stores `pti` and model tag. |

**Extending**

- Add or edit rules in `crm_rules.py` (shared CRM / banking / client-coverage paths) or in `trading_rules.py` / `credit_risk_rules.py`; register new scenario keys in `business_rules/engine.py`.
- For **AI / LLM**-driven logic later, keep the same post-process hook: call your model from a rule function and merge outputs into row dicts (still subject to schema types).

**Note**: With only `--schema-sql` / `--schema-json` and **no** `--scenario`, rules **do not** run unless you add **`--business-rules-as`** with a known scenario (`crm`, `trading`, `credit_risk`, `banking`, `client_coverage`, …). You can also call `apply_business_rules(name, tables, seed)` from Python.

**DDL / TEXT columns**: SQLite `TEXT` columns such as `currency` and `account_type` are parsed as `STRING`s. The generator maps known code-like column names to **ISO codes / account types** (not random letters). **`ticker` / `symbol`** (e.g. on `instruments`) are filled with **uppercase ticker-like symbols** (pool of stylized names + random A–Z runs), not mixed-case gibberish. Use **`--use-faker`** for similar semantics on those fields.

---

*Last updated: 2026-04 — Documented “implementation vs. post-export validation” (`crm_rules` vs `scripts/validate_pq_crm.py` / tests / notebook). `--scenario crm` = unified schema; `client_coverage` = alias; `engine` dispatches to `apply_crm_rules` for `crm` / `client_coverage` / `banking` as applicable.*
