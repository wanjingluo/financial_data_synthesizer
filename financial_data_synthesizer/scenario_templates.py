from __future__ import annotations

from .models import Column, ColumnKind, DataSchema, Table


def _crm_kyc_addendum_tables() -> list[Table]:
    """RM coverage + KYC (client line). client_accounts use customer_id → customers (no separate clients)."""
    kyc_vals = ["Active", "Pending", "Expired"]
    acct_st = ["Active", "Dormant", "Closed"]
    san = ["Clear", "Match", "Under_Review"]
    asg_type = ["Primary", "Secondary", "Regional_Shared", "Product_Shared"]
    scope = ["Full", "Region", "Product_Line"]
    return [
        Table(
            name="relationship_managers",
            columns=[
                Column("rm_id", ColumnKind.STRING, is_primary_key=True),
                Column("full_name", ColumnKind.STRING),
                Column(
                    "home_region",
                    ColumnKind.CATEGORICAL,
                    categorical_values=["AMER", "EMEA", "APAC", "GLBL"],
                ),
                Column(
                    "product_focus",
                    ColumnKind.CATEGORICAL,
                    categorical_values=["Cash", "Credit", "Markets", "Wealth", "All"],
                ),
                Column("metadata_json", ColumnKind.JSON),
            ],
        ),
        Table(
            name="client_accounts",
            columns=[
                Column("serving_account_id", ColumnKind.STRING, is_primary_key=True),
                Column(
                    "customer_id",
                    ColumnKind.STRING,
                    fk_ref_table="customers",
                    fk_ref_column="customer_id",
                ),
                Column(
                    "account_status",
                    ColumnKind.CATEGORICAL,
                    categorical_values=acct_st,
                ),
                Column("kyc_status", ColumnKind.CATEGORICAL, categorical_values=kyc_vals),
                Column("kyc_expiry_date", ColumnKind.TIMESTAMP),
                Column("sanctions_status", ColumnKind.CATEGORICAL, categorical_values=san),
                Column(
                    "coverage_owner_id",
                    ColumnKind.STRING,
                    fk_ref_table="relationship_managers",
                    fk_ref_column="rm_id",
                ),
                Column("is_servicing_eligible", ColumnKind.BOOLEAN),
                Column("account_metadata_json", ColumnKind.JSON),
            ],
        ),
        Table(
            name="coverage_assignments",
            columns=[
                Column("coverage_assignment_id", ColumnKind.STRING, is_primary_key=True),
                Column(
                    "serving_account_id",
                    ColumnKind.STRING,
                    fk_ref_table="client_accounts",
                    fk_ref_column="serving_account_id",
                ),
                Column(
                    "rm_id",
                    ColumnKind.STRING,
                    fk_ref_table="relationship_managers",
                    fk_ref_column="rm_id",
                ),
                Column("assignment_type", ColumnKind.CATEGORICAL, categorical_values=asg_type),
                Column("coverage_scope", ColumnKind.CATEGORICAL, categorical_values=scope),
                Column("region_code", ColumnKind.STRING),
                Column("product_line", ColumnKind.STRING),
                Column("details_json", ColumnKind.JSON),
            ],
        ),
    ]


def _crm_schema() -> DataSchema:
    return DataSchema(
        tables=[
            Table(
                name="customers",
                columns=[
                    Column("customer_id", ColumnKind.STRING, is_primary_key=True),
                    Column("name", ColumnKind.STRING),
                    Column("age", ColumnKind.INTEGER),
                    Column(
                        "country",
                        ColumnKind.CATEGORICAL,
                        categorical_values=["US", "GB", "DE", "FR", "JP", "SG"],
                    ),
                    Column("profile_json", ColumnKind.JSON),
                ],
            ),
            Table(
                name="accounts",
                columns=[
                    Column("account_id", ColumnKind.STRING, is_primary_key=True),
                    Column(
                        "customer_id",
                        ColumnKind.STRING,
                        fk_ref_table="customers",
                        fk_ref_column="customer_id",
                    ),
                    Column(
                        "account_type",
                        ColumnKind.CATEGORICAL,
                        categorical_values=["checking", "savings", "investment", "credit"],
                    ),
                    Column("balance", ColumnKind.NUMERIC),
                    Column("metadata_json", ColumnKind.JSON),
                ],
            ),
            Table(
                name="transactions",
                columns=[
                    Column("transaction_id", ColumnKind.STRING, is_primary_key=True),
                    Column(
                        "account_id",
                        ColumnKind.STRING,
                        fk_ref_table="accounts",
                        fk_ref_column="account_id",
                    ),
                    Column("amount", ColumnKind.NUMERIC),
                    Column(
                        "currency",
                        ColumnKind.CATEGORICAL,
                        categorical_values=["USD", "EUR", "GBP", "JPY"],
                    ),
                    Column("transaction_time", ColumnKind.TIMESTAMP),
                    Column("details_json", ColumnKind.JSON),
                ],
            ),
            Table(
                name="customer_interactions",
                columns=[
                    Column("interaction_id", ColumnKind.STRING, is_primary_key=True),
                    Column(
                        "customer_id",
                        ColumnKind.STRING,
                        fk_ref_table="customers",
                        fk_ref_column="customer_id",
                    ),
                    Column(
                        "channel",
                        ColumnKind.CATEGORICAL,
                        categorical_values=["branch", "phone", "chat", "email", "app"],
                    ),
                    Column("interaction_time", ColumnKind.TIMESTAMP),
                    Column("summary_json", ColumnKind.JSON),
                ],
            ),
            *_crm_kyc_addendum_tables(),
        ]
    )


def _trading_schema() -> DataSchema:
    return DataSchema(
        tables=[
            Table(
                name="portfolios",
                columns=[
                    Column("portfolio_id", ColumnKind.STRING, is_primary_key=True),
                    Column("name", ColumnKind.STRING),
                    Column("base_currency", ColumnKind.CATEGORICAL, categorical_values=["USD", "EUR"]),
                    Column("metadata_json", ColumnKind.JSON),
                ],
            ),
            Table(
                name="instruments",
                columns=[
                    Column("instrument_id", ColumnKind.STRING, is_primary_key=True),
                    Column(
                        "asset_class",
                        ColumnKind.CATEGORICAL,
                        categorical_values=["equity", "bond", "fx", "commodity"],
                    ),
                    Column("ticker", ColumnKind.STRING),
                    Column("details_json", ColumnKind.JSON),
                ],
            ),
            Table(
                name="trade_orders",
                columns=[
                    Column("order_id", ColumnKind.STRING, is_primary_key=True),
                    Column(
                        "portfolio_id",
                        ColumnKind.STRING,
                        fk_ref_table="portfolios",
                        fk_ref_column="portfolio_id",
                    ),
                    Column(
                        "instrument_id",
                        ColumnKind.STRING,
                        fk_ref_table="instruments",
                        fk_ref_column="instrument_id",
                    ),
                    Column("side", ColumnKind.CATEGORICAL, categorical_values=["buy", "sell"]),
                    Column("quantity", ColumnKind.NUMERIC),
                    Column("limit_price", ColumnKind.NUMERIC),
                    Column("order_time", ColumnKind.TIMESTAMP),
                    Column("attrs_json", ColumnKind.JSON),
                ],
            ),
            Table(
                name="trade_executions",
                columns=[
                    Column("execution_id", ColumnKind.STRING, is_primary_key=True),
                    Column(
                        "order_id",
                        ColumnKind.STRING,
                        fk_ref_table="trade_orders",
                        fk_ref_column="order_id",
                    ),
                    Column("fill_price", ColumnKind.NUMERIC),
                    Column("fill_qty", ColumnKind.NUMERIC),
                    Column("execution_time", ColumnKind.TIMESTAMP),
                    Column("details_json", ColumnKind.JSON),
                ],
            ),
        ]
    )


def _credit_risk_schema() -> DataSchema:
    return DataSchema(
        tables=[
            Table(
                name="borrowers",
                columns=[
                    Column("borrower_id", ColumnKind.STRING, is_primary_key=True),
                    Column("annual_income", ColumnKind.NUMERIC),
                    Column("employment_years", ColumnKind.INTEGER),
                    Column(
                        "region",
                        ColumnKind.CATEGORICAL,
                        categorical_values=["NE", "SE", "MW", "W", "other"],
                    ),
                    Column("profile_json", ColumnKind.JSON),
                ],
            ),
            Table(
                name="loan_contracts",
                columns=[
                    Column("loan_id", ColumnKind.STRING, is_primary_key=True),
                    Column(
                        "borrower_id",
                        ColumnKind.STRING,
                        fk_ref_table="borrowers",
                        fk_ref_column="borrower_id",
                    ),
                    Column("principal", ColumnKind.NUMERIC),
                    Column("rate", ColumnKind.NUMERIC),
                    Column("term_months", ColumnKind.INTEGER),
                    Column("contract_json", ColumnKind.JSON),
                ],
            ),
            Table(
                name="repayment_history",
                columns=[
                    Column("payment_id", ColumnKind.STRING, is_primary_key=True),
                    Column(
                        "loan_id",
                        ColumnKind.STRING,
                        fk_ref_table="loan_contracts",
                        fk_ref_column="loan_id",
                    ),
                    Column("due_date", ColumnKind.TIMESTAMP),
                    Column("paid_amount", ColumnKind.NUMERIC),
                    Column("status", ColumnKind.CATEGORICAL, categorical_values=["on_time", "late", "missed"]),
                    Column("details_json", ColumnKind.JSON),
                ],
            ),
            Table(
                name="risk_indicators",
                columns=[
                    Column("snapshot_id", ColumnKind.STRING, is_primary_key=True),
                    Column(
                        "loan_id",
                        ColumnKind.STRING,
                        fk_ref_table="loan_contracts",
                        fk_ref_column="loan_id",
                    ),
                    Column("pd", ColumnKind.NUMERIC),
                    Column("lgd", ColumnKind.NUMERIC),
                    Column("as_of", ColumnKind.TIMESTAMP),
                    Column("features_json", ColumnKind.JSON),
                ],
            ),
        ]
    )


def _banking_schema() -> DataSchema:
    """Retail banking: accounts with ordered status history (chain per account_id)."""
    status_values = ["opened", "active", "frozen", "closed"]
    return DataSchema(
        tables=[
            Table(
                name="customers",
                columns=[
                    Column("customer_id", ColumnKind.STRING, is_primary_key=True),
                    Column("name", ColumnKind.STRING),
                    Column(
                        "country",
                        ColumnKind.CATEGORICAL,
                        categorical_values=["US", "GB", "DE", "FR", "JP", "SG"],
                    ),
                    Column("profile_json", ColumnKind.JSON),
                ],
            ),
            Table(
                name="bank_accounts",
                columns=[
                    Column("account_id", ColumnKind.STRING, is_primary_key=True),
                    Column(
                        "customer_id",
                        ColumnKind.STRING,
                        fk_ref_table="customers",
                        fk_ref_column="customer_id",
                    ),
                    Column("opened_at", ColumnKind.TIMESTAMP),
                    Column(
                        "current_status",
                        ColumnKind.CATEGORICAL,
                        categorical_values=status_values,
                    ),
                    Column("metadata_json", ColumnKind.JSON),
                ],
            ),
            Table(
                name="account_status_history",
                columns=[
                    Column("event_id", ColumnKind.STRING, is_primary_key=True),
                    Column(
                        "account_id",
                        ColumnKind.STRING,
                        fk_ref_table="bank_accounts",
                        fk_ref_column="account_id",
                    ),
                    Column("event_time", ColumnKind.TIMESTAMP),
                    Column(
                        "status",
                        ColumnKind.CATEGORICAL,
                        categorical_values=status_values,
                    ),
                    Column("details_json", ColumnKind.JSON),
                ],
            ),
        ]
    )


def _client_coverage_schema() -> DataSchema:
    """Same unified schema as `crm` (RM + KYC + retail tables); use `--scenario crm` or this alias."""
    return _crm_schema()


SCENARIOS: dict[str, DataSchema] = {
    "crm": _crm_schema(),
    "trading": _trading_schema(),
    "credit_risk": _credit_risk_schema(),
    "banking": _banking_schema(),
    "client_coverage": _client_coverage_schema(),
}
