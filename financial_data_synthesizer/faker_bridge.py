from __future__ import annotations

from typing import Any

from .models import Column, ColumnKind


class FakerBridge:
    """
    Maps table/column semantics to Faker providers for human-readable fields.
    Returns None when the column should fall back to the default generator.
    """

    def __init__(self, seed: int, locale: str = "en_US") -> None:
        from faker import Faker

        self._fake = Faker(locale)
        self._fake.seed_instance(seed)

    def maybe_value(self, table: str, col: Column) -> Any | None:
        if col.is_primary_key or col.fk_ref_table:
            return None
        if col.kind == ColumnKind.JSON:
            return None

        t = table.lower()
        n = col.name.lower()

        if col.kind == ColumnKind.CATEGORICAL and col.categorical_values:
            return self._fake.random_element(col.categorical_values)

        if n in ("name", "full_name", "customer_name", "borrower_name"):
            return self._fake.name()
        if n == "email" or n.endswith("_email"):
            return self._fake.email()
        if n in ("phone", "mobile", "phone_number", "msisdn"):
            return self._fake.phone_number()
        if n in ("address", "street_address", "billing_address"):
            return self._fake.address().replace("\n", ", ")
        if n in ("city",):
            return self._fake.city()
        if n in ("postcode", "zip", "zip_code"):
            return self._fake.postcode()
        if n in ("company", "company_name", "employer"):
            return self._fake.company()
        if n in ("iban",):
            return self._fake.iban()

        if n in ("ticker", "symbol"):
            return self._fake.lexify("????", letters="ABCDEFGHIJKLMNOPQRSTUVWXYZ")

        if n in ("country", "country_code", "region"):
            return self._fake.country_code()

        # DDL often uses TEXT for codes; same as synthesis._categorical lists
        if n == "currency" or "currency" in n:
            return self._fake.random_element(["USD", "EUR", "GBP", "JPY", "SGD"])
        if n == "account_type" or n.endswith("_account_type"):
            return self._fake.random_element(["checking", "savings", "investment", "credit"])

        if col.kind == ColumnKind.INTEGER:
            if n in ("age",):
                return self._fake.random_int(min=18, max=85)
            if n in ("term_months",):
                return self._fake.random_int(min=6, max=360)
            if n in ("employment_years",):
                return self._fake.random_int(min=0, max=40)

        if col.kind == ColumnKind.TIMESTAMP:
            return self._fake.date_time_between(start_date="-2y", end_date="now").isoformat()

        if col.kind in (ColumnKind.FLOAT, ColumnKind.NUMERIC):
            if n in ("rate", "interest_rate", "apr") and "loan" in t:
                return round(
                    self._fake.pyfloat(left_digits=0, right_digits=4, positive=True, min_value=0.01, max_value=0.15),
                    6,
                )

        return None
