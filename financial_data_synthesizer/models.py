from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class ColumnKind(str, Enum):
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    NUMERIC = "numeric"
    CATEGORICAL = "categorical"
    JSON = "json"
    TIMESTAMP = "timestamp"
    TEXT = "text"
    BOOLEAN = "boolean"


@dataclass
class Column:
    name: str
    kind: ColumnKind
    nullable: bool = True
    is_primary_key: bool = False
    fk_ref_table: str | None = None
    fk_ref_column: str | None = None
    categorical_values: list[str] | None = None
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class Table:
    name: str
    columns: list[Column]

    def column_by_name(self, name: str) -> Column | None:
        for c in self.columns:
            if c.name == name:
                return c
        return None


@dataclass
class DataSchema:
    tables: list[Table]

    def table_by_name(self, name: str) -> Table | None:
        for t in self.tables:
            if t.name == name:
                return t
        return None
