from __future__ import annotations

import json
import re
from pathlib import Path

from .models import Column, ColumnKind, DataSchema, Table

_KIND_MAP = {
    "text": ColumnKind.STRING,
    "varchar": ColumnKind.STRING,
    "char": ColumnKind.STRING,
    "string": ColumnKind.STRING,
    "integer": ColumnKind.INTEGER,
    "int": ColumnKind.INTEGER,
    "bigint": ColumnKind.INTEGER,
    "float": ColumnKind.FLOAT,
    "double": ColumnKind.FLOAT,
    "real": ColumnKind.FLOAT,
    "numeric": ColumnKind.NUMERIC,
    "decimal": ColumnKind.NUMERIC,
    "boolean": ColumnKind.BOOLEAN,
    "bool": ColumnKind.BOOLEAN,
    "timestamp": ColumnKind.TIMESTAMP,
    "datetime": ColumnKind.TIMESTAMP,
    "date": ColumnKind.TIMESTAMP,
    "json": ColumnKind.JSON,
    "jsonb": ColumnKind.JSON,
    "categorical": ColumnKind.CATEGORICAL,
}


def _normalize_sql_type(raw: str) -> ColumnKind:
    u = raw.strip().upper()
    base = u.split("(")[0].strip().lower()
    if base in _KIND_MAP:
        return _KIND_MAP[base]
    if "TIMESTAMP" in u or "DATETIME" in u:
        return ColumnKind.TIMESTAMP
    if "JSON" in u:
        return ColumnKind.JSON
    return ColumnKind.STRING


def _iter_create_table_bodies(sql_text: str) -> list[tuple[str, str]]:
    """Extract (table_name, inner_body) using balanced parentheses (handles nested parens in FK)."""
    out: list[tuple[str, str]] = []
    i = 0
    while i < len(sql_text):
        m = re.search(
            r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\s*\(",
            sql_text[i:],
            re.IGNORECASE,
        )
        if not m:
            break
        tname = m.group(1)
        start = i + m.end() - 1
        depth = 0
        j = start
        while j < len(sql_text):
            ch = sql_text[j]
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    body = sql_text[start + 1 : j]
                    out.append((tname, body))
                    i = j + 1
                    break
            j += 1
        else:
            break
    return out


def parse_sqlite_ddl(sql_text: str) -> DataSchema:
    """Parse a subset of SQLite/PostgreSQL CREATE TABLE DDL (PRIMARY KEY, FOREIGN KEY)."""
    text = re.sub(r"--[^\n]*", "", sql_text)
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)
    tables: list[Table] = []
    for tname, body in _iter_create_table_bodies(text):
        columns: list[Column] = []
        pk_cols: set[str] = set()
        fks: dict[str, tuple[str, str]] = {}
        for raw_line in body.split(","):
            line = " ".join(raw_line.split())
            if not line:
                continue
            ul = line.upper()
            if ul.startswith("PRIMARY KEY"):
                inner = re.search(r"PRIMARY\s+KEY\s*\(([^)]+)\)", line, re.IGNORECASE)
                if inner:
                    for c in inner.group(1).split(","):
                        pk_cols.add(c.strip().strip('"').strip("'"))
                continue
            if ul.startswith("FOREIGN KEY"):
                fk = re.search(
                    r"FOREIGN\s+KEY\s*\((\w+)\)\s*REFERENCES\s+(\w+)\s*\((\w+)\)",
                    line,
                    re.IGNORECASE,
                )
                if fk:
                    fks[fk.group(1)] = (fk.group(2), fk.group(3))
                continue
            if ul.startswith("UNIQUE") or ul.startswith("CONSTRAINT"):
                continue
            parts = line.split()
            if len(parts) < 2:
                continue
            col_name = parts[0].strip('"').strip("'")
            col_type = parts[1]
            kind = _normalize_sql_type(col_type)
            is_pk = col_name in pk_cols or (
                "PRIMARY KEY" in ul and "AUTOINCREMENT" not in ul
            )
            columns.append(
                Column(
                    name=col_name,
                    kind=kind,
                    is_primary_key=is_pk,
                    fk_ref_table=None,
                    fk_ref_column=None,
                )
            )
        for i, col in enumerate(columns):
            fr = fks.get(col.name)
            if fr:
                columns[i] = Column(
                    name=col.name,
                    kind=col.kind,
                    nullable=col.nullable,
                    is_primary_key=col.is_primary_key,
                    fk_ref_table=fr[0],
                    fk_ref_column=fr[1],
                    categorical_values=col.categorical_values,
                    extra=col.extra,
                )
        tables.append(Table(name=tname, columns=columns))
    return DataSchema(tables=tables)


def load_schema_sql(path: str | Path) -> DataSchema:
    return parse_sqlite_ddl(Path(path).read_text(encoding="utf-8"))


def _kind_from_json(s: str) -> ColumnKind:
    s = (s or "string").lower()
    try:
        return ColumnKind(s)
    except ValueError:
        return ColumnKind.STRING


def parse_json_schema(data: dict) -> DataSchema:
    """Parse internal JSON schema format (see data/sample_schema_full.json)."""
    tables: list[Table] = []
    for t in data.get("tables", []):
        cols: list[Column] = []
        for c in t.get("columns", []):
            kind = _kind_from_json(c.get("type") or "string")
            cols.append(
                Column(
                    name=c["name"],
                    kind=kind,
                    is_primary_key=bool(c.get("primary_key")),
                    fk_ref_table=c.get("references_table"),
                    fk_ref_column=c.get("references_column"),
                    categorical_values=c.get("categorical_values"),
                )
            )
        tables.append(Table(name=t["name"], columns=cols))
    return DataSchema(tables=tables)


def load_schema_json(path: str | Path) -> DataSchema:
    return parse_json_schema(json.loads(Path(path).read_text(encoding="utf-8")))
