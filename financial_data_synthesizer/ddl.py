from __future__ import annotations

from .models import ColumnKind, DataSchema
from .topology import table_generation_order


def _sqlite_col_type(kind: ColumnKind) -> str:
    if kind == ColumnKind.INTEGER:
        return "INTEGER"
    if kind in (ColumnKind.FLOAT, ColumnKind.NUMERIC):
        return "NUMERIC"
    if kind == ColumnKind.BOOLEAN:
        return "INTEGER"
    if kind == ColumnKind.TIMESTAMP:
        return "TEXT"
    if kind == ColumnKind.JSON:
        return "TEXT"
    return "TEXT"


def to_sqlite_ddl(schema: DataSchema) -> str:
    order = table_generation_order(schema)
    by_name = {t.name: t for t in schema.tables}
    parts: list[str] = []
    for name in order:
        t = by_name[name]
        col_lines: list[str] = []
        pk_cols = [c.name for c in t.columns if c.is_primary_key]
        for c in t.columns:
            line = f'  "{c.name}" {_sqlite_col_type(c.kind)}'
            if c.is_primary_key and len(pk_cols) == 1:
                line += " PRIMARY KEY"
            col_lines.append(line)
        if len(pk_cols) > 1:
            pk_list = ", ".join(f'"{c}"' for c in pk_cols)
            col_lines.append(f"  PRIMARY KEY ({pk_list})")
        for c in t.columns:
            if c.fk_ref_table and c.fk_ref_column:
                col_lines.append(
                    f'  FOREIGN KEY("{c.name}") REFERENCES "{c.fk_ref_table}"("{c.fk_ref_column}")'
                )
        body = ",\n".join(col_lines)
        parts.append(f'CREATE TABLE IF NOT EXISTS "{t.name}" (\n{body}\n);')
    return "\n\n".join(parts)
