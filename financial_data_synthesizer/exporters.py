from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd

from .ddl import to_sqlite_ddl
from .models import DataSchema
from .topology import table_generation_order


def export_sqlite(
    schema: DataSchema,
    tables: dict[str, list[dict[str, Any]]],
    path: str | Path,
) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()
    conn = sqlite3.connect(str(path))
    try:
        conn.executescript(to_sqlite_ddl(schema))
        order = table_generation_order(schema)
        for tname in order:
            if tname not in tables:
                continue
            rows = tables[tname]
            if not rows:
                continue
            cols = list(rows[0].keys())
            placeholders = ", ".join("?" * len(cols))
            quoted = ", ".join(f'"{c}"' for c in cols)
            sql = f'INSERT INTO "{tname}" ({quoted}) VALUES ({placeholders})'
            batch: list[tuple[Any, ...]] = []
            for r in rows:
                batch.append(tuple(_sqlite_param(r[c]) for c in cols))
                if len(batch) >= 10_000:
                    conn.executemany(sql, batch)
                    batch.clear()
            if batch:
                conn.executemany(sql, batch)
        conn.commit()
    finally:
        conn.close()


def _sqlite_param(v: Any) -> Any:
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return v


def export_parquet_dir(
    tables: dict[str, list[dict[str, Any]]],
    out_dir: str | Path,
) -> None:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    for name, rows in tables.items():
        df = pd.DataFrame(rows)
        df.to_parquet(out / f"{name}.parquet", index=False)


def export_parquet_dataset(
    tables: dict[str, list[dict[str, Any]]],
    path: str | Path,
) -> None:
    """Single Parquet file with hive-style column __table__ (for simple interchange)."""
    frames = []
    for name, rows in tables.items():
        if not rows:
            continue
        df = pd.DataFrame(rows)
        df["__table__"] = name
        frames.append(df)
    if not frames:
        return
    out = pd.concat(frames, ignore_index=True)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(path, index=False)


def export_delta(
    tables: dict[str, list[dict[str, Any]]],
    root: str | Path,
    *,
    partition_cols: list[str] | None = None,
) -> None:
    """Delta Lake (requires `deltalake` + pyarrow)."""
    try:
        from deltalake import write_deltalake
    except ImportError as e:
        raise ImportError("Install deltalake and pyarrow for Delta export") from e

    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    for name, rows in tables.items():
        if not rows:
            continue
        df = pd.DataFrame(rows)
        df["__table__"] = name
        table_path = str(root / name)
        write_deltalake(
            table_path,
            df,
            mode="overwrite",
            partition_by=partition_cols,
        )
