from __future__ import annotations

import json
import random
import sqlite3
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def load_tables_from_sqlite(path: str | Path) -> dict[str, pd.DataFrame]:
    """Load every user table from SQLite into DataFrames (excludes sqlite_*)."""
    path = Path(path)
    conn = sqlite3.connect(str(path))
    try:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
        names = [r[0] for r in cur.fetchall()]
        out: dict[str, pd.DataFrame] = {}
        for n in names:
            out[n] = pd.read_sql_query(f'SELECT * FROM "{n}"', conn)
        return out
    finally:
        conn.close()


def prepare_dataframes_for_sdv(dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Coerce object columns to str (JSON → string) for SDV."""
    prepared: dict[str, pd.DataFrame] = {}
    for name, df in dfs.items():
        d = df.copy()
        for col in d.columns:
            if isinstance(d[col].dtype, pd.CategoricalDtype):
                d[col] = d[col].astype(str)
            elif d[col].dtype == object:

                def _cell(x: Any) -> str:
                    if x is None:
                        return ""
                    if isinstance(x, float) and pd.isna(x):
                        return ""
                    if isinstance(x, (dict, list)):
                        return json.dumps(x, ensure_ascii=False)
                    return str(x)

                d[col] = d[col].map(_cell)
        prepared[name] = d
    return prepared


def fit_hma(
    dfs: dict[str, pd.DataFrame],
    *,
    seed: int = 0,
):
    from sdv.metadata import Metadata
    from sdv.multi_table import HMASynthesizer

    random.seed(seed)
    np.random.seed(seed % (2**32 - 1))

    data = prepare_dataframes_for_sdv(dfs)
    metadata = Metadata.detect_from_dataframes(data)
    synth = HMASynthesizer(metadata)
    synth.fit(data)
    return synth


def sample_hma(synthesizer, *, scale: float = 1.0) -> dict[str, pd.DataFrame]:
    return synthesizer.sample(scale=scale)


def dataframes_to_row_dicts(dfs: dict[str, pd.DataFrame]) -> dict[str, list[dict[str, Any]]]:
    """Convert SDV output to nested dicts with JSON-safe scalars."""
    out: dict[str, list[dict[str, Any]]] = {}
    for name, df in dfs.items():
        df2 = df.replace({np.nan: None})
        rows = []
        for rec in df2.to_dict(orient="records"):
            clean: dict[str, Any] = {}
            for k, v in rec.items():
                if hasattr(v, "item"):
                    v = v.item()
                clean[k] = v
            rows.append(clean)
        out[name] = rows
    return out


def run_sdv_from_training_frames(
    dfs: dict[str, pd.DataFrame],
    *,
    seed: int,
    scale: float,
) -> dict[str, list[dict[str, Any]]]:
    """Fit HMA on training tables and return sampled row dicts."""
    synth = fit_hma(dfs, seed=seed)
    sampled = sample_hma(synth, scale=scale)
    return dataframes_to_row_dicts(sampled)
