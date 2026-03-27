from __future__ import annotations

from collections import defaultdict, deque

from .models import DataSchema


def table_generation_order(schema: DataSchema) -> list[str]:
    """Return table names in FK-safe order (referenced tables before dependents)."""
    names = [t.name for t in schema.tables]
    graph: dict[str, list[str]] = defaultdict(list)
    indegree: dict[str, int] = {n: 0 for n in names}

    for t in schema.tables:
        for c in t.columns:
            if c.fk_ref_table and c.fk_ref_table in indegree:
                graph[c.fk_ref_table].append(t.name)
                indegree[t.name] += 1

    q = deque([n for n in names if indegree[n] == 0])
    out: list[str] = []
    while q:
        n = q.popleft()
        out.append(n)
        for child in graph[n]:
            indegree[child] -= 1
            if indegree[child] == 0:
                q.append(child)

    if len(out) != len(names):
        remaining = [x for x in names if x not in out]
        out.extend(remaining)
    return out
