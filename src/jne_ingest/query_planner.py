from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Optional, Tuple


_COUNT_MARKERS: Tuple[str, ...] = (
    "cuanto",
    "cuantos",
    "cuanta",
    "cuantas",
    "cantidad",
    "numero",
    "total",
)


@dataclass(frozen=True)
class QueryPlan:
    operation: str
    normalized_query: str
    wants_count: bool


def normalize_query_text(query: str) -> str:
    translation = str.maketrans("áéíóúÁÉÍÓÚñÑ", "aeiouAEIOUnN")
    cleaned = str(query or "").strip().lower().translate(translation)
    return re.sub(r"\s+", " ", cleaned)


def plan_query(query: str) -> QueryPlan:
    normalized = normalize_query_text(query)
    if not normalized:
        return QueryPlan(operation="search", normalized_query=normalized, wants_count=False)

    wants_count = any(marker in normalized for marker in _COUNT_MARKERS)
    if wants_count:
        return QueryPlan(operation="aggregate_count", normalized_query=normalized, wants_count=True)

    return QueryPlan(operation="search", normalized_query=normalized, wants_count=False)
