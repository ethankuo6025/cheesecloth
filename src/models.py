"""Shared data contracts used across the project.

`Fact` is the canonical shape for a row coming out of `db.query_facts` /
`query.resolve`. It is a NamedTuple, so callers may still unpack positionally
or index numerically (backwards-compatible with the old tuple layout) but
new code should use named attributes (`f.value`, `f.end_date`, ...).
"""
from __future__ import annotations

from datetime import date
from typing import NamedTuple


class Fact(NamedTuple):
    local_name: str
    period_type: str
    value: str | float | int | None
    instant_date: date | None
    start_date: date | None
    end_date: date | None
    unit: str | None
    decimals: int | None
    accession_number: str
