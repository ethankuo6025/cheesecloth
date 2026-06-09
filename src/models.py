"""shared data contracts used across the project"""
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

class Metric(NamedTuple):
    key: str
    display_name: str
    format_type: str
