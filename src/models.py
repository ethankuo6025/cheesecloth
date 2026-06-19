"""shared data contracts used across the project"""
from __future__ import annotations

from datetime import date
from typing import NamedTuple
from dataclasses import dataclass, field
from enum import Enum

class PeriodType(Enum):
    INSTANT = "instant"
    DURATION = "duration"

@dataclass(frozen=True)
class Fact:
    local_name: str
    period_type: str
    value: str | float | int | None
    instant_date: date | None
    start_date: date | None
    end_date: date | None
    unit: str | None
    decimals: int | None
    accession_number: str

@dataclass(frozen=True)
class Metric:
    key: str
    display_name: str
    format_type: str

@dataclass(frozen=True)
class ParsedFact:
    ticker: str
    cik: str
    accession_number: str
    qname: str
    namespace: str
    local_name: str
    period_type: PeriodType
    value: str | None = None
    instant_date: date | None = None
    start_date: date | None = None
    end_date: date | None = None
    unit: str | None = None
    decimals: int | None = None
    dimensions: dict[str, str] = field(default_factory=dict)

@dataclass(frozen=True)
class Filing:
    cik: str
    accession_number: str
    entry_file: str
    filing_type: str