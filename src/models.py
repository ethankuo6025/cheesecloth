"""shared data contracts used across the project"""
from __future__ import annotations

from datetime import date
from dataclasses import dataclass, field
from enum import Enum

class SECFilingParserError(Exception):
    pass

class TickerNotFoundError(SECFilingParserError):
    """thrown by SECFilingParser._get_cik() on invalid tickers."""
    pass

class FilingFetchError(SECFilingParserError):
    """thrown by SECFilingParser._get_json() on http issues."""
    pass

class NumericalFetchError(SECFilingParserError):
    """thrown by SECFilingParser.get_numerical_facts() on http issues."""
    pass

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
    accession_number: str

@dataclass(frozen=True)
class Metric:
    key: str
    display_name: str
    format_type: str

@dataclass(frozen=True)
class TextualFact:
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
    dimensions: dict[str, str] = field(default_factory=dict)

@dataclass(frozen=True)
class Filing:
    cik: str
    accession_number: str
    entry_file: str
    filing_type: str

@dataclass(frozen=True)
class NumericalFact:
    ticker: str
    cik: str
    accession_number: str
    taxonomy: str
    fname: str
    unit: str
    period_type: PeriodType
    value: str | None = None
    instant_date: date | None = None
    start_date: date | None = None
    end_date: date | None = None
    fiscal_year: int | None = None
    fiscal_period: str | None = None
    form: str | None = None
    filed_date: date | None = None