from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Literal

from helpers import get_facts

LOCAL_NAME_IDX = 0
PERIOD_TYPE_IDX = 1
VALUE_IDX = 2
INSTANT_DATE_IDX = 3
START_DATE_IDX = 4
END_DATE_IDX = 5
UNIT_IDX = 6
DECIMALS_IDX = 7
ACCESSION_IDX = 8

def _safe_pct(a: float, b: float) -> float | None:
    return None if b == 0 else (a / b) * 100

@dataclass
class MetricInput:
    name: str
    query: str  # key in qnames_mapping

@dataclass
class MetricDefinition:
    key: str
    display_name: str
    inputs: tuple[MetricInput, ...]
    compute: Callable[[list[float]], float | None]
    format_type: Literal["percentage", "ratio", "multiple", "currency"] = "percentage"
    # when True, match inputs by end/instant date only instead of exact period.
    # needed when mixing duration facts (e.g. net income) with instant facts (e.g. assets).
    match_by_end_date: bool = False

METRICS_REGISTRY: dict[str, MetricDefinition] = {
    "gross_margin": MetricDefinition(
        key="gross_margin",
        display_name="Gross Margin",
        inputs=(
            MetricInput("gross_profit", "gross"),
            MetricInput("revenue", "revenue"),
        ),
        compute=lambda v: _safe_pct(v[0], v[1]),
    ),
}

def _get_period_key(fact: tuple, by_end_date: bool = False) -> tuple | None:
    instant = fact[INSTANT_DATE_IDX]
    start = fact[START_DATE_IDX]
    end = fact[END_DATE_IDX]

    if by_end_date:
        # collapse to a single anchor date so duration and instant can match
        anchor = end or instant
        return ("anchor", anchor) if anchor else None

    if instant:
        return ("instant", instant)
    if start and end:
        return ("duration", start, end)
    return None


def _parse_value(value) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", ""))
    except (ValueError, TypeError):
        return None


def _build_period_index(facts: list[tuple], by_end_date: bool = False) -> dict[tuple, tuple]:
    index: dict[tuple, tuple] = {}
    for fact in facts:
        key = _get_period_key(fact, by_end_date)
        if key and key not in index:
            index[key] = fact
    return index


def calculate_metric(ticker: str, metric_key: str, query_type: str) -> list[tuple]:
    """Fetch inputs, match by period, compute metric, return fact-like tuples."""
    if metric_key not in METRICS_REGISTRY:
        raise ValueError(f"Unknown metric: {metric_key!r}")

    defn = METRICS_REGISTRY[metric_key]

    input_facts: list[list[tuple]] = []
    for inp in defn.inputs:
        try:
            facts = get_facts(ticker, inp.query, query_type)
        except ValueError:
            return []
        input_facts.append(facts)

    if any(len(f) == 0 for f in input_facts):
        return []

    by_end = defn.match_by_end_date
    period_indices = [_build_period_index(facts, by_end) for facts in input_facts]

    common_periods = set(period_indices[0].keys())
    for idx in period_indices[1:]:
        common_periods &= set(idx.keys())

    if not common_periods:
        return []

    unit_map = {"percentage": "%", "multiple": "x", "currency": "USD", "ratio": ""}
    unit_str = unit_map.get(defn.format_type, "")

    results: list[tuple] = []
    for period_key in common_periods:
        values: list[float] = []
        ref_fact = period_indices[0][period_key]

        for idx in period_indices:
            val = _parse_value(idx[period_key][VALUE_IDX])
            if val is None:
                break
            values.append(val)
        else:
            computed = defn.compute(values)
            if computed is not None:
                results.append((
                    defn.display_name,
                    ref_fact[PERIOD_TYPE_IDX],
                    computed,
                    ref_fact[INSTANT_DATE_IDX],
                    ref_fact[START_DATE_IDX],
                    ref_fact[END_DATE_IDX],
                    unit_str,
                    2,
                    ref_fact[ACCESSION_IDX],
                ))

    results.sort(
        key=lambda r: r[INSTANT_DATE_IDX] or r[END_DATE_IDX] or r[START_DATE_IDX],
        reverse=True,
    )
    return results