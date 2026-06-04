from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Literal

from db import query_facts
from models import Fact


def _safe_div(a: float, b: float) -> float | None:
    return None if b == 0 else a / b

def _safe_pct(a: float, b: float) -> float | None:
    return None if b == 0 else (a / b) * 100

@dataclass
class Concepts:
    key: str
    display_name: str
    resolutions: list
    format_type: Literal["percentage", "ratio", "multiple", "currency", "raw", "text"] = "raw"
    match_by_end_date: bool = False
    kind: Literal["numeric", "textual"] = "numeric"


REGISTRY: dict[str, Concepts] = {

    # components

    "revenue_goods": Concepts(
        key="revenue_goods",
        display_name="Revenue (Goods)",
        resolutions=[
            ["us-gaap:SalesRevenueGoodsNet"]
        ],
    ),
    "revenue_services": Concepts(
        key="revenue_services",
        display_name="Revenue (Services)",
        resolutions=[
            ["us-gaap:SalesRevenueServicesNet"]
        ],
    ),

    # totals

    "revenue": Concepts(
        key="revenue",
        display_name="Revenue",
        resolutions=[
            ["us-gaap:Revenues",
            "us-gaap:SalesRevenueNet",
            "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
            "us-gaap:TotalRevenuesAndOtherIncome",
            "us-gaap:OperatingRevenueDirect",
            "us-gaap:HealthCareOrganizationRevenue",
            "us-gaap:InterestAndDividendIncomeOperating",
            "us-gaap:RealEstateRevenueNet",
            "us-gaap:PremiumRevenueNet",
            "us-gaap:OilAndGasRevenue"],
            (("revenue_goods", "revenue_services"), lambda v: v[0] + v[1]),
        ],
    ),
    "eps": Concepts(
        key="eps",
        display_name="Diluted EPS",
        resolutions=[
            ["us-gaap:EarningsPerShareDiluted"]
        ],
    ),
    "liabilities": Concepts(
        key="liabilities",
        display_name="Total Liabilities",
        resolutions=[
            ["us-gaap:Liabilities"],
            (("noncurrent_liabilities", "current_liabilities"), lambda v: v[0] + v[1], (0.0, 0.0)),
        ],
    ),
    "gross": Concepts(
        key="gross",
        display_name="Gross Profit",
        resolutions=[
            ["us-gaap:GrossProfit"],
            (("revenue", "cost_of_revenue"), lambda v: v[0] - v[1]),
        ],
    ),
    "operating": Concepts(
        key="operating",
        display_name="Operating Income",
        resolutions=[[
            "us-gaap:OperatingIncomeLoss",
            "us-gaap:OperatingProfitLoss",
            "us-gaap:IncomeLossFromOperatingActivities",
        ]],
    ),
    "net": Concepts(
        key="net",
        display_name="Net Income",
        resolutions=[
            ["us-gaap:NetIncomeLoss"]
        ],
    ),
    "shares_outstanding": Concepts(
        key="shares_outstanding",
        display_name="Shares Outstanding",
        resolutions=[[
            "us-gaap:EntityCommonStockSharesOutstanding",
            "us-gaap:CommonStockSharesOutstanding",
            "us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding",
            "us-gaap:WeightedAverageNumberOfSharesOutstandingBasic",
        ]],
    ),
    # ---------------- qualitative ----------------
    "risk_factors": Concepts(
        key="risk_factors",
        display_name="Risk Factors",
        resolutions=[["us-gaap:RiskFactorsTextBlock"]],
        format_type="text",
        kind="qualitative",
    ),
    "mda": Concepts(
        key="mda",
        display_name="Management's Discussion & Analysis",
        resolutions=[[
            "us-gaap:ManagementsDiscussionAndAnalysisTextBlock",
        ]],
        format_type="text",
        kind="qualitative",
    )
}


def _get_period_key(fact: Fact, by_end_date: bool = False) -> tuple | None:
    if by_end_date:
        anchor = fact.end_date or fact.instant_date
        return ("anchor", anchor) if anchor else None

    if fact.instant_date:
        return ("instant", fact.instant_date)
    if fact.start_date and fact.end_date:
        return ("duration", fact.start_date, fact.end_date)
    return None


def _parse_value(value) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", ""))
    except (ValueError, TypeError):
        return None


def _build_period_index(facts: list[Fact], by_end_date: bool = False) -> dict[tuple, Fact]:
    index: dict[tuple, Fact] = {}
    for fact in facts:
        key = _get_period_key(fact, by_end_date)
        if key and key not in index:
            index[key] = fact
    return index


_UNIT_FOR_FORMAT: dict[str, str] = {
    "percentage": "%",
    "multiple": "x",
    "currency": "USD",
    "ratio": "",
}


def _resolve_formula(
    ticker: str,
    metric: Concepts,
    input_keys: tuple[str, ...],
    compute: Callable[[list[float]], float | None],
    query_type: str,
    defaults: tuple[float | None, ...] | None = None,
) -> list[Fact]:
    input_facts: list[list[Fact]] = []
    for key in input_keys:
        try:
            facts = resolve(ticker, key, query_type)
        except ValueError:
            return []
        input_facts.append(facts)

    if defaults is None:
        if any(not f for f in input_facts):
            return []
    else:
        if all(not f for f in input_facts):
            return []

    by_end = metric.match_by_end_date
    period_indices = [_build_period_index(facts, by_end) for facts in input_facts]

    if defaults is None:
        common_periods = set(period_indices[0].keys())
        for idx in period_indices[1:]:
            common_periods &= set(idx.keys())
    else:
        common_periods = set().union(*(set(idx.keys()) for idx in period_indices))

    if not common_periods:
        return []

    first_nonempty = next((idx for idx in period_indices if idx), None)
    if first_nonempty is None:
        return []
    unit_str = _UNIT_FOR_FORMAT.get(
        metric.format_type,
        first_nonempty[next(iter(first_nonempty))].unit or "",
    )

    results: list[Fact] = []
    for period_key in common_periods:
        values: list[float] = []
        ref_fact: Fact | None = None
        skip = False

        for i, idx in enumerate(period_indices):
            if period_key in idx:
                fact = idx[period_key]
                if ref_fact is None:
                    ref_fact = fact
                val = _parse_value(fact.value)
                if val is None:
                    skip = True
                    break
                values.append(val)
            elif defaults is not None and (default := defaults[i]) is not None:
                values.append(default)
            else:
                skip = True
                break

        if skip or ref_fact is None:
            continue

        computed = compute(values)
        if computed is not None:
            results.append(Fact(
                local_name=metric.display_name,
                period_type=ref_fact.period_type,
                value=computed,
                instant_date=ref_fact.instant_date,
                start_date=ref_fact.start_date,
                end_date=ref_fact.end_date,
                unit=unit_str,
                decimals=2,
                accession_number=ref_fact.accession_number,
            ))

    results.sort(
        key=lambda r: r.instant_date or r.end_date,
        reverse=True,
    )
    return results


def resolve(ticker: str, key: str, query_type: str) -> list[Fact]:
    """resolve a metric to `Fact`s, trying each resolution strategy in order."""
    if key not in REGISTRY:
        raise ValueError(f"Unknown metric: {key!r}")

    metric = REGISTRY[key]
    for resolution in metric.resolutions:
        if isinstance(resolution, list):
            result = query_facts(ticker, resolution, query_type, fact_kind=metric.kind)
            if result:
                return result
        else:
            input_keys, compute, *rest = resolution
            defaults = rest[0] if rest else None
            result = _resolve_formula(ticker, metric, input_keys, compute, query_type, defaults)
            if result:
                return result

    return []
