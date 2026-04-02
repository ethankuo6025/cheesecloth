from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Literal

from db import query_facts

LOCAL_NAME_IDX = 0
PERIOD_TYPE_IDX = 1
VALUE_IDX = 2
INSTANT_DATE_IDX = 3
START_DATE_IDX = 4
END_DATE_IDX = 5
UNIT_IDX = 6
DECIMALS_IDX = 7
ACCESSION_IDX = 8

def _safe_div(a: float, b: float) -> float | None:
    return None if b == 0 else a / b

def _safe_pct(a: float, b: float) -> float | None:
    return None if b == 0 else (a / b) * 100

@dataclass
class Concepts:
    key: str
    display_name: str
    resolutions: list # can have direct qname lookups or tuples representing calculations
    format_type: Literal["percentage", "ratio", "multiple", "currency", "raw"] = "raw"
    match_by_end_date: bool = False


REGISTRY: dict[str, Concepts] = {

    '''components'''

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
    "cost_of_revenue": Concepts(
        key="cost_of_revenue",
        display_name="Cost of Revenue",
        resolutions=[
            ["us-gaap:CostOfRevenue",
            "us-gaap:CostOfGoodsAndServicesSold"]
        ],
    ),
    "lease_liabilities": Concepts(
        key="lease_liabilities",
        display_name="Lease Liabilities",
        resolutions=[
            ["us-gaap:FinanceLeaseLiability"]
        ],
    ),
    "current_liabilities": Concepts(
        key="current_liabilities",
        display_name="Current Liabilities",
        resolutions=[
            ["us-gaap:LiabilitiesCurrent"]
        ],
    ),
    "noncurrent_liabilities": Concepts(
        key="noncurrent_liabilities",
        display_name="Current Liabilities",
        resolutions=[
            ["us-gaap:LiabilitiesNoncurrent"]
        ],
    ),
    "lt_debt_noncurrent": Concepts(
        key="lt_debt_noncurrent",
        display_name="LT Debt Noncurrent",
        resolutions=[
            ["us-gaap:LongTermDebtNoncurrent"]
        ],
    ),
    "lt_debt_current": Concepts(
        key="lt_debt_current",
        display_name="LT Debt Current",
        resolutions=[
            ["us-gaap:LongTermDebtCurrent"]
        ],
    ),

    '''totals'''

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
        resolutions=[
            ["us-gaap:OperatingIncomeLoss",
            "us-gaap:OperatingProfitLoss",
            "us-gaap:IncomeLossFromOperatingActivities"]
        ],
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
        resolutions=[
            ["us-gaap:EntityCommonStockSharesOutstanding",
            "us-gaap:CommonStockSharesOutstanding",
            "us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding",
            "us-gaap:WeightedAverageNumberOfSharesOutstandingBasic"]
        ],
    ),
    "total_assets": Concepts(
        key="total_assets",
        display_name="Total Assets",
        resolutions=[
            ["us-gaap:Assets"]
        ],
    ),
    "cash_on_hand": Concepts(
        key="cash_on_hand",
        display_name="Cash on Hand",
        resolutions=[
            ["us-gaap:CashAndCashEquivalentsAtCarryingValue",
            "us-gaap:CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
            "us-gaap:Cash"],
        ],
    ),
    "debt": Concepts(
        key="debt",
        display_name="Debt",
        resolutions=[
            (("lt_debt_noncurrent", "lt_debt_current"), lambda v: v[0] + v[1], (0.0, 0.0)),
            ["us-gaap:LongTermDebtAndCapitalLeaseObligations",
            "us-gaap:LongTermDebt"]
        ],
    ),
    "equity": Concepts(
        key="equity",
        display_name="Stockholders' Equity",
        resolutions=[
            ["us-gaap:StockholdersEquity"]
        ],
    ),
    "current_assets": Concepts(
        key="current_assets",
        display_name="Current Assets",
        resolutions=[
            ["us-gaap:AssetsCurrent"]
        ],
    ),

    '''metrics'''

    "gross_margin": Concepts(
        key="gross_margin",
        display_name="Gross Margin",
        resolutions=[
            (("gross", "revenue"), lambda v: _safe_pct(v[0], v[1]))
        ],
        format_type="percentage",
    ),
    "operating_margin": Concepts(
        key="operating_margin",
        display_name="Operating Margin",
        resolutions=[
            (("operating", "revenue"), lambda v: _safe_pct(v[0], v[1]))
        ],
        format_type="percentage",
    ),
    "profit_margin": Concepts(
        key="profit_margin",
        display_name="Profit Margin",
        resolutions=[
            (("net", "revenue"), lambda v: _safe_pct(v[0], v[1]))
        ],
        format_type="percentage",
    ),
    "cash_to_assets": Concepts(
        key="cash_to_assets",
        display_name="Cash/Assets",
        resolutions=[
            (("cash_on_hand", "total_assets"), lambda v: _safe_pct(v[0], v[1]))
        ],
        format_type="percentage",
    ),
    "debt_to_assets": Concepts(
        key="debt_to_assets",
        display_name="Debt/Assets",
        resolutions=[
            (("debt", "total_assets"), lambda v: _safe_pct(v[0], v[1]))
        ],
        format_type="percentage",
    ),
    "debt_to_equity": Concepts(
        key="debt_to_equity",
        display_name="Debt/Equity",
        resolutions=[
            (("debt", "equity"), lambda v: _safe_div(v[0], v[1]))
        ],
        format_type="multiple",
    ),
    "current_ratio": Concepts(
        key="current_ratio",
        display_name="Current Ratio",
        resolutions=[
            (("current_assets", "current_liabilities"), lambda v: _safe_div(v[0], v[1]))
        ],
        format_type="multiple",
    ),
    "net_debt": Concepts(
        key="net_debt",
        display_name="Net Debt",
        resolutions=[
            (("debt", "cash_on_hand"), lambda v: v[0] - v[1])
        ],
        format_type="currency",
    ),
    "working_capital": Concepts(
        key="working_capital",
        display_name="Working Capital",
        resolutions=[
            (("current_assets", "current_liabilities"), lambda v: v[0] - v[1])
        ],
        format_type="currency",
    ),
    "roa": Concepts(
        key="roa",
        display_name="Return on Assets",
        resolutions=[
            (("net", "total_assets"), lambda v: _safe_pct(v[0], v[1]))
        ],
        format_type="percentage",
        match_by_end_date=True,
    ),
    "roe": Concepts(
        key="roe",
        display_name="Return on Equity",
        resolutions=[
            (("net", "equity"), lambda v: _safe_pct(v[0], v[1]))
        ],
        format_type="percentage",
        match_by_end_date=True,
    ),
}


def _get_period_key(fact: tuple, by_end_date: bool = False) -> tuple | None:
    instant = fact[INSTANT_DATE_IDX]
    start = fact[START_DATE_IDX]
    end = fact[END_DATE_IDX]

    if by_end_date:
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
) -> list[tuple]:
    input_facts: list[list[tuple]] = []
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
        first_nonempty[next(iter(first_nonempty))][UNIT_IDX] or "",
    )

    results: list[tuple] = []
    for period_key in common_periods:
        values: list[float] = []
        ref_fact = None
        skip = False

        for i, idx in enumerate(period_indices):
            if period_key in idx:
                fact = idx[period_key]
                if ref_fact is None:
                    ref_fact = fact
                val = _parse_value(fact[VALUE_IDX])
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
            results.append((
                metric.display_name,
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
        key=lambda r: r[INSTANT_DATE_IDX] or r[END_DATE_IDX],
        reverse=True,
    )
    return results


def resolve(ticker: str, key: str, query_type: str) -> list[tuple]:
    """Resolve a metric to fact-like tuples, trying each resolution strategy in order."""
    if key not in REGISTRY:
        raise ValueError(f"Unknown metric: {key!r}")

    metric = REGISTRY[key]

    for resolution in metric.resolutions:
        if isinstance(resolution, list):
            result = query_facts(ticker, resolution, query_type)
            if result:
                return result
        else:
            input_keys, compute, *rest = resolution
            defaults = rest[0] if rest else None
            result = _resolve_formula(ticker, metric, input_keys, compute, query_type, defaults)
            if result:
                return result

    return []
