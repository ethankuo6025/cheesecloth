from operator import itemgetter
from db import query_facts_by_qname
from datetime import timedelta

qnames_mapping = {
    "revenue": [
        "us-gaap:Revenues", 
        "us-gaap:SalesRevenueNet", 
        "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
        "us-gaap:RevenueFromContractWithCustomerIncludingAssessedTax",
        "us-gaap:SalesRevenueGoodsNet", 
        "us-gaap:SalesRevenueServicesNet"
    ],
    "eps": [
        "us-gaap:EarningsPerShareDiluted",
        "us-gaap:IncomeLossFromContinuingOperationsPerDilutedShare",
        "us-gaap:ProfitLossPerShareDiluted"
    ],
    "liabilities": [
        "us-gaap:Liabilities"
    ],
    "gross": [
        "us-gaap:GrossProfit",
        "us-gaap:SalesRevenueNet",
        "us-gaap:SalesRevenueGoodsNet"
    ],
    "operating": [
        "us-gaap:OperatingIncomeLoss",
        "us-gaap:OperatingProfitLoss",
        "us-gaap:IncomeLossFromOperatingActivities"
    ],
    "net": [
        "us-gaap:NetIncomeLoss",
        "us-gaap:NetIncomeLossAvailableToCommonStockholdersBasic",
        "us-gaap:ProfitLoss"
    ],
    "shares_outstanding": [
        "us-gaap:EntityCommonStockSharesOutstanding",
        "us-gaap:CommonStockSharesOutstanding",
        "us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding",
        "us-gaap:WeightedAverageNumberOfSharesOutstandingBasic"
    ],
    "total_assets": [
        "us-gaap:Assets"
    ],
    "cash_on_hand": [
        "us-gaap:CashAndCashEquivalentsAtCarryingValue",
        "us-gaap:CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
        "us-gaap:Cash"
    ],
    "long_term_debt": [
        "us-gaap:LongTermDebtNoncurrent",
        "us-gaap:LongTermDebt",
        "us-gaap:LongTermDebtAndCapitalLeaseObligations"
    ]
}

VALUE_IDX = 2
INST_DATE_IDX = 3
START_DATE_IDX = 4
END_DATE_IDX = 5
ACC_IDX = 8

def _filter_dedup_and_sort(facts: list[tuple], query_type: str) -> list[tuple]:
    """
    Filter and deduplicate facts based on query type.
    
    - Instant date facts are ALWAYS included regardless of query_type
    - Duration facts are filtered based on query_type:
      - "annual": date ranges > 350 days
      - "quarterly": date ranges < 100 days  
      - "all": no filtering
    """
    seen, deduped = set(), []
    for f in facts:
        period = f[INST_DATE_IDX] or (f[START_DATE_IDX], f[END_DATE_IDX])
        if period not in seen:
            seen.add(period)
            deduped.append(f)

    filtered = []
    for f in deduped:
        if f[INST_DATE_IDX]:
            filtered.append(f)
        elif f[START_DATE_IDX] and f[END_DATE_IDX]:
            if query_type == "all":
                filtered.append(f)
            else:
                duration = f[END_DATE_IDX] - f[START_DATE_IDX]
                if query_type == "annual" and duration > timedelta(days=350):
                    filtered.append(f)
                elif query_type == "quarterly" and duration < timedelta(days=100):
                    filtered.append(f)
        else:
            filtered.append(f)

    return sorted(filtered, key=lambda x: x[INST_DATE_IDX] or x[END_DATE_IDX] or x[START_DATE_IDX], reverse=True)

def get_facts(ticker: str, target_qname: str, query_type: str) -> list[tuple]:
    try:
        qnames_hierarchy = qnames_mapping[target_qname]
    except KeyError as e:
        raise ValueError(f"Unknown query field: {target_qname!r}") from e

    results: list[tuple] = []
    filings: set[str] = set()
    for qname in qnames_hierarchy:
        # check accession_number: fact[8]
        facts = [fact for fact in query_facts_by_qname(ticker, qname) if fact[8] not in filings]
        filings.update(map(itemgetter(8), facts)) #set([f[8] for f in facts])
        results += facts
    return _filter_dedup_and_sort(results, query_type)
