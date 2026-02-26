from datetime import timedelta
from db import query_facts_by_qname

qnames_mapping: dict[str, list[tuple[str, ...]]] = {
    "revenue": [
        ("us-gaap:Revenues",),
        ("us-gaap:SalesRevenueNet",),
        ("us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",),
        ("us-gaap:RevenueFromContractWithCustomerIncludingAssessedTax",),
        ("us-gaap:SalesRevenueGoodsNet",),
        ("us-gaap:SalesRevenueServicesNet",),
    ],
    "eps": [
        ("us-gaap:EarningsPerShareDiluted",),
        ("us-gaap:IncomeLossFromContinuingOperationsPerDilutedShare",),
        ("us-gaap:ProfitLossPerShareDiluted",),
    ],
    "debt": [
        ("us-gaap:Debt",),
        ("us-gaap:DebtAndCapitalLeaseObligations",),
        ("us-gaap:DebtAndFinanceLeaseObligations",),
        ("us-gaap:LongTermDebtAndCapitalLeaseObligations",),
        ("us-gaap:LongTermDebtAndFinanceLeases",),
        ("us-gaap:DebtCurrent", "us-gaap:DebtNoncurrent"),
        ("us-gaap:DebtCurrent", "us-gaap:LongTermDebt"),
        ("us-gaap:LongTermDebtCurrent", "us-gaap:LongTermDebtNoncurrent"),
        ("us-gaap:LongTermDebtCurrent", "us-gaap:LongTermDebt"),
    ],
}

VALUE_IDX = 2
INST_DATE_IDX = 3
START_DATE_IDX = 4
END_DATE_IDX = 5
ACC_IDX = 8

def _filter_and_sort(facts: list[tuple], query_type: str) -> list[tuple]:
    seen, deduped = set(), []
    for f in facts:
        period = f[INST_DATE_IDX] or (f[START_DATE_IDX], f[END_DATE_IDX])
        if period not in seen:
            seen.add(period)
            deduped.append(f)

    if query_type == "annual":
        deduped = [f for f in deduped if f[INST_DATE_IDX] or (f[END_DATE_IDX] - f[START_DATE_IDX] > timedelta(days=350))]
    elif query_type == "quarterly":
        deduped = [f for f in deduped if f[INST_DATE_IDX] or (f[END_DATE_IDX] - f[START_DATE_IDX] < timedelta(days=100))]

    return sorted(deduped, key=lambda x: x[INST_DATE_IDX] or x[END_DATE_IDX], reverse=True)

def get_facts(ticker: str, target_qname: str, query_type: str) -> list[tuple]:
    if target_qname not in qnames_mapping:
        raise ValueError(f"Unknown query field: {target_qname!r}")

    results = []
    seen_filings = set()

    for group in qnames_mapping[target_qname]:
        fact_map = {}  # ((accession, period), qname) -> fact
        contexts = set() # set of (accession, period)

        for qname in group:
            for f in query_facts_by_qname(ticker, qname):
                acc = f[ACC_IDX]
                if acc in seen_filings:
                    continue
                
                period = ("I", f[INST_DATE_IDX]) if f[INST_DATE_IDX] else ("D", f[START_DATE_IDX], f[END_DATE_IDX])
                context = (acc, period)
                
                fact_map[(context, qname)] = f
                contexts.add(context)

        successful_filings = set()
        
        for context in contexts:
            if all((context, q) in fact_map for q in group):
                try:
                    total = sum(
                        float(str(fact_map[(context, q)][VALUE_IDX]).replace(",", "")) 
                        for q in group
                    )
                    
                    template = list(fact_map[(context, group[0])])
                    template[VALUE_IDX] = str(total)
                    
                    results.append(tuple(template))
                    successful_filings.add(context[0])  # context[0] = accession number
                except (ValueError, TypeError):
                    continue
                    
        seen_filings.update(successful_filings)

    return _filter_and_sort(results, query_type)