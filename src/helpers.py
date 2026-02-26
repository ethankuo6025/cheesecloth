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
    ]
}

def sort_by_date(facts: list[tuple], descending: bool=True) -> list[tuple]:
    if facts[0][3]: # using instance dates
        col = 3
    else: # using end dates
        col = 5
    sorted_facts = sorted(facts, key=lambda x: x[col], reverse=descending)
    return sorted_facts

def dedup(facts: list[tuple]) -> list[tuple]:
    seen = set()
    deduped_facts = []
    
    for fact in facts:
        instance_date = fact[3]
        start_date = fact[4]
        end_date = fact[5]
        period = instance_date if instance_date is not None else (start_date, end_date)
        if period not in seen:
            seen.add(period)
            deduped_facts.append(fact)
            
    return deduped_facts

def get_facts(ticker: str, target_qname: str, query_type: str) -> list[tuple]:
    try:
        qnames_hierarchy = qnames_mapping[target_qname]
    except KeyError as e:
        raise ValueError(f"Unknown query field: {target_qname!r}") from e

    results: list[tuple] = []
    filings: set[str] = set()
    for qname in qnames_hierarchy:
        # check accession_number: fact[8]
        facts = [fact for fact in query_facts_by_qname(ticker, qname) if fact[7] not in filings]
        filings.update(map(itemgetter(8), facts)) #set([f[8] for f in facts])
        results += facts
    return filter_facts(sort_by_date(dedup(results)), query_type)

def filter_facts(facts: list[tuple], query_type: str) -> list[tuple]:
    if query_type == "annual":
        facts = [fact for fact in facts if fact[3] is not None or (fact[5] - fact[4] > timedelta(days=350))]
    elif query_type == "quarterly":
        facts = [fact for fact in facts if fact[3] is not None or (fact[5] - fact[4] < timedelta(days=100))]
    return facts