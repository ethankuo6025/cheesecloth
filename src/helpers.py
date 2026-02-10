from db import query_facts_by_qname
from datetime import timedelta
qname_mapping = {
    "revenue": ["us-gaap:Revenues",
                 "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax"],
    "eps": ["us-gaap:EarningsPerShareDiluted"]}

def get_facts(ticker: str, target_qname: str, query_type: str) -> list[tuple]:
    try:
        qnames = qname_mapping[target_qname]
    except KeyError as e:
        raise ValueError(f"Unknown query field: {target_qname!r}") from e

    qnames = qname_mapping[target_qname]
    facts = []
    for qname in qnames:
        facts += query_facts_by_qname(ticker, qname)
    return filter_facts(facts, query_type)

def filter_facts(facts: list[tuple], query_type: str) -> list[tuple]:
    if query_type == "annual":
        facts = [fact for fact in facts if fact[3] is not None or (fact[5] - fact[4] > timedelta(days=350))]
    return facts