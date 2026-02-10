from db import query_facts_by_qname

qname_mapping = {
    "revenue": ["us-gaap:Revenues",
                 "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax"],
    "eps": ["us-gaap:EarningsPerShareDiluted"]}

def get_facts(ticker: str, query_type: str) -> list[tuple]:
    try:
        qnames = qname_mapping[query_type]
    except KeyError as e:
        raise ValueError(f"Unknown query field: {query_type!r}") from e

    qnames = qname_mapping[query_type]
    results = []

    for qname in qnames:
        results += query_facts_by_qname(ticker, qname)
    return results