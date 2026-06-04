from __future__ import annotations

import db
from models import Fact


def resolve(ticker: str, key: str, query_type: str) -> list[Fact]:
    """resolve a metric to `Fact`s using a specific company's configured mappings."""
    metric = db.get_metric(key)
    if metric is None:
        raise ValueError(f"Unknown metric: {key!r}")

    qnames = db.get_metric_mappings(ticker, key)
    if not qnames:
        return []

    # query_facts ranks qnames by array_position, so priority order is preserved.
    # Textual facts (no unit) are exactly the 'text'-formatted metrics.
    fact_kind = "textual" if metric.format_type == "text" else "numeric"
    return db.query_facts(ticker, qnames, query_type, fact_kind=fact_kind)
