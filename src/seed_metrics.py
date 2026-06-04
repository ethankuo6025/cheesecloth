from __future__ import annotations

import logging

import query

logger = logging.getLogger(__name__)

# key, display_name, format_type
SEED_METRICS: list[tuple[str, str, str]] = [
    ("cash_on_hand",         "Cash on Hand",                       "currency"),
    ("cost_of_revenue",      "Cost of Revenue",                    "currency"),
    ("current_assets",       "Current Assets",                     "currency"),
    ("current_liabilities",  "Current Liabilities",                "currency"),
    ("eps",                  "Diluted EPS",                        "currency"),
    ("gross",                "Gross Profit",                       "currency"),
    ("liabilities",          "Total Liabilities",                  "currency"),
    ("long_term_total_debt", "Long-Term Total Debt",               "currency"),
    ("mda",                  "Management's Discussion & Analysis", "text"),
    ("net",                  "Net Income",                         "currency"),
    ("operating",            "Operating Income",                   "currency"),
    ("revenue",              "Revenue",                            "currency"),
    ("revenue_goods",        "Revenue (Goods)",                    "currency"),
    ("revenue_services",     "Revenue (Services)",                 "currency"),
    ("risk_factors",         "Risk Factors",                       "text"),
    ("shares_outstanding",   "Shares Outstanding",                 "number"),
    ("stockholders_equity",  "Stockholders' Equity",               "currency"),
    ("total_assets",         "Total Assets",                       "currency"),
]


def seed_metrics() -> int:
    """upsert every catalog metric. returns the number of metrics."""
    for key, display_name, format_type in SEED_METRICS:
        query.add_metric(key, display_name, format_type)
    logger.info("Seeded %d metric(s) into the catalog.", len(SEED_METRICS))
    return len(SEED_METRICS)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    seed_metrics()
