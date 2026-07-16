"""
variance in E/P and S/P among the S&P500 and NASDAQ 100
2. Grab their total revenue, share count, EPS, and share price (average for last 8 quarters?)
3. Compile into dataframe, average row wise, calculate TR/(SP*SC) and EPS/SP
4. analyze change over time
"""

from io import StringIO
import pandas as pd
import requests
# from scrape_textual import open_parser, ingest_textual_ticker
# from db_setup import get_connection
# from models import SECFilingParserError
from config import nonsec_headers
from db_setup import get_available_tickers
from update_numerical import ingest_numerical_tickers

SKIP_UPDATED = True  # skip tickers that already have data in the DB

def get_html(url):
    return requests.get(url, headers=nonsec_headers()).text

spy = pd.read_html(StringIO(get_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")))[0]["Symbol"].to_list()

qqq = pd.read_html(StringIO(get_html("https://en.wikipedia.org/wiki/List_of_NASDAQ-100_companies")))[0]["Ticker"].to_list()

tickers = sorted({s.replace(".", "-") for s in spy + qqq})

if SKIP_UPDATED:
    already_updated = {t for t, _ in get_available_tickers()}
    skipped = [t for t in tickers if t in already_updated]
    tickers = [t for t in tickers if t not in already_updated]
    if skipped:
        print(f"Skipping {len(skipped)} already-updated ticker(s): {', '.join(skipped)}")

total_upserted, total_failed = ingest_numerical_tickers(tickers)

print(f"\nDone. Total upserted: {total_upserted}, total failed: {total_failed}")
