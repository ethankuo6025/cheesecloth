"""
variance in E/P and S/P among the S&P500 and NASDAQ 100
2. Grab their total revenue, share count, EPS, and share price (average for last 8 quarters?)
3. Compile into dataframe, average row wise, calculate TR/(SP*SC) and EPS/SP
4. analyze change over time
"""

from io import StringIO
import pandas as pd
import requests
# from add import open_parser, scrape_ticker
# from db_setup import get_connection
# from models import SECFilingParserError
from config import nonsec_headers
from update import update_tickers
def get_html(url):
    return requests.get(url, headers=nonsec_headers()).text

spy = pd.read_html(StringIO(get_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")))[0]["Symbol"].to_list()

qqq = pd.read_html(StringIO(get_html("https://en.wikipedia.org/wiki/List_of_NASDAQ-100_companies")))[0]["Ticker"].to_list()

tickers = sorted({s.replace(".", "-") for s in spy + qqq})

total_upserted, total_failed = update_tickers(tickers)

print(f"\nDone. Total upserted: {total_upserted}, total failed: {total_failed}")
