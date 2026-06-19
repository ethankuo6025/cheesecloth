"""global config file"""
import os
from typing import Any
from dotenv import load_dotenv

load_dotenv()

DEFAULT_FILING_TYPES = ("10-K", "10-Q")

ARELLE_PLUGINS_PATH = os.getenv("ARELLE_PLUGINS_PATH", "")

SEC_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
def db_kwargs() -> dict[str, Any]:
    """connection kwargs for psycopg.connect(**db_kwargs())."""
    return {
        "host": DB_HOST,
        "port": DB_PORT,
        "dbname": DB_NAME,
        "user": DB_USER,
        "password": DB_PASSWORD,
    }

# https://www.sec.gov/about/webmaster-frequently-asked-questions#developers
SEC_USER_AGENT = os.getenv("SEC_USER_AGENT")
def sec_headers() -> dict[str, str]:
    """HTTP headers for requests to SEC EDGAR."""
    return {
        "User-Agent": SEC_USER_AGENT,
        "Accept-Encoding": "gzip",
        "Accept": "application/json",
    }
