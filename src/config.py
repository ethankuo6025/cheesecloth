from __future__ import annotations
import os
from typing import Any
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
SEC_USER_AGENT = os.getenv("SEC_USER_AGENT")


def db_kwargs(db_name: str | None = None) -> dict[str, Any]:
    """connection kwargs for psycopg.connect(**db_kwargs())."""
    return {
        "host": DB_HOST,
        "port": DB_PORT,
        "dbname": db_name or DB_NAME,
        "user": DB_USER,
        "password": DB_PASSWORD,
    }


# https://www.sec.gov/about/webmaster-frequently-asked-questions#developers
def sec_headers() -> dict[str, str]:
    """HTTP headers for requests to SEC EDGAR."""
    return {
        "User-Agent": SEC_USER_AGENT,
        "Accept-Encoding": "gzip",
        "Accept": "application/json",
    }


# ── Arelle ───────────────────────────────────────────────────────────
ARELLE_PLUGINS_PATH = os.getenv("ARELLE_PLUGINS_PATH", "")
