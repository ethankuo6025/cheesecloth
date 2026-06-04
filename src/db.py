from __future__ import annotations

import logging
import os
import sys
from contextlib import contextmanager
from typing import cast

import psycopg
from psycopg import Error, sql
from psycopg.abc import Query

import config
from models import Fact

logger = logging.getLogger(__name__)

@contextmanager
def get_cursor(write: bool = True, db_name: str | None = None):
    conn = None
    cursor = None
    try:
        conn = get_connection(db_name)
        cursor = conn.cursor()
        yield cursor
        if write:
            conn.commit()
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_connection(db_name: str | None = None) -> psycopg.Connection:
    try:
        return psycopg.connect(**config.db_kwargs(db_name))
    except Error as e:
        logger.error(
            "Connection error (db=%s, host=%s, port=%s, user=%s): %s",
            db_name or config.DB_NAME, config.DB_HOST, config.DB_PORT, config.DB_USER, e,
        )
        raise

def create_database(db_name: str) -> tuple[int, str]:
    """create the database if it doesn't exist. returns (code, message)."""
    ALREADY_EXISTS = "cheesecloth database already exists."
    SUCCESSFUL = "cheesecloth database has been setup successfully."
    conn = None
    try:
        conn = get_connection("postgres")
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (db_name,),
            )
            if cursor.fetchone():
                return (-1, ALREADY_EXISTS)
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
        return (0, SUCCESSFUL)
    except Error as e:
        return (1, f"Error creating database: {e}")
    finally:
        if conn:
            conn.close()

def init_schema() -> tuple[int, str]:
    """initialize/update the database schema from ddl.sql."""
    SUCCESSFUL = "initializing the schema completed successfully."

    ddl_path = os.path.join(os.path.dirname(__file__), "ddl.sql")
    if not os.path.exists(ddl_path):
        return (1, f"ddl.sql not found at {ddl_path}")

    try:
        with open(ddl_path, "r") as f:
            ddl_sql = f.read()

        conn = get_connection()
        try:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute(cast(Query, ddl_sql))
            return (0, SUCCESSFUL)
        finally:
            conn.close()
    except Error as e:
        return (1, f"Error initializing cheesecloth schema: {e}")

def setup_database(db_name: str) -> int:
    """
    full database setup: create the database if needed, then initialize/update
    the schema and seed the metric catalog.
    """
    code, msg = create_database(db_name)
    print(msg)
    if code == 1:  # could not create or connect — nothing more to do
        return code

    code, msg = init_schema()
    print(msg)
    if code != 0:
        return code

    from seed_metrics import seed_metrics
    try:
        print(f"Seeded {seed_metrics()} metric(s) into the catalog.")
    except Error as e:
        print(f"Error seeding metrics: {e}")

    return code

def reset_database(db_name: str) -> bool:
    """drops ALL tables and recreate. warning: deletes all data."""
    try:
        conn = get_connection("postgres")
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(db_name))
            )
        conn.close()
        print("All tables dropped.")
        return setup_database(db_name) == 0
    except Error as e:
        logger.error("Error resetting database: %s", e)
        return False

def get_available_tickers() -> list[tuple]:
    """return [(ticker, updated_at), ...] sorted alphabetically."""
    with get_cursor(write=False) as cursor:
        cursor.execute("SELECT ticker, updated_at FROM companies ORDER BY ticker")
        return cursor.fetchall()

def query_facts(
    ticker: str,
    qnames: list[str],
    query_type: str,
    fact_kind: str = "numeric",
) -> list[tuple]:
    """
    fetch facts for a ticker, picking the highest-priority qname per filing,
    filtering by period type and fact kind, deduplicating, and sorting by date.
    """
    with get_cursor(write=False) as cursor:
        cursor.execute(
            """
            WITH
            ranked_facts AS (
                SELECT
                    f.local_name,
                    f.period_type,
                    f.value,
                    f.instant_date,
                    f.start_date,
                    f.end_date,
                    f.unit,
                    f.decimals,
                    f.accession_number,
                    array_position(%s::text[], f.qname) AS qname_rank
                FROM facts f
                JOIN companies c ON c.cik = f.cik
                WHERE c.ticker = %s
                  AND f.qname = ANY(%s::text[])
                  AND f.dimensions = '{}'::jsonb
                  AND (
                      %s = 'all'
                      OR (%s = 'numeric' AND f.unit IS NOT NULL)
                      OR (%s = 'textual'  AND f.unit IS NULL)
                  )
            ),
            best_qname_per_filing AS (
                SELECT accession_number, MIN(qname_rank) AS best_rank
                FROM ranked_facts
                GROUP BY accession_number
            ),
            filtered_facts AS (
                SELECT rf.local_name, rf.period_type, rf.value,
                       rf.instant_date, rf.start_date, rf.end_date,
                       rf.unit, rf.decimals, rf.accession_number
                FROM ranked_facts rf
                JOIN best_qname_per_filing bq
                    ON rf.accession_number = bq.accession_number
                   AND rf.qname_rank = bq.best_rank
                WHERE
                    rf.instant_date IS NOT NULL
                    OR (rf.start_date IS NOT NULL AND rf.end_date IS NOT NULL AND (
                        %s = 'all'
                        OR (%s = 'annual'    AND (rf.end_date - rf.start_date) > 350)
                        OR (%s = 'quarterly' AND (rf.end_date - rf.start_date) < 100)
                    ))
                    OR (rf.instant_date IS NULL AND rf.start_date IS NULL AND rf.end_date IS NULL)
            ),
            deduped AS (
                SELECT DISTINCT ON (instant_date, start_date, end_date)
                    local_name, period_type, value,
                    instant_date, start_date, end_date,
                    unit, decimals, accession_number
                FROM filtered_facts
                ORDER BY instant_date, start_date, end_date, accession_number
            )
            SELECT *
            FROM deduped
            ORDER BY COALESCE(end_date, instant_date, start_date) DESC NULLS LAST
            """,
            (
                qnames, ticker.upper(), qnames,
                fact_kind, fact_kind, fact_kind,
                query_type, query_type, query_type,
            ),
        )
        return [Fact(*row) for row in cursor.fetchall()]

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv) > 1 and sys.argv[1] == "--reset":
        confirm = input("This will DELETE ALL DATA. Type 'yes I understand' to confirm: ")
        if confirm == "yes I understand":
            if create_database(config.DB_NAME):
                reset_database(config.DB_NAME)
        else:
            print("Incorrect response: operation cancelled.")
    else:
        setup_database(config.DB_NAME)
