import sys
import os
import psycopg
from psycopg import Error, sql
from typing import cast
from psycopg.abc import Query
from contextlib import contextmanager
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

@contextmanager
def get_cursor(write=True, db_name=DB_NAME):
    conn = None
    cursor = None
    try:
        conn = get_connection(db_name)
        if conn is None:
            raise Exception("Failed to connect to database")
        cursor = conn.cursor()
        yield cursor
        if write:
            conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            
def get_connection(db_name=DB_NAME):
    try:
        conn = psycopg.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=db_name,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        return conn
    except Error as e:
        print(f"Connection error (db={db_name}, host={DB_HOST}, port={DB_PORT}, user={DB_USER}): {e}")
        raise

def create_database(db_name):
    """create the database if it doesn't exist. Returns True if exists or created."""
    ALREADY_EXISTS = "cheesecloth database already exists."
    SUCCESSFUL = "cheesecloth database has been setup successfully."
    conn = None
    try:
        conn = get_connection("postgres")
        if conn is None:
            return (1, "Error creating database: could not connect to postgres database")
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (DB_NAME,)
            )
            if cursor.fetchone():
                return(-1, ALREADY_EXISTS)
            cursor.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(db_name)))
        return(0, SUCCESSFUL)
    except Error as e:
        return(1, f"Error creating database: {e}")
    finally:
        if conn:
            conn.close()


def init_schema():
    """initialize/update the database schema from ddl.sql."""
    SUCCESSFUL = "initializing the schema completed successfully."

    ddl_path = os.path.join(os.path.dirname(__file__), "ddl.sql")
    
    if not os.path.exists(ddl_path):
        return(1, f"ddl.sql not found at {ddl_path}")
    
    try:
        with open(ddl_path, "r") as f:
            ddl_sql = f.read()
        
        conn = get_connection()
        if conn is None:
            return(1, "Could not connect to the database.")
        
        try:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute(cast(Query, ddl_sql))
            return(0, SUCCESSFUL)
        finally:
            conn.close()
            
    except Error as e:
        return(1, f"Error initializing cheesecloth schema: {e}")

def setup_database(db_name):
    """full database setup: create database and initialize/update schema."""
    code, msg = create_database(db_name)
    if code == 0:
        print(msg)
        code, msg = init_schema()
    print(msg)
    return code

def reset_database(db_name):
    """drops ALL tables and recreate. WARNING: Deletes all data!"""
    try:
        conn = get_connection("postgres")
        if conn is None:
            return False
        
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(db_name)))
        conn.close()
        print("All tables dropped.")
        return setup_database(db_name)
    except Error as e:
        print(f"Error resetting database: {e}")
        return False


def get_available_tickers() -> list[tuple]:
    """Return [(ticker, updated_at), ...] sorted alphabetically."""
    with get_cursor(write=False) as cursor:
        cursor.execute(
            "SELECT ticker, updated_at FROM companies ORDER BY ticker"
        )
        return cursor.fetchall()

def get_facts(ticker: str, qname: str) -> list[tuple]:
    """Fetch facts for a ticker and qname"""
    with get_cursor(write=False) as cursor:
        cursor.execute(
            """
            SELECT
                f.local_name,
                f.period_type,
                f.value,
                f.instant_date,
                f.start_date,
                f.end_date,
                f.unit,
                f.accession_number
            FROM facts f
            JOIN companies c ON c.cik = f.cik
            WHERE c.ticker = %s
              AND f.qname LIKE %s
              AND f.dimensions = '{}'::jsonb
            ORDER BY
                COALESCE(f.end_date, f.instant_date) DESC NULLS LAST
            """,
            (ticker.upper(), qname),
        )
        return cursor.fetchall()

if __name__ == "__main__":    
    if len(sys.argv) > 1 and sys.argv[1] == "--reset":
        confirm = input("This will DELETE ALL DATA. Type 'yes I understand' to confirm: ")
        if confirm == 'yes I understand':
            if create_database(DB_NAME):
                reset_database(DB_NAME)
        else:
            print("Incorrect response: operation cancelled.")
    else:
        setup_database(DB_NAME)