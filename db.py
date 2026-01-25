import psycopg
from psycopg import Error
from contextlib import contextmanager
import os

try:
    from private import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
except ImportError as e:
    print(f"private.py not found or setup incorrectly: {e}")
    exit(1)


def get_connection(db_name=DB_NAME):
    try:
        conn = psycopg.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=db_name,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        return conn
    except Error as e:
        return None


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


def test_connection(db_name=DB_NAME):
    conn = get_connection(db_name)
    if conn:
        conn.close()
        return True
    return False


def create_database():
    """Create the database if it doesn't exist. Returns True if exists or created."""
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
            cursor.execute(f'CREATE DATABASE "{DB_NAME}"')
        return(0, SUCCESSFUL)
    except Error as e:
        return(1, f"Error creating database: {e}")
    finally:
        if conn:
            conn.close()


def init_schema():
    """Initialize/update the database schema from ddl.sql."""
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
                cursor.execute(ddl_sql)
            return(0, SUCCESSFUL)
        finally:
            conn.close()
            
    except Error as e:
        return(1, f"Error initializing cheesecloth schema: {e}")

def setup_database():
    """Full database setup: create database and initialize/update schema."""
    code, msg = create_database()
    if code == 0:
        print(msg)
        code, msg = init_schema()
    print(msg)
    return code

if __name__ == "__main__":
    code = setup_database()
    print(f"exit code: {code}")