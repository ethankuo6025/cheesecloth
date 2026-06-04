from __future__ import annotations

from db import get_cursor, query_facts
from models import Fact, Metric


def resolve(ticker: str, key: str, query_type: str) -> list[Fact]:
    """resolve a metric to `Fact`s using a specific company's configured mappings."""
    metric = get_metric(key)
    if metric is None:
        raise ValueError(f"Unknown metric: {key!r}")

    qnames = get_metric_mappings(ticker, key)
    if not qnames:
        return []

    fact_kind = "textual" if metric.format_type == "text" else "numeric"
    return query_facts(ticker, qnames, query_type, fact_kind=fact_kind)


def get_cik_for_ticker(ticker: str) -> str | None:
    """return the CIK for a ticker, or None if it isn't in the database."""
    with get_cursor(write=False) as cursor:
        cursor.execute(
            "SELECT cik FROM companies WHERE ticker = %s", (ticker.upper(),)
        )
        row = cursor.fetchone()
        return row[0] if row else None


def get_metrics() -> list[Metric]:
    """return the full metric catalog, ordered alphabetically by key."""
    with get_cursor(write=False) as cursor:
        cursor.execute(
            "SELECT key, display_name, format_type FROM metrics ORDER BY key"
        )
        return [Metric(*row) for row in cursor.fetchall()]


def get_metric(key: str) -> Metric | None:
    """return a single catalog metric by key, or None if unknown."""
    with get_cursor(write=False) as cursor:
        cursor.execute(
            "SELECT key, display_name, format_type FROM metrics WHERE key = %s",
            (key,),
        )
        row = cursor.fetchone()
        return Metric(*row) if row else None


def add_metric(
    key: str,
    display_name: str,
    format_type: str = "text",
) -> None:
    """insert a new catalog metric (no-op if the key already exists)."""
    with get_cursor() as cursor:
        cursor.execute(
            "INSERT INTO metrics (key, display_name, format_type) "
            "VALUES (%s, %s, %s) ON CONFLICT (key) DO NOTHING",
            (key, display_name, format_type),
        )


def get_metric_mappings(ticker: str, metric_key: str) -> list[str]:
    """return this company's qnames for `metric_key`, in priority order."""
    with get_cursor(write=False) as cursor:
        cursor.execute(
            """
            SELECT mm.qname
            FROM metric_mappings mm
            JOIN companies c ON c.cik = mm.cik
            WHERE c.ticker = %s AND mm.metric_key = %s
            ORDER BY mm.priority, mm.qname
            """,
            (ticker.upper(), metric_key),
        )
        return [row[0] for row in cursor.fetchall()]


def get_company_concepts(ticker: str, search: str | None = None) -> list[tuple]:
    """distinct concepts a company actually reported, for the mapping UI."""
    clauses = ["c.ticker = %s", "f.dimensions = '{}'::jsonb"]
    params: list = [ticker.upper()]
    if search:
        clauses.append("(f.qname ILIKE %s OR f.local_name ILIKE %s)")
        like = f"%{search}%"
        params.extend([like, like])

    with get_cursor(write=False) as cursor:
        cursor.execute(
            f"""
            SELECT
                f.qname,
                MIN(f.local_name) AS local_name,
                COUNT(*) AS fact_count,
                (ARRAY_AGG(f.value ORDER BY
                    COALESCE(f.end_date, f.instant_date, f.start_date) DESC NULLS LAST
                ))[1] AS latest_value
            FROM facts f
            JOIN companies c ON c.cik = f.cik
            WHERE {" AND ".join(clauses)}
            GROUP BY f.qname
            ORDER BY fact_count DESC, f.qname
            """,
            params,
        )
        return cursor.fetchall()


def get_mappings_for_ticker(ticker: str) -> list[tuple]:
    """
    existing mappings for a ticker, joined to catalog display names.
    returns [(metric_key, display_name, qname, priority), ...].
    """
    with get_cursor(write=False) as cursor:
        cursor.execute(
            """
            SELECT mm.metric_key, m.display_name, mm.qname, mm.priority
            FROM metric_mappings mm
            JOIN companies c ON c.cik = mm.cik
            JOIN metrics m   ON m.key = mm.metric_key
            WHERE c.ticker = %s
            ORDER BY mm.metric_key, mm.priority, mm.qname
            """,
            (ticker.upper(),),
        )
        return cursor.fetchall()


def add_metric_mapping(cik: str, metric_key: str, qname: str, priority: int = 0) -> None:
    """map a company's qname onto a catalog metric (upserts the priority)."""
    with get_cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO metric_mappings (cik, metric_key, qname, priority)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (cik, metric_key, qname) DO UPDATE SET priority = EXCLUDED.priority
            """,
            (cik, metric_key, qname, priority),
        )


def remove_metric_mapping(cik: str, metric_key: str, qname: str) -> bool:
    """remove a single mapping. returns True if a row was deleted."""
    with get_cursor() as cursor:
        cursor.execute(
            "DELETE FROM metric_mappings WHERE cik = %s AND metric_key = %s AND qname = %s",
            (cik, metric_key, qname),
        )
        return cursor.rowcount > 0
