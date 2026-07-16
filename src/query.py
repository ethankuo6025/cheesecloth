from __future__ import annotations

from db_setup import get_cursor
from models import Fact, Metric

def _ranked_fact_sql(table: str, qname_expr: str, name_col: str, extra_where: str = "") -> str:
    """
    shared shape for both numerical and textual: rank qnames by
    caller-supplied priority, keep the highest-priority qname per filing,
    filter by period type, dedupe, and sort by date. `table` selects which
    table to read from; `qname_expr` is how to compute a fully-qualified
    concept name for that table (numerical has no stored qname -- it's
    always just `taxonomy || ':' || fname`); `name_col` is the bare display
    name column; `extra_where` adds any table-specific predicate (e.g.
    numerical has no `dimensions` column to filter on).
    """
    unit_col = "f.unit" if table == "numerical" else "NULL::varchar AS unit"
    return f"""
WITH
ranked_facts AS (
    SELECT
        f.{name_col} AS local_name,
        f.period_type,
        f.value,
        f.instant_date,
        f.start_date,
        f.end_date,
        {unit_col},
        f.accession_number,
        array_position(%s::text[], {qname_expr}) AS qname_rank
    FROM {table} f
    JOIN companies c ON c.cik = f.cik
    WHERE c.ticker = %s
        AND {qname_expr} = ANY(%s::text[])
        {extra_where}
),
best_qname_per_filing AS (
    SELECT accession_number, MIN(qname_rank) AS best_rank
    FROM ranked_facts
    GROUP BY accession_number
),
filtered_facts AS (
    SELECT rf.local_name, rf.period_type, rf.value,
            rf.instant_date, rf.start_date, rf.end_date,
            rf.unit, rf.accession_number
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
        unit, accession_number
    FROM filtered_facts
    ORDER BY instant_date, start_date, end_date, accession_number
)
SELECT *
FROM deduped
ORDER BY COALESCE(end_date, instant_date, start_date) DESC NULLS LAST
"""

_NUMERICAL_FETCH_SQL = _ranked_fact_sql("numerical", "(f.taxonomy || ':' || f.fname)", "fname")
_TEXTUAL_FETCH_SQL = _ranked_fact_sql("textual", "f.qname", "local_name", "AND f.dimensions = '{}'::jsonb")

def query_facts(
    ticker: str,
    qnames: list[str],
    query_type: str,
    fact_kind: str = "numerical",
) -> list[Fact]:
    """
    fetch facts for a ticker, picking the highest-priority qname per filing,
    filtering by period type, deduplicating, and sorting by date. `fact_kind`
    selects whether to read from numerical or textual.
    """
    sql = _NUMERICAL_FETCH_SQL if fact_kind == "numerical" else _TEXTUAL_FETCH_SQL
    with get_cursor(write=False) as cursor:
        cursor.execute(
            sql,
            (qnames, ticker.upper(), qnames, query_type, query_type, query_type),
        )
        return [Fact(*row) for row in cursor.fetchall()]

def resolve(ticker: str, key: str, query_type: str) -> list[Fact]:
    """resolve a metric to Fact objects using a specific company's configured mappings."""
    metric = get_metric(key)
    if metric is None:
        raise ValueError(f"Unknown metric: {key!r}")

    qnames = get_metric_mappings(ticker, key)
    if not qnames:
        return []

    fact_kind = "textual" if metric.format_type == "text" else "numerical"
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


def _company_concepts_sql(
    table: str, qname_expr: str, name_col: str, has_search: bool, extra_where: str = ""
) -> str:
    """shared per-table aggregate used by get_company_concepts()'s UNION ALL."""
    where = "c.ticker = %s"
    if has_search:
        where += f" AND ({qname_expr} ILIKE %s OR f.{name_col} ILIKE %s)"
    where += extra_where
    return f"""
        SELECT
            {qname_expr} AS qname,
            MIN(f.{name_col}) AS local_name,
            COUNT(*) AS fact_count,
            (ARRAY_AGG(f.value ORDER BY
                COALESCE(f.end_date, f.instant_date, f.start_date) DESC NULLS LAST
            ))[1] AS latest_value
        FROM {table} f
        JOIN companies c ON c.cik = f.cik
        WHERE {where}
        GROUP BY {qname_expr}
    """

def get_company_concepts(ticker: str, search: str | None = None) -> list[tuple]:
    """distinct concepts a company actually reported, for the mapping UI."""
    has_search = bool(search)
    params: list = [ticker.upper()]
    if has_search:
        like = f"%{search}%"
        params.extend([like, like])

    numeric_sql = _company_concepts_sql(
        "numerical", "(f.taxonomy || ':' || f.fname)", "fname", has_search
    )
    textual_sql = _company_concepts_sql(
        "textual", "f.qname", "local_name", has_search, " AND f.dimensions = '{}'::jsonb"
    )
    sql = f"""
        SELECT * FROM (
            {numeric_sql}
            UNION ALL
            {textual_sql}
        ) concepts
        ORDER BY fact_count DESC, qname
    """

    with get_cursor(write=False) as cursor:
        cursor.execute(sql, params + params)
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
