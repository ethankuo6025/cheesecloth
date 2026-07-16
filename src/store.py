"""stores parsed facts to the database"""
import hashlib
import json
import logging
from psycopg import Connection
from models import Filing, TextualFact, NumericalFact

logger = logging.getLogger(__name__)

def compute_textual_fact_hash(f: TextualFact) -> str:
    """unique identity for deduplication"""
    dims = json.dumps(f.dimensions, sort_keys=True, separators=(",", ":"))

    # excludes value
    data = (
        f"{f.cik}|"
        f"{f.qname}|{f.local_name}|"
        f"{f.period_type.value}|"
        f"{f.instant_date}|{f.start_date}|{f.end_date}|"
        f"{dims}"
    )
    return hashlib.sha256(data.encode("utf-8")).hexdigest()[:64]

def _build_textual_fact_params(facts: list[TextualFact]) -> tuple[list[tuple], int]:
    """
    serialise facts into executemany param tuples.

    returns (params, failed) where `failed` is the count of facts that could
    not be serialised. each failure is logged.
    """
    params: list[tuple] = []
    failed = 0
    for fact in facts:
        try:
            params.append(
                (
                    compute_textual_fact_hash(fact),
                    fact.cik,
                    fact.accession_number,
                    fact.qname,
                    fact.namespace,
                    fact.local_name,
                    fact.period_type.value,
                    fact.value,
                    fact.instant_date,
                    fact.start_date,
                    fact.end_date,
                    json.dumps(fact.dimensions, sort_keys=True, separators=(",", ":")),
                )
            )
        except Exception:
            failed += 1
            logger.warning(
                "Could not serialise fact qname=%s accession=%s — skipping",
                getattr(fact, "qname", "?"),
                getattr(fact, "accession_number", "?"),
                exc_info=True,
            )
    return params, failed

_TEXTUAL_UPSERT_SQL = """
INSERT INTO textual (
  fact_hash, cik, accession_number, qname, namespace,
  local_name, period_type, value, instant_date, start_date,
  end_date, dimensions
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (fact_hash) DO UPDATE SET
  accession_number = CASE
    WHEN EXCLUDED.accession_number > textual.accession_number
      THEN EXCLUDED.accession_number
    ELSE textual.accession_number
  END,
  value = CASE
    WHEN EXCLUDED.accession_number > textual.accession_number
      THEN EXCLUDED.value
    ELSE textual.value
  END
"""

def store_textual_facts(
    conn: Connection,
    filings: list[Filing],
    facts: list[TextualFact],
    batch_size: int = 500,
) -> tuple[int, int]:
    """
    upsert textual facts (and their parent filings) using the caller's connection.
    """
    if not facts:
        return 0, 0

    upserted = failed = 0
    cik = facts[0].cik
    ticker = facts[0].ticker
    filing_params = [(filing.cik, filing.accession_number) for filing in filings]

    # Ensure the company + filings rows exist before we add facts pointing at them.
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM companies WHERE cik = %s", (cik,))
        if not cur.fetchall():
            cur.execute(
                """
                INSERT INTO companies (cik, ticker) VALUES (%s, %s)
                  ON CONFLICT (cik) DO NOTHING
                """,
                (cik, ticker),
            )
        cur.executemany(
            "INSERT INTO filings (cik, accession_number) VALUES (%s, %s) "
            "ON CONFLICT DO NOTHING",
            filing_params,
        )

    for i in range(0, len(facts), batch_size):
        batch = facts[i : i + batch_size]
        params, batch_failed = _build_textual_fact_params(batch)
        failed += batch_failed
        if not params:
            continue

        try:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.executemany(_TEXTUAL_UPSERT_SQL, params)
            upserted += len(params)
        except Exception:
            failed += len(params)
            logger.error(
                "Failed to upsert batch of %d textual fact(s) (offset %d) — skipping",
                len(params), i,
                exc_info=True,
            )
    return upserted, failed

def compute_numerical_fact_hash(f: NumericalFact) -> str:
    """
    unique identity for deduplication.
    """
    data = (
        f"{f.cik}|{f.taxonomy}:{f.fname}|{f.unit}|"
        f"{f.period_type.value}|"
        f"{f.instant_date}|{f.start_date}|{f.end_date}"
    )
    return hashlib.sha256(data.encode("utf-8")).hexdigest()[:64]

def _build_numerical_fact_params(facts: list[NumericalFact]) -> tuple[list[tuple], int]:
    """
    serialise company facts into executemany param tuples.
    """
    params: list[tuple] = []
    failed = 0
    for fact in facts:
        try:
            params.append(
                (
                    compute_numerical_fact_hash(fact),
                    fact.cik,
                    fact.accession_number,
                    fact.taxonomy,
                    fact.fname,
                    fact.unit,
                    fact.value,
                    fact.period_type.value,
                    fact.instant_date,
                    fact.start_date,
                    fact.end_date,
                    fact.fiscal_year,
                    fact.fiscal_period,
                    fact.form,
                    fact.filed_date,
                )
            )
        except Exception:
            failed += 1
            logger.warning(
                "Could not serialise company fact taxonomy=%s fname=%s accession=%s — skipping",
                getattr(fact, "taxonomy", "?"),
                getattr(fact, "fname", "?"),
                getattr(fact, "accession_number", "?"),
                exc_info=True,
            )
    return params, failed

# NULLs sort as '-infinity' so a present filed_date always beats a NULL
PREFER_EXCLUDED = """(
    COALESCE(EXCLUDED.filed_date, '-infinity'::date) > COALESCE(numerical.filed_date, '-infinity'::date)
    OR (
      COALESCE(EXCLUDED.filed_date, '-infinity'::date) = COALESCE(numerical.filed_date, '-infinity'::date)
      AND EXCLUDED.accession_number > numerical.accession_number
    )
  )"""

_NUMERICAL_LATEST_WINS_COLUMNS = (
    "accession_number", "value", "fiscal_year", "fiscal_period", "form", "filed_date",
)

_numerical_set_clause = ",\n  ".join(
    f"{col} = CASE WHEN {PREFER_EXCLUDED} THEN EXCLUDED.{col} ELSE numerical.{col} END"
    for col in _NUMERICAL_LATEST_WINS_COLUMNS
)

_NUMERICAL_UPSERT_SQL = f"""
INSERT INTO numerical (
  fact_hash, cik, accession_number, taxonomy, fname,
  unit, value, period_type, instant_date, start_date,
  end_date, fiscal_year, fiscal_period, form, filed_date
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (fact_hash) DO UPDATE SET
  {_numerical_set_clause}
"""

def store_numerical_facts(
    conn: Connection,
    facts: list[NumericalFact],
    batch_size: int = 500,
) -> tuple[int, int]:
    """
    upsert numeric (company facts) rows and their parent filings using the
    caller's connection.

    returns `(upserted, failed)`. per-fact serialisation failures and per-batch
    DB errors are logged; the function does not raise.
    """
    if not facts:
        return 0, 0

    upserted = failed = 0
    cik = facts[0].cik
    ticker = facts[0].ticker
    filing_params = sorted({(f.cik, f.accession_number) for f in facts})

    # Ensure the company + filings rows exist before we add facts pointing at them.
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM companies WHERE cik = %s", (cik,))
        if not cur.fetchall():
            cur.execute(
                """
                INSERT INTO companies (cik, ticker) VALUES (%s, %s)
                  ON CONFLICT (cik) DO NOTHING
                """,
                (cik, ticker),
            )
        cur.executemany(
            "INSERT INTO filings (cik, accession_number) VALUES (%s, %s) "
            "ON CONFLICT DO NOTHING",
            filing_params,
        )

    for i in range(0, len(facts), batch_size):
        batch = facts[i : i + batch_size]
        params, batch_failed = _build_numerical_fact_params(batch)
        failed += batch_failed
        if not params:
            continue

        try:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.executemany(_NUMERICAL_UPSERT_SQL, params)
            upserted += len(params)
        except Exception:
            failed += len(params)
            logger.error(
                "Failed to upsert batch of %d numeric fact(s) (offset %d) — skipping",
                len(params), i,
                exc_info=True,
            )
    return upserted, failed
