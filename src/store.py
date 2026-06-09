"""stores parsed facts to the database"""
import hashlib
import json
import logging
from psycopg import Connection
from parser import Filing, ParsedFact

logger = logging.getLogger(__name__)

def compute_fact_hash(f: ParsedFact) -> str:
    """unique identity for deduplication"""
    dims = json.dumps(f.dimensions, sort_keys=True, separators=(",", ":"))

    # excludes value/decimals/precision
    data = (
        f"{f.cik}|"
        f"{f.qname}|{f.local_name}|"
        f"{f.period_type.value}|{f.unit}|"
        f"{f.instant_date}|{f.start_date}|{f.end_date}|"
        f"{dims}"
    )
    return hashlib.sha256(data.encode("utf-8")).hexdigest()[:64]

def _build_fact_params(facts: list[ParsedFact]) -> tuple[list[tuple], int]:
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
                    compute_fact_hash(fact),
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
                    fact.unit,
                    fact.decimals,
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

_UPSERT_SQL = """
INSERT INTO facts (
  fact_hash, cik, accession_number, qname, namespace,
  local_name, period_type, value, instant_date, start_date,
  end_date, unit, decimals, dimensions
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (fact_hash) DO UPDATE SET
  accession_number = CASE
    WHEN facts.decimals IS NOT NULL
      AND (EXCLUDED.decimals IS NULL
        OR facts.decimals > EXCLUDED.decimals)
      THEN facts.accession_number
    WHEN EXCLUDED.decimals IS NOT NULL
      AND (facts.decimals IS NULL
        OR EXCLUDED.decimals > facts.decimals)
      THEN EXCLUDED.accession_number
    WHEN EXCLUDED.accession_number > facts.accession_number
      THEN EXCLUDED.accession_number
    ELSE facts.accession_number
  END,
  value = CASE
    WHEN facts.decimals IS NOT NULL
      AND (EXCLUDED.decimals IS NULL
        OR facts.decimals > EXCLUDED.decimals)
      THEN facts.value
    WHEN EXCLUDED.decimals IS NOT NULL
      AND (facts.decimals IS NULL
        OR EXCLUDED.decimals > facts.decimals)
      THEN EXCLUDED.value
    WHEN EXCLUDED.accession_number > facts.accession_number
      THEN EXCLUDED.value
    ELSE facts.value
  END,
  decimals = CASE
    WHEN EXCLUDED.decimals IS NULL THEN facts.decimals
    WHEN facts.decimals IS NULL THEN EXCLUDED.decimals
    WHEN EXCLUDED.decimals > facts.decimals THEN EXCLUDED.decimals
    ELSE facts.decimals
  END
"""

def store_facts(
    conn: Connection,
    filings: list[Filing],
    facts: list[ParsedFact],
    batch_size: int = 500,
) -> tuple[int, int]:
    """
    upsert facts and their parent filings using the caller's connection.

    returns `(upserted, failed)`. per-fact serialisation failures and per-batch
    DB errors are logged; the function does not raise.
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
        params, batch_failed = _build_fact_params(batch)
        failed += batch_failed
        if not params:
            continue

        with conn.transaction():
            with conn.cursor() as cur:
                try:
                    cur.executemany(_UPSERT_SQL, params)
                    upserted += len(params)
                except Exception:
                    failed += len(params)
                    logger.error(
                        "Failed to upsert batch of %d fact(s) (offset %d) — skipping",
                        len(params), i,
                        exc_info=True,
                    )
    return upserted, failed
