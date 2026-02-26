from __future__ import annotations

import hashlib
import json
import logging

from psycopg import Connection

from dotenv import load_dotenv
import os
from parser import ParsedFact, Filing

logger = logging.getLogger(__name__)

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

CONNINFO = f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD}"


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


def store_facts(
    filings: list[Filing],
    facts: list[ParsedFact],
    batch_size: int = 500,
) -> tuple[int, int]:
    """store facts with deduplication"""
    if not facts:
        return 0, 0

    upserted = failed = 0

    with Connection.connect(CONNINFO) as conn:
        cik = facts[0].cik
        ticker = facts[0].ticker
        filing_params = [(filing.cik, filing.accession_number) for filing in filings]

        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM companies WHERE cik = %s", (cik,))
            company_exists = cur.fetchall()
            if not company_exists:
                cur.execute(
                    """
                    INSERT INTO companies (cik, ticker) VALUES (%s, %s)
                      ON CONFLICT (cik) DO NOTHING
                    """,
                    (cik, ticker),
                )
            # insert parsed filings into database
            cur.executemany(
                "INSERT INTO filings (cik, accession_number) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                filing_params
            )

        for i in range(0, len(facts), batch_size):
            batch = facts[i : i + batch_size]

            with conn.transaction():
                with conn.cursor() as cur:
                    params: list[tuple] = []
                    for fact in batch:
                        try:
                            fact_hash = compute_fact_hash(fact)
                            params.append(
                                (
                                    fact_hash,
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
                                    json.dumps(fact.dimensions, sort_keys=True, separators=(",", ":"))
                                )
                            )
                        except Exception as e:
                            failed += 1
                            logger.info("Error building params for %s: %s", fact.qname, e)

                    if params:
                        try:
                            cur.executemany(
                                """
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
                                """,
                                params,
                            )
                            upserted += len(params)
                        except Exception as e:
                            failed += len(params)
                            logger.info("Batch insert failed (%d rows): %s", len(params), e)

            logger.info("Processed %d/%d", min(i + batch_size, len(facts)), len(facts))

    return upserted, failed
