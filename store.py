from __future__ import annotations

import hashlib
import json
import logging

from psycopg import AsyncConnection

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
        f"{f.cik}|{f.accession_number}|"
        f"{f.namespace}|{f.qname}|{f.local_name}|"
        f"{f.period_type.value}|{f.unit}|"
        f"{f.instant_date}|{f.start_date}|{f.end_date}|"
        f"{dims}"
    )
    return hashlib.sha256(data.encode("utf-8")).hexdigest()[:64]


def resolve_keep_higher(stored: int | None, incoming: int | None) -> int | None:
    """mirrors the CASE logic in the ON CONFLICT for decimals/precision."""
    if incoming is None:
        return stored
    if stored is None:
        return incoming
    return incoming if incoming > stored else stored


def would_update(stored: tuple, incoming: tuple) -> bool:
    """return True if the ON CONFLICT SET would actually change any column."""
    s_val, s_dec, s_prec = stored
    i_val, i_dec, i_prec = incoming
    if s_val != i_val:
        return True
    if resolve_keep_higher(s_dec, i_dec) != s_dec:
        return True
    if resolve_keep_higher(s_prec, i_prec) != s_prec:
        return True
    return False


async def store_facts(
    filings: list[Filing],
    facts: list[ParsedFact],
    batch_size: int = 500,
) -> tuple[int, int]:
    """store facts with deduplication"""
    if not facts:
        return 0, 0

    upserted = failed = 0

    async with await AsyncConnection.connect(CONNINFO) as conn:
        cik = facts[0].cik
        ticker = facts[0].ticker
        filing_params = [(filing.cik, filing.accession_number) for filing in filings]
        
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1 FROM companies WHERE cik = %s", (cik,))
            company_exists = await cur.fetchall()
            if not company_exists:
                await cur.execute(
                    """
                    INSERT INTO companies (cik, ticker) VALUES (%s, %s)
                        ON CONFLICT (cik) DO NOTHING
                    """,
                    (cik, ticker),
                )
            # insert parsed filings into database
            await cur.executemany(
                "INSERT INTO filings (cik, accession_number) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                filing_params
            )
                
        for i in range(0, len(facts), batch_size):
            batch = facts[i : i + batch_size]

            async with conn.transaction():
                async with conn.cursor() as cur:
                    params: list[tuple] = []
                    hashes: list[str] = []
                    for fact in batch:
                        try:
                            fact_hash = compute_fact_hash(fact)
                            hashes.append(fact_hash)
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
                                    fact.precision,
                                    json.dumps(fact.dimensions, sort_keys=True, separators=(",", ":"))
                                )
                            )
                        except Exception as e:
                            failed += 1
                            logger.info("Error inserting %s: %s", fact.qname, e)

                    if params:
                        try:
                            await cur.execute(
                                "SELECT fact_hash, value, decimals, precision FROM facts WHERE fact_hash = ANY(%s)", 
                                (hashes,)
                            )
                            # build lookup of hash -> (value, decimals, precision)
                            existing: dict[str, tuple] = {}
                            async for row in cur: # cur is async so for loop needs to be too
                                existing[row[0]] = (row[1], row[2], row[3])

                            # drop anything already in the db that wouldn't actually change
                            params = [
                                p for p in params
                                if p[0] not in existing
                                or would_update(existing[p[0]], (p[7], p[12], p[13]))
                            ] # use values, decimals, and precision

                            await cur.executemany(
                                """
                                INSERT INTO facts (
                                    fact_hash, cik, accession_number, qname, namespace,
                                    local_name, period_type, value, instant_date, start_date,
                                    end_date, unit, decimals, precision, dimensions
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (fact_hash) DO UPDATE SET
                                    value = EXCLUDED.value,
                                    decimals = CASE
                                        WHEN EXCLUDED.decimals IS NULL THEN facts.decimals
                                        WHEN facts.decimals IS NULL THEN EXCLUDED.decimals
                                        WHEN EXCLUDED.decimals > facts.decimals THEN EXCLUDED.decimals
                                        ELSE facts.decimals
                                    END,
                                    precision = CASE
                                        WHEN EXCLUDED.precision IS NULL THEN facts.precision
                                        WHEN facts.precision IS NULL THEN EXCLUDED.precision
                                        WHEN EXCLUDED.precision > facts.precision THEN EXCLUDED.precision
                                        ELSE facts.precision
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
