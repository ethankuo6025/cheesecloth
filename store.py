from __future__ import annotations

import hashlib
import json
import logging

from psycopg import AsyncConnection

from dotenv import load_dotenv
import os
from parser import ParsedFact

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
    facts: list[ParsedFact],
    batch_size: int = 500,
) -> tuple[int, int]:
    """store facts with deduplication"""
    if not facts:
        return 0, 0

    upserted = failed = 0
    company_cache: dict[str, int] = {}

    async with await AsyncConnection.connect(CONNINFO) as conn:
        tickers = list({f.ticker for f in facts})
        async with conn.cursor() as cur:
            await cur.execute("SELECT ticker, id FROM companies WHERE ticker = ANY(%s)", (tickers,))
            async for row in cur:
                company_cache[row[0]] = row[1]

        for i in range(0, len(facts), batch_size):
            batch = facts[i : i + batch_size]

            async with conn.transaction():
                async with conn.cursor() as cur:
                    missing = {f.ticker: f.cik for f in batch if f.ticker not in company_cache}
                    for tkr, cik in missing.items():
                        await cur.execute(
                            """
                            INSERT INTO companies (ticker, cik) VALUES (%s, %s)
                            ON CONFLICT (ticker) DO UPDATE SET
                                cik = EXCLUDED.cik,
                                updated_at = CURRENT_TIMESTAMP
                            RETURNING id
                            """,
                            (tkr, cik),
                        )
                        row = await cur.fetchone()
                        if row is None:
                            raise RuntimeError(f"Failed to upsert company {tkr}")
                        company_cache[tkr] = row[0]

                    params: list[tuple] = []
                    hashes: list[str] = []
                    for fact in batch:
                        try:
                            fact_hash = compute_fact_hash(fact)
                            hashes.append(fact_hash)
                            params.append(
                                (
                                    fact_hash,
                                    company_cache[fact.ticker],
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
                                    fact_hash, company_id, accession_number, qname, namespace,
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
                                    END,
                                    updated_at = CURRENT_TIMESTAMP
                                """,
                                params,
                            )
                            upserted += len(params)
                        except Exception as e:
                            failed += len(params)
                            logger.info("Batch insert failed (%d rows): %s", len(params), e)

            logger.info("Processed %d/%d", min(i + batch_size, len(facts)), len(facts))

    return upserted, failed
