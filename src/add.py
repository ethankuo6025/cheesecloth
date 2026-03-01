from __future__ import annotations

import argparse
import logging
import os
import sys
from collections.abc import Sequence

import psycopg
from dotenv import load_dotenv

from parser import SECFilingParser, TickerNotFoundError
from store import store_facts
from ticket_loader import TickerLoadError, load_tickers_from_file

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
CONNINFO = (
    f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} "
    f"user={DB_USER} password={DB_PASSWORD}"
)

DEFAULT_FILING_TYPES = ("10-K", "10-Q")

logger = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO)

def parse_and_store(
    parser: SECFilingParser,
    ticker: str,
    filing_types: str | set[str] = "10-K",
    max_filings: int | None = None,
    batch_size: int = 500,
) -> tuple[int, int]:
    """
    Parse all un-stored filings of the requested type(s) for `ticker`
    and persist their facts.

    Returns:
        (total_upserted, total_failed)
    """
    cik, filings_to_parse = parser.get_filings_to_parse(
        ticker,
        filing_types,
        max_filings,
    )

    if not filings_to_parse:
        return 0, 0

    total_upserted = total_failed = 0
    ticker_upper = ticker.upper()

    for i, filing in enumerate(filings_to_parse, start=1):
        logger.info(
            " Processing filing %d/%d: %s",
            i, len(filings_to_parse), filing.accession_number,
        )

        try:
            facts = parser.parse_filing(filing, ticker_upper, cik)
            upserted, failed = store_facts([filing], facts, batch_size=batch_size)
            total_upserted += upserted
            total_failed += failed
            logger.info(
                " Filing %s: %d facts upserted, %d failed",
                filing.accession_number, upserted, failed,
            )
        except Exception as e:
            logger.error(
                " Failed to process filing %s: %s",
                filing.accession_number, e, exc_info=True,
            )
            total_failed += 1
            continue

    return total_upserted, total_failed

def process_ticker(
    conn: psycopg.Connection,
    ticker: str,
    filing_types: Sequence[str],
    max_filings: int | None,
    max_retries: int,
    timeout: float,
) -> tuple[int, int]:
    """
    Run the full pipeline for one ticker across every requested filing type.

    Returns:
        (upserted, failed) aggregated across all filing types.
    """
    total_upserted = total_failed = 0
    for ftype in filing_types:
        with SECFilingParser(conn, max_retries=max_retries, timeout=timeout) as sec_parser:
            up, fail = parse_and_store(
                sec_parser,
                ticker=ticker,
                filing_types=ftype,
                max_filings=max_filings,
            )
            total_upserted += up
            total_failed += fail
    return total_upserted, total_failed

def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "tickers",
        nargs="*",
        help="Ticker symbols passed inline.",
    )
    ap.add_argument(
        "-f", "--file",
        action="append",
        dest="files",
        metavar="PATH",
        default=[],
        help="Path to a text file with one ticker per line. May be repeated.",
    )
    ap.add_argument(
        "--filing-types",
        nargs="+",
        default=list(DEFAULT_FILING_TYPES),
        help=f"Filing form types to fetch. Default: {list(DEFAULT_FILING_TYPES)}",
    )
    ap.add_argument(
        "--max-filings",
        type=int,
        default=None,
        help="Cap on filings per ticker+type. Default: no cap.",
    )
    ap.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="HTTP transport retry count. Default: 3.",
    )
    ap.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="HTTP timeout in seconds. Default: 30.",
    )
    args = ap.parse_args(argv)
    # resolve ticker set from all sources.
    try:
        # combine inline tickers with tickers loaded from zero or more files
        results: list[str] = []

        if args.tickers:
            results.extend(t.strip().upper() for t in args.tickers if t)

        if args.files:
            for fp in args.files:
                results.extend(load_tickers_from_file(fp))

        tickers = list(set(results))
    except TickerLoadError as e:
        ap.error(str(e))  # exits with status 2

    if not tickers:
        ap.error("No tickers supplied. Pass symbols inline or use --file.")

    logger.info(" Processing %d ticker(s): %s", len(tickers), ", ".join(tickers))

    total_upserted = 0
    total_failed = 0
    with psycopg.connect(CONNINFO) as conn:
        for ticker in tickers:
            try:
                upserted, failed = process_ticker(
                    conn,
                    ticker=ticker,
                    filing_types=args.filing_types,
                    max_filings=args.max_filings,
                    max_retries=args.max_retries,
                    timeout=args.timeout,
                )
                total_upserted += upserted
                total_failed += failed
                print(f"[{ticker}] upserted={upserted} failed={failed}")
            except TickerNotFoundError:
                print(f"[{ticker}] not found in SEC EDGAR — skipping.")

    print(f"\nDone. Total upserted: {total_upserted}, total failed: {total_failed}")
    return 0 if total_failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())