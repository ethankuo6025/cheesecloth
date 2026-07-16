"""CLI entry point for scraping SEC filings into the database THROUGH ARELLE."""
import argparse
import logging
import sys
from collections.abc import Sequence

from db_setup import get_connection
from parser import SECFilingParser
from models import SECFilingParserError
from store import store_textual_facts
from ticker_loader import TickerLoadError, load_tickers_from_file
from config import DEFAULT_FILING_TYPES
logger = logging.getLogger(__name__)


def _ingest_textual_filing_type(
    parser: SECFilingParser,
    ticker: str,
    filing_types: str | set[str] = "10-K",
    max_filings: int | None = None,
    batch_size: int = 500,
) -> tuple[int, int]:
    """
    parse all un-stored filings of a single requested type for `ticker` and
    persist their facts. reuses `parser.conn` for storage so all writes
    participate in one transactional context.
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
            upserted, failed = store_textual_facts(
                parser.conn, [filing], facts, batch_size=batch_size
            )
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

def ingest_textual_ticker(
    parser: SECFilingParser,
    ticker: str,
    filing_types: Sequence[str],
    max_filings: int | None = None,
) -> tuple[int, int]:
    """
    run the textual (Arelle) ingest for one ticker across every requested
    filing type.
    """
    total_upserted = total_failed = 0
    for ftype in filing_types:
        up, fail = _ingest_textual_filing_type(
            parser,
            ticker=ticker,
            filing_types=ftype,
            max_filings=max_filings,
        )
        total_upserted += up
        total_failed += fail
    return total_upserted, total_failed

def ingest_textual_tickers(
    tickers: Sequence[str],
    filing_types: Sequence[str] = ("10-K", "10-Q"),
    max_retries: int = 3,
    timeout: float = 30.0,
) -> tuple[int, int]:
    """
    run the textual (Arelle) ingest for each ticker in `tickers`.
    opens its own DB connection and parser session. callable directly from
    another program, not just via main()'s CLI.
    """
    total_upserted = total_failed = 0
    with get_connection() as conn:
        with open_parser(conn, max_retries=max_retries, timeout=timeout) as parser:
            for ticker in tickers:
                try:
                    upserted, failed = ingest_textual_ticker(parser, ticker, filing_types)
                    total_upserted += upserted
                    total_failed += failed
                    print(f"[{ticker}] upserted={upserted} failed={failed}")
                except SECFilingParserError:
                    print(f"[{ticker}] not found in SEC EDGAR. Skipping.")
                conn.commit()
    return total_upserted, total_failed

def open_parser(conn, max_retries=3, timeout=30.0) -> SECFilingParser:
    return SECFilingParser(conn, max_retries=max_retries, timeout=timeout)

def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO)

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
        help=f"Filing form types to fetch. Default = {list(DEFAULT_FILING_TYPES)}",
    )
    ap.add_argument(
        "--max-filings",
        type=int,
        default=None,
        help="Cap on filings per ticker+type. Default = none",
    )
    ap.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="HTTP transport retry count. Default = 3",
    )
    ap.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="HTTP timeout in seconds. Default = 30s",
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

    total_upserted, total_failed = ingest_textual_tickers(
        tickers,
        filing_types=tuple(args.filing_types),
        max_retries=args.max_retries,
        timeout=args.timeout,
    )
    print(f"\nDone. Total upserted: {total_upserted}, total failed: {total_failed}")

if __name__ == "__main__":
    sys.exit(main())