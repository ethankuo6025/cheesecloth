"""parser for quantitative facts"""
import argparse
import logging
import sys
from collections.abc import Sequence

from db_setup import get_connection, get_available_tickers
from models import SECFilingParserError
from store import store_numerical_facts
from ticker_loader import TickerLoadError, load_tickers_from_file
from add import open_parser
from parser import SECFilingParser

logger = logging.getLogger(__name__)


def update_ticker(
    parser: SECFilingParser,
    ticker: str,
    batch_size: int = 500,
) -> tuple[int, int]:
    facts = parser.get_numerical_facts(ticker)
    return store_numerical_facts(parser.conn, facts, batch_size=batch_size)


def update_tickers(
    tickers: Sequence[str],
    max_retries: int = 3,
    timeout: float = 30.0,
) -> tuple[int, int]:
    """
    run the quantitative Company Facts update for each ticker in `tickers`.
    opens its own DB connection and parser session. callable directly from
    another program, not just via main()'s CLI.
    """
    total_upserted = total_failed = 0
    with get_connection() as conn:
        with open_parser(conn, max_retries=max_retries, timeout=timeout) as parser:
            for ticker in tickers:
                try:
                    upserted, failed = update_ticker(parser, ticker)
                    total_upserted += upserted
                    total_failed += failed
                    print(f"[{ticker}] upserted={upserted} failed={failed}")
                except SECFilingParserError:
                    print(f"[{ticker}] not found in SEC EDGAR. skipping.")
    return total_upserted, total_failed

def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO)

    ap = argparse.ArgumentParser()
    ap.add_argument(
        "tickers",
        nargs="*",
        help="Ticker symbols passed inline. Defaults to every ticker already in the database.",
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

    # resolve ticker set from all sources
    try:
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
        tickers = [t for t, _ in get_available_tickers()]

    if not tickers:
        ap.error("No tickers supplied, and none found in the database. Pass symbols inline or use --file.")

    logger.info(" Updating %d ticker(s): %s", len(tickers), ", ".join(tickers))

    total_upserted, total_failed = update_tickers(
        tickers, max_retries=args.max_retries, timeout=args.timeout
    )
    print(f"\nDone. Total upserted: {total_upserted}, total failed: {total_failed}")

if __name__ == "__main__":
    sys.exit(main())
