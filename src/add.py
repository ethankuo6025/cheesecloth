import logging
import os

from dotenv import load_dotenv
from psycopg_pool import ConnectionPool

from parser import SECFilingParser, TickerNotFoundError
from store import store_facts
import selectors
import asyncio
import sys

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
CONNINFO = f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD}"

logging.getLogger("arelle").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

async def parse_and_store(
    parser: SECFilingParser,
    ticker: str,
    filing_types: str = "10-K",
    max_filings: int | None = None,
    batch_size: int = 500,
) -> tuple[int, int]:
    """
    Parse and store filings incrementally, one at a time.
    This prevents memory issues and ensures progress is saved after each filing.
    """
    # Get list of filings to parse (already filtered for unscanned ones)
    cik, filings_to_parse = await asyncio.to_thread(
        parser.get_filings_to_parse,
        ticker,
        filing_types,
        max_filings,
    )
    
    if not filings_to_parse:
        return 0, 0
    
    total_upserted = total_failed = 0
    ticker_upper = ticker.upper()
    
    # Process each filing individually
    for i, filing in enumerate(filings_to_parse):
        logger.info("Processing filing %d/%d: %s", i + 1, len(filings_to_parse), filing.accession_number)
        
        try:
            # parse filing
            facts = await asyncio.to_thread(
                parser.parse_filing,
                filing,
                ticker_upper,
                cik
            )

            # store contents asynchronously 
            upserted, failed = await store_facts([filing], facts, batch_size=batch_size)
            total_upserted += upserted
            total_failed += failed
            
            logger.info("Filing %s: %d facts upserted, %d failed", filing.accession_number, upserted, failed)
            
        except Exception as e:
            logger.error("Failed to process filing %s: %s", filing.accession_number, e, exc_info=True)
            total_failed += 1
            continue
    
    return total_upserted, total_failed


async def main(ticker: str, filing_types: list[str] = ["10-K", "10-Q"]):
    logging.basicConfig(level=logging.INFO)
    with ConnectionPool(CONNINFO) as pool:
        with pool.connection() as conn:
            for filing_type in filing_types:
                with SECFilingParser(conn, max_retries=3, timeout=30.0) as parser:
                    upserted, failed = await parse_and_store(
                        parser,
                        ticker=ticker,
                        filing_types=filing_type,
                        max_filings=None,
                    )
                    print(f"\nDone: {upserted} upserted, {failed} failed")

if __name__ == "__main__":  
    if len(sys.argv) > 1:
        tickers = sys.argv[1:]
        for ticker in tickers:
            try:
                asyncio.run(
                    main(ticker, ["10-K", "10-Q"]),
                    loop_factory=lambda: asyncio.SelectorEventLoop(selectors.SelectSelector()),
                )
            except TickerNotFoundError:
                print(f"'{ticker}' was not found in SEC EDGAR. Skipping.")
