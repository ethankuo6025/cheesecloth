import logging

from parser import SECFilingParser
from store import store_facts
import selectors
import asyncio

async def parse_and_store(
    parser: SECFilingParser,
    ticker: str,
    filing_types: str = "10-K",
    max_filings: int | None = None,
    batch_size: int = 500,
) -> tuple[int, int]:
    filings, facts = await asyncio.to_thread(
        parser.parse_filings,
        ticker,
        filing_types=filing_types,
        max_filings=max_filings,
    )
    return await store_facts(filings, facts, batch_size=batch_size)


async def main():
    logging.basicConfig(level=logging.INFO)

    with SECFilingParser(max_retries=3, timeout=30.0) as parser:
        upserted, failed = await parse_and_store(
            parser,
            ticker="HOOD",
            filing_types="10-K",
        )
    print(f"\nDone: {upserted} upserted, {failed} failed")


if __name__ == "__main__":
    asyncio.run(
        main(),
        loop_factory=lambda: asyncio.SelectorEventLoop(selectors.SelectSelector()),
    )