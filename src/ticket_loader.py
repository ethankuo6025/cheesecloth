from __future__ import annotations

from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class TickerLoadError(Exception):
    """raised when a ticker source cannot be read."""
    pass

def load_tickers_from_file(path: str) -> list[str]:
    """read tickers from a plain-text file."""
    p = Path(path)
    try:
        raw = p.read_text(encoding="utf-8")
    except OSError as e:
        raise TickerLoadError(f"Could not read ticker file '{p}': {e}") from e

    tickers: list[str] = []
    for _, line in enumerate(raw.splitlines()):
        if "#" in line:
            line = line.split("#", 1)[0]
        line = line.strip()
        if line is not None:
            tickers.append(line.upper())

    logger.info("Loaded %d ticker(s) from %s", len(tickers), p)
    return tickers