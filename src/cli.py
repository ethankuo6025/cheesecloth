import asyncio
import selectors
import logging
import shutil
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit import PromptSession
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.shortcuts import clear as clear_screen

from parser import SECFilingParser
from main import parse_and_store
from db import get_available_tickers
from helpers import get_facts

logger = logging.getLogger(__name__)

MAX_UI_HEIGHT = 30
COMMANDS = ["ticker", "revenue", "eps", "help", "quit"]

ui_state = []
cmd_session = None
form_session = None

parser_ctx = None  # global parser kept open for the session

class AbortInput(Exception):
    pass

kb = KeyBindings()
kb.add("c-z")(lambda event: event.app.exit(exception=AbortInput()))

def header_line():
    return "═" * shutil.get_terminal_size(fallback=(80, 24))[0]

def reset_ui():
    global ui_state
    ui_state = []

def add_ui(*lines):
    for line in lines:
        ui_state.extend(line if isinstance(line, list) else [str(line)])

def render():
    clear_screen()
    rows = shutil.get_terminal_size(fallback=(80, 24))[1]
    visible = ui_state[-min(len(ui_state), min(MAX_UI_HEIGHT, max(5, rows - 5))):]

    print(header_line())
    print("  SEC FILING EXPLORER  │  'help' for commands  │  Ctrl+Z cancel  │  Ctrl+C exit")
    print(header_line())
    for line in visible:
        print(line)
    print(header_line())

def prompt_str(prompt_text: str, required=True, default=None) -> str | None:
    prompt_text = f"{prompt_text} [{default}]: " if default else f"{prompt_text}: "
    while True:
        val = form_session.prompt(prompt_text).strip() # type:ignore
        if val:
            return val
        if default:
            return default
        if not required:
            return None
        print("  This field is required.")

def prompt_yes_no(prompt_text: str, default=False):
    hint = "[Y/n]" if default else "[y/N]"
    val = form_session.prompt(f"{prompt_text} {hint}: ").strip().lower() # type:ignore
    return val in ("y", "yes") if val else default

def prompt_int(prompt_text: str, default=None, min_val=None, max_val=None):
    prompt_text = f"{prompt_text} [{default}]: " if default is not None else f"{prompt_text}: "
    while True:
        val = form_session.prompt(prompt_text).strip() # type:ignore
        if not val and default is not None:
            return default
        try:
            n = int(val)
            if min_val is not None and n < min_val:
                print(f"  Must be at least {min_val}.")
                continue
            if max_val is not None and n > max_val:
                print(f"  Must be at most {max_val}.")
                continue
            return n
        except ValueError:
            print("  Enter a number.")

def _run_scrape(ticker: str) -> tuple[int, int]:
    """synchronously run the async scrape pipeline. Called via asyncio.run."""
    async def _inner():
        upserted, failed = await parse_and_store(
            parser_ctx, # type:ignore
            ticker=ticker,
            filing_types="10-K",
        )
        return upserted, failed

    return asyncio.run(
        _inner(),
        loop_factory=lambda: asyncio.SelectorEventLoop(selectors.SelectSelector()),
    )

def _prompt_and_scrape_ticker() -> str | None:
    """processes new ticker for database."""
    ticker = prompt_str("Enter ticker to scrape (e.g. AAPL)", required=True)
    ticker = ticker.upper() # type:ignore

    print(f"\n  Scraping SEC filings for {ticker} – this may take a moment...")

    try:
        upserted, failed = _run_scrape(ticker)
    except Exception as exc:
        logger.error("Scrape failed: %s", exc, exc_info=True)
        print(f"  ✗ Scrape failed: {exc}")
        return None

    if upserted == 0 and failed == 0:
        print(f"  ✗ No filings found for '{ticker}'. Check the ticker symbol.")
        return None

    print(f"  Done – {upserted} facts stored, {failed} failed.")
    return ticker

def prompt_ticker_selection() -> str | None:
    """returns the ticker selected by user."""

    available = get_available_tickers()  # [(ticker, updated_at), ...]

    print("\n── Select Ticker ──")

    if available:
        print("\n  Available tickers:")
        for i, (ticker, updated_at) in enumerate(available, 1):
            print(f"    {i}. {ticker:<12}  (last updated: {updated_at.date()})")
    else:
        print("\n  No tickers in the database yet.")

    print(f"\n  Enter number to select, or 'add' to scrape a new ticker.")

    ticker_words = [t for t, _ in available]
    ticker_completer = WordCompleter(
        ticker_words + ["add"],
        ignore_case=True,
        sentence=True,
    )

    while True:
        val = form_session.prompt(  # type: ignore
            "  Ticker: ",
            completer=ticker_completer,
            complete_while_typing=True,
        ).strip().lower()

        if not val:
            print("  Please make a selection, or type 'add'.")
            continue

        if val == "add":
            new_ticker = _prompt_and_scrape_ticker()
            return new_ticker

        if val.isdigit():
            idx = int(val)
            if 1 <= idx <= len(available):
                return available[idx - 1][0]
            print(f"  Invalid number. Enter 1–{len(available)} or 'add'.")
            continue

        # directly typing a ticker
        upper_val = val.upper()
        match = next((t for t, _ in available if t == upper_val), None)
        if match:
            return match

        print("  Not recognised. Enter a list number, a valid ticker, or 'add'.")

def _format_table(headers: list[str], rows: list[tuple]) -> list[str]:
    """displays tuples in CLI."""
    if not rows:
        return ["  (no data)"]

    widths = [
        max(len(str(h)), max(len(str(cell) if cell is not None else "") for cell in col))
        for h, col in zip(headers, zip(*rows))
    ]

    fmt = " | ".join(f"{{:<{w}}}" for w in widths)
    sep = "-+-".join("-" * w for w in widths)

    lines = ["  " + fmt.format(*headers), "  " + sep]
    for row in rows:
        lines.append("  " + fmt.format(*(str(c) if c is not None else "" for c in row)))
    return lines

def _format_fact_rows(raw_rows) -> list[tuple]:
    """converts raw DB rows into display tuples."""
    out = []
    for row in raw_rows:
        local_name, period_type, value, instant_date, start_date, end_date, unit, accession = row

        # build a friendly date string
        if period_type == "instant" and instant_date:
            date_str = str(instant_date)
        elif start_date and end_date:
            date_str = f"{start_date} -> {end_date}"
        else:
            date_str = "–"

        # format numeric value
        try:
            numeric = float(value)
            if abs(numeric) >= 1_000_000_000:
                value_str = f"${numeric / 1_000_000_000:.2f}B"
            elif abs(numeric) >= 1_000_000:
                value_str = f"${numeric / 1_000_000:.2f}M"
            else:
                value_str = f"${numeric:,.2f}"
        except (TypeError, ValueError):
            value_str = str(value) if value is not None else "–"

        unit_str = unit or "–"
        accession_short = accession[-8:] if accession else "–"

        out.append((date_str, value_str, unit_str, accession_short))

    return out

def cmd_ticker() -> list[str]:
    """select a ticker to work with for this session."""
    ticker = prompt_ticker_selection()
    if not ticker:
        return ["No ticker selected."]

    global _active_ticker
    _active_ticker = ticker
    return [f"Active ticker set to {ticker}.", "Use 'revenue' or 'eps' to explore data."]

def cmd_revenue() -> list[str]:
    """show revenue facts for the active ticker."""
    ticker = _get_active_ticker()
    if ticker is None:
        return ["No ticker selected. Run 'ticker' first."]

    print(f"\n── Revenue for {ticker} ──")
    raw = get_facts(ticker, "revenue", "annual")

    if not raw:
        return [
            f"No revenue data found for {ticker}.",
            "The ticker may not have 10-K filings with us-gaap:Revenues.",
        ]

    rows = _format_fact_rows(raw)
    headers = ["Period", "Revenue", "Unit", "Accession (tail)"]
    lines = [f"Revenue - {ticker}", ""]
    lines += _format_table(headers, rows)
    lines += ["", f"  {len(rows)} record(s) found."]
    return lines

def cmd_eps() -> list[str]:
    """show diluted EPS facts for the active ticker."""
    ticker = _get_active_ticker()
    if ticker is None:
        return ["No ticker selected. Run 'ticker' first."]

    print(f"\n── Diluted EPS for {ticker} ──")
    raw = get_facts(ticker, "eps", "annual")

    if not raw:
        return [
            f"No EPS data found for {ticker}.",
            "The ticker may not have 10-K filings with us-gaap:EarningsPerShareDiluted.",
        ]

    rows = _format_fact_rows(raw)
    headers = ["Period", "EPS (Diluted)", "Unit", "Accession (tail)"]
    lines = [f"Diluted EPS - {ticker}", ""]
    lines += _format_table(headers, rows)
    lines += ["", f"  {len(rows)} record(s) found."]
    return lines

def cmd_help() -> list[str]:
    return ["""
┌────────────────────────────────────────────────────────────────────────┐
│                              CHEESECLOTH                               │
├────────────────────────────────────────────────────────────────────────┤
│  COMMANDS                                                              │
│    - ticker   Select an existing ticker or scrape a new one            │
│      - revenue  Shows available revenues for a parsed ticker           │
│      - eps      Show diluted EPS for a parsed ticker                   │
│    - help     Show this screen                                         │
│    - quit     Exit the program                                         │
│                                                                        │
│  WORKFLOW                                                              │
│    1.  Run 'ticker' to pick a company                                  │
│    2.  Pick from the list OR type 'add' to scrape from the EDGAR API   │
│    3.  Run 'revenue' or 'eps' to view financial data                   │
│                                                                        │
│  Ctrl+C to exit  │  Ctrl+Z to cancel current input                     │
└────────────────────────────────────────────────────────────────────────┘
"""]

_active_ticker: str | None = None

def _get_active_ticker() -> str | None:
    return _active_ticker

COMMAND_MAP = {
    "ticker":  cmd_ticker,
    "revenue": cmd_revenue,
    "eps":     cmd_eps,
    "help":    cmd_help,
}

def process_command(cmd: str) -> list[str]:
    cmd = cmd.strip().lower()
    if not cmd:
        return []

    if cmd in ("quit", "exit", "q"):
        raise KeyboardInterrupt

    if cmd in COMMAND_MAP:
        return COMMAND_MAP[cmd]()

    matches = [c for c in COMMAND_MAP if c.startswith(cmd)]
    if len(matches) == 1:
        return COMMAND_MAP[matches[0]]()
    if len(matches) > 1:
        return [f"Ambiguous: {', '.join(matches)}"]

    return [f"Unknown command: '{cmd}'. Type 'help' for commands."]

def main():
    global cmd_session, form_session, parser_ctx

    logging.basicConfig(level=logging.WARNING)  # keep the terminal clean

    cmd_session = PromptSession(
        completer=WordCompleter(COMMANDS, ignore_case=True, sentence=True),
        key_bindings=kb,
    )
    form_session = PromptSession(key_bindings=kb)

    print(header_line())
    print("CHEESECLOTH")
    print(header_line())

    # keep one parser alive for the entire session (re-uses the HTTP session)
    parser_ctx = SECFilingParser(max_retries=3, timeout=30.0).__enter__()

    try:
        reset_ui()
        tickers = get_available_tickers()
        if tickers:
            add_ui(
                "Run 'ticker' to select one, or 'help' for all commands.",
            )
        else:
            add_ui(
                "No tickers in the database yet.",
                "",
                "Run 'ticker' into 'add' to scrape your first company.",
            )
        render()

        while True:
            try:
                raw = cmd_session.prompt("\n> ").strip()
                result = process_command(raw)
                if result:
                    reset_ui()
                    add_ui(result)
                    render()
            except AbortInput:
                reset_ui()
                add_ui("Cancelled.")
                render()

    except (EOFError, KeyboardInterrupt):
        print("\n\nGoodbye!")
    finally:
        try:
            parser_ctx.__exit__(None, None, None)
        except Exception:
            pass

if __name__ == "__main__":
    main()