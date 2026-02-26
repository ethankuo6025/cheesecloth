import logging
import shutil
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit import PromptSession
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.shortcuts import clear as clear_screen

from parser import SECFilingParser, TickerNotFoundError
from add import parse_and_store
from db import get_available_tickers, get_connection
from helpers import get_facts

logger = logging.getLogger(__name__)

MAX_UI_HEIGHT = 30
COMMANDS = ["ticker", "revenue", "eps", "liabilities", "help", "quit"]

ui_state = []
cmd_session = None
form_session = None

parser_ctx = None  # global parser kept open for the session

class AbortInput(Exception):
    pass

kb = KeyBindings()
kb.add("c-z")(lambda event: event.app.exit(exception=AbortInput()))

def _header_line():
    return "═" * shutil.get_terminal_size(fallback=(80, 24))[0]

def _reset_ui():
    global ui_state
    ui_state = []

def _add_ui(*lines):
    for line in lines:
        ui_state.extend(line if isinstance(line, list) else [str(line)])

def _render():
    clear_screen()
    rows = shutil.get_terminal_size(fallback=(80, 24))[1]
    visible = ui_state[-min(len(ui_state), min(MAX_UI_HEIGHT, max(5, rows - 5))):]

    print(_header_line())
    print("  SEC FILING EXPLORER  │  'help' for commands  │  Ctrl+Z cancel  │  Ctrl+C exit")
    print(_header_line())
    for line in visible:
        print(line)
    print(_header_line())

def _prompt_str(prompt_text: str, required=True, default=None) -> str | None:
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

def _prompt_yes_no(prompt_text: str, default=False):
    # reused from another project, maybe needed later
    hint = "[Y/n]" if default else "[y/N]"
    val = form_session.prompt(f"{prompt_text} {hint}: ").strip().lower() # type:ignore
    return val in ("y", "yes") if val else default

def _prompt_int(prompt_text: str, default=None, min_val=None, max_val=None):
    # reused from another project, maybe needed later
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
    total_upserted = 0
    total_failed = 0
    for filing_type in ("10-K", "10-Q"):
        upserted, failed = parse_and_store(parser_ctx, ticker=ticker, filing_types=filing_type)  # type:ignore
        total_upserted += upserted
        total_failed += failed
    return total_upserted, total_failed

def _prompt_and_scrape_ticker() -> str | None:
    """processes new ticker for database."""
    ticker = _prompt_str("Enter ticker to scrape (e.g. AAPL)", required=True)
    ticker = ticker.upper() # type:ignore

    print(f"\n  Scraping SEC filings for {ticker} - this may take a moment...")

    try:
        upserted, failed = _run_scrape(ticker)
    except TickerNotFoundError:
        print(f"  '{ticker}' was not found in SEC EDGAR. Check the ticker symbol.")
        return None
    except Exception as exc:
        print(f"   Scrape failed: {exc}")
        return None

    if upserted == 0 and failed == 0:
        print(f"  No filings found for '{ticker}'. Check the ticker symbol.")
        return None

    print(f"  Done - {upserted} facts stored, {failed} failed.")
    return ticker

def _prompt_ticker_selection() -> str | None:
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
            print(f"  Invalid number. Enter 1-{len(available)} or 'add'.")
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
        _, period_type, value, instant_date, start_date, end_date, unit, decimals,accession = row

        # build date string
        if period_type == "instant" and instant_date:
            date_str = str(instant_date)
        elif start_date and end_date:
            date_str = f"{start_date} -> {end_date}"
        else:
            date_str = "-"

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
            value_str = str(value) if value is not None else "-"

        unit_str = unit or "-"
        accession_short = accession[-8:] if accession else "-"

        out.append((date_str, value_str, unit_str, accession_short))

    return out

def _cmd_ticker() -> list[str]:
    """select a ticker to work with for this session."""
    ticker = _prompt_ticker_selection()
    if not ticker:
        return ["No ticker selected."]

    global _active_ticker
    _active_ticker = ticker
    return [f"Active ticker set to {ticker}.", "Use 'revenue' or 'eps' to explore data."]

def _cmd_query(query: str, display_name: str) -> list[str]:
    """show queried facts for the active ticker."""
    ticker = _get_active_ticker()
    if ticker is None:
        return ["No ticker selected. Run 'ticker' first."]

    print(f"\n── {display_name} for {ticker} ──")
    raw = get_facts(ticker, query, "annual")

    if not raw:
        return [
            f"No {display_name.lower()} data found for {ticker}."
        ]

    rows = _format_fact_rows(raw)
    headers = ["Period", display_name, "Unit", "Accession (tail)"]
    lines = [f"{display_name} - {ticker}", ""]
    lines += _format_table(headers, rows)
    lines += ["", f"  {len(rows)} record(s) found."]
    return lines

def _cmd_help() -> list[str]:
    return ["""
┌────────────────────────────────────────────────────────────────────────┐
│                              CHEESECLOTH                               │
├────────────────────────────────────────────────────────────────────────┤
│  COMMANDS                                                              │
│    - ticker   Select an existing ticker or scrape a new one            │
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

def _cmd_revenue():
    return _cmd_query(query="revenue", display_name="Revenue")

def _cmd_eps():
    return _cmd_query(query="eps", display_name="Diluted EPS")

def _cmd_liabilities():
    return _cmd_query(query="liabilities", display_name="Total Debt")

COMMAND_MAP = {
    "ticker":  _cmd_ticker,
    "revenue": _cmd_revenue,
    "eps":     _cmd_eps,
    "liabilities":    _cmd_liabilities,
    "help":    _cmd_help,
}

def _process_command(cmd: str) -> list[str]:
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

def _main():
    global cmd_session, form_session, parser_ctx

    logging.basicConfig(level=logging.WARNING)  # keep terminal clean

    cmd_session = PromptSession(
        completer=WordCompleter(COMMANDS, ignore_case=True, sentence=True),
        key_bindings=kb,
    )
    form_session = PromptSession(key_bindings=kb)

    print(_header_line())
    print("CHEESECLOTH")
    print(_header_line())

    # keep one parser alive for the entire session
    conn = get_connection()
    parser_ctx = SECFilingParser(conn, max_retries=3, timeout=30.0).__enter__()

    try:
        _reset_ui()
        tickers = get_available_tickers()
        if tickers:
            _add_ui(
                "Run 'ticker' to select one, or 'help' for all commands.",
            )
        else:
            _add_ui(
                "No tickers in the database yet.",
                "",
                "Run 'ticker' into 'add' to scrape your first company.",
            )
        _render()

        while True:
            try:
                raw = cmd_session.prompt("\n> ").strip()
                result = _process_command(raw)
                if result:
                    _reset_ui()
                    _add_ui(result)
                    _render()
            except AbortInput:
                _reset_ui()
                _add_ui("Cancelled.")
                _render()

    except (EOFError, KeyboardInterrupt):
        print("\n\nGoodbye!")
    finally:
        try:
            parser_ctx.__exit__(None, None, None)
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    _main()