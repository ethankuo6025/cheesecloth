import logging
import shutil
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit import PromptSession
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.shortcuts import clear as clear_screen

from parser import SECFilingParser, TickerNotFoundError
from add import parse_and_store
from db import get_available_tickers, get_connection
from query import resolve, REGISTRY

logger = logging.getLogger(__name__)

BASE_COMMANDS = ["ticker", "mode", "help", "quit"]
QUERY_COMMANDS = [
    "revenue", "eps", "gross", "operating", "net",
    "liabilities", "assets", "cash", "debt", "shares",
]
METRIC_COMMANDS = [
    "gross_margin", "op_margin", "profit_margin",
    "cash_ratio", "debt_ratio", "debt_equity",
    "current_ratio", "net_debt", "working_cap",
    "roa", "roe",
]

ui_state = []
cmd_session = None
form_session = None

parser_ctx = None  # global parser kept open for the session

_active_ticker: str | None = None
_query_mode: str = "annual"  # "annual", "quarterly", or "all"

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

def _get_available_commands() -> list[str]:
    """Returns commands available based on current state."""
    if _active_ticker:
        return BASE_COMMANDS + QUERY_COMMANDS + METRIC_COMMANDS
    return BASE_COMMANDS

def _render():
    clear_screen()
    print(_header_line())
    ticker_info = f"Ticker: {_active_ticker}" if _active_ticker else "No ticker"
    mode_info = f"Mode: {_query_mode.upper()}"
    print(f"  CHEESECLOTH  │  {ticker_info}  │  {mode_info}  │  'help' for commands")
    print(_header_line())
    for line in ui_state:
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
            return _prompt_and_scrape_ticker()

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


def _format_fact_rows(
    raw_rows,
    is_per_share: bool = False,
    is_count: bool = False,
    is_percentage: bool = False,
    is_multiple: bool = False,
) -> list[tuple]:
    """converts raw DB rows into display tuples."""
    out = []
    for row in raw_rows:
        _, period_type, value, instant_date, start_date, end_date, unit, decimals, accession = row

        # build date string
        if period_type == "instant" and instant_date:
            date_str = str(instant_date)
        elif start_date and end_date:
            date_str = f"{start_date} -> {end_date}"
        else:
            date_str = "-"

        # format numeric value
        try:
            numeric = float(value) if not isinstance(value, (int, float)) else value
            
            if is_percentage:
                value_str = f"{numeric:.2f}%"
            elif is_multiple:
                value_str = f"{numeric:.2f}x"
            elif is_per_share:
                value_str = f"${numeric:.2f}"
            elif is_count:
                if abs(numeric) >= 1_000_000_000:
                    value_str = f"{numeric / 1_000_000_000:.2f}B"
                elif abs(numeric) >= 1_000_000:
                    value_str = f"{numeric / 1_000_000:.2f}M"
                elif abs(numeric) >= 1_000:
                    value_str = f"{numeric / 1_000:.2f}K"
                else:
                    value_str = f"{numeric:,.0f}"
            else:
                sign = "-" if numeric < 0 else ""
                abs_numeric = abs(numeric)
                if abs_numeric >= 1_000_000_000:
                    value_str = f"{sign}${abs_numeric / 1_000_000_000:.2f}B"
                elif abs_numeric >= 1_000_000:
                    value_str = f"${abs_numeric / 1_000_000:.2f}M"
                elif abs_numeric >= 1_000:
                    value_str = f"${abs_numeric / 1_000:.2f}K"
                else:
                    value_str = f"{sign}${abs_numeric:,.2f}"
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
    return [f"Active ticker set to {ticker}. Type 'help' for commands."]


def _cmd_mode() -> list[str]:
    global _query_mode
    
    print("\n── Query Mode ──")
    print(f"  Current: {_query_mode.upper()}")
    print("  1. annual  2. quarterly  3. all")
    
    mode_completer = WordCompleter(
        ["annual", "quarterly", "all", "1", "2", "3"],
        ignore_case=True,
    )
    
    val = form_session.prompt("  Select: ", completer=mode_completer).strip().lower()  # type: ignore
    
    modes = {
        "1": "annual", "2": "quarterly", "3": "all",
        "annual": "annual", "quarterly": "quarterly", "all": "all",
        "a": "annual", "q": "quarterly"
    }
    
    if val in modes:
        _query_mode = modes[val]
        return [f"Query mode: {_query_mode.upper()}"]
    
    return [f"Invalid. Mode unchanged: {_query_mode.upper()}"]

def _cmd_query(query: str, display_name: str, is_per_share: bool = False, is_count: bool = False) -> list[str]:
    """show queried facts for the active ticker."""
    mode_label = _query_mode.upper()
    raw = resolve(_active_ticker, query, _query_mode)  # type: ignore

    if not raw:
        return [f"No {display_name.lower()} data for {_active_ticker} ({mode_label})."]

    rows = _format_fact_rows(raw, is_per_share=is_per_share, is_count=is_count)
    headers = ["Period", display_name, "Unit", "Accession"]
    lines = [f"{display_name} - {_active_ticker} ({mode_label})", ""]
    lines += _format_table(headers, rows)
    lines += ["", f"  {len(rows)} record(s)."]
    return lines

def _cmd_metric(metric_key: str) -> list[str]:
    defn = REGISTRY.get(metric_key)
    if not defn:
        return [f"Unknown metric: {metric_key}"]

    mode_label = _query_mode.upper()

    try:
        raw = resolve(_active_ticker, metric_key, _query_mode)  # type: ignore
    except ValueError as e:
        return [f"Error: {e}"]

    if not raw:
        return [f"No {defn.display_name.lower()} data for {_active_ticker} ({mode_label})."]

    is_pct = defn.format_type == "percentage"
    is_mult = defn.format_type == "multiple"
    
    rows = _format_fact_rows(raw, is_percentage=is_pct, is_multiple=is_mult)
    headers = ["Period", defn.display_name, "Unit", "Accession"]
    lines = [f"{defn.display_name} - {_active_ticker} ({mode_label})", ""]
    lines += _format_table(headers, rows)
    lines += ["", f"  {len(rows)} record(s)."]
    return lines

def _cmd_help() -> list[str]:
    lines = [
        "COMMANDS",
        "  ticker  - Select or add a ticker",
        "  mode    - Toggle annual/quarterly/all",
        "  help    - Show this help",
        "  quit    - Exit",
        "",
    ]
    if _active_ticker:
        lines += [
            "QUERIES",
            "  revenue, gross, operating, net, eps,",
            "  assets, liabilities, cash, debt, shares,",
            "  gross_margin, op_margin, profit_margin,",
            "  cash_ratio, debt_ratio, debt_equity, current_ratio,",
            "  net_debt, working_cap, roa, roe",
        ]
    else:
        lines += ["Run 'ticker' first to enable queries."]
    
    lines += ["", "KEYS: Ctrl+C exit | Ctrl+Z cancel"]
    return lines

def _cmd_revenue():
    return _cmd_query(query="revenue", display_name="Revenue")

def _cmd_eps():
    return _cmd_query(query="eps", display_name="Diluted EPS", is_per_share=True)

def _cmd_gross():
    return _cmd_query(query="gross", display_name="Gross Profit")

def _cmd_operating():
    return _cmd_query(query="operating", display_name="Operating Income")

def _cmd_net():
    return _cmd_query(query="net", display_name="Net Income")

def _cmd_liabilities():
    return _cmd_query(query="liabilities", display_name="Total Liabilities")

def _cmd_assets():
    return _cmd_query(query="total_assets", display_name="Total Assets")

def _cmd_cash():
    return _cmd_query(query="cash_on_hand", display_name="Cash on Hand")

def _cmd_debt():
    return _cmd_query(query="long_term_total_debt", display_name="Long-Term Debt")

def _cmd_shares():
    return _cmd_query(query="shares_outstanding", display_name="Shares Outstanding", is_count=True)

BASE_COMMAND_MAP = {
    "ticker": _cmd_ticker,
    "mode":   _cmd_mode,
    "help":   _cmd_help,
}

QUERY_COMMAND_MAP = {
    "revenue":     _cmd_revenue,
    "eps":         _cmd_eps,
    "gross":       _cmd_gross,
    "operating":   _cmd_operating,
    "net":         _cmd_net,
    "liabilities": _cmd_liabilities,
    "assets":      _cmd_assets,
    "cash":        _cmd_cash,
    "debt":        _cmd_debt,
    "shares":      _cmd_shares,
}

METRIC_COMMAND_KEYS = {
    "gross_margin": "gross_margin",
    "op_margin": "operating_margin",
    "profit_margin": "profit_margin",
    "cash_ratio": "cash_to_assets",
    "debt_ratio": "debt_to_assets",
    "debt_equity": "debt_to_equity",
    "current_ratio": "current_ratio",
    "net_debt": "net_debt",
    "working_cap": "working_capital",
    "roa": "roa",
    "roe": "roe",
}

def _process_command(cmd: str) -> list[str]:
    cmd = cmd.strip().lower()
    if not cmd:
        return []

    if cmd in ("quit", "exit", "q"):
        raise KeyboardInterrupt

    if cmd in BASE_COMMAND_MAP:
        return BASE_COMMAND_MAP[cmd]()

    if cmd in QUERY_COMMAND_MAP:
        if not _active_ticker:
            return ["No ticker selected. Run 'ticker' first."]
        return QUERY_COMMAND_MAP[cmd]()

    if cmd in METRIC_COMMAND_KEYS:
        if not _active_ticker:
            return ["No ticker selected. Run 'ticker' first."]
        return _cmd_metric(METRIC_COMMAND_KEYS[cmd])

    # prefix matching
    all_commands = set(BASE_COMMAND_MAP)
    if _active_ticker:
        all_commands.update(QUERY_COMMAND_MAP)
        all_commands.update(METRIC_COMMAND_KEYS)
    
    matches = [c for c in all_commands if c.startswith(cmd)]
    
    if len(matches) == 1:
        return _process_command(matches[0])
    if len(matches) > 1:
        return [f"Ambiguous: {', '.join(matches)}"]

    # check if query command doesn't have ticker
    ticker_cmds = set(QUERY_COMMAND_MAP) | set(METRIC_COMMAND_KEYS)
    if any(c.startswith(cmd) for c in ticker_cmds) and not _active_ticker:
        return ["No ticker selected. Run 'ticker' first."]

    return [f"Unknown: '{cmd}'. Type 'help'."]

def _main():
    global cmd_session, form_session, parser_ctx

    cmd_session = PromptSession(key_bindings=kb)
    form_session = PromptSession(key_bindings=kb)

    print(_header_line())
    print("CHEESECLOTH - SEC Filing Explorer")
    print(_header_line())

    conn = get_connection()
    parser_ctx = SECFilingParser(conn, max_retries=3, timeout=30.0).__enter__()

    try:
        _reset_ui()
        tickers = get_available_tickers()
        if tickers:
            _add_ui("Run 'ticker' to select one, or 'help' for commands.")
        else:
            _add_ui(
                "No tickers in database.",
                "Run 'ticker' then 'add' to scrape your first company.",
            )
        _render()

        while True:
            try:
                available_cmds = _get_available_commands()
                completer = WordCompleter(available_cmds, ignore_case=True, sentence=True)
                
                raw = cmd_session.prompt("\n> ", completer=completer).strip()
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