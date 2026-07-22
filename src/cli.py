"""CLI/TUI interface for accessing screener"""
import logging
import shutil
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit import PromptSession
from prompt_toolkit.completion import WordCompleter, FuzzyWordCompleter, NestedCompleter
from prompt_toolkit.shortcuts import clear as clear_screen

from scrape_textual import ingest_textual_ticker, open_parser
from models import TickerNotFoundError
from db_setup import get_available_tickers, get_connection
import query
from models import Metric

logger = logging.getLogger(__name__)

ALLOWED_FORMATS = {"percentage", "ratio", "currency", "number", "text"}

ui_state = []
cmd_session = None
form_session = None

parser = None  # global parser kept open for the session

_active_ticker: str | None = None
_query_mode: str = "annual"  # "annual", "quarterly", or "all"

_catalog: list[Metric] | None = None

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

# ── Metric catalog ──

def _get_catalog() -> list[Metric]:
    global _catalog
    if _catalog is None:
        _catalog = query.get_metrics()
    return _catalog

def _refresh_catalog():
    global _catalog
    _catalog = None

def _catalog_map() -> dict[str, Metric]:
    return {m.key: m for m in _get_catalog()}

def _short_value(val) -> str:
    """a compact one-line preview of a fact value for the mapping browser."""
    if val is None:
        return "-"
    s = str(val).strip().replace("\n", " ")
    return (s[:18] + "…") if len(s) > 18 else s

def _build_command_completer() -> NestedCompleter:
    """
    nested completer for the main prompt: completes the command word, then its
    arguments after a space (e.g. `mode ` -> annual/quarterly/all, `map ` ->
    metric keys, `ticker ` -> known symbols). rebuilt each prompt so it tracks
    the active ticker, catalog, and ticker list.
    """
    metric_keys = {m.key: None for m in _get_catalog()}
    ticker_opts: dict[str, None] = {t: None for t, _ in get_available_tickers()}
    ticker_opts["add"] = None

    nested: dict[str, object] = {
        "ticker": ticker_opts,
        "mode": {"annual": None, "quarterly": None, "all": None},
        "metrics": None,
        "help": None,
        "quit": None,
    }
    if _active_ticker:
        nested["map"] = dict(metric_keys)      # `map <metric>`
        nested.update(metric_keys)             # bare metric key = query
    else:
        nested["map"] = None

    return NestedCompleter.from_nested_dict(nested)

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

def _prompt_and_scrape_ticker() -> str | None:
    """processes new ticker for database."""
    ticker = _prompt_str("Enter ticker to scrape (e.g. AAPL)", required=True)
    ticker = ticker.upper() # type:ignore

    print(f"\n  Scraping SEC filings for {ticker} - this may take a moment...")

    try:
        upserted, failed = ingest_textual_ticker(parser, ticker, ("10-K", "10-Q"))  # type:ignore
    except TickerNotFoundError:
        print(f"  '{ticker}' was not found in SEC EDGAR. Check the ticker symbol.")
        parser.conn.commit()  # type:ignore
        return None
    except Exception as exc:
        print(f"   Scrape failed: {exc}")
        parser.conn.commit()  # type:ignore
        return None
    parser.conn.commit()  # type:ignore

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


def _format_fact_rows(raw_rows, format_type: str = "currency") -> list[tuple]:
    """converts raw DB rows into display tuples, formatted per the metric's type."""
    out = []
    for row in raw_rows:
        period_type = row.period_type
        value = row.value
        instant_date = row.instant_date
        start_date = row.start_date
        end_date = row.end_date
        unit = row.unit
        accession = row.accession_number

        # build date string
        if period_type == "instant" and instant_date:
            date_str = str(instant_date)
        elif start_date and end_date:
            date_str = f"{start_date} -> {end_date}"
        else:
            date_str = "-"

        if format_type == "text":
            out.append((date_str, _short_value(value), unit or "-",
                        accession[-8:] if accession else "-"))
            continue

        # format numeric value
        try:
            numeric = float(value) if not isinstance(value, (int, float)) else value

            if format_type == "percentage":
                value_str = f"{numeric:.2f}%"
            elif format_type == "ratio":
                value_str = f"{numeric:.2f}"
            elif format_type == "number":
                if abs(numeric) >= 1_000_000_000:
                    value_str = f"{numeric / 1_000_000_000:.2f}B"
                elif abs(numeric) >= 1_000_000:
                    value_str = f"{numeric / 1_000_000:.2f}M"
                elif abs(numeric) >= 1_000:
                    value_str = f"{numeric / 1_000:.2f}K"
                else:
                    value_str = f"{numeric:,.0f}"
            else:  # currency
                sign = "-" if numeric < 0 else ""
                abs_numeric = abs(numeric)
                if abs_numeric >= 1_000_000_000:
                    value_str = f"{sign}${abs_numeric / 1_000_000_000:.2f}B"
                elif abs_numeric >= 1_000_000:
                    value_str = f"{sign}${abs_numeric / 1_000_000:.2f}M"
                elif abs_numeric >= 1_000:
                    value_str = f"{sign}${abs_numeric / 1_000:.2f}K"
                else:
                    value_str = f"{sign}${abs_numeric:,.2f}"
        except (TypeError, ValueError):
            value_str = _short_value(value)

        unit_str = unit or "-"
        accession_short = accession[-8:] if accession else "-"

        out.append((date_str, value_str, unit_str, accession_short))

    return out

def _cmd_ticker(arg: str = "") -> list[str]:
    """select a ticker to work with for this session (inline `ticker <sym>` or a prompt)."""
    global _active_ticker
    arg = arg.strip()

    if arg:
        if arg.lower() == "add":
            ticker = _prompt_and_scrape_ticker()
            if not ticker:
                return ["No ticker added."]
            _active_ticker = ticker
            return [f"Active ticker set to {ticker}. Type 'help' for commands."]
        available = {t.upper() for t, _ in get_available_tickers()}
        if arg.upper() in available:
            _active_ticker = arg.upper()
            return [f"Active ticker set to {_active_ticker}. Type 'help' for commands."]
        return [
            f"'{arg.upper()}' isn't in the database.",
            "Type 'ticker add' to scrape it, or 'ticker' to list what's available.",
        ]

    ticker = _prompt_ticker_selection()
    if not ticker:
        return ["No ticker selected."]
    _active_ticker = ticker
    return [f"Active ticker set to {ticker}. Type 'help' for commands."]


MODE_ALIASES = {
    "1": "annual", "2": "quarterly", "3": "all",
    "annual": "annual", "quarterly": "quarterly", "all": "all",
    "a": "annual", "q": "quarterly",
}

def _mode_help(note: str | None = None) -> list[str]:
    lines = [note] if note else []
    lines += [
        "MODE — how periods are filtered in queries",
        f"  Current: {_query_mode.upper()}",
        "  Usage: mode <annual|quarterly|all>",
        "    annual     only full-year periods",
        "    quarterly  only quarterly periods",
        "    all        every period",
    ]
    return lines

def _cmd_mode(arg: str = "") -> list[str]:
    global _query_mode
    arg = arg.strip().lower()
    if not arg:
        return _mode_help()
    if arg in MODE_ALIASES:
        _query_mode = MODE_ALIASES[arg]
        return [f"Query mode: {_query_mode.upper()}"]
    return _mode_help(f"Invalid mode '{arg}'. Mode unchanged.")

def _cmd_query(metric: Metric) -> list[str]:
    """show queried facts for the active ticker, formatted per the catalog."""
    mode_label = _query_mode.upper()

    try:
        raw = query.resolve(_active_ticker, metric.key, _query_mode)  # type: ignore
    except ValueError as e:
        return [f"Error: {e}"]

    if not raw:
        # Distinguish "no mapping configured" from "mapped, but no rows".
        if not query.get_metric_mappings(_active_ticker, metric.key):  # type: ignore
            return [
                f"No mapping for '{metric.display_name}' on {_active_ticker}.",
                "Run 'map' to choose which reported concepts feed this metric.",
            ]
        return [f"No {metric.display_name.lower()} data for {_active_ticker} ({mode_label})."]

    rows = _format_fact_rows(raw, metric.format_type)
    headers = ["Period", metric.display_name, "Unit", "Accession"]
    lines = [f"{metric.display_name} - {_active_ticker} ({mode_label})", ""]
    lines += _format_table(headers, rows)
    lines += ["", f"  {len(rows)} record(s)."]
    return lines

def _cmd_metrics(arg: str = "") -> list[str]:
    """list the metric catalog, marking which are mapped for the active ticker."""
    catalog = _get_catalog()
    lines = ["METRIC CATALOG", ""]

    mapped: set[str] = set()
    if _active_ticker:
        mapped = {row[0] for row in query.get_mappings_for_ticker(_active_ticker)}

    for m in catalog:
        if _active_ticker:
            tag = "✓" if m.key in mapped else " "
            lines.append(f"  [{tag}] {m.key:<22} {m.display_name}")
        else:
            lines.append(f"      {m.key:<22} {m.display_name}")

    if _active_ticker:
        lines += ["", "✓ = mapped. Run 'map' to configure, or type a metric key to query."]
    else:
        lines += ["", "Select a ticker first to query or map these metrics."]
    return lines

# ── Mapping workflow ──

def _create_metric() -> Metric | None:
    """prompt for and create a new catalog metric."""
    key = _prompt_str("  New metric key (e.g. inventory)", required=True)
    assert key is not None
    key = key.strip().lower()
    if query.get_metric(key):
        print(f"  Metric '{key}' already exists.")
        return query.get_metric(key)

    display = _prompt_str("  Display name", required=True)
    fmt = _prompt_str(
        "  Format [currency/number/percentage/ratio/text]",
        default="currency",
    )
    if fmt not in ALLOWED_FORMATS:
        print(f"  Unknown format '{fmt}', using 'currency'.")
        fmt = "currency"

    query.add_metric(key, display, fmt)
    _refresh_catalog()
    print(f"  Created metric '{key}'.")
    return query.get_metric(key)

def _select_metric_for_mapping() -> Metric | None:
    """show the catalog and let the user pick a metric to map (or create one)."""
    catalog = _get_catalog()
    mapped = {row[0] for row in query.get_mappings_for_ticker(_active_ticker)}  # type: ignore

    print(f"\n── Map Concepts: {_active_ticker} ──")
    print("  Select a metric to map this company's reported concepts onto:\n")
    for i, m in enumerate(catalog, 1):
        tag = "✓" if m.key in mapped else " "
        print(f"    {i:>2}. [{tag}] {m.key:<22} {m.display_name}")
    print("\n  Enter a number or metric key, 'new' to add a metric, or Enter to cancel.")

    completer = WordCompleter(
        [m.key for m in catalog] + ["new"], ignore_case=True, sentence=True
    )
    val = form_session.prompt("  metric> ", completer=completer).strip()  # type: ignore
    if not val:
        return None
    if val.lower() == "new":
        return _create_metric()
    if val.isdigit() and 1 <= int(val) <= len(catalog):
        return catalog[int(val) - 1]
    match = next((m for m in catalog if m.key.lower() == val.lower()), None)
    if match:
        return match
    print(f"  Unknown metric: '{val}'.")
    return None

def _browse_and_select_concepts(ticker: str, metric: Metric) -> list[str]:
    """pick concepts to map, one at a time, with a metadata-rich fuzzy dropdown."""
    concepts = query.get_company_concepts(ticker)
    if not concepts:
        print("  This company has no reported concepts. Scrape it first.")
        return []

    all_qnames: list[str] = []
    meta: dict[str, str] = {}
    for qname, local_name, fact_count, latest_value in concepts:
        all_qnames.append(qname)
        meta[qname] = f"{local_name}  ·  {fact_count}×  ·  {_short_value(latest_value)}"

    completer = FuzzyWordCompleter(all_qnames, meta_dict=meta)
    qname_set = set(all_qnames)

    print(f"\n  Search concepts for {ticker} to feed '{metric.display_name}'.")

    picks: list[str] = []
    while True:
        label = f"  concept ({len(picks)} picked)> " if picks else "  concept> "
        val = form_session.prompt(  # type: ignore
            label, completer=completer, complete_while_typing=True
        ).strip()
        if not val:
            break
        if val in qname_set:
            match = val
        else:
            match = next((q for q in all_qnames if q.lower() == val.lower()), None)
        if match is None:
            print(f"  No concept matches '{val}'. Keep typing to search, or Enter to finish.")
            continue
        if match in picks:
            print(f"  '{match}' already selected.")
            continue
        picks.append(match)
        print(f"  + {match}   ({meta[match]})")

    return picks

def _map_metric(metric: Metric) -> list[str]:
    """view/add/remove mappings for one metric on the active ticker."""
    ticker = _active_ticker
    cik = query.get_cik_for_ticker(ticker)  # type: ignore
    if not cik:
        return [f"{ticker} is not in the database."]

    while True:
        existing = [
            (q, p)
            for k, _dn, q, p in query.get_mappings_for_ticker(ticker)  # type: ignore
            if k == metric.key
        ]
        print(f"\n── {metric.display_name}  ({metric.key})  —  {ticker} ──")
        if existing:
            print("  Current mappings (priority order):")
            for i, (q, p) in enumerate(existing, 1):
                print(f"    {i}. {q}   (priority {p})")
        else:
            print("  No mappings yet.")
        print("\n  'add' to map concepts, 'rm <n>' to remove, Enter to go back.")

        val = form_session.prompt("  map> ").strip().lower()  # type: ignore
        if not val:
            return [f"Mappings for '{metric.display_name}' on {ticker} saved."]

        if val == "add":
            picks = _browse_and_select_concepts(ticker, metric)  # type: ignore
            if not picks:
                continue
            base = max((p for _, p in existing), default=-1) + 1
            for offset, qname in enumerate(picks):
                query.add_metric_mapping(cik, metric.key, qname, base + offset)
            print(f"  Mapped {len(picks)} concept(s) onto '{metric.key}'.")
        elif val.startswith("rm"):
            arg = val[2:].strip()
            if arg.isdigit() and 1 <= int(arg) <= len(existing):
                qname = existing[int(arg) - 1][0]
                query.remove_metric_mapping(cik, metric.key, qname)
                print(f"  Removed {qname}.")
            else:
                print("  Usage: rm <number>")
        else:
            print("  Unknown. Use 'add', 'rm <n>', or Enter to go back.")

def _cmd_map(arg: str = "") -> list[str]:
    """entry point for the per-company concept-to-metric mapping workflow.

    `map <metric>` jumps straight into that metric; bare `map` shows the picker.
    """
    if not _active_ticker:
        return ["No ticker selected. Run 'ticker' first."]

    arg = arg.strip().lower()
    if arg:
        catalog = _catalog_map()
        metric = catalog.get(arg)
        if metric is None:
            matches = sorted(k for k in catalog if k.startswith(arg))
            if len(matches) == 1:
                metric = catalog[matches[0]]
            elif len(matches) > 1:
                return [f"Ambiguous metric: {', '.join(matches)}"]
            else:
                return [f"Unknown metric '{arg}'. Type 'metrics' to list them."]
        return _map_metric(metric)

    metric = _select_metric_for_mapping()
    if metric is None:
        return ["Mapping cancelled."]
    return _map_metric(metric)

def _cmd_help(arg: str = "") -> list[str]:
    lines = [
        "COMMANDS  (Tab completes commands and their arguments)",
        "  ticker [SYM]          - Select a ticker; 'ticker add' to scrape a new one",
        "  mode <a|q|all>        - Set period filter (annual/quarterly/all)",
        "  map [METRIC]          - Map reported concepts onto a metric",
        "  metrics               - List the metric catalog (✓ = mapped for this ticker)",
        "  help                  - Show this help",
        "  quit                  - Exit",
        "",
    ]
    if _active_ticker:
        mapped = {row[0] for row in query.get_mappings_for_ticker(_active_ticker)}
        ready = [m.key for m in _get_catalog() if m.key in mapped]
        lines += ["QUERIES (type a metric key directly)"]
        if ready:
            lines += ["  Mapped: " + ", ".join(ready)]
        else:
            lines += ["  Nothing mapped yet for this ticker — run 'map' first."]
        lines += ["  See all metric keys with 'metrics'."]
    else:
        lines += ["Run 'ticker' first to enable queries and mapping."]

    lines += ["", "KEYS: Ctrl+C exit | Ctrl+Z cancel"]
    return lines

BASE_COMMAND_MAP = {
    "ticker":  _cmd_ticker,
    "mode":    _cmd_mode,
    "map":     _cmd_map,
    "metrics": _cmd_metrics,
    "help":    _cmd_help,
}

def _resolve_command(token: str) -> tuple[str | None, str | None]:
    """
    resolve a command token to a canonical command/metric key via exact then
    unique-prefix match. returns (name, error): exactly one of the two is set,
    or both None when the token matches nothing.
    """
    token = token.lower()
    catalog = _catalog_map()
    if token in BASE_COMMAND_MAP or token in catalog:
        return token, None

    candidates = set(BASE_COMMAND_MAP)
    if _active_ticker:
        candidates |= set(catalog)

    matches = sorted(c for c in candidates if c.startswith(token))
    if len(matches) == 1:
        return matches[0], None
    if len(matches) > 1:
        return None, f"Ambiguous: {', '.join(matches)}"
    return None, None

def _process_command(raw: str) -> list[str]:
    raw = raw.strip()
    if not raw:
        return []

    parts = raw.split(maxsplit=1)
    token = parts[0].lower()
    arg = parts[1].strip() if len(parts) > 1 else ""

    if token in ("quit", "exit", "q"):
        raise KeyboardInterrupt

    name, err = _resolve_command(token)
    if err:
        return [err]
    if name is None:
        if not _active_ticker and any(k.startswith(token) for k in _catalog_map()):
            return ["No ticker selected. Run 'ticker' first."]
        return [f"Unknown: '{token}'. Type 'help'."]

    if name in BASE_COMMAND_MAP:
        return BASE_COMMAND_MAP[name](arg)

    # otherwise it's a metric key -> query
    if not _active_ticker:
        return ["No ticker selected. Run 'ticker' first."]
    return _cmd_query(_catalog_map()[name])

def _main():
    global cmd_session, form_session, parser

    cmd_session = PromptSession(key_bindings=kb)
    form_session = PromptSession(key_bindings=kb)

    print(_header_line())
    print("CHEESECLOTH - SEC Filing Explorer")
    print(_header_line())

    conn = get_connection()
    parser = open_parser(conn=conn)

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
                completer = _build_command_completer()

                raw = cmd_session.prompt(
                    "\n> ", completer=completer, complete_while_typing=True
                ).strip()
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
                raise
            except Exception as exc:
                logger.exception("Command failed")
                _reset_ui()
                _add_ui(f"Error: {exc}")
                _render()

    except (EOFError, KeyboardInterrupt):
        print("\n\nGoodbye!")
    finally:
        try:
            parser.__exit__(None, None, None)
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    _main()
