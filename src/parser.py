from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import Any, cast
import httpx
import os
from dotenv import load_dotenv
from psycopg import Connection
import rate_limiter
from arelle.api.Session import Session
from arelle.RuntimeOptions import RuntimeOptions
from personal_header import header
import logging
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


logger = logging.getLogger(__name__)
load_dotenv()

ARELLE_PLUGINS_PATH = os.getenv("ARELLE_PLUGINS_PATH")

class PeriodType(Enum):
    INSTANT = "instant"
    DURATION = "duration"

@dataclass
class ParsedFact:
    ticker: str
    cik: str
    accession_number: str
    qname: str
    namespace: str
    local_name: str
    period_type: PeriodType
    value: str | None = None
    instant_date: date | None = None
    start_date: date | None = None
    end_date: date | None = None
    unit: str | None = None
    decimals: int | None = None
    dimensions: dict[str, str] = field(default_factory=dict)

@dataclass
class Filing:
    cik: str
    accession_number: str
    entry_file: str
    filing_type: str

def _is_quantitative(parsed: ParsedFact) -> bool:
    """return True only if the fact has a unit and a numeric value."""
    if parsed.unit is None:
        return False
    if parsed.value is None:
        return False
    try:
        float(parsed.value.replace(",", ""))
    except (ValueError, AttributeError):
        return False    
    return True

class SECFilingParserError(Exception):
    pass

class TickerNotFoundError(SECFilingParserError):
    """thrown by _get_cik() on invalid tickers."""
    pass

class FilingFetchError(SECFilingParserError):
    """thrown by _get_json() on http issues."""
    pass

class SECFilingParser:
    """parses xbrl facts from sec edgar filings (defaults to 10-ks)."""
    def __init__(
        self,
        conn: Connection,
        max_retries: int = 3,
        timeout: float = 30.0,
        headers: dict[str, str] | None = None,
    ):
        self._conn = conn
        self._ticker_to_cik: dict[str, str] | None = None
        self._client = httpx.Client(
            timeout=timeout,
            headers= headers or header(),
            follow_redirects=True,
            transport=httpx.HTTPTransport(retries=max_retries),
        )
        self._options: RuntimeOptions = RuntimeOptions(
            entrypointFile=None,
            internetConnectivity="online",
            keepOpen=True,
            logFile="logToStructuredMessage",
            logFormat="[%(messageCode)s] %(message)s - %(file)s",
            logLevel="WARNING",
            plugins=f"{ARELLE_PLUGINS_PATH}|rate_limiter.py"
        )

    def close(self) -> None:
        self._client.close()
    
    # needed for the with SECFilingParser() as parser
    def __enter__(self) -> "SECFilingParser":
        self._session = Session().__enter__()
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def _get_json(self, url: str) -> Any:
        """get json from a url with rate limiting."""
        rate_limiter.wait(url)
        try:
            r = self._client.get(url)
            r.raise_for_status()
            return r.json()
        except httpx.HTTPError as e:
            raise FilingFetchError(f"Request failed for {url}: {e}") from e

    def _get_ticker_to_cik(self) -> dict[str, str]:
        """fetch and cache ticker-to-cik mapping."""
        if self._ticker_to_cik is not None:
            return self._ticker_to_cik

        data = self._get_json("https://www.sec.gov/files/company_tickers.json")
        if not isinstance(data, dict):
            raise SECFilingParserError("Unexpected ticker-to-cik payload.")

        self._ticker_to_cik = {
            e["ticker"].upper(): str(e["cik_str"]).zfill(10)
            for e in data.values()
            if isinstance(e, dict) and "ticker" in e and "cik_str" in e
        }
        return self._ticker_to_cik

    def _get_cik(self, ticker: str) -> str:
        """get cik for a ticker."""
        mapping = self._get_ticker_to_cik()
        t = ticker.upper()
        if t not in mapping:
            raise TickerNotFoundError(f"Ticker '{ticker}' not found")
        return mapping[t]
    
    def _get_entry_url(self, cik: str, accession_number: str) -> str:
        base = f"https://www.sec.gov/Archives/edgar/data/{cik.lstrip("0")}/{accession_number.replace("-", "")}/"
        idx = self._get_json(base + "index.json")
        items = idx.get("directory", {}).get("item", [])
        names = [it.get("name") for it in items if isinstance(it, dict) and it.get("name")]
        names = cast(list[str], names)

        xmls = [n for n in names if n.lower().endswith(".xml") or n.lower().endswith(".xbrl")]
        candidates = []
        for n in xmls:
            nl = n.lower()
            if any(s in nl for s in ("_cal", "_def", "_lab", "_pre", "schema", "summary")):
                continue
            candidates.append(n)

        for n in candidates:
            url = base + n
            r = self._client.get(url, headers={"Range": "bytes=0-65535"})
            t = r.text.lower()
            if "<xbrl" in t:
                return n

        return ""
    
    def _get_filings(
        self,
        cik: str,
        filing_types: set[str],
        max_filings: int | None = None,
    ) -> list[Filing]:
        """get list of filings for specified filing types, excluding already-scanned ones."""
        meta = self._get_json(f"https://data.sec.gov/submissions/CIK{cik}.json")

        try:
            recent = meta["filings"]["recent"]
        except KeyError as e:
            raise SECFilingParserError(f"Unexpected metadata structure/key: {e}") from e
        forms = recent["form"]
        acc = recent["accessionNumber"]
        docs = recent["primaryDocument"]
        is_ixbrl = recent["isInlineXBRL"]

        for i in range(len(docs)):
            if is_ixbrl[i] != 1 and forms[i] in filing_types:
                docs[i] = self._get_entry_url(cik=cik, accession_number=acc[i])

        if not (isinstance(acc, list) and isinstance(docs, list) and isinstance(forms, list)):
            raise SECFilingParserError("Unexpected metadata structure: filings.recent fields are not lists")

        if not (len(acc) == len(docs) == len(forms)):
            raise SECFilingParserError(
                f"mismatched array lengths: acc={len(acc)}, docs={len(docs)}, forms={len(forms)}"
            )
        
        filings = [Filing
                    (
                        cik=cik, 
                        accession_number=a, 
                        entry_file=d, 
                        filing_type=f, 
                    ) 
                    for a, d, f in zip(acc, docs, forms)
                    if d != "" and f in filing_types]
        
        matching_count = len(filings)
        types_str = ", ".join(sorted(filing_types))

        if matching_count == 0:
            logger.info(" No %s filings exist for CIK %s", types_str, cik)
            return []

        
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT accession_number FROM filings WHERE cik = %s AND accession_number = ANY(%s)",
                (cik, acc),
            )
            already_exists = {row[0] for row in cur.fetchall()}
            filings = [f for f in filings if f.accession_number not in already_exists]

        unprocessed_count = len(filings)
        if unprocessed_count == 0:
            logger.info(
                " All %d %s filing(s) for CIK %s already stored; nothing new to process",
                matching_count, types_str, cik,
            )
        else:
            logger.info(
                " %d of %d %s filing(s) for CIK %s are new",
                unprocessed_count, matching_count, types_str, cik,
            )

        return filings[:max_filings] if max_filings and max_filings < len(filings) else filings


    def _extract_qname(self, fact) -> tuple[str, str, str]:
        concept = getattr(fact, "concept", None)
        qname = getattr(fact, "qname", None) or (concept.qname if concept else None)
        if qname is None:
            raise SECFilingParserError("Fact has no QName.")

        qname_str = str(qname)
        namespace = str(qname.namespaceURI) if hasattr(qname, "namespaceURI") else ""
        local_name = str(qname.localName) if hasattr(qname, "localName") else qname_str
        return qname_str, namespace, local_name

    def _extract_value(self, fact) -> str | None:
        return None if getattr(fact, "isNil", False) else getattr(fact, "value", None)

    def _extract_decimals(self, fact) -> int | None:
        decimals = None
        raw_dec = getattr(fact, "decimals", None)
        if raw_dec is not None and raw_dec != "INF":
            try:
                decimals = int(raw_dec)
            except (ValueError, TypeError):
                pass

        return decimals

    def _extract_unit(self, fact) -> str | None:
        unit_str = None
        unit_obj = getattr(fact, "unit", None)
        if unit_obj is not None:
            try:
                if hasattr(unit_obj, "measures") and unit_obj.measures and len(unit_obj.measures) == 2:
                    nums, dens = unit_obj.measures
                    num_s = "*".join(str(m) for m in nums) if nums else ""
                    den_s = "*".join(str(m) for m in dens) if dens else ""
                    unit_str = f"{num_s}/{den_s}" if den_s else (num_s or None)
                else:
                    unit_str = str(unit_obj.id) if hasattr(unit_obj, "id") else str(unit_obj)
            except Exception:
                unit_str = str(unit_obj)
        return unit_str

    def _extract_period(self, ctx) -> tuple[PeriodType, date | None, date | None, date | None]:
        period_type = PeriodType.INSTANT
        instant_date: date | None = None
        start_date: date | None = None
        end_date: date | None = None

        if getattr(ctx, "isInstantPeriod", False):
            period_type = PeriodType.INSTANT
            dt = getattr(ctx, "instantDatetime", None)
            if dt:
                instant_date = dt.date()
        elif getattr(ctx, "isStartEndPeriod", False):
            period_type = PeriodType.DURATION
            sd = getattr(ctx, "startDatetime", None)
            ed = getattr(ctx, "endDatetime", None)
            if sd:
                start_date = sd.date()
            if ed:
                end_date = ed.date()

        return period_type, instant_date, start_date, end_date

    def _extract_dimensions(self, ctx) -> dict[str, str]:
        dimensions: dict[str, str] = {}
        if hasattr(ctx, "qnameDims"):
            for dim_q, dim_v in ctx.qnameDims.items():
                k = str(dim_q)
                if hasattr(dim_v, "memberQname") and dim_v.memberQname:
                    dimensions[k] = str(dim_v.memberQname)
                elif hasattr(dim_v, "typedMember") and dim_v.typedMember is not None:
                    tm = dim_v.typedMember
                    dimensions[k] = str(tm.text) if hasattr(tm, "text") else str(tm)
                else:
                    dimensions[k] = str(dim_v)
        return dimensions

    def _parse_fact(self, fact, ticker: str, cik: str, accession_number: str) -> ParsedFact:
        """parse a single arelle fact into a parsedfact."""
        qname_str, namespace, local_name = self._extract_qname(fact)
        value = self._extract_value(fact)
        decimals = self._extract_decimals(fact)
        unit_str = self._extract_unit(fact)

        ctx = getattr(fact, "context", None)
        if ctx is None:
            raise SECFilingParserError(f"Fact has no context: {qname_str}")

        period_type, instant_date, start_date, end_date = self._extract_period(ctx)
        dimensions = self._extract_dimensions(ctx)

        return ParsedFact(
            ticker=ticker,
            cik=cik,
            accession_number=accession_number,
            qname=qname_str,
            namespace=namespace,
            local_name=local_name,
            value=value,
            period_type=period_type,
            instant_date=instant_date,
            start_date=start_date,
            end_date=end_date,
            unit=unit_str,
            decimals=decimals,
            dimensions=dimensions,
        )

    def get_filings_to_parse(
        self,
        ticker: str,
        filing_types: str | set[str] = "10-K",
        max_filings: int | None = None,
    ) -> tuple[str, list[Filing]]:
        """get list of filings that need to be parsed (already filtered for unscanned filings)."""
        if isinstance(filing_types, str):
            filing_types = {filing_types}

        ticker = ticker.upper()
        cik = self._get_cik(ticker)
        filings = self._get_filings(cik, filing_types, max_filings)
        return cik, filings

    def parse_filing(
        self,
        filing: Filing,
        ticker: str,
        cik: str,
    ) -> list[ParsedFact]:
        """parse a single filing and return its facts."""
        accession_number = filing.accession_number
        filename = filing.entry_file
        accession_number_nd = accession_number.replace("-", "")
        url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_number_nd}/{filename}"
        
        self._options.entrypointFile = url

        try:
            with Session() as session:
                session.run(self._options)
                models = session.get_models()
                if not models:
                    raise SECFilingParserError(f"No models loaded from {url}")

            facts = list(models[0].factsInInstance)
            parsed_facts: list[ParsedFact] = []
            for fact in facts:
                try:
                    parsed = self._parse_fact(fact, ticker, cik, accession_number)
                    if _is_quantitative(parsed):
                        parsed_facts.append(parsed)
                except SECFilingParserError as e:
                    logger.debug("skip fact: %s", e)

            return parsed_facts

        except SECFilingParserError:
            raise
        except Exception as e:
            raise SECFilingParserError(f"Error parsing {url}: {e}") from e
