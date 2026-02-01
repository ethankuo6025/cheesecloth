from dataclasses import dataclass, field
from datetime import date
from enum import Enum

import httpx
import rate_limiter
from arelle.api.Session import Session
from arelle.RuntimeOptions import RuntimeOptions
from personal_header import header

headers = header()

class PeriodType(Enum):
    INSTANT = "instant"
    DURATION = "duration"
    FOREVER = "forever"

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
    precision: int | None = None
    dimensions: dict[str, str] = field(default_factory=dict)

class SECFilingParserError(Exception):
    pass

class TickerNotFoundError(SECFilingParserError):
    """thrown by _get_cik() on invalid tickers."""
    pass

class FilingFetchError(SECFilingParserError):
    """thrown by _fetch-json on HTTP issues."""
    pass

class SECFilingParser:
    """parses XBRL facts from SEC EDGAR 10-K filings."""

    def __init__(self):
        self._ticker_to_cik: dict[str, str] | None = None

    def _fetch_json(self, url: str) -> dict:
        """fetch JSON from a URL with rate limiting."""
        rate_limiter.wait()
        try:
            with httpx.Client(timeout=30.0, headers=headers, follow_redirects=True) as client:
                r = client.get(url)
                r.raise_for_status()
                return r.json()
        except httpx.HTTPError as e:
            raise FilingFetchError(f"Failed to fetch JSON from {url}: {e}") from e

    def _get_ticker_to_cik(self) -> dict[str, str]:
        """fetch and cache ticker-to-cik mapping."""
        if self._ticker_to_cik is not None:
            return self._ticker_to_cik

        tickers_json = self._fetch_json("https://www.sec.gov/files/company_tickers.json")
        self._ticker_to_cik = {ticker["ticker"]: str(ticker["cik_str"]).zfill(10) for ticker in tickers_json.values()}
        return self._ticker_to_cik

    def _get_cik(self, ticker: str) -> str:
        """Get CIK for a ticker."""
        mapping = self._get_ticker_to_cik()
        t = ticker.upper()
        if t not in mapping:
            raise TickerNotFoundError(f"Ticker '{ticker}' not found")
        return mapping[t]

    def _get_10k_filings(self, cik: str, max_filings: int | None = None) -> list[tuple[str, str]]:
        """get list of (accession_number, filename) for 10-K filings."""

        url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        meta = self._fetch_json(url)

        try:
            recent = meta["filings"]["recent"]
            acc = recent["accessionNumber"]
            docs = recent["primaryDocument"]
            descs = recent["primaryDocDescription"]
        except KeyError as e:
            raise SECFilingParserError(f"Unexpected metadata structure, missing key: {e}") from e

        if not (isinstance(acc, list) and isinstance(docs, list) and isinstance(descs, list)):
            raise SECFilingParserError("Unexpected metadata structure: filings.recent fields are not lists")

        if not (len(acc) == len(docs) == len(descs)):
            raise SECFilingParserError(
                f"Mismatched array lengths: acc={len(acc)}, docs={len(docs)}, descs={len(descs)}"
            )

        ten_ks = [(a, d) for a, d, desc in zip(acc, docs, descs) if desc == "10-K"]
        return ten_ks[:max_filings] if max_filings else ten_ks

    def _parse_fact(self, fact, ticker: str, cik: str, accession_number: str) -> ParsedFact:
        """Parse a single Arelle fact into a ParsedFact. Raises SECFilingParserError on problems."""

        # QName
        concept = getattr(fact, "concept", None)
        qname = getattr(fact, "qname", None) or (concept.qname if concept else None)
        if qname is None:
            raise SECFilingParserError("Fact has no qname")

        qname_str = str(qname)
        namespace = str(qname.namespaceURI) if hasattr(qname, "namespaceURI") else ""
        local_name = str(qname.localName) if hasattr(qname, "localName") else qname_str

        # value
        value = None if getattr(fact, "isNil", False) else getattr(fact, "value", None)

        # decimals or precision
        decimals = None
        precision = None

        raw_dec = getattr(fact, "decimals", None)
        if raw_dec is not None and raw_dec != "INF":
            try:
                decimals = int(raw_dec)
            except (ValueError, TypeError):
                pass

        raw_prec = getattr(fact, "precision", None)
        if raw_prec is not None and raw_prec != "INF":
            try:
                precision = int(raw_prec)
            except (ValueError, TypeError):
                pass

        # unitRef
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

        # --- context: period + dimensions ---
        ctx = getattr(fact, "context", None)
        if ctx is None:
            raise SECFilingParserError(f"Fact has no context: {qname_str}")

        period_type = PeriodType.INSTANT
        instant_date = None
        start_date = None
        end_date = None

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
        elif getattr(ctx, "isForeverPeriod", False):
            period_type = PeriodType.FOREVER
        else:
            # fallback: try to infer
            dt = getattr(ctx, "instantDatetime", None)
            if dt:
                period_type = PeriodType.INSTANT
                instant_date = dt.date()
            else:
                sd = getattr(ctx, "startDatetime", None)
                ed = getattr(ctx, "endDatetime", None)
                if sd:
                    period_type = PeriodType.DURATION
                    start_date = sd.date()
                    if ed:
                        end_date = ed.date()

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
            precision=precision,
            dimensions=dimensions,
        )

    def parse_10k_filings(
        self,
        ticker: str,
        max_filings: int | None = None,
        arelle_plugins: str = "ixbrl-viewer",
        verbose: bool = True,
    ) -> list[ParsedFact]:
        """
        Main entry point. Parses 10-K filings for a ticker and returns all XBRL facts.

        Raises:
          - TickerNotFoundError: invalid ticker
          - FilingFetchError: network/HTTP failures fetching SEC JSON
          - SECFilingParserError: unexpected SEC schema or parsing errors
        """
        ticker = ticker.upper()
        cik = self._get_cik(ticker)
        filings = self._get_10k_filings(cik, max_filings)

        if not filings:
            if verbose:
                print(f"No 10-K filings found for {ticker}")
            return []

        all_facts: list[ParsedFact] = []

        for i, (acc_num, filename) in enumerate(filings):
            acc_nd = acc_num.replace("-", "")
            url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nd}/{filename}"
            if verbose:
                print(f"[{i+1}/{len(filings)}] {acc_num}")

            options = RuntimeOptions(
                entrypointFile=url,
                internetConnectivity="online",
                keepOpen=True,
                logFile="logToStructuredMessage",
                logFormat="[%(messageCode)s] %(message)s - %(file)s",
                plugins=arelle_plugins,
            )

            try:
                with Session() as session:
                    session.run(options)
                    models = session.get_models()
                    if not models:
                        raise SECFilingParserError(f"No models loaded from {url}")

                    facts = list(models[0].factsInInstance)
                    count = 0
                    for fact in facts:
                        try:
                            all_facts.append(self._parse_fact(fact, ticker, cik, acc_num))
                            count += 1
                        except SECFilingParserError as e:
                            if verbose:
                                print(f"  skip fact: {e}")

                    if verbose:
                        print(f"  {count} facts extracted")

            except SECFilingParserError:
                raise
            except Exception as e:
                raise SECFilingParserError(f"Error parsing {url}: {e}") from e

        return all_facts

if __name__ == "__main__":
    parser = SECFilingParser()
    facts = parser.parse_10k_filings("NVDA", max_filings=1, verbose=True)