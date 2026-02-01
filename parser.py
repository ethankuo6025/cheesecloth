from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import Any

import httpx
import logging

import rate_limiter
from arelle.api.Session import Session
from arelle.RuntimeOptions import RuntimeOptions
from personal_header import header
logger = logging.getLogger(__name__)


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
    precision: int | None = None
    dimensions: dict[str, str] = field(default_factory=dict)

class SECFilingParserError(Exception):
    pass

class TickerNotFoundError(SECFilingParserError):
    pass

class FilingFetchError(SECFilingParserError):
    pass

class SECFilingParser:
    def __init__(
        self,
        max_retries: int = 3,
        timeout: float = 30.0,
        headers: dict[str, str] | None = None,
    ):
        self._ticker_to_cik: dict[str, str] | None = None
        self._client = httpx.Client(
            timeout=timeout,
            headers=header(),
            follow_redirects=True,
            transport=httpx.HTTPTransport(retries=max_retries),
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "SECFilingParser":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def _get_json(self, url: str) -> Any:
        rate_limiter.wait()
        try:
            r = self._client.get(url)
            r.raise_for_status()
            return r.json()
        except httpx.HTTPError as e:
            raise FilingFetchError(f"Request failed for {url}: {e}") from e

    def _get_ticker_to_cik(self) -> dict[str, str]:
        if self._ticker_to_cik is not None:
            return self._ticker_to_cik

        data = self._get_json("https://www.sec.gov/files/company_tickers.json")
        if not isinstance(data, dict):
            raise SECFilingParserError("Unexpected ticker-to-cik payload")

        self._ticker_to_cik = {
            e["ticker"].upper(): str(e["cik_str"]).zfill(10)
            for e in data.values()
            if isinstance(e, dict) and "ticker" in e and "cik_str" in e
        }
        return self._ticker_to_cik

    def _get_cik(self, ticker: str) -> str:
        mapping = self._get_ticker_to_cik()
        t = ticker.upper()
        if t not in mapping:
            raise TickerNotFoundError(f"Ticker '{ticker}' not found")
        return mapping[t]

    def _get_filings(
        self,
        cik: str,
        filing_types: set[str],
        max_filings: int | None = None,
    ) -> list[tuple[str, str, str]]:
        meta = self._get_json(f"https://data.sec.gov/submissions/CIK{cik}.json")

        try:
            recent = meta["filings"]["recent"]
            acc = recent["accessionNumber"]
            docs = recent["primaryDocument"]
            forms = recent["form"]
        except Exception as e:
            raise SECFilingParserError(f"Unexpected metadata structure: {e}") from e

        if not (isinstance(acc, list) and isinstance(docs, list) and isinstance(forms, list)):
            raise SECFilingParserError("Unexpected metadata structure: recent fields are not lists")

        if not (len(acc) == len(docs) == len(forms)):
            raise SECFilingParserError(
                f"Mismatched array lengths: acc={len(acc)}, docs={len(docs)}, forms={len(forms)}"
            )

        filings = [(a, d, f) for a, d, f in zip(acc, docs, forms) if f in filing_types]
        return filings[:max_filings] if max_filings else filings

    def _extract_qname(self, fact) -> tuple[str, str, str]:
        concept = getattr(fact, "concept", None)
        qname = getattr(fact, "qname", None) or (concept.qname if concept else None)
        if qname is None:
            raise SECFilingParserError("Fact has no qname")

        qname_str = str(qname)
        namespace = str(qname.namespaceURI) if hasattr(qname, "namespaceURI") else ""
        local_name = str(qname.localName) if hasattr(qname, "localName") else qname_str
        return qname_str, namespace, local_name

    def _extract_value(self, fact) -> str | None:
        return None if getattr(fact, "isNil", False) else getattr(fact, "value", None)

    def _extract_decimals_precision(self, fact) -> tuple[int | None, int | None]:
        decimals: int | None = None
        precision: int | None = None

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

        return decimals, precision

    def _extract_unit(self, fact) -> str | None:
        unit_obj = getattr(fact, "unit", None)
        if unit_obj is None:
            return None

        try:
            if hasattr(unit_obj, "measures") and unit_obj.measures and len(unit_obj.measures) == 2:
                nums, dens = unit_obj.measures
                num_s = "*".join(str(m) for m in nums) if nums else ""
                den_s = "*".join(str(m) for m in dens) if dens else ""
                return f"{num_s}/{den_s}" if den_s else (num_s or None)
            return str(unit_obj.id) if hasattr(unit_obj, "id") else str(unit_obj)
        except Exception:
            return str(unit_obj)

    def _extract_period(self, ctx) -> tuple[PeriodType, date | None, date | None, date | None]:
        period_type = PeriodType.INSTANT
        instant_date: date | None = None
        start_date: date | None = None
        end_date: date | None = None

        if getattr(ctx, "isInstantPeriod", False):
            dt = getattr(ctx, "instantDatetime", None)
            if dt:
                instant_date = dt.date()
            period_type = PeriodType.INSTANT
        elif getattr(ctx, "isStartEndPeriod", False):
            sd = getattr(ctx, "startDatetime", None)
            ed = getattr(ctx, "endDatetime", None)
            if sd:
                start_date = sd.date()
            if ed:
                end_date = ed.date()
            period_type = PeriodType.DURATION

        return period_type, instant_date, start_date, end_date

    def _extract_dimensions(self, ctx) -> dict[str, str]:
        dimensions: dict[str, str] = {}
        if not hasattr(ctx, "qnameDims"):
            return dimensions

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
        qname_str, namespace, local_name = self._extract_qname(fact)
        value = self._extract_value(fact)
        decimals, precision = self._extract_decimals_precision(fact)
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
            precision=precision,
            dimensions=dimensions,
        )

    def parse_filings(
        self,
        ticker: str,
        filing_types: str | set[str] = "10-K",
        max_filings: int | None = None,
        arelle_plugins: str = "ixbrl-viewer",
    ) -> list[ParsedFact]:
        if isinstance(filing_types, str):
            filing_types = {filing_types}

        ticker = ticker.upper()
        cik = self._get_cik(ticker)
        filings = self._get_filings(cik, filing_types, max_filings)

        if not filings:
            logger.info("No %s filings found for %s", filing_types, ticker)
            return []

        all_facts: list[ParsedFact] = []

        for i, (acc_num, filename, form_type) in enumerate(filings):
            acc_nd = acc_num.replace("-", "")
            url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nd}/{filename}"
            logger.info("[%d/%d] %s %s", i + 1, len(filings), form_type, acc_num)

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
                            logger.debug("skip fact: %s", e)

                    logger.info("%d facts extracted", count)

            except SECFilingParserError:
                raise
            except Exception as e:
                raise SECFilingParserError(f"Error parsing {url}: {e}") from e

        return all_facts


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    with SECFilingParser(max_retries=3, timeout=30.0) as parser:
        facts = parser.parse_filings("NVDA", filing_types="10-K", max_filings=1)
        print("Total parsed facts:", len(facts))
