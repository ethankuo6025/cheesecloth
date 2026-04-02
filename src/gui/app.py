import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import logging
import threading
import uuid
from time import time
from flask import Flask, jsonify, request, render_template
from datetime import datetime, date, timedelta
from decimal import Decimal

from parser import SECFilingParser, TickerNotFoundError
from add import parse_and_store
from db import get_available_tickers, get_connection
from query import resolve, REGISTRY

logger = logging.getLogger(__name__)

app = Flask(__name__)

# ── Background job tracking ──

_jobs = {}
_jobs_lock = threading.Lock()
_JOB_TTL = 600


def _cleanup_jobs():
    now = time()
    with _jobs_lock:
        expired = [jid for jid, j in _jobs.items()
                   if j["status"] in ("complete", "error") and now - j["updated_at"] > _JOB_TTL]
        for jid in expired:
            del _jobs[jid]


def _scrape_worker(job_id, ticker):
    job = _jobs[job_id]
    conn = None
    try:
        job.update(progress=0.05, step="init",
                   message="Connecting to SEC EDGAR…", updated_at=time())

        conn = get_connection()

        with SECFilingParser(conn, max_retries=3, timeout=30.0) as parser:
            total_upserted = 0
            total_failed = 0

            phases = [
                ("10-K", 0.10, 0.48, "Scraping 10-K annual filings…"),
                ("10-Q", 0.52, 0.90, "Scraping 10-Q quarterly filings…"),
            ]

            for filing_type, start_pct, end_pct, msg in phases:
                job.update(progress=start_pct, step=filing_type,
                           message=msg, updated_at=time())

                upserted, failed = parse_and_store(
                    parser, ticker=ticker, filing_types=filing_type
                )
                total_upserted += upserted
                total_failed += failed

                job.update(progress=end_pct, updated_at=time())

            job.update(progress=0.95, step="finalize",
                       message="Finalizing…", updated_at=time())

            if total_upserted == 0 and total_failed == 0:
                existing = [t for t, _ in get_available_tickers() if t == ticker]
                if existing:
                    job.update(
                        status="complete", progress=1.0, step="done",
                        message=f"'{ticker}' is already up to date. Nothing new to scrape.",
                        result={"upserted": 0, "failed": 0, "already_current": True},
                        updated_at=time(),
                    )
                else:
                    job.update(
                        status="error", progress=1.0, step="done",
                        message=f"No filings found for '{ticker}'.",
                        updated_at=time(),
                    )
            else:
                job.update(
                    status="complete", progress=1.0, step="done",
                    message=f"Done — {total_upserted} facts stored, {total_failed} failed.",
                    result={"upserted": total_upserted, "failed": total_failed,
                            "already_current": False},
                    updated_at=time(),
                )

    except TickerNotFoundError:
        job.update(status="error", progress=1.0, step="done",
                   message=f"'{ticker}' not found in SEC EDGAR.",
                   updated_at=time())
    except Exception as exc:
        logger.exception("Scrape failed for %s", ticker)
        job.update(status="error", progress=1.0, step="done",
                   message=f"Scrape failed: {exc}", updated_at=time())
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ── Serialisation ──

def _ser(val):
    if val is None:
        return None
    if isinstance(val, (date, datetime)):
        return val.isoformat()
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, timedelta):
        return val.days
    return val


def _ser_row(row):
    return {
        "qname": row[0], "period_type": row[1], "value": _ser(row[2]),
        "instant_date": _ser(row[3]), "start_date": _ser(row[4]),
        "end_date": _ser(row[5]), "unit": row[6], "decimals": row[7],
        "accession": row[8],
    }


# ── Routes: pages ──

@app.route("/")
def index():
    return render_template("index.html")


# ── Routes: tickers ──

@app.route("/api/tickers", methods=["GET"])
def list_tickers():
    return jsonify([
        {"ticker": t, "updated_at": _ser(u)}
        for t, u in get_available_tickers()
    ])


# ── Routes: async scrape ──

@app.route("/api/scrape", methods=["POST"])
def start_scrape():
    _cleanup_jobs()

    data = request.get_json(force=True)
    ticker = data.get("ticker", "").strip().upper()
    if not ticker:
        return jsonify({"error": "Ticker is required."}), 400

    # Reject if already scraping this ticker
    with _jobs_lock:
        for jid, j in _jobs.items():
            if j["ticker"] == ticker and j["status"] == "running":
                return jsonify({
                    "error": f"A scrape for '{ticker}' is already running.",
                    "job_id": jid,
                }), 409

    job_id = uuid.uuid4().hex[:10]
    now = time()
    _jobs[job_id] = {
        "job_id": job_id,
        "ticker": ticker,
        "status": "running",
        "progress": 0.0,
        "step": "queued",
        "message": "Starting…",
        "result": None,
        "created_at": now,
        "updated_at": now,
    }

    t = threading.Thread(target=_scrape_worker, args=(job_id, ticker), daemon=True)
    t.start()

    return jsonify({"job_id": job_id, "ticker": ticker}), 202


@app.route("/api/scrape/<job_id>", methods=["GET"])
def scrape_status(job_id):
    job = _jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404
    return jsonify({
        "job_id":   job["job_id"],
        "ticker":   job["ticker"],
        "status":   job["status"],
        "progress": job["progress"],
        "step":     job["step"],
        "message":  job["message"],
        "result":   job["result"],
    })


@app.route("/api/scrape", methods=["GET"])
def list_active_scrapes():
    """Return any running jobs so the frontend can resume polling after refresh."""
    active = [
        {
            "job_id":   j["job_id"],
            "ticker":   j["ticker"],
            "status":   j["status"],
            "progress": j["progress"],
            "step":     j["step"],
            "message":  j["message"],
            "result":   j["result"],
        }
        for j in _jobs.values()
        if j["status"] == "running"
    ]
    return jsonify(active)


# ── Routes: facts ──

@app.route("/api/facts/<ticker>/<metric>", methods=["GET"])
def get_metric_facts(ticker, metric):
    ticker = ticker.upper()
    mode = request.args.get("mode", "annual")
    if metric not in REGISTRY:
        return jsonify({"error": f"Unknown metric: {metric}"}), 400
    try:
        raw = resolve(ticker, metric, mode)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    return jsonify({
        "ticker": ticker, "metric": metric, "mode": mode,
        "facts": [_ser_row(r) for r in raw],
    })


@app.route("/api/metrics", methods=["GET"])
def list_metrics():
    return jsonify({
        "revenue":            {"label": "Revenue",            "is_per_share": False, "is_count": False},
        "eps":                {"label": "Diluted EPS",        "is_per_share": True,  "is_count": False},
        "gross":              {"label": "Gross Profit",       "is_per_share": False, "is_count": False},
        "operating":          {"label": "Operating Income",   "is_per_share": False, "is_count": False},
        "net":                {"label": "Net Income",         "is_per_share": False, "is_count": False},
        "liabilities":        {"label": "Total Liabilities",  "is_per_share": False, "is_count": False},
        "total_assets":       {"label": "Total Assets",       "is_per_share": False, "is_count": False},
        "cash_on_hand":       {"label": "Cash on Hand",       "is_per_share": False, "is_count": False},
        "long_term_total_debt": {"label": "Long-Term Total Debt", "is_per_share": False, "is_count": False},
        "shares_outstanding": {"label": "Shares Outstanding", "is_per_share": False, "is_count": True},
    })


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
