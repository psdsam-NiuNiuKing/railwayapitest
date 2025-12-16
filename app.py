from __future__ import annotations

import logging
import os
from datetime import date, datetime
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from massive_bench import (
    bench_contracts_all_contracts,
    bench_daily_close,
    bench_quote_at_timestamp,
    bench_trades,
    bench_snapshot_options,
    bench_ws_trades,
)

load_dotenv()


def _get_env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        return int(raw)
    except Exception:
        return default


def _get_env_str(name: str, default: str = "") -> str:
    v = os.environ.get(name)
    return v if v is not None else default


def _to_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


LOG_LEVEL = _get_env_str("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
LOGGER = logging.getLogger("railwayapitest")

app = FastAPI(title="Massive API Railway Bench", version="0.1.0")


def _require_api_key() -> str:
    api_key = _get_env_str("MASSIVE_API_KEY", "")
    if not api_key:
        raise HTTPException(status_code=500, detail="MASSIVE_API_KEY is not set")
    return api_key


@app.get("/")
def root() -> dict[str, Any]:
    return {
        "service": "railwayapitest",
        "endpoints": [
            "GET /health",
            "POST /bench/daily_close",
            "POST /bench/contracts",
            "POST /bench/trades",
            "POST /bench/quote_at",
        ],
        "notes": {
            "set_env": ["MASSIVE_API_KEY", "MAX_CONCURRENT", "HTTP_TIMEOUT_TOTAL", "LOG_LEVEL"],
            "trace": "Set MASSIVE_BENCH_TRACE=true to log per-request lines (can be noisy).",
        },
    }


@app.get("/health")
def health() -> dict[str, Any]:
    return {
        "ok": True,
        "has_api_key": bool(_get_env_str("MASSIVE_API_KEY", "")),
        "max_concurrent_default": _get_env_int("MAX_CONCURRENT", 250),
        "timeout_total_default": _get_env_int("HTTP_TIMEOUT_TOTAL", 60),
        "log_level": LOG_LEVEL,
    }


class DailyCloseRequest(BaseModel):
    ticker: str = Field(..., examples=["SPY"])
    date: str = Field(..., description="YYYY-MM-DD", examples=["2025-11-24"])
    requests: int = Field(10, ge=1, le=500)
    max_concurrent: int | None = Field(None, ge=1, le=2000)
    timeout_total_s: int | None = Field(None, ge=10, le=600)


@app.post("/bench/daily_close")
async def bench_daily_close_route(req: DailyCloseRequest) -> dict[str, Any]:
    api_key = _require_api_key()
    max_concurrent = req.max_concurrent or _get_env_int("MAX_CONCURRENT", 250)
    timeout_total_s = req.timeout_total_s or _get_env_int("HTTP_TIMEOUT_TOTAL", 60)

    LOGGER.info(
        "bench daily_close ticker=%s date=%s requests=%s max_concurrent=%s timeout=%s",
        req.ticker,
        req.date,
        req.requests,
        max_concurrent,
        timeout_total_s,
    )

    return await bench_daily_close(
        ticker=req.ticker.strip().upper(),
        session_date=_to_date(req.date),
        api_key=api_key,
        requests=req.requests,
        max_concurrent=max_concurrent,
        timeout_total_s=timeout_total_s,
    )


class ContractsRequest(BaseModel):
    tickers: list[str] = Field(..., examples=[["SPY", "QQQ", "AAPL"]])
    as_of: str = Field(..., description="YYYY-MM-DD", examples=["2025-11-24"])
    limit: int = Field(250, ge=10, le=1000)
    expiration_horizon_days: int = Field(365, ge=7, le=730)
    include_expired: bool = Field(
        False,
        description="When true, queries both expired=false and expired=true and unions results. Usually not needed when expiration_date.gte=as_of is set.",
    )
    auto_spot_hint: bool = Field(
        True,
        description="When true, fetches the underlying daily close and uses it to apply strike_price band filters (reduces contract pages).",
    )
    strike_low_mult: float = Field(0.7, ge=0.01, le=10.0)
    strike_high_mult: float = Field(1.3, ge=0.01, le=10.0)
    include_sample_option_tickers: bool = Field(
        False,
        description="When true, returns a small sample of option tickers per underlying (useful for /bench/trades and /bench/quote_at)",
    )
    sample_limit: int = Field(10, ge=1, le=50)
    max_concurrent: int | None = Field(None, ge=1, le=2000)
    timeout_total_s: int | None = Field(None, ge=10, le=900)


@app.post("/bench/contracts")
async def bench_contracts_route(req: ContractsRequest) -> dict[str, Any]:
    api_key = _require_api_key()
    max_concurrent = req.max_concurrent or _get_env_int("MAX_CONCURRENT", 250)
    timeout_total_s = req.timeout_total_s or _get_env_int("HTTP_TIMEOUT_TOTAL", 120)

    tickers = [t.strip().upper() for t in req.tickers if t and t.strip()]
    if not tickers:
        raise HTTPException(status_code=400, detail="tickers is empty")

    LOGGER.info(
        "bench contracts tickers=%s as_of=%s max_concurrent=%s timeout=%s",
        ",".join(tickers[:25]),
        req.as_of,
        max_concurrent,
        timeout_total_s,
    )

    return await bench_contracts_all_contracts(
        tickers=tickers,
        as_of=_to_date(req.as_of),
        api_key=api_key,
        limit=req.limit,
        expiration_horizon_days=req.expiration_horizon_days,
        include_expired=bool(req.include_expired),
        auto_spot_hint=bool(req.auto_spot_hint),
        strike_band=(float(req.strike_low_mult), float(req.strike_high_mult)),
        include_sample_option_tickers=bool(req.include_sample_option_tickers),
        sample_limit=int(req.sample_limit),
        max_concurrent=max_concurrent,
        timeout_total_s=timeout_total_s,
    )

@app.get("/bench/snapshot")
async def bench_snapshot(
    ticker: str = Query("SPY"),
    as_of: str = Query(date.today().isoformat()),
    limit: int = Query(1000, ge=1, le=1000),
    max_pages: int | None = Query(None, ge=1, le=500),
    max_concurrent: int = Query(50, ge=1, le=2000),
    timeout_total_s: int = Query(120, ge=10, le=600),
):
    api_key = _require_api_key()
    return await bench_snapshot_options(
        ticker=ticker.strip().upper(),
        as_of=_to_date(as_of),
        api_key=api_key,
        limit=limit,
        max_pages=max_pages,
        max_concurrent=max_concurrent,
        timeout_total_s=timeout_total_s,
    )

@app.get("/bench/ws")
def bench_ws(
    duration_s: float = Query(5.0, ge=0.5, le=30.0),
    max_messages: int = Query(5000, ge=1, le=500000),
    websocket_url: str = Query("wss://socket.massive.com/options"),
    subscribe: str = Query("T.*"),
):
    api_key = _require_api_key()
    return bench_ws_trades(
        api_key=api_key,
        websocket_url=websocket_url,
        subscribe_params=subscribe,
        duration_s=duration_s,
        max_messages=max_messages,
    )


class TradesRequest(BaseModel):
    option_tickers: list[str] = Field(..., examples=[["O:SPY251219C00600000"]])
    date: str = Field(..., description="YYYY-MM-DD", examples=["2025-11-24"])
    max_pages_per_contract: int = Field(2, ge=1, le=20)
    max_concurrent: int | None = Field(None, ge=1, le=2000)
    timeout_total_s: int | None = Field(None, ge=10, le=900)


@app.post("/bench/trades")
async def bench_trades_route(req: TradesRequest) -> dict[str, Any]:
    api_key = _require_api_key()
    max_concurrent = req.max_concurrent or _get_env_int("MAX_CONCURRENT", 250)
    timeout_total_s = req.timeout_total_s or _get_env_int("HTTP_TIMEOUT_TOTAL", 180)

    ots = [t.strip() for t in req.option_tickers if t and t.strip()]
    if not ots:
        raise HTTPException(status_code=400, detail="option_tickers is empty")

    LOGGER.info(
        "bench trades contracts=%s date=%s max_concurrent=%s timeout=%s pages_limit=%s",
        len(ots),
        req.date,
        max_concurrent,
        timeout_total_s,
        req.max_pages_per_contract,
    )

    return await bench_trades(
        option_tickers=ots,
        session_date=_to_date(req.date),
        api_key=api_key,
        max_concurrent=max_concurrent,
        timeout_total_s=timeout_total_s,
        max_pages_per_contract=req.max_pages_per_contract,
    )


class QuoteAtRequest(BaseModel):
    option_ticker: str = Field(..., examples=["O:SPY251219C00600000"])
    timestamp_ns: int = Field(..., description="Nanoseconds since epoch")
    requests: int = Field(25, ge=1, le=1000)
    max_concurrent: int | None = Field(None, ge=1, le=2000)
    timeout_total_s: int | None = Field(None, ge=10, le=600)


@app.post("/bench/quote_at")
async def bench_quote_at_route(req: QuoteAtRequest) -> dict[str, Any]:
    api_key = _require_api_key()
    max_concurrent = req.max_concurrent or _get_env_int("MAX_CONCURRENT", 250)
    timeout_total_s = req.timeout_total_s or _get_env_int("HTTP_TIMEOUT_TOTAL", 60)

    ot = req.option_ticker.strip()
    if not ot:
        raise HTTPException(status_code=400, detail="option_ticker is empty")

    LOGGER.info(
        "bench quote_at option_ticker=%s requests=%s max_concurrent=%s timeout=%s",
        ot,
        req.requests,
        max_concurrent,
        timeout_total_s,
    )

    return await bench_quote_at_timestamp(
        option_ticker=ot,
        timestamp_ns=int(req.timestamp_ns),
        api_key=api_key,
        requests=req.requests,
        max_concurrent=max_concurrent,
        timeout_total_s=timeout_total_s,
    )
