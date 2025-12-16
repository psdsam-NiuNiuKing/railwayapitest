"""Historical downloader for Railway deployment.

Writes daily files under `config.intraday_flow_root/YYYY-MM-DD/`:
- summaries.parquet (required by UI)
- trades.parquet (optional but enables PRO drilldowns)
- large_orders.json (optional; speeds up large-order views)
- scan_metadata.json (optional)

This implementation is intentionally self-contained (does not depend on the local
`realmarket_monitor` package).
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Literal

import aiohttp
import pandas as pd

from config import config

LOGGER = logging.getLogger(__name__)

MASSIVE_REST_BASE = "https://api.massive.com"
OPTION_MULTIPLIER = 100


# ----------------------------
# Time / parsing helpers
# ----------------------------

def _parse_timestamp(value: int | float | str | None) -> datetime | None:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        # Nanoseconds
        if value > 1e15:
            seconds = float(value) / 1_000_000_000
        elif value > 1e12:
            seconds = float(value) / 1_000
        else:
            seconds = float(value)
        return datetime.fromtimestamp(seconds, tz=timezone.utc)

    if isinstance(value, str):
        try:
            clean = value.replace("Z", "+00:00")
            ts = datetime.fromisoformat(clean)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            return ts
        except ValueError:
            return None

    return None


def _to_ns(ts: datetime) -> int:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return int(ts.timestamp() * 1_000_000_000)


def _parse_option_details(option_ticker: str) -> dict[str, Any]:
    result: dict[str, Any] = {"strike": None, "exp": None, "type": None, "underlying": None}

    try:
        s = option_ticker[2:] if option_ticker.startswith("O:") else option_ticker

        digit_start = None
        for i, c in enumerate(s):
            if c.isdigit():
                digit_start = i
                break

        if digit_start is None or len(s) < digit_start + 15:
            return result

        result["underlying"] = s[:digit_start]

        date_part = s[digit_start : digit_start + 6]
        type_part = s[digit_start + 6]
        strike_part = s[digit_start + 7 :]

        result["exp"] = f"{date_part[2:4]}/{date_part[4:6]}"
        result["type"] = "Call" if type_part.upper() == "C" else "Put"

        if strike_part.isdigit() and len(strike_part) >= 8:
            result["strike"] = int(strike_part[:8]) / 1000

    except Exception:
        return result

    return result


# ----------------------------
# Massive REST calls
# ----------------------------

async def _get_json(
    session: aiohttp.ClientSession,
    url: str,
    headers: dict[str, str],
    params: dict[str, Any] | None = None,
) -> tuple[int, dict[str, Any] | None]:
    try:
        async with session.get(url, headers=headers, params=params) as resp:
            status = resp.status
            if status != 200:
                return status, None
            return status, await resp.json()
    except Exception:
        return 0, None


async def _get_json_meta(
    session: aiohttp.ClientSession,
    url: str,
    headers: dict[str, str],
    params: dict[str, Any] | None = None,
    max_error_text: int = 500,
) -> tuple[int, dict[str, Any] | None, str | None]:
    """Like _get_json, but captures a small error body snippet for debugging."""
    max_attempts = 4
    base_sleep = 0.35

    for attempt in range(1, max_attempts + 1):
        try:
            async with session.get(url, headers=headers, params=params) as resp:
                status = resp.status

                if status == 429:
                    # Rate limited. Respect Retry-After if provided, else exponential backoff.
                    retry_after = resp.headers.get("Retry-After")
                    try:
                        wait_s = float(retry_after) if retry_after else (base_sleep * (2 ** (attempt - 1)))
                    except Exception:
                        wait_s = base_sleep * (2 ** (attempt - 1))
                    # Add small jitter and allow longer waits for strict rate limiting.
                    wait_s = wait_s * (0.9 + 0.2 * random.random())
                    wait_s = max(0.1, min(wait_s, 30.0))
                    if attempt < max_attempts:
                        await asyncio.sleep(wait_s)
                        continue

                if status != 200:
                    try:
                        txt = await resp.text()
                        txt = (txt or "")[:max_error_text]
                    except Exception:
                        txt = None
                    return status, None, txt

                return status, await resp.json(), None
        except Exception as e:
            # transient failure: retry a couple times
            if attempt < max_attempts:
                await asyncio.sleep(base_sleep * (2 ** (attempt - 1)))
                continue
            return 0, None, str(e)

    return 0, None, "unexpected retry loop exit"


async def fetch_contracts_for_underlying_async(
    session: aiohttp.ClientSession,
    ticker: str,
    as_of: date,
    headers: dict[str, str],
    semaphore: asyncio.Semaphore,
    limit: int = 250,
    api_debug: dict[str, Any] | None = None,
) -> tuple[str, list[dict[str, Any]]]:
    """Backward compatible wrapper.

    Historically this tried multiple endpoints and fell back to snapshot.
    The app now uses explicit functions for each behavior:
    - `fetch_contracts_all_contracts_async` (downloader),
    - `fetch_contracts_snapshot_async` (full/quick scan).
    """
    return await fetch_contracts_all_contracts_async(
        session=session,
        ticker=ticker,
        as_of=as_of,
        headers=headers,
        semaphore=semaphore,
        limit=limit,
        api_debug=api_debug,
    )


async def fetch_contracts_all_contracts_async(
    session: aiohttp.ClientSession,
    ticker: str,
    as_of: date,
    headers: dict[str, str],
    semaphore: asyncio.Semaphore,
    limit: int = 1000,
    spot_hint: float | None = None,
    expiration_horizon_days: int = 365,
    strike_band: tuple[float, float] = (0.7, 1.3),
    include_expired: bool = False,
    api_debug: dict[str, Any] | None = None,
) -> tuple[str, list[dict[str, Any]]]:
    """Downloader contract sourcing: Massive all-contracts endpoint (no snapshot fallback).

    Massive has had endpoint naming differences; we only try *all-contracts* variants.
    """
    async with semaphore:
        # Polygon-style reference endpoint for "all contracts".
        # NOTE:
        # - The reference endpoint can return *all historical expirations* unless filtered.
        # - We mostly want the "current chain" for the session: expiration_date >= session_date.
        # - Querying expired=true can roughly double pages; keep it optional and fall back if needed.
        base_url = f"{MASSIVE_REST_BASE}/v3/reference/options/contracts"

        exp_lte = as_of + timedelta(days=max(1, int(expiration_horizon_days)))
        base_params: dict[str, Any] = {
            "underlying_ticker": ticker,
            "as_of": as_of.isoformat(),
            "expiration_date.gte": as_of.isoformat(),
            "expiration_date.lte": exp_lte.isoformat(),
            "limit": int(limit),
        }

        if spot_hint and spot_hint > 0:
            low_mult, high_mult = strike_band
            try:
                low = float(spot_hint) * float(low_mult)
                high = float(spot_hint) * float(high_mult)
                if low > 0 and high > low:
                    base_params["strike_price.gte"] = round(low, 4)
                    base_params["strike_price.lte"] = round(high, 4)
            except Exception:
                pass

        by_ot: dict[str, dict[str, Any]] = {}
        total_pages = 0
        last_status: int | None = None
        last_err: str | None = None

        expired_flags: tuple[str, ...] = ("false", "true") if include_expired else ("false",)

        async def _union_one_expired_flag(expired_flag: str) -> None:
            nonlocal total_pages, last_status, last_err

            params = dict(base_params)
            params["expired"] = expired_flag

            next_url: str | None = base_url
            current_params: dict[str, Any] | None = dict(params)

            status, data, err_text = await _get_json_meta(session, next_url, headers, current_params)
            last_status = status
            last_err = err_text
            if api_debug is not None:
                api_debug.setdefault("contracts", {}).setdefault(ticker, []).append(
                    {"url": base_url, "params": dict(params), "status": int(status), "error": err_text}
                )
            if status != 200 or not data:
                return

            while next_url:
                if data is None:
                    break

                total_pages += 1
                for item in data.get("results", []) or []:
                    parsed = _parse_contract_payload_item(ticker, item)
                    ot = parsed.get("option_ticker") if parsed else None
                    if ot:
                        by_ot.setdefault(ot, parsed)

                next_url = data.get("next_url")
                current_params = None
                if not next_url:
                    break

                status, data, err_text = await _get_json_meta(session, next_url, headers, current_params)
                if status != 200:
                    if api_debug is not None:
                        api_debug.setdefault("contracts", {}).setdefault(ticker, []).append(
                            {"url": str(next_url), "params": None, "status": int(status), "error": err_text}
                        )
                    break

        for expired_flag in expired_flags:
            await _union_one_expired_flag(expired_flag)

        # Safe fallback: if expired=false returns nothing (or errors), try expired=true once.
        if not by_ot and not include_expired:
            await _union_one_expired_flag("true")

        contracts = list(by_ot.values())
        if api_debug is not None:
            api_debug.setdefault("contracts_chosen", {})[ticker] = {
                "url": base_url,
                "status": int(last_status or 0),
                "contracts": len(contracts),
                "pages": int(total_pages),
                "filters": {
                    "expiration_date.gte": base_params.get("expiration_date.gte"),
                    "expiration_date.lte": base_params.get("expiration_date.lte"),
                    "strike_price.gte": base_params.get("strike_price.gte"),
                    "strike_price.lte": base_params.get("strike_price.lte"),
                    "expired": ["false", "true"],
                },
            }

        if not contracts:
            LOGGER.debug(f"No filtered all-contracts list for {ticker} (last_status={last_status}, err={last_err})")
        return ticker, contracts


async def fetch_contracts_snapshot_async(
    session: aiohttp.ClientSession,
    ticker: str,
    as_of: date,
    headers: dict[str, str],
    semaphore: asyncio.Semaphore,
    limit: int = 250,
    api_debug: dict[str, Any] | None = None,
) -> tuple[str, list[dict[str, Any]]]:
    """Full/Quick Scan contract sourcing: Massive snapshot/options endpoint."""
    async with semaphore:
        # IMPORTANT: this endpoint is for the *current trading day*.
        # The local app calls it with no as_of/date parameter and uses day.volume filtering.
        # For historical session dates, we must fall back to reference contracts.
        try:
            if as_of != date.today():
                if api_debug is not None:
                    api_debug.setdefault("contracts_chosen", {})[ticker] = {
                        "url": f"{MASSIVE_REST_BASE}/v3/snapshot/options/{ticker}",
                        "status": 0,
                        "contracts": 0,
                        "pages": 0,
                        "note": "snapshot/options is current-day only; skipped for historical date",
                    }
                return ticker, []
        except Exception:
            # If date comparison fails for any reason, continue best-effort.
            pass

        base_url = f"{MASSIVE_REST_BASE}/v3/snapshot/options/{ticker}"
        # Snapshot endpoint hard-caps at 250.
        safe_limit = max(1, min(int(limit), 250))
        # Match local app: do not pass as_of/date.
        first_params_variants: list[dict[str, Any]] = [
            {"limit": safe_limit},
        ]

        contracts: list[dict[str, Any]] = []
        next_url: str | None = base_url
        current_params: dict[str, Any] | None = None

        status = 0
        data: dict[str, Any] | None = None
        err_text: str | None = None

        for p in first_params_variants:
            current_params = dict(p)
            status, data, err_text = await _get_json_meta(session, next_url, headers, current_params)
            if api_debug is not None:
                api_debug.setdefault("contracts", {}).setdefault(ticker, []).append(
                    {"url": base_url, "params": dict(p), "status": int(status), "error": err_text}
                )
            if status == 200 and data:
                break
            # Retry only on bad-request type errors; otherwise fail fast.
            if int(status) not in (400,):
                break

        if status != 200 or not data:
            if api_debug is not None:
                api_debug.setdefault("contracts_chosen", {})[ticker] = {
                    "url": base_url,
                    "status": int(status or 0),
                    "contracts": 0,
                    "pages": 0,
                }
            return ticker, []

        pages = 0
        while next_url:
            if data is None:
                break

            pages += 1
            for item in data.get("results", []) or []:
                parsed = _parse_contract_payload_item(ticker, item)
                if parsed and parsed.get("option_ticker"):
                    contracts.append(parsed)

            next_url = data.get("next_url")
            current_params = None
            if not next_url:
                break

            status, data, err_text = await _get_json_meta(session, next_url, headers, current_params)
            if status != 200:
                if api_debug is not None:
                    api_debug.setdefault("contracts", {}).setdefault(ticker, []).append(
                        {"url": str(next_url), "params": None, "status": int(status), "error": err_text}
                    )
                break

        if api_debug is not None:
            api_debug.setdefault("contracts_chosen", {})[ticker] = {
                "url": base_url,
                "status": int(status or 0),
                "contracts": len(contracts),
                "pages": pages,
            }

        return ticker, contracts


def _parse_contract_payload_item(underlying: str, item: dict[str, Any]) -> dict[str, Any] | None:
    """Best-effort parse of a contract record from Massive."""
    try:
        details = item.get("details") or {}

        option_ticker = (
            details.get("ticker")
            or item.get("ticker")
            or item.get("option_ticker")
            or item.get("options_ticker")
        )
        if not option_ticker:
            return None

        # Strike/expiration/type
        strike = details.get("strike_price")
        exp = details.get("expiration_date")
        contract_type = details.get("contract_type")

        if strike is None:
            strike = item.get("strike_price") or item.get("strike")

        if exp is None:
            exp = item.get("expiration_date") or item.get("expiration")

        if contract_type is None:
            contract_type = item.get("contract_type") or item.get("type")

        # OI / day volume
        open_interest = item.get("open_interest")
        if open_interest is None:
            open_interest = item.get("oi")

        day_data = item.get("day") or {}
        day_volume = day_data.get("volume")

        # IV
        implied_volatility = item.get("implied_volatility")
        if implied_volatility is None:
            implied_volatility = item.get("iv")

        # Underlying price
        underlying_asset = item.get("underlying_asset") or {}
        underlying_price = (
            underlying_asset.get("price")
            or item.get("underlying_price")
            or item.get("underlying")
            or item.get("underlying_last")
        )

        # Normalize
        try:
            strike_f = float(strike) if strike is not None else None
        except Exception:
            strike_f = None

        # exp stays as string (YYYY-MM-DD) if present; UI only needs strike/type in many places
        try:
            oi_i = int(open_interest) if open_interest is not None else 0
        except Exception:
            oi_i = 0

        try:
            vol_i = int(day_volume) if day_volume is not None else 0
        except Exception:
            vol_i = 0

        try:
            iv_f = float(implied_volatility) if implied_volatility is not None else None
        except Exception:
            iv_f = None

        try:
            spot_f = float(underlying_price) if underlying_price is not None else None
        except Exception:
            spot_f = None

        ctype = str(contract_type or "").lower() if contract_type is not None else ""
        if ctype in ("call", "c"):
            ctype = "call"
        elif ctype in ("put", "p"):
            ctype = "put"

        return {
            "underlying_ticker": underlying,
            "option_ticker": option_ticker,
            "strike": strike_f,
            "expiration": exp,
            "contract_type": ctype or None,
            "open_interest": oi_i,
            "day_volume": vol_i,
            "implied_volatility": iv_f,
            "underlying_price": spot_f,
        }
    except Exception:
        return None


async def fetch_underlying_spot_snapshot_async(
    session: aiohttp.ClientSession,
    ticker: str,
    as_of: date,
    headers: dict[str, str],
    semaphore: asyncio.Semaphore,
    api_debug: dict[str, Any] | None = None,
) -> float | None:
    """Best-effort underlying spot using snapshot/options.

    This is used only for `underlying_price` and for scoring/ATM IV when contract
    sourcing comes from all-contracts (which often omits spot).
    """
    async with semaphore:
        try:
            if as_of != date.today():
                if api_debug is not None:
                    api_debug.setdefault("spot", {}).setdefault(ticker, []).append(
                        {
                            "url": f"{MASSIVE_REST_BASE}/v3/snapshot/options/{ticker}",
                            "params": None,
                            "status": 0,
                            "error": "snapshot/options is current-day only; skipped for historical date",
                        }
                    )
                return None
        except Exception:
            pass

        url = f"{MASSIVE_REST_BASE}/v3/snapshot/options/{ticker}"
        params_variants: list[dict[str, Any]] = [
            {"limit": 10},
        ]

        status = 0
        data: dict[str, Any] | None = None
        err_text: str | None = None
        used_params: dict[str, Any] | None = None

        for p in params_variants:
            used_params = dict(p)
            status, data, err_text = await _get_json_meta(session, url, headers, used_params)
            if api_debug is not None:
                api_debug.setdefault("spot", {}).setdefault(ticker, []).append(
                    {"url": url, "params": dict(p), "status": int(status), "error": err_text}
                )
            if status == 200 and data:
                break
            if int(status) not in (400,):
                break

        if status != 200 or not data:
            return None

        for item in data.get("results", []) or []:
            parsed = _parse_contract_payload_item(ticker, item)
            sp = parsed.get("underlying_price") if parsed else None
            if sp is None:
                continue
            try:
                return float(sp)
            except Exception:
                continue

        return None


async def fetch_underlying_spot_daily_close_async(
    session: aiohttp.ClientSession,
    ticker: str,
    as_of: date,
    headers: dict[str, str],
    semaphore: asyncio.Semaphore,
    api_debug: dict[str, Any] | None = None,
) -> float | None:
    """Underlying spot hint using the daily aggregate bar.

    This avoids snapshot dependency (works best for historical dates; for the
    current session it may return the latest computed close/last).
    """
    async with semaphore:
        url = f"{MASSIVE_REST_BASE}/v2/aggs/ticker/{ticker}/range/1/day/{as_of.isoformat()}/{as_of.isoformat()}"
        params: dict[str, Any] = {
            "adjusted": "true",
            "sort": "desc",
            "limit": 1,
        }

        status, data, err_text = await _get_json_meta(session, url, headers, params)
        if api_debug is not None:
            api_debug.setdefault("spot", {}).setdefault(ticker, []).append(
                {"url": url, "params": dict(params), "status": int(status), "error": err_text}
            )

        if status != 200 or not data:
            return None

        results = data.get("results") or []
        if not results:
            return None

        row = results[0] or {}
        # Prefer close; fall back to vwap/open if needed.
        for k in ("c", "vw", "o"):
            v = row.get(k)
            try:
                if v is not None:
                    fv = float(v)
                    if fv > 0:
                        return fv
            except Exception:
                continue

        return None


def _parse_date(value: Any) -> date | None:
    if not value:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value[:10]).date()
        except Exception:
            return None
    return None


def _calc_otm_weight(contract_type: str | None, strike: float | None, spot: float | None) -> float:
    if not contract_type or strike is None or spot is None or spot <= 0:
        return 1.0
    try:
        moneyness = float(strike) / float(spot)
    except Exception:
        return 1.0

    ct = str(contract_type).lower()
    if ct == "call":
        if moneyness > 1.0:
            return 1.0 + min((moneyness - 1.0) * 2.0, 1.0)
        return 1.0
    # put
    if moneyness < 1.0:
        return 1.0 + min((1.0 - moneyness) * 2.0, 1.0)
    return 1.0


def _calc_dte_weight(expiration_date: date | None, session_date: date) -> float:
    if not expiration_date:
        return 1.0
    try:
        dte = int((expiration_date - session_date).days)
    except Exception:
        return 1.0

    if dte <= 7:
        return 1.0
    if dte <= 30:
        return 1.0 + (dte - 7) / 46
    if dte <= 90:
        return 1.5 + (dte - 30) / 120
    return 2.0 + min((dte - 90) / 180, 1.0)


async def fetch_trades_for_contract_async(
    session: aiohttp.ClientSession,
    option_ticker: str,
    session_date: date,
    headers: dict[str, str],
    semaphore: asyncio.Semaphore,
    api_debug: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    async with semaphore:
        url = f"{MASSIVE_REST_BASE}/v3/trades/{option_ticker}"
        params: dict[str, Any] = {
            "timestamp": session_date.isoformat(),
            "limit": 50000,
            "order": "asc",
            "sort": "timestamp",
        }

        trades: list[dict[str, Any]] = []
        next_url: str | None = url
        current_params: dict[str, Any] | None = params

        while next_url:
            status, data, err_text = await _get_json_meta(session, next_url, headers, current_params)
            if status != 200 or not data:
                if api_debug is not None:
                    stats = api_debug.setdefault("trades", {"status_counts": {}, "fail_samples": []})
                    key = str(int(status))
                    stats["status_counts"][key] = int(stats["status_counts"].get(key, 0)) + 1
                    if len(stats["fail_samples"]) < 30:
                        stats["fail_samples"].append(
                            {
                                "option_ticker": option_ticker,
                                "status": int(status),
                                "error": err_text,
                            }
                        )
                return trades

            for row in data.get("results", []) or []:
                ts = _parse_timestamp(row.get("participant_timestamp") or row.get("sip_timestamp") or row.get("t"))
                if ts is None:
                    continue

                price = row.get("price") or row.get("p")
                size = row.get("size") or row.get("s")
                if price is None or size is None:
                    continue

                try:
                    price_f = float(price)
                    size_i = int(size)
                except Exception:
                    continue

                trades.append(
                    {
                        "option_ticker": option_ticker,
                        "timestamp": ts,
                        "price": price_f,
                        "size": size_i,
                        "exchange": row.get("exchange") or row.get("x"),
                        "conditions": row.get("conditions") or row.get("c"),
                        "sequence_number": row.get("sequence_number") or row.get("q"),
                    }
                )

            next_url = data.get("next_url")
            current_params = None

        return trades


async def fetch_quote_at_timestamp_async(
    session: aiohttp.ClientSession,
    option_ticker: str,
    timestamp_ns: int,
    headers: dict[str, str],
    api_debug: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Fetch most recent quote at or before timestamp (REST, no subscription limits)."""
    url = f"{MASSIVE_REST_BASE}/v3/quotes/{option_ticker}"
    params = {
        "timestamp.lte": str(timestamp_ns),
        "limit": 1,
        "order": "desc",
        "sort": "timestamp",
    }

    status, data, err_text = await _get_json_meta(session, url, headers, params)
    if status != 200 or not data:
        if api_debug is not None:
            stats = api_debug.setdefault("quotes", {"status_counts": {}, "fail_samples": []})
            key = str(int(status))
            stats["status_counts"][key] = int(stats["status_counts"].get(key, 0)) + 1
            if len(stats["fail_samples"]) < 30:
                stats["fail_samples"].append(
                    {
                        "option_ticker": option_ticker,
                        "status": int(status),
                        "error": err_text,
                    }
                )
        return None

    results = data.get("results", []) or []
    if not results:
        return None

    row = results[0]
    ts = _parse_timestamp(row.get("sip_timestamp"))
    if ts is None:
        return None

    bid = row.get("bid_price")
    ask = row.get("ask_price")
    if bid is None or ask is None:
        return None

    try:
        bid_f = float(bid)
        ask_f = float(ask)
    except Exception:
        return None

    if bid_f <= 0 or ask_f <= 0:
        return None

    return {
        "timestamp": ts,
        "bid_price": bid_f,
        "ask_price": ask_f,
        "bid_size": int(row.get("bid_size", 0) or 0),
        "ask_size": int(row.get("ask_size", 0) or 0),
    }


# ----------------------------
# Large order detection / scoring
# ----------------------------

@dataclass
class LargeOrder:
    underlying_ticker: str
    option_ticker: str
    timestamp: datetime
    total_contracts: int
    avg_price: float
    total_notional: float
    order_type: str
    direction: str = "unknown"  # buy/sell/unknown
    direction_confidence: float = 0.0
    avg_bid: float | None = None
    avg_ask: float | None = None
    spot_price: float | None = None
    trades: list[dict[str, Any]] | None = None


def _classify_from_quote(price: float, bid: float, ask: float) -> tuple[str, float]:
    """Local-style quote classifier.

    Returns (direction, confidence) where direction is:
    buy|sell|neutral|unknown
    """
    if bid <= 0 or ask <= 0 or bid >= ask or not math.isfinite(price):
        return "unknown", 0.0

    spread = ask - bid

    # At or above ask => buy
    if price >= ask:
        above_ask = price - ask
        conf = min(1.0, 0.9 + (above_ask / spread) * 0.1) if spread > 0 else 0.95
        return "buy", float(conf)

    # At or below bid => sell
    if price <= bid:
        below_bid = bid - price
        conf = min(1.0, 0.9 + (below_bid / spread) * 0.1) if spread > 0 else 0.95
        return "sell", float(conf)

    # Between bid/ask
    position = (price - bid) / spread

    if position >= 0.75:
        conf = 0.5 + (position - 0.5) * 0.8
        return "buy", float(conf)
    if position <= 0.25:
        conf = 0.5 + (0.5 - position) * 0.8
        return "sell", float(conf)
    if abs(position - 0.5) < 0.01:
        return "neutral", 0.1
    if position > 0.5:
        conf = 0.2 + (position - 0.5) * 1.0
        return "buy", float(conf)
    conf = 0.2 + (0.5 - position) * 1.0
    return "sell", float(conf)


def detect_large_orders_from_trades(
    underlying: str,
    trades: list[dict[str, Any]],
    window_ms: int,
    min_contracts: int,
    min_notional: float,
    spot_price: float | None,
) -> list[LargeOrder]:
    if not trades:
        return []

    window = timedelta(milliseconds=int(window_ms))

    # Group by option_ticker (avoid Pandas overhead; keeps parity with local logic).
    by_ot: dict[str, list[dict[str, Any]]] = {}
    for t in trades:
        ot = t.get("option_ticker")
        if not ot:
            continue
        by_ot.setdefault(str(ot), []).append(t)

    orders: list[LargeOrder] = []

    for option_ticker, rows in by_ot.items():
        # Sort by timestamp; ignore rows with unparseable timestamps.
        cleaned: list[dict[str, Any]] = []
        for r in rows:
            ts = r.get("timestamp")
            if isinstance(ts, str):
                try:
                    ts = _parse_timestamp(ts)
                except Exception:
                    ts = None
            if not isinstance(ts, datetime):
                continue
            rr = dict(r)
            rr["timestamp"] = ts
            cleaned.append(rr)

        if not cleaned:
            continue

        cleaned.sort(key=lambda x: x["timestamp"])
        n = len(cleaned)
        processed = [False] * n

        i = 0
        while i < n:
            if processed[i]:
                i += 1
                continue

            start_ts: datetime = cleaned[i]["timestamp"]

            # Local parity: single large trade takes precedence over sweep/burst windows.
            try:
                single_price = float(cleaned[i].get("price") or 0.0)
                single_size = int(cleaned[i].get("size") or 0)
            except Exception:
                single_price = 0.0
                single_size = 0

            single_notional = float(single_price * single_size * OPTION_MULTIPLIER) if single_size > 0 else 0.0
            if single_size >= int(min_contracts) and single_notional >= float(min_notional):
                trades_payload = [
                    {
                        "option_ticker": option_ticker,
                        "timestamp": start_ts.isoformat(),
                        "price": float(single_price),
                        "size": int(single_size),
                        "exchange": cleaned[i].get("exchange"),
                        "conditions": cleaned[i].get("conditions"),
                    }
                ]

                orders.append(
                    LargeOrder(
                        underlying_ticker=underlying,
                        option_ticker=str(option_ticker),
                        timestamp=start_ts,
                        total_contracts=int(single_size),
                        avg_price=float(single_price),
                        total_notional=float(single_notional),
                        order_type="single",
                        spot_price=spot_price,
                        trades=trades_payload,
                    )
                )
                processed[i] = True
                i += 1
                continue

            end = i
            total_contracts = 0
            notional_sum = 0.0
            px_size_sum = 0.0
            exchanges: set[Any] = set()

            while end < n and (cleaned[end]["timestamp"] - start_ts) <= window:
                if not processed[end]:
                    try:
                        price_f = float(cleaned[end].get("price") or 0.0)
                        size_i = int(cleaned[end].get("size") or 0)
                    except Exception:
                        price_f = 0.0
                        size_i = 0

                    if size_i > 0 and price_f > 0:
                        total_contracts += size_i
                        px_size_sum += price_f * size_i
                        notional_sum += price_f * size_i * OPTION_MULTIPLIER
                        ex = cleaned[end].get("exchange")
                        if ex is not None:
                            exchanges.add(ex)

                end += 1

            if total_contracts >= int(min_contracts) and notional_sum >= float(min_notional):
                avg_price = float(px_size_sum / total_contracts) if total_contracts > 0 else 0.0

                # Order type heuristic (local-style): sweep if multiple exchanges within the window.
                if len(exchanges) >= 2:
                    order_type = "sweep"
                elif (end - i) >= 2:
                    order_type = "burst"
                else:
                    order_type = "single"

                last_ts = cleaned[end - 1]["timestamp"]

                trades_payload: list[dict[str, Any]] = []
                for k in range(i, end):
                    r = cleaned[k]
                    tsv = r.get("timestamp")
                    trades_payload.append(
                        {
                            "option_ticker": option_ticker,
                            "timestamp": (tsv.isoformat() if isinstance(tsv, datetime) else str(tsv)),
                            "price": float(r.get("price") or 0),
                            "size": int(r.get("size") or 0),
                            "exchange": r.get("exchange"),
                            "conditions": r.get("conditions"),
                        }
                    )

                orders.append(
                    LargeOrder(
                        underlying_ticker=underlying,
                        option_ticker=str(option_ticker),
                        timestamp=last_ts,
                        total_contracts=int(total_contracts),
                        avg_price=float(avg_price),
                        total_notional=float(notional_sum),
                        order_type=str(order_type),
                        spot_price=spot_price,
                        trades=trades_payload,
                    )
                )

                for k in range(i, end):
                    processed[k] = True

                i = end
                continue

            i += 1

    return orders


async def enrich_large_orders_with_quotes_async(
    session: aiohttp.ClientSession,
    orders: list[LargeOrder],
    headers: dict[str, str],
    semaphore: asyncio.Semaphore,
    progress_callback: Callable[[int, int, str], None] | None = None,
) -> None:
    if not orders:
        return

    completed = 0

    async def enrich_one(order: LargeOrder) -> None:
        nonlocal completed
        async with semaphore:
            ts_ns = _to_ns(order.timestamp)
            quote = await fetch_quote_at_timestamp_async(session, order.option_ticker, ts_ns, headers, api_debug=None)
            if quote:
                order.avg_bid = quote.get("bid_price")
                order.avg_ask = quote.get("ask_price")
                direction, conf = _classify_from_quote(order.avg_price, order.avg_bid, order.avg_ask)
                order.direction = direction
                order.direction_confidence = conf

        completed += 1
        if progress_callback and completed % 100 == 0:
            progress_callback(completed, len(orders), "quotes")

    await asyncio.gather(*(enrich_one(o) for o in orders), return_exceptions=True)


def _sentiment_score(direction: str, option_type: str | None, notional: float) -> float:
    """Bullish positive, bearish negative, based on direction x call/put."""
    opt = (option_type or "").lower()
    is_call = opt == "call"
    is_put = opt == "put"

    if direction == "buy":
        if is_call:
            return +notional
        if is_put:
            return -notional
    if direction == "sell":
        if is_call:
            return -notional
        if is_put:
            return +notional
    return 0.0


def _compute_atm_iv(contracts: list[dict[str, Any]], spot: float | None) -> float | None:
    if not contracts or not spot or spot <= 0:
        return None

    best: list[tuple[float, float]] = []
    for c in contracts:
        strike = c.get("strike")
        iv = c.get("implied_volatility")
        if strike is None or iv is None:
            continue
        try:
            d = abs(float(strike) - float(spot))
            best.append((d, float(iv)))
        except Exception:
            continue

    if not best:
        return None

    best.sort(key=lambda x: x[0])
    # take median of top-k nearest strikes
    k = min(10, len(best))
    ivs = [iv for _, iv in best[:k] if iv and iv > 0]
    if not ivs:
        return None

    ivs.sort()
    mid = len(ivs) // 2
    return float(ivs[mid])


# ----------------------------
# Orchestration + persistence
# ----------------------------

@dataclass
class DayDownloadResult:
    session_date: date
    tickers_requested: int
    tickers_with_trades: int
    contracts_processed: int
    trades_downloaded: int
    large_orders_detected: int
    large_orders_enriched: int
    duration_seconds: float
    errors: list[str]


async def download_day_async(
    session_date: date,
    tickers: list[str],
    max_concurrent: int,
    min_contracts_large: int,
    min_notional_large: float,
    window_ms: int,
    progress_callback: Callable[[int, int, str], None] | None = None,
    contract_source: Literal["snapshot", "all-contracts"] = "all-contracts",
) -> DayDownloadResult:
    start = time.time()

    api_key = config.massive_api_key
    if not api_key:
        raise RuntimeError("MASSIVE_API_KEY is not configured")

    headers = {"Authorization": f"Bearer {api_key}"}

    connector = aiohttp.TCPConnector(
        limit=max_concurrent,
        limit_per_host=max_concurrent,
        force_close=False,
        enable_cleanup_closed=True,
        ttl_dns_cache=600,
        keepalive_timeout=60,
    )
    timeout = aiohttp.ClientTimeout(total=3600, connect=60, sock_read=120)

    contract_sema = asyncio.Semaphore(max(10, min(max_concurrent, 200)))
    trades_sema = asyncio.Semaphore(max(10, int(max_concurrent)))
    quote_sema = asyncio.Semaphore(max(10, int(max_concurrent)))
    max_tickers_concurrent = max(1, int(getattr(config, "max_tickers_concurrent", 5) or 5))
    ticker_sema = asyncio.Semaphore(int(max_tickers_concurrent))

    errors: list[str] = []
    api_debug: dict[str, Any] = {
        "contracts": {},
        "contracts_chosen": {},
        "trades": {"status_counts": {}, "fail_samples": []},
        "quotes": {"status_counts": {}, "fail_samples": []},
    }

    def _chunked(seq: list[str], size: int) -> list[list[str]]:
        if size <= 0:
            return [seq]
        return [seq[i : i + size] for i in range(0, len(seq), size)]

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Prefetch spot hints (used to tighten strike bands for all-contracts).
        # IMPORTANT: keep this bounded; full scans can have thousands of tickers.
        spot_hints: dict[str, float | None] = {}
        if contract_source == "all-contracts" and tickers:
            spot_sema = asyncio.Semaphore(max(5, min(max_concurrent, 100)))

            async def fetch_spot_hint_one(t: str) -> tuple[str, float | None]:
                sp = await fetch_underlying_spot_daily_close_async(
                    session=session,
                    ticker=t,
                    as_of=session_date,
                    headers=headers,
                    semaphore=spot_sema,
                    api_debug=api_debug,
                )
                return t, sp

            # Chunk task creation to avoid building thousands of tasks at once.
            spot_chunks = _chunked(list(tickers), size=max(50, int(max_tickers_concurrent) * 50))
            for chunk in spot_chunks:
                spot_results = await asyncio.gather(*(fetch_spot_hint_one(t) for t in chunk), return_exceptions=True)
                for r in spot_results:
                    if isinstance(r, Exception):
                        errors.append(str(r))
                        continue
                    t, sp = r
                    spot_hints[t] = sp

        trades_downloaded = 0
        contracts_processed = 0
        tickers_with_trades = 0

        summary_rows: list[dict[str, Any]] = []
        all_orders: list[LargeOrder] = []
        enriched_count = 0

        # Only store full trades when explicitly enabled (Railway default is False).
        store_trades = bool(getattr(config, "store_trades", False))
        trades_rows: list[dict[str, Any]] = []

        total_tickers = len(tickers)
        total_units = max(1, int(total_tickers) * 100)
        progress_done_units = 0
        progress_lock = asyncio.Lock()

        def _progress(units_done: int, msg: str) -> None:
            if not progress_callback:
                return
            try:
                progress_callback(int(units_done), int(total_units), str(msg))
            except Exception:
                return

        async def _mark_ticker_done(underlying: str, msg: str) -> None:
            nonlocal progress_done_units
            if not progress_callback:
                return
            async with progress_lock:
                progress_done_units = min(total_units, int(progress_done_units + 100))
                _progress(progress_done_units, f"ticker: {underlying} ({msg})")

        async def process_ticker(underlying: str) -> None:
            nonlocal trades_downloaded, contracts_processed, tickers_with_trades, enriched_count

            async with ticker_sema:
                try:
                    if contract_source == "snapshot":
                        _u, contracts = await fetch_contracts_snapshot_async(
                            session,
                            underlying,
                            session_date,
                            headers,
                            contract_sema,
                            limit=int(getattr(config, "snapshot_contracts_limit", 250)),
                            api_debug=api_debug,
                        )

                        # If snapshot endpoint fails (commonly 400 due to param naming),
                        # fall back to all-contracts so scans don't silently return empty.
                        if not contracts:
                            chosen = (api_debug.get("contracts_chosen") or {}).get(underlying) if api_debug else None
                            st = 0
                            try:
                                st = int((chosen or {}).get("status", 0) or 0)
                            except Exception:
                                st = 0
                            # status==0 is used when snapshot was skipped for historical dates.
                            if st in (0, 400, 404):
                                errors.append(f"{underlying}: snapshot contracts unavailable (status={st}); falling back to all-contracts")
                                _u, contracts = await fetch_contracts_all_contracts_async(
                                    session,
                                    underlying,
                                    session_date,
                                    headers,
                                    contract_sema,
                                    limit=int(getattr(config, "contracts_limit", 1000)),
                                    spot_hint=spot_hints.get(underlying),
                                    expiration_horizon_days=int(getattr(config, "expiration_horizon_days", 365)),
                                    strike_band=(
                                        float(getattr(config, "strike_low_mult", 0.7)),
                                        float(getattr(config, "strike_high_mult", 1.3)),
                                    ),
                                    include_expired=bool(getattr(config, "include_expired_contracts", False)),
                                    api_debug=api_debug,
                                )
                    else:
                        _u, contracts = await fetch_contracts_all_contracts_async(
                            session,
                            underlying,
                            session_date,
                            headers,
                            contract_sema,
                            limit=int(getattr(config, "contracts_limit", 1000)),
                            spot_hint=spot_hints.get(underlying),
                            expiration_horizon_days=int(getattr(config, "expiration_horizon_days", 365)),
                            strike_band=(
                                float(getattr(config, "strike_low_mult", 0.7)),
                                float(getattr(config, "strike_high_mult", 1.3)),
                            ),
                            include_expired=bool(getattr(config, "include_expired_contracts", False)),
                            api_debug=api_debug,
                        )
                except Exception as e:
                    errors.append(f"{underlying}: contracts error: {e}")
                    await _mark_ticker_done(underlying, "contracts error")
                    return

                if not contracts:
                    await _mark_ticker_done(underlying, "no contracts")
                    return

                # For scan runs, keep the contract universe bounded.
                # Snapshot responses include `day_volume` for many contracts; prefer active-only.
                # Also apply expiration horizon + strike band trimming.
                try:
                    spot_for_filter = spot_hints.get(underlying)
                    exp_horizon_days = int(getattr(config, "expiration_horizon_days", 365))
                    exp_lte = session_date + timedelta(days=exp_horizon_days)

                    low_mult = float(getattr(config, "strike_low_mult", 0.7))
                    high_mult = float(getattr(config, "strike_high_mult", 1.3))

                    low_strike = None
                    high_strike = None
                    if spot_for_filter and float(spot_for_filter) > 0:
                        low_strike = float(spot_for_filter) * low_mult
                        high_strike = float(spot_for_filter) * high_mult

                    filtered: list[dict[str, Any]] = []
                    for c in contracts:
                        # Active-only is only safe/meaningful for snapshot-derived contracts.
                        # The all-contracts endpoint may include day fields that are zero/empty,
                        # which would incorrectly filter out the entire chain.
                        if contract_source == "snapshot":
                            dv = c.get("day_volume")
                            if dv is not None:
                                try:
                                    if float(dv) <= 0:
                                        continue
                                except Exception:
                                    pass

                        exp_d = _parse_date(c.get("expiration"))
                        if exp_d is not None:
                            if exp_d < session_date or exp_d > exp_lte:
                                continue

                        if low_strike is not None and high_strike is not None:
                            strike = c.get("strike")
                            try:
                                strike_f = float(strike)
                            except Exception:
                                strike_f = None
                            if strike_f is not None:
                                if strike_f < low_strike or strike_f > high_strike:
                                    continue

                        filtered.append(c)

                    contracts = filtered
                except Exception:
                    # Never let filtering errors break scans.
                    pass

                # Spot used for summary/weights.
                spot = spot_hints.get(underlying)
                if spot is None:
                    spot = next((c.get("underlying_price") for c in contracts if c.get("underlying_price") is not None), None)

                option_to_contract: dict[str, dict[str, Any]] = {}
                option_tickers: list[str] = []
                for c in contracts:
                    ot = c.get("option_ticker")
                    if not ot:
                        continue
                    option_tickers.append(str(ot))
                    option_to_contract[str(ot)] = c

                if not option_tickers:
                    await _mark_ticker_done(underlying, "no option tickers")
                    return

                ticker_trades: list[dict[str, Any]] = []
                trade_batches = _chunked(option_tickers, size=max(50, min(250, int(max_concurrent // 2) or 100)))
                for batch in trade_batches:
                    async def fetch_one(ot: str) -> tuple[str, list[dict[str, Any]]]:
                        tds = await fetch_trades_for_contract_async(
                            session,
                            ot,
                            session_date,
                            headers,
                            trades_sema,
                            api_debug=api_debug,
                        )
                        return ot, tds

                    batch_results = await asyncio.gather(*(fetch_one(ot) for ot in batch), return_exceptions=True)
                    for br in batch_results:
                        if isinstance(br, Exception):
                            errors.append(str(br))
                            continue
                        ot, tds = br
                        if not tds:
                            continue
                        contracts_processed += 1
                        trades_downloaded += len(tds)

                        c = option_to_contract.get(ot) or {}
                        exp_date = _parse_date(c.get("expiration"))
                        strike = c.get("strike")
                        contract_type = c.get("contract_type")
                        for tr in tds:
                            tr["underlying_ticker"] = underlying
                            tr["spot_price"] = spot
                            tr["direction"] = "unknown"
                            tr["direction_confidence"] = None
                            tr["direction_method"] = "none"
                            try:
                                tr["dollar_value"] = float(tr.get("price") or 0) * float(tr.get("size") or 0) * OPTION_MULTIPLIER
                            except Exception:
                                tr["dollar_value"] = None

                            tr["strike"] = strike
                            tr["expiration"] = exp_date.isoformat() if exp_date else c.get("expiration")
                            tr["contract_type"] = contract_type

                        ticker_trades.extend(tds)

                if not ticker_trades:
                    await _mark_ticker_done(underlying, "no trades")
                    return

                tickers_with_trades += 1

                orders = detect_large_orders_from_trades(
                    underlying=underlying,
                    trades=ticker_trades,
                    window_ms=window_ms,
                    min_contracts=min_contracts_large,
                    min_notional=min_notional_large,
                    spot_price=spot,
                )

                if orders:
                    await enrich_large_orders_with_quotes_async(session, orders, headers, quote_sema, None)
                    all_orders.extend(orders)
                    enriched_count += sum(1 for o in orders if o.direction in ("buy", "sell", "neutral"))

                if store_trades:
                    per_trade_index: dict[tuple[str, str, float, int], dict[str, Any]] = {}
                    for tr in ticker_trades:
                        key = (
                            str(tr.get("option_ticker")),
                            str(tr.get("timestamp")),
                            float(tr.get("price") or 0),
                            int(tr.get("size") or 0),
                        )
                        per_trade_index[key] = tr

                    for o in orders:
                        if not o.trades:
                            continue
                        for tr in o.trades:
                            key = (
                                str(tr.get("option_ticker")),
                                str(tr.get("timestamp")),
                                float(tr.get("price") or 0),
                                int(tr.get("size") or 0),
                            )
                            row = per_trade_index.get(key)
                            if not row:
                                continue
                            row["bid_price"] = o.avg_bid
                            row["ask_price"] = o.avg_ask
                            row["direction"] = o.direction
                            row["direction_confidence"] = o.direction_confidence
                            row["direction_method"] = "quote"

                # Build summary for this ticker
                buy_notional = 0.0
                sell_notional = 0.0
                directional_score = 0.0

                for o in orders or []:
                    info = _parse_option_details(o.option_ticker)
                    opt_type = info.get("type")

                    if o.direction == "buy":
                        buy_notional += o.total_notional
                    elif o.direction == "sell":
                        sell_notional += o.total_notional

                    cc = option_to_contract.get(o.option_ticker) or {}
                    strike = cc.get("strike")
                    exp_date = _parse_date(cc.get("expiration"))
                    contract_type = cc.get("contract_type")

                    otm_w = _calc_otm_weight(contract_type, strike, spot)
                    dte_w = _calc_dte_weight(exp_date, session_date)
                    weighted_notional = float(o.total_notional) * float(otm_w) * float(dte_w)
                    directional_score += _sentiment_score(o.direction, opt_type, weighted_notional)

                try:
                    total_volume = int(sum(int(tr.get("size") or 0) for tr in ticker_trades))
                except Exception:
                    total_volume = 0
                trade_count = int(len(ticker_trades))

                buy_volume = 0
                sell_volume = 0
                neutral_volume = 0
                if store_trades:
                    for tr in ticker_trades:
                        d = str(tr.get("direction") or "unknown")
                        sz = int(tr.get("size") or 0)
                        if d == "buy":
                            buy_volume += sz
                        elif d == "sell":
                            sell_volume += sz
                        elif d == "neutral":
                            neutral_volume += sz

                atm_iv = _compute_atm_iv(contracts, spot)

                summary_rows.append(
                    {
                        "ticker": underlying,
                        "session_date": session_date.isoformat(),
                        "last_updated": datetime.now(tz=timezone.utc).isoformat(),
                        "directional_score": float(directional_score),
                        "large_order_count": int(len(orders or [])),
                        "large_order_buy_notional": float(buy_notional),
                        "large_order_sell_notional": float(sell_notional),
                        "net_large_order_notional": float(buy_notional - sell_notional),
                        "total_volume": int(total_volume),
                        "total_trade_count": int(trade_count),
                        "buy_volume": int(buy_volume),
                        "sell_volume": int(sell_volume),
                        "neutral_volume": int(neutral_volume),
                        "atm_iv": float(atm_iv) if atm_iv is not None else None,
                        "underlying_price": float(spot) if spot is not None else None,
                    }
                )

                if store_trades:
                    trades_rows.extend(ticker_trades)

                await _mark_ticker_done(underlying, "done")

        # Process tickers with bounded parallelism.
        # Chunk task creation so full scans don't allocate huge task lists.
        ticker_chunks = _chunked(list(tickers), size=max(20, int(max_tickers_concurrent) * 5))
        for chunk in ticker_chunks:
            results = await asyncio.gather(*(process_ticker(t) for t in chunk), return_exceptions=True)
            for r in results:
                if isinstance(r, Exception):
                    errors.append(str(r))

        # 6) Persist
        out_dir = _ensure_flow_dir(session_date)

        # trades.parquet (optional)
        if store_trades and trades_rows:
            trades_df = pd.DataFrame(trades_rows)
            if not trades_df.empty:
                trades_df["timestamp"] = pd.to_datetime(trades_df["timestamp"], utc=True, errors="coerce")
                trades_df = trades_df.dropna(subset=["timestamp"])
                trades_df.to_parquet(out_dir / "trades.parquet", index=False)

        # large_orders.json
        orders_payload: list[dict[str, Any]] = []
        for o in all_orders:
            info = _parse_option_details(o.option_ticker)
            orders_payload.append(
                {
                    "timestamp": o.timestamp.isoformat(),
                    "underlying_ticker": o.underlying_ticker,
                    "option_ticker": o.option_ticker,
                    "option_type": info.get("type"),
                    "strike": info.get("strike"),
                    "expiration": info.get("exp"),
                    "total_contracts": o.total_contracts,
                    "size": o.total_contracts,
                    "avg_price": o.avg_price,
                    "price": o.avg_price,
                    "total_notional": o.total_notional,
                    "notional": o.total_notional,
                    "premium": o.total_notional,
                    "order_type": o.order_type,
                    "avg_bid": o.avg_bid,
                    "avg_ask": o.avg_ask,
                    "direction": o.direction,
                    "order_direction": "net_buyer" if o.direction == "buy" else "net_seller" if o.direction == "sell" else "unknown",
                    "order_direction_confidence": o.direction_confidence,
                    "spot_price": o.spot_price,
                    "trades": o.trades or [],
                }
            )

        (out_dir / "large_orders.json").write_text(json.dumps(orders_payload))

        # summaries.parquet (required)
        # Always write the file (even if empty) so the UI can discover the session.
        summary_columns = [
            "ticker",
            "session_date",
            "last_updated",
            "directional_score",
            "large_order_count",
            "large_order_buy_notional",
            "large_order_sell_notional",
            "net_large_order_notional",
            "total_volume",
            "total_trade_count",
            "buy_volume",
            "sell_volume",
            "neutral_volume",
            "atm_iv",
            "underlying_price",
        ]
        summaries_df = pd.DataFrame(summary_rows, columns=summary_columns)
        summaries_df.to_parquet(out_dir / "summaries.parquet", index=False)

        # scan_metadata.json (optional)
        finished_at = datetime.now(tz=timezone.utc)
        duration_seconds = float(time.time() - start)

        # Provide both historical-downloader keys and the keys expected by SharedStateManager.
        tickers_with_large_orders = sum(1 for r in summary_rows if int(r.get("large_order_count", 0) or 0) > 0)

        meta = {
            # SharedState-compatible fields
            "scan_time": finished_at.isoformat(),
            "session_date": session_date.isoformat(),
            "tickers_scanned": int(len(tickers)),
            "tickers_with_activity": int(tickers_with_trades),
            "tickers_with_large_orders": int(tickers_with_large_orders),
            "total_large_orders": int(len(all_orders)),
            "scan_duration_seconds": duration_seconds,

            # Downloader-specific fields (kept for debugging)
            "generated_at": finished_at.isoformat(),
            "tickers_requested": int(len(tickers)),
            "tickers_with_trades": int(tickers_with_trades),
            "contracts_processed": int(contracts_processed),
            "trades_downloaded": int(trades_downloaded),
            "large_orders_detected": int(len(all_orders)),
            "large_orders_enriched": int(sum(1 for o in all_orders if o.direction in ("buy", "sell", "neutral"))),
            "massive_api_debug": api_debug,
            "errors": errors[:50],
        }
        (out_dir / "scan_metadata.json").write_text(json.dumps(meta, indent=2))

    duration = time.time() - start
    return DayDownloadResult(
        session_date=session_date,
        tickers_requested=len(tickers),
        tickers_with_trades=tickers_with_trades,
        contracts_processed=contracts_processed,
        trades_downloaded=trades_downloaded,
        large_orders_detected=len(all_orders),
        large_orders_enriched=enriched_count,
        duration_seconds=duration,
        errors=errors,
    )


def _ensure_flow_dir(session_date: date) -> Path:
    root = config.intraday_flow_root
    root.mkdir(parents=True, exist_ok=True)
    out_dir = root / session_date.strftime("%Y-%m-%d")
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def iter_trading_days(start: date, end: date, skip_weekends: bool = True) -> Iterable[date]:
    cur = start
    while cur <= end:
        if skip_weekends and cur.weekday() >= 5:
            cur += timedelta(days=1)
            continue
        yield cur
        cur += timedelta(days=1)


def download_date_range(
    start_date: date,
    end_date: date,
    tickers: list[str],
    max_concurrent: int | None = None,
    min_contracts_large: int | None = None,
    min_notional_large: float | None = None,
    window_ms: int = 2000,
    skip_weekends: bool = True,
    progress_callback: Callable[[int, int, str], None] | None = None,
) -> list[DayDownloadResult]:
    """Historical downloader wrapper (contract sourcing via all-contracts)."""
    max_concurrent = max_concurrent or config.max_concurrent
    min_contracts_large = min_contracts_large or config.large_order_size_threshold
    min_notional_large = float(min_notional_large or config.large_order_notional_threshold)

    results: list[DayDownloadResult] = []

    days = list(iter_trading_days(start_date, end_date, skip_weekends=skip_weekends))
    total_days = len(days)

    for i, d in enumerate(days, start=1):
        if progress_callback:
            progress_callback(i - 1, total_days, f"day: {d.isoformat()}")

        r = asyncio.run(
            download_day_async(
                session_date=d,
                tickers=tickers,
                max_concurrent=max_concurrent,
                min_contracts_large=min_contracts_large,
                min_notional_large=min_notional_large,
                window_ms=window_ms,
                progress_callback=progress_callback,
                contract_source="all-contracts",
            )
        )
        results.append(r)

        if progress_callback:
            progress_callback(i, total_days, f"done: {d.isoformat()} ({r.trades_downloaded:,} trades)")

    return results


def scan_date_range(
    start_date: date,
    end_date: date,
    tickers: list[str],
    max_concurrent: int | None = None,
    min_contracts_large: int | None = None,
    min_notional_large: float | None = None,
    window_ms: int = 2000,
    skip_weekends: bool = True,
    progress_callback: Callable[[int, int, str], None] | None = None,
    contract_source: Literal["snapshot", "all-contracts"] = "snapshot",
) -> list[DayDownloadResult]:
    """Full/Quick Scan wrapper.

    Notes:
    - "snapshot" here refers to Massive's snapshot API endpoint (not local snapshot files).
      It's typically far more efficient for scans because it includes day volume.
    - "all-contracts" is useful for historical backfills but can be too large for full scans.
    """
    max_concurrent = max_concurrent or config.max_concurrent
    min_contracts_large = min_contracts_large or config.large_order_size_threshold
    min_notional_large = float(min_notional_large or config.large_order_notional_threshold)

    results: list[DayDownloadResult] = []

    days = list(iter_trading_days(start_date, end_date, skip_weekends=skip_weekends))
    total_days = len(days)

    for _i, d in enumerate(days, start=1):

        r = asyncio.run(
            download_day_async(
                session_date=d,
                tickers=tickers,
                max_concurrent=max_concurrent,
                min_contracts_large=min_contracts_large,
                min_notional_large=min_notional_large,
                window_ms=window_ms,
                progress_callback=progress_callback,
                contract_source=contract_source,
            )
        )
        results.append(r)

        # Progress is emitted by download_day_async in per-ticker units.

    return results
