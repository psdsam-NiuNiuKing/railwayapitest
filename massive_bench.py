from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any

import aiohttp
import websocket

LOGGER = logging.getLogger(__name__)

MASSIVE_REST_BASE = "https://api.massive.com"


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class RequestStats:
    total: int = 0
    ok: int = 0
    status_counts: dict[int, int] = field(default_factory=dict)
    exception_count: int = 0
    rate_limited_429: int = 0
    retries: int = 0
    bytes_read: int = 0
    durations_ms: list[int] = field(default_factory=list)

    def observe(self, status: int | None, duration_ms: int, bytes_read: int, retries: int = 0) -> None:
        self.total += 1
        if status is None:
            self.exception_count += 1
        else:
            self.status_counts[status] = self.status_counts.get(status, 0) + 1
            if status == 200:
                self.ok += 1
            if status == 429:
                self.rate_limited_429 += 1
        if retries:
            self.retries += retries
        self.bytes_read += int(bytes_read or 0)
        self.durations_ms.append(int(duration_ms))

    def summary(self) -> dict[str, Any]:
        if self.durations_ms:
            d = sorted(self.durations_ms)
            p50 = d[len(d) // 2]
            p90 = d[int(len(d) * 0.90) - 1]
            p99 = d[int(len(d) * 0.99) - 1]
            avg = sum(d) / len(d)
        else:
            p50 = p90 = p99 = avg = 0
        return {
            "requests_total": self.total,
            "requests_ok": self.ok,
            "status_counts": dict(sorted(self.status_counts.items())),
            "exceptions": self.exception_count,
            "rate_limited_429": self.rate_limited_429,
            "retries": self.retries,
            "bytes_read": self.bytes_read,
            "latency_ms": {"p50": int(p50), "p90": int(p90), "p99": int(p99), "avg": float(avg)},
        }


async def _get_json_with_retries(
    session: aiohttp.ClientSession,
    url: str,
    headers: dict[str, str],
    params: dict[str, Any] | None,
    *,
    max_attempts: int = 4,
    trace: bool = False,
) -> tuple[int | None, dict[str, Any] | None, str | None, int, int, int]:
    """Returns (status, json, err_text, retries_used, duration_ms, bytes_read)."""
    start = _now_ms()
    bytes_read = 0

    for attempt in range(1, max_attempts + 1):
        try:
            async with session.get(url, params=params, headers=headers) as resp:
                status = resp.status

                if status == 429:
                    retry_after = resp.headers.get("Retry-After")
                    try:
                        wait_s = float(retry_after) if retry_after else min(10.0, 0.5 * (2 ** (attempt - 1)))
                    except Exception:
                        wait_s = min(10.0, 0.5 * (2 ** (attempt - 1)))

                    if trace:
                        LOGGER.warning(
                            "429 rate limited: attempt=%s wait_s=%.2f url=%s",
                            attempt,
                            wait_s,
                            url,
                        )

                    if attempt < max_attempts:
                        await asyncio.sleep(max(0.05, min(wait_s, 30.0)))
                        continue

                if status != 200:
                    try:
                        txt = (await resp.text())[:800]
                        bytes_read += len(txt.encode("utf-8", errors="ignore"))
                    except Exception:
                        txt = None
                    duration_ms = _now_ms() - start
                    if trace:
                        LOGGER.info("HTTP %s in %sms url=%s", status, duration_ms, url)
                    return status, None, txt, attempt - 1, duration_ms, bytes_read

                data = await resp.json()
                try:
                    # best-effort size estimate
                    raw = await resp.read()
                    bytes_read += len(raw)
                except Exception:
                    pass
                duration_ms = _now_ms() - start
                if trace:
                    LOGGER.info("HTTP 200 in %sms url=%s", duration_ms, url)
                return 200, data, None, attempt - 1, duration_ms, bytes_read

        except Exception as e:
            if trace:
                LOGGER.warning("HTTP exception attempt=%s url=%s err=%s", attempt, url, str(e)[:200])
            if attempt < max_attempts:
                await asyncio.sleep(min(10.0, 0.5 * (2 ** (attempt - 1))))
                continue
            duration_ms = _now_ms() - start
            return None, None, str(e), attempt - 1, duration_ms, bytes_read

    duration_ms = _now_ms() - start
    return None, None, "unexpected retry loop exit", max_attempts, duration_ms, bytes_read


def _to_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


async def create_session(*, max_concurrent: int, timeout_total_s: int) -> aiohttp.ClientSession:
    connector = aiohttp.TCPConnector(
        limit=int(max_concurrent),
        limit_per_host=int(max_concurrent),
        force_close=False,
        enable_cleanup_closed=True,
        ttl_dns_cache=600,
        keepalive_timeout=60,
    )
    timeout = aiohttp.ClientTimeout(total=float(timeout_total_s), connect=30, sock_read=max(30, int(timeout_total_s)))
    return aiohttp.ClientSession(connector=connector, timeout=timeout)


async def bench_daily_close(
    ticker: str,
    session_date: date,
    api_key: str,
    *,
    requests: int = 10,
    max_concurrent: int = 50,
    timeout_total_s: int = 60,
) -> dict[str, Any]:
    trace = _env_bool("MASSIVE_BENCH_TRACE", False)
    headers = {"Authorization": f"Bearer {api_key}"}
    url = f"{MASSIVE_REST_BASE}/v2/aggs/ticker/{ticker}/range/1/day/{session_date.isoformat()}/{session_date.isoformat()}"
    params: dict[str, Any] = {"adjusted": "true"}

    sem = asyncio.Semaphore(int(max_concurrent))
    stats = RequestStats()

    async with await create_session(max_concurrent=max_concurrent, timeout_total_s=timeout_total_s) as session:
        async def one() -> None:
            async with sem:
                status, data, err, retries, dur, nbytes = await _get_json_with_retries(
                    session, url, headers, params, max_attempts=4, trace=trace
                )
                stats.observe(status, dur, nbytes, retries=retries)
                if status != 200 and trace:
                    LOGGER.info("daily_close error status=%s err=%s", status, (err or "")[:200])
                _ = data

        started = time.time()
        await asyncio.gather(*[one() for _ in range(int(requests))])
        elapsed = time.time() - started

    out = stats.summary()
    out.update({"endpoint": "v2/aggs/day", "ticker": ticker, "date": session_date.isoformat(), "elapsed_s": elapsed})
    return out


async def bench_contracts_all_contracts(
    tickers: list[str],
    as_of: date,
    api_key: str,
    *,
    limit: int = 1000,
    expiration_horizon_days: int = 365,
    strike_band: tuple[float, float] | None = (0.7, 1.3),
    spot_hint: float | None = None,
    include_expired: bool = False,
    auto_spot_hint: bool = False,
    include_sample_option_tickers: bool = False,
    sample_limit: int = 10,
    max_concurrent: int = 100,
    timeout_total_s: int = 120,
) -> dict[str, Any]:
    trace = _env_bool("MASSIVE_BENCH_TRACE", False)
    headers = {"Authorization": f"Bearer {api_key}"}
    base_url = f"{MASSIVE_REST_BASE}/v3/reference/options/contracts"

    sem = asyncio.Semaphore(int(max_concurrent))
    stats = RequestStats()

    exp_lte = as_of + timedelta(days=max(1, int(expiration_horizon_days)))

    base_params_template: dict[str, Any] = {
        "as_of": as_of.isoformat(),
        "expiration_date.gte": as_of.isoformat(),
        "expiration_date.lte": exp_lte.isoformat(),
        "limit": int(limit),
    }

    per_ticker: dict[str, dict[str, Any]] = {}

    def _pick_sample_option_tickers(
        candidates: list[tuple[str, float | None, date | None]],
        *,
        spot: float | None,
        as_of_d: date,
        n: int,
    ) -> list[str]:
        if n <= 0:
            return []
        if not candidates:
            return []

        # If we don't have a spot hint, just return the first unique tickers.
        if not spot or spot <= 0:
            out: list[str] = []
            seen: set[str] = set()
            for ot, _strike, _exp in candidates:
                if ot in seen:
                    continue
                seen.add(ot)
                out.append(ot)
                if len(out) >= n:
                    break
            return out

        buckets: list[tuple[int, int]] = [(0, 7), (8, 30), (31, 90), (91, 3650)]
        per_bucket: list[list[tuple[float, str]]] = [[] for _ in buckets]

        for ot, strike, exp in candidates:
            if strike is None or exp is None:
                continue
            dte = (exp - as_of_d).days
            if dte < 0:
                continue
            dist = abs(float(strike) - float(spot)) / float(spot)
            dte_penalty = min(3650.0, float(dte)) / 3650.0
            score = dist + 0.15 * dte_penalty
            for i, (lo, hi) in enumerate(buckets):
                if lo <= dte <= hi:
                    per_bucket[i].append((score, ot))
                    break

        if not any(per_bucket):
            return [c[0] for c in candidates[:n]]

        for b in per_bucket:
            b.sort(key=lambda x: x[0])

        out: list[str] = []
        seen: set[str] = set()
        while len(out) < n:
            progressed = False
            for b in per_bucket:
                if not b:
                    continue
                _score, ot = b.pop(0)
                if ot in seen:
                    continue
                seen.add(ot)
                out.append(ot)
                progressed = True
                if len(out) >= n:
                    break
            if not progressed:
                break

        if len(out) < n:
            for ot, _strike, _exp in candidates:
                if ot in seen:
                    continue
                seen.add(ot)
                out.append(ot)
                if len(out) >= n:
                    break

        return out

    async def _fetch_daily_close_once(
        session: aiohttp.ClientSession,
        ticker: str,
    ) -> float | None:
        url = f"{MASSIVE_REST_BASE}/v2/aggs/ticker/{ticker}/range/1/day/{as_of.isoformat()}/{as_of.isoformat()}"
        params: dict[str, Any] = {"adjusted": "true"}
        status, data, err, retries, dur, nbytes = await _get_json_with_retries(
            session,
            url,
            headers,
            params,
            max_attempts=4,
            trace=trace,
        )
        stats.observe(status, dur, nbytes, retries=retries)
        if status != 200 or not data:
            if trace:
                LOGGER.info("daily_close hint failed ticker=%s status=%s err=%s", ticker, status, (err or "")[:200])
            return None
        results = data.get("results", []) or []
        if not results:
            return None
        close_v = (results[0] or {}).get("c")
        try:
            close_f = float(close_v)
        except Exception:
            return None
        return close_f if close_f > 0 else None

    async with await create_session(max_concurrent=max_concurrent, timeout_total_s=timeout_total_s) as session:
        async def fetch_for_ticker(ticker: str) -> None:
            by_ot: dict[str, Any] = {}
            candidates: list[tuple[str, float | None, date | None]] = []
            pages = 0
            results = 0

            spot_used: float | None = None
            if spot_hint and spot_hint > 0:
                spot_used = float(spot_hint)
            elif auto_spot_hint:
                # One extra request per ticker, but often reduces contracts pages by 5-50x.
                spot_used = await _fetch_daily_close_once(session, ticker)

            base_params: dict[str, Any] = dict(base_params_template)
            if strike_band and spot_used and spot_used > 0:
                try:
                    lo, hi = strike_band
                    low_v = float(spot_used) * float(lo)
                    high_v = float(spot_used) * float(hi)
                    if low_v > 0 and high_v > low_v:
                        base_params["strike_price.gte"] = round(low_v, 4)
                        base_params["strike_price.lte"] = round(high_v, 4)
                except Exception:
                    pass

            expired_flags = ("false", "true") if include_expired else ("false",)
            for expired_flag in expired_flags:
                next_url: str | None = base_url
                current_params: dict[str, Any] | None = dict(base_params)
                current_params["underlying_ticker"] = ticker
                current_params["expired"] = expired_flag

                while next_url:
                    async with sem:
                        status, data, err, retries, dur, nbytes = await _get_json_with_retries(
                            session,
                            str(next_url),
                            headers,
                            current_params,
                            max_attempts=4,
                            trace=trace,
                        )
                        stats.observe(status, dur, nbytes, retries=retries)

                    if status != 200 or not data:
                        if trace:
                            LOGGER.info(
                                "contracts failed ticker=%s expired=%s status=%s err=%s",
                                ticker,
                                expired_flag,
                                status,
                                (err or "")[:200],
                            )
                        break

                    pages += 1
                    for item in (data.get("results", []) or []):
                        details = item.get("details") or {}
                        ot = details.get("ticker") or item.get("ticker") or item.get("option_ticker")
                        if ot:
                            ot_s = str(ot)
                            by_ot.setdefault(ot_s, 1)
                            if include_sample_option_tickers:
                                strike = details.get("strike_price")
                                exp_raw = details.get("expiration_date")
                                exp_d: date | None = None
                                try:
                                    if exp_raw:
                                        exp_d = _to_date(str(exp_raw))
                                except Exception:
                                    exp_d = None
                                strike_f: float | None
                                try:
                                    strike_f = float(strike) if strike is not None else None
                                except Exception:
                                    strike_f = None
                                candidates.append((ot_s, strike_f, exp_d))
                    results = len(by_ot)

                    next_url = data.get("next_url")
                    current_params = None

            sample_ots: list[str] = []
            if include_sample_option_tickers:
                sample_ots = _pick_sample_option_tickers(
                    candidates,
                    spot=(float(spot_used) if spot_used else None),
                    as_of_d=as_of,
                    n=int(sample_limit),
                )

            per_ticker[ticker] = {
                "contracts_unique": int(results),
                "pages": int(pages),
                "spot_hint_used": (round(float(spot_used), 4) if spot_used else None),
                "strike_band": (list(strike_band) if strike_band else None),
                **({"sample_option_tickers": sample_ots} if include_sample_option_tickers else {}),
            }

        started = time.time()
        await asyncio.gather(*[fetch_for_ticker(t.strip().upper()) for t in tickers if t.strip()])
        elapsed = time.time() - started

    out = stats.summary()
    out.update(
        {
            "endpoint": "v3/reference/options/contracts",
            "as_of": as_of.isoformat(),
            "tickers": tickers,
            "elapsed_s": elapsed,
            "per_ticker": per_ticker,
        }
    )
    return out


async def bench_snapshot_options(
    ticker: str,
    as_of: date,
    api_key: str,
    *,
    limit: int = 1000,
    max_pages: int | None = None,
    max_concurrent: int = 50,
    timeout_total_s: int = 120,
) -> dict[str, Any]:
    """Benchmark /v3/snapshot/options/{ticker} pagination cost."""
    trace = _env_bool("MASSIVE_BENCH_TRACE", False)
    headers = {"Authorization": f"Bearer {api_key}"}
    base_url = f"{MASSIVE_REST_BASE}/v3/snapshot/options/{ticker}"
    # Match local app behavior: snapshot/options is used for the current day and
    # is called without an as_of/date parameter.
    first_params: dict[str, Any] = {"limit": int(limit)}

    stats = RequestStats()

    async with await create_session(max_concurrent=max_concurrent, timeout_total_s=timeout_total_s) as session:
        started = time.time()

        last_status: int | None = None
        last_error: str | None = None

        next_url: str | None = base_url
        current_params: dict[str, Any] | None = dict(first_params)
        contracts = 0
        pages = 0

        while next_url:
            status, data, err, retries, dur, nbytes = await _get_json_with_retries(
                session,
                str(next_url),
                headers,
                current_params,
                max_attempts=4,
                trace=trace,
            )
            stats.observe(status, dur, nbytes, retries=retries)
            if status != 200 or not data:
                last_status = status
                last_error = err
                if trace:
                    LOGGER.info("snapshot/options failed ticker=%s status=%s err=%s", ticker, status, (err or "")[:200])
                break

            pages += 1
            contracts += len(data.get("results", []) or [])

            if max_pages is not None and pages >= int(max_pages):
                break

            next_url = data.get("next_url")
            current_params = None

        elapsed = time.time() - started

    out = stats.summary()
    out.update(
        {
            "endpoint": "v3/snapshot/options/{ticker}",
            "ticker": ticker,
            "as_of": as_of.isoformat(),
            "limit": int(limit),
            "pages": int(pages),
            "contracts": int(contracts),
            "elapsed_s": elapsed,
            "last_status": last_status,
            "last_error": (last_error[:800] if isinstance(last_error, str) else last_error),
        }
    )
    return out


def bench_ws_trades(
    api_key: str,
    *,
    websocket_url: str = "wss://socket.massive.com/options",
    subscribe_params: str = "T.*",
    duration_s: float = 5.0,
    max_messages: int = 5000,
) -> dict[str, Any]:
    """Benchmark WebSocket auth+subscribe throughput (messages observed)."""
    started = time.time()
    messages = 0
    trade_events = 0
    status_events = 0
    errors: list[str] = []

    def on_open(ws: websocket.WebSocketApp) -> None:
        try:
            ws.send(json.dumps({"action": "auth", "params": api_key}))
        except Exception as e:
            errors.append(f"on_open auth failed: {e}")

    def on_message(ws: websocket.WebSocketApp, message: str) -> None:
        nonlocal messages, trade_events, status_events
        messages += 1

        try:
            data = json.loads(message)
        except Exception:
            data = None

        def handle_one(m: Any) -> None:
            nonlocal trade_events, status_events
            if not isinstance(m, dict):
                return
            ev = m.get("ev")
            if ev == "status":
                status_events += 1
                if m.get("status") == "auth_success":
                    try:
                        ws.send(json.dumps({"action": "subscribe", "params": subscribe_params}))
                    except Exception as e:
                        errors.append(f"subscribe failed: {e}")
            elif ev == "T":
                trade_events += 1

        if isinstance(data, list):
            for m in data:
                handle_one(m)
        else:
            handle_one(data)

        if messages >= int(max_messages) or (time.time() - started) >= float(duration_s):
            try:
                ws.close()
            except Exception:
                pass

    def on_error(ws: websocket.WebSocketApp, error: Any) -> None:
        errors.append(str(error)[:300])

    def on_close(ws: websocket.WebSocketApp, close_status_code: Any, close_msg: Any) -> None:
        _ = close_status_code
        _ = close_msg

    ws = websocket.WebSocketApp(
        websocket_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.run_forever(ping_interval=30, ping_timeout=10)

    elapsed = time.time() - started
    mps = messages / elapsed if elapsed > 0 else 0.0
    tps = trade_events / elapsed if elapsed > 0 else 0.0

    return {
        "endpoint": "ws/options",
        "websocket_url": websocket_url,
        "subscribe": subscribe_params,
        "duration_s": float(duration_s),
        "elapsed_s": float(elapsed),
        "messages": int(messages),
        "trade_events": int(trade_events),
        "status_events": int(status_events),
        "messages_per_s": float(mps),
        "trade_events_per_s": float(tps),
        "errors": errors,
    }


async def bench_trades(
    option_tickers: list[str],
    session_date: date,
    api_key: str,
    *,
    max_concurrent: int = 100,
    timeout_total_s: int = 180,
    max_pages_per_contract: int = 2,
) -> dict[str, Any]:
    trace = _env_bool("MASSIVE_BENCH_TRACE", False)
    headers = {"Authorization": f"Bearer {api_key}"}

    sem = asyncio.Semaphore(int(max_concurrent))
    stats = RequestStats()

    per_contract: dict[str, dict[str, Any]] = {}

    async with await create_session(max_concurrent=max_concurrent, timeout_total_s=timeout_total_s) as session:
        async def fetch_contract(ot: str) -> None:
            url = f"{MASSIVE_REST_BASE}/v3/trades/{ot}"
            params: dict[str, Any] | None = {
                "timestamp": session_date.isoformat(),
                "limit": 50000,
                "order": "asc",
                "sort": "timestamp",
            }
            next_url: str | None = url
            pages = 0
            trade_rows = 0

            while next_url and pages < int(max_pages_per_contract):
                async with sem:
                    status, data, err, retries, dur, nbytes = await _get_json_with_retries(
                        session,
                        str(next_url),
                        headers,
                        params,
                        max_attempts=4,
                        trace=trace,
                    )
                    stats.observe(status, dur, nbytes, retries=retries)

                if status != 200 or not data:
                    if trace:
                        LOGGER.info("trades failed ot=%s status=%s err=%s", ot, status, (err or "")[:200])
                    break

                pages += 1
                trade_rows += len(data.get("results", []) or [])
                next_url = data.get("next_url")
                params = None

            per_contract[ot] = {"pages": int(pages), "trades_counted": int(trade_rows)}

        started = time.time()
        await asyncio.gather(*[fetch_contract(ot) for ot in option_tickers if ot])
        elapsed = time.time() - started

    out = stats.summary()
    out.update(
        {
            "endpoint": "v3/trades",
            "date": session_date.isoformat(),
            "contracts": option_tickers,
            "elapsed_s": elapsed,
            "per_contract": per_contract,
            "max_pages_per_contract": int(max_pages_per_contract),
        }
    )
    return out


async def bench_quote_at_timestamp(
    option_ticker: str,
    timestamp_ns: int,
    api_key: str,
    *,
    requests: int = 25,
    max_concurrent: int = 50,
    timeout_total_s: int = 60,
) -> dict[str, Any]:
    trace = _env_bool("MASSIVE_BENCH_TRACE", False)
    headers = {"Authorization": f"Bearer {api_key}"}
    url = f"{MASSIVE_REST_BASE}/v3/quotes/{option_ticker}"
    params = {
        "timestamp.lte": str(int(timestamp_ns)),
        "limit": 1,
        "order": "desc",
        "sort": "timestamp",
    }

    sem = asyncio.Semaphore(int(max_concurrent))
    stats = RequestStats()

    async with await create_session(max_concurrent=max_concurrent, timeout_total_s=timeout_total_s) as session:
        async def one() -> None:
            async with sem:
                status, data, err, retries, dur, nbytes = await _get_json_with_retries(
                    session, url, headers, params, max_attempts=4, trace=trace
                )
                stats.observe(status, dur, nbytes, retries=retries)
                if status != 200 and trace:
                    LOGGER.info("quote_at error status=%s err=%s", status, (err or "")[:200])
                _ = data

        started = time.time()
        await asyncio.gather(*[one() for _ in range(int(requests))])
        elapsed = time.time() - started

    out = stats.summary()
    out.update(
        {
            "endpoint": "v3/quotes (timestamp.lte limit=1)",
            "option_ticker": option_ticker,
            "timestamp_ns": int(timestamp_ns),
            "elapsed_s": elapsed,
        }
    )
    return out
