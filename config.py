"""Config for railwayapitest.

This is intentionally aligned with the Railway app's downloader expectations:
- Uses MASSIVE_API_KEY
- Uses MAX_CONCURRENT / MAX_TICKERS_CONCURRENT
- Writes outputs under /data/intradayoptionflow/YYYY-MM-DD
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


def _env_int_first(names: list[str], default: int) -> int:
    for n in names:
        raw = os.environ.get(n)
        if raw is None or raw == "":
            continue
        try:
            return int(raw)
        except Exception:
            continue
    return int(default)


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass
class AppConfig:
    massive_api_key: str = field(default_factory=lambda: os.environ.get("MASSIVE_API_KEY", ""))

    # Storage
    data_root: Path = field(default_factory=lambda: Path(os.environ.get("DATA_ROOT", "/data")))
    intraday_flow_root: Path = field(
        default_factory=lambda: Path(os.environ.get("INTRADAY_FLOW_ROOT", str(Path(os.environ.get("DATA_ROOT", "/data")) / "intradayoptionflow")))
    )

    # Concurrency
    max_concurrent: int = field(default_factory=lambda: _env_int("MAX_CONCURRENT", 100))
    max_tickers_concurrent: int = field(default_factory=lambda: _env_int_first(["MAX_TICKERS_CONCURRENT"], 5))

    # Contract discovery tuning
    contracts_limit: int = field(default_factory=lambda: _env_int_first(["CONTRACTS_LIMIT"], 1000))
    snapshot_contracts_limit: int = field(default_factory=lambda: _env_int_first(["SNAPSHOT_CONTRACTS_LIMIT"], 250))
    expiration_horizon_days: int = field(default_factory=lambda: _env_int_first(["EXPIRATION_HORIZON_DAYS"], 365))
    strike_low_mult: float = field(default_factory=lambda: _env_float("STRIKE_LOW_MULT", 0.7))
    strike_high_mult: float = field(default_factory=lambda: _env_float("STRIKE_HIGH_MULT", 1.3))
    include_expired_contracts: bool = field(default_factory=lambda: _env_bool("INCLUDE_EXPIRED_CONTRACTS", False))

    # Output detail
    store_trades: bool = field(default_factory=lambda: _env_bool("STORE_TRADES", False))

    # Thresholds
    large_order_size_threshold: int = field(default_factory=lambda: _env_int("LARGE_ORDER_SIZE_THRESHOLD", 1000))
    large_order_notional_threshold: int = field(
        default_factory=lambda: _env_int_first(
            ["LARGE_ORDER_DOLLAR_THRESHOLD", "LARGE_ORDER_NOTIONAL_THRESHOLD"],
            10000,
        )
    )


config = AppConfig()
