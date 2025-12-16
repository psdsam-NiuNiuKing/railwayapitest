# railwayapitest

Minimal Railway-deployable service to benchmark and debug Massive API behavior (429s, timeouts, paging) and to tune concurrency safely.

This is intentionally small and **only** tests the REST patterns your monitor uses:
- `GET /v2/aggs/.../day` (daily close)
- `GET /v3/reference/options/contracts` (all-contracts)
- `GET /v3/trades/{option_ticker}` (trades)
- `GET /v3/quotes/{option_ticker}?timestamp.lte=...&limit=1` (quote at timestamp)

It also exposes a **real-pipeline runner** endpoint that uses the exact same downloader
as the Railway Streamlit app and writes the same artifacts under `/data/intradayoptionflow/YYYY-MM-DD/`.

## Deploy to Railway (separate project)

1. Create a new Railway project from this repo.
2. Root directory should be the repo root (default).
3. Add environment variables:
   - `MASSIVE_API_KEY` (required)
   - `MAX_CONCURRENT` (optional, start at `100`)
   - `MAX_TICKERS_CONCURRENT` (optional, start at `5`)
   - `STORE_TRADES` (optional: `true` to also write `trades.parquet`)
   - `CONTRACTS_LIMIT` (optional, start at `1000`)
   - `EXPIRATION_HORIZON_DAYS` (optional, start at `365`)
   - `STRIKE_LOW_MULT` / `STRIKE_HIGH_MULT` (optional, start at `0.7` / `1.3`)
   - `INCLUDE_EXPIRED_CONTRACTS` (optional, default `false`)
   - `HTTP_TIMEOUT_TOTAL` (optional, start at `60`)
   - `LOG_LEVEL` (optional: `INFO` or `DEBUG`)
   - `MASSIVE_BENCH_TRACE` (optional: `true` to log per-request lines)
4. Deploy.

The service exposes:
- `GET /health`
- `GET /docs` (interactive Swagger UI)
- `POST /bench/contracts`
- `POST /bench/trades`
- `POST /bench/quote_at`
- `POST /bench/daily_close`
- `POST /run/download_day` (real pipeline: writes `/data/intradayoptionflow/YYYY-MM-DD/`)

## Local run

From repo root:

- `python -m venv .venv`
- `.venv\\Scripts\\activate`
- `pip install -r railwayapitest/requirements.txt`
- `set MASSIVE_API_KEY=...`
- `uvicorn railwayapitest.app:app --reload --port 8000`
 - `uvicorn app:app --reload --port 8000`

## Example curl

### Contracts

`curl -X POST http://localhost:8000/bench/contracts -H "Content-Type: application/json" -d "{\"tickers\":[\"SPY\",\"QQQ\"],\"as_of\":\"2025-11-24\",\"include_sample_option_tickers\":true,\"sample_limit\":5}"`

### Trades (first 2 pages per contract)

`curl -X POST http://localhost:8000/bench/trades -H "Content-Type: application/json" -d "{\"option_tickers\":[\"O:SPY251219C00600000\"],\"date\":\"2025-11-24\",\"max_pages_per_contract\":2}"`

### Quote-at-timestamp (repeated)

`curl -X POST http://localhost:8000/bench/quote_at -H "Content-Type: application/json" -d "{\"option_ticker\":\"O:SPY251219C00600000\",\"timestamp_ns\":1732463400000000000,\"requests\":50}"`

### Daily close (repeated)

`curl -X POST http://localhost:8000/bench/daily_close -H "Content-Type: application/json" -d "{\"ticker\":\"SPY\",\"date\":\"2025-11-24\",\"requests\":50}"`

### Real pipeline run (writes parquet/json artifacts)

`curl -X POST http://localhost:8000/run/download_day -H "Content-Type: application/json" -d "{\"session_date\":\"2025-11-24\",\"tickers\":[\"SPY\",\"QQQ\"],\"max_concurrent\":100,\"max_tickers_concurrent\":5,\"contract_source\":\"all-contracts\"}"`

## What to send back

From Railway logs (or local terminal), please paste:
- the full log block around a run (especially any `429` lines)
- the JSON response from the endpoint
- the env values you used (`MAX_CONCURRENT`, `HTTP_TIMEOUT_TOTAL`, `MASSIVE_BENCH_TRACE`)

## Findings (keep for later)

These are notes from initial Railway benchmarks (Dec 2025) to apply later when tuning the Railway app downloader.

### Biggest bottleneck: contract discovery pagination

Contract discovery via `v3/reference/options/contracts` is dominated by pagination latency, not concurrency.

Recommended knobs (in order of impact):

1) Increase `limit` (Massive appears to honor higher limits)
- Example: `limit=1000` reduced SPY pages from ~29 to ~7 and total time from ~20s to ~6s.

2) Avoid the extra `expired=true` pass unless you have evidence it adds needed results
- With `expiration_date.gte=as_of`, querying `expired=true` is usually redundant.
- Using `include_expired=false` avoids doubling page fetch work.

3) Narrow the universe when appropriate
- Example: `expiration_horizon_days=60` and strike band `0.9..1.1` reduced SPY to ~3 pages and ~1.8s.

### Quotes are fast and not rate-limited (good news)

`v3/quotes` with `timestamp.lte` (`limit=1`, `order=desc`) sustained high request rates with 0 observed 429s during tests (e.g. 300 requests at concurrency 250).

### Trades are also fine

Single-contract `v3/trades` fetches were low-latency in tests and not rate-limited.

### How this maps to the Railway app

- For historical runs, focus on optimizing contract discovery (paging). Use `limit=1000` and skip `expired=true` unless required.
- For the Railway full scan UI, you may not need aggressive contract paging optimization if you’re intentionally scanning a smaller subset.
