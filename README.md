# railwayapitest

Minimal Railway-deployable service to benchmark and debug Massive API behavior (429s, timeouts, paging) and to tune concurrency safely.

This is intentionally small and **only** tests the REST patterns your monitor uses:
- `GET /v2/aggs/.../day` (daily close)
- `GET /v3/reference/options/contracts` (all-contracts)
- `GET /v3/trades/{option_ticker}` (trades)
- `GET /v3/quotes/{option_ticker}?timestamp.lte=...&limit=1` (quote at timestamp)

## Deploy to Railway (separate project)

1. Create a new Railway project from this repo.
2. Set **Root Directory** to `railwayapitest` (monorepo mode).
3. Add environment variables:
   - `MASSIVE_API_KEY` (required)
   - `MAX_CONCURRENT` (optional, start at `250`)
   - `HTTP_TIMEOUT_TOTAL` (optional, start at `60`)
   - `LOG_LEVEL` (optional: `INFO` or `DEBUG`)
   - `MASSIVE_BENCH_TRACE` (optional: `true` to log per-request lines)
4. Deploy.

The service exposes:
- `GET /health`
- `POST /bench/contracts`
- `POST /bench/trades`
- `POST /bench/quote_at`
- `POST /bench/daily_close`

## Local run

From repo root:

- `python -m venv .venv`
- `.venv\\Scripts\\activate`
- `pip install -r railwayapitest/requirements.txt`
- `set MASSIVE_API_KEY=...`
- `uvicorn railwayapitest.app:app --reload --port 8000`

## Example curl

### Contracts

`curl -X POST http://localhost:8000/bench/contracts -H "Content-Type: application/json" -d "{\"tickers\":[\"SPY\",\"QQQ\"],\"as_of\":\"2025-11-24\"}"`

### Trades (first 2 pages per contract)

`curl -X POST http://localhost:8000/bench/trades -H "Content-Type: application/json" -d "{\"option_tickers\":[\"O:SPY251219C00600000\"],\"date\":\"2025-11-24\",\"max_pages_per_contract\":2}"`

### Quote-at-timestamp (repeated)

`curl -X POST http://localhost:8000/bench/quote_at -H "Content-Type: application/json" -d "{\"option_ticker\":\"O:SPY251219C00600000\",\"timestamp_ns\":1732463400000000000,\"requests\":50}"`

### Daily close (repeated)

`curl -X POST http://localhost:8000/bench/daily_close -H "Content-Type: application/json" -d "{\"ticker\":\"SPY\",\"date\":\"2025-11-24\",\"requests\":50}"`

## What to send back

From Railway logs (or local terminal), please paste:
- the full log block around a run (especially any `429` lines)
- the JSON response from the endpoint
- the env values you used (`MAX_CONCURRENT`, `HTTP_TIMEOUT_TOTAL`, `MASSIVE_BENCH_TRACE`)
