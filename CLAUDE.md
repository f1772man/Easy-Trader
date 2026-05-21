# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Easy Trader** is a Korean stock (KRX) automated trading bot using the KIS (Korea Investment & Securities) Open API. It runs as a Flask + background-thread service deployed in Docker.

## Running and Deployment

```bash
# Local (dev/test) — requires .env with KIS credentials
python main.py

# Docker rebuild & restart (production)
bash rebuild.sh

# Run a test script inside Docker
docker exec -it easy-trader python3 test_trade_filter.py

# Health check
curl http://localhost:8080/health
curl http://localhost:8080/status
```

No formal test suite exists. `test_trade_filter.py` is a manual validation script for the trade-amount filter logic (read-only, does not place orders).

## Key Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `KIS_SVR` | `vps` | `prod` = live, `vps` = paper trading |
| `KIS_APP_KEY` / `KIS_APP_SECRET` | — | KIS REST credentials |
| `KIS_ACCOUNT_STOCK` | — | Live account number |
| `KIS_PAPER_ACCOUNT_STOCK` | — | Paper account number |
| `FIREBASE_DATABASE_URL` | — | Firebase Realtime DB URL |
| `GOOGLE_APPLICATION_CREDENTIALS` | — | Path to GCP service account JSON |
| `MAX_POSITION_SIZE` | `1000000` | Budget per position (KRW) |
| `TICK_INTERVAL_SECONDS` | `5` | Main loop interval |
| `MAX_WORKERS` | `5` | ThreadPoolExecutor pool size |
| `INCLUDE_PARTIAL_5MIN` | `false` | Include in-progress 5-min candle in strategy |
| `WATCH_SYMBOLS` | `005930,000660` | Fallback symbols if Firestore is empty |
| `REENTER_COOLDOWN_SECONDS` | `60` | Re-entry cooldown after non-trailing exit |
| `LOG_LEVEL` | `INFO` | Python logging level |

Secrets are mounted at `/app/secrets` in Docker; `kis_config/` holds the KIS access token file.

## Architecture

```
main.py                     Flask app + daemon thread launching TradingEngine
trader/
  engine.py                 Core engine (TradingEngine class)
  strategy.py               Signal logic — stateless, pure function get_strategy_signal()
  kis_auth.py               KIS REST/WS auth, token management, _url_fetch()
  kis_api.py                Thin trading API layer (buy/sell/snapshot) over kis_auth
  ws_tick_collector.py      WebSocket tick → 1-min & 5-min candle builder (WsTickCollector)
  firebase.py               Firestore + Firebase Realtime DB client (FirebaseClient)
  telegram.py               Telegram alert helpers
logging_config.py           KST-aware logging setup
strategy_filter.json        Hot-reload buy-signal blocklist (no restart needed)
test_trade_filter.py        Manual test for trade-amount filter
DATA/                       YYYYMMDD/1m/ and YYYYMMDD/5m/ CSV dumps
```

### Engine lifecycle (`TradingEngine.run`)

1. Authenticates to KIS (`kis_auth.auth`)
2. Restores positions from Firestore on restart
3. Starts `WsTickCollector` (WebSocket, separate thread)
4. Loops every `TICK_INTERVAL_SECONDS` calling `_tick()`

### Daily flow inside `_tick()`

- **08:50–09:00** — Pre-market warmup: loads yesterday's OHLCV snapshot + Firestore `strategy_results` cache for all watch symbols via ThreadPoolExecutor
- **09:20–09:25** — `_filter_by_trade_amount()`: reads Firestore `strategy_results` (summaryBadge == "진입"), fetches live price/gap/trade-value from KIS REST, selects top-10 by trade amount, writes to `target_stocks` collection, and updates `_watch_symbols`
- **09:00–15:30** — Processes each symbol in parallel; calls `get_strategy_signal()` with merged candle data (yesterday seed + today's 5-min) and acts on BUY/SELL signals
- **After 15:31** — Flushes all 1-min and 5-min candles to CSV once

### Candle data flow

`WsTickCollector` (WebSocket thread) → raw ticks → aggregated into 1-min rows → 5-min completed candles.  
`engine._process_symbol` merges: last-20 candles of yesterday's `candles_5m` (from KIS snapshot) + today's 5-min candles → DataFrame passed to `get_strategy_signal`.

### Signal logic (`strategy.py`)

`get_strategy_signal(params)` is a pure function (no I/O). It returns `{"signal": "BUY"|"SELL"|"HOLD", "reason": str, "energy": dict}`. The engine layers additional guards on top:

- `strategy_filter.json` blocklist (hot-reloaded every 5 s) — blocks specific buy reasons without restart
- Re-entry cooldown (`_today_sold`) — prevents immediate re-buy after sell
- Order deduplication (`_inflight_orders`, same-bar key, cooldown)

### Firestore collections used

| Collection | Purpose |
|---|---|
| `strategy_results` | Daily stock analysis (upstream; engine reads only) |
| `target_stocks` | Top-10 selected symbols for the day (engine writes) |
| `positions` | Active holdings (persisted across restarts) |
| `trade_log` | BUY/SELL history |
| `sold_positions` | Same-day sold symbols (re-entry guard) |
| `meta/golden_pick_status` | Badge update timestamp for Flutter app |
| `EASY_TRADER/…` | Firebase Realtime DB: heartbeat, system status |

## Important Conventions

- All times are KST (`timezone(timedelta(hours=9))`). Time-based conditionals use `hm = now.hour * 100 + now.minute` integer comparisons (e.g., `900 <= hm <= 1530`).
- `_positions` and `_today_sold` are mutated from multiple threads; always use their respective locks.
- `strategy_filter.json` `blocked_reasons` list is the runtime kill-switch for specific buy signals — edit and save; engine picks it up within 5 seconds.
- `KIS_SVR=vps` routes all orders to the paper trading endpoint. `KIS_SVR=prod` routes to live trading.
