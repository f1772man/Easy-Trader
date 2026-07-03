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

## Log Analysis Reference (updated 2026-07-03)

Log files: `/tmp/easy_trader_YYYYMMDD.log` (one per trading day, ~90–130 KB).

### Analysis checklist per day

**[1] Scenario verification**
- Phase1 (08:50): grep `gate=` `confidence=` `scan_hm` `market_top_n` — confirm market_analysis/latest was read before market open.
- Phase2 (09:01–09:10): grep `composite` `delta` `clamp` `클램핑` — KOSPI+KOSDAQ score → scan time/count adjustment. Dual-drop clamping forces `-3` adjustment when both indices fall.
- Confirm actual scan time and selected stock count match Phase2 decisions. Mismatches so far are always "pool shortage" (Firestore `strategy_results` has too few candidates), not a logic bug.

**[2] Trade history**
- BUY: grep `매수|BUY|진입` → time, ticker, price, reason (GC+전고돌파 / 거래량폭발 / 전일고가돌파)
- SELL: grep `매도|SELL|청산` → time, reason (트레일링 / 손절 / EMA데드크로스 / 상한가이탈 / 시간청산), P&L
- Check for **overnight carry** positions: any BUY after ~15:00 with no same-day SELL → will appear as "전일이월"손절at 09:00 next day.

**[3] Known recurring anomalies (seen in June 2026)**
- **WS disconnects**: 20 of 20 trading days had at least one `WARNING WS 연결 끊김`. Pattern: "no close frame" → "timed out during handshake". Tick gaps during reconnect may distort 5-min candles.
- **Late-day BUY (15:00–15:30)**: BUY signals after 15:00 cannot be closed same day → forced next-day loss. Seen 06/09 15:19 and 06/22 15:03–15:14 (4 positions, all stopped out next morning).
- **Intraday restart**: If engine starts after 09:00 (e.g., 06/05 09:23), Phase1 and Phase2 are skipped entirely — scan runs without market gate evaluation. Look for absence of `gate=` log before first trade.
- **grpc AuthMetadataPluginCallback exception**: Occurs after SSLError on `/token`. Seen 06/05 as cascading failure (WS storm → SSL error → grpc auth failure → Firestore transaction error → RTDB disconnect). Isolated to that day.
- **Telegram send errors**: `Read timed out` or `ConnectionResetError(104)` — alert may be lost. Seen 06/05 (×2), 06/26 (×1).
- **P&L log omission**: Occasional SELL log with no pnl field (seen 06/05 코웨이). Daily totals may be understated on those days.

**[4] June 2026 baseline**
- Trading days: 20 / Win: 40 / Loss: 53 / Win rate: 43%
- Total realized P&L: +660,935 KRW (paper trading, `KIS_SVR=vps`)
- Most profit came from overnight carry wins (e.g., 06/18 SK하이닉스 이월 +241,000원, 삼성전기 이월 +129,000원)
- Worst single day: 06/10 −96,700원 (2 prior-day carryovers + 4 new stop-outs)

### Grep cheatsheet for log analysis
```bash
# Phase1/2 scenario
grep -E "gate=|confidence=|scan_hm|market_top_n|composite|delta|clamp|클램핑" /tmp/easy_trader_YYYYMMDD.log

# Trade events
grep -E "매수|매도|BUY|SELL|진입|청산|trailing|손절|profit|P&L|손익|reason" /tmp/easy_trader_YYYYMMDD.log

# Anomalies
grep -E "ERROR|WARNING|Exception|Traceback|WebSocket|reconnect|재연결|오류|실패|불일치" /tmp/easy_trader_YYYYMMDD.log

# Overnight carry check (BUY after 15:00)
grep "BUY\|매수" /tmp/easy_trader_YYYYMMDD.log | awk -F'[: ]' '$2>=15'
```
