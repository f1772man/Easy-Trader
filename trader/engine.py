"""
engine.py
고속 트레이딩 엔진
- WebSocket 완성 5분봉 + 진행중 5분봉 사용
- strategy_results 캐시 사용
- 장전 전일 snapshot + strategy 캐시 워밍업
- ThreadPoolExecutor 병렬 종목 처리
- 빠른 루프(기본 5초) + 장중 즉시성 개선
"""

import os
import csv
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import numpy as np

import trader.kis_api as kis_api
from trader.ws_tick_collector import WsTickCollector
from trader.kis_auth import auth
from trader.strategy import get_strategy_signal, DEFAULT_CONFIG
from trader.telegram import notify_buy, notify_sell, notify_error
from trader.firebase import FirebaseClient
from trader.domestic_stock_functions import chk_holiday
from google.cloud import firestore

logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))
DATA_DIR = Path(os.environ.get("DATA_DIR", "DATA"))
CSV_COLUMNS = ["time", "open", "high", "low", "close", "volume"]
WATCH_SYMBOLS = os.environ.get("WATCH_SYMBOLS", "005930,000660").split(",")
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "5").split("#")[0].strip())
TICK_INTERVAL_SECONDS = float(os.environ.get("TICK_INTERVAL_SECONDS", "5").split("#")[0].strip())
INCLUDE_PARTIAL_5MIN = os.environ.get("INCLUDE_PARTIAL_5MIN", "false").strip().lower() in ("1", "true", "y", "yes")
HEARTBEAT_INTERVAL_SECONDS = int(os.environ.get("HEARTBEAT_INTERVAL_SECONDS", "15").split("#")[0].strip())
ORDER_COOLDOWN_SECONDS = float(os.environ.get("ORDER_COOLDOWN_SECONDS", "3").split("#")[0].strip())


# ── pandas SMA & EMA 헬퍼 ───────────────────────────────────
def _ma_at(df: pd.DataFrame, period: int, i: int) -> Optional[float]:
    if i < period - 1 or len(df) <= i:
        return None
    val = df["close"].iloc[max(0, i - period + 1):i + 1].mean()
    return float(val) if not np.isnan(val) else None

def _ema_at(df: pd.DataFrame, period: int, i: int) -> Optional[float]:
    if i < period - 1 or len(df) <= i:
        return None
    series = df["close"].iloc[:i + 1]
    val = series.ewm(span=period, adjust=False).mean().iloc[-1]
    return float(val) if not np.isnan(val) else None

# ── 리스트 → DataFrame ───────────────────────────────
def _to_df(candles: list) -> pd.DataFrame:
    if not candles:
        return pd.DataFrame(columns=CSV_COLUMNS)
    df = pd.DataFrame(candles, columns=CSV_COLUMNS)
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.reset_index(drop=True)


class TradingEngine:
    def __init__(self):
        self.firebase = FirebaseClient()
        self.cfg = DEFAULT_CONFIG.copy()

        self.is_running = False
        self.last_tick_time: Optional[str] = None

        # 포지션 (스레드 안전)
        self._positions: dict = {}
        self._positions_lock = threading.Lock()

        # 전일 snapshot 캐시
        self._prev_snapshot_cache: dict = {}
        self._snapshot_lock = threading.RLock()
        self._cache_date: str = ""

        # strategy_results 캐시
        self._strategy_cache: dict = {}
        self._strategy_lock = threading.RLock()
        self._strategy_cache_date: str = ""

        # 감시 목록 + 메타
        self._watch_symbols: list[str] = []
        self._symbol_meta: dict = {}

        # 기존 구조 호환용 버퍼/캐시
        self._1min_buffer: dict = {}
        self._buffer_lock = threading.Lock()
        self._5min_cache: dict = {}

        # WebSocket 체결 수신기 (종목 로드 후 start)
        self._collector: WsTickCollector = None

        # 루프/상태 제어
        self._last_heartbeat_ts = 0.0
        self._last_flush_date = ""
        self._last_market_scan_key = ""
        self._premarket_warmup_done_date = ""
        self._holiday_checked_date: str = ""
        self._today_opnd_yn: str = "Y"

        # 주문 중복 방지
        self._order_lock = threading.RLock()
        self._inflight_orders: set[str] = set()
        self._last_order_keys: dict[str, str] = {}
        self._last_order_ts: dict[str, float] = {}

        # 재매수 방지: 당일 익절 종목 + 매도 시각
        self._today_sold: dict[str, float] = {}   # symbol → 매도 완료 timestamp
        self._today_sold_lock = threading.Lock()  # ✅ race condition 방지
        self._reenter_cooldown_sec: float = float(
            os.environ.get("REENTER_COOLDOWN_SECONDS", "60").split("#")[0].strip()
        )  # 기본 1분 쿨다운

        # Firestore 클라이언트 (공유 싱글턴 — 매 호출마다 신규 생성 방지)
        self._fs = firestore.Client()

    # ── 종목명(코드) ──────────────────────────────────
    def _display_name(self, symbol: str) -> str:
        name = self._symbol_meta.get(symbol, {}).get("name", "")
        return f"{name}({symbol})" if name else symbol

    # ── 메인 루프 ─────────────────────────────────────
    def run(self):
        logger.info("🚀 트레이딩 엔진 시작")
        self.is_running = True

        svr = os.environ.get("KIS_SVR", "vps")
        auth(svr=svr)
        logger.info(f"✅ KIS 인증 완료 (mode={svr})")

        from trader.telegram import send_telegram
        send_telegram("🚀 EASY TRADER 엔진 시작\nmode=prod | Firebase 연결 완료")

        self._restore_positions()

        self._cache_date = datetime.now(KST).strftime("%Y%m%d")
        self._reload_watch_symbols()
        self._warmup_market_data()

        self._collector = WsTickCollector(list(self._watch_symbols))
        self._collector.start()

        while self.is_running:
            try:
                self._tick()
            except Exception as e:
                logger.error(f"❌ 틱 처리 오류: {e}")
                notify_error(str(e))
            time.sleep(TICK_INTERVAL_SECONDS)

    def stop(self):
        self.is_running = False
        try:
            self._flush_candle_csv(datetime.now(KST).strftime("%Y%m%d"))
        except Exception as e:
            logger.error(f"[CSV] stop 저장 실패: {e}")
        if self._collector:
            self._collector.stop()

    # ── 1회 틱 ────────────────────────────────────────
    def _tick(self):
        now = datetime.now(KST)
        self.last_tick_time = now.strftime("%Y-%m-%d %H:%M:%S KST")

        self._rollover_if_needed(now)
        self._maybe_update_heartbeat(now)
        self._maybe_premarket_warmup(now)

        hm = now.hour * 100 + now.minute

        if not (900 <= hm <= 1530):
            if hm >= 1531:
                self._flush_market_data_once(now.strftime("%Y%m%d"))
            return

        symbols = [s.strip() for s in self._watch_symbols if s.strip()]
        if not symbols:
            return

        tick_start = time.time()
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(self._process_symbol_timed, sym, now): sym for sym in symbols}
            for future in as_completed(futures):
                sym = futures[future]
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"종목 처리 오류 ({self._display_name(sym)}): {e}")

        elapsed = time.time() - tick_start
        logger.debug(f"[tick] {len(symbols)}종목 처리 완료 ({elapsed:.2f}s)")

    def _rollover_if_needed(self, now: datetime):
        today_str = now.strftime("%Y%m%d")
        if self._cache_date == today_str:
            return

        with self._snapshot_lock:
            self._prev_snapshot_cache.clear()
        with self._strategy_lock:
            self._strategy_cache.clear()
            self._strategy_cache_date = ""
        with self._buffer_lock:
            self._1min_buffer.clear()
            self._5min_cache.clear()

        self._cache_date = today_str
        self._last_flush_date = ""
        self._last_market_scan_key = ""
        self._premarket_warmup_done_date = ""

        with self._order_lock:
            self._inflight_orders.clear()
            self._last_order_keys.clear()
            self._last_order_ts.clear()

        self.firebase.cleanup_sold_positions()
        self._today_sold.clear()

        logger.info(f"[캐시] 날짜 변경 → snapshot/strategy/버퍼/주문가드/재매수방지 초기화 ({today_str})")
        self._reload_watch_symbols()
        if self._collector:
            self._collector.reset_day()

    def _maybe_update_heartbeat(self, now: datetime):
        ts = time.time()
        if ts - self._last_heartbeat_ts < HEARTBEAT_INTERVAL_SECONDS:
            return

        with self._positions_lock:
            holding_count = len(self._positions)

        self.firebase.update_heartbeat({
            "running": True,
            "last_tick": self.last_tick_time,
            "holding_count": holding_count,
        })
        self._last_heartbeat_ts = ts

    def _maybe_premarket_warmup(self, now: datetime):
        hm = now.hour * 100 + now.minute
        today_str = now.strftime("%Y%m%d")

        if not (850 <= hm < 900):
            return

        # 1. 휴장일 체크는 하루 1번만
        if self._holiday_checked_date != today_str:
            try:
                df = chk_holiday(bass_dt=today_str, max_depth=1)
                opnd_yn = "Y"
                if df is not None and not df.empty and "opnd_yn" in df.columns:
                    row = df[df["bass_dt"] == today_str] if "bass_dt" in df.columns else df.iloc[:1]
                    opnd_yn = str(row["opnd_yn"].iloc[0]).strip().upper() if not row.empty else "Y"
            except Exception as e:
                logger.warning(f"[장전워밍업] 휴장일 조회 실패({e}) → 개장으로 간주")
                opnd_yn = "Y"
            self._today_opnd_yn = opnd_yn
            self._holiday_checked_date = today_str

        if self._today_opnd_yn != "Y":
            logger.info(f"[장전워밍업] {today_str} 휴장일 → 스킵")
            self._premarket_warmup_done_date = today_str
            return

        # 2. 워밍업만 재시도
        if self._premarket_warmup_done_date != today_str:
            logger.info("[장전워밍업] 전일 snapshot + strategy cache 사전 로드")
            success = self._warmup_market_data()
            if success:
                self._premarket_warmup_done_date = today_str
            else:
                logger.warning("[장전워밍업] 일부 실패 → 다음 틱에서 재시도")

    def _flush_market_data_once(self, date_str: str):
        if self._last_flush_date == date_str:
            return
        self._flush_candle_csv(date_str)
        self._last_flush_date = date_str

    # ── 종목별 처리 (타이밍 래퍼) ───────────────────────
    def _process_symbol_timed(self, symbol: str, now: datetime):
        t0 = time.time()
        self._process_symbol(symbol, now)
        elapsed = time.time() - t0
        logger.debug(f"[{self._display_name(symbol)}] 처리시간 {elapsed:.3f}s")

    # ── 종목별 처리 ───────────────────────────────────
    def _process_symbol(self, symbol: str, now: datetime):
        display = self._display_name(symbol)

        today_5m = self._get_today_5min_for_strategy(symbol, now, include_partial=INCLUDE_PARTIAL_5MIN)
        completed_5m = self._collector.get_5min(symbol) if self._collector else []

        if not today_5m:
            logger.debug(f"[{display}] 당일 5분봉 없음 → 대기")
            return

        prev_snapshot = self._get_prev_snapshot(symbol)
        if not prev_snapshot:
            logger.warning(f"[{display}] 전일 snapshot 없음 → 스킵")
            return

        prev_day_high = prev_snapshot["high"]
        prev_day_low = prev_snapshot["low"]
        prev_day_close = prev_snapshot["close"]
        prev_day_volume = float(prev_snapshot.get("volume", 0) or 0)
        prev_5m_seed = prev_snapshot.get("candles_5m", []) or []

        pivot_p = (prev_day_high + prev_day_low + prev_day_close) / 3
        pivot_r2 = pivot_p + (prev_day_high - prev_day_low)

        seed = (prev_5m_seed or [])[-20:]
        merged = seed + today_5m
        df = _to_df(merged)

        logger.debug(
            f"[{display}] 전일seed={len(seed)} | 완성5분봉={len(completed_5m)} | 전략용당일={len(today_5m)} | 합계={len(df)}"
        )

        if len(df) < 20:
            logger.debug(f"[{display}] MA 계산 불가 ({len(df)}봉) → 스킵")
            return

        i = len(df) - 1
        ma5_curr = _ma_at(df, 5, i)
        ma20_curr = _ma_at(df, 20, i)
        ma5_prev = _ma_at(df, 5, i - 1)
        ma20_prev = _ma_at(df, 20, i - 1)
        ma5_prev2 = _ma_at(df, 5, i - 2) if i >= 2 else None

        # EMA 추가 2026.03.26
        ema5_curr  = _ema_at(df, 5,  i)
        ema20_curr = _ema_at(df, 20, i)
        ema5_prev  = _ema_at(df, 5,  i - 1)
        ema20_prev = _ema_at(df, 20, i - 1)

        if None in (
            ma5_curr, ma20_curr, ma5_prev, ma20_prev,
            ema5_curr, ema20_curr, ema5_prev, ema20_prev
        ):            
            logger.debug(f"[{display}] MA None → 스킵")
            return

        # lock 밖에서 미리 수집
        ws_1min = self._collector.get_1min(symbol) if self._collector else []

        # lock 안에서 pos 읽기 + max_price 계산까지 한번에
        with self._positions_lock:
            pos = self._positions.get(symbol, {})
            is_holding = bool(pos)
            entry_price = pos.get("entry_price", 0)
            prev_max = pos.get("max_price", entry_price)

            entry_time = pos.get("entry_time", "")
            if entry_time and ws_1min:
                entry_bucket = entry_time[:13]
                ws_1min_after = [r for r in ws_1min if r[0] > entry_bucket]
            else:
                ws_1min_after = ws_1min

            current_high = max((r[2] for r in ws_1min_after), default=float(entry_price or 0))
            if prev_max is None or pd.isna(prev_max):
                prev_max = float(entry_price or current_high)
            max_price = max(float(prev_max), current_high)

        strategy_data = self._get_strategy_cached(symbol)
        daily_vcp_score = strategy_data.get("dailyVcpScore", 0)
        daily_strategy = strategy_data.get("dailyStrategy", "")
        daily_pivot_point = strategy_data.get("dailyPivotPoint", 0.0)
        daily_stop_loss = strategy_data.get("dailyStopLoss", 0.0)

        data = df[CSV_COLUMNS].values.tolist()
        result = get_strategy_signal({
            "i": i,
            "data": data,
            "cfg": self.cfg,
            "isHolding": is_holding,
            "entryPrice": entry_price,
            "maxPriceAfterEntry": max_price,
            "ma5_curr": ma5_curr,
            "ma20_curr": ma20_curr,
            "ma5_prev": ma5_prev,
            "ma20_prev": ma20_prev,
            "ma5_prev2": ma5_prev2,
            "ema5_curr": ema5_curr,
            "ema20_curr": ema20_curr,
            "ema5_prev": ema5_prev,
            "ema20_prev": ema20_prev,
            "prevDayHigh": prev_day_high,
            "prevDayVolume": prev_day_volume,
            "prevDayClose": prev_day_close,
            "pivotR2": pivot_r2,
            "data1Min": self._get_today_1min(symbol),
            "dailyVcpScore": daily_vcp_score,
            "dailyStrategy": daily_strategy,
            "dailyPivotPoint": daily_pivot_point,
            "dailyStopLoss": daily_stop_loss,
        })

        signal = result["signal"]
        reason = result["reason"]
        energy = result["energy"]
        close = int(df["close"].iloc[i])

        if is_holding:
            with self._positions_lock:
                live_pos = self._positions.get(symbol)
                if live_pos:
                    live_pos["max_price"] = max_price
                    self._positions[symbol] = live_pos

        bar_time = str(df["time"].iloc[i])

        if signal == "BUY" and not is_holding:
            if not self._can_reenter(symbol):
                logger.info(f"[재매수방지] {display} → 쿨다운 중, 스킵")
            elif self._can_place_order(symbol, "BUY", bar_time):
                executed = False
                try:
                    executed = self._execute_buy(symbol, close, reason, energy)
                finally:
                    self._finalize_order(symbol, "BUY", bar_time, executed)

        elif signal == "SELL" and is_holding:
            if self._can_place_order(symbol, "SELL", bar_time):
                executed = False
                try:
                    executed = self._execute_sell(symbol, close, reason, entry_price)
                finally:
                    self._finalize_order(symbol, "SELL", bar_time, executed)

    # ── 현재 당일 1분봉/5분봉 보조 ─────────────────────
    def _get_today_1min(self, symbol: str) -> list:
        if not self._collector:
            return []
        return self._collector.get_1min(symbol)

    def _build_partial_5min_bar(self, symbol: str, now: datetime) -> Optional[list]:
        rows = self._get_today_1min(symbol)
        if not rows:
            return None

        current_bucket = (now.hour * 60 + now.minute) // 5 * 5
        current_date = now.strftime("%Y%m%d")
        bucket_rows = []

        for row in rows:
            t = row[0]
            if "_" not in t:
                continue
            dt_part, hhmm = t.split("_", 1)
            if dt_part != current_date:
                continue
            minute = int(hhmm[:2]) * 60 + int(hhmm[2:])
            if (minute // 5) * 5 == current_bucket:
                bucket_rows.append(row)

        if not bucket_rows:
            return None

        return [
            f"{current_date}_{current_bucket // 60:02d}{current_bucket % 60:02d}",
            bucket_rows[0][1],
            max(r[2] for r in bucket_rows),
            min(r[3] for r in bucket_rows),
            bucket_rows[-1][4],
            sum(r[5] for r in bucket_rows),
        ]

    def _get_today_5min_for_strategy(self, symbol: str, now: datetime, include_partial: bool = True) -> list:
        completed = self._collector.get_5min(symbol) if self._collector else []
        if not include_partial:
            return completed

        partial = self._build_partial_5min_bar(symbol, now)
        if not partial:
            return completed

        merged = list(completed)
        if merged and merged[-1][0] == partial[0]:
            merged[-1] = partial
        elif not merged or merged[-1][0] != partial[0]:
            merged.append(partial)
        return merged

    # ── strategy_results 캐시 ─────────────────────────
    def _load_strategy_result(self, symbol: str) -> dict:
        try:
            doc = self._fs.collection("strategy_results").document(symbol).get()  # ✅ 공유 클라이언트

            if not doc.exists:
                return {
                    "dailyVcpScore": 0,
                    "dailyStrategy": "",
                    "dailyPivotPoint": 0.0,
                    "dailyStopLoss": 0.0,
                }

            vd = doc.to_dict() or {}
            return {
                "dailyVcpScore": int(vd.get("vcpScore", 0) or 0),
                "dailyStrategy": str(vd.get("strategy", "") or ""),
                "dailyPivotPoint": float(vd.get("pivotPoint", 0) or 0),
                "dailyStopLoss": float(vd.get("stopLoss", 0) or 0),
            }
        except Exception as e:
            logger.debug(f"[{self._display_name(symbol)}] strategy_results 조회 실패: {e}")
            return {
                "dailyVcpScore": 0,
                "dailyStrategy": "",
                "dailyPivotPoint": 0.0,
                "dailyStopLoss": 0.0,
            }

    def _get_strategy_cached(self, symbol: str) -> dict:
        with self._strategy_lock:
            cached = self._strategy_cache.get(symbol)
        if cached is not None:
            return cached

        data = self._load_strategy_result(symbol)
        with self._strategy_lock:
            self._strategy_cache[symbol] = data
        return data

    # ── 재매수 방지 ─────────────────────────────────────
    def _can_reenter(self, symbol: str) -> bool:
        """당일 익절 종목 재매수 쿨다운 체크. True = 재매수 허용."""
        with self._today_sold_lock:  # ✅ ThreadPoolExecutor race condition 방지
            sold_ts = self._today_sold.get(symbol)
        if sold_ts is None:
            return True
        elapsed = time.time() - sold_ts
        if elapsed < self._reenter_cooldown_sec:
            remaining = int(self._reenter_cooldown_sec - elapsed)
            logger.debug(
                f"[재매수방지] {self._display_name(symbol)} "
                f"쿨다운 {remaining}초 남음 (매도 후 {int(elapsed)}초 경과)"
            )
            return False
        # 쿨다운 만료 → 진입 허용 (레코드는 유지, 재매수 조건 필터가 2차 방어)
        return True

    # ── 주문 가드 ───────────────────────────────────────
    def _make_order_bar_key(self, symbol: str, signal: str, bar_time: str) -> str:
        return f"{symbol}|{signal}|{bar_time}"

    def _can_place_order(self, symbol: str, signal: str, bar_time: str, cooldown_sec: float = ORDER_COOLDOWN_SECONDS) -> bool:
        key = self._make_order_bar_key(symbol, signal, bar_time)
        now_ts = time.time()

        with self._order_lock:
            if symbol in self._inflight_orders:
                logger.info(f"[주문차단] {self._display_name(symbol)} | 사유: inflight | key={key}")
                return False

            last_key = self._last_order_keys.get(symbol)
            if last_key == key:
                logger.info(f"[주문차단] {self._display_name(symbol)} | 사유: same-bar-duplicate | key={key}")
                return False

            last_ts = self._last_order_ts.get(symbol, 0.0)
            if now_ts - last_ts < cooldown_sec:
                logger.info(
                    f"[주문차단] {self._display_name(symbol)} | "
                    f"사유: cooldown({cooldown_sec}s) | key={key}"
                )
                return False

            self._inflight_orders.add(symbol)
            return True

    def _finalize_order(self, symbol: str, signal: str, bar_time: str, executed: bool):
        key = self._make_order_bar_key(symbol, signal, bar_time)
        now_ts = time.time()

        with self._order_lock:
            self._inflight_orders.discard(symbol)
            if executed:
                self._last_order_keys[symbol] = key
                self._last_order_ts[symbol] = now_ts

    # ── 매수/매도 실행 ─────────────────────────────────
    def _execute_buy(self, symbol: str, price: int, reason: str, energy: dict) -> bool:
        display = self._display_name(symbol)
        budget = int(os.environ.get("MAX_POSITION_SIZE", "1000000").split("#")[0].strip())
        qty = max(1, budget // max(price, 1))
        logger.info(f"🟢 매수 실행: {display} {qty}주 @ {price}원 / {reason}")

        result = kis_api.buy_order(symbol, qty, price)
        if result is not None:
            pos = {
                "entry_price": price,
                "max_price": price,
                "qty": qty,
                "reason": reason,
                "energy_score": energy.get("score", 0),
                "name": display,
                "entry_time": datetime.now(KST).strftime("%Y%m%d_%H%M%S"),  # ✅ 매수 시각 기록 (초 단위)
                "is_holding": True,
            }
            with self._positions_lock:
                self._positions[symbol] = pos
            self.firebase.save_trade_state(symbol, pos)
            self.firebase.log_trade("BUY", symbol, {
                "price":      price,
                "qty":        qty,
                "reason":     reason,
                "name":       display,
                "entry_price": price,
                "entry_time":  pos["entry_time"],
            })
            notify_buy(display, symbol, price, reason, energy.get("score", 0))
            return True

        return False

    def _execute_sell(self, symbol: str, price: int, reason: str, entry_price: float) -> bool:
        display = self._display_name(symbol)
        with self._positions_lock:
            pos = self._positions.get(symbol, {})
        qty = pos.get("qty", 1)
        profit_pct = (price / entry_price - 1) * 100 if entry_price else 0
        logger.info(f"🔴 매도 실행: {display} {qty}주 @ {price}원 / {reason} / 수익:{profit_pct:.2f}%")

        result = kis_api.sell_order(symbol, qty, price)
        if result is not None:
            with self._positions_lock:
                self._positions.pop(symbol, None)
            with self._today_sold_lock:  # ✅ race condition 방지
                self._today_sold[symbol] = time.time()   # 재매수 방지: 매도 시각 기록
            self.firebase.delete_trade_state(symbol)
            self.firebase.log_trade(
                "SELL",
                symbol,
                {
                    "price":       price,
                    "qty":         qty,
                    "reason":      reason,
                    "profit_pct":  round(profit_pct, 2),
                    "name":        display,
                    "entry_price": entry_price,
                    "entry_time":  pos.get("entry_time", ""),
                },
            )
            notify_sell(display, symbol, price, reason, profit_pct)
            return True

        return False

    # ── 감시 목록 로드 ────────────────────────────────
    def _load_target_symbols_with_meta(self) -> tuple[list[str], dict]:
        try:
            docs = self._fs.collection("target_stocks").stream()  # ✅ 공유 클라이언트
            items, meta = [], {}
            for doc in docs:
                data = doc.to_dict()
                if not data:
                    continue
                ticker = data.get("ticker", "").strip()
                score = float(data.get("score", 0))
                name = data.get("name", ticker)
                strategy = data.get("strategy", "-")
                if ticker:
                    items.append((ticker, score))
                    meta[ticker] = {"name": name, "strategy": strategy, "score": score}
            items.sort(key=lambda x: x[1], reverse=True)
            symbols = [t for t, _ in items]
            logger.info(f"[target_stocks] {len(symbols)}개 로드")
            return symbols, meta
        except Exception as e:
            logger.error(f"[target_stocks] 조회 실패: {e}")
            return [], {}

    def _reload_watch_symbols(self):
        from trader.telegram import send_telegram

        new_symbols, symbol_meta = self._load_target_symbols_with_meta()

        if not new_symbols:
            fallback = [s.strip() for s in os.environ.get("WATCH_SYMBOLS", ",".join(WATCH_SYMBOLS)).split(",") if s.strip()]
            logger.warning(f"[target_stocks] 폴백: {fallback}")
            self._watch_symbols = fallback
            send_telegram(f"⚠️ target_stocks 없음 → 폴백: {', '.join(fallback)}")
            return

        prev_symbols = self._watch_symbols[:]
        self._watch_symbols = new_symbols
        self._symbol_meta = symbol_meta

        if self._collector:
            self._collector.update_symbols(list(self._watch_symbols))

        added = [s for s in new_symbols if s not in prev_symbols]
        removed = [s for s in prev_symbols if s not in new_symbols]

        with self._snapshot_lock:
            for sym in removed:
                self._prev_snapshot_cache.pop(sym, None)

        with self._strategy_lock:
            for sym in removed:
                self._strategy_cache.pop(sym, None)

        with self._order_lock:
            for sym in removed:
                self._inflight_orders.discard(sym)
                self._last_order_keys.pop(sym, None)
                self._last_order_ts.pop(sym, None)

        if added:
            self._preload_symbols_data(added)

        logger.info(f"[감시목록] {len(new_symbols)}개 | 추가={added} | 제거={removed}")

        if not prev_symbols or added or removed:
            now_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
            lines = [f"📋 감시 종목 갱신 ({now_str})", f"총 {len(new_symbols)}개"]
            if added:
                lines += ["", "✅ 신규 편입:"]
                for t in added:
                    m = symbol_meta.get(t, {})
                    lines.append(f"  • {m.get('name', t)}({t}) [{m.get('strategy', '-')}] {m.get('score', 0):.1f}점")
            if removed:
                lines += ["", "❌ 제외:"]
                for t in removed:
                    lines.append(f"  • {t}")
            if not added and not removed:
                lines += ["", "종목 목록:"]
                for t in new_symbols[:15]:
                    m = symbol_meta.get(t, {})
                    lines.append(f"  • {m.get('name', t)}({t}) [{m.get('strategy', '-')}] {m.get('score', 0):.1f}점")
                if len(new_symbols) > 15:
                    lines.append(f"  ... 외 {len(new_symbols) - 15}개")
            send_telegram("\n".join(lines))

    # ── 전일 snapshot/strategy 워밍업 ─────────────────
    def _get_prev_snapshot(self, symbol: str) -> Optional[dict]:
        with self._snapshot_lock:
            cached = self._prev_snapshot_cache.get(symbol, "__MISSING__")
        if cached != "__MISSING__":
            return cached

        logger.info(f"[snapshot] {self._display_name(symbol)} API 조회 시작")
        try:
            snap = kis_api.get_prev_day_snapshot(symbol)
        except Exception as e:
            logger.error(f"[snapshot] {self._display_name(symbol)} 오류: {e}")
            snap = None

        with self._snapshot_lock:
            self._prev_snapshot_cache[symbol] = snap
        return snap

    def _preload_symbols_data(self, symbols: list[str]):
        if not symbols:
            return

        def _load(sym: str):
            snap = None
            strategy_data = None
            try:
                snap = kis_api.get_prev_day_snapshot(sym)
            except Exception as e:
                logger.error(f"[워밍업][snapshot] {self._display_name(sym)} ❌ {e}")
            try:
                strategy_data = self._load_strategy_result(sym)
            except Exception as e:
                logger.error(f"[워밍업][strategy] {self._display_name(sym)} ❌ {e}")

            with self._snapshot_lock:
                self._prev_snapshot_cache[sym] = snap
            with self._strategy_lock:
                self._strategy_cache[sym] = strategy_data or {
                    "dailyVcpScore": 0,
                    "dailyStrategy": "",
                    "dailyPivotPoint": 0.0,
                    "dailyStopLoss": 0.0,
                }

            ok_snapshot = bool(snap)
            ok_strategy = strategy_data is not None
            logger.info(
                f"[워밍업] {self._display_name(sym)} snapshot={'OK' if ok_snapshot else 'FAIL'} | strategy={'OK' if ok_strategy else 'FAIL'}"
            )
            return ok_snapshot and ok_strategy

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            list(ex.map(_load, symbols))

    def _warmup_market_data(self):
        symbols = list(self._watch_symbols)
        if not symbols:
            return

        from trader.telegram import send_telegram

        logger.info(f"[워밍업] {len(symbols)}종목 전일 snapshot + strategy_results 로드")
        send_telegram(
            f"⏳ 장전 데이터 로드 중... ({len(symbols)}종목)\n전일 1분봉 + 전략 캐시를 준비합니다."
        )

        results = []

        def _load(sym: str):
            snap = None
            strategy_data = None
            try:
                snap = kis_api.get_prev_day_snapshot(sym)
            except Exception as e:
                logger.error(f"[워밍업][snapshot] {self._display_name(sym)} ❌ {e}")
            try:
                strategy_data = self._load_strategy_result(sym)
            except Exception as e:
                logger.error(f"[워밍업][strategy] {self._display_name(sym)} ❌ {e}")

            with self._snapshot_lock:
                self._prev_snapshot_cache[sym] = snap
            with self._strategy_lock:
                self._strategy_cache[sym] = strategy_data or {
                    "dailyVcpScore": 0,
                    "dailyStrategy": "",
                    "dailyPivotPoint": 0.0,
                    "dailyStopLoss": 0.0,
                }

            ok_snapshot = bool(snap)
            ok_strategy = strategy_data is not None
            logger.info(
                f"[워밍업] {self._display_name(sym)} snapshot={'OK' if ok_snapshot else 'FAIL'} | strategy={'OK' if ok_strategy else 'FAIL'}"
            )
            return ok_snapshot and ok_strategy

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            results = list(ex.map(_load, symbols))

        ok = sum(results)
        fail = len(results) - ok
        self._strategy_cache_date = datetime.now(KST).strftime("%Y%m%d")
        if ok == len(results):
            self._premarket_warmup_done_date = self._strategy_cache_date

        logger.info(f"[워밍업] 완료 — 성공:{ok} / 실패:{fail}")
        send_telegram(
            f"✅ 장전 데이터 로드 완료\n성공:{ok} / 실패:{fail}\n신호 판별 준비가 끝났습니다."
        )
        return fail == 0

    # ── CSV 저장 ──────────────────────────────────────
    def _flush_candle_csv(self, date_str: str):
        cache_1m = {}
        cache_5m = {}

        if self._collector:
            for sym in self._watch_symbols:
                cache_1m[sym] = self._collector.get_1min(sym)
                cache_5m[sym] = self._collector.get_5min(sym)
        else:
            with self._buffer_lock:
                cache_1m = {sym: [row[:6] for row in rows] for sym, rows in self._1min_buffer.items()}
                cache_5m = {sym: list(rows) for sym, rows in self._5min_cache.items()}

        total_1m = sum(len(v) for v in cache_1m.values())
        total_5m = sum(len(v) for v in cache_5m.values())
        if total_1m == 0 and total_5m == 0:
            logger.warning("[CSV] 저장할 1분봉/5분봉 데이터가 없습니다.")
            return

        base_dir = DATA_DIR / date_str
        dir_1m = base_dir / "1m"
        dir_5m = base_dir / "5m"
        dir_1m.mkdir(parents=True, exist_ok=True)
        dir_5m.mkdir(parents=True, exist_ok=True)

        saved_1m = 0
        saved_5m = 0

        for symbol, rows in cache_1m.items():
            if not rows:
                continue
            try:
                filepath = dir_1m / f"{symbol}_{date_str}_1m.csv"
                with open(filepath, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(CSV_COLUMNS)
                    writer.writerows([row[:6] for row in rows])
                saved_1m += 1
                logger.info(f"[CSV][1m] {symbol} → {filepath} ({len(rows)}봉)")
            except Exception as e:
                logger.error(f"[CSV][1m] {symbol} 저장 실패: {e}")

        for symbol, rows in cache_5m.items():
            if not rows:
                continue
            try:
                filepath = dir_5m / f"{symbol}_{date_str}_5m.csv"
                with open(filepath, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(CSV_COLUMNS)
                    writer.writerows(rows)
                saved_5m += 1
                logger.info(f"[CSV][5m] {symbol} → {filepath} ({len(rows)}봉)")
            except Exception as e:
                logger.error(f"[CSV][5m] {symbol} 저장 실패: {e}")

        logger.info(
            f"[CSV] 저장 완료 | 1m:{saved_1m}종목/{total_1m}봉 | 5m:{saved_5m}종목/{total_5m}봉 | dir={base_dir}"
        )

    # ── 포지션 복원 ───────────────────────────────────
    def _restore_positions(self):
        positions = self.firebase.get_all_positions()
        today_sold = self.firebase.get_sold_positions()  # is_holding=False 별도 조회 필요
        with self._positions_lock:
            for p in positions:
                sym = p.pop("symbol", None)
                if sym and p.get("entry_price", 0) > 0:
                    self._positions[sym] = p
                else:
                    logger.warning(f"[포지션복원] {sym} entry_price 없음 → 복원 스킵")
        for p in today_sold:
            sym = p.get("symbol")
            if sym:
                self._today_sold[sym] = time.time()
        logger.info(f"포지션 복원: {len(self._positions)}개 | 당일매도: {len(self._today_sold)}개")

    def get_status(self) -> dict:
        with self._positions_lock:
            positions = list(self._positions.keys())
        return {
            "running": self.is_running,
            "last_tick": self.last_tick_time,
            "holding_count": len(positions),
            "positions": positions,
            "watch_symbols": self._watch_symbols,
            "tick_interval_seconds": TICK_INTERVAL_SECONDS,
            "include_partial_5min": INCLUDE_PARTIAL_5MIN,
        }
