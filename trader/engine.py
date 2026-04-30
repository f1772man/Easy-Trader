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
import json
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
from google.cloud import firestore

logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))
DATA_DIR = Path(os.environ.get("DATA_DIR", "DATA"))
STRATEGY_FILTER_PATH = Path(os.environ.get("STRATEGY_FILTER_PATH", "strategy_filter.json"))
STRATEGY_FILTER_RELOAD_SECONDS = float(os.environ.get("STRATEGY_FILTER_RELOAD_SECONDS", "5").split("#")[0].strip())
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

def _normalize_name(name: str, symbol: str) -> str:
    """'종목명(코드)' 형태로 저장된 기존 데이터를 '종목명'으로 정규화"""
    if not name:
        return ""
    suffix = f"({symbol})"
    if name.endswith(suffix):
        return name[:-len(suffix)]
    return name


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
        self._today_sold: dict[str, tuple] = {}   # symbol → (timestamp, reason, exit_price)
        self._today_sold_lock = threading.Lock()  # ✅ race condition 방지
        self._reenter_cooldown_sec: float = float(
            os.environ.get("REENTER_COOLDOWN_SECONDS", "60").split("#")[0].strip()
        )  # 기본 1분 쿨다운

        # 상한가 터치 여부 (symbol → bool)
        self._touched_limit_up: dict[str, bool] = {}

        # 손절 대기 (symbol → 진입 시 마지막 1분봉 time_key)
        self._stop_loss_pending: dict[str, str] = {}  # ← 추가

        # Firestore 클라이언트 (공유 싱글턴 — 매 호출마다 신규 생성 방지)
        self._fs = firestore.Client()

        # 전략 필터: strategy_filter.json 변경 감지 후 재로드
        self._blocked_reasons: set[str] = set()
        self._strategy_filter_mtime: float = 0.0
        self._strategy_filter_last_check_ts: float = 0.0
        self._reload_strategy_filter_if_needed(force=True)

    # ── 전략 필터 재로드 ───────────────────────────────
    def _reload_strategy_filter_if_needed(self, force: bool = False):
        """
        strategy_filter.json 변경 시 차단 매수 조건을 재로드한다.
        재배포/재시작 없이 파일 수정만으로 다음 BUY 판단부터 반영된다.
        """
        now_ts = time.time()
        if not force and now_ts - self._strategy_filter_last_check_ts < STRATEGY_FILTER_RELOAD_SECONDS:
            return

        self._strategy_filter_last_check_ts = now_ts
        path = STRATEGY_FILTER_PATH

        try:
            mtime = path.stat().st_mtime
        except FileNotFoundError:
            if self._blocked_reasons:
                self._blocked_reasons = set()
                self._strategy_filter_mtime = 0.0
                logger.info("[전략필터] 파일 없음 → 차단조건 초기화")
            return
        except Exception as e:
            logger.warning(f"[전략필터] 파일 상태 확인 실패: {e}")
            return

        if not force and mtime == self._strategy_filter_mtime:
            return

        try:
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)

            blocked_reasons = data.get("blocked_reasons", [])
            if not isinstance(blocked_reasons, list):
                logger.warning("[전략필터] blocked_reasons 형식 오류 → 기존 필터 유지")
                return

            self._blocked_reasons = {
                str(reason).strip()
                for reason in blocked_reasons
                if str(reason).strip()
            }
            self._strategy_filter_mtime = mtime

            logger.info(
                f"[전략필터] 재로드 완료 | file={path} | 차단조건={len(self._blocked_reasons)}개"
            )
        except Exception as e:
            logger.warning(f"[전략필터] 재로드 실패: {e}")

    # ── 종목명(코드) ──────────────────────────────────
    def _display_name(self, symbol: str) -> str:
        with self._positions_lock:
            pos_name = self._positions.get(symbol, {}).get("name", "")
        meta_name = self._symbol_meta.get(symbol, {}).get("name", "")
        name = pos_name or meta_name
        return f"{_normalize_name(name, symbol)}({symbol})" if name else symbol

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
        self._touched_limit_up.clear()
        self._stop_loss_pending.clear()

        logger.info(f"[캐시] 날짜 변경 → snapshot/strategy/버퍼/주문가드/재매수방지 초기화 ({today_str})")
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

    def _fetch_holiday_once(self, today_str: str) -> str:
        """chk_holiday API 1페이지만 조회 (재귀 없음)"""
        import trader.kis_auth as ka
        params = {
            "BASS_DT": today_str,
            "CTX_AREA_FK": "",
            "CTX_AREA_NK": ""
        }
        res = ka._url_fetch(
            "/uapi/domestic-stock/v1/quotations/chk-holiday",
            "CTCA0903R", "", params
        )
        if not res.isOK():
            logger.warning("[휴장일조회] API 실패 → 개장으로 간주")
            return "Y"
        output = res.getBody().output
        if not isinstance(output, list):
            output = [output]
        df = pd.DataFrame(output)
        if df.empty or "opnd_yn" not in df.columns:
            return "Y"
        row = df[df["bass_dt"] == today_str] if "bass_dt" in df.columns else df.iloc[:1]
        if row.empty:
            return "Y"
        opnd_yn = str(row["opnd_yn"].iloc[0]).strip().upper()
        logger.info(f"[휴장일조회] {today_str} opnd_yn={opnd_yn}")
        return opnd_yn

    def _maybe_premarket_warmup(self, now: datetime):
        hm = now.hour * 100 + now.minute
        today_str = now.strftime("%Y%m%d")

        if not (850 <= hm < 900):
            return

        # 1. 휴장일 체크는 하루 1번만
        if self._holiday_checked_date != today_str:
            try:
                opnd_yn = self._fetch_holiday_once(today_str)
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
            logger.info("[장전워밍업] 감시 종목 갱신 후 전일 snapshot + strategy cache 사전 로드")
            self._reload_watch_symbols()
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

        # lock 밖에서 미리 수집
        ws_1min = self._collector.get_1min(symbol) if self._collector else []
        today_5m = self._get_today_5min_for_strategy(symbol, now, include_partial=INCLUDE_PARTIAL_5MIN)
        completed_5m = self._collector.get_5min(symbol) if self._collector else []

        if not ws_1min and not completed_5m:
            logger.debug(f"[{display}] 첫 체결 전 → 대기")
            return
            
        if not today_5m:
            logger.warning(f"[{display}] 5분봉 없음 | 1분봉={len(ws_1min)} | 완성5분봉={len(completed_5m)}")
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
        ema5_prev2 = _ema_at(df, 5,  i - 2) if i >= 2 else None
        ema5_5bars_ago = _ema_at(df, 5,  i - 5) if i >= 5 else None
        ema20_prev = _ema_at(df, 20, i - 1)

        if None in (
            ma5_curr, ma20_curr, ma5_prev, ma20_prev,
            ema5_curr, ema20_curr, ema5_prev, ema20_prev
        ):            
            logger.debug(f"[{display}] MA None → 스킵")
            return        

        # lock 안에서 pos 읽기 + max_price 계산까지 한번에
        with self._positions_lock:
            pos = self._positions.get(symbol, {})
            is_holding = bool(pos)
            entry_price = pos.get("entry_price", 0)
            prev_max = pos.get("max_price", entry_price)

            entry_time = pos.get("entry_time", "")
            if entry_time and ws_1min:
                # entry_time = "YYYYMMDD_HHMMSS" (초단위)
                # ws_1min r[0] = "YYYYMMDD_HHMM" (분단위)
                # 진입 분("YYYYMMDD_HHMM")과 같은 봉은 포함해야
                # max_price 계산에서 진입 분 고가가 누락되지 않음
                entry_bucket = entry_time[:11]  # "YYYYMMDD_HH" → 시 단위 앞 11자
                entry_min = entry_time[9:13]    # "HHMM"
                entry_time_key = f"{entry_time[:8]}_{entry_min}"  # "YYYYMMDD_HHMM"
                ws_1min_after = [r for r in ws_1min if r[0] >= entry_time_key]
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
        can_reenter, trailing_exit_price = self._can_reenter(symbol)
        result = get_strategy_signal({
            "symbol": symbol,
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
            "ema5_prev2": ema5_prev2,
            "ema5_5bars_ago": ema5_5bars_ago,
            "prevDayHigh": prev_day_high,
            "prevDayVolume": prev_day_volume,
            "prevDayClose": prev_day_close,
            "pivotR2": pivot_r2,
            "data1Min": ws_1min,
            "dailyVcpScore": daily_vcp_score,
            "dailyStrategy": daily_strategy,
            "dailyPivotPoint": daily_pivot_point,
            "dailyStopLoss": daily_stop_loss,
            "touchedLimitUp": self._touched_limit_up.get(symbol, False),
            "trailingExitPrice": trailing_exit_price,
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

        # strategy 호출 전 수집한 ws_1min 재사용 (시점 통일)
        last_1m_close = ws_1min[-1][4] if ws_1min else None
        current_price = int(last_1m_close) if last_1m_close else int(close)

        if signal == "BUY" and not is_holding:
            self._reload_strategy_filter_if_needed()

            if reason in self._blocked_reasons:
                logger.info(f"[BUY-BLOCK][{symbol}] {display} reason={reason}")
                return

            if not can_reenter:
                logger.info(f"[재매수방지] {display} → 쿨다운 중, 스킵")
            elif self._can_place_order(symbol, "BUY", bar_time):
                executed = False
                try:
                    executed = self._execute_buy(symbol, current_price, reason, energy)
                finally:
                    self._finalize_order(symbol, "BUY", bar_time, executed)

        elif signal == "SELL" and is_holding:
            if reason == "손절대기":
                pending_bar = self._stop_loss_pending.get(symbol)
                last_bar_key = ws_1min[-1][0] if ws_1min else None

                if pending_bar is None:
                    self._stop_loss_pending[symbol] = last_bar_key
                    logger.info(f"[손절대기] {display} | 봉={last_bar_key} → 다음 1분봉 종가 확인 예정")
                elif last_bar_key != pending_bar:
                    last_close = ws_1min[-1][4] if ws_1min else current_price
                    pending_profit = (last_close / entry_price - 1) * 100 if entry_price else 0
                    daily_stop_loss = float(self._get_strategy_cached(symbol).get("dailyStopLoss") or 0)
                    stop_pct = (
                        abs(daily_stop_loss / entry_price - 1) * 100
                        if daily_stop_loss > 0 and entry_price > 0
                        else self.cfg.get("stopLoss", 2.0)
                    )
                    if pending_profit <= -stop_pct:
                        logger.info(f"[손절확정] {display} | 종가:{last_close} | 수익:{pending_profit:.2f}%")
                        if self._can_place_order(symbol, "SELL", last_bar_key):
                            executed = False
                            try:
                                executed = self._execute_sell(symbol, last_close, reason, entry_price)
                            finally:
                                self._finalize_order(symbol, "SELL", last_bar_key, executed)
                                if executed:
                                    self._stop_loss_pending.pop(symbol, None)
                                    self._touched_limit_up.pop(symbol, None)
                    else:
                        logger.info(f"[손절취소] {display} | 종가:{last_close} | 수익:{pending_profit:.2f}% 회복")
                        self._stop_loss_pending.pop(symbol, None)
            else:
                if self._can_place_order(symbol, "SELL", bar_time):
                    executed = False
                    try:
                        executed = self._execute_sell(symbol, current_price, reason, entry_price)
                    finally:
                        self._finalize_order(symbol, "SELL", bar_time, executed)
                        if executed:
                            self._stop_loss_pending.pop(symbol, None)
                            self._touched_limit_up.pop(symbol, None)

        elif reason == "상한가터치-대기":
            self._touched_limit_up[symbol] = True
            logger.info(f"[상한가터치] {display} → 다음 봉 이탈 시 매도 대기")

    # ── 현재 당일 1분봉/5분봉 보조 ─────────────────────
    def _get_today_1min(self, symbol: str) -> list:
        if not self._collector:
            return []
        return self._collector.get_1min(symbol)

    def _build_partial_5min_bar(self, symbol: str, now: datetime) -> Optional[list]:
        rows = self._get_today_1min(symbol)
        if not rows:
            return None

        last_time = rows[-1][0]
        date_str = last_time.split("_")[0]

        # 버킷 계산: collector와 동일한 공식 재사용
        cur_bucket_key = self._collector._calc_bucket(last_time)
        _, bkt_hhmm = cur_bucket_key.split("_")
        current_bucket = int(bkt_hhmm[:2]) * 60 + int(bkt_hhmm[2:])

        bucket_rows = []
        for row in rows:
            t = row[0]
            if "_" not in t:
                continue
            dt_part, hhmm = t.split("_", 1)
            if dt_part != date_str:
                continue
            minute = int(hhmm[:2]) * 60 + int(hhmm[2:])
            row_bucket = ((minute // 5) + 1) * 5
            if row_bucket == current_bucket:
                bucket_rows.append(row)

        if not bucket_rows:
            return None

        return [
            cur_bucket_key,
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
        else:
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
    def _can_reenter(self, symbol: str) -> tuple[bool, float]:
        """재매수 가능 여부 + 트레일링 청산가 반환. (allowed, trailing_exit_price)"""
        with self._today_sold_lock:  # ✅ ThreadPoolExecutor race condition 방지
            record = self._today_sold.get(symbol)
        if record is None:
            return True, 0.0
        sold_ts, sold_reason, exit_price = record
        elapsed = time.time() - sold_ts
        # 트레일링 청산이면 쿨다운 없이 즉시 허용 (가격·EMA 필터는 strategy에서)
        if "트레일링" in sold_reason:
            return True, float(exit_price)
        # 그 외 청산(손절, EMA이탈 등)은 기존 쿨다운 유지
        if elapsed < self._reenter_cooldown_sec:
            remaining = int(self._reenter_cooldown_sec - elapsed)
            logger.debug(
                f"[재매수방지] {self._display_name(symbol)} "
                f"쿨다운 {remaining}초 남음 (매도 후 {int(elapsed)}초 경과)"
            )
            return False, 0.0
        # 쿨다운 만료 → 진입 허용 (레코드는 유지, 재매수 조건 필터가 2차 방어)
        return True, 0.0

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
        raw_name = self._symbol_meta.get(symbol, {}).get("name", "")
        display = f"{raw_name}({symbol})" if raw_name else symbol

        budget = int(os.environ.get("MAX_POSITION_SIZE", "1000000").split("#")[0].strip())
        qty = max(1, budget // max(price, 1))
        logger.info(
            f"[BUY][{symbol}] {display} "
            f"price={price} qty={qty} reason={reason}"
        )

        result = kis_api.buy_order(symbol, qty, price)
        if result is not None:
            pos = {
                "entry_price": price,
                "max_price": price,
                "qty": qty,
                "reason": reason,
                "energy_score": energy.get("score", 0),
                "name": raw_name,  # positions: 종목명만 저장
                "entry_time": datetime.now(KST).strftime("%Y%m%d_%H%M%S"),  # ✅ 매수 시각 기록 (초 단위)
                "is_holding": True,
            }
            with self._positions_lock:
                self._positions[symbol] = pos
            self.firebase.save_trade_state(symbol, pos)
            self.firebase.log_trade("BUY", symbol, {
                "price": price,
                "qty": qty,
                "reason": reason,
                "name": raw_name,  # trade_log: 종목명만 저장
                "entry_price": price,
                "entry_time": pos["entry_time"],
            })
            notify_buy(symbol, raw_name, price, reason, energy.get("score", 0))
            return True

        return False

    def _execute_sell(self, symbol: str, price: int, reason: str, entry_price: float) -> bool:
        with self._positions_lock:
            pos = self._positions.get(symbol, {})

        raw_name = pos.get("name") or self._symbol_meta.get(symbol, {}).get("name", "")
        display = f"{_normalize_name(raw_name, symbol)}({symbol})" if raw_name else symbol

        qty = pos.get("qty", 1)
        profit_pct = (price / entry_price - 1) * 100 if entry_price else 0
        logger.info(
            f"[SELL][{symbol}] {display} "
            f"price={price} qty={qty} profit={profit_pct:.2f}% reason={reason}"
        )

        result = kis_api.sell_order(symbol, qty, price)
        if result is not None:
            with self._positions_lock:
                self._positions.pop(symbol, None)
            with self._today_sold_lock:  # ✅ race condition 방지
                self._today_sold[symbol] = (time.time(), reason, price)  # 재매수 방지: 시각·사유·청산가 기록
            self.firebase.delete_trade_state(symbol)
            self.firebase.log_trade(
                "SELL",
                symbol,
                {
                    "price": price,
                    "qty": qty,
                    "reason": reason,
                    "profit_pct": round(profit_pct, 2),
                    "name": raw_name,  # trade_log: 종목명만 저장
                    "entry_price": entry_price,
                    "entry_time": pos.get("entry_time", ""),
                },
            )
            notify_sell(symbol, raw_name, price, reason, profit_pct)
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
        self._symbol_meta = symbol_meta

        final_symbols = list(new_symbols)

        with self._positions_lock:
            holding_symbols = list(self._positions.keys())

        force_added = [s for s in holding_symbols if s not in final_symbols]
        if force_added:
            final_symbols += force_added

        self._watch_symbols = final_symbols

        added = [s for s in new_symbols if s not in prev_symbols]
        removed = [s for s in prev_symbols if s not in new_symbols]

        if self._collector:
            self._collector.update_symbols(list(self._watch_symbols))

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
            self._warmup_market_data()

        # 복원된 포지션 종목은 target_stocks와 무관하게 감시 목록에 강제 포함
        with self._positions_lock:
            holding_symbols = list(self._positions.keys())
        force_added = [s for s in holding_symbols if s not in self._watch_symbols]
        if force_added:
            self._watch_symbols = list(self._watch_symbols) + force_added
            if self._collector:
                self._collector.update_symbols(list(self._watch_symbols))
            logger.info(f"[감시목록] 보유 포지션 강제 편입: {force_added}")
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                list(ex.map(self._load_symbol_data, force_added))

        logger.info(f"[감시목록] {len(self._watch_symbols)}개 | 추가={added} | 제거={removed}")

        if not prev_symbols or added or removed:
            overlap = [s for s in new_symbols if s in prev_symbols] if prev_symbols else []
            now_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
            holding_extra = [s for s in self._watch_symbols if s not in new_symbols]
            lines = [
                f"📋 감시 종목 갱신 ({now_str})",
                f"총 {len(self._watch_symbols)}개 (전략 {len(new_symbols)} + 포지션 {len(holding_extra)})"
            ]
            if added:
                lines += ["", f"✅ 신규 편입: {len(added)}개"]
                for t in added:
                    m = symbol_meta.get(t, {})
                    lines.append(f"  • {m.get('name', t)}({t}) [{m.get('strategy', '-')}] {m.get('score', 0):.1f}점")
            # 연속 편입 (추가)
            if overlap:
                lines += ["", f"🔁 연속 편입: {len(overlap)}개"]
                for t in overlap:
                    m = symbol_meta.get(t, {})
                    lines.append(f"  • {m.get('name', t)}({t}) [{m.get('strategy', '-')}] {m.get('score', 0):.1f}점")
            if holding_extra:
                lines += ["", f"📌 포지션 유지: {len(holding_extra)}개"]
                for t in holding_extra:
                    lines.append(f"  • {self._display_name(t)}")
            if removed:
                lines += ["", "❌ 제외:"]
                for t in removed:
                    lines.append(f"  • {t}")
            if not added and not removed and not overlap:
                lines += ["", "📄 종목 목록:"]
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

    def _load_symbol_data(self, sym: str) -> bool:
        """종목 1개의 snapshot + strategy 로드 → 캐시 저장. 공통 로더."""
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

    def _warmup_market_data(self) -> bool:
        symbols = list(self._watch_symbols)
        if not symbols:
            return True

        # 이미 snapshot 캐시가 있는 종목은 스킵 (이중 로드 방지)
        with self._snapshot_lock:
            symbols = [s for s in symbols if self._prev_snapshot_cache.get(s, "__MISSING__") == "__MISSING__"]
        if not symbols:
            logger.info("[워밍업] 모든 종목 캐시 존재 → 스킵")
            self._premarket_warmup_done_date = datetime.now(KST).strftime("%Y%m%d")
            return True

        from trader.telegram import send_telegram

        all_watch = list(self._watch_symbols)
        logger.info(f"[워밍업] {len(symbols)}종목 전일 snapshot + strategy_results 로드")
        send_telegram(
            f"⏳ 장전 데이터 로드 중... ({len(symbols)}종목)\n전일 1분봉 + 전략 캐시를 준비합니다."
        )

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            results = list(ex.map(self._load_symbol_data, symbols))

        ok = sum(results)
        fail = len(results) - ok
        self._strategy_cache_date = datetime.now(KST).strftime("%Y%m%d")
        if ok == len(results):
            self._premarket_warmup_done_date = self._strategy_cache_date

        logger.info(f"[워밍업] 완료 — 성공:{ok} / 실패:{fail}")

        # 완료 메시지: 전체 감시 종목 목록 표시
        lines = [f"✅ 장전 데이터 로드 완료", f"성공:{ok} / 실패:{fail}", f"신호 판별 준비가 끝났습니다.", ""]
        lines.append(f"📋 감시 종목 ({len(all_watch)}개):")
        for s in all_watch:
            m = self._symbol_meta.get(s, {})
            name = m.get("name", "") or self._display_name(s)
            strategy = m.get("strategy", "")
            score = m.get("score", 0)
            with self._positions_lock:
                is_holding = s in self._positions
            tag = " 📌" if is_holding else ""
            if strategy:
                lines.append(f"  • {name}({s}) [{strategy}] {score:.1f}점{tag}")
            else:
                lines.append(f"  • {name}({s}){tag}")
        send_telegram("\n".join(lines))
        return fail == 0

    # ── CSV 저장 ──────────────────────────────────────
    def _flush_candle_csv(self, date_str: str):
        cache_1m = {}
        cache_5m = {}

        if self._collector:
            for sym in self._watch_symbols:
                cache_1m[sym] = self._collector.get_1min(sym)
                cache_5m[sym] = self._collector.get_5min(sym)

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
        today_sold = self.firebase.get_sold_positions()
        restored = []

        with self._positions_lock:
            for p in positions:
                sym = p.pop("symbol", None)
                if not sym or p.get("entry_price", 0) <= 0:
                    logger.warning(f"[포지션복원] {sym} entry_price 없음 → 복원 스킵")
                    continue

                raw_name = p.get("name", "") or ""
                p["name"] = _normalize_name(raw_name, sym)

                self._positions[sym] = p
                restored.append((sym, p["name"]))

        for sym, raw_name in restored:
            display = f"{raw_name}({sym})" if raw_name else sym
            logger.info(f"[포지션복원] {display} 복원 완료")

        for p in today_sold:
            sym = p.get("symbol")
            if sym:
                self._today_sold[sym] = (time.time(), "복원", 0)

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
