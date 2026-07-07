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
from trader.minute_store import MinuteStoreManager
from trader.ws_tick_collector import WsTickCollector
from trader.kis_auth import auth, _url_fetch
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
# RVT(상대 거래대금) 기반 종목 선정 토글
#   false (기본): 기존 거래대금 0.7 + 상대강도 0.3 으로 운영 정렬
#   true       : RVT 0.625 + 상대강도 0.375 로 운영 정렬 (수십 거래일 검증 후 전환용)
# 토글 무관하게 두 순위는 항상 계산·로그 출력되며 target_stocks에 함께 저장된다.
USE_RVT_SCORING = os.environ.get("USE_RVT_SCORING", "false").strip().lower() in ("1", "true", "y", "yes")


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

        # 분봉 데이터 인프라
        self._minute_store: MinuteStoreManager = MinuteStoreManager()

        # market_analysis/latest 스냅샷 (selection 아카이빙 + manifest용)
        self._market_analysis_snapshot: dict = {}

        # 루프/상태 제어
        self._last_heartbeat_ts = 0.0
        self._last_flush_date = ""
        self._premarket_warmup_done_date = ""
        self._holiday_checked_date: str = ""
        self._today_opnd_yn: str = "Y"
        self._trade_filter_done: bool = False   # 당일 거래대금 필터 완료 여부
        self._market_open_notified: bool = False  # 09:00 보유종목 처리 알림 (하루 1회)

        # 마켓 게이트 (foreign_signal 기반 동적 스캔 타이밍)
        self._market_gate: str = "UNKNOWN"        # gate 값 (BULL/BEAR/NEUTRAL/UNKNOWN)
        self._market_gate_confidence: float = 0.0 # phase1 신뢰도
        self._market_gate_policy: str = "ALLOW"   # phase1 entry_policy
        self._market_top_n: int = 10              # 거래대금 필터 선정 종목 수
        self._scan_hm: int = 920                  # 거래대금 필터 실행 기준 시각 (hhmm)
        self._gate_loaded_date: str = ""          # Firestore 로드 완료 날짜 (하루 1회 가드)
        self._gate_pending: bool = False          # 전 gate → phase2 실시간 확정 대기
        self._gate_retry_count: int = 0           # phase1 재시도 횟수 (최대 5회)

        # 주문 중복 방지
        self._order_lock = threading.RLock()
        self._inflight_orders: set[str] = set()
        self._last_order_keys: dict[str, str] = {}
        self._last_order_ts: dict[str, float] = {}

        # 재매수 방지: 당일 익절 종목 + 매도 시각
        self._today_sold: dict[str, tuple] = {}   # symbol → (timestamp, reason, exit_price)
        self._today_sold_lock = threading.Lock()  # ✅ race condition 방지
        self._rsi2_entry_attempted: set[str] = set()   # RSI2 당일 진입 시도(또는 포기) 완료 종목
        self._rsi2_early_check_done: bool = False       # 09:00 Firestore 조회 1회 완료 여부
        self._reenter_cooldown_sec: float = float(
            os.environ.get("REENTER_COOLDOWN_SECONDS", "60").split("#")[0].strip()
        )  # 기본 1분 쿨다운

        # 상한가 터치 여부 (symbol → bool)
        self._touched_limit_up: dict[str, bool] = {}

        # 손절 대기 (symbol → 진입 시 마지막 1분봉 time_key)
        self._stop_loss_pending: dict[str, str] = {}  # ← 추가

        # 일일 실현손익 통계 (당일만, 장 마감 시 요약 로그용)
        self._daily_realized_pnl: int = 0
        self._daily_wins: int = 0
        self._daily_losses: int = 0

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

        # 전일 보유 종목은 거래대금 필터 전에도 09:00 즉시 매매 대응해야 하므로
        # 엔진 시작 시점부터 감시목록/WS 구독에 먼저 편입한다.
        with self._positions_lock:
            holding_symbols = list(self._positions.keys())

        self._watch_symbols = holding_symbols[:]
        self._collector = WsTickCollector(list(self._watch_symbols))
        self._collector.register_minute_store(self._minute_store)
        self._collector.start()

        if holding_symbols:
            logger.info(f"[보유종목] 장시작 전 구독 준비: {holding_symbols}")

        # ── 비거래일 sleep 루프 (restart:always 재시작 스팸 방지) ──────────────
        # 부팅 시퀀스가 완료된 뒤에도 비거래일이면 메인 루프에 진입하지 않고
        # 1시간 간격으로 거래일 전환을 확인한다.
        if not self.is_krx_open():
            _WEEKDAY_KO = ["월", "화", "수", "목", "금", "토", "일"]
            while self.is_running:
                now = datetime.now(KST)
                # 다음 날 08:45까지 한 번에 sleep (매 시간 깨울 필요 없음)
                next_check = (now + timedelta(days=1)).replace(
                    hour=8, minute=45, second=0, microsecond=0
                )
                sleep_secs = max(60, (next_check - now).total_seconds())
                day_label = _WEEKDAY_KO[now.weekday()]
                wake_str = next_check.strftime("%m/%d %H:%M")
                logger.info(f"[휴장일루프] {now.strftime('%Y%m%d')}({day_label}) 비거래일 → {wake_str} KST 재확인 ({sleep_secs/3600:.1f}h)")
                send_telegram(f"🛌 [휴장일] {now.strftime('%Y-%m-%d')}({day_label}) → {wake_str} KST 재확인")
                # 큰 sleep 대신 60초 단위로 나눠 heartbeat 갱신 (모니터 오알림 방지)
                slept = 0.0
                while slept < sleep_secs and self.is_running:
                    time.sleep(min(60, sleep_secs - slept))
                    slept += 60
                    self.firebase.update_heartbeat({"running": True})
                # 날짜 바뀌면 캐시 리셋 후 거래일 여부 재판단
                self._holiday_checked_date = ""
                if self.is_krx_open():
                    logger.info("[휴장일루프] 거래일 전환 감지 → 정상 루프 시작")
                    send_telegram("🔔 [거래일전환] 거래일 감지 → 엔진 활성화")
                    break

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

        hm = now.hour * 100 + now.minute

        # 휴장일 체크: 매일 08:50 이전 첫 틱에서 확정 (시간 무관)
        self._check_holiday_once(now)

        # 08:50~08:59 장전 워밍업 (휴장일 체크는 위에서 이미 완료)
        self._maybe_premarket_warmup(now)

        if not (900 <= hm <= 1530):
            if hm >= 1531 and self._today_opnd_yn == "Y":  # 휴장일엔 flush 스킵
                self._flush_market_data_once(now.strftime("%Y%m%d"))
            return

        # 휴장일이면 매매 전체 차단
        if self._today_opnd_yn != "Y":
            return

        # ── 마켓 게이트 2단계 확정 (09:01~09:10, BLOCK 대기 중인 경우만) ──
        if self._gate_pending and 901 <= hm <= 910:
            self._load_market_gate_phase2()

        # ── RSI2 역전 모드: 09:00~09:04 조기 편입 (실패·재시작 시 재시도 창) ──
        if not self._trade_filter_done and 900 <= hm <= 904 and not self._rsi2_early_check_done:
            self._try_rsi2_early_load(now)

        # ── 거래대금 필터: 동적 시각(scan_hm) 자동 실행 (하루 1회) ──
        _scan_hm  = self._scan_hm                          # gate 기반 동적 시각
        _scan_h   = _scan_hm // 100
        _scan_m   = _scan_hm % 100
        _fall_m   = _scan_m + 5
        _fallback_hm = (_scan_h * 100 + _fall_m) if _fall_m < 60 \
                       else ((_scan_h + 1) * 100 + (_fall_m - 60))  # 분 60 초과 안전 처리
        if not self._trade_filter_done and _scan_hm <= hm <= _fallback_hm:
            self._filter_by_trade_amount()

        # ── 폴백: fallback_hm 초과 후에도 미완료 시 즉시 실행 ──────
        if not self._trade_filter_done and hm > _fallback_hm:
            try:
                _today_str = datetime.now(KST).strftime("%Y%m%d")
                _ts_snap   = list(self._fs.collection("target_stocks").stream())
                _reversal  = any(
                    (d.to_dict() or {}).get("strategy") == "RSI2_REVERSAL"
                    for d in _ts_snap
                )
                if _reversal:
                    # RSI2 모드: tr_pbmn_date 검사 불필요 — _apply_rsi2_reversal_filter에서
                    # selected_date 직전 영업일 검증 후 처리 (fail-closed 포함)
                    logger.warning("[거래대금필터] RSI2 역전 모드 감지 → 역전 필터 즉시 실행")
                    self._filter_by_trade_amount(_ts_snap)
                else:
                    _existing = [
                        d for d in _ts_snap
                        if (d.to_dict() or {}).get("tr_pbmn_date") == _today_str
                    ]
                    if _existing:
                        logger.warning("[거래대금필터] 당일 target_stocks 존재 → 재선정 스킵, 보유종목만 유지")
                        with self._positions_lock:
                            holding = list(self._positions.keys())
                        if holding:
                            self._watch_symbols = holding[:]
                            if self._collector:
                                self._collector.update_symbols(holding[:])
                        self._trade_filter_done = True
                    else:
                        logger.warning(f"[거래대금필터] {_fallback_hm} 초과 미완료 → 즉시 실행")
                        self._filter_by_trade_amount()
            except Exception as e:
                logger.warning(f"[거래대금필터] target_stocks 조회 실패({e}) → 즉시 실행")
                self._filter_by_trade_amount()

        # ── 거래대금 필터 미완료 시: 보유 포지션만 처리 ──
        if not self._trade_filter_done:
            with self._positions_lock:
                holding_symbols = list(self._positions.keys())
            if not holding_symbols:
                return
            symbols = holding_symbols
            if not self._market_open_notified:
                from trader.telegram import send_telegram
                send_telegram(f"🔔 [장시작] 보유종목 {holding_symbols} 신호처리 시작 (거래대금필터 대기 중)")
                self._market_open_notified = True
        else:
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
        self._premarket_warmup_done_date = ""

        with self._order_lock:
            self._inflight_orders.clear()
            self._last_order_keys.clear()
            self._last_order_ts.clear()

        self.firebase.cleanup_sold_positions()
        self._today_sold.clear()
        self._rsi2_entry_attempted.clear()
        self._rsi2_early_check_done = False
        self._touched_limit_up.clear()
        self._stop_loss_pending.clear()
        self._trade_filter_done = False
        self._market_open_notified = False
        self._daily_realized_pnl = 0
        self._daily_wins = 0
        self._daily_losses = 0
        self._holiday_checked_date = ""   # 다음날 휴장일 재조회를 위해 리셋
        self._today_opnd_yn = "Y"         # 기본값 복원

        # 마켓 게이트 리셋 (다음날 새로 로드)
        self._market_gate = "UNKNOWN"
        self._market_gate_confidence = 0.0
        self._market_gate_policy = "ALLOW"
        self._market_top_n = 10
        self._scan_hm = 920
        self._gate_loaded_date = ""
        self._gate_pending = False
        self._gate_retry_count = 0

        # watch_symbols 리셋 → 보유종목만 유지 (08:50 워밍업이 보유종목 전용으로 실행되도록)
        with self._positions_lock:
            holding_symbols = list(self._positions.keys())
        if self._watch_symbols != holding_symbols:
            self._watch_symbols = holding_symbols[:]
            if self._collector:
                self._collector.update_symbols(list(self._watch_symbols))
            logger.info(f"[캐시] watch_symbols 롤오버 리셋 → 보유종목만 유지: {holding_symbols}")

        logger.info(f"[캐시] 날짜 변경 → snapshot/strategy/버퍼/주문가드/재매수방지 초기화 ({today_str})")
        self._minute_store.reset_day()
        if self._collector:
            self._collector.reset_day()

    def _maybe_update_heartbeat(self, now: datetime):
        ts = time.time()
        if ts - self._last_heartbeat_ts < HEARTBEAT_INTERVAL_SECONDS:
            return

        with self._positions_lock:
            positions = dict(self._positions)

        self.firebase.update_heartbeat({
            "running": True,
            "last_tick": self.last_tick_time,
            "holding_count": len(positions),
        })

        # 평가손익 계산 후 daily_summary 갱신 (15초마다)
        try:
            unrealized_pnl = 0
            for sym, pos in positions.items():
                last_candles = self._collector.get_1min(sym) if self._collector else []
                last_price = last_candles[-1][4] if last_candles else 0  # 마지막 1분봉 종가
                entry_price = pos.get("entry_price", 0)
                qty = pos.get("qty", 0)
                if last_price and entry_price:
                    unrealized_pnl += int((last_price - entry_price) * qty)

            today = now.strftime("%Y-%m-%d")
            self._fs.collection("daily_trade_summary").document(today).set(
                {"unrealized_pnl": unrealized_pnl, "updated_at": now.isoformat()},
                merge=True,
            )
        except Exception as e:
            logger.error(f"[daily_summary] unrealized 갱신 실패: {e}")

        self._last_heartbeat_ts = ts

    def _check_holiday_once(self, now: datetime):
        """휴장일 여부를 하루 1번만 조회 — _tick() 초입에서 시간 무관하게 호출"""
        today_str = now.strftime("%Y%m%d")
        if self._holiday_checked_date == today_str:
            return
        # 주말은 API 호출 없이 즉시 비개장 처리
        if now.weekday() >= 5:
            self._today_opnd_yn = "N"
            self._holiday_checked_date = today_str
            logger.info(f"[휴장일조회] {today_str} 주말(weekday={now.weekday()}) → 비개장")
            return
        try:
            opnd_yn = self._fetch_holiday_once(today_str)
        except Exception as e:
            logger.warning(f"[휴장일조회] 실패({e}) → 개장으로 간주")
            opnd_yn = "Y"
        self._today_opnd_yn = opnd_yn
        self._holiday_checked_date = today_str
        if opnd_yn != "Y":
            logger.info(f"[휴장일조회] {today_str} 휴장일 → 모든 매매 처리 스킵")

    def _fetch_holiday_once(self, today_str: str) -> str:
        """kis_api._fetch_holiday_output 사용 (1회 호출, 재귀 없음)"""
        from trader.kis_api import _fetch_holiday_output
        output = _fetch_holiday_output(today_str)
        if not output:
            logger.warning("[휴장일조회] API 실패 → 개장으로 간주")
            return "Y"
        df = pd.DataFrame(output)
        if df.empty or "opnd_yn" not in df.columns:
            return "Y"
        row = df[df["bass_dt"] == today_str] if "bass_dt" in df.columns else df.iloc[:1]
        if row.empty:
            return "Y"
        opnd_yn = str(row["opnd_yn"].iloc[0]).strip().upper()
        logger.info(f"[휴장일조회] {today_str} opnd_yn={opnd_yn}")
        return opnd_yn

    def is_krx_open(self) -> bool:
        """KRX 개장 여부 판단 (fail-CLOSED).

        우선순위:
        1. weekday() >= 5 → 즉시 False (API 무관)
        2. 당일 캐시된 opnd_yn 재사용
        3. 캐시 없으면 직접 조회 — 실패 시 False(fail-CLOSED)
        """
        now = datetime.now(KST)
        # 1. 주말 확정 차단
        if now.weekday() >= 5:
            return False
        # 2. 당일 캐시 재사용
        today_str = now.strftime("%Y%m%d")
        if self._holiday_checked_date == today_str:
            return self._today_opnd_yn == "Y"
        # 3. 캐시 미확정 → 직접 조회, 실패 시 fail-CLOSED
        try:
            opnd_yn = self._fetch_holiday_once(today_str)
            return opnd_yn == "Y"
        except Exception as e:
            logger.warning(f"[개장판정] 조회 실패({e}) → 비개장으로 처리(fail-CLOSED)")
            return False

    def _maybe_premarket_warmup(self, now: datetime):
        hm = now.hour * 100 + now.minute
        today_str = now.strftime("%Y%m%d")

        if not (850 <= hm < 900):
            return

        # 휴장일이면 스킵 (_tick에서 _check_holiday_once 완료됨)
        if self._today_opnd_yn != "Y":
            logger.info(f"[장전워밍업] {today_str} 휴장일 → 스킵")
            self._premarket_warmup_done_date = today_str
            return

        # 1. 전일 보유 종목을 거래대금 필터와 무관하게 감시목록/WS에 강제 편입
        with self._positions_lock:
            holding_symbols = list(self._positions.keys())

        if holding_symbols:
            added = [s for s in holding_symbols if s not in self._watch_symbols]
            if added:
                self._watch_symbols = list(self._watch_symbols) + added
                if self._collector:
                    self._collector.update_symbols(list(self._watch_symbols))
                logger.info(f"[장전워밍업] 보유종목 WS 구독 추가: {added}")
                from trader.telegram import send_telegram
                send_telegram(f"⏰ [워밍업] 보유종목 {added} 감시목록 편입")

        # 3. 워밍업만 재시도
        if self._premarket_warmup_done_date != today_str:
            logger.info("[장전워밍업] 전일 보유종목 snapshot + strategy cache 사전 로드")
            success = self._warmup_market_data()
            if success:
                self._premarket_warmup_done_date = today_str
                from trader.telegram import send_telegram
                send_telegram("⏰ [워밍업] snapshot + 1분봉 seed 완료")
            else:
                logger.warning("[장전워밍업] 일부 실패 → 다음 틱에서 재시도")

        # 4. 마켓 게이트 1단계 로드 (Firestore gate/policy — 코스닥 갭은 09:00 이후 2단계에서 확정)
        self._load_market_gate_phase1(today_str)

    def _load_market_gate_phase1(self, today_str: str):
        """
        [1단계 — 08:50~08:59 워밍업 타임]
        Firestore market_analysis/latest → foreign_signal 필드에서
        전날 외국인 수급 분석 결과를 읽어 scan_hm / market_top_n 을 잠정 결정한다.
        모든 gate가 _gate_pending=True 로 설정되어 09:01 이후 phase2에서 최종 확정한다.
        텔레그램 발송은 phase2 완료 후 1회만 발송한다.
        """
        if self._gate_loaded_date == today_str:
            return

        # ── 1. Firestore 조회 ─────────────────────────────
        try:
            doc = self._fs.collection("market_analysis").document("latest").get()
            if not doc.exists:
                self._gate_retry_count += 1
                if self._gate_retry_count > 5:
                    logger.warning("[마켓게이트][1단계] 최대 재시도 초과 → 기본값(09:20·10종목) 유지")
                    self._gate_loaded_date = today_str
                else:
                    logger.warning(
                        f"[마켓게이트][1단계] market_analysis/latest 없음 → "
                        f"다음 틱 재시도 ({self._gate_retry_count}/5)"
                    )
                return

            fs_data    = (doc.to_dict() or {}).get("foreign_signal", {})
            self._market_analysis_snapshot = doc.to_dict() or {}
            gate       = str(fs_data.get("gate", "UNKNOWN")).strip().upper()
            policy     = str(fs_data.get("entry_policy", "ALLOW")).strip().upper()
            confidence = float(fs_data.get("confidence", 0))
            updated_at = str(fs_data.get("updated_at", ""))

        except Exception as e:
            self._gate_retry_count += 1
            if self._gate_retry_count > 5:
                logger.warning(f"[마켓게이트][1단계] 최대 재시도 초과({e}) → 기본값(09:20·10종목) 유지")
                self._gate_loaded_date = today_str
            else:
                logger.warning(
                    f"[마켓게이트][1단계] Firestore 조회 실패({e}) → "
                    f"다음 틱 재시도 ({self._gate_retry_count}/5)"
                )
            return

        self._gate_retry_count       = 0
        self._market_gate            = gate
        self._market_gate_confidence = confidence
        self._market_gate_policy     = policy

        # ── 2. 잠정값 결정 (전일 수급 기반) ─────────────────
        if gate == "BULL" and confidence >= 0.8:
            self._scan_hm      = 910
            self._market_top_n = 10
            label = "🟢 BULL(고신뢰) → 잠정 09:10·10종목"
        elif gate == "BULL":
            self._scan_hm      = 915
            self._market_top_n = 10
            label = "🟡 BULL(저신뢰) → 잠정 09:15·10종목"
        elif gate == "NEUTRAL":
            self._scan_hm      = 920
            self._market_top_n = 10
            label = "⚪ NEUTRAL → 잠정 09:20·10종목"
        elif policy == "BLOCK" and confidence >= 0.8:
            self._scan_hm      = 930
            self._market_top_n = 5
            label = "⛔ BEAR(BLOCK/고신뢰) → 잠정 09:30·5종목"
        elif policy == "BLOCK":
            self._scan_hm      = 925
            self._market_top_n = 7
            label = "⚠️ BEAR(BLOCK/저신뢰) → 잠정 09:25·7종목"
        elif gate == "BEAR" and confidence >= 0.8:
            self._scan_hm      = 925
            self._market_top_n = 5
            label = "🔴 BEAR(고신뢰) → 잠정 09:25·5종목"
        elif gate == "BEAR":
            self._scan_hm      = 920
            self._market_top_n = 7
            label = "🟠 BEAR(저신뢰) → 잠정 09:20·7종목"
        else:
            self._scan_hm      = 920
            self._market_top_n = 10
            label = f"❓ {gate}(UNKNOWN) → 잠정 09:20·10종목"

        self._gate_pending     = True
        self._gate_loaded_date = today_str

        logger.info(
            f"[마켓게이트][1단계] {label} | gate={gate} | policy={policy} | "
            f"confidence={confidence:.0%} | updated={updated_at[:10] if updated_at else '?'}"
        )
        # 텔레그램은 phase2 확정 후 1회만 발송

    def _load_market_gate_phase2(self):
        """
        [2단계 — 09:01~09:10, 전 gate 실행]
        코스피(0001) + 코스닥(1001) 실시간 등락률로 phase1 잠정값을 보정해 최종 확정한다.

        복합 점수 → 잠정 scan_hm 보정:
          ≥ +4 : -10분 / top_n +2
          ≥ +2 : -5분  / top_n +1
           0~+1: 잠정값 유지
          -1~-2: +5분  / top_n -1
           < -2: +10분 / top_n -2

        gate별 클램핑:
          BULL / NEUTRAL          : scan_hm [09:10~09:25], top_n [7~10]
          BEAR/BLOCK 저신뢰        : scan_hm [09:15~09:30], top_n [5~10]
          BEAR/BLOCK 고신뢰        : scan_hm [09:20~09:30], top_n [5~7]
        """
        if not self._gate_pending:
            return

        from trader.telegram import send_telegram
        from trader.kis_api import get_index_change_rate

        def _retry_index(index_code: str, label: str, retry: int = 3, delay: float = 0.5) -> Optional[float]:
            for attempt in range(1, retry + 1):
                value = get_index_change_rate(index_code)
                if value is not None:
                    if attempt > 1:
                        logger.info(f"[마켓게이트][2단계] {label} 조회 성공 ({attempt}/{retry}) | {value:+.2f}%")
                    return value
                logger.warning(f"[마켓게이트][2단계] {label} 미준비 ({attempt}/{retry})")
                if attempt < retry:
                    time.sleep(delay)
            return None

        try:
            # ── 1. 지수 조회 ──────────────────────────────────
            kosdaq_ctrt = _retry_index("1001", "코스닥")
            if kosdaq_ctrt is None:
                logger.warning("[마켓게이트][2단계] 코스닥 3회 실패 → 다음 틱 재시도")
                return

            kospi_failed = False
            kospi_ctrt   = _retry_index("0001", "코스피")
            if kospi_ctrt is None:
                kospi_failed = True
                kospi_ctrt   = 0.0
                logger.warning("[마켓게이트][2단계] 코스피 3회 실패 → 0으로 처리")

            # ── 2. 복합 점수 산출 (코스닥 3:2 코스피) ─────────
            if kosdaq_ctrt >= 1.5:    kosdaq_score = 3
            elif kosdaq_ctrt >= 0.5:  kosdaq_score = 1
            elif kosdaq_ctrt >= 0.0:  kosdaq_score = 0
            elif kosdaq_ctrt >= -0.5: kosdaq_score = -1
            elif kosdaq_ctrt >= -1.5: kosdaq_score = -1
            else:                     kosdaq_score = -2

            if kospi_ctrt >= 1.5:     kospi_score = 2
            elif kospi_ctrt >= 0.5:   kospi_score = 1
            elif kospi_ctrt >= 0.0:   kospi_score = 0
            elif kospi_ctrt >= -0.5:  kospi_score = -1
            elif kospi_ctrt >= -1.5:  kospi_score = -1
            else:                     kospi_score = -2

            total_score = kosdaq_score + kospi_score

            # ── 3. 사후 보정 ───────────────────────────────────
            if kospi_ctrt <= -1.5 and total_score > -2:
                logger.info(f"[마켓게이트][2단계] 코스피 급락({kospi_ctrt:+.2f}%) → 총점 {total_score:+d} → -2 보정")
                total_score = -2
            if kosdaq_ctrt <= -1.0 and kospi_ctrt <= -1.0:
                logger.info(f"[마켓게이트][2단계] 양시장 동반급락 → 총점 {total_score:+d} → -3 강제")
                total_score = -3

            # ── 4. 잠정값 기준 보정 ───────────────────────────
            gate       = self._market_gate
            policy     = self._market_gate_policy
            confidence = self._market_gate_confidence
            is_high_conf_block = (policy == "BLOCK" and confidence >= 0.8)
            is_bear_or_block   = (gate == "BEAR" or policy == "BLOCK")

            base_hm = self._scan_hm
            base_n  = self._market_top_n

            if total_score >= 4:
                delta_min, delta_n = -10, +2
            elif total_score >= 2:
                delta_min, delta_n = -5,  +1
            elif total_score >= 0:
                delta_min, delta_n =  0,   0
            elif total_score >= -2:
                delta_min, delta_n = +5,  -1
            else:
                delta_min, delta_n = +10, -2

            base_total_min = (base_hm // 100) * 60 + (base_hm % 100)
            adj_total_min  = base_total_min + delta_min
            adj_hm = (adj_total_min // 60) * 100 + (adj_total_min % 60)
            adj_n  = base_n + delta_n

            # ── 5. gate별 클램핑 ──────────────────────────────
            if is_high_conf_block:
                adj_hm = max(920, min(adj_hm, 930))
                adj_n  = max(5,   min(adj_n,  7))
            elif is_bear_or_block:
                adj_hm = max(915, min(adj_hm, 930))
                adj_n  = max(5,   min(adj_n,  10))
            else:
                adj_hm = max(910, min(adj_hm, 925))
                adj_n  = max(7,   min(adj_n,  10))

            self._scan_hm      = adj_hm
            self._market_top_n = adj_n
            self._gate_pending = False

            # ── 6. 결과 로그 / 텔레그램 ──────────────────────
            scan_str    = f"{adj_hm // 100:02d}:{adj_hm % 100:02d}"
            kospi_label = "조회실패(0처리)" if kospi_failed else f"{kospi_ctrt:+.2f}%"
            result      = f"{scan_str}·{adj_n}종목"
            gate_label  = f"{gate}({'BLOCK/' if policy == 'BLOCK' else ''}{'고신뢰' if confidence >= 0.8 else '저신뢰'})"

            logger.info(
                f"[마켓게이트][2단계] 코스닥{kosdaq_ctrt:+.2f}%(점수:{kosdaq_score:+d}) | "
                f"코스피{kospi_label}(점수:{kospi_score:+d}) | "
                f"합계:{total_score:+d} | 잠정→확정: "
                f"{base_hm//100:02d}:{base_hm%100:02d}·{base_n}종목 → {result}"
            )
            is_rsi2 = any(
                v.get("strategy") == "RSI2_REVERSAL" for v in self._symbol_meta.values()
            )
            if is_rsi2:
                send_telegram(
                    f"📊 [지수 참고] {gate_label}\n"
                    f"코스닥 {kosdaq_ctrt:+.2f}% / 코스피 {kospi_label}\n"
                    f"복합점수: {total_score:+d}\n"
                    f"gate={gate} | policy={policy} | 신뢰도={confidence:.0%}\n"
                    f"※ RSI2 모드: 09:00 시가 진입 완료, 등록 종목 전량 편입, "
                    f"scan_hm/BLOCK/종목수 미적용"
                )
            else:
                send_telegram(
                    f"📊 [마켓게이트 확정] {gate_label} → {result}\n"
                    f"코스닥 {kosdaq_ctrt:+.2f}% / 코스피 {kospi_label}\n"
                    f"복합점수: {total_score:+d} (잠정 {base_hm//100:02d}:{base_hm%100:02d}·{base_n}종목에서 보정)\n"
                    f"gate={gate} | policy={policy} | 신뢰도={confidence:.0%}"
                )

        except Exception as e:
            logger.warning(f"[마켓게이트][2단계] 오류({e}) → 잠정값 그대로 확정")
            self._gate_pending = False

    @staticmethod
    def _prev_biz_day(date: datetime) -> str:
        """직전 영업일 날짜 반환 (주말만 건너뜀 — 공휴일 미처리)."""
        d = date.date() - timedelta(days=1)
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        return d.strftime("%Y%m%d")

    def _try_rsi2_early_load(self, now: datetime):
        """RSI2 역전 모드 감지 시 09:00~09:04에 조기 편입 실행.
        조회 예외 시 _rsi2_early_check_done=False 유지 → 다음 틱 재시도.
        비역전 모드 확인 시 True → 이후 틱 불필요한 조회 방지.
        역전 모드 성공 시 _trade_filter_done=True가 되므로 중복 실행은 상위 가드가 차단.
        """
        try:
            ts_snap = list(self._fs.collection("target_stocks").stream())
            is_reversal = any(
                (d.to_dict() or {}).get("strategy") == "RSI2_REVERSAL"
                for d in ts_snap
            )
            if not is_reversal:
                self._rsi2_early_check_done = True  # 비역전 모드: 재조회 불필요
                return
            today_str = now.strftime("%Y%m%d")
            hm_str = str(now.hour * 100 + now.minute)
            logger.info(f"[RSI2] 조기 편입 실행 ({hm_str}, 시가 진입 준비)")
            self._apply_rsi2_reversal_filter(ts_snap, now, today_str, hm_str)
            # 성공 시: _trade_filter_done=True → 상위 가드가 재진입 차단 (플래그 별도 불필요)
        except Exception as e:
            logger.warning(f"[RSI2] 조기 편입 조회 실패, 다음 틱 재시도: {e}")
            # _rsi2_early_check_done은 False로 유지 → 재시도 허용

    def _apply_rsi2_reversal_filter(
        self, ts_docs: list, now: datetime, today_str: str, now_hm: str
    ):
        """
        RSI2 역전 모드 종목 편입.
        배치가 전일 16:00에 target_stocks에 저장한 종목을 그대로 사용.
        재정렬·재선정 금지. score 내림차순은 배치 저장 순서 보완용.
        selected_date 필드 없으면 fail-closed(스킵).
        결격 제외: temp_stop_yn / mang_issu_cls_code / sltr_yn / 시가상한가.
        """
        import time as _time
        from trader.telegram import send_telegram

        logger.info(f"[RSI2역전필터] 시작 ({now_hm}) — 배치 확정 순서 유지, 결격 제외만")

        # ── 1. target_stocks 로드 ─────────────────────────────────────────
        items = []
        for doc in ts_docs:
            data = doc.to_dict() or {}
            if data.get("strategy") != "RSI2_REVERSAL":
                continue
            ticker = data.get("ticker", "").strip()
            if not ticker:
                continue
            items.append({
                "ticker":            ticker,
                "name":              data.get("name", ticker),
                "score":             float(data.get("score", 0)),
                "stop_loss":         float(data.get("stop_loss", 0) or 0),
                "selected_date":     str(data.get("selected_date", "") or ""),
                "strategy":          "RSI2_REVERSAL",
                "passed_strategies": data.get("passed_strategies", []),
                "financial_grade":   data.get("financial_grade", ""),
                "exchange":          data.get("exchange", "KRX"),
                "confidence":        int(data.get("confidence", 0)),
            })

        if not items:
            logger.warning("[RSI2역전필터] RSI2_REVERSAL 종목 없음 → 스킵")
            send_telegram("⚠️ [RSI2역전필터] target_stocks에 역전 종목 없음 → 스킵")
            self._trade_filter_done = True
            return

        # ── 2. selected_date 직전 영업일 검증 (fail-closed) ──────────────
        prev_biz     = self._prev_biz_day(now)
        sample_date  = items[0]["selected_date"]

        if not sample_date:
            logger.error(
                f"[RSI2역전필터] selected_date 필드 없음 → fail-closed (스킵). "
                f"배치 스키마에 selected_date 추가 필요."
            )
            send_telegram("❌ [RSI2역전필터] selected_date 필드 없음 → fail-closed. 배치 스키마 확인 필요.")
            self._trade_filter_done = True
            return

        if sample_date != prev_biz:
            logger.error(
                f"[RSI2역전필터] selected_date({sample_date}) ≠ 직전영업일({prev_biz}) "
                f"→ fail-closed (stale 데이터)."
            )
            send_telegram(
                f"❌ [RSI2역전필터] selected_date({sample_date}) ≠ 직전영업일({prev_biz})\n"
                f"stale 데이터 → fail-closed. 배치 실행 여부 확인 필요."
            )
            self._trade_filter_done = True
            return

        # ── 3. score 내림차순 정렬 (개수 절단 없음 — 등록 전량 편입) ─────
        items.sort(key=lambda x: x["score"], reverse=True)

        logger.info(f"[RSI2역전필터] {len(items)}개 종목 결격 검사 시작 (직전영업일={prev_biz})")

        # ── 4. 결격 제외 ─────────────────────────────────────────────────
        # temp_stop_yn=Y(임시거래정지), mang_issu_cls_code≠N(관리종목),
        # sltr_yn=Y(정리매매), 시가≥전일종가×1.295(상한가 시가)
        selected = []
        excluded = []

        for item in items:
            ticker = item["ticker"]
            try:
                res = _url_fetch(
                    "/uapi/domestic-stock/v1/quotations/inquire-price",
                    "FHKST01010100", "",
                    {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker},
                )
                _time.sleep(0.2)

                if not res.isOK():
                    excluded.append((ticker, item["name"], "API실패"))
                    logger.warning(f"[RSI2역전필터] {ticker} API실패 → 결격")
                    continue

                output    = res.getBody().output
                temp_stop = str(output.get("temp_stop_yn",       "N") or "N").strip()
                mang_issu = str(output.get("mang_issu_cls_code", "N") or "N").strip()
                sltr      = str(output.get("sltr_yn",            "N") or "N").strip()
                stck_sdpr = int(output.get("stck_sdpr", 0) or 0)
                stck_oprc = int(output.get("stck_oprc", 0) or 0)

                if temp_stop == "Y":
                    reason = "임시거래정지(temp_stop_yn=Y)"
                elif mang_issu != "N":
                    reason = f"관리종목(mang_issu_cls_code={mang_issu})"
                elif sltr == "Y":
                    reason = "정리매매(sltr_yn=Y)"
                elif stck_sdpr > 0 and stck_oprc >= int(stck_sdpr * 1.295):
                    reason = f"시가상한가({stck_oprc}/{stck_sdpr})"
                else:
                    reason = None

                if reason:
                    logger.warning(f"[RSI2역전필터] {ticker} {reason} → 결격")
                    excluded.append((ticker, item["name"], reason))
                else:
                    selected.append(item)

            except Exception as e:
                excluded.append((ticker, item["name"], f"오류:{e}"))
                logger.warning(f"[RSI2역전필터] {ticker} 오류({e}) → 결격")

        if excluded:
            exc_lines = "\n".join(
                f"  {name}({code}): {reason}"
                for code, name, reason in excluded
            )
            logger.info(f"[RSI2역전필터] 결격 제외 {len(excluded)}개:\n{exc_lines}")
            send_telegram(f"⚠️ [RSI2역전필터] 결격 제외 {len(excluded)}개\n{exc_lines}")

        if not selected:
            logger.warning("[RSI2역전필터] 결격 제외 후 진입 종목 없음 → 스킵")
            send_telegram("⚠️ [RSI2역전필터] 결격 제외 후 진입 종목 없음")
            self._trade_filter_done = True
            return

        logger.info(f"[RSI2역전필터] 최종 선정 {len(selected)}개:")
        for i, item in enumerate(selected, 1):
            logger.info(
                f"  {i}위 {item['name']}({item['ticker']}) | score={item['score']:.1f}"
            )

        # ── 5. symbol_meta 갱신 ──────────────────────────────────────────
        for item in selected:
            self._symbol_meta[item["ticker"]] = {
                "name":      item["name"],
                "strategy":  "RSI2_REVERSAL",
                "score":     item["score"],
                "stop_loss": item.get("stop_loss", 0.0),  # 절대가(원화) — 배치 저장 단위와 일치해야 함
                "exchange":  item.get("exchange", "KRX"),
            }

        # ── 6. watch_symbols 교체 (보유 포지션 강제 편입) ────────────────
        new_symbols = [item["ticker"] for item in selected]
        with self._positions_lock:
            holding_symbols = list(self._positions.keys())
        for s in holding_symbols:
            if s not in new_symbols:
                new_symbols.append(s)

        prev_symbols        = self._watch_symbols[:]
        self._watch_symbols = new_symbols
        if self._collector:
            self._collector.update_symbols(list(self._watch_symbols))

        added   = [s for s in new_symbols if s not in prev_symbols]
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

        self._trade_filter_done = True

        # ── 7. MinuteStore 초기화 ─────────────────────────────────────────
        try:
            self._minute_store.init_from_selection(
                pool=selected,
                selected=selected,
                gate_state={
                    "gate":         self._market_gate,
                    "confidence":   self._market_gate_confidence,
                    "policy":       self._market_gate_policy,
                    "scan_hm":      self._scan_hm,
                    "market_top_n": self._market_top_n,
                },
                market_analysis=self._market_analysis_snapshot,
                ws_collector=self._collector,
            )
        except Exception as e:
            logger.error(f"[MinuteStore] init_from_selection 실패: {e}")

        logger.info(
            f"[RSI2역전필터] watch_symbols 교체 완료: "
            f"{len(new_symbols)}개 (추가={added}, 제거={removed})"
        )

        # ── 8. 전일 데이터 워밍업 ─────────────────────────────────────────
        self._warmup_market_data()

        # ── 9. 텔레그램 알림 ──────────────────────────────────────────────
        lines = [f"🔄 <b>RSI2 역전 종목 편입 완료</b> ({now_hm}, {len(selected)}개)"]
        for i, item in enumerate(selected, 1):
            lines.append(
                f"{i}위 {item['name']}({item['ticker']}) | score={item['score']:.1f}"
            )
        send_telegram("\n".join(lines))

    def _filter_by_trade_amount(self, _preloaded_ts_docs: list = None):
        """
        strategy_results 전체 거래대금 + 갭 조건 조회 → 상위 TOP_N개로 watch_symbols 교체
        - scan_hm(마켓게이트 기반 동적 시각) 자동 실행 (하루 1회)
        - scan_hm+5분 이후 미완료 시 즉시 실행 (폴백)
        - TOP_N: gate=BULL/NEUTRAL → 10개, BEAR(저신뢰) → 7개, BEAR(고신뢰)/BLOCK → 5개
        - confidence 모드: tr_pbmn_date == today 이면 재시작 복원 스킵
        - RSI2 역전 모드: _apply_rsi2_reversal_filter() 위임
        """
        import time as _time
        from trader.telegram import send_telegram

        # 비거래일 가드 — 정규 트리거·폴백·재시작 복원 모든 경로를 여기서 차단
        # weekday() >= 5 체크가 첫 API 호출보다 반드시 앞서므로 주말엔 API 미호출
        if not self.is_krx_open():
            send_telegram("🛌 [휴장일] 거래대금필터 스킵")
            self._trade_filter_done = True
            return

        if self._gate_pending:
            logger.warning(
                "[마켓게이트] gate pending 미해소 상태로 필터 실행 "
                f"(지수 확인 실패) → scan_hm={self._scan_hm}, top_n={self._market_top_n}"
            )
            send_telegram(
                "⚠️ [마켓게이트] gate pending 미해소\n지수 확인 실패 → 잠정값으로 실행"
            )
            self._gate_pending = False

        TOP_N        = self._market_top_n  # gate 기반 동적 종목 수 (기본 10, BEAR 시 5~7)
        API_INTERVAL = 0.2   # 초
        GAP_MIN_PCT  = 1.0   # 시가갭 하한 (%)
        GAP_MAX_PCT  = 15.0  # 시가갭 상한 (%) — 과열 제외

        now       = datetime.now(KST)
        today_str = now.strftime("%Y%m%d")
        now_hm    = now.strftime("%H:%M")

        # ── RSI2 역전 모드 감지 ────────────────────────────────────────────
        # target_stocks에 strategy == 'RSI2_REVERSAL' 종목이 하나라도 있으면 역전 모드.
        try:
            _ts_docs   = _preloaded_ts_docs if _preloaded_ts_docs is not None \
                         else list(self._fs.collection("target_stocks").stream())
            _is_reversal = any(
                (d.to_dict() or {}).get("strategy") == "RSI2_REVERSAL"
                for d in _ts_docs
            )
        except Exception as e:
            logger.warning(f"[역전모드감지] target_stocks 조회 실패({e}) → confidence 모드로 진행")
            _is_reversal = False
            _ts_docs     = []

        if _is_reversal:
            self._apply_rsi2_reversal_filter(_ts_docs, now, today_str, now_hm)
            return

        logger.info(f"[거래대금필터] 시작 ({now_hm}) — strategy_results 전체 조회")

        # ── 1. strategy_results 읽기 (summaryBadge == '진입') ──
        try:
            docs = self._fs.collection("strategy_results").stream()
            candidates = []
            for doc in docs:
                data = doc.to_dict()
                if not data:
                    continue
                if data.get("summaryBadge") != "진입":
                    continue
                ticker = data.get("code", "").strip()
                if not ticker:
                    continue
                candidates.append({
                    "ticker":            ticker,
                    "name":              data.get("name", ticker),
                    "strategy":          "",
                    "score":             float(data.get("confidence", 0)),
                    "passed_strategies": data.get("passed_strategies", []),
                    "financial_grade":   data.get("valuation", ""),
                    "confidence":        int(data.get("confidence", 0)),
                    "exchange":          data.get("exchange", "KRX"),
                })
        except Exception as e:
            logger.error(f"[거래대금필터] strategy_results 읽기 실패: {e}")
            return

        if not candidates:
            logger.warning("[거래대금필터] strategy_results 진입 후보 없음 → 스킵")
            send_telegram("⚠️ [거래대금필터] 진입 후보 종목 없음 → 스킵")
            self._trade_filter_done = True
            return

        logger.info(f"[거래대금필터] {len(candidates)}개 후보 거래대금+갭 조회 시작")
        send_telegram(f"🔍 [거래대금필터] {len(candidates)}개 종목 거래대금+갭 조회 시작 ({now_hm})")

        # ── 2. 거래대금 + 갭 조회 및 필터 ──────────────
        results    = []
        fail_count = 0
        skip_count = 0
        skip_flat  = 0   # 시가갭 1% 미만 (평평)
        skip_hot   = 0   # 시가갭 15% 초과 (과열)
        skip_neg   = 0   # 갭 마이너스 + 장중 미회복

        for item in candidates:
            ticker = item["ticker"]
            try:
                res = _url_fetch(
                    "/uapi/domestic-stock/v1/quotations/inquire-price",
                    "FHKST01010100", "",
                    {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker},
                )
                _time.sleep(API_INTERVAL)

                if not res.isOK():
                    fail_count += 1
                    logger.warning(f"[거래대금필터] {ticker} API 실패 → 제외")
                    continue

                output = res.getBody().output
                stck_prpr    = int(output.get("stck_prpr",    0) or 0)
                stck_oprc    = int(output.get("stck_oprc",    0) or 0)
                stck_sdpr    = int(output.get("stck_sdpr",    0) or 0)
                acml_tr_pbmn = int(output.get("acml_tr_pbmn", 0) or 0)
                acml_vol     = int(output.get("acml_vol",     0) or 0)

                if stck_sdpr <= 0:
                    fail_count += 1
                    continue

                gap_pct     = round((stck_oprc - stck_sdpr) / stck_sdpr * 100, 2)
                current_pct = round((stck_prpr - stck_sdpr) / stck_sdpr * 100, 2)
                gap_hold    = stck_prpr >= stck_oprc

                # 갭 조건 필터
                # cond1: 시가갭 1~15%
                # cond2: 시가갭 마이너스 + 현재가 ≥ 시가 + 시가 대비 현재갭 ≥ 1%
                # cond3: 시가갭 마이너스 + 저가가 시가 아래로 내려갔다가 + 저가 대비 현재가 ≥ 2% 반등 + 현재가 ≥ 시가
                cond1 = GAP_MIN_PCT <= gap_pct <= GAP_MAX_PCT
                intraday_pct = round((stck_prpr - stck_oprc) / stck_oprc * 100, 2) if stck_oprc > 0 else 0
                stck_lwpr    = int(output.get("stck_lwpr", 0) or 0)
                low_pct      = round((stck_prpr - stck_lwpr) / stck_lwpr * 100, 2) if stck_lwpr > 0 else 0
                cond2 = gap_pct < 0 and gap_hold and intraday_pct >= 1.0
                cond3 = gap_pct < 0 and stck_lwpr < stck_oprc and low_pct >= 2.0 and gap_hold
                if not (cond1 or cond2 or cond3):
                    skip_count += 1
                    if gap_pct > GAP_MAX_PCT:
                        skip_hot += 1
                    elif gap_pct >= 0:
                        skip_flat += 1
                    else:
                        skip_neg += 1
                    continue

                item["tr_pbmn"]     = acml_tr_pbmn
                item["acml_vol"]    = acml_vol
                item["gap_pct"]     = gap_pct
                item["current_pct"] = current_pct
                item["price"]       = stck_prpr
                results.append(item)

            except Exception as e:
                fail_count += 1
                logger.warning(f"[거래대금필터] {ticker} 오류: {e} → 제외")

        logger.info(
            f"[거래대금필터] 조회 완료: {len(results)}개 통과 / "
            f"{skip_count}개 갭미달(평평={skip_flat}/과열={skip_hot}/마이너스미회복={skip_neg}) / "
            f"{fail_count}개 실패"
        )

        if not results:
            logger.error("[거래대금필터] 갭 조건 통과 종목 없음 → watch_symbols 유지")
            self._trade_filter_done = True
            return

        # ── 3. 거래대금 상위 풀 추출 → 당일상대강도(tday_rltv) 복합 점수 정렬 ────
        # 설계 근거:
        #   거래대금: 시장 관심도 + 유동성 확보 → 진입 가능한 종목 필터
        #   tday_rltv: 당일 상대강도 (100 이상=매수우위, 100 미만=매도우위)
        #              FHKST01010300(주식체결) 응답 필드 — 장 초반 방향성 예측력 높음
        #   거래대금 하한선(300억): 미만 종목은 슬리피지/미체결 위험
        #   비중: 거래대금 0.7 + 상대강도 0.3 (유동성 우선, 방향성 보조)
        MIN_TR_PBMN = 300_0000_0000   # 300억 하한선
        POOL_SIZE   = max(TOP_N * 2, 20)  # 상대강도 비교 풀 (최소 20개)

        # 거래대금 하한선 필터
        results_filtered = [x for x in results if x["tr_pbmn"] >= MIN_TR_PBMN]
        if not results_filtered:
            logger.warning(
                f"[거래대금필터] 300억 하한선 통과 종목 없음 → 하한선 없이 진행"
            )
            results_filtered = results

        results_filtered.sort(key=lambda x: x["tr_pbmn"], reverse=True)
        pool = results_filtered[:POOL_SIZE]

        # 당일 상대강도 조회 (FHKST01010300 — 주식체결)
        logger.info(f"[거래대금필터] 상대강도 조회 시작 ({len(pool)}개)")
        for item in pool:
            ticker = item["ticker"]
            try:
                res = _url_fetch(
                    "/uapi/domestic-stock/v1/quotations/inquire-ccnl",
                    "FHKST01010300", "",
                    {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker},
                )
                _time.sleep(API_INTERVAL)
                if res.isOK():
                    ccnl_output = res.getBody().output
                    # output은 최근 체결 리스트 — 첫 번째 레코드의 tday_rltv 사용
                    if isinstance(ccnl_output, list) and ccnl_output:
                        item["exec_strength"] = float(ccnl_output[0].get("tday_rltv", 100.0) or 100.0)
                    else:
                        item["exec_strength"] = 100.0
                else:
                    item["exec_strength"] = 100.0
                    logger.warning(f"[거래대금필터] {ticker} 상대강도 조회 실패 → 100 기본값")
            except Exception as e:
                item["exec_strength"] = 100.0
                logger.warning(f"[거래대금필터] {ticker} 상대강도 오류: {e} → 100 기본값")

        # ── RVT(상대 거래량) 조회 ────────────────────────────
        # daily_candles/{ticker}.avg_volume_20d (20일 평균 거래량, 주식 수) 사용.
        # 거래대금이 아닌 거래량 비율(RVOL)로 평가 — 절대 거래대금이 큰 종목 편향 완화.
        for item in pool:
            ticker = item["ticker"]
            try:
                snap = self._fs.collection("daily_candles").document(ticker).get()
                if not snap.exists:
                    item["avg_volume_20d"] = 0
                    item["rvt"]            = None
                    logger.warning(f"[거래대금필터] {ticker} daily_candles 문서 없음 → RVT 제외")
                    continue
                data    = snap.to_dict() or {}
                avg_vol = float(data.get("avg_volume_20d", 0) or 0)
                item["avg_volume_20d"] = avg_vol
                if avg_vol > 0 and item["acml_vol"] > 0:
                    item["rvt"] = round(item["acml_vol"] / avg_vol, 4)
                else:
                    item["rvt"] = None
                    logger.warning(
                        f"[거래대금필터] {ticker} RVT 계산 불가 "
                        f"(avg20={avg_vol}, cur_vol={item['acml_vol']}) → 제외"
                    )
            except Exception as e:
                item["avg_volume_20d"] = 0
                item["rvt"]            = None
                logger.warning(f"[거래대금필터] {ticker} daily_candles 조회 오류: {e} → RVT 제외")

        # ── A안: 거래대금 0.7 + 상대강도 0.3 (운영 정렬 — 현행 유지) ─────────
        tr_vals = [x["tr_pbmn"]       for x in pool]
        es_vals = [x["exec_strength"]  for x in pool]
        tr_min, tr_max = min(tr_vals), max(tr_vals)
        es_min, es_max = min(es_vals), max(es_vals)

        for item in pool:
            tr_norm = (item["tr_pbmn"]      - tr_min) / (tr_max - tr_min) if tr_max > tr_min else 1.0
            es_norm = (item["exec_strength"] - es_min) / (es_max - es_min) if es_max > es_min else 1.0
            item["composite_score"] = round(tr_norm * 0.7 + es_norm * 0.3, 4)

        # ── B안: RVT 0.625 + 상대강도 0.375 (병행 산출, 검증용) ───────────
        # 가중치 50/30 → 외국인 20% 제외 후 정규화: 0.5/(0.5+0.3)=0.625, 0.3/(0.5+0.3)=0.375
        valid_rvts = [x["rvt"] for x in pool if x["rvt"] is not None]
        if valid_rvts:
            rvt_min, rvt_max = min(valid_rvts), max(valid_rvts)
        else:
            rvt_min, rvt_max = 0.0, 0.0

        for item in pool:
            if item["rvt"] is None:
                item["rvt_score"] = None
                continue
            rvt_norm = (item["rvt"] - rvt_min) / (rvt_max - rvt_min) if rvt_max > rvt_min else 1.0
            es_norm  = (item["exec_strength"] - es_min) / (es_max - es_min) if es_max > es_min else 1.0
            item["rvt_score"] = round(rvt_norm * 0.625 + es_norm * 0.375, 4)

        # 두 정렬 모두 수행
        pool_a = sorted(pool, key=lambda x: x["composite_score"], reverse=True)
        pool_b = sorted(
            pool,
            # RVT 계산 실패 종목은 후순위로
            key=lambda x: (x["rvt_score"] if x["rvt_score"] is not None else -1),
            reverse=True,
        )
        a_rank_map = {x["ticker"]: i + 1 for i, x in enumerate(pool_a)}
        b_rank_map = {x["ticker"]: i + 1 for i, x in enumerate(pool_b)}
        for item in pool:
            item["a_rank"] = a_rank_map[item["ticker"]]
            item["b_rank"] = b_rank_map[item["ticker"]]

        # ── 운영 정렬 선택 (USE_RVT_SCORING 토글) ─────────────────────
        if USE_RVT_SCORING:
            selected = pool_b[:TOP_N]
            sort_label = "B안(RVT)"
        else:
            selected = pool_a[:TOP_N]
            sort_label = "A안(거래대금)"

        # ── [종목선정 비교] — 풀 전체 A/B 순위 비교 로그 ────────────
        logger.info(f"[종목선정 비교] 풀 {len(pool)}개 — 운영 정렬: {sort_label}")
        logger.info(
            "  "
            "A순위 B순위  Δ   "
            "종목명(코드)        "
            "현재거래대금  20일평균거래량   RVT   상대강도"
        )
        for item in sorted(pool, key=lambda x: x["a_rank"]):
            pbmn_bil = item["tr_pbmn"] / 1e8
            avg_vol  = item.get("avg_volume_20d", 0)
            rvt_str  = f"{item['rvt']:>5.2f}" if item["rvt"] is not None else "  N/A"
            delta    = item["a_rank"] - item["b_rank"]  # +면 B에서 순위↑(상승), -면 하락
            arrow    = f"▲{delta}" if delta > 0 else (f"▼{-delta}" if delta < 0 else " 0")
            name     = f"{item['name']}({item['ticker']})"
            logger.info(
                f"  {item['a_rank']:>3}  {item['b_rank']:>3}  {arrow:>3}   "
                f"{name:<20}  "
                f"{pbmn_bil:>6.0f}억      "
                f"{avg_vol:>12,.0f}   "
                f"{rvt_str}    {item['exec_strength']:>5.1f}"
            )

        logger.info(f"[거래대금필터] 운영 선정 {len(selected)}개 ({sort_label}):")
        for i, item in enumerate(selected, 1):
            pbmn_bil = item["tr_pbmn"] / 1e8
            rvt_str  = f"{item['rvt']:.2f}" if item["rvt"] is not None else "N/A"
            logger.info(
                f"  {i}위 {item['name']}({item['ticker']}) "
                f"| 거래대금 {pbmn_bil:.0f}억 "
                f"| RVT {rvt_str} "
                f"| 상대강도 {item['exec_strength']:.1f} "
                f"| A점수 {item['composite_score']:.3f} "
                f"| B점수 {item['rvt_score'] if item['rvt_score'] is not None else 'N/A'} "
                f"| 시가갭 {item['gap_pct']:+.1f}%"
            )

        # ── 4. target_stocks 상위 10개에 거래대금 필드 저장 (merge) ──
        try:
            # 기존 target_stocks 전체 삭제
            existing = self._fs.collection("target_stocks").stream()
            del_batch = self._fs.batch()
            del_count = 0
            for doc in existing:
                del_batch.delete(doc.reference)
                del_count += 1
                if del_count % 400 == 0:
                    del_batch.commit()
                    del_batch = self._fs.batch()
            del_batch.commit()
            logger.info(f"[거래대금필터] 기존 target_stocks {del_count}개 삭제")

            # 신규 저장
            batch = self._fs.batch()
            for i, item in enumerate(selected, 1):
                ref = self._fs.collection("target_stocks").document(item["ticker"])
                batch.set(ref, {
                    "ticker":            item["ticker"],
                    "name":              item["name"],
                    "exchange":          item.get("exchange", "KRX"),
                    "financial_grade":   item.get("financial_grade", ""),
                    "confidence":        item.get("confidence", 0),
                    "passed_strategies": item.get("passed_strategies", []),
                    "tr_pbmn":           item["tr_pbmn"],
                    "tr_pbmn_rank":      i,
                    "tr_pbmn_at":        now_hm,
                    "tr_pbmn_date":      today_str,
                    "gap_pct":           item["gap_pct"],
                    "current_pct":       item["current_pct"],
                    "tday_rltv":         item.get("exec_strength", 100.0),
                    "composite_score":   item.get("composite_score", 0.0),
                    "score":             item["score"],
                    # ── RVT 백테스트 필드 (운영 정렬 무관, 항상 함께 저장) ──
                    "acml_vol":          item.get("acml_vol", 0),
                    "avg_volume_20d":    item.get("avg_volume_20d", 0),
                    "rvt":               item.get("rvt"),  # None 허용
                    "rvt_score":         item.get("rvt_score"),
                    "a_rank":            item.get("a_rank", i),
                    "b_rank":            item.get("b_rank", i),
                    "sort_mode":         "B" if USE_RVT_SCORING else "A",
                })
            batch.commit()
            logger.info(f"[거래대금필터] target_stocks {len(selected)}개 저장 완료")

            # meta/golden_pick_status 갱신 — Flutter UPDATE 배지용
            self._fs.collection("meta").document("golden_pick_status").set({
                "updated_at": now.isoformat(),
                "date":       today_str,
                "count":      len(selected),
            })
            logger.info("[거래대금필터] meta/golden_pick_status 갱신 완료")
        except Exception as e:
            logger.error(f"[거래대금필터] Firestore 저장 실패: {e}")
        
        # ── 5. watch_symbols 교체 + symbol_meta 갱신 ────────────────────────
        new_symbols = [item["ticker"] for item in selected]

        # symbol_meta 갱신 (종목명 소실 방지)
        for item in selected:
            self._symbol_meta[item["ticker"]] = {
                "name":     item["name"],
                "strategy": item.get("passed_strategies", [""])[0] if item.get("passed_strategies") else "",
                "score":    item["score"],
            }

        # 보유 포지션 강제 편입
        with self._positions_lock:
            holding_symbols = list(self._positions.keys())
        for s in holding_symbols:
            if s not in new_symbols:
                new_symbols.append(s)

        prev_symbols      = self._watch_symbols[:]
        self._watch_symbols = new_symbols

        if self._collector:
            self._collector.update_symbols(list(self._watch_symbols))

        added   = [s for s in new_symbols if s not in prev_symbols]
        removed = [s for s in prev_symbols if s not in new_symbols]

        # 제거된 종목 캐시 정리
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

        self._trade_filter_done = True

        # 분봉 수집 초기화 (pool 전체 저장 대상, 백필은 백그라운드)
        try:
            self._minute_store.init_from_selection(
                pool=pool,
                selected=selected,
                gate_state={
                    "gate":         self._market_gate,
                    "confidence":   self._market_gate_confidence,
                    "policy":       self._market_gate_policy,
                    "scan_hm":      self._scan_hm,
                    "market_top_n": self._market_top_n,
                },
                market_analysis=self._market_analysis_snapshot,
                ws_collector=self._collector,
            )
        except Exception as e:
            logger.error(f"[MinuteStore] init_from_selection 실패: {e}")

        logger.info(
            f"[거래대금필터] watch_symbols 교체 완료: "
            f"{len(new_symbols)}개 (추가={added}, 제거={removed})"
        )
        send_telegram(f"✅ [거래대금필터] 신규종목 {added} 편입 완료")

        # ── 6. 전일 데이터 워밍업 ────────────────────────
        self._warmup_market_data()

        # ── 7. 텔레그램 알림 ─────────────────────────────
        sort_tag = "B안(RVT)" if USE_RVT_SCORING else "A안(거래대금)"
        lines = [f"💹 <b>종목 선정 완료 — {sort_tag}</b> ({now_hm})"]
        for i, item in enumerate(selected, 1):
            pbmn_bil = item["tr_pbmn"] / 1e8
            rvt_str  = f"{item['rvt']:.2f}" if item.get("rvt") is not None else "N/A"
            lines.append(
                f"{i}위 {item['name']}({item['ticker']}) "
                f"| {pbmn_bil:.0f}억 "
                f"| RVT {rvt_str} "
                f"| 상대강도 {item.get('exec_strength', 0):.0f} "
                f"| 시가갭 {item['gap_pct']:+.1f}%"
            )
        send_telegram("\n".join(lines))

    def _flush_market_data_once(self, date_str: str):
        if self._last_flush_date == date_str:
            return
        self._flush_candle_csv(date_str)
        self._last_flush_date = date_str

        # 분봉 parquet 저장 + 품질 검증
        try:
            self._minute_store.finalize_day(date_str)
            self._minute_store.validate_day(date_str)
        except Exception as e:
            logger.error(f"[MinuteStore] 마감 처리 실패: {e}")

        total = self._daily_wins + self._daily_losses
        win_rate = round(self._daily_wins / total * 100, 1) if total else 0.0
        with self._positions_lock:
            carryover = list(self._positions.keys())
        carryover_str = f"{len(carryover)}개: {carryover}" if carryover else "없음"
        logger.info(
            f"[일마감요약] {date_str} | "
            f"매도={total}건 | 승={self._daily_wins} 패={self._daily_losses} 승률={win_rate}% | "
            f"실현손익={self._daily_realized_pnl:+,}원 | 이월포지션={carryover_str}"
        )

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

        # ── RSI2_REVERSAL 전용 시가 진입 (ws_1min 불필요, REST stck_oprc 사용) ──────
        if self._symbol_meta.get(symbol, {}).get("strategy") == "RSI2_REVERSAL":
            with self._positions_lock:
                _rsi2_holding = bool(self._positions.get(symbol))
            if not _rsi2_holding:
                hm_now = now.hour * 100 + now.minute
                if symbol in self._rsi2_entry_attempted:
                    return  # 이미 시도(진입 성공 or 포기) → BUY 재시도 없음
                if 900 <= hm_now <= 904:
                    self._execute_rsi2_open_entry(symbol, now)
                    return  # 시도 완료 or 시가 대기 → 이번 틱 종료
                if hm_now >= 905:
                    logger.info(f"[RSI2진입포기][{display}] 09:05 초과 시가 미확인 → 진입 포기")
                    self._rsi2_entry_attempted.add(symbol)
                    from trader.telegram import send_telegram
                    send_telegram(f"⏰ [RSI2진입포기] {display} 09:05 초과 시가 미확인 → 당일 진입 포기")
                return  # 미보유 RSI2: 일반 BUY 경로 차단
        # ── END RSI2 early entry ────────────────────────────────────────────────

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
        # RSI2: target_stocks stop_loss 필드 사용 (절대가, 원화 단위 — 배치 저장 단위와 일치 필요)
        if self._symbol_meta.get(symbol, {}).get("strategy") == "RSI2_REVERSAL":
            daily_stop_loss = float(self._symbol_meta.get(symbol, {}).get("stop_loss", 0.0))
        else:
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

        # ── 의미 있는 차단 사유 INFO 출력 ─────────────────
        _BLOCK_KEYWORDS = ("차단", "하회", "과열", "부족", "이격", "마감시간", "미달", "이탈")
        if signal != "BUY" and not is_holding and reason and any(k in reason for k in _BLOCK_KEYWORDS):
            logger.info(f"[HOLD-차단][{display}] {reason} | price={current_price:,}")

        if signal == "BUY" and not is_holding:
            dist_ema5 = round((current_price / ema5_curr - 1) * 100, 1) if ema5_curr else 0
            prev_vols = [r[5] for r in today_5m[-21:-1]] if len(today_5m) > 1 else []
            avg_vol_5m = sum(prev_vols) / len(prev_vols) if prev_vols else 0
            vol_ratio_log = round(today_5m[-1][5] / avg_vol_5m, 1) if avg_vol_5m > 0 and today_5m else 0
            logger.info(
                f"[BUY-신호][{display}] reason={reason}"
                f" | price={current_price:,} | dist_ema5={dist_ema5:+.1f}%"
                f" | vol≈{vol_ratio_log:.1f}x | energy={energy.get('score', 0)}"
            )

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
            max_profit = round((max_price / entry_price - 1) * 100, 2) if entry_price else 0
            cur_profit = round((current_price / entry_price - 1) * 100, 2) if entry_price else 0
            logger.info(
                f"[SELL-신호][{display}] reason={reason}"
                f" | price={current_price:,} | cur={cur_profit:+.2f}% | peak={max_profit:+.2f}%"
            )
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

    # ── 당일 거래 요약 갱신 ────────────────────────────────
    def _update_daily_summary(self, symbol: str, name: str, price: int,
                               qty: int, profit_pct: float, entry_price: float):
        """
        매도 체결 시마다 daily_trade_summary/{date} 문서를 누적 갱신합니다.
        Firestore 트랜잭션으로 race condition을 방지합니다.
        """
        try:
            today = datetime.now(KST).strftime("%Y-%m-%d")
            ref = self._fs.collection("daily_trade_summary").document(today)

            if not entry_price:
                logger.warning(f"[daily_summary] {symbol} entry_price 없음 → pnl 계산 스킵")
                return
            pnl = int((price - entry_price) * qty)
            is_win = profit_pct > 0

            @firestore.transactional
            def _txn(transaction, ref):
                snapshot = ref.get(transaction=transaction)
                if snapshot.exists:
                    d = snapshot.to_dict() or {}
                else:
                    d = {
                        "date": today,
                        "realized_pnl": 0,
                        "trade_count": 0,
                        "win_count": 0,
                        "loss_count": 0,
                        "win_rate": 0.0,
                        "updated_at": "",
                    }

                d["realized_pnl"] = d.get("realized_pnl", 0) + pnl
                d["trade_count"]  = d.get("trade_count",  0) + 1
                d["win_count"]    = d.get("win_count",    0) + (1 if is_win else 0)
                d["loss_count"]   = d.get("loss_count",   0) + (0 if is_win else 1)

                total = d["win_count"] + d["loss_count"]
                d["win_rate"] = round(d["win_count"] / total * 100, 1) if total else 0.0
                d["updated_at"] = datetime.now(KST).isoformat()

                # unrealized_pnl은 heartbeat에서 merge=True로만 관리 → 트랜잭션에서 제외
                d.pop("unrealized_pnl", None)
                transaction.set(ref, d, merge=True)

            txn = self._fs.transaction()
            _txn(txn, ref)

            logger.info(
                f"[daily_summary] {name}({symbol}) 매도 반영 | "
                f"pnl={pnl:+,}원 profit={profit_pct:+.2f}% | "
                f"today={today}"
            )

        except Exception as e:
            logger.error(f"[daily_summary] 갱신 실패: {e}")

    # ── RSI2 전용 시가 진입 ────────────────────────────
    def _execute_rsi2_open_entry(self, symbol: str, now: datetime):
        """RSI2 역전 모드 전용 시가 진입: REST stck_oprc 확인 후 시가+0.5% 지정가.

        현재 페이퍼 트레이딩 전제: buy_order()가 즉시 체결 → 미체결 상태 없음.

        # TODO(실계좌 전환 시 필수): 시가 지정가 미체결 처리 구현
        #   옵션 A — 주문번호 추적 후 09:05에 미체결 잔량 취소 API 호출
        #   옵션 B — ORD_DVSN을 IOC 지정가로 변경 (KIS API 코드값 문서 확인 필요)
        #   현재 09:05 분기는 '시가 미확인' 종목 전용이며 미체결 주문을 취소하지 않음.
        """
        display = self._display_name(symbol)

        # NXT 종목 대응: exchange → FID_COND_MRKT_DIV_CODE 매핑
        # domestic_stock_functions.py: J:KRX, NX:NXT, UN:통합
        # exchange 원본값이 로그에 출력되므로 첫 가동일에 불일치 즉시 확인 가능
        exchange = self._symbol_meta.get(symbol, {}).get("exchange", "KRX")
        mrkt_div = "NX" if exchange == "NXT" else "J"
        logger.info(f"[RSI2시가조회][{display}] exchange={exchange!r} → mrkt_div={mrkt_div!r}")

        try:
            res = _url_fetch(
                "/uapi/domestic-stock/v1/quotations/inquire-price",
                "FHKST01010100", "",
                {"FID_COND_MRKT_DIV_CODE": mrkt_div, "FID_INPUT_ISCD": symbol},
            )
        except Exception as e:
            logger.warning(f"[RSI2시가조회][{display}] API 예외({e}) → 다음 틱 재시도")
            return  # attempted 미등록 → 다음 틱 재시도

        if not res.isOK():
            logger.warning(f"[RSI2시가조회][{display}] API 실패(mrkt={mrkt_div}) → 다음 틱 재시도")
            return

        output = res.getBody().output
        stck_oprc = int(output.get("stck_oprc", 0) or 0)
        stck_sdpr = int(output.get("stck_sdpr", 0) or 0)  # 기준가(전일종가)

        if stck_oprc == 0:
            logger.debug(f"[RSI2시가조회][{display}] stck_oprc=0 → 시가 미확인, 다음 틱 재시도")
            return  # attempted 미등록

        # guard: 금요일 13:30 이후 (진입창 09:00~09:05에서 실질 미동작, 완결성 유지)
        if now.weekday() == 4 and (now.hour * 100 + now.minute) >= 1330:
            logger.info(f"[RSI2진입차단][{display}] 금요일오후 → 포기")
            self._rsi2_entry_attempted.add(symbol)
            return

        # guard: 시가 상한가 근접 (시가 ≥ 기준가×1.29)
        if stck_sdpr > 0 and stck_oprc >= int(stck_sdpr * 1.29):
            logger.info(
                f"[RSI2진입차단][{display}] 시가상한가근접"
                f" ({stck_oprc:,}/{stck_sdpr:,}) → 포기"
            )
            self._rsi2_entry_attempted.add(symbol)
            return

        limit_price = int(stck_oprc * 1.005)
        logger.info(
            f"[RSI2시가진입][{display}] stck_oprc={stck_oprc:,}"
            f" limit={limit_price:,} stck_sdpr={stck_sdpr:,} mrkt={mrkt_div}"
        )

        bar_time = now.strftime("%Y%m%d_0900")  # 당일 09:00 고정 키 (중복주문 방지)
        if self._can_place_order(symbol, "BUY", bar_time):
            executed = False
            try:
                executed = self._execute_buy(symbol, limit_price, "RSI2_OPEN", {"score": 0})
            finally:
                self._finalize_order(symbol, "BUY", bar_time, executed)

        self._rsi2_entry_attempted.add(symbol)  # 성공/실패 무관하게 당일 1회 완료

    # ── 매수/매도 실행 ─────────────────────────────────
    def _execute_buy(self, symbol: str, price: int, reason: str, energy: dict) -> bool:
        with self._positions_lock:
            pos_name = self._positions.get(symbol, {}).get("name", "")
        raw_name = pos_name or self._symbol_meta.get(symbol, {}).get("name", "")
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
            pos = dict(self._positions.get(symbol, {}))

        pos_name = pos.get("name", "")
        meta_name = self._symbol_meta.get(symbol, {}).get("name", "")

        logger.info(
            f"[SELL-NAME] symbol={symbol} | "
            f"pos_name={pos_name or '-'} | "
            f"meta_name={meta_name or '-'}"
        )

        raw_name = pos_name or meta_name
        raw_name = _normalize_name(raw_name, symbol)

        if not raw_name:
            logger.warning(f"[SELL-NAME-MISSING] symbol={symbol} | reason={reason}")

        display = f"{raw_name}({symbol})" if raw_name else symbol

        today_str = datetime.now(KST).strftime("%Y%m%d")
        entry_time = pos.get("entry_time", "")
        is_carryover = bool(entry_time) and entry_time[:8] != today_str
        carry_tag = "[이월]" if is_carryover else ""

        qty = pos.get("qty", 1)
        realized_pnl = int((price - entry_price) * qty) if entry_price else 0
        profit_pct = (price / entry_price - 1) * 100 if entry_price else 0
        logger.info(
            f"[SELL]{carry_tag}[{symbol}] {display} "
            f"price={price:,} qty={qty} "
            f"realized_pnl={realized_pnl:+,}원 profit={profit_pct:+.2f}% reason={reason}"
        )

        result = kis_api.sell_order(symbol, qty, price)
        if result is not None:
            self._daily_realized_pnl += realized_pnl
            if profit_pct > 0:
                self._daily_wins += 1
            else:
                self._daily_losses += 1
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
            self._update_daily_summary(
                symbol, raw_name, price,
                qty, profit_pct, entry_price,
            )
            return True

        return False

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
        # 로드된 snapshot에서 전일 날짜 추출
        with self._snapshot_lock:
            prev_date = next(
                (v.get("date", "") for v in self._prev_snapshot_cache.values() if v and v.get("date")),
                ""
            )
        prev_date_str = f"{prev_date[:4]}-{prev_date[4:6]}-{prev_date[6:]}" if prev_date else "확인 불가"
        lines = [f"✅ 전일({prev_date_str}) 1분봉 데이터 로드 완료", f"성공:{ok} / 실패:{fail}", f"신호 판별 준비가 끝났습니다.", ""]
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
