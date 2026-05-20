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
        self._premarket_warmup_done_date = ""
        self._holiday_checked_date: str = ""
        self._today_opnd_yn: str = "Y"
        self._trade_filter_done: bool = False   # 당일 거래대금 필터 완료 여부
        self._market_open_notified: bool = False  # 09:00 보유종목 처리 알림 (하루 1회)

        # 마켓 게이트 (foreign_signal 기반 동적 스캔 타이밍)
        self._market_gate: str = "UNKNOWN"        # gate 값 (BULL/BEAR/NEUTRAL/UNKNOWN)
        self._market_top_n: int = 10              # 거래대금 필터 선정 종목 수
        self._scan_hm: int = 920                  # 거래대금 필터 실행 기준 시각 (hhmm)
        self._gate_loaded_date: str = ""          # Firestore 로드 완료 날짜 (하루 1회 가드)
        self._gate_block_pending: bool = False    # BLOCK 고신뢰 → 09:00 이후 코스닥 갭 확인 대기
        self._gate_retry_count: int = 0           # phase1 재시도 횟수 (최대 5회)

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

        # 전일 보유 종목은 거래대금 필터 전에도 09:00 즉시 매매 대응해야 하므로
        # 엔진 시작 시점부터 감시목록/WS 구독에 먼저 편입한다.
        with self._positions_lock:
            holding_symbols = list(self._positions.keys())

        self._watch_symbols = holding_symbols[:]
        self._collector = WsTickCollector(list(self._watch_symbols))
        self._collector.start()

        if holding_symbols:
            logger.info(f"[보유종목] 장시작 전 구독 준비: {holding_symbols}")

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

        # 08:50~08:59 전일 보유 종목 전일 1분봉/snapshot 선로딩
        self._maybe_premarket_warmup(now)

        if not (900 <= hm <= 1530):
            if hm >= 1531:
                self._flush_market_data_once(now.strftime("%Y%m%d"))
            return

        # ── 마켓 게이트 2단계 확정 (09:00~09:10, BLOCK 대기 중인 경우만) ──
        if self._gate_block_pending and 900 <= hm <= 910:
            self._load_market_gate_phase2()

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
            logger.warning(f"[거래대금필터] {_fallback_hm} 초과 미완료 → 즉시 실행")
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
        self._touched_limit_up.clear()
        self._stop_loss_pending.clear()
        self._trade_filter_done = False
        self._market_open_notified = False

        # 마켓 게이트 리셋 (다음날 새로 로드)
        self._market_gate = "UNKNOWN"
        self._market_top_n = 10
        self._scan_hm = 920
        self._gate_loaded_date = ""
        self._gate_block_pending = False
        self._gate_retry_count = 0

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

        # 2. 전일 보유 종목을 거래대금 필터와 무관하게 감시목록/WS에 강제 편입
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

        BLOCK + 고신뢰인 경우에는 코스닥 지수가 프리마켓에서 0을 반환할 수 있으므로
        즉시 판단하지 않고 _gate_block_pending=True 로 표시해 두고,
        09:00 이후 _load_market_gate_phase2() 에서 코스닥 갭을 확인 후 최종 확정한다.
        """
        if self._gate_loaded_date == today_str:
            return

        from trader.telegram import send_telegram

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

        self._gate_retry_count = 0  # 성공 시 리셋
        self._market_gate = gate

        # ── 2. 타이밍 + 종목 수 잠정 결정 ─────────────────
        if policy == "BLOCK" and confidence >= 0.8:
            # 코스닥 갭 확인 필요 → 2단계에서 최종 확정 (잠정: 최대 지연)
            self._scan_hm           = 930
            self._market_top_n      = 5
            self._gate_block_pending = True
            label = "⛔ BEAR(BLOCK/고신뢰) → 잠정 09:30·5종목 | 코스닥 갭 확인 대기"

        elif gate == "BULL" and confidence >= 0.8:
            self._scan_hm      = 910
            self._market_top_n = 10
            label = "🟢 BULL(고신뢰) → 09:10 조기 | 10종목"

        elif gate == "BULL":
            self._scan_hm      = 915
            self._market_top_n = 10
            label = "🟡 BULL(저신뢰) → 09:15 | 10종목"

        elif gate == "NEUTRAL":
            self._scan_hm      = 920
            self._market_top_n = 10
            label = "⚪ NEUTRAL → 09:20 기본 | 10종목"

        elif gate == "BEAR" and confidence >= 0.8:
            self._scan_hm      = 925
            self._market_top_n = 5
            label = "🔴 BEAR(고신뢰) → 09:25 지연 | 5종목"

        elif gate == "BEAR":
            self._scan_hm      = 920
            self._market_top_n = 7
            label = "🟠 BEAR(저신뢰) → 09:20 기본 | 7종목"

        else:
            self._scan_hm      = 920
            self._market_top_n = 10
            label = f"❓ {gate}(UNKNOWN) → 09:20 기본 | 10종목"

        self._gate_loaded_date = today_str

        logger.info(
            f"[마켓게이트][1단계] {label} | gate={gate} | policy={policy} | "
            f"confidence={confidence:.0%} | updated={updated_at[:10] if updated_at else '?'}"
        )
        # BLOCK은 phase2 확정 후 결과를 1회만 발송 (phase1 잠정 알림 생략)
        if not self._gate_block_pending:
            send_telegram(
                f"📊 [마켓게이트] {label}\n"
                f"gate={gate} | policy={policy} | 신뢰도={confidence:.0%}\n"
                f"기준일: {updated_at[:10] if updated_at else '?'}"
            )

    def _load_market_gate_phase2(self):
        """
        [2단계 — 09:00 이후 첫 tick]
        BLOCK + 고신뢰(_gate_block_pending=True) 인 경우에만 실행.
        코스닥 지수(1001) 현재 등락률을 조회해 +1.5% 이상이면 BLOCK을 해제한다.

        - BLOCK 해제 시: scan_hm=925, top_n=5 (BEAR 고신뢰 수준으로 완화)
        - BLOCK 유지 시: scan_hm=930, top_n=5 그대로
        """
        if not self._gate_block_pending:
            return

        from trader.telegram import send_telegram

        try:
            res = _url_fetch(
                "/uapi/domestic-stock/v1/quotations/inquire-index-price",
                "FHPUP02100000", "",
                {
                    "FID_COND_MRKT_DIV_CODE": "U",
                    "FID_INPUT_ISCD": "1001",   # 코스닥
                },
            )
            if not res.isOK():
                logger.warning("[마켓게이트][2단계] 코스닥 지수 조회 실패 → BLOCK 유지")
                self._gate_block_pending = False
                return

            output      = res.getBody().output
            kosdaq_prpr = float(output.get("bstp_nmix_prpr", 0) or 0)
            kosdaq_ctrt = float(output.get("bstp_nmix_prdy_ctrt", 0) or 0)  # 전일 대비 등락률

            if kosdaq_prpr == 0:
                logger.warning(
                    "[마켓게이트][2단계] 코스닥 지수 0 반환 (데이터 미준비) → 다음 틱 재시도"
                )
                # _gate_block_pending = True 유지 → 900~910 범위 내 다음 틱에서 재시도
                return

            if kosdaq_ctrt >= 1.5:
                # BLOCK 해제 → BEAR 고신뢰 수준으로 완화
                self._scan_hm      = 925
                self._market_top_n = 5
                label = (
                    f"✅ BLOCK 해제 — 코스닥 {kosdaq_ctrt:+.2f}% ≥ +1.5%\n"
                    f"→ 09:25 지연·5종목으로 완화"
                )
            else:
                label = (
                    f"⛔ BLOCK 유지 — 코스닥 {kosdaq_ctrt:+.2f}% < +1.5%\n"
                    f"→ 09:30 지연·5종목 유지"
                )

            self._gate_block_pending = False  # 2단계 완료

            logger.info(f"[마켓게이트][2단계] {label.replace(chr(10), ' | ')}")
            send_telegram(f"📊 [마켓게이트 확정] {label}")

        except Exception as e:
            logger.warning(f"[마켓게이트][2단계] 오류({e}) → BLOCK 유지")
            self._gate_block_pending = False

    def _filter_by_trade_amount(self):
        """
        strategy_results 전체 거래대금 + 갭 조건 조회 → 상위 TOP_N개로 watch_symbols 교체
        - scan_hm(마켓게이트 기반 동적 시각) 자동 실행 (하루 1회)
        - scan_hm+5분 이후 미완료 시 즉시 실행 (폴백)
        - TOP_N: gate=BULL/NEUTRAL → 10개, BEAR(저신뢰) → 7개, BEAR(고신뢰)/BLOCK → 5개
        - 엔진 재시작 시 tr_pbmn_date == today 이면 재조회 없이 복원
        """
        import time as _time
        from trader.telegram import send_telegram

        if self._gate_block_pending:
            logger.warning(
                "[마켓게이트] BLOCK pending 미해소 상태로 필터 실행 "
                f"(코스닥 갭 확인 실패) → scan_hm={self._scan_hm}, top_n={self._market_top_n}"
            )
            send_telegram(
                "⚠️ [마켓게이트] BLOCK pending 미해소\n코스닥 갭 확인 실패 → 09:30 기본 실행"
            )
            self._gate_block_pending = False

        TOP_N        = self._market_top_n  # gate 기반 동적 종목 수 (기본 10, BEAR 시 5~7)
        API_INTERVAL = 0.2   # 초
        GAP_MIN_PCT  = 1.0   # 시가갭 하한 (%)
        GAP_MAX_PCT  = 15.0  # 시가갭 상한 (%) — 과열 제외

        now       = datetime.now(KST)
        today_str = now.strftime("%Y%m%d")
        now_hm    = now.strftime("%H:%M")

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
                    continue

                item["tr_pbmn"]     = acml_tr_pbmn
                item["gap_pct"]     = gap_pct
                item["current_pct"] = current_pct
                item["price"]       = stck_prpr
                results.append(item)

            except Exception as e:
                fail_count += 1
                logger.warning(f"[거래대금필터] {ticker} 오류: {e} → 제외")

        logger.info(
            f"[거래대금필터] 조회 완료: {len(results)}개 통과 / "
            f"{skip_count}개 갭미달 / {fail_count}개 실패"
        )

        if not results:
            logger.error("[거래대금필터] 갭 조건 통과 종목 없음 → watch_symbols 유지")
            self._trade_filter_done = True
            return

        # ── 3. 거래대금 기준 정렬 → 상위 TOP_N 선정 ────
        results.sort(key=lambda x: x["tr_pbmn"], reverse=True)
        selected = results[:TOP_N]

        logger.info(f"[거래대금필터] 상위 {len(selected)}개 선정:")
        for i, item in enumerate(selected, 1):
            pbmn_bil = item["tr_pbmn"] / 1e8
            logger.info(
                f"  {i}위 {item['name']}({item['ticker']}) "
                f"| 거래대금 {pbmn_bil:.0f}억 "
                f"| 시가갭 {item['gap_pct']:+.1f}% "
                f"| 현재갭 {item['current_pct']:+.1f}%"
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
                    "score":             item["score"],
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
        logger.info(
            f"[거래대금필터] watch_symbols 교체 완료: "
            f"{len(new_symbols)}개 (추가={added}, 제거={removed})"
        )
        send_telegram(f"✅ [거래대금필터] 신규종목 {added} 편입 완료")

        # ── 6. 전일 데이터 워밍업 ────────────────────────
        self._warmup_market_data()

        # ── 7. 텔레그램 알림 ─────────────────────────────
        lines = [f"💹 <b>거래대금 기준 종목 선정 완료</b> ({now_hm})"]
        for i, item in enumerate(selected, 1):
            pbmn_bil = item["tr_pbmn"] / 1e8
            lines.append(
                f"{i}위 {item['name']}({item['ticker']}) "
                f"| {pbmn_bil:.0f}억 "
                f"| 시가갭 {item['gap_pct']:+.1f}%"
            )
        send_telegram("\n".join(lines))

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
        if signal not in ("BUY", "SELL") and reason:
            logger.info(f"[{display}] HOLD | {reason}")
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
