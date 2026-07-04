"""
minute_store.py — 분봉 데이터 인프라 (저장·검증 전용)
상태 머신: INIT → REST_BACKFILL → WS_RUNNING
          → (RECONNECT → REST_RECOVERY → WS_RUNNING)*
          → FINALIZE → VALIDATE
"""
from __future__ import annotations

import enum
import hashlib
import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger   = logging.getLogger(__name__)
KST      = timezone(timedelta(hours=9))

_DATA_DIR  = Path(os.environ.get("DATA_DIR", "DATA"))
MINUTE_DIR = _DATA_DIR / "minute"

SCHEMA_VERSION    = "1"
ENGINE_VERSION    = "1"
SELECTION_VERSION = "1"
COLLECTOR_VERSION = "1"


# ── 상태 열거형 ────────────────────────────────────────────────
class CollectorState(enum.Enum):
    INIT          = "INIT"
    REST_BACKFILL = "REST_BACKFILL"
    WS_RUNNING    = "WS_RUNNING"
    RECONNECT     = "RECONNECT"
    REST_RECOVERY = "REST_RECOVERY"
    FINALIZE      = "FINALIZE"
    VALIDATE      = "VALIDATE"


# ── 분봉 레코드 ───────────────────────────────────────────────
@dataclass
class BarRecord:
    ts:          str   # "YYYYMMDD_HHMM"
    open:        int
    high:        int
    low:         int
    close:       int
    volume:      int   # bar 내 체결량
    acml_volume: int   # 당일 누적 거래량
                       # WS: ACML_VOL 필드 / REST: running-sum 근사
                       # (2026-07-07 FHKST03010230 실측 후 개선 예정)
    incomplete:  bool = False  # reconnect 공백 bar
    source:      str  = "WS"  # "WS" | "REST"


# ── 이벤트 로그 ───────────────────────────────────────────────
class EventWriter:
    """stdout(logger.info) + events.jsonl 동시 기록."""

    def __init__(self, date_str: str):
        self._date_str = date_str
        self._lock = threading.Lock()
        self._file: Optional[Any] = None

    def _ensure_open(self):
        if self._file is not None:
            return
        path = MINUTE_DIR / self._date_str / "events.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        self._file = path.open("a", encoding="utf-8")

    def write(self, code: str, event: str, detail: dict):
        entry = {
            "ts":     datetime.now(KST).isoformat(),
            "code":   code,
            "event":  event,
            "detail": detail,
        }
        logger.info(f"[MinuteStore][{code}] {event} {detail}")
        with self._lock:
            self._ensure_open()
            self._file.write(json.dumps(entry, ensure_ascii=False) + "\n")
            self._file.flush()

    def close(self):
        with self._lock:
            if self._file:
                self._file.close()
                self._file = None


# ── 원자 저장 헬퍼 ────────────────────────────────────────────
def _write_parquet_atomic(symbol: str, date_str: str,
                          bars: Dict[str, BarRecord]) -> Path:
    import pandas as pd

    dir_path = MINUTE_DIR / date_str
    dir_path.mkdir(parents=True, exist_ok=True)
    final_path = dir_path / f"{symbol}.parquet"

    rows = [
        [b.ts, b.open, b.high, b.low, b.close, b.volume, b.acml_volume]
        for b in sorted(bars.values(), key=lambda x: x.ts)
        if not b.incomplete
    ]
    df = pd.DataFrame(
        rows, columns=["ts", "open", "high", "low", "close", "volume", "acml_volume"]
    )
    tmp_path = final_path.with_suffix(".tmp")
    df.to_parquet(tmp_path, index=False)
    os.replace(tmp_path, final_path)
    return final_path


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _write_json_atomic(path: Path, data: dict):
    tmp = path.with_suffix(".tmp")
    tmp.parent.mkdir(parents=True, exist_ok=True)
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


# ── 품질 검증 ─────────────────────────────────────────────────
def _validate_bars(symbol: str, date_str: str,
                   bars: Dict[str, BarRecord]) -> dict:
    from datetime import datetime as _dt, timedelta as _td

    ts_list     = sorted(bars.keys())
    hard_errors: list = []
    soft_warns:  list = []
    prev_close  = None

    for ts in ts_list:
        b = bars[ts]
        # hard: OHLC 관계 위반
        if b.high < b.low:
            hard_errors.append({"ts": ts, "err": "high_lt_low"})
        if not (b.low <= b.open <= b.high):
            hard_errors.append({"ts": ts, "err": "open_out_of_range"})
        if not (b.low <= b.close <= b.high):
            hard_errors.append({"ts": ts, "err": "close_out_of_range"})
        if b.close <= 0 or b.open <= 0:
            hard_errors.append({"ts": ts, "err": "price_nonpositive"})
        if b.volume < 0:
            hard_errors.append({"ts": ts, "err": "negative_volume"})
        # soft: 직전 종가 대비 시가 ±10% 초과 (분봉 점프는 정상이므로 hard 아님)
        if prev_close and prev_close > 0:
            jump = abs(b.open - prev_close) / prev_close
            if jump > 0.10:
                soft_warns.append({"ts": ts, "warn": "gap_jump_10pct",
                                   "pct": round(jump * 100, 2)})
        prev_close = b.close

    # 결측 ts (09:00~15:30)
    missing = []
    cur = _dt.strptime(f"{date_str}_0900", "%Y%m%d_%H%M")
    end = _dt.strptime(f"{date_str}_1530", "%Y%m%d_%H%M")
    ts_set = set(ts_list)
    while cur <= end:
        key = cur.strftime("%Y%m%d_%H%M")
        if key not in ts_set:
            missing.append(key)
        cur += _td(minutes=1)

    return {
        "symbol":        symbol,
        "date":          date_str,
        "bar_count":     len(ts_list),
        "first_ts":      ts_list[0]  if ts_list else None,
        "last_ts":       ts_list[-1] if ts_list else None,
        "missing_count": len(missing),
        "missing_ts":    missing[:20],
        "hard_errors":   hard_errors,
        "soft_warns":    soft_warns[:10],
    }


# ── REST 백필 ─────────────────────────────────────────────────
def _fetch_today_1min_bars(symbol: str, date_str: str) -> List[BarRecord]:
    """
    FHKST03010230 (inquire-time-dailychartprice) 당일 버전.
    필드명은 get_prev_day_1min_candles() 에서 이미 검증된 값 재사용.
    acml_volume: running-sum 근사. 2026-07-07 실측 후 개선 예정.
    """
    from trader.kis_auth import _url_fetch

    now_hm = int(datetime.now(KST).strftime("%H%M"))
    if now_hm < 900:
        return []

    times    = ["153000", "133000", "113000", "093000"]
    all_rows: Dict[tuple, dict] = {}

    for t in times:
        res = _url_fetch(
            "/uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice",
            "FHKST03010230", "",
            {
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD":         symbol,
                "FID_INPUT_DATE_1":       date_str,
                "FID_INPUT_HOUR_1":       t,
                "FID_PW_DATA_INCU_YN":   "Y",
                "FID_FAKE_TICK_INCU_YN": " ",
            },
        )
        if not res.isOK():
            logger.warning(
                f"[MinuteStore] {symbol} REST 백필 실패 t={t} "
                f"code={res.getErrorCode()} msg={res.getErrorMessage()}"
            )
            continue
        raw = getattr(res.getBody(), "output2", []) or []
        for r in raw:
            d      = (r.get("stck_bsop_date", "") or "").strip()
            raw_t  = (r.get("stck_cntg_hour", "") or "").strip()
            hhmmss = "".join(c for c in raw_t if c.isdigit()).zfill(6)[:6]
            if d != date_str or not hhmmss:
                continue
            if hhmmss < "090000" or hhmmss > "153000":
                continue
            all_rows[(d, hhmmss)] = r

    out: List[BarRecord] = []
    running_acml = 0
    for (d, hhmmss) in sorted(all_rows.keys()):
        r = all_rows[(d, hhmmss)]
        o = int(r.get("stck_oprc", 0) or 0)
        h = int(r.get("stck_hgpr", 0) or 0)
        l = int(r.get("stck_lwpr", 0) or 0)
        c = int(r.get("stck_prpr", 0) or 0)
        v = int(r.get("cntg_vol",  0) or 0)
        if c == 0:
            continue
        running_acml += v
        out.append(BarRecord(
            ts=f"{d}_{hhmmss[:4]}",
            open=o, high=h, low=l, close=c,
            volume=v, acml_volume=running_acml,
            source="REST",
        ))
    return out


# ── 단일 종목 수집기 ──────────────────────────────────────────
class SymbolCollector:
    def __init__(self, symbol: str, date_str: str, ew: EventWriter):
        self.symbol   = symbol
        self.date_str = date_str
        self._state   = CollectorState.INIT
        self._bars: Dict[str, BarRecord] = {}
        self._lock    = threading.Lock()
        self._ew      = ew

    @property
    def state(self) -> CollectorState:
        return self._state

    def _transition(self, new: CollectorState, detail: str = ""):
        self._ew.write(self.symbol, "STATE_TRANSITION",
                       {"from": self._state.value, "to": new.value, "detail": detail})
        self._state = new

    def seed_from_ws_buffer(self, ws_1min: list):
        """WsTickCollector 기존 1min 버퍼로 초기 seed (INIT 상태에서 호출)."""
        if self._state != CollectorState.INIT:
            return
        with self._lock:
            for row in ws_1min:
                ts = row[0]
                self._bars[ts] = BarRecord(
                    ts=ts,
                    open=int(row[1]), high=int(row[2]),
                    low=int(row[3]),  close=int(row[4]),
                    volume=int(row[5]), acml_volume=0,
                    incomplete=False, source="WS",
                )
        self._ew.write(self.symbol, "SEED_WS_BUFFER", {"seeded": len(ws_1min)})

    def start_backfill(self, rest_bars: List[BarRecord]):
        """
        REST 백필 적용.
        - INIT 진입 시: rule(a) WS bar는 건드리지 않고 gap만 채움
        - REST_RECOVERY 진입 시: rule(b) incomplete bar만 overwrite
        """
        is_recovery = (self._state == CollectorState.REST_RECOVERY)
        if self._state == CollectorState.INIT:
            self._transition(CollectorState.REST_BACKFILL,
                             f"rest_bars={len(rest_bars)}")
        elif not is_recovery:
            logger.warning(f"[{self.symbol}] start_backfill: 잘못된 상태 {self._state}")
            return

        gap_filled = 0
        recovered  = 0
        with self._lock:
            for b in rest_bars:
                if is_recovery:
                    existing = self._bars.get(b.ts)
                    if existing and not existing.incomplete:
                        continue  # 정상 WS bar 보존
                    b.incomplete = False
                    self._bars[b.ts] = b
                    recovered += 1
                else:
                    if b.ts in self._bars and self._bars[b.ts].source == "WS":
                        continue  # rule(a): WS 우선
                    self._bars[b.ts] = b
                    gap_filled += 1

        if is_recovery:
            self._transition(CollectorState.WS_RUNNING,
                             f"recovered={recovered}")
            self._ew.write(self.symbol, "RECOVERY_DONE", {"recovered": recovered})
        else:
            self._transition(CollectorState.WS_RUNNING,
                             f"gap_filled={gap_filled}")
            self._ew.write(self.symbol, "BACKFILL_DONE",
                           {"rest_bars": len(rest_bars), "gap_filled": gap_filled})

    def on_tick(self, hhmm: str, price: int, vol: int, acml_vol: int):
        """WS 틱 수신. REST_BACKFILL·WS_RUNNING 모두 허용 — rule(a) WS 항상 우선."""
        if self._state not in (CollectorState.REST_BACKFILL, CollectorState.WS_RUNNING):
            return
        ts = f"{self.date_str}_{hhmm}"
        with self._lock:
            b = self._bars.get(ts)
            if b is None:
                self._bars[ts] = BarRecord(
                    ts=ts, open=price, high=price, low=price, close=price,
                    volume=vol, acml_volume=acml_vol,
                    incomplete=False, source="WS",
                )
            else:
                b.high        = max(b.high, price)
                b.low         = min(b.low, price)
                b.close       = price
                b.volume     += vol
                b.acml_volume = acml_vol
                b.incomplete  = False
                b.source      = "WS"

    def on_disconnect(self, hhmm: str):
        if self._state != CollectorState.WS_RUNNING:
            return
        reconnect_key = f"{self.date_str}_{hhmm}"
        with self._lock:
            for ts, b in self._bars.items():
                if ts >= reconnect_key:
                    b.incomplete = True
        self._transition(CollectorState.RECONNECT, f"at={hhmm}")

    def on_reconnect(self, hhmm: str):
        if self._state != CollectorState.RECONNECT:
            return
        self._transition(CollectorState.REST_RECOVERY, f"at={hhmm}")

    def finalize(self) -> Path:
        if self._state not in (CollectorState.WS_RUNNING, CollectorState.RECONNECT,
                               CollectorState.REST_RECOVERY, CollectorState.REST_BACKFILL):
            logger.warning(f"[{self.symbol}] finalize: 비정상 상태={self._state}")
        self._transition(CollectorState.FINALIZE, "")
        path = _write_parquet_atomic(self.symbol, self.date_str, self._bars)
        self._ew.write(self.symbol, "FINALIZE",
                       {"bars": len(self._bars), "path": str(path)})
        return path

    def validate(self) -> dict:
        self._transition(CollectorState.VALIDATE, "")
        result = _validate_bars(self.symbol, self.date_str, self._bars)
        self._ew.write(self.symbol, "VALIDATE", {
            "bar_count":   result["bar_count"],
            "missing":     result["missing_count"],
            "hard_errors": len(result["hard_errors"]),
        })
        return result


# ── 매니저 ────────────────────────────────────────────────────
class MinuteStoreManager:
    def __init__(self):
        self._collectors: Dict[str, SymbolCollector] = {}
        self._ew:          Optional[EventWriter]     = None
        self._date_str:    str                       = ""
        self._lock         = threading.Lock()
        # finalize 시 manifest 구성용 스냅샷
        self._pool_snap:   list = []
        self._sel_snap:    list = []
        self._gate_snap:   dict = {}
        self._mkt_snap:    dict = {}

    def init_from_selection(
        self,
        pool:            list,   # 300억 floor pool 전체 (engine item dict)
        selected:        list,   # TOP_N 선정 종목 item dict
        gate_state:      dict,   # {gate, confidence, policy, scan_hm, market_top_n}
        market_analysis: dict,   # Firestore market_analysis/latest 스냅샷
        ws_collector,            # WsTickCollector (seed용)
    ):
        now      = datetime.now(KST)
        date_str = now.strftime("%Y%m%d")

        with self._lock:
            if self._date_str == date_str and self._collectors:
                logger.info("[MinuteStore] 당일 이미 초기화 → 스킵")
                return
            if self._ew:
                self._ew.close()
            self._date_str = date_str
            self._ew       = EventWriter(date_str)
            self._collectors.clear()
            self._pool_snap = pool
            self._sel_snap  = selected
            self._gate_snap = gate_state
            self._mkt_snap  = market_analysis

        all_symbols = list({item["ticker"] for item in pool})
        self._ew.write("MANAGER", "INIT_FROM_SELECTION", {
            "pool":    len(all_symbols),
            "top_n":   len(selected),
            "gate":    gate_state.get("gate"),
            "scan_hm": gate_state.get("scan_hm"),
        })

        # Firestore 메타 아카이빙
        _archive_selection_meta(date_str, pool, selected, gate_state, market_analysis)

        # 종목 수집기 생성 + WS 버퍼 seed
        to_backfill = []
        with self._lock:
            for sym in all_symbols:
                col = SymbolCollector(sym, date_str, self._ew)
                ws_1min = ws_collector.get_1min(sym)
                if ws_1min:
                    col.seed_from_ws_buffer(ws_1min)
                self._collectors[sym] = col
                to_backfill.append(col)

        def _do_backfill():
            for col in to_backfill:
                try:
                    bars = _fetch_today_1min_bars(col.symbol, date_str)
                    col.start_backfill(bars)
                except Exception as e:
                    logger.error(f"[MinuteStore] {col.symbol} 백필 오류: {e}")
                    if col.state == CollectorState.INIT:
                        col._transition(CollectorState.WS_RUNNING, "backfill_error")
                time.sleep(0.25)

        threading.Thread(target=_do_backfill, daemon=True,
                         name="MinuteStore-Backfill").start()

    def on_tick(self, symbol: str, hhmm: str,
                price: int, vol: int, acml_vol: int):
        col = self._collectors.get(symbol)
        if col:
            col.on_tick(hhmm, price, vol, acml_vol)

    def on_disconnect(self, hhmm: str):
        with self._lock:
            cols = list(self._collectors.values())
        for col in cols:
            col.on_disconnect(hhmm)

    def on_reconnect(self, hhmm: str, ws_collector):
        with self._lock:
            cols     = list(self._collectors.values())
            date_str = self._date_str
        for col in cols:
            col.on_reconnect(hhmm)

        def _do_recovery():
            for col in cols:
                if col.state != CollectorState.REST_RECOVERY:
                    continue
                try:
                    bars = _fetch_today_1min_bars(col.symbol, date_str)
                    col.start_backfill(bars)
                except Exception as e:
                    logger.error(f"[MinuteStore] {col.symbol} recovery 오류: {e}")
                    col._transition(CollectorState.WS_RUNNING, "recovery_error")
                time.sleep(0.25)

        threading.Thread(target=_do_recovery, daemon=True,
                         name="MinuteStore-Recovery").start()

    def finalize_day(self, date_str: str) -> dict:
        """장 마감 후 parquet + manifest 저장. engine._flush_market_data_once에서 호출."""
        with self._lock:
            cols = list(self._collectors.values())
            pool = self._pool_snap
            sel  = self._sel_snap
            gate = self._gate_snap
            mkt  = self._mkt_snap
            ew   = self._ew

        if not cols:
            logger.warning("[MinuteStore] finalize_day: 수집 종목 없음")
            return {}

        for col in cols:
            if col.state in (CollectorState.INIT, CollectorState.REST_BACKFILL):
                logger.warning(f"[MinuteStore] {col.symbol} 백필 미완료 상태로 finalize")

        files_meta = {}
        for col in cols:
            try:
                path = col.finalize()
                files_meta[col.symbol] = {
                    "path":      str(path),
                    "sha256":    _sha256_file(path),
                    "file_size": path.stat().st_size,
                }
            except Exception as e:
                logger.error(f"[MinuteStore] {col.symbol} finalize 실패: {e}")

        manifest = {
            "date":              date_str,
            "created_at":        datetime.now(KST).isoformat(),
            "schema_version":    SCHEMA_VERSION,
            "engine_version":    ENGINE_VERSION,
            "selection_version": SELECTION_VERSION,
            "collector_version": COLLECTOR_VERSION,
            "gate":              gate,
            "market_analysis":   mkt,
            "candidate_pool": [
                {k: v for k, v in item.items()
                 if k in ("ticker", "name", "tr_pbmn", "composite_score",
                           "rvt_score", "a_rank", "b_rank",
                           "exec_strength", "gap_pct")}
                for item in pool
            ],
            "selected": [item.get("ticker") for item in sel],
            "files":    files_meta,
        }
        try:
            _write_json_atomic(MINUTE_DIR / date_str / "manifest.json", manifest)
            if ew:
                ew.write("MANAGER", "MANIFEST_WRITTEN", {"files": len(files_meta)})
        except Exception as e:
            logger.error(f"[MinuteStore] manifest 저장 실패: {e}")

        return files_meta

    def validate_day(self, date_str: str) -> List[dict]:
        """품질 검증 — finalize 후 호출. daily_review.sh 에서도 별도 실행 가능."""
        with self._lock:
            cols = list(self._collectors.values())
        from trader.telegram import send_telegram

        results       = []
        hard_total    = 0
        missing_total = 0
        for col in cols:
            try:
                r = col.validate()
                results.append(r)
                hard_total    += len(r["hard_errors"])
                missing_total += r["missing_count"]
            except Exception as e:
                logger.error(f"[MinuteStore] {col.symbol} validate 실패: {e}")

        logger.info(
            f"[MinuteStore] validate_day 완료 | 종목={len(results)} | "
            f"hard오류={hard_total} | 결측봉={missing_total}"
        )
        if hard_total or missing_total > 50:
            send_telegram(
                f"⚠️ [MinuteStore 검증] {date_str}\n"
                f"hard오류={hard_total} | 결측봉={missing_total}"
            )
        return results

    def reset_day(self):
        """날짜 변경 시 초기화 (engine._rollover_if_needed에서 호출)."""
        with self._lock:
            if self._ew:
                self._ew.close()
                self._ew = None
            self._collectors.clear()
            self._date_str = ""
            self._pool_snap = []
            self._sel_snap  = []
            self._gate_snap = {}
            self._mkt_snap  = {}
        logger.info("[MinuteStore] 일자 변경 → 초기화")


# ── Firestore 메타 아카이빙 ───────────────────────────────────
def _archive_selection_meta(
    date_str:        str,
    pool:            list,
    selected:        list,
    gate_state:      dict,
    market_analysis: dict,
):
    """
    selection_archive/{date_str} + market_analysis/{date_str} 날짜키 아카이빙.
    덮어쓰기 금지: 이미 존재하면 스킵.
    """
    try:
        from google.cloud import firestore as _fs
        fs = _fs.Client()

        sel_ref = fs.collection("selection_archive").document(date_str)
        if not sel_ref.get().exists:
            sel_ref.set({
                "date":              date_str,
                "created_at":        datetime.now(KST).isoformat(),
                "schema_version":    SCHEMA_VERSION,
                "engine_version":    ENGINE_VERSION,
                "selection_version": SELECTION_VERSION,
                "collector_version": COLLECTOR_VERSION,
                "gate":            gate_state.get("gate", ""),
                "gate_confidence": gate_state.get("confidence", 0.0),
                "gate_policy":     gate_state.get("policy", ""),
                "scan_hm":         gate_state.get("scan_hm", 920),
                "market_top_n":    gate_state.get("market_top_n", 10),
                "candidate_pool":  [
                    {k: v for k, v in item.items()
                     if k in ("ticker", "name", "tr_pbmn", "composite_score",
                               "rvt_score", "a_rank", "b_rank",
                               "exec_strength", "gap_pct")}
                    for item in pool
                ],
                "selected": [item.get("ticker") for item in selected],
            })
            logger.info(f"[MinuteStore] selection_archive/{date_str} 저장 완료")
        else:
            logger.info(f"[MinuteStore] selection_archive/{date_str} 이미 존재 → 스킵")

        # market_analysis 날짜키 아카이빙
        if market_analysis:
            mkt_ref = fs.collection("market_analysis").document(date_str)
            if not mkt_ref.get().exists:
                mkt_ref.set(market_analysis)
                logger.info(f"[MinuteStore] market_analysis/{date_str} 저장 완료")

    except Exception as e:
        logger.error(f"[MinuteStore] Firestore 아카이빙 실패: {e}")
