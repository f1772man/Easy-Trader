# -*- coding: utf-8 -*-
"""
kis_api.py
트레이딩 전용 API 레이어
- kis_auth.py의 _url_fetch() 를 통해 모든 REST 호출
- 직접 requests 사용 없음
"""
from __future__ import annotations
import logging
import os
from typing import Optional

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Tuple


from trader.kis_auth import (
    auth, _url_fetch,
    getTREnv, isPaperTrading,
)
@dataclass
class Candle:
    ts: datetime           # 5분 버킷 시작시각
    o: int
    h: int
    l: int
    c: int
    v: int

logger = logging.getLogger(__name__)


def ensure_auth(svr: str = None):
    svr = svr or os.environ.get("KIS_SVR", "vps")
    auth(svr=svr)


# ── 5분봉 ────────────────────────────────────────────
def _normalize_time_hhmmss(t: str) -> str:
    """
    stck_cntg_hour 방어:
    - 숫자만 남기고
    - 왼쪽 0패딩 후 HHMMSS 6자리로 자름
    """
    t = (t or "").strip()
    t = "".join(c for c in t if c.isdigit())
    t = t.zfill(6)
    return t[:6]
def _parse_dt_kst(date_yyyymmdd: str, time_hhmmss: str) -> datetime:
    date_yyyymmdd = (date_yyyymmdd or "").strip()
    hhmmss = _normalize_time_hhmmss(time_hhmmss)
    # naive datetime (KST로 해석할 용도)
    return datetime.strptime(date_yyyymmdd + hhmmss, "%Y%m%d%H%M%S")

def _floor_to_5min(dt: datetime) -> datetime:
    m = (dt.minute // 5) * 5
    return dt.replace(minute=m, second=0, microsecond=0)


def build_5min_candles_from_output2(output2: List[Dict[str, Any]]) -> List[List[Any]]:
    if not output2:
        return []

    parsed: List[Tuple[datetime, Dict[str, Any]]] = []
    for r in output2:
        dt = _parse_dt_kst(r.get("stck_bsop_date", ""), r.get("stck_cntg_hour", ""))
        parsed.append((dt, r))
    parsed.sort(key=lambda x: x[0])

    buckets: Dict[datetime, Candle] = {}
    for dt, r in parsed:
        bucket = _floor_to_5min(dt)

        price = int(r.get("stck_prpr", 0) or 0)  # 체결가
        v = int(r.get("cntg_vol", 0) or 0)

        if price == 0:
            continue  # 가격 없는 tick 스킵

        if bucket not in buckets:
            buckets[bucket] = Candle(ts=bucket, o=price, h=price, l=price, c=price, v=v)
        else:
            cd = buckets[bucket]
            cd.h = max(cd.h, price)
            cd.l = min(cd.l, price)
            cd.c = price
            cd.v += v

    out: List[List[Any]] = []
    for bucket_dt in sorted(buckets.keys()):
        cd = buckets[bucket_dt]
        out.append([cd.ts.strftime("%Y%m%d_%H%M"), cd.o, cd.h, cd.l, cd.c, cd.v])
    return out



def _hhmmss_from_kst(dt_kst: datetime) -> str:
    return dt_kst.strftime("%H%M%S")


def _make_30min_times_kst(from_kst: datetime, to_kst: datetime) -> List[str]:
    """
    from_kst -> to_kst 까지 30분 단위로 내려가며 HHMMSS 리스트 생성 (포함)
    예: 14:25 -> 14:00, 13:30, 13:00 ... 09:00
    """
    times = []
    cur = from_kst.replace(second=0, microsecond=0)

    # 30분 경계로 내림
    minute = 30 if cur.minute >= 30 else 0
    cur = cur.replace(minute=minute)

    while cur >= to_kst:
        times.append(_hhmmss_from_kst(cur))
        cur -= timedelta(minutes=30)
    return times


def get_5min_candles(symbol: str) -> List[List[Any]]:
    TARGET_COUNT = 50

    kst = timezone(timedelta(hours=9))
    now_kst = datetime.now(timezone.utc).astimezone(kst)
    today = now_kst.strftime("%Y%m%d")

    open_kst = now_kst.replace(hour=9, minute=0, second=0, microsecond=0)
    if now_kst < open_kst:
        return []

    times = _make_30min_times_kst(now_kst, open_kst)
    logger.info(f"[5분봉] times: {times}")

    all_rows: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for t in times:
        res = _url_fetch(
            "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice",
            "FHKST03010200",
            "",
            {
                "FID_ETC_CLS_CODE": "",
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": symbol,
                "FID_INPUT_HOUR_1": t,
                "FID_PW_DATA_INCU_YN": "Y",
            },
        )
        logger.info(f"[5분봉] t={t}, isOK={res.isOK()}")
        if not res.isOK():
            continue

        raw = getattr(res.getBody(), "output2", []) or []
        logger.info(f"[5분봉] t={t}, raw={len(raw)}건")

        if raw:
            sample = raw[0]
            logger.info(f"[5분봉] sample → date={sample.get('stck_bsop_date')}, hour={sample.get('stck_cntg_hour')}")

        for r in raw:
            d = (r.get("stck_bsop_date", "") or "").strip()
            if d != today:
                continue  # ✅ 오늘만

            hhmmss = _normalize_time_hhmmss(r.get("stck_cntg_hour", ""))
            if not hhmmss:
                continue

            # ✅ 정규장만 (09:00:30 ~ 20:00:00)
            if hhmmss < "090000" or hhmmss > "153000":
                continue

            all_rows[(d, hhmmss)] = r

        logger.info(f"[5분봉] t={t} 처리 후 누적 all_rows={len(all_rows)}건")

        if len(all_rows) >= 350:
            break

    logger.info(f"[5분봉] {symbol} today={today}, 최종 all_rows={len(all_rows)}건")

    merged = list(all_rows.values())
    merged.sort(key=lambda r: (r.get("stck_bsop_date", ""), _normalize_time_hhmmss(r.get("stck_cntg_hour", ""))))

    candles_5m = build_5min_candles_from_output2(merged)
    logger.info(f"[5분봉] build 후 candles_5m={len(candles_5m)}봉")

    candles_5m = [c for c in candles_5m if str(c[0]).startswith(today + "_")]
    logger.info(f"[5분봉] 오늘 필터 후 candles_5m={len(candles_5m)}봉")

    # ✅ (추가 안전) 오늘만 남기기
    candles_5m = [c for c in candles_5m if str(c[0]).startswith(today + "_")]

    # ✅ 진행중 5분봉 제거
    now_bucket = _floor_to_5min(now_kst.replace(tzinfo=None)).strftime("%Y%m%d_%H%M")
    candles_5m = [c for c in candles_5m if c[0] != now_bucket]
    out = candles_5m[-TARGET_COUNT:]
    logger.info(f"5분봉(완성봉) {symbol} {len(out)}봉 (전체={len(candles_5m)}봉)")
    return candles_5m[-TARGET_COUNT:]


# ── 현재가 ────────────────────────────────────────────
def get_current_price(symbol: str) -> Optional[int]:
    res = _url_fetch(
        "/uapi/domestic-stock/v1/quotations/inquire-price",
        "FHKST01010100", "",
        {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": symbol},
    )
    if not res.isOK():
        res.printError(f"현재가/{symbol}")
        return None
    try:
        output = getattr(res.getBody(), "output", None)
        return int(output.stck_prpr) if output else None
    except Exception as e:
        logger.error(f"현재가 파싱 실패({symbol}): {e}")
        return None


# ── 매수 주문 (페이퍼 트레이딩) ─────────────────────────
def buy_order(symbol: str, qty: int, price: int = 0) -> Optional[dict]:
    """실제 주문 없이 로그/기록만 남김 (페이퍼 트레이딩 모드)"""
    logger.info(f"📝 [페이퍼] 매수 기록: {symbol} {qty}주 @ {price:,}원")
    return {}


# ── 매도 주문 (페이퍼 트레이딩) ─────────────────────────
def sell_order(symbol: str, qty: int, price: int = 0) -> Optional[dict]:
    """실제 주문 없이 로그/기록만 남김 (페이퍼 트레이딩 모드)"""
    logger.info(f"📝 [페이퍼] 매도 기록: {symbol} {qty}주 @ {price:,}원")
    return {}


# ── 잔고 조회 ─────────────────────────────────────────
def get_balance() -> list:
    """TTTC8434R → 모의 시 VTTC8434R 자동 변환"""
    env = getTREnv()
    res = _url_fetch(
        "/uapi/domestic-stock/v1/trading/inquire-balance",
        "TTTC8434R", "",
        {
            "CANO": env.my_acct,
            "ACNT_PRDT_CD": env.my_prod,
            "AFHR_FLPR_YN": "N", "OFL_YN": "",
            "INQR_DVSN": "02", "UNPR_DVSN": "01",
            "FUND_STTL_ICLD_YN": "N",
            "FNCG_AMT_AUTO_RDPT_YN": "N",
            "PRCS_DVSN": "01",
            "CTX_AREA_FK100": "", "CTX_AREA_NK100": "",
        },
    )
    if res.isOK():
        return getattr(res.getBody(), "output1", []) or []
    res.printError("잔고조회")
    return []

# ── 전일 일봉 ─────────────────────────────────────────
def get_daily_candle_prev(symbol: str) -> Optional[dict]:
    """전일 일봉 데이터 반환 (당일 제외 가장 최근 1봉)"""
    kst = timezone(timedelta(hours=9))
    today = datetime.now(timezone.utc).astimezone(kst).strftime("%Y%m%d")

    res = _url_fetch(
        "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
        "FHKST03010100", "",
        {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": symbol,
            "FID_INPUT_DATE_1": "",
            "FID_INPUT_DATE_2": today,
            "FID_PERIOD_DIV_CODE": "D",
            "FID_ORG_ADJ_PRC": "0",
        },
    )
    if not res.isOK():
        logger.warning(f"[일봉] {symbol} API 실패")
        return None

    raw = getattr(res.getBody(), "output2", []) or []

    for r in raw:
        d = (r.get("stck_bsop_date", "") or "").strip()
        if d and d != today:
            logger.info(
                f"[일봉] {symbol} 전일={d} "
                f"O={r.get('stck_oprc')} H={r.get('stck_hgpr')} "
                f"L={r.get('stck_lwpr')} C={r.get('stck_clpr')} "
                f"V={r.get('acml_vol')}"
            )
            return {
                "date":   d,
                "open":   float(r.get("stck_oprc", 0) or 0),
                "high":   float(r.get("stck_hgpr", 0) or 0),
                "low":    float(r.get("stck_lwpr", 0) or 0),
                "close":  float(r.get("stck_clpr", 0) or 0),
                "volume": int(r.get("acml_vol", 0) or 0),
            }

    logger.warning(f"[일봉] {symbol} 전일 데이터 없음")
    return None
    
# ── 전일 요약 정보 함수 ──────────────────────────────
def get_prev_day_snapshot(symbol: str) -> Optional[dict]:
    """
    전일 1분봉 기반 요약 정보
    반환:
    {
        "date": "YYYYMMDD",
        "high": ...,
        "low": ...,
        "close": ...,
        "candles_1m": [...],
        "candles_5m": [...],
    }
    """
    candles_1m = get_prev_day_1min_candles(symbol)
    if not candles_1m:
        return None

    highs = [float(c[2]) for c in candles_1m]
    lows = [float(c[3]) for c in candles_1m]
    closes = [float(c[4]) for c in candles_1m]

    candles_5m = convert_1min_to_5min(candles_1m)

    return {
        "date": str(candles_1m[-1][0]).split("_")[0],
        "high": max(highs),
        "low": min(lows),
        "close": closes[-1],
        "candles_1m": candles_1m,
        "candles_5m": candles_5m,
    }

    # ── 전일 1분봉 조회 함수 ──────────────────────────────
def get_prev_day_1min_candles(symbol: str) -> List[List[Any]]:
    """
    전일 1분봉 조회 — FHKST03010230 (주식일별분봉조회, 120건/회)
    4회 호출로 전일 전체(390분) 커버 (기존 14회 → 4회)
    output2가 분봉 OHLCV를 직접 반환하므로 별도 집계 불필요.
    """
    kst = timezone(timedelta(hours=9))
    now_kst = datetime.now(timezone.utc).astimezone(kst)

    # 전일 날짜 계산 (주말 건너뜀)
    prev_dt = now_kst - timedelta(days=1)
    while prev_dt.weekday() >= 5:
        prev_dt -= timedelta(days=1)
    prev_date = prev_dt.strftime("%Y%m%d")

    times = ["153000", "133000", "113000", "093000"]
    logger.info(f"[전일1분봉] {symbol} prev_date={prev_date}, times={times}")

    all_rows: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for t in times:
        res = _url_fetch(
            "/uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice",
            "FHKST03010230",
            "",
            {
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD":         symbol,
                "FID_INPUT_DATE_1":       prev_date,
                "FID_INPUT_HOUR_1":       t,
                "FID_PW_DATA_INCU_YN":   "Y",
                "FID_FAKE_TICK_INCU_YN": " ",
            },
        )
        logger.info(f"[전일1분봉] t={t}, isOK={res.isOK()}")
        if not res.isOK():
            logger.error(
                f"[전일1분봉 실패] "
                f"symbol={symbol}, t={t}, "
                f"status={res.getResCode()}, "
                f"code={res.getErrorCode()}, "
                f"msg={res.getErrorMessage()}"
            )
            continue

        raw = getattr(res.getBody(), "output2", []) or []
        logger.info(f"[전일1분봉] t={t}, raw={len(raw)}건")

        for r in raw:
            d = (r.get("stck_bsop_date", "") or "").strip()
            if not d or d != prev_date:
                continue
            hhmmss = _normalize_time_hhmmss(r.get("stck_cntg_hour", ""))
            if not hhmmss or hhmmss < "090000" or hhmmss > "153000":
                continue
            all_rows[(d, hhmmss)] = r

    if not all_rows:
        logger.warning(f"[전일1분봉] {symbol} 데이터 없음")
        return []

    out: List[List[Any]] = []
    for (d, hhmmss) in sorted(all_rows.keys()):
        r    = all_rows[(d, hhmmss)]
        ts   = f"{d}_{hhmmss[:4]}"
        o    = int(r.get("stck_oprc", 0) or 0)
        h    = int(r.get("stck_hgpr", 0) or 0)
        l    = int(r.get("stck_lwpr", 0) or 0)
        c    = int(r.get("stck_prpr", 0) or 0)
        v    = int(r.get("cntg_vol",  0) or 0)
        if c == 0:
            continue
        out.append([ts, o, h, l, c, v])

    logger.info(f"[전일1분봉] {symbol} prev_date={prev_date}, candles_1m={len(out)}")
    return out

    # ── 1분봉 → 5분봉 변환 함수 ──────────────────────────────
def convert_1min_to_5min(candles_1m: List[List[Any]]) -> List[List[Any]]:
    """
    1분봉 리스트를 5분봉으로 변환
    입력:
      [ts, o, h, l, c, v]
    출력:
      [ts, o, h, l, c, v]
    """
    if not candles_1m:
        return []

    out: List[List[Any]] = []
    chunk: List[List[Any]] = []

    for row in candles_1m:
        chunk.append(row)
        if len(chunk) == 5:
            ts = chunk[0][0]
            o = int(chunk[0][1])
            h = max(int(x[2]) for x in chunk)
            l = min(int(x[3]) for x in chunk)
            c = int(chunk[-1][4])
            v = sum(int(x[5]) for x in chunk)
            out.append([ts, o, h, l, c, v])
            chunk = []

    return out

    # ── 1분봉 생성 함수 ──────────────────────────────
def build_1min_candles_from_output2(output2: List[Dict[str, Any]]) -> List[List[Any]]:
    """
    output2 체결 데이터 → 1분봉 생성
    반환 형식:
    [
        [YYYYMMDD_HHMM, open, high, low, close, volume],
        ...
    ]
    """
    if not output2:
        return []

    parsed: List[Tuple[datetime, Dict[str, Any]]] = []
    for r in output2:
        dt = _parse_dt_kst(r.get("stck_bsop_date", ""), r.get("stck_cntg_hour", ""))
        parsed.append((dt, r))
    parsed.sort(key=lambda x: x[0])

    buckets: Dict[datetime, Candle] = {}
    for dt, r in parsed:
        bucket = dt.replace(second=0, microsecond=0)

        price = int(r.get("stck_prpr", 0) or 0)
        v = int(r.get("cntg_vol", 0) or 0)

        if price == 0:
            continue

        if bucket not in buckets:
            buckets[bucket] = Candle(ts=bucket, o=price, h=price, l=price, c=price, v=v)
        else:
            cd = buckets[bucket]
            cd.h = max(cd.h, price)
            cd.l = min(cd.l, price)
            cd.c = price
            cd.v += v

    out: List[List[Any]] = []
    for bucket_dt in sorted(buckets.keys()):
        cd = buckets[bucket_dt]
        out.append([cd.ts.strftime("%Y%m%d_%H%M"), cd.o, cd.h, cd.l, cd.c, cd.v])
    return out