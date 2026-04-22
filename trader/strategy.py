"""
strategy.py
공용 전략 판별 엔진 — GAS strategy.gs Python 변환
실시간 / 백테스트 동일하게 사용

[VCP 파라미터]
  dailyVcpScore    — 일봉 VCP 점수 (기본 0)
  dailyStrategy    — "VCP돌파" / "VCP응축" / "" (기본 "")
  dailyPivotPoint  — 일봉 Pivot Point (기본 0)
  dailyStopLoss    — 일봉 손절가 (기본 0)
  호출부(tick_engine 등)가 Firestore strategy_results 에서 읽어서 주입.
  넘기지 않으면 기본값으로 동작 → 기존 조건1~6만 판별.
"""

import math
import logging
import datetime
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════
# priceIndex 상수
# ════════════════════════════════════════════════════════
class PriceIndex:
    TIME   = 0
    OPEN   = 1
    HIGH   = 2
    LOW    = 3
    CLOSE  = 4
    VOLUME = 5


# ════════════════════════════════════════════════════════
# 전략 설정 기본값  (StrategyConfig.js 기준)
# ════════════════════════════════════════════════════════
DEFAULT_CONFIG: Dict[str, Any] = {
    # [1] 이평선 — JS의 maShort/maLong 과 동일
    "maShort": 5,
    "maLong":  20,

    # [2] GC 판별
    "gcBreakout": {
        "startHm":   930,
        "endHm":     1130,
        "validBars": 15,   # JS 원본 기본값
    },

    # [3] 전일고가 돌파 조건 세부
    "prevHigh": {
        "startHm":           905,
        "endHm":             1030,
        "breakoutBufferPct": 0.2,
        "volMultiplier":     1.5,
        "closeMarginPct":    0.1,
        "minBodyRatio":      0.5,
        "minTradeValue":     300_000_000,
    },

    # [4] 수익 관리
    "targetProfit":  5.0,
    "stopLoss":      2.0,
    "trailingStart": 3.0,
    "trailingStop":  1.5,

    # [5] 거래 비용
    "fee": 0.2,

    # [6] 시간 제어
    "buyStopTime": 1520,

    # [7] 진입 조건
    "swingLookback":    10,
    "volMultiplier":    2.0,
    "gapThreshold":     2.5,
    "steadyRisingBars": 5,
    "ma5SlopeMinPct":   0.003,
    "ma5ConsecBars":    4,
    "dayHighDropLimit": 0.03,

    # [8] VCP
    "vcpEarlyEntry": True,

    # [9] 1분봉 구간 기반 빠른 매도
    "fastExit1m": {
        "enabled": True,
        "excludeFirstBarInBucket": True,
        "minBars": 2,
        "weakBars": 2,
        "needConsecutiveWeakClose": True,
    },

    # [10] priceIndex
    "priceIndex": {
        "time":   PriceIndex.TIME,
        "open":   PriceIndex.OPEN,
        "high":   PriceIndex.HIGH,
        "low":    PriceIndex.LOW,
        "close":  PriceIndex.CLOSE,
        "volume": PriceIndex.VOLUME,
    },
}

# 금요일 파라미터 보정값
FRIDAY_CONFIG = {
    "entryTimeLimitHm": 1330,
    "trailingStartMul": 0.7,
    "trailingStopMul":  0.7,
}


# ════════════════════════════════════════════════════════
# 메인 신호 판별 함수
# ════════════════════════════════════════════════════════
def get_strategy_signal(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    5분봉 전략 신호 판별.

    ┌──────────────────────────┬──────────────────────────────────┐
    │ 필수 키                  │ 설명                              │
    ├──────────────────────────┼──────────────────────────────────┤
    │ i              int       │ 현재 봉 인덱스                    │
    │ data           list      │ 5분봉 배열                        │
    │ cfg            dict      │ 전략 설정 (DEFAULT_CONFIG)        │
    │ isHolding      bool      │ 현재 보유 여부                    │
    │ entryPrice     float     │ 진입 단가 (미보유=0)              │
    │ maxPriceAfterEntry float │ 진입 후 최고가 (미보유=0)         │
    │ ma5_curr       float     │ 현재봉 MA5                       │
    │ ma20_curr      float     │ 현재봉 MA20                      │
    │ ma5_prev       float     │ 직전봉 MA5                       │
    │ ma20_prev      float     │ 직전봉 MA20                      │
    │ prevDayHigh    float     │ 전일 고가                        │
    │ pivotR2        float     │ 피봇 R2                          │
    ├──────────────────────────┼──────────────────────────────────┤
    │ 선택 키 (기본값 있음)     │                                  │
    ├──────────────────────────┼──────────────────────────────────┤
    │ ma5_prev2      float     │ 2봉 전 MA5 (None 허용)           │
    │ prevDayVolume  float     │ 전일 거래량 (0=소외주차단 비활성) │
    │ data1Min       list      │ 1분봉 (None=조건5 비활성)         │
    │ dailyVcpScore  int       │ 일봉 VCP 점수 (기본 0)            │
    │ dailyStrategy  str       │ "VCP돌파"/"VCP응축"/"" (기본 "")  │
    │ dailyPivotPoint float    │ 일봉 Pivot Point (기본 0)         │
    │ dailyStopLoss  float     │ 일봉 손절가 (기본 0)              │
    └──────────────────────────┴──────────────────────────────────┘

    Returns
    -------
    { "signal": "BUY"|"SELL"|"HOLD"|None, "reason": str, "energy": dict }
    """

    # ── 파라미터 언패킹 ───────────────────────────────────
    i                     = params["i"]
    data                  = params["data"]
    cfg                   = params["cfg"]
    is_holding            = params["isHolding"]
    entry_price           = params.get("entryPrice", 0) or 0
    max_price_after_entry = params.get("maxPriceAfterEntry", 0) or 0
    trailing_exit_price   = params.get("trailingExitPrice", 0) or 0
    ma5_curr              = params["ma5_curr"]
    ma20_curr             = params["ma20_curr"]
    ma5_prev              = params["ma5_prev"]
    ma20_prev             = params["ma20_prev"]
    ma5_prev2             = params.get("ma5_prev2")
    ema5_prev2            = params.get("ema5_prev2")
    ema5_curr             = params["ema5_curr"]
    ema20_curr            = params["ema20_curr"]
    ema5_prev             = params["ema5_prev"]
    ema20_prev            = params["ema20_prev"]
    prev_day_high         = params.get("prevDayHigh", 0) or 0
    pivot_r2              = params.get("pivotR2", 0) or 0
    prev_day_volume       = params.get("prevDayVolume", 0) or 0
    prev_day_close        = params.get("prevDayClose", 0) or 0
    data_1min             = params.get("data1Min")
    # VCP 파라미터 — Firestore strategy_results 에서 읽어서 주입, 없으면 기본값
    daily_vcp_score       = int(params.get("dailyVcpScore", 0) or 0)
    daily_strategy        = params.get("dailyStrategy", "") or ""
    daily_pivot_point     = float(params.get("dailyPivotPoint", 0) or 0)
    daily_stop_loss       = float(params.get("dailyStopLoss", 0) or 0)

    pi       = cfg["priceIndex"]
    bar      = data[i]
    close    = bar[pi["close"]]
    high     = bar[pi["high"]]
    low      = bar[pi["low"]]
    open_    = bar[pi["open"]]
    volume   = bar[pi["volume"]]
    time_str = str(bar[pi["time"]])

    logger.debug(
        f"🔍 [전략진입] 시간:{time_str} | 종가:{close} | 보유:{is_holding} | "
        f"MA5:{ma5_curr:.0f} | MA20:{ma20_curr:.0f} | 진입가:{entry_price or '-'} | "
        f"전일고가:{prev_day_high} | 피봇R2:{pivot_r2} | "
        f"VCP점수:{daily_vcp_score} | VCP전략:{daily_strategy}"
    )
    logger.debug(f"🕯️ [캔들raw] index:{i} | {bar}")
    logger.debug(f"⚙️ [priceIndex] {pi}")

    # ── 에너지 응축 점수 ─────────────────────────────────
    energy: Dict[str, Any] = {
        "score": 0, "boxRange": 0, "maDiff": 0,
        "isVolContracting": False,
        "breakoutLine": math.inf, "isBreakout": False,
    }
    if i >= 48:
        result = _calc_energy_score(data, i, cfg)
        energy = result
        energy["isBreakout"] = close > result["breakoutLine"]
        logger.debug(
            f"⚡ [에너지응축] 점수:{energy['score']} | "
            f"박스상단:{energy['breakoutLine']} | 현재가:{close} | "
            f"돌파:{energy['isBreakout']} | "
            f"박스범위:{energy['boxRange']*100:.2f}% | "
            f"MA이격:{energy['maDiff']*100:.2f}% | "
            f"거래량수축:{energy['isVolContracting']}"
        )

    # ── 시간 파싱 ────────────────────────────────────────
    hm_str = time_str.split("_")[1] if "_" in time_str else ""
    hm     = hm_str[:4]
    hm_int = int(hm) if hm.isdigit() else 0

    # ════════════════════════════════════════════════════
    # 매수 신호 판별
    # ════════════════════════════════════════════════════
    if not is_holding:

        # ── 장외 시간 차단 ───────────────────────────────
        if hm_int < 900 or hm_int >= 2000:
            return {"signal": "HOLD", "reason": "장외시간", "energy": energy}

        # ── 상한가 근접 매수 차단 (29%) ──────────────────
        _prev_close_buy = params.get("prevDayClose", 0) or close
        if _prev_close_buy > 0 and close >= _prev_close_buy * 1.29:
            return {"signal": "HOLD", "reason": "상한가근접매수차단", "energy": energy}

        # ── 금요일 진입 제한 ─────────────────────────────
        is_friday = (datetime.datetime.now().weekday() == 4)
        if is_friday:
            if hm_int >= FRIDAY_CONFIG["entryTimeLimitHm"]:
                return {"signal": "HOLD", "reason": "금요일오후진입차단", "energy": energy}
            cfg = dict(cfg)
            cfg["trailingStart"] = cfg.get("trailingStart", 3.0) * FRIDAY_CONFIG["trailingStartMul"]
            cfg["trailingStop"]  = cfg.get("trailingStop",  1.5) * FRIDAY_CONFIG["trailingStopMul"]

        # ── 당일 누적 거래량 ─────────────────────────────
        today = time_str.split("_")[0]
        cumulative_volume = sum(
            data[k][pi["volume"]]
            for k in range(i + 1)
            if str(data[k][pi["time"]]).split("_")[0] == today
        )

        # ── 소외주 / 오후 수급 부족 차단 ─────────────────
        if prev_day_volume > 0:
            vol_ratio = cumulative_volume / prev_day_volume
            if hm_int >= 1030 and vol_ratio < 0.3:
                return {"signal": "HOLD", "reason": "소외주차단(10:30기준 거래량 30% 미달)", "energy": energy}
            if hm_int >= 1300 and vol_ratio < 0.5:
                return {"signal": "HOLD", "reason": "오후수급부족", "energy": energy}

        # ── 공통 지표 ────────────────────────────────────
        is_early_morning = "0900" <= hm <= "1030"
        is_ma_bull       = ma5_curr > ma20_curr

        swing_high        = _find_last_swing_high(data, i - 1, pi["high"])
        if swing_high is None:
            is_swing_breakout = False
        else:
            is_swing_breakout = close > swing_high

        is_pivot_r2_breakout = close > pivot_r2
        is_bull_candle       = close > open_

        avg_vol     = _calc_avg_vol(data, i, 20, pi["volume"])
        avg_vol_day = _calc_avg_vol(data, i, 20, pi["volume"])
        vol_ratio = (volume / avg_vol) if avg_vol > 0 else 0.0
        is_vol_explosion = (
            vol_ratio >= cfg["volMultiplier"]
            and volume > avg_vol_day * 0.8
        )

        gap_from_open = _calc_gap_from_open(data, i, close, pi)
        is_gap_up     = gap_from_open > cfg["gapThreshold"] * 0.7

        ma5_up1         = ma5_curr > ma5_prev
        
        ma5_up2         = (ma5_prev > ma5_prev2) if ma5_prev2 is not None else True
        ma5_slope_pct   = (ma5_curr - ma5_prev) / ma5_prev if ma5_prev > 0 else 0
        ma5_slope_ok    = ma5_slope_pct >= cfg.get("ma5SlopeMinPct", 0.003)
        price_above_ma5 = close > ma5_curr
        ema5_up1 = ema5_curr > ema5_prev
        is_ema5_falling = (
            not ema5_up1
            or (ema5_prev2 is not None and ema5_prev < ema5_prev2)
        )
        ema5_5bars_ago  = params.get("ema5_5bars_ago")
        ema5_slope_ok   = (ema5_5bars_ago is not None and ema5_curr > ema5_5bars_ago)

        # 트레일링 청산 후 재진입 필터
        # trailing_exit_price > 0 이면 트레일링 청산 직후 재진입 시도임
        _trailing_reentry_max_pct = cfg.get("trailingReentryMaxPct", 1.5)
        trailing_reentry_price_ok = (
            trailing_exit_price <= 0
            or close <= trailing_exit_price * (1 + _trailing_reentry_max_pct / 100)
        )
        trailing_reentry_ema_ok = (
            trailing_exit_price <= 0
            or not is_ema5_falling
        )

        is_steady_up = (
            _check_steady_rising(
                data_1min, len(data_1min) - 1,
                cfg["steadyRisingBars"], pi["close"]
            )
            if data_1min else False
        )

        is_energy_breakout = (
            energy["score"] >= 60
            and energy["isBreakout"]
            and is_vol_explosion
        )        

        # ── GC 판별 ──────────────────────────────────────
        gc_cfg        = cfg.get("gcBreakout", {})
        gc_start_hm   = gc_cfg.get("startHm",   930)
        gc_end_hm     = gc_cfg.get("endHm",     1130)
        gc_valid_bars = gc_cfg.get("validBars",   15)

        is_within_gc_window     = gc_start_hm <= hm_int <= gc_end_hm
        has_recent_golden_cross = False

        for j in range(max(1, i - gc_valid_bars + 1), i + 1):
            m5p  = _ma(data, j - 1, cfg["maShort"], pi["close"])
            m20p = _ma(data, j - 1, cfg["maLong"],  pi["close"])
            m5c  = _ma(data, j,     cfg["maShort"], pi["close"])
            m20c = _ma(data, j,     cfg["maLong"],  pi["close"])
            if None not in (m5p, m20p, m5c, m20c):
                if m5p <= m20p and m5c > m20c:
                    has_recent_golden_cross = True
                    break

        # ── VCP 공통 변수 ─────────────────────────────────
        is_vcp_high              = daily_vcp_score >= 70
        is_vcp_medium            = daily_vcp_score >= 50
        is_vcp_breakout_strategy = daily_strategy == "VCP돌파"
        is_vcp_squeeze_strategy  = daily_strategy == "VCP응축"

        is_pivot_breakout = (
            daily_pivot_point > 0
            and close > daily_pivot_point
            and close <= daily_pivot_point * 1.03
        )

        if daily_stop_loss > 0:
            logger.debug(
                f"📌 [일봉손절가] {daily_stop_loss} | 현재가:{close} | "
                f"여유:{(close - daily_stop_loss) / close * 100:.1f}%"
            )

        # ════════════════════════════════════════════════
        # 매수 조건 (우선순위 순)
        # ════════════════════════════════════════════════

        # ── 공통 가드: 현재가가 EMA20 위에 있어야 매수 가능 ──
        if close <= ema20_curr:
            return {"signal": None, "reason": "", "energy": energy}        

        # ──────────────────────────────────────────────
        # [조건0-A] VCP 최우선: 일봉 VCP돌파 고점수 + 5분봉 Pivot 돌파
        # ──────────────────────────────────────────────
        if (is_vcp_breakout_strategy and is_vcp_high
                and is_pivot_breakout and is_vol_explosion):
            logger.debug(
                f"🏆 [VCP최우선-A] VCP점수:{daily_vcp_score} | "
                f"Pivot:{daily_pivot_point} | 현재가:{close}"
            )
            return {
                "signal": "BUY",
                "reason": f"VCP돌파+Pivot(점수{daily_vcp_score})",
                "energy": energy,
            }

        # ──────────────────────────────────────────────
        # [조건0-B] VCP 고점수 + 5분봉 에너지 응축 돌파
        # ──────────────────────────────────────────────
        if is_vcp_high and is_energy_breakout:
            logger.debug(
                f"🥇 [VCP최우선-B] VCP점수:{daily_vcp_score} | "
                f"에너지점수:{energy['score']}"
            )
            return {
                "signal": "BUY",
                "reason": f"VCP고점수+에너지돌파(점수{daily_vcp_score})",
                "energy": energy,
            }

        # ──────────────────────────────────────────────
        # [조건0-C] VCP 응축 조기 진입
        #   cfg.vcpEarlyEntry = False 로 비활성화 가능
        # ──────────────────────────────────────────────
        if (cfg.get("vcpEarlyEntry", True)
                and is_vcp_squeeze_strategy and is_vcp_medium
                and has_recent_golden_cross and is_within_gc_window
                and is_swing_breakout):
            logger.debug(
                f"🥈 [VCP조기진입] VCP점수:{daily_vcp_score} | GC+전고돌파"
            )
            return {
                "signal": "BUY",
                "reason": f"VCP응축+GC전고(점수{daily_vcp_score})",
                "energy": energy,
            }

        # ──────────────────────────────────────────────
        # [조건1] GC + 전고점 돌파
        # ──────────────────────────────────────────────        
        if has_recent_golden_cross and is_within_gc_window and is_swing_breakout:

            # ──────────────────────────────────────────────
            # [1] EMA 정렬 체크 (기존 유지)
            # ──────────────────────────────────────────────
            if not (ema5_curr > ema20_curr and close > ema5_curr and close > ma5_curr):
                return {"signal": "HOLD", "reason": "GC전고돌파-EMA미정렬", "energy": energy}

            # ──────────────────────────────────────────────
            # [2] 돌파폭 필터
            # ──────────────────────────────────────────────
            breakout_level = swing_high

            breakout_margin_pct = ((close / breakout_level) - 1) * 100 if breakout_level else 0
            if breakout_margin_pct < cfg.get("minBreakoutMarginPct", 0.3):
                return {"signal": "HOLD", "reason": "돌파폭부족", "energy": energy}

            # ──────────────────────────────────────────────
            # [3] 이격 과열 차단
            # ──────────────────────────────────────────────
            dist_from_ema5 = ((close / ema5_curr) - 1) * 100 if ema5_curr else 0
            if dist_from_ema5 > cfg.get("maxDistanceFromEma5Pct", 3.0):
                return {"signal": "HOLD", "reason": "이격과열", "energy": energy}

            # ──────────────────────────────────────────────
            # [4] 확인봉 (핵심)
            # ──────────────────────────────────────────────
            if i <= 0:
                return {"signal": "HOLD", "reason": "데이터부족", "energy": energy}

            prev_close = data[i - 1][pi["close"]]
            
            prev_breakout = (
                swing_high is not None
                and prev_close > swing_high
            )

            if not prev_breakout:
                return {"signal": "HOLD", "reason": "돌파확인대기", "energy": energy}

            tag = f"+VCP{daily_vcp_score}" if is_vcp_medium else ""
            return {"signal": "BUY", "reason": "GC+전고돌파확인{tag}", "energy": energy}
        
        """
        if has_recent_golden_cross and is_within_gc_window and is_swing_breakout:
            if not (ema5_curr > ema20_curr and close > ema5_curr and ema5_slope_ok):
                logger.debug(
                    f"⛔ [GC전고차단] EMA 미정렬 | "
                    f"ema5:{ema5_curr:.0f} | ema20:{ema20_curr:.0f} | close:{close:.0f}"
                )
                return {"signal": "HOLD", "reason": "GC전고돌파-EMA미정렬", "energy": energy}
            tag = f"+VCP{daily_vcp_score}" if is_vcp_medium else ""
            return {"signal": "BUY", "reason": f"GC+전고돌파{tag}", "energy": energy}
        """
        # ──────────────────────────────────────────────
        # [조건1-B] GC + 거래량 급증
        # ──────────────────────────────────────────────
        if (is_within_gc_window and is_ma_bull
                and vol_ratio >= 2
                and ma5_up1 and is_bull_candle and price_above_ma5):
            tag = f"+VCP{daily_vcp_score}" if is_vcp_medium else ""
            if not trailing_reentry_price_ok:
                pct = (close / trailing_exit_price - 1) * 100
                return {"signal": "HOLD", "reason": f"트레일링재진입차단(+{pct:.1f}%)", "energy": energy}
            if not trailing_reentry_ema_ok:
                return {"signal": "HOLD", "reason": "트레일링재진입차단(EMA5하락)", "energy": energy}
            return {"signal": "BUY", "reason": f"GC+거래량급증({vol_ratio:.1f}배){tag}", "energy": energy}
        
        # ──────────────────────────────────────────────
        # [조건2] 전일 고가 돌파
        # ──────────────────────────────────────────────
        prev_close = data[i - 1][pi["close"]] if i > 0 else None
        ph_cfg     = cfg.get("prevHigh", {})

        breakout_buffer_pct = ph_cfg.get("breakoutBufferPct", 0.2)
        breakout_line       = prev_day_high * (1 + breakout_buffer_pct / 100)

        is_fresh_prev_high_breakout = (
            prev_close is not None
            and prev_close <= breakout_line
            and close > breakout_line
        )
        trend_ok = ma5_curr > ma20_curr and ma5_curr > ma5_prev and close > ma5_curr
        vol_ok   = volume > avg_vol * ph_cfg.get("volMultiplier", 1.5)

        close_margin_pct    = ph_cfg.get("closeMarginPct", 0.1)
        close_strong_enough = close >= breakout_line * (1 + close_margin_pct / 100)

        body_         = abs(close - open_)
        range_        = high - low
        body_ratio_ok = range_ > 0 and (body_ / range_) >= ph_cfg.get("minBodyRatio", 0.5)

        trade_value_ok = (close * volume) >= ph_cfg.get("minTradeValue", 300_000_000)
        ph_time_ok     = ph_cfg.get("startHm", 905) <= hm_int <= ph_cfg.get("endHm", 1030)

        if (ph_time_ok and is_bull_candle and is_fresh_prev_high_breakout
                and trend_ok and vol_ok and close_strong_enough
                and body_ratio_ok and trade_value_ok):
            return {"signal": "BUY", "reason": "전일고가돌파", "energy": energy}

        # ──────────────────────────────────────────────
        # [조건3] 장 초반 피봇 R2 돌파
        #   MA5 하락 기울기이면 BUY 대신 HOLD
        # ──────────────────────────────────────────────
        # 변경: EMA5 기울기 판단        

        if is_early_morning and is_bull_candle and is_pivot_r2_breakout:
            if is_ema5_falling:
                logger.debug(
                    f"⛔ [피봇R2차단] EMA5 하락 기울기 | "
                    f"ema5_prev2:{ema5_prev2} | ema5_prev:{ema5_prev} | ema5_curr:{ema5_curr}"
                )
                return {"signal": "HOLD", "reason": "피봇R2차단(EMA5하락)", "energy": energy}
            return {"signal": "BUY", "reason": "피봇R2돌파", "energy": energy}

        # ──────────────────────────────────────────────
        # [조건4] 거래량 폭발 + MA5 연속 상승
        # ──────────────────────────────────────────────
        if (is_ma_bull and is_vol_explosion and is_gap_up
                and ma5_up1 and ma5_up2 and ma5_slope_ok and price_above_ma5):
            if not trailing_reentry_price_ok:
                pct = (close / trailing_exit_price - 1) * 100
                return {"signal": "HOLD", "reason": f"트레일링재진입차단(+{pct:.1f}%)", "energy": energy}
            if not trailing_reentry_ema_ok:
                return {"signal": "HOLD", "reason": "트레일링재진입차단(EMA5하락)", "energy": energy}
            return {
                "signal": "BUY",
                "reason": f"거래량폭발({vol_ratio:.1f}배)+시가대비상승+MA5상승(연속)",
                "energy": energy,
            }

        # ──────────────────────────────────────────────
        # [조건5] 1분봉 지속 상승
        # ──────────────────────────────────────────────
        if (
            is_ma_bull
            and is_steady_up
            and ma5_up1
            and ma5_slope_ok
            and price_above_ma5
            and volume > avg_vol * 5
        ):
            if not trailing_reentry_price_ok:
                pct = (close / trailing_exit_price - 1) * 100
                return {"signal": "HOLD", "reason": f"트레일링재진입차단(+{pct:.1f}%)", "energy": energy}
            if not trailing_reentry_ema_ok:
                return {"signal": "HOLD", "reason": "트레일링재진입차단(EMA5하락)", "energy": energy}
            return {"signal": "BUY", "reason": "1분봉지속상승", "energy": energy}

        # ──────────────────────────────────────────────
        # [조건5-A] 1분봉 EMA 골든크로스 + 거래량 폭발
        # ──────────────────────────────────────────────
        if data_1min and len(data_1min) >= 10:
            closes_1m = [row[PriceIndex.CLOSE] for row in data_1min]
            vols_1m   = [row[PriceIndex.VOLUME] for row in data_1min]            

            ema5_curr_1m  = _ema(closes_1m[-5:], 5)
            ema20_curr_1m = _ema(closes_1m[-20:], 20) if len(closes_1m) >= 20 else None

            ema5_prev_1m  = _ema(closes_1m[-6:-1], 5) if len(closes_1m) >= 6 else None
            ema20_prev_1m = _ema(closes_1m[-21:-1], 20) if len(closes_1m) >= 21 else None

            # 거래량 평균 (최근 5~10봉)
            avg_vol = sum(vols_1m[-10:-1]) / max(len(vols_1m[-10:-1]), 1)
            vol_explosion = volume >= avg_vol * 10

            ema_cross = (
                ema5_prev_1m is not None and ema20_prev_1m is not None and
                ema5_curr_1m is not None and ema20_curr_1m is not None and
                ema5_prev_1m <= ema20_prev_1m and
                ema5_curr_1m > ema20_curr_1m
            )

            if ema_cross and vol_explosion:
                return {
                    "signal": "BUY",
                    "reason": "1분봉EMA골든+거래량폭발",
                    "energy": energy
                }
                
        # ──────────────────────────────────────────────
        # [조건6] 에너지 응축 돌파
        #   VCP 중간점수(>=50) 이면 임계값 55점으로 완화 (기본 60점)
        # ──────────────────────────────────────────────
        energy_threshold = 55 if is_vcp_medium else 60
        if (energy["score"] >= energy_threshold
                and energy["isBreakout"]
                and is_vol_explosion):
            tag = f"+VCP{daily_vcp_score}" if is_vcp_medium else ""
            if not trailing_reentry_price_ok:
                pct = (close / trailing_exit_price - 1) * 100
                return {"signal": "HOLD", "reason": f"트레일링재진입차단(+{pct:.1f}%)", "energy": energy}
            if not trailing_reentry_ema_ok:
                return {"signal": "HOLD", "reason": "트레일링재진입차단(EMA5하락)", "energy": energy}
            return {"signal": "BUY", "reason": f"에너지응축돌파{tag}", "energy": energy}

    # ════════════════════════════════════════════════════
    # 매도 신호 판별
    # ════════════════════════════════════════════════════
    else:
        fast_exit_cfg = cfg.get("fastExit1m", {}) or {}
        interval_1m = _extract_1m_interval_window(
            data_1min,
            time_str,
            exclude_first_bar=fast_exit_cfg.get("excludeFirstBarInBucket", True),
        )
        interval_enabled = fast_exit_cfg.get("enabled", True)
        interval_weak = (
            _is_weak_1m_interval_window(
                interval_1m,
                weak_bars=fast_exit_cfg.get("weakBars", 2),
                need_consecutive_weak_close=fast_exit_cfg.get("needConsecutiveWeakClose", True),
            )
            if interval_enabled and len(interval_1m) >= fast_exit_cfg.get("minBars", 2)
            else False
        )

        # 1분봉 마지막 close를 현재가로 우선 사용 (5분봉보다 실시간성 높음)
        last_1m_close = data_1min[-1][PriceIndex.CLOSE] if data_1min else None
        current_price = last_1m_close if last_1m_close else close
        current_profit    = (current_price / entry_price - 1) * 100 if entry_price else 0
        max_profit_so_far = (max_price_after_entry / entry_price - 1) * 100 if entry_price else 0

        # ──────────────────────────────────────────────
        # [A-0] 상한가 터치 후 이탈 매도
        # ──────────────────────────────────────────────
        prev_close = params.get("prevDayClose", 0) or close
        limit_up_price = prev_close * 1.29
        touched_limit_up = params.get("touchedLimitUp", False)

        if touched_limit_up and high < limit_up_price:
            logger.debug(
                f"🚀 [상한가이탈매도] prev_close:{prev_close} | "
                f"target:{limit_up_price:.2f} | high:{high}"
            )
            return {
                "signal": "SELL",
                "reason": "상한가이탈",
                "energy": energy,
            }

        if high >= limit_up_price:
            logger.debug(
                f"🚀 [상한가터치-대기] prev_close:{prev_close} | "
                f"target:{limit_up_price:.2f} | high:{high}"
            )
            return {
                "signal": "HOLD",
                "reason": "상한가터치-대기",
                "energy": energy,
            }

        # ──────────────────────────────────────────────
        # [A-1] 1분봉 구간 약세 기반 빠른 청산
        #   현재 1분봉이 속한 5분 구간 안의 1분봉들을 함께 보고 판단
        # ──────────────────────────────────────────────
        if interval_weak and len(interval_1m) >= fast_exit_cfg.get("minBars", 2):
            closes_1m = [row[PriceIndex.CLOSE] for row in interval_1m]
            highs_1m = [row[PriceIndex.HIGH] for row in interval_1m]
            interval_drop_pct = ((closes_1m[-1] / max(highs_1m)) - 1) * 100 if highs_1m and max(highs_1m) > 0 else 0
            if current_profit < -1.5 and interval_drop_pct <= -1.2:
                logger.debug(
                    f"⚠️ [1분구간빠른청산] 현재수익:{current_profit:.2f}% | 구간하락:{interval_drop_pct:.2f}% | 구간봉수:{len(interval_1m)}"
                )
                return {"signal": "SELL", "reason": "1분구간약세청산", "energy": energy}        

        # ──────────────────────────────────────────────
        # [C] 손절
        #   dailyStopLoss > 0 → VCP 일봉 손절가 기준
        #   아니면            → cfg.stopLoss 기준
        #   (VCP 손절가가 cfg.stopLoss보다 넓을 수 있으므로 통합 처리)
        # ──────────────────────────────────────────────
        effective_stop_pct = (
            abs(daily_stop_loss / entry_price - 1) * 100
            if daily_stop_loss > 0 and entry_price > 0
            else cfg.get("stopLoss", 2.0)
        )
        if current_profit <= -effective_stop_pct:
            logger.debug(
                f"🛑 [손절대기] 현재수익:{current_profit:.2f}% | 기준:-{effective_stop_pct:.2f}% → engine에서 다음 1분봉 확인"
            )            
            return {"signal": "SELL", "reason": "손절대기", "energy": energy}
        
        # ──────────────────────────────────────────────
        # [B] 트레일링 스탑
        # ──────────────────────────────────────────────
        drop_from_peak = max_profit_so_far - current_profit
        trailing_start = cfg.get("trailingStart", 3.0)
        trailing_stop  = cfg.get("trailingStop",  1.5)
        hard_drop_limit = cfg.get("hardDropLimit", 2.5)

        ema5_break = False

        """
        if data_1min and len(data_1min) >= 6:
            closes_1m = [row[PriceIndex.CLOSE] for row in data_1min]
            opens_1m  = [row[PriceIndex.OPEN] for row in data_1min]

            ema5_curr_1m = _ema(closes_1m[-5:], 5)
            ema5_prev_1m = _ema(closes_1m[-6:-1], 5)

            if ema5_curr_1m is not None and ema5_prev_1m is not None:
                ema5_break = (
                    closes_1m[-1] < ema5_curr_1m     # 현재 1분봉 종가가 EMA5 아래
                    and opens_1m[-1] < ema5_curr_1m  # 현재 1분봉 시가도 EMA5 아래
                    and ema5_curr_1m < ema5_prev_1m  # 1분 EMA5 하락 중
                )
        """
        
        # 5분봉 기준 EMA5 이탈
        if ema5_curr is not None and ema5_prev is not None:
            ema5_break = (
                close < ema5_curr     # 현재 5분봉 종가가 EMA5 아래
                and open_ < ema5_curr # 현재 5분봉 시가도 EMA5 아래
                and ema5_curr < ema5_prev  # EMA5 하락 중
            )

        logger.info(
            f"[TRAIL-CHECK] entry:{entry_price} | "
            f"max_after_entry:{max_price_after_entry} | "
            f"current_profit:{current_profit:.2f}% | "
            f"max_profit:{max_profit_so_far:.2f}% | "
            f"drop:{drop_from_peak:.2f}% | "
            f"start:{trailing_start:.2f}% | stop:{trailing_stop:.2f}%"
        )
        
        if (
            max_profit_so_far >= trailing_start
            and drop_from_peak >= trailing_stop
            and ema5_break
        ):
            reason = f"트레일링+EMA5이탈(5분봉)(시작{trailing_start:.2f}%, 되돌림{trailing_stop:.2f}%↓)"
            if interval_weak:
                reason = f"1분구간+{reason}"
            return {
                "signal": "SELL",
                "reason": reason,
                "energy": energy,
            }

        # 추가: EMA5 무관 하드컷
        if (
            max_profit_so_far >= trailing_start
            and drop_from_peak >= hard_drop_limit
        ):
            reason = f"트레일링강제(되돌림{drop_from_peak:.2f}%↓, 한계{hard_drop_limit:.2f}%)"
            if interval_weak:
                reason = f"1분구간+{reason}"
            return {
                "signal": "SELL",
                "reason": reason,
                "energy": energy,
            }
        
        # ──────────────────────────────────────────────
        # [D] EMA 데드크로스 확정
        #   단순 교차가 아니라
        #   1) 교차 발생
        #   2) 이격도 확보
        #   3) EMA5 하락 기울기
        #   4) 지속성 확인
        # ──────────────────────────────────────────────
        dead_cross_gap_pct = cfg.get("deadCrossGapPct", 0.10)          # 최소 이격도 (%)
        dead_cross_confirm_bars = cfg.get("deadCrossConfirmBars", 1)   # 지속성용, 현재는 1봉 확인 개념

        diff_prev = ema5_prev - ema20_prev
        diff_curr = ema5_curr - ema20_curr

        # 1) 교차 발생
        dead_cross = diff_prev >= 0 and diff_curr < 0

        # 2) 이격도: EMA5가 EMA20 아래로 얼마나 내려갔는지
        gap_pct = ((ema20_curr - ema5_curr) / ema20_curr * 100) if ema20_curr > 0 else 0.0
        gap_ok = gap_pct >= dead_cross_gap_pct

        # 3) 기울기: EMA5가 실제 하락 중인지
        ema5_down = (
            ema5_prev2 is not None
            and ema5_curr < ema5_prev < ema5_prev2
        )

        # EMA20도 꺾이거나 최소한 상승 둔화 상태인지 함께 확인
        ema20_not_rising = ema20_curr <= ema20_prev

        # 4) 지속성:
        # 현재봉 종가도 EMA20 아래에 있어야 하고,
        # EMA5 자체도 EMA20 아래에 머문 상태를 확인
        close_below_ema20 = close < ema20_curr
        stay_below_ok = diff_curr < 0 and close_below_ema20

        if dead_cross and gap_ok and ema5_down and ema20_not_rising and stay_below_ok:
            logger.debug(
                f"📉 [EMA데드크로스확정] "
                f"diff_prev:{diff_prev:.4f} | diff_curr:{diff_curr:.4f} | "
                f"gap:{gap_pct:.3f}% | "
                f"ema5_prev2:{ema5_prev2} | ema5_prev:{ema5_prev} | ema5_curr:{ema5_curr} | "
                f"ema20_prev:{ema20_prev} | ema20_curr:{ema20_curr} | close:{close}"
            )
            return {
                "signal": "SELL",
                "reason": f"EMA데드크로스확정(이격{gap_pct:.2f}%)",
                "energy": energy,
            }

    return {"signal": None, "reason": "", "energy": energy}


def _extract_1m_interval_window(data_1min: Optional[list], time_str: str, exclude_first_bar: bool = True) -> list:
    """현재 1분봉이 속한 5분 구간의 1분봉만 추출한다.
    예) 판단 기준 09:04이면 같은 구간의 09:01~09:04를 사용하도록 첫 분(bar) 제외 옵션 지원.
    """
    if not data_1min or not time_str or "_" not in time_str:
        return []

    date_str, hhmm = time_str.split("_", 1)
    if len(hhmm) < 4 or not hhmm[:4].isdigit():
        return []

    current_minute = int(hhmm[:2]) * 60 + int(hhmm[2:4])
    bucket_start = (current_minute // 5) * 5
    filtered = []

    for row in data_1min:
        row_time = str(row[PriceIndex.TIME])
        if "_" not in row_time:
            continue
        row_date, row_hhmm = row_time.split("_", 1)
        if row_date != date_str or len(row_hhmm) < 4 or not row_hhmm[:4].isdigit():
            continue
        row_minute = int(row_hhmm[:2]) * 60 + int(row_hhmm[2:4])
        if bucket_start <= row_minute <= current_minute:
            filtered.append(row)

    filtered.sort(key=lambda r: str(r[PriceIndex.TIME]))
    if exclude_first_bar and len(filtered) >= 2:
        filtered = filtered[1:]
    return filtered


def _is_weak_1m_interval_window(window_1m: list, weak_bars: int = 2, need_consecutive_weak_close: bool = True) -> bool:
    if len(window_1m) < max(1, weak_bars):
        return False

    bearish_count = sum(1 for row in window_1m if row[PriceIndex.CLOSE] <= row[PriceIndex.OPEN])
    if bearish_count < weak_bars:
        return False

    if need_consecutive_weak_close and len(window_1m) >= 2:
        closes = [row[PriceIndex.CLOSE] for row in window_1m]
        if not all(closes[idx] <= closes[idx - 1] for idx in range(1, len(closes))):
            return False

    return True


# ════════════════════════════════════════════════════════
# 에너지 응축 점수 계산
# ════════════════════════════════════════════════════════
def _calc_energy_score(data: list, i: int, cfg: dict) -> Dict[str, Any]:
    """최근 48봉 기준 에너지 응축 점수 계산."""
    pi     = cfg["priceIndex"]
    start  = max(0, i - 48)
    window = data[start : i + 1]

    highs  = [b[pi["high"]]   for b in window]
    lows   = [b[pi["low"]]    for b in window]
    closes = [b[pi["close"]]  for b in window]
    vols   = [b[pi["volume"]] for b in window]

    box_high  = max(highs)
    box_low   = min(lows)
    mid       = (box_high + box_low) / 2 if (box_high + box_low) > 0 else 1
    box_range = (box_high - box_low) / mid

    ma5  = sum(closes[-5:])  / 5  if len(closes) >= 5  else closes[-1]
    ma20 = sum(closes[-20:]) / 20 if len(closes) >= 20 else closes[-1]
    ma_diff = abs(ma5 - ma20) / ma20 if ma20 else 0

    recent_vol_avg     = sum(vols[-5:])    / 5  if len(vols) >= 5  else vols[-1]
    older_vol_avg      = sum(vols[-20:-5]) / 15 if len(vols) >= 20 else recent_vol_avg
    is_vol_contracting = recent_vol_avg < older_vol_avg * 0.7

    score = 0
    if box_range < 0.05:    score += 30
    if ma_diff   < 0.02:    score += 30
    if is_vol_contracting:  score += 40

    return {
        "score":            score,
        "boxRange":         box_range,
        "maDiff":           ma_diff,
        "isVolContracting": is_vol_contracting,
        "breakoutLine":     box_high,
        "isBreakout":       False,
    }


# ════════════════════════════════════════════════════════
# 이동평균
# ════════════════════════════════════════════════════════
def _ma(data: list, i: int, period: int, close_idx: int) -> Optional[float]:
    """인덱스 i 기준 SMA. 데이터 부족 시 None."""
    if i < period - 1:
        return None
    return sum(data[k][close_idx] for k in range(i - period + 1, i + 1)) / period


def _ema(series, period):
    if len(series) < period:
        return None
    k = 2 / (period + 1)
    ema = series[0]
    for price in series[1:]:
        ema = price * k + ema * (1 - k)
    return ema


def calc_ma(data: list, i: int, period: int, close_idx: int) -> Optional[float]:
    """외부 공개용 MA 계산 함수."""
    return _ma(data, i, period, close_idx)


# ════════════════════════════════════════════════════════
# 보조 함수
# ════════════════════════════════════════════════════════
def _calc_avg_vol(data: list, i: int, n: int, vol_idx: int) -> float:
    """직전 n봉 평균 거래량 (현재봉 미포함)."""
    start = max(0, i - n)
    slc   = data[start:i]
    return sum(b[vol_idx] for b in slc) / len(slc) if slc else 0.0


def _calc_gap_from_open(data: list, i: int, close: float, pi: dict) -> float:
    """당일 시가 대비 현재가 상승률 (%)."""
    today = str(data[i][pi["time"]]).split("_")[0]
    for k in range(i + 1):
        if str(data[k][pi["time"]]).split("_")[0] == today:
            day_open = data[k][pi["open"]]
            return (close - day_open) / day_open * 100 if day_open else 0.0
    return 0.0


def _check_steady_rising(data: list, i: int, n: int, close_idx: int) -> bool:
    """최근 n봉 연속 상승 여부."""
    if i < n:
        return False
    for k in range(i - n + 1, i + 1):
        if data[k][close_idx] <= data[k - 1][close_idx]:
            return False
    return True


def _find_last_swing_high(data: list, i: int, high_idx: int) -> float:
    """좌우 2봉 대비 피크인 마지막 스윙 고점. 없으면 -inf."""
    for k in range(i - 2, 1, -1):
        if k + 2 >= len(data):
            continue
        h = data[k][high_idx]
        if (h > data[k - 1][high_idx]
                and h > data[k - 2][high_idx]
                and h > data[k + 1][high_idx]
                and h > data[k + 2][high_idx]):
            return h
    return None
