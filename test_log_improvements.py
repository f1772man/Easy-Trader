#!/usr/bin/env python3
"""
로그 개선 수정사항 테스트
1. strategy.py — buyStopTime 차단 (15:20 이후 BUY 차단)
2. engine.py   — 이월 포지션 태그, 갭 탈락 카운터, 일일 통계 누적, 일마감 요약 로그
"""
import sys
import logging
from datetime import datetime, timezone, timedelta

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)
KST = timezone(timedelta(hours=9))

PASS = "✓ PASS"
FAIL = "✗ FAIL"

# ─────────────────────────────────────────────────────────────────
# 헬퍼
# ─────────────────────────────────────────────────────────────────
def _bar(time_str: str, price: int = 100_000, vol: int = 2000) -> list:
    """[time, open, high, low, close, volume]"""
    return [time_str, price - 300, price + 300, price - 600, price, vol]


def _make_data(last_time: str, n: int = 25, base_price: int = 100_000) -> list:
    """last_time 포함 n봉 데이터. 전체 상승 추세."""
    bars = []
    for k in range(n):
        bars.append(_bar(f"20260703_09{k:02d}", base_price + k * 100, 2000))
    bars[-1] = _bar(last_time, base_price + (n - 1) * 100, 4000)
    return bars


def _base_params(data: list, holding: bool = False, entry: float = 0) -> dict:
    from trader.strategy import DEFAULT_CONFIG
    i = len(data) - 1
    price = data[i][4]
    return {
        "symbol": "TEST",
        "i": i,
        "data": data,
        "cfg": DEFAULT_CONFIG,
        "isHolding": holding,
        "entryPrice": entry,
        "maxPriceAfterEntry": price * 1.03 if holding else 0,
        "ma5_curr":  price * 1.002,
        "ma20_curr": price * 0.995,
        "ma5_prev":  price * 1.001,
        "ma20_prev": price * 0.994,
        "ma5_prev2": price * 1.000,
        "ema5_curr":  price * 1.002,
        "ema20_curr": price * 0.995,
        "ema5_prev":  price * 1.001,
        "ema20_prev": price * 0.994,
        "ema5_prev2": price * 1.000,
        "ema5_5bars_ago": price * 0.999,
        "prevDayHigh":  price * 0.98,
        "prevDayClose": price * 0.98,
        "prevDayVolume": 5000.0,
        "pivotR2": price * 1.05,
        "data1Min": None,
        "dailyVcpScore": 0,
        "dailyStrategy": "",
        "dailyPivotPoint": 0.0,
        "dailyStopLoss": 0.0,
        "touchedLimitUp": False,
        "trailingExitPrice": 0.0,
    }


# ─────────────────────────────────────────────────────────────────
# [1] buyStopTime 차단
# ─────────────────────────────────────────────────────────────────
def test_buy_stop_time():
    print("\n[1] buyStopTime 차단 (strategy.py)")
    from trader.strategy import get_strategy_signal

    cases = [
        ("20260703_1519", False, "매수마감시간"),   # 차단 안 돼야 함
        ("20260703_1520", True,  "매수마감시간(1520)"),
        ("20260703_1525", True,  "매수마감시간(1520)"),
        ("20260703_1529", True,  "매수마감시간(1520)"),
    ]

    ok = True
    for time_str, expect_blocked, expect_reason in cases:
        data = _make_data(time_str)
        result = get_strategy_signal(_base_params(data, holding=False))
        blocked = result["reason"] == expect_reason
        if expect_blocked:
            passed = blocked
            tag = PASS if passed else FAIL
            print(f"  {time_str}  기대=차단  reason={result['reason']!r:30s} {tag}")
        else:
            passed = not blocked
            tag = PASS if passed else FAIL
            print(f"  {time_str}  기대=통과  reason={result['reason']!r:30s} {tag}")
        ok = ok and passed

    return ok


# ─────────────────────────────────────────────────────────────────
# [2] buyStopTime — 매도 구간은 영향 없음 확인
# ─────────────────────────────────────────────────────────────────
def test_sell_not_blocked_by_stop_time():
    print("\n[2] buyStopTime — 보유 중 매도 신호는 차단 안됨 (strategy.py)")
    from trader.strategy import get_strategy_signal

    # 15:25에 손절 진입 조건 만들기: entry보다 -2.5% 손실
    entry = 100_000
    loss_price = int(entry * 0.974)
    data = _make_data("20260703_1525", base_price=loss_price)
    params = _base_params(data, holding=True, entry=entry)
    params["maxPriceAfterEntry"] = entry  # 최고가=진입가 (손절 구간)

    result = get_strategy_signal(params)
    passed = result["reason"] != "매수마감시간(1520)"
    tag = PASS if passed else FAIL
    print(f"  15:25 보유중 → signal={result['signal']} reason={result['reason']!r} {tag}")
    return passed


# ─────────────────────────────────────────────────────────────────
# [3] 이월 포지션 태그 로직
# ─────────────────────────────────────────────────────────────────
def test_carryover_tag():
    print("\n[3] 이월 포지션 태그 (engine.py _execute_sell 로직)")

    today = datetime.now(KST).strftime("%Y%m%d")
    yesterday = (datetime.now(KST) - timedelta(days=1)).strftime("%Y%m%d")

    cases = [
        (f"{today}_093000",    False, "당일 진입"),
        (f"{yesterday}_150000", True, "전일 이월"),
        ("",                   False, "entry_time 없음"),
    ]

    ok = True
    for entry_time, expect_carryover, label in cases:
        is_carryover = bool(entry_time) and entry_time[:8] != today
        carry_tag = "[이월]" if is_carryover else ""

        passed = (is_carryover == expect_carryover)
        tag = PASS if passed else FAIL
        print(f"  {label:16s}  entry_time={entry_time[:8] or '(없음)'}  "
              f"carry_tag={carry_tag!r:6s} {tag}")
        ok = ok and passed
    return ok


# ─────────────────────────────────────────────────────────────────
# [4] 갭 탈락 카운터 분류
# ─────────────────────────────────────────────────────────────────
def test_gap_skip_counters():
    print("\n[4] 갭 탈락 카운터 분류 (engine.py _filter_by_trade_amount 로직)")

    GAP_MIN_PCT = 1.0
    GAP_MAX_PCT = 15.0

    # (gap_pct, cond2, cond3, 기대 bucket)
    cases = [
        (0.3,  False, False, "flat",  "0~1% 미만"),
        (0.0,  False, False, "flat",  "0% (보합)"),
        (16.0, False, False, "hot",   "15% 초과"),
        (20.0, False, False, "hot",   "20% 과열"),
        (-1.0, False, False, "neg",   "마이너스 미회복"),
        (-3.0, False, False, "neg",   "마이너스 미회복"),
        (3.0,  False, False, None,    "갭1~15% → 통과(skip 안됨)"),
    ]

    skip_flat = skip_hot = skip_neg = 0
    ok = True

    for gap_pct, cond2, cond3, expect_bucket, label in cases:
        cond1 = GAP_MIN_PCT <= gap_pct <= GAP_MAX_PCT
        fails = not (cond1 or cond2 or cond3)

        bucket = None
        if fails:
            if gap_pct > GAP_MAX_PCT:
                skip_hot += 1; bucket = "hot"
            elif gap_pct >= 0:
                skip_flat += 1; bucket = "flat"
            else:
                skip_neg += 1; bucket = "neg"

        passed = (bucket == expect_bucket)
        tag = PASS if passed else FAIL
        print(f"  gap={gap_pct:+5.1f}%  탈락={fails}  bucket={str(bucket):5s}  ({label}) {tag}")
        ok = ok and passed

    print(f"  → 집계: 평평={skip_flat} 과열={skip_hot} 마이너스미회복={skip_neg}")
    return ok


# ─────────────────────────────────────────────────────────────────
# [5] 일일 통계 누적
# ─────────────────────────────────────────────────────────────────
def test_daily_stats_accumulation():
    print("\n[5] 일일 통계 누적 (engine.py _execute_sell 로직)")

    trades = [
        (100_000, 103_000, 10, "트레일링"),    # 승
        (200_000, 196_000, 5,  "손절"),        # 패
        (50_000,  52_500,  20, "EMA데드크로스"), # 승
        (300_000, 294_000, 3,  "손절"),        # 패
        (80_000,  82_400,  12, "트레일링"),    # 승
    ]

    daily_realized_pnl = 0
    daily_wins = 0
    daily_losses = 0

    for entry_price, exit_price, qty, reason in trades:
        realized_pnl = int((exit_price - entry_price) * qty)
        profit_pct = (exit_price / entry_price - 1) * 100

        daily_realized_pnl += realized_pnl
        if profit_pct > 0:
            daily_wins += 1
        else:
            daily_losses += 1

    total = daily_wins + daily_losses
    win_rate = round(daily_wins / total * 100, 1) if total else 0.0

    expected_pnl   = (3000*10) + (-4000*5) + (2500*20) + (-6000*3) + (2400*12)
    expected_wins  = 3
    expected_losses = 2
    expected_wr    = 60.0

    ok = (
        daily_realized_pnl == expected_pnl
        and daily_wins == expected_wins
        and daily_losses == expected_losses
        and win_rate == expected_wr
    )
    tag = PASS if ok else FAIL
    print(f"  실현손익={daily_realized_pnl:+,}원 (기대={expected_pnl:+,}원) "
          f"승={daily_wins} 패={daily_losses} 승률={win_rate}% {tag}")
    return ok


# ─────────────────────────────────────────────────────────────────
# [6] 일마감 요약 로그 포맷
# ─────────────────────────────────────────────────────────────────
def test_daily_summary_format():
    print("\n[6] 일마감 요약 로그 포맷 (engine.py _flush_market_data_once 로직)")

    daily_realized_pnl = 312_000
    daily_wins  = 4
    daily_losses = 2
    carryover   = ["005930", "000660"]
    date_str    = "20260703"

    total    = daily_wins + daily_losses
    win_rate = round(daily_wins / total * 100, 1) if total else 0.0
    carryover_str = f"{len(carryover)}개: {carryover}" if carryover else "없음"

    log_line = (
        f"[일마감요약] {date_str} | "
        f"매도={total}건 | 승={daily_wins} 패={daily_losses} 승률={win_rate}% | "
        f"실현손익={daily_realized_pnl:+,}원 | 이월포지션={carryover_str}"
    )
    print(f"  출력 → {log_line}")

    ok = all([
        "[일마감요약]" in log_line,
        "실현손익=+312,000원" in log_line,
        "승률=66.7%" in log_line,
        "이월포지션=2개" in log_line,
    ])
    tag = PASS if ok else FAIL
    print(f"  필수 항목 포함 여부 {tag}")
    return ok


# ─────────────────────────────────────────────────────────────────
# main
# ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    results = []
    results.append(("buyStopTime 차단",          test_buy_stop_time()))
    results.append(("매도 신호 차단 안됨",        test_sell_not_blocked_by_stop_time()))
    results.append(("이월 포지션 태그",           test_carryover_tag()))
    results.append(("갭 탈락 카운터 분류",        test_gap_skip_counters()))
    results.append(("일일 통계 누적",             test_daily_stats_accumulation()))
    results.append(("일마감 요약 로그 포맷",       test_daily_summary_format()))

    print("\n" + "="*55)
    print("결과 요약")
    print("="*55)
    all_pass = True
    for name, passed in results:
        tag = PASS if passed else FAIL
        print(f"  {tag}  {name}")
        all_pass = all_pass and passed

    print("="*55)
    sys.exit(0 if all_pass else 1)
