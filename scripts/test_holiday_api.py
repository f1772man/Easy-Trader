#!/usr/bin/env python3
"""
test_prev_trading_date.py
get_prev_trading_date 수정 검증 스크립트

실행 방법: docker exec easy-trader python scripts/test_prev_trading_date.py
"""
import sys
import os
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from trader.kis_auth import auth

def main():
    print("=" * 60)
    print("get_prev_trading_date 수정 검증")
    print("=" * 60)

    svr = os.environ.get("KIS_SVR", "prod")
    print(f"\n[1] 인증 시작 (svr={svr})")
    auth(svr=svr)
    print("    인증 완료")

    from trader.kis_api import get_prev_trading_date

    kst = timezone(timedelta(hours=9))
    today = datetime.now(kst).strftime("%Y%m%d")

    # ── 테스트 케이스 ────────────────────────────────────
    test_cases = [
        (today,       "오늘 기준 직전 거래일"),
        ("20260528",  "0528 기준 (수동검증: 20260527 기대)"),
        ("20260527",  "0527 기준 (수동검증: 20260522 기대)"),
        ("20260501",  "월초 기준 (연휴 넘어 전월 거래일)"),
        ("20260302",  "3월 초 기준"),
    ]

    print(f"\n[2] 테스트 실행")
    print(f"{'─' * 60}")

    all_pass = True
    for base_date, desc in test_cases:
        result = get_prev_trading_date(base_date)
        # 결과 검증: 반드시 base_date 미만이어야 함
        if result and result < base_date:
            status = "✅"
        elif not result:
            status = "⚠️ 빈값"
            all_pass = False
        else:
            status = f"❌ 오류 (base_date={base_date} 이상)"
            all_pass = False

        print(f"  {status} {desc}")
        print(f"     base_date={base_date} → prev={result}")

    print(f"\n{'─' * 60}")
    if all_pass:
        print("✅ 전체 통과")
    else:
        print("❌ 일부 실패 — 위 결과 확인 필요")
    print("=" * 60)

if __name__ == "__main__":
    main() 