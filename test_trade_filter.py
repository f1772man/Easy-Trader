"""
거래대금 + 갭 조건 테스트 스크립트
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

사용법:
  docker exec -it easy-trader python3 test_trade_filter.py

기능:
  - target_stocks 전체 종목 조회
  - 거래대금 + 갭 조건 필터링
  - 상위 10개 선정 결과 출력
  - 실제 저장은 하지 않음 (테스트 전용)
"""

import os
import time
from trader.kis_auth import auth, _url_fetch

# ── 설정 ──────────────────────────────────────────
GAP_MIN_PCT     = 1.0   # 시가갭 하한 (%)
GAP_MAX_PCT     = 15.0  # 시가갭 상한 (%) — 과열 제외
CURRENT_MIN_PCT = 0.5   # 현재갭 하한 (%) — 갭 유지 확인
TOP_N           = 10    # 선정 종목 수
API_INTERVAL    = 0.2   # API 호출 간격 (초)

# ── KIS 인증 ──────────────────────────────────────
print("[인증] KIS 인증 시작...")
auth(svr=os.environ.get('KIS_SVR', 'prod'))
print("[인증] 완료\n")

# ── Firestore에서 strategy_results 읽기 ──────────
from google.cloud import firestore
db = firestore.Client()

print("[strategy_results] 읽기 시작...")
docs = list(db.collection('strategy_results').stream())
tickers = []
for doc in docs:
    data = doc.to_dict()
    ticker = data.get('code', '').strip()
    name   = data.get('name', ticker)
    badge  = data.get('summaryBadge', '')
    if ticker and badge == '진입':
        tickers.append({'ticker': ticker, 'name': name})

print(f"[strategy_results] 진입 후보 {len(tickers)}개 종목 로드\n")

# ── 종목별 조회 ────────────────────────────────────
print(f"{'='*60}")
print(f"{'종목':<12} {'시가갭':>7} {'현재갭':>7} {'갭유지':>6} {'거래대금':>12} {'통과':>5}")
print(f"{'='*60}")

results   = []
fail_count = 0
skip_count = 0

for item in tickers:
    ticker = item['ticker']
    name   = item['name']

    try:
        res = _url_fetch(
            '/uapi/domestic-stock/v1/quotations/inquire-price',
            'FHKST01010100', '',
            {'FID_COND_MRKT_DIV_CODE': 'J', 'FID_INPUT_ISCD': ticker},
        )
        time.sleep(API_INTERVAL)

        if not res.isOK():
            fail_count += 1
            continue

        output = res.getBody().output
        stck_prpr    = int(output.get('stck_prpr',    0) or 0)  # 현재가
        stck_oprc    = int(output.get('stck_oprc',    0) or 0)  # 시가
        stck_sdpr    = int(output.get('stck_sdpr',    0) or 0)  # 전일종가
        acml_tr_pbmn = int(output.get('acml_tr_pbmn', 0) or 0)  # 거래대금

        if stck_sdpr <= 0:
            fail_count += 1
            continue

        gap_pct     = round((stck_oprc - stck_sdpr) / stck_sdpr * 100, 2)
        current_pct = round((stck_prpr - stck_sdpr) / stck_sdpr * 100, 2)
        gap_hold    = stck_prpr >= stck_oprc  # 현재가 >= 시가

        # 필터 조건
        pass_gap_min     = gap_pct >= GAP_MIN_PCT
        pass_gap_max     = gap_pct <= GAP_MAX_PCT
        pass_current_min = current_pct >= CURRENT_MIN_PCT
        pass_gap_hold    = gap_hold
        passed = pass_gap_min and pass_gap_max and pass_current_min and pass_gap_hold

        pbmn_bil = acml_tr_pbmn / 1e8
        label    = f"{name}({ticker})"

        print(
            f"{label:<12} "
            f"{gap_pct:>+6.1f}% "
            f"{current_pct:>+6.1f}% "
            f"{'O' if gap_hold else 'X':>6} "
            f"{pbmn_bil:>10.0f}억 "
            f"{'✅' if passed else '❌':>5}"
        )

        if passed:
            item['tr_pbmn']     = acml_tr_pbmn
            item['gap_pct']     = gap_pct
            item['current_pct'] = current_pct
            item['price']       = stck_prpr
            results.append(item)
        else:
            skip_count += 1

    except Exception as e:
        fail_count += 1
        print(f"{ticker:<12} 오류: {e}")

print(f"{'='*60}\n")

# ── 거래대금 정렬 → 상위 TOP_N 선정 ──────────────
results.sort(key=lambda x: x['tr_pbmn'], reverse=True)
selected = results[:TOP_N]

# ── 결과 출력 ──────────────────────────────────────
print(f"[결과] 전체 {len(tickers)}개 → 필터 통과 {len(results)}개 → 선정 {len(selected)}개")
print(f"[결과] 실패 {fail_count}개 / 조건 미달 {skip_count}개\n")

print(f"{'='*60}")
print(f"▶ 최종 선정 종목 (거래대금 상위 {TOP_N}개)")
print(f"{'='*60}")
for i, item in enumerate(selected, 1):
    pbmn_bil = item['tr_pbmn'] / 1e8
    print(
        f"{i:>2}위 {item['name']}({item['ticker']}) "
        f"| 시가갭 {item['gap_pct']:+.1f}% "
        f"| 현재갭 {item['current_pct']:+.1f}% "
        f"| 거래대금 {pbmn_bil:.0f}억"
    )

if not selected:
    print("  선정된 종목 없음 — GAP_MIN_PCT 기준 조정 필요")
    print(f"  현재 기준: 시가갭 >= {GAP_MIN_PCT}%, 현재갭 >= {CURRENT_MIN_PCT}%")

print(f"{'='*60}")
