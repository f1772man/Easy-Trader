# Changelog

## [Unreleased] — 2026-07-03

### Bug Fix
- **strategy.py** `buyStopTime` 차단 로직 구현
  - `DEFAULT_CONFIG`에 선언된 `buyStopTime: 1520`이 실제로 체크되지 않던 버그 수정
  - `get_strategy_signal()` BUY 섹션 초입에 `hm_int >= buyStopTime` 조건 추가
  - 15:20 이후 BUY 신호 원천 차단 → 장마감 직전 이월 포지션 발생 방지
  - 보유 중 매도 신호(SELL)는 영향 없음
  - 근거: 06/09 15:19 브이엠, 06/22 15:03~15:14 4종목 매수 → 익일 전량 손절

### Improvement — 로그 분석 강화
- **engine.py** `_execute_sell` — 이월 포지션 태그 및 실현손익 일관 출력
  - `entry_time` 날짜가 당일과 다르면 `[SELL][이월]` 태그 추가
  - `realized_pnl` (원화 절대금액) 을 SELL 로그에 항상 포함
  - 기존: `profit=2.43%` → 변경: `realized_pnl=+56,000원 profit=+2.43%`
  - 체결 성공 시 `_daily_realized_pnl`, `_daily_wins`, `_daily_losses` 누적

- **engine.py** `_process_symbol` — BUY/SELL 신호 로그 지표값 추가
  - BUY 신호 발생 시 `dist_ema5`, `vol_ratio`, `energy score` 함께 출력
  - SELL 신호 발생 시 `cur_profit`, `peak_profit` 함께 출력
  - 의미 있는 HOLD 차단 사유(EMA하회·과열·수급부족·마감시간 등) INFO 레벨로 출력

- **engine.py** `_filter_by_trade_amount` — 갭 탈락 종목 3종 분류
  - 기존: `skip_count`(합산) 만 로그
  - 변경: `평평(갭 0~1%)` / `과열(갭 >15%)` / `마이너스미회복` 3가지로 분류 집계

- **engine.py** `_flush_market_data_once` — 일마감 요약 로그 추가
  - 장 마감(15:31) CSV flush 시점에 `[일마감요약]` 1줄 출력
  - 포함 항목: 날짜, 매도 건수, 승/패 수, 승률, 실현손익 합계, 이월 포지션 목록

- **engine.py** `__init__` / `_rollover_if_needed`
  - 일일 통계 변수(`_daily_realized_pnl`, `_daily_wins`, `_daily_losses`) 추가
  - 날짜 변경(자정 롤오버) 시 자동 리셋

### Test
- `test_log_improvements.py` 추가
  - buyStopTime 차단 / 보유 중 매도 미차단 / 이월 태그 / 갭 카운터 / 일일 통계 / 일마감 요약 포맷 6종 검증
  - 외부 의존성(KIS API, Firebase) 없이 실행 가능

---

## 이전 변경 이력

### feat(engine): RVT(상대 거래량) 기반 B안 종목 선정 병행 산출
- 거래대금 0.7 + 상대강도 0.3 기존 A안 유지
- RVT 0.625 + 상대강도 0.375 B안 병행 산출 및 target_stocks에 함께 저장
- `USE_RVT_SCORING` 환경변수로 운영 정렬 전환 (기본 false = A안)

### fix: 장내 재시작 시 거래대금 필터 재선정 방지
- 재시작 시 `target_stocks`에 당일 데이터 존재 여부 확인 후 재선정 스킵

### feat: 마켓게이트 phase2 + 거래대금·상대강도 복합 종목 선정
- 09:01~09:10 KOSPI+KOSDAQ 실시간 등락률로 scan_hm / market_top_n 보정
- 양시장 동반급락 시 강제 -3 클램핑

### feat(engine): 마켓 게이트 기반 동적 스캔 타이밍 도입
- Firestore `market_analysis/latest` Phase1 읽기 (08:50)
- gate(BULL/BEAR/NEUTRAL) + confidence로 scan_hm·market_top_n 잠정 결정
