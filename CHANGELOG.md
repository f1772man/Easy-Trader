# CHANGELOG

---

## [2026-04-21]

### 수정

* `strategy.py` — 조건5-A `1분봉EMA골든+거래량폭발` 진입 필터 강화

  * 기존: 1분봉 EMA5/EMA20 골든크로스 + 거래량 5배 시 즉시 진입
  * 변경: 거래량 10배로 수정

* `strategy.py` — 조건5 `1분봉지속상승` 진입 조건 강화

  * 기존: 1분봉 n개 연속 상승 시 진입
  * 변경: volume > avg_vol * 5 거래량 5배로     

* `strategy.py` — 매수 공통 필터 추가

  * 모든 BUY 조건 앞단에 추세 필터 적용

    * `close > ema20_curr and ema5_curr > ema20_curr`
  * 하락 추세 구간 진입 차단

* `strategy.py` — 조건1-B `GC+거래량급증` 거래량 기준 강화

  * 기존: `vol_ratio >= 2`
  * 변경: `vol_ratio >= 3`
  * 중간 구간 진입 감소 및 신호 정밀도 개선

* `strategy.py` — 조건4 `거래량폭발` 진입 민감도 완화

  * MA 기울기 조건 강화 (`ma5_slope_pct 상향`)
  * 단기 노이즈 상승 진입 억제

---

### 매도 로직 개선

* `strategy.py` — EMA 데드크로스 조건 고도화

  * 기존: 단순 교차 발생 시 매도

    ```python
    ema5_prev >= ema20_prev and ema5_curr < ema20_curr
    ```
  * 변경: 이격도 + 기울기 + 지속성 기반 확정 교차로 변경

    * 교차 발생 (`diff_prev >= 0 and diff_curr < 0`)
    * 이격도 (`gap_pct >= deadCrossGapPct`)
    * EMA5 하락 기울기 (`ema5_curr < ema5_prev < ema5_prev2`)
    * EMA20 상승 중단 (`ema20_curr <= ema20_prev`)
    * 종가 EMA20 하회 (`close < ema20_curr`)
  * 미세 교차 및 노이즈 매도 제거

* `strategy.py` — EMA20 이탈 매도 조건 완화

  * 기존: 1회 이탈 시 즉시 매도
  * 변경: 2회 연속 이탈 또는 조건 강화 기반 매도 (권장 구조)
  * 휩쏠림 구간에서 과도한 손절 방지

* `strategy.py` — 1분봉 약세 기반 빠른 청산 조건 완화

  * 기존: `interval_drop_pct <= -0.8`
  * 변경: 손실 조건 및 하락폭 기준 강화

    * `current_profit < -1.5`
    * `interval_drop_pct <= -1.2`
  * 단기 변동성에 의한 조기 청산 감소

---

### 설계 변경

* 진입 로직 구조 재정의

  * 기존: 1분봉 기반 즉시 진입 중심
  * 변경:

    * 5분봉 추세 필터 기반 진입
    * 1분봉은 보조 신호로 역할 축소

* 매도 로직 구조 개선

  * 기존: 이벤트 기반 (교차/이탈 즉시 반응)
  * 변경:

    * 추세 기반 (이격도 + 기울기 + 지속성)
    * 노이즈 제거 중심

---

### 효과

* 동일 종목 반복 매매 (재진입/재청산) 감소
* 노이즈 구간 진입 차단
* 추세 기반 매매로 안정성 향상
* 실전 체결 품질 개선

---

### 주의사항

* 진입 신호 빈도 감소 (정상적인 변화)
* 초기 수익 기회 일부 감소 가능 → 대신 손실 감소 효과 기대
* `deadCrossGapPct`, `vol_ratio` 등 파라미터는 시장 상황에 따라 튜닝 필요

---


### 추가
- `ws_tick_collector.py` — `_subscribe_symbol()`, `_unsubscribe_symbol()` 함수 추가  
  - WebSocket 연결 유지 상태에서 종목별 구독/해제 메시지 전송 기능 구현  
- `ws_tick_collector.py` — `_ws`, `_loop`, `_connected_event` 상태 변수 추가  
  - 런타임 중 WebSocket 핸들 및 이벤트 루프 접근을 위한 구조 개선  

### 수정
- `ws_tick_collector.py` `update_symbols()` — stop/start 방식 제거  
  - 기존: `stop() → _symbols 교체 → start()`  
  - 변경: **증분 구독 방식 (added / removed 기준)**  
    - 추가 종목 → subscribe 메시지 전송  
    - 제거 종목 → unsubscribe 메시지 전송  
- `ws_tick_collector.py` `_ws_main()` — 재연결 시 현재 `_symbols` 기준 전체 재구독  
  - WebSocket 재연결 발생 시 감시 종목 누락 방지  

- `engine.py` `_reload_watch_symbols()` — collector 호출 구조 수정  
  - 기존: `update_symbols()` 최대 2회 호출 (중간 상태 + 최종 상태)  
  - 변경: 최종 감시 목록 구성 후 **1회만 호출**  

### 설계 변경
- WebSocket 구독 방식 변경  
  - 기존:  
    - 초기 연결 시 전체 종목 일괄 구독  
    - 종목 변경 시 전체 재시작 (stop/start)  
  - 변경:  
    - 연결 유지 상태에서 종목별 증분 구독  
    - WebSocket 재연결 시에만 전체 재구독  

- 감시 종목 갱신 흐름 개선  
  - 기존:  
    - `new_symbols` 적용 → collector 반영  
    - `force_added` 적용 → collector 재반영  
  - 변경:  
    - `final_symbols = new_symbols + force_added`  
    - collector 1회 반영  

### 주의사항
- unsubscribe `tr_type` 값 확인 필요  
  - 일부 코드: `"0"`  
  - 일부 코드: `"2"`  
  - 실제 KIS WebSocket 동작 기준 확인 필요  

- 초기 적용 시 안정성을 위해 단계적 적용 권장  
  - 1차: 추가 종목 subscribe만 적용  
  - 2차: unsubscribe 적용  

- 기존 구조 대비 실시간 민감도 증가  
  - 잘못된 unsubscribe 적용 시 체결 데이터 누락 가능  

---

## [2026-04-20]

### 추가
- `strategy.py` — 조건5-A `1분봉EMA골든+거래량폭발` 매수 조건 추가  
  - 1분봉 기준 EMA5/EMA20 골든크로스 발생 시점 포착  
  - 거래량이 최근 5~10봉 평균 대비 5배 이상일 때 매수 트리거  
- `strategy.py` — `_ema()` 함수 추가  
  - 리스트 기반 EMA 계산 로직 구현 (1분봉 전용, pandas 의존성 없음)  
- `strategy.py` — `avg_vol_1m` 계산 로직 추가  
  - 1분봉 최근 5~10봉 평균 거래량 계산용  

### 수정
- `strategy.py` — 1분봉 거래량 폭발 조건 비교 기준 수정  
  - 기존: `volume (5분봉)` vs `avg_vol (1분봉)` → 기준 불일치  
  - 수정: `vols_1m[-1]` 기준으로 동일 1분봉 데이터 비교  
- `strategy.py` — 매수 조건 평가 순서 조정  
  - 조건5-A(`1분봉EMA골든+거래량폭발`)를 기존 조건5(`1분봉지속상승`)보다 우선 적용  

### 설계 변경
- 매수 로직 계층 분리  
  - 기존: 5분봉 기반 단일 진입 구조  
  - 변경:  
    - 추세형: GC / VCP / 에너지  
    - 모멘텀형: 1분봉 EMA + 거래량 폭발 (조건5-A)  
- 1분봉 활용 범위 확장  
  - 기존: 매도/보조 판단  
  - 변경: 초기 진입 트리거로 확장  

### 주의사항
- 신규 조건은 급등 초입 포착 목적 → 신호 빈도 증가 가능  
- 단독 사용 시 노이즈 증가 → 기존 EMA20 필터 유지 권장  

---

## [2026-04-13]

### 추가
- `engine.py` `_today_sold` — 타입 `float` → `tuple(timestamp, reason, exit_price)`: 청산 사유·가격 함께 저장
- `engine.py` `_can_reenter()` — 반환 타입 `bool` → `tuple[bool, float]`: 트레일링 청산 시 쿨다운 제거, `trailing_exit_price` 반환
- `engine.py` `_process_symbol()` — `_can_reenter()` 호출을 `get_strategy_signal()` 전으로 이동: `trailingExitPrice` params 주입
- `strategy.py` — `trailingExitPrice` 파라미터 수신: 트레일링 청산 후 재진입 여부 판별
- `strategy.py` — `trailing_reentry_price_ok` / `trailing_reentry_ema_ok` 변수 추가: 가격 상승폭(기본 1.5%) 및 EMA5 하락 여부로 재진입 차단
- `strategy.py` — `vol_ratio` 변수 추가: `거래량폭발`, `GC+거래량급증` reason에 실제 배율 표기 (예: `거래량폭발(3.2배)`)

### 수정
- `strategy.py` 조건1-B `GC+거래량급증` — `volume > avg_vol * 2` → `vol_ratio >= 2` + 트레일링 재진입 필터 적용
- `strategy.py` 조건4 `거래량폭발` — 트레일링 재진입 필터 적용 + reason에 배율 추가
- `strategy.py` 조건5 `1분봉지속상승` — 트레일링 재진입 필터 적용
- `strategy.py` 조건6 `에너지응축돌파` — 트레일링 재진입 필터 적용
- `engine.py` `_restore_positions()` — `_today_sold` 복원 시 튜플 형식으로 통일: `(time.time(), "복원", 0)`

---

## [2026-04-09]

### 추가
- `engine.py` — `_normalize_name()` 함수 추가: 기존 Firebase에 `"종목명(코드)"` 형태로 저장된 데이터 정규화
- `engine.py` — `_load_symbol_data()` 공통 로더 추출: `_preload_symbols_data()`와 `_warmup_market_data()` 내부 중복 `_load()` 통합
- `engine.py` — `_stop_loss_pending: dict[str, str]` 추가: 손절 2봉 확인을 위한 대기 상태 관리
- `engine.py` `_maybe_premarket_warmup()` — 워밍업 전 `_reload_watch_symbols()` 호출 추가: 배치 갱신(08:32) 반영
- `engine.py` `_reload_watch_symbols()` — `force_added` 종목 캐시 로드 추가: 보유 포지션 강제 편입 시 snapshot/strategy 즉시 로드
- `engine.py` `_rollover_if_needed()` — `_stop_loss_pending.clear()` 추가

### 수정
- `engine.py` `_display_name()` — `_positions_lock` 재진입 데드락 제거: lock 없이 `_symbol_meta`만 참조하도록 변경
- `engine.py` `_restore_positions()` — 로그를 `_positions_lock` 블록 밖으로 이동: 데드락 방지
- `engine.py` `_execute_sell()` — `_normalize_name()` 적용: 이중표기(`"종목명(코드)(코드)"`) 방지
- `engine.py` `_process_symbol()` — `ws_1min[-1][2]` → `ws_1min[-1][4]`: 고가 기준 → 종가 기준으로 주문가 수정
- `engine.py` `_process_symbol()` — 첫 체결 전 종목 분석 차단: `ws_1min`과 `completed_5m` 모두 없으면 즉시 return
- `engine.py` `_process_symbol()` — 손절 2봉 확인 로직: `reason == "손절대기"` 시 다음 1분봉 종가 확인 후 매도
- `engine.py` `_warmup_market_data()` — `_premarket_warmup_done_date` 세트 복구: 캐시 전체 존재 시 및 전체 성공 시 세트
- `strategy.py` `[C] 손절` — `data_1min[-2]` 직전봉 비교 로직 제거: engine의 `_stop_loss_pending`으로 위임

### 제거
- `engine.py` — `_preload_symbols_data()` 제거: `_warmup_market_data()`로 통합
- `engine.py` `_rollover_if_needed()` — `_reload_watch_symbols()` 호출 제거: 자정 시점 Firestore 미갱신 상태에서 불필요한 호출

### 보류
- `engine.py` — 상한가 이탈 후 당일 재매수 영구 차단 (`float("inf")` 방식)