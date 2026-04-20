# CHANGELOG

---

## [2026-04-20]

### 수정
- `ws_tick_collector.py` `update_symbols()` — 단순 리스트 변경 → collector 재시작 방식으로 변경
  - 기존: `_symbols` 값만 변경 (실제 WS 구독 미반영)
  - 변경: `stop()` → `_symbols` 교체 → `start()` 재시작으로 WS 재구독 수행
  - 효과: 종목 갱신 시 기존 종목 수집 문제 해결 및 신규 종목 정상 반영

### 개선
- 종목 변경 시 1분/5분 버퍼 초기화 추가
  - 기존 데이터와 신규 종목 데이터 혼합 방지

---

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

### 추가
- `strategy.py` — 트레일링 스탑에 1분봉 EMA5 이탈 확인 조건 추가  
  - 기존 5분봉 EMA5 기준 → 1분봉 EMA5 기준으로 매도 타이밍 보정  
  - 수익 구간에서 고점 대비 하락 후 **1분봉 EMA5 이탈 시 매도 트리거** 적용  
- `strategy.py` — `_ema()` 함수 추가  
  - 1분봉 리스트 기반 EMA 계산 지원 (pandas 의존성 없이 경량 처리)  

### 수정
- `strategy.py` 트레일링 스탑 EMA 판정 로직 변경  
  - 기존: 5분봉 `close`, `open`, `ema5_curr` 기반 EMA5 이탈 판정  
  - 수정: 1분봉 `close`, `open`, `EMA5(1m)` 기반 이탈 판정으로 변경  
- `strategy.py` 트레일링 스탑 반응 속도 개선  
  - 5분봉 기준 대비 매도 타이밍을 1분 단위로 앞당겨 수익 보전 강화  

### 설계 변경
- 트레일링 스탑 판정 기준 일관성 확보  
  - 기존: 수익률 계산은 1분봉, EMA 판정은 5분봉 (혼합 구조)  
  - 변경: 수익률 + EMA 판정 모두 1분봉 기준으로 통일  
- 매도 로직 정밀화  
  - 기존: 완만한 추세 기반 익절  
  - 변경: 단기 모멘텀 약화(1분 EMA5 이탈) 반영한 빠른 익절 구조  

### 주의사항
- 1분봉 기준으로 변경됨에 따라 매도 신호 민감도 증가  
- 단기 눌림 구간에서도 매도 발생 가능 → 필요 시 `trailingStop` 값 조정 권장  

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