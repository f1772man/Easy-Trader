# CHANGELOG

## [미배포] - 작업 중
### 추가
### 수정
### 보류

---

## [2026-04-09]
### 수정
- `engine.py` `_display_name()` — `_positions_lock` 재진입 데드락 제거
- `engine.py` `_restore_positions()` — 로그를 lock 밖으로 이동 (데드락 방지)
- `engine.py` `_execute_sell()` — `_normalize_name()` 적용, 이중표기 방지
- `engine.py` `_process_symbol()` — `ws_1min[-1][2]` → `[4]` 종가 기준 수정
- `engine.py` `_process_symbol()` — 첫 체결 전 종목 분석 차단 추가
- `strategy.py` `[C] 손절` — `data_1min[-2]` 직전봉 비교 로직 제거

### 추가
- `engine.py` — `_normalize_name()` 함수 추가 (기존 DB 포맷 정규화)
- `engine.py` — `_load_symbol_data()` 공통 로더 추출 (`_preload_symbols_data` 중복 제거)
- `engine.py` — `_stop_loss_pending` 추가 (손절 2봉 확인 로직)
- `engine.py` `_maybe_premarket_warmup()` — 워밍업 전 `_reload_watch_symbols()` 호출 추가
- `engine.py` `_reload_watch_symbols()` — `force_added` 캐시 로드 추가

### 제거
- `engine.py` `_rollover_if_needed()` — `_reload_watch_symbols()` 호출 제거 (불필요)
- `engine.py` — `_preload_symbols_data()` 제거 → `_warmup_market_data()` 통합

### 보류
- `engine.py` — 상한가 이탈 후 당일 재매수 영구 차단 (`float("inf")`)