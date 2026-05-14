# CHANGELOG

---

## [2026-05-14]

### 제거

* `strategy.py` — 매수 조건 4개 제거 (10개 → 6개)

  * 제거 이유: 오늘 5분봉 데이터 분석 결과, 손실이 집중된 조건과 조건 철학이 중복되는 조건 정리

  | 제거 조건 | 이유 |
  |-----------|------|
  | 조건0-B `VCP고점수+에너지돌파` | 조건0-A(`VCP돌파+Pivot`)와 중복 |
  | 조건0-C `VCP응축+GC전고` | 조건1(`GC+전고돌파`)과 철학 중복 |
  | 조건3 `피봇R2돌파` | 오늘 2건 전부 손절(-2.60%, -2.63%), 이론적 저항선 신뢰도 낮음 |
  | 조건5-A `1분봉EMA골든+거래량폭발` | 5분봉 엔진과 타임프레임 불일치, 오늘 발화 없음 |

* 남은 매수 조건 (6개):

  1. `VCP돌파+Pivot` (조건0-A)
  2. `GC+전고돌파확인` (조건1)
  3. `전일고가돌파` (조건2)
  4. `거래량폭발+MA5` (조건4)
  5. `1분봉지속상승` (조건5)
  6. `에너지응축돌파` (조건6)

---

### 수정

* `strategy.py` — 조건2 `전일고가돌파` 확인봉 추가

  * 기존:

    * 현재봉이 처음 breakout_line을 돌파하면 즉시 BUY
    * 갭 상승 직후 첫 봉이 전일고가를 잠깐 넘었다가 되돌아오는 패턴에 취약

    ```python
    is_fresh_prev_high_breakout = (
        prev_close is not None
        and prev_close <= breakout_line
        and close > breakout_line
    )
    ```

  * 변경:

    * 2봉 전은 breakout_line 아래, 직전봉이 이미 위에 있어야 진입 허용
    * 오늘 115500 손절 케이스 차단 가능

    ```python
    prev_close_2 = data[i - 2][pi["close"]] if i >= 2 else None
    is_fresh_prev_high_breakout = (
        prev_close is not None
        and prev_close_2 is not None
        and prev_close_2 <= breakout_line   # 2봉 전은 돌파 전
        and prev_close > breakout_line       # 직전봉이 확인봉
        and close > breakout_line            # 현재봉도 유지
    )
    ```

---

* `strategy.py` — 공통 가드에 진입봉 변동폭 과대 차단 추가

  * 기존:

    * 당일상승률·EMA5이격·EMA20이격만 체크
    * 진입봉 자체의 변동폭이 크더라도 통과

  * 변경:

    * `(high - low) / close * 100 > maxBarRangePct(기본 3.5%)` 시 차단
    * 장대양봉 고점 추격 진입 방지

    ```python
    bar_range_pct = (high - low) / close * 100 if close > 0 else 0
    max_bar_range_pct = guard_cfg.get("maxBarRangePct", 3.5)
    if bar_range_pct > max_bar_range_pct:
        return {"signal": "HOLD", "reason": f"진입봉과대({bar_range_pct:.1f}%>{max_bar_range_pct:.1f}%)", "energy": energy}
    ```

  * 오늘 분석 기준:

    * 072950 진입봉 변동폭 4.4% → 차단
    * 417840 진입봉 변동폭 4.7% → 차단
    * 322000·064400(익절) 진입봉 변동폭 2~3%대 → 통과

---

* `engine.py` — `_process_symbol()` 손절대기 로직 제거

  * 기존:

    * `reason == "손절대기"` 시 다음 1분봉 종가 확인 후 매도
    * `_stop_loss_pending` 딕셔너리로 대기 상태 관리

  * 변경:

    * 손절 신호 발생 즉시 `current_price`로 매도
    * 대기 로직 제거로 코드 단순화

---

### 설계 변경

* 매수 조건 단순화 방향 확립

  * 기존: 10개 조건 (VCP 3개, GC, 전일고가, 피봇R2, 거래량폭발, 1분봉지속, 1분봉EMA, 에너지응축)
  * 변경: 6개 조건으로 축소 — 서로 다른 철학의 조건 혼재 해소

* 진입 품질 필터 방향 전환

  * 기존: 직전 3봉 응축(tightRange) 중심
  * 변경: 진입봉 변동폭 + 확인봉 중심
  * 근거: 오늘 5분봉 분석 결과 손절 5건 중 4건이 tightRange 통과 — 응축이 아닌 진입봉 품질이 문제였음

---

### 효과

* 장대양봉 고점 추격 진입 차단
* 전일고가 일시 돌파 후 되돌림 손절 감소
* 피봇R2 허위 신호 제거
* 매수 조건 구조 단순화로 신호 일관성 향상

---

### 주의사항

* 전일고가돌파 확인봉 조건으로 인해 진입 타이밍 1봉 지연 (정상 동작)
* 진입봉 변동폭 3.5% 기준은 시장 상황에 따라 조정 필요
  * 변동성이 큰 날에는 4.0%로 완화 검토
* 손절 즉시 실행으로 전환 — 되돌림 후 회복 종목 일부 조기 청산 가능

---

### 분석 기반

* 2026-05-14 실거래 로그 + 5분봉 데이터 (14개 종목) 분석
* 손절 6건 분석 결과:

  | 종목 | 조건 | 진입봉 변동폭 | tightRange | 결과 |
  |------|------|--------------|-----------|------|
  | 115500 | 전일고가돌파 | 1.0% | ✅ 통과 | -2.79% |
  | 417840 | 전일고가돌파 | 4.7% | ✅ 통과 | -3.87% |
  | 072950 | 전일고가돌파 | 4.4% | ✅ 통과 | -2.17% |
  | 267260 | 피봇R2돌파 | 1.2% | ✅ 통과 | -2.60% |
  | 218410 | 피봇R2돌파 | 2.7% | ❌ 차단됐을것 | -2.63% |
  | 321000 | 전일고가돌파 | — | ✅ 통과 | -2.63% |

* 수익 2건(322000 +4.50%, 064400 +2.95%)은 진입 직후 연속 상승 — 진입봉 변동폭 2~3%대

---

## [2026-05-13]

### 설계 변경

* 종목 선정 아키텍처 전면 개편

  * 기존:

    * `daily-candles-job` → `strategy_results` 생성 → `target_stocks` 저장
    * `engine.py` 시작 시 `target_stocks` 읽기 → `watch_symbols` 구성 → 매매 시작

  * 변경:

    * `daily-candles-job` → `strategy_results` 생성만 수행 (`target_stocks` 저장 제거)
    * `engine.py` 09:05~09:10 → `strategy_results` 전체 조회 → 거래대금 + 갭 조건 필터 → 상위 10개 선정 → `target_stocks` 저장 → `watch_symbols` 구성 → 매매 시작

---

### 추가

* `engine.py` — `_filter_by_trade_amount()` 함수 신규 추가

  * 실행 시점:
    * 09:05~09:10 자동 실행 (하루 1회)
    * 09:10 이후 미완료 시 즉시 실행 (폴백)
    * 엔진 재시작 시 `tr_pbmn_date == today` 이고 `tr_pbmn > 0` 이면 재조회 없이 복원

  * 처리 흐름:
    1. `strategy_results` 전체 읽기 (`summaryBadge == '진입'` 조건)
    2. KIS API(`FHKST01010100`) 호출 — 현재가 + 시가 + 전일종가 + 누적 거래대금 수집
    3. 갭 조건 필터 적용:
       * 시가갭 `1.0% ~ 15.0%`
       * 현재갭 `>= 0.5%`
       * 현재가 >= 시가 (갭 유지)
    4. 거래대금 내림차순 정렬 → 상위 10개 선정
    5. 기존 `target_stocks` 전체 삭제 후 신규 저장
    6. `_warmup_market_data()` 호출
    7. `watch_symbols` 교체
    8. 텔레그램 알림 (종목명, 거래대금, 시가갭 포함)

* `engine.py` — `_trade_filter_done: bool` 클래스 변수 추가

  * 당일 거래대금 필터 완료 여부 관리
  * 재시작 시 `target_stocks`의 `tr_pbmn_date`, `tr_pbmn` 필드로 자동 복원

* `kis_api.py` — `get_trade_amount()` 함수 추가

  * `FHKST01010100` API에서 현재가 + 누적 거래대금(`acml_tr_pbmn`) 반환
  * 반환: `{'price': int, 'tr_pbmn': int}`

---

### 수정

* `engine.py` — `start()` 초기화 구조 변경

  * 기존: 시작 시 `_reload_watch_symbols()` + `_warmup_market_data()` 호출 → `WsTickCollector` 시작
  * 변경: `WsTickCollector([])` 빈 리스트로 시작 → `_filter_by_trade_amount()` 완료 후 `watch_symbols` 구성

* `engine.py` — `_maybe_premarket_warmup()` 구조 변경

  * 기존: 워밍업 전 `_reload_watch_symbols()` 호출
  * 변경: `_reload_watch_symbols()` 제거 → `_warmup_market_data()`만 수행

* `engine.py` — `_load_target_symbols_with_meta()` 재시작 복원 로직 추가

  * `tr_pbmn_date == today` 이고 `tr_pbmn > 0` 인 종목 감지 시 `_trade_filter_done = True` 복원
  * 거래대금 필터 완료 시 `tr_pbmn_rank` 기준 정렬
  * 미완료 시 기존 `score` 기준 정렬 유지

* `engine.py` — `_tick()` 거래대금 필터 트리거 추가

  ```python
  # 09:05~09:10 자동 실행
  if not self._trade_filter_done and 905 <= hm <= 910:
      self._filter_by_trade_amount()

  # 09:10 이후 폴백
  if not self._trade_filter_done and hm > 910:
      self._filter_by_trade_amount()
  ```

* `candles/main.py` — `_build_target_stocks()` 호출 제거

  * 기존: `strategy_results` 생성 후 `_build_target_stocks()` 호출하여 `target_stocks` 저장
  * 변경: `strategy_results` 생성만 수행, `target_stocks` 저장은 `engine.py`에서 처리

---

### 저장 필드 변경

* `target_stocks` 컬렉션 신규 저장 필드:

  | 필드 | 내용 |
  |------|------|
  | `tr_pbmn` | 당일 누적 거래대금 |
  | `tr_pbmn_rank` | 거래대금 순위 (1~10) |
  | `tr_pbmn_at` | 조회 시각 (HH:MM) |
  | `tr_pbmn_date` | 조회 날짜 (YYYYMMDD) — 재시작 복원 판단용 |
  | `gap_pct` | 시가갭 (%) |
  | `current_pct` | 현재갭 (%) |

---

### 효과

* 장 시작 후 실제 수급(거래대금)과 갭 상승 여부를 동시에 반영한 종목 선정
* 예상 거래대금(장전 추정) 대신 실제 거래대금(09:05~09:10) 기준으로 정확도 향상
* 갭 하락 또는 갭 유지 실패 종목 자동 제외
* 엔진 재시작 시 재조회 없이 당일 선정 결과 자동 복원

---

### 주의사항

* 09:05 이전 매매 신호 없음 (watch_symbols 비어있음)
* `strategy_results`에 `summaryBadge == '진입'` 종목이 없으면 필터 실패
* 갭 조건 기준 (`GAP_MIN_PCT=1.0`, `GAP_MAX_PCT=15.0`, `CURRENT_MIN_PCT=0.5`) 은 시장 상황에 따라 조정 필요
* API 호출 수 = `strategy_results` 진입 후보 수 × 0.2초 — 종목 수에 따라 소요 시간 증가

---

### Cloud Run 변경

* `premarket-filter` Cloud Run Scheduler 비활성화

  * 기존 08:58 예상 거래대금 기반 필터 → engine.py 내부 실제 거래대금 기반 필터로 대체
  * Cloud Run 서비스 코드는 유지 (롤백 대비)
---

## [2026-05-09]

### 수정

* `strategy.py` — 조건1 `GC+전고돌파확인` 거래량 지속성 필터 추가

  * 기존:

    * 돌파 확인봉만 유지되면 진입 가능
    * 거래량 감소 상태의 돌파도 매수 허용

  * 변경:

    * 직전봉 대비 거래량 유지 여부 확인 추가

    ```python
    prev_volume = data[i - 1][pi["volume"]] if i > 0 else 0

    volume_keep_ok = (
        prev_volume > 0
        and volume >= prev_volume * 0.8
    )

    if not volume_keep_ok:
        return {
            "signal": "HOLD",
            "reason": "돌파거래량감소",
            "energy": energy
        }
    ```

  * 거래량 감소 상태의 후반 돌파 추격 진입 차단

---

* `strategy.py` — 조건1 `GC+전고돌파확인` EMA5 이격 과열 제한 강화

  * 기존:

    ```python
    if dist_from_ema5 > cfg.get("maxDistanceFromEma5Pct", 3.0):
    ```

  * 변경:

    * 시간대별 EMA5 이격 허용 범위 차등 적용

    ```python
    max_ema5_dist = (
        4.5 if hm_int <= 930 else
        3.5 if hm_int <= 1000 else
        2.5
    )

    if dist_from_ema5 > max_ema5_dist:
        return {
            "signal": "HOLD",
            "reason": "이격과열",
            "energy": energy
        }
    ```

  * 장초반 강한 추세는 허용하면서
    후반 과열 추격 진입 차단

---

* `strategy.py` — 조건1 `GC+전고돌파확인` 장중 상승률 과열 차단 추가

  * 신규 추가:

    ```python
    day_rise_pct = (
        ((close / day_open) - 1) * 100
        if day_open else 0
    )

    max_day_rise_pct = (
        15 if hm_int <= 930 else
        12 if hm_int <= 1000 else
        8
    )

    if day_rise_pct > max_day_rise_pct:
        return {
            "signal": "HOLD",
            "reason": "당일과열추격차단",
            "energy": energy
        }
    ```

  * 이미 큰 폭 상승한 종목의 후반 재추격 매수 차단

---

### 설계 변경

* `GC+전고돌파확인` 전략 구조 개선

  * 기존:

    * 돌파 발생 여부 중심
    * Breakout 자체를 진입 신호로 사용

  * 변경:

    * 돌파 위치
    * 거래량 지속성
    * EMA5 과열 여부
    * 장중 상승 과열 상태

    를 함께 평가하는 구조로 변경

---

### 효과

* 후반 추격 매수 감소
* 거래량 감소 돌파 실패 구간 차단
* EMA5 과열 구간 진입 감소
* 장중 고점 부근 재진입 감소
* `GC+전고돌파확인` 조건 손절 빈도 감소 기대

---

### 주의사항

* 초기 급등 종목 일부 진입 누락 가능
* 장초반 강한 모멘텀 종목은 시간대별 완화 기준으로 일부 허용
* 진입 빈도 감소는 정상 동작
* 수익보다 손실 감소 및 체결 품질 개선 목적의 수정

---

### 분석 기반

* `휴온스글로벌(084110)` 2026-05-08 5분봉 데이터 분석 기반 적용
* 주요 손실 패턴:

  * 후반 돌파 추격
  * 거래량 감소 상태 돌파
  * EMA5 과열 구간 진입
  * 장중 +10% 이상 상승 후 재진입

* 로그 기준 `GC+전고돌파확인` 조건 손실 비중이 높아 우선 수정 적용

---

## [2026-05-07]

### 추가

* `engine.py` — `_execute_sell()` 종목명 추적용 디버그 로그 추가

  * 매도 실행 직전 positions 및 `_symbol_meta` 의 종목명 상태 출력

  * 추가 코드:

    ```python
    logger.info(
        f"[SELL-NAME] symbol={symbol} | "
        f"pos_name={pos_name or '-'} | "
        f"meta_name={meta_name or '-'}"
    )
    ```

  * 종목명 최종 생성 실패 시 warning 로그 출력

    ```python
    logger.warning(
        f"[SELL-NAME-MISSING] symbol={symbol} | reason={reason}"
    )
    ```

---

### 수정

* `engine.py` — `_execute_sell()` positions 조회 방식 안정화

  * 기존:

    ```python
    with self._positions_lock:
        pos = self._positions.get(symbol, {})
    ```

  * 변경:

    ```python
    with self._positions_lock:
        pos = dict(self._positions.get(symbol, {}))
    ```

  * 병렬 처리(ThreadPoolExecutor) 환경에서  
    다른 스레드의 `positions.pop()` 영향 최소화

---

* `engine.py` — `_execute_sell()` 종목명 생성 구조 개선

  * 기존:

    ```python
    raw_name = pos.get("name") or self._symbol_meta.get(symbol, {}).get("name", "")
    display = f"{_normalize_name(raw_name, symbol)}({symbol})" if raw_name else symbol
    ```

  * 변경:

    ```python
    pos_name = pos.get("name", "")
    meta_name = self._symbol_meta.get(symbol, {}).get("name", "")

    raw_name = pos_name or meta_name
    raw_name = _normalize_name(raw_name, symbol)

    display = f"{raw_name}({symbol})" if raw_name else symbol
    ```

  * 종목명 source를 분리하여 추적 가능하도록 개선

---

### 설계 변경

* 매도 알림 종목명 생성 과정 추적 강화

  * 기존:

    * 종목명 누락 발생 시 원인 확인 어려움
    * positions / `_symbol_meta` 중 어느 경로가 비어있는지 확인 불가

  * 변경:

    * positions 기반 종목명
    * `_symbol_meta` 기반 종목명
    * 최종 display 생성 결과

    를 각각 추적 가능하도록 로그 구조 개선

---

### 효과

* `종목: 092790` 형태의 종목명 누락 원인 분석 가능
* 병렬 매도 처리 중 positions 참조 안정성 향상
* 매도 알림 종목명 생성 경로 추적 가능
* 향후 legacy 포지션 데이터 문제 분석 용이

---

### 주의사항

* INFO/WARNING 로그 증가 가능
* 장중 매도 빈도가 많을 경우 로그량 증가 가능

---

## [2026-05-13]

### 설계 변경

* 종목 선정 아키텍처 전면 개편

  * 기존:

    * `daily-candles-job` → `strategy_results` 생성 → `target_stocks` 저장
    * `engine.py` 시작 시 `target_stocks` 읽기 → `watch_symbols` 구성 → 매매 시작

  * 변경:

    * `daily-candles-job` → `strategy_results` 생성만 수행 (`target_stocks` 저장 제거)
    * `engine.py` 09:05~09:10 → `strategy_results` 전체 조회 → 거래대금 + 갭 조건 필터 → 상위 10개 선정 → `target_stocks` 저장 → `watch_symbols` 구성 → 매매 시작

---

### 추가

* `engine.py` — `_filter_by_trade_amount()` 함수 신규 추가

  * 실행 시점:
    * 09:05~09:10 자동 실행 (하루 1회)
    * 09:10 이후 미완료 시 즉시 실행 (폴백)
    * 엔진 재시작 시 `tr_pbmn_date == today` 이고 `tr_pbmn > 0` 이면 재조회 없이 복원

  * 처리 흐름:
    1. `strategy_results` 전체 읽기 (`summaryBadge == '진입'` 조건)
    2. KIS API(`FHKST01010100`) 호출 — 현재가 + 시가 + 전일종가 + 누적 거래대금 수집
    3. 갭 조건 필터 적용:
       * 시가갭 `1.0% ~ 15.0%`
       * 현재갭 `>= 0.5%`
       * 현재가 >= 시가 (갭 유지)
    4. 거래대금 내림차순 정렬 → 상위 10개 선정
    5. 기존 `target_stocks` 전체 삭제 후 신규 저장
    6. `_warmup_market_data()` 호출
    7. `watch_symbols` 교체
    8. 텔레그램 알림 (종목명, 거래대금, 시가갭 포함)

* `engine.py` — `_trade_filter_done: bool` 클래스 변수 추가

  * 당일 거래대금 필터 완료 여부 관리
  * 재시작 시 `target_stocks`의 `tr_pbmn_date`, `tr_pbmn` 필드로 자동 복원

* `kis_api.py` — `get_trade_amount()` 함수 추가

  * `FHKST01010100` API에서 현재가 + 누적 거래대금(`acml_tr_pbmn`) 반환
  * 반환: `{'price': int, 'tr_pbmn': int}`

---

### 수정

* `engine.py` — `start()` 초기화 구조 변경

  * 기존: 시작 시 `_reload_watch_symbols()` + `_warmup_market_data()` 호출 → `WsTickCollector` 시작
  * 변경: `WsTickCollector([])` 빈 리스트로 시작 → `_filter_by_trade_amount()` 완료 후 `watch_symbols` 구성

* `engine.py` — `_maybe_premarket_warmup()` 구조 변경

  * 기존: 워밍업 전 `_reload_watch_symbols()` 호출
  * 변경: `_reload_watch_symbols()` 제거 → `_warmup_market_data()`만 수행

* `engine.py` — `_load_target_symbols_with_meta()` 재시작 복원 로직 추가

  * `tr_pbmn_date == today` 이고 `tr_pbmn > 0` 인 종목 감지 시 `_trade_filter_done = True` 복원
  * 거래대금 필터 완료 시 `tr_pbmn_rank` 기준 정렬
  * 미완료 시 기존 `score` 기준 정렬 유지

* `engine.py` — `_tick()` 거래대금 필터 트리거 추가

  ```python
  # 09:05~09:10 자동 실행
  if not self._trade_filter_done and 905 <= hm <= 910:
      self._filter_by_trade_amount()

  # 09:10 이후 폴백
  if not self._trade_filter_done and hm > 910:
      self._filter_by_trade_amount()
  ```

* `candles/main.py` — `_build_target_stocks()` 호출 제거

  * 기존: `strategy_results` 생성 후 `_build_target_stocks()` 호출하여 `target_stocks` 저장
  * 변경: `strategy_results` 생성만 수행, `target_stocks` 저장은 `engine.py`에서 처리

---

### 저장 필드 변경

* `target_stocks` 컬렉션 신규 저장 필드:

  | 필드 | 내용 |
  |------|------|
  | `tr_pbmn` | 당일 누적 거래대금 |
  | `tr_pbmn_rank` | 거래대금 순위 (1~10) |
  | `tr_pbmn_at` | 조회 시각 (HH:MM) |
  | `tr_pbmn_date` | 조회 날짜 (YYYYMMDD) — 재시작 복원 판단용 |
  | `gap_pct` | 시가갭 (%) |
  | `current_pct` | 현재갭 (%) |

---

### 효과

* 장 시작 후 실제 수급(거래대금)과 갭 상승 여부를 동시에 반영한 종목 선정
* 예상 거래대금(장전 추정) 대신 실제 거래대금(09:05~09:10) 기준으로 정확도 향상
* 갭 하락 또는 갭 유지 실패 종목 자동 제외
* 엔진 재시작 시 재조회 없이 당일 선정 결과 자동 복원

---

### 주의사항

* 09:05 이전 매매 신호 없음 (watch_symbols 비어있음)
* `strategy_results`에 `summaryBadge == '진입'` 종목이 없으면 필터 실패
* 갭 조건 기준 (`GAP_MIN_PCT=1.0`, `GAP_MAX_PCT=15.0`, `CURRENT_MIN_PCT=0.5`) 은 시장 상황에 따라 조정 필요
* API 호출 수 = `strategy_results` 진입 후보 수 × 0.2초 — 종목 수에 따라 소요 시간 증가

---

### Cloud Run 변경

* `premarket-filter` Cloud Run Scheduler 비활성화

  * 기존 08:58 예상 거래대금 기반 필터 → engine.py 내부 실제 거래대금 기반 필터로 대체
  * Cloud Run 서비스 코드는 유지 (롤백 대비)
---

## [2026-05-09]

### 수정

* `strategy.py` — 조건1 `GC+전고돌파확인` 거래량 지속성 필터 추가

  * 기존:

    * 돌파 확인봉만 유지되면 진입 가능
    * 거래량 감소 상태의 돌파도 매수 허용

  * 변경:

    * 직전봉 대비 거래량 유지 여부 확인 추가

    ```python
    prev_volume = data[i - 1][pi["volume"]] if i > 0 else 0

    volume_keep_ok = (
        prev_volume > 0
        and volume >= prev_volume * 0.8
    )

    if not volume_keep_ok:
        return {
            "signal": "HOLD",
            "reason": "돌파거래량감소",
            "energy": energy
        }
    ```

  * 거래량 감소 상태의 후반 돌파 추격 진입 차단

---

* `strategy.py` — 조건1 `GC+전고돌파확인` EMA5 이격 과열 제한 강화

  * 기존:

    ```python
    if dist_from_ema5 > cfg.get("maxDistanceFromEma5Pct", 3.0):
    ```

  * 변경:

    * 시간대별 EMA5 이격 허용 범위 차등 적용

    ```python
    max_ema5_dist = (
        4.5 if hm_int <= 930 else
        3.5 if hm_int <= 1000 else
        2.5
    )

    if dist_from_ema5 > max_ema5_dist:
        return {
            "signal": "HOLD",
            "reason": "이격과열",
            "energy": energy
        }
    ```

  * 장초반 강한 추세는 허용하면서
    후반 과열 추격 진입 차단

---

* `strategy.py` — 조건1 `GC+전고돌파확인` 장중 상승률 과열 차단 추가

  * 신규 추가:

    ```python
    day_rise_pct = (
        ((close / day_open) - 1) * 100
        if day_open else 0
    )

    max_day_rise_pct = (
        15 if hm_int <= 930 else
        12 if hm_int <= 1000 else
        8
    )

    if day_rise_pct > max_day_rise_pct:
        return {
            "signal": "HOLD",
            "reason": "당일과열추격차단",
            "energy": energy
        }
    ```

  * 이미 큰 폭 상승한 종목의 후반 재추격 매수 차단

---

### 설계 변경

* `GC+전고돌파확인` 전략 구조 개선

  * 기존:

    * 돌파 발생 여부 중심
    * Breakout 자체를 진입 신호로 사용

  * 변경:

    * 돌파 위치
    * 거래량 지속성
    * EMA5 과열 여부
    * 장중 상승 과열 상태

    를 함께 평가하는 구조로 변경

---

### 효과

* 후반 추격 매수 감소
* 거래량 감소 돌파 실패 구간 차단
* EMA5 과열 구간 진입 감소
* 장중 고점 부근 재진입 감소
* `GC+전고돌파확인` 조건 손절 빈도 감소 기대

---

### 주의사항

* 초기 급등 종목 일부 진입 누락 가능
* 장초반 강한 모멘텀 종목은 시간대별 완화 기준으로 일부 허용
* 진입 빈도 감소는 정상 동작
* 수익보다 손실 감소 및 체결 품질 개선 목적의 수정

---

### 분석 기반

* `휴온스글로벌(084110)` 2026-05-08 5분봉 데이터 분석 기반 적용
* 주요 손실 패턴:

  * 후반 돌파 추격
  * 거래량 감소 상태 돌파
  * EMA5 과열 구간 진입
  * 장중 +10% 이상 상승 후 재진입

* 로그 기준 `GC+전고돌파확인` 조건 손실 비중이 높아 우선 수정 적용

---

## [2026-05-07]

### 추가

* `engine.py` — `_execute_sell()` 종목명 추적용 디버그 로그 추가

  * 매도 실행 직전 positions 및 `_symbol_meta` 의 종목명 상태 출력

  * 추가 코드:

    ```python
    logger.info(
        f"[SELL-NAME] symbol={symbol} | "
        f"pos_name={pos_name or '-'} | "
        f"meta_name={meta_name or '-'}"
    )
    ```

  * 종목명 최종 생성 실패 시 warning 로그 출력

    ```python
    logger.warning(
        f"[SELL-NAME-MISSING] symbol={symbol} | reason={reason}"
    )
    ```

---

### 수정

* `engine.py` — `_execute_sell()` positions 조회 방식 안정화

  * 기존:

    ```python
    with self._positions_lock:
        pos = self._positions.get(symbol, {})
    ```

  * 변경:

    ```python
    with self._positions_lock:
        pos = dict(self._positions.get(symbol, {}))
    ```

  * 병렬 처리(ThreadPoolExecutor) 환경에서  
    다른 스레드의 `positions.pop()` 영향 최소화

---

* `engine.py` — `_execute_sell()` 종목명 생성 구조 개선

  * 기존:

    ```python
    raw_name = pos.get("name") or self._symbol_meta.get(symbol, {}).get("name", "")
    display = f"{_normalize_name(raw_name, symbol)}({symbol})" if raw_name else symbol
    ```

  * 변경:

    ```python
    pos_name = pos.get("name", "")
    meta_name = self._symbol_meta.get(symbol, {}).get("name", "")

    raw_name = pos_name or meta_name
    raw_name = _normalize_name(raw_name, symbol)

    display = f"{raw_name}({symbol})" if raw_name else symbol
    ```

  * 종목명 source를 분리하여 추적 가능하도록 개선

---

### 설계 변경

* 매도 알림 종목명 생성 과정 추적 강화

  * 기존:

    * 종목명 누락 발생 시 원인 확인 어려움
    * positions / `_symbol_meta` 중 어느 경로가 비어있는지 확인 불가

  * 변경:

    * positions 기반 종목명
    * `_symbol_meta` 기반 종목명
    * 최종 display 생성 결과

    를 각각 추적 가능하도록 로그 구조 개선

---

### 효과

* `종목: 092790` 형태의 종목명 누락 원인 분석 가능
* 병렬 매도 처리 중 positions 참조 안정성 향상
* 매도 알림 종목명 생성 경로 추적 가능
* 향후 legacy 포지션 데이터 문제 분석 용이

---

### 주의사항

* INFO/WARNING 로그 증가 가능
* 장중 매도 빈도가 많을 경우 로그량 증가 가능
* 원인 분석 완료 후 DEBUG 레벨 전환 권장

---

## [2026-04-30]

### 추가

* `engine.py` — 전략 필터(`strategy_filter.json`) 기반 매수 차단 기능 추가  

  * `strategy_filter.json` 파일을 통해 매수 조건(reason)별 차단 가능  

  * 매수 신호 발생 시 필터 적용  

    ```python
    if reason in self._blocked_reasons:
        logger.info(f"[BUY-BLOCK][{symbol}] {display} reason={reason}")
        return
    ```

---

* `engine.py` — 전략 필터 실시간 재로드 기능 추가 (재배포 없이 반영)

  * 파일 변경 시간(`mtime`) 기반 자동 감지  

    ```python
    if mtime != self._strategy_filter_mtime:
        self._blocked_reasons = ...
    ```

  * 최대 약 5초 이내 반영  

---

### 수정

* `engine.py` — 매수/매도 로그 포맷 구조 변경  

  * 기존:

    ```text
    🟢 매수 실행: 삼성전자(005930) 10주 @ 72000원
    ```

  * 변경:

    ```text
    [BUY][005930] 삼성전자(005930) price=72000 qty=10 reason=...
    ```

  * 매도 로그 동일 구조 적용  

---

* `engine.py` — `get_strategy_signal()` 호출 시 `symbol` 파라미터 전달 추가  

  ```python
  "symbol": symbol
---

## [2026-04-27]

### 수정

* `engine.py` — `_reload_watch_symbols()` 텔레그램 알림 메시지 구조 개선

  * 기존: 총 개수만 표시되어 구성(전략/포지션) 구분이 불명확

    ```python
    lines = [
        f"📋 감시 종목 갱신 ({now_str})",
        f"총 {len(self._watch_symbols)}개"
    ]
    ```

  * 변경: **전략 종목 + 포지션 종목 구성 분리 표시**

    ```python
    lines = [
        f"📋 감시 종목 갱신 ({now_str})",
        f"총 {len(self._watch_symbols)}개 (전략 {len(new_symbols)} + 포지션 {len(holding_extra)})"
    ]
    ```

---

* `engine.py` — 감시 종목 분류 항목 추가

  * 기존:

    * 신규 편입 / 포지션 유지 / 제외만 표시

  * 변경:

    * **연속 편입(overlap) 항목 추가**

    ```python
    overlap = [s for s in new_symbols if s in prev_symbols] if prev_symbols else []
    ```

---

### 설계 변경

* 감시 종목 상태 분류 구조 재정의

  * 기존:

    * 신규 / 제거 중심 단순 비교 구조

  * 변경:

    * 신규 편입 (`added`)
    * 연속 편입 (`overlap`)
    * 포지션 유지 (`holding_extra`)
    * 제외 (`removed`)

---

### 효과

* 감시 종목 구성에 대한 가시성 향상
* 전략 결과와 실제 보유 포지션 구분 명확화
* “총 개수 vs 세부 항목 불일치”에 대한 혼란 제거
* 운영 중 디버깅 및 판단 속도 개선

---

### 주의사항

* `연속 편입`은 **보유 여부와 무관**한 개념

  * 단순히 이전 전략 리스트와 현재 리스트의 교집합

* `포지션 유지`와 의미 혼동 주의 필요

  * 포지션 유지 = 실제 계좌 보유 종목  
  * 연속 편입 = 전략 결과 유지 종목

---

### 추가

* `engine.py` — 종목 목록 출력 조건 개선

  * 기존:

    ```python
    if not added and not removed:
    ```

  * 변경:

    ```python
    if not added and not removed and not overlap:
    ```

  * 변경 이유:

    * 연속 편입 출력과 종목 목록 출력 중복 방지

---

### UI 개선

* 종목 목록 출력 시 헤더 추가

  ```python
  lines += ["", "📄 종목 목록:"]
---

## [2026-04-23]

### 수정

* `strategy.py` — 5분봉 EMA5 이탈 매도 조건 제거

  * 기존: 5분봉 기준 EMA5 하향 이탈 시 매도 조건 존재

    ```python
    if ema5_curr is not None and ema5_prev is not None:
        ema5_break = (
            close < ema5_curr
            and open_ < ema5_curr
            and ema5_curr < ema5_prev
        )
    ```

  * 변경: **해당 EMA5 이탈 매도 조건 완전 제거**

---

### 설계 변경

* 단기 EMA 기반 매도 로직 축소

  * 기존:
    * EMA5 이탈 시 즉시 매도 (단기 반응형)

  * 변경:
    * 트레일링 스탑 및 추세 기반 매도 중심 구조로 단순화
    * EMA5 단기 흔들림에 의한 조기 청산 제거

---

### 효과

* 단기 노이즈(EMA5 일시 이탈)로 인한 조기 매도 감소
* 상승 추세 유지 구간에서 보유 지속 가능
* 트레일링 기반 수익 극대화 구조 강화

---

### 주의사항

* 단기 반락 구간에서 대응 속도 저하 가능
* EMA5 기반 빠른 손절 기능이 사라짐
* 트레일링 조건(trailingStart, trailingStop)에 대한 의존도 증가

---

### 수정

* `ws_tick_collector.py` — `_update_5min()` 5분봉 버킷 기준 로직 오류 수정

  * 기존: **버킷 기준 불일치 (시작시각 + 종료시각 혼용)**

    * `cur_bucket`은 종료시각 기준

      ```python
      bucket_m = ((minute_total // 5) + 1) * 5
      ```

    * 실제 집계 키(`bkt_key`)는 시작시각 기준

      ```python
      bkt_m = (bh * 60 + bm) // 5 * 5
      ```

    → 동일 5분봉 내 데이터가 서로 다른 기준으로 처리됨

  * 문제점:

    * 09:01 ~ 09:04 데이터가 `09:00` 봉으로 묶이면서  
      `cur_bucket=09:05` 기준과 충돌

    * 진행 중 봉이 **완성봉으로 조기 확정되는 문제 발생**

    * collector 5분봉과 engine partial 5분봉 기준 불일치

---

  * 변경: **종료시각 기준으로 완전 통일**

    ```python
    bkt_m = ((bh * 60 + bm) // 5 + 1) * 5
    ```

    * 09:01 ~ 09:04 → 09:05 봉
    * 09:05 ~ 09:09 → 09:10 봉

---

### 설계 변경

* 5분봉 기준 통일

  * 기존:

    * collector: 시작시각 기준
    * engine(partial): 종료시각 기준

  * 변경:

    * collector + engine 모두 **종료시각 기준으로 통일**

---

### 효과

* 5분봉 생성 로직 일관성 확보
* 진행 중 봉의 조기 완료 처리 제거
* strategy용 5분봉과 저장용 5분봉 기준 일치
* 실시간 전략 신호 정확도 개선

---

### 주의사항

* 5분봉 timestamp가 기존과 달라짐

  * 기존: `09:00, 09:05`
  * 변경: `09:05, 09:10`

* 기존 저장 데이터와 시간 기준 불일치 발생 가능

* 백테스트 / 로그 비교 시 기준 차이 고려 필요

---

### 핵심 요약

> 5분봉을 “시작시각 기준”에서 “종료시각 기준”으로 전환하면서,  
> `_update_5min()` 내부 버킷 계산이 일관되지 않아 발생한 문제를 수정함.
---

## [2026-04-22]

### 수정

* `strategy.py` — 조건1 `GC+전고돌파` 진입 로직 개선

  * 기존: 돌파 발생 시 즉시 진입

    ```python
    if has_recent_golden_cross and is_within_gc_window and is_swing_breakout:
        return BUY
    ```

  * 변경: **확인봉 기반 진입 구조로 수정**

    * 이전 봉에서도 돌파 상태 유지 시에만 진입
    * 단기 가짜 돌파(윗꼬리, 순간 돌파) 차단

    ```python
    prev_close = data[i - 1][close]
    prev_breakout = prev_close > swing_high
    ```

---

* `strategy.py` — 조건1 `GC+전고돌파` 돌파폭 필터 추가

  * 최소 돌파폭 기준 도입

    * `breakout_margin_pct >= minBreakoutMarginPct`

  * 미세 돌파(틱 돌파) 진입 차단

---

* `strategy.py` — 조건1 `GC+전고돌파` 이격 과열 차단 추가

  * EMA5 대비 과도한 이격 시 진입 차단

    ```python
    (close / ema5_curr - 1) * 100 <= maxDistanceFromEma5Pct
    ```

  * 과열 구간 추격 매수 방지

---

### 설계 변경

* 돌파 전략 구조 변경

  * 기존:

    * 돌파 즉시 진입 (Breakout = Entry)

  * 변경:

    * 돌파 → 유지 확인 → 진입 (Breakout + Confirmation)

---

### 효과

* 가짜 돌파 진입 감소
* 초반 눌림 후 손절 발생 빈도 감소
* 진입 신호 신뢰도 상승
* 실전 체결 품질 개선

---

### 주의사항

* 진입 타이밍이 1봉 지연됨 (정상 동작)
* 초기 급등 종목 일부 놓칠 수 있음
* 대신 손실 감소 및 승률 개선 기대

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