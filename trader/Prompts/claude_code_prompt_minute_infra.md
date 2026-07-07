# 작업: 분봉 데이터 인프라 구축 (1단계 — 저장/검증만, 전략 연구 아님)

## 배경

Easy Trader의 백테스트 검증 결과, 기존 선정 로직의 거래당 EV가 일봉 데이터로는
부호 판정이 불가능함이 확인됨 (거래의 ~50%가 하루 안에 TP+3%/SL−3% 양측 터치).
이를 해결하려면 분봉 기반 replay가 필요하며, 그 선행 작업으로 분봉 데이터
인프라를 먼저 구축한다.

**이번 작업의 범위는 데이터 저장·검증 인프라까지다. Fast Exit / Trailing /
Time Stop 등 전략 연구·변경은 이번 범위에서 명시적으로 제외한다.**

## 확정된 설계 (변경하지 말 것)

1. **저장 대상**: 엔진이 실제 평가한 최종 candidate set 전체
   (300억 floor 통과 후 pool = max(TOP_N×2, 20) 종목) + 현재 보유 포지션.
   TOP_N만 저장하지 않는다 — 이후 선정 로직 변경 실험을 replay하기 위함.

2. **수집 구조**: 선정 확정 직후 KIS REST 당일분봉 API로 09:00~현재 구간
   백필 → 이후 WebSocket 실시간 집계를 append.
   (WebSocket은 구독 시점 이후만 수신되므로 백필 없이는 개장 초반이 빈다.)
   - **백필은 idempotent해야 한다**: 동일 구간을 여러 번 백필해도 timestamp
     기준 병합·중복 제거로 결과가 동일할 것.
   - **동일 ts 충돌 규칙 (확정)**:
     (a) 실시간 운영 중 — WS 집계 bar가 REST 백필 bar를 항상 overwrite
     (REST는 초기 기준 데이터, WS는 실시간 확정 데이터).
     (b) 복구 패스(REST_RECOVERY) — WS reconnect 공백 등으로 **불완전
     flag가 붙은 구간의 bar에 한해** REST가 WS를 overwrite. 정상 bar는
     복구 패스가 건드리지 않는다. (틱 유실로 불완전한 WS bar가 영구
     보존되는 것을 막기 위함.)

3. **원본 저장소: VM 로컬 parquet** (Firestore에 분봉 원본 저장 금지).
   경로 구조: `data/minute/{YYYYMMDD}/{code}.parquet`
   bar 스키마: ts, open, high, low, close, volume, acml_volume
   - 저장 경로는 반드시 **host volume mount** 아래여야 한다. 컨테이너 내부
     경로에 저장하면 rebuild.sh 재빌드 시 소실된다. run 스크립트의 volume
     mount 현황을 먼저 확인하고, 없으면 mount 추가를 변경안에 포함할 것.
   - **원자적 저장(atomic write) 필수**: `.tmp` 파일에 쓴 뒤 `os.replace()`로
     rename. 쓰기 도중 장애/프로세스 재시작/VM 종료가 발생해도 기존 파일은
     온전하게 유지되어야 한다. 부분 쓰기된 parquet이 남으면 안 된다.

4. **Firestore는 메타데이터만**: 날짜 키로 아카이빙 (덮어쓰기 금지)
   - 당일 selection 결과: candidate pool 전체 + 각 종목 ranking score 구성값
   - gate 상태 (phase1/phase2), scan 파라미터 (scan_hm, market_top_n)
   - market_analysis 스냅샷 (현재 `latest` 덮어쓰기 → 날짜 키 아카이빙 병행)
   - `schema_version`, `engine_version`, `selection_version`,
     `collector_version` 4종 버전 필드 필수 (collector_version은 분봉 수집
     로직 변경 시 증가 — 수집 로직이 바뀌면 replay 해석이 달라질 수 있음)

4-1. **Replay Manifest (VM, 날짜당 JSON 1개)**: `data/minute/{YYYYMMDD}/manifest.json`
   - 내용: candidate_pool(점수 포함), selected, gate 상태, market_analysis
     스냅샷, 4종 버전, minute parquet 파일 목록(**각 파일의 sha256 해시와
     file_size 포함** — replay 시 파일 손상 검증용), created_at
   - 목적: replay 진입점 단일화 — manifest 하나만 읽으면 그날의 조건을 100%
     재현 가능해야 한다. Firestore 메타데이터와 내용이 일부 중복되는 것은
     의도적임 (VM 단독으로 replay가 완결되도록). manifest도 atomic write 적용.

5. **품질 검증**: 장 마감 후 종목-일 단위로 다음 항목 기록
   - bars 수 (기대치 대비, 거래정지/VI 예외 허용), 최초/최종 timestamp,
     중복 timestamp 수, 결측 timestamp 목록, REST 재백필 성공 여부
   - **무결성 검사 — hard 오류** (해당 bar를 오류로 기록):
     high < low, OHLC 관계 위반 (open/close가 [low, high] 밖), 가격 ≤ 0,
     volume < 0
   - **무결성 검사 — soft 경고** (임계치 기반 이상치 플래그만, 오류 아님):
     직전 bar 종가 대비 현재 bar 시가 급변 (예: ±10% 초과 시 플래그).
     분봉에서 체결 공백에 의한 가격 점프는 정상이므로 hard 오류로 처리하지
     말 것.
   - 누락 발견 시 REST 재백필 1회 시도, 실패분은 Telegram 경고
   - daily_review.sh cron 흐름에 검증 스텝 추가

6. (선택, 여유 시) daily_review.sh에 GCS 버킷 일일 백업(rsync) 추가.

## 진행 순서 — 반드시 조사부터

**코드를 쓰기 전에 아래를 먼저 확인하고 결과를 보고할 것:**

1. `ws_tick_collector.py` 읽기 — 분봉 집계가 완결되는 지점(append hook 위치)과
   현재 WebSocket 등록 종목 수 확인. KIS 연결당 등록 한도(41건) 내에서
   pool 20종목 + 보유 포지션 구독이 가능한지 판단.
2. `engine.py` 읽기 — 선정 확정 시점(백필 트리거 + selection 아카이빙 삽입
   지점)과 `_gate_pending` 관련 phase1/phase2 상태를 어디서 읽을지 확인.
3. `kis_api.py` 읽기 — 당일분봉 REST 메서드 존재 여부 확인.
4. **KIS 당일분봉 API를 실제 1회 호출하여 응답 원문 확인** — 필드명과
   1회 호출당 반환 건수 제한을 실데이터로 검증한 뒤 백필 반복 호출 로직을
   설계할 것. 문서나 추정으로 필드명을 가정하지 말 것 (과거 seln_cnqn_smtn
   오류 사례 있음).
5. run 스크립트 / rebuild.sh — volume mount 현황 확인.
6. **WebSocket reconnect 처리 확인** — `ws_tick_collector.py`에서 재연결 시
   집계 중이던 분봉 버퍼가 어떻게 되는지 (유실/중복/재구독 지연) 실코드로
   확인하고 보고할 것. 재연결 구간의 결측 bar는 REST 재백필로 복구하는
   경로가 설계에 포함되어야 한다.

## 산출물

- 신규 모듈 `minute_store.py`: REST 백필(idempotent), WS append 수신,
  parquet atomic 저장, manifest 생성, 품질 검증 리포트
  - **collector는 명시적 상태 머신으로 구현할 것**:
    INIT → REST_BACKFILL → WS_RUNNING → (RECONNECT → REST_RECOVERY →
    WS_RUNNING)* → FINALIZE → VALIDATE.
    상태 전이를 enum + 단일 전이 함수로 관리하고, 각 상태에서 허용되는
    쓰기 동작(위 ts 충돌 규칙의 (a)/(b) 적용 여부 포함)을 상태에 결합할 것.
  - **이벤트 로그**: 모든 상태 전이와 주요 동작(백필 bars 수, WS append,
    reconnect 발생, recovery로 교체된 bars 수, validate 결과)을 이벤트
    단위로 기록. 출력은 두 곳 — (1) stdout: 기존 docker logs →
    daily_review.sh 파이프라인에 자동 포함, (2) `data/minute/{YYYYMMDD}/
    events.jsonl`: 날짜별 영구 보관 (ts, code, event, detail 필드).
    목적은 "왜 이 날짜 데이터가 이 상태인가"를 추적 가능하게 하는 것.
- `engine.py` / `ws_tick_collector.py` / `daily_review.sh` 통합 수정
- CHANGELOG.md에 날짜별 항목 추가 (추가/수정/보류 형식)

## 작업 규칙

- 파일 수정 전 반드시: (1) 변경 요약과 이유 (2) before 코드 (3) after 코드를
  제시하고 승인받은 뒤 적용한다.
- 근거는 실제 코드 라인과 실제 API 응답으로 제시한다. 추정 금지.
- 요청된 범위 외 수정 금지 (예: BLOCK 라우팅 버그, BEAR dead code 등
  알려진 이슈를 발견해도 이번 작업에서 건드리지 않는다).
- 배포는 rebuild.sh 기준으로 안내한다 (docker restart는 docker cp 변경분
  소실됨).
