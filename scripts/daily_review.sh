#!/bin/bash
# ============================================================
# daily_review.sh
# 장 마감 후 로그 수집 → Claude Code 분석 → 자동 수정 → 텔레그램 보고
# Cloud Scheduler: 15:40 KST 월~금
# ============================================================

set -uo pipefail

# ── 설정 ────────────────────────────────────────────────────
PROJECT_DIR="/home/f1227/easy-trader"
CONTAINER_NAME="easy-trader"

TODAY=$(date '+%Y-%m-%d')
TODAY_COMPACT=$(date '+%Y%m%d')

LOG_FILE="/tmp/easy_trader_${TODAY_COMPACT}.log"
ANALYSIS_FILE="/tmp/analysis_${TODAY_COMPACT}.txt"
CLAUDE_PROMPT_FILE="/tmp/claude_prompt_${TODAY_COMPACT}.txt"

# .env 직접 로드
set -a
source "${PROJECT_DIR}/.env"
set +a

BOT_TOKEN="${TELEGRAM_BOT_TOKEN}"
CHAT_ID="${TELEGRAM_CHAT_ID}"

# ── 함수 ────────────────────────────────────────────────────
send_telegram() {
    local msg="$1"
    msg="${msg:0:4000}"

    local response http_code body
    local json
    json=$(python3 -c "
import json, sys
data = {'chat_id': '${CHAT_ID}', 'text': sys.stdin.read()}
print(json.dumps(data, ensure_ascii=False))
" <<< "${msg}")

    response=$(curl -s -w "\n%{http_code}" -X POST \
        "https://api.telegram.org/bot${BOT_TOKEN}/sendMessage" \
        -H "Content-Type: application/json; charset=utf-8" \
        -d "${json}")

    http_code=$(echo "${response}" | tail -1)
    body=$(echo "${response}" | head -n -1)

    if [ "${http_code}" != "200" ]; then
        echo "[WARN] 텔레그램 발송 실패 (HTTP ${http_code}): ${body}" >&2
    fi
}   

die() {
    local msg="$1"
    echo "[ERROR] ${msg}" >&2
    send_telegram "❌ [daily_review 오류] ${TODAY}

${msg}"
    exit 1
}

# ── 사전 점검 ────────────────────────────────────────────────
if [ ! -d "${PROJECT_DIR}" ]; then
    die "프로젝트 폴더가 없습니다: ${PROJECT_DIR}"
fi

if [ ! -f "${PROJECT_DIR}/.env" ]; then
    die ".env 파일이 없습니다: ${PROJECT_DIR}/.env"
fi

if [ -z "${BOT_TOKEN:-}" ] || [ -z "${CHAT_ID:-}" ]; then
    die "텔레그램 설정이 없습니다. TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID를 확인하세요."
fi

if ! command -v docker >/dev/null 2>&1; then
    die "docker 명령을 찾을 수 없습니다."
fi

if ! command -v claude >/dev/null 2>&1; then
    die "claude 명령을 찾을 수 없습니다."
fi

# ── 1단계: 로그 수집 ────────────────────────────────────────
echo "[1/4] 오늘 로그 수집 중..."

docker logs "${CONTAINER_NAME}" \
    --since "${TODAY}T08:40:00+09:00" \
    --until "${TODAY}T15:35:00+09:00" \
    > "${LOG_FILE}" 2>&1

LINE_COUNT=$(wc -l < "${LOG_FILE}")
echo "  → ${LINE_COUNT}줄 수집 완료: ${LOG_FILE}"

if [ "${LINE_COUNT}" -lt 10 ]; then
    die "로그가 너무 적습니다 (${LINE_COUNT}줄). 컨테이너 상태를 확인하세요."
fi

# ── 2단계: Claude Code 분석 ─────────────────────────────────
echo "[2/4] Claude Code 분석 중..."

cd "${PROJECT_DIR}" || die "프로젝트 폴더로 이동 실패: ${PROJECT_DIR}"

PRE_DIFF=$(git diff --name-only 2>/dev/null || true)

cat > "${CLAUDE_PROMPT_FILE}" << 'PROMPT_EOF'
오늘 장중 로그 파일을 읽고 아래 항목을 분석해줘.
출력 시 마크다운(##, --, **, |표|) 사용 금지. 아래 형식을 그대로 따라줘.
"파일 수정 권한" 등 부가 설명 문장은 출력하지 마.

[1] 시나리오 검증
08:50 워밍업: ✅/❌ 성공 N/실패 N
08:50 WS구독: ✅/❌ 첫체결시각
09:00 보유종목: ✅/❌ 처리내용
09:20 거래대금필터: ✅/❌ N통과/N갭미달
09:21 신규편입: ✅/❌ N종목 추가

[2] 에러/경고
(없으면 "이상 없음", 있으면 시각 레벨 내용 한줄씩)

[3] 매매결과
거래: N건
손익: +/-N원
승률: N% (N/N)

진입
. 종목코드 종목명
(한줄에 한 종목, 전일보유 청산이면 "+ 전일보유: 종목코드 종목명" 추가)

청산
. 종목코드 종목명
(한줄에 한 종목)

보유
종목코드 종목명 @ 가격 × 수량
(없으면 "없음")

종목별
✅/❌/➖ 종목코드 종목명  +/-N원 비고
(✅ 수익, ❌ 손실, ➖ 소폭)

[4] 코드수정
(수정했으면 변경 내용 요약, 없으면 "수정 없음")

아래는 오늘 장중 로그야:
PROMPT_EOF

# 로그를 stdin으로 pipe, 프롬프트는 -p 인자로 전달
cat "${LOG_FILE}" | claude -p "$(cat "${CLAUDE_PROMPT_FILE}")" > "${ANALYSIS_FILE}" 2>&1
CLAUDE_EXIT=$?

if [ ${CLAUDE_EXIT} -ne 0 ] || [ "$(wc -c < "${ANALYSIS_FILE}")" -lt 50 ]; then
    die "Claude 분석 실패 (exit=${CLAUDE_EXIT}, output=$(head -c 200 "${ANALYSIS_FILE}"))"
fi

# ── 3단계: 수정 여부 판단 ───────────────────────────────────
echo "[3/4] 수정 여부 확인 중..."

POST_DIFF=$(git diff --name-only 2>/dev/null || true)

NEW_CHANGES=""
for f in ${POST_DIFF}; do
    if ! echo "${PRE_DIFF}" | grep -qx "${f}"; then
        NEW_CHANGES="${NEW_CHANGES} ${f}"
    fi
done

NEW_CHANGES=$(echo "${NEW_CHANGES}" | xargs)

if [ -n "${NEW_CHANGES}" ]; then
    echo "  → 코드 수정 감지: ${NEW_CHANGES}"
    MODIFIED=true
else
    echo "  → 수정 없음"
    MODIFIED=false
fi

# ── 4단계: 텔레그램 보고 ────────────────────────────────────
echo "[4/4] 텔레그램 보고 중..."

SUMMARY=$(cat "${ANALYSIS_FILE}" | head -c 3800)

if [ "${MODIFIED}" = true ]; then
    DIFF_DETAIL=$(git diff --stat -- ${NEW_CHANGES} 2>/dev/null | head -20)

    send_telegram "🔧 [Easy Trader 일일 리뷰] ${TODAY}

${SUMMARY}

✏️ 코드 수정됨
${DIFF_DETAIL}

⚠️ 확인 후 재배포 필요: ./rebuild.sh"

    git add -- ${NEW_CHANGES}
    git commit -m "auto: ${TODAY} 일일 리뷰 자동 수정"

    echo "  → git commit 완료, 재배포는 수동"
else
    send_telegram "✅ [Easy Trader 일일 리뷰] ${TODAY}

${SUMMARY}

수정 사항 없음"
fi

echo "완료."