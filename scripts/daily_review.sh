#!/bin/bash
# ============================================================
# daily_review.sh
# 장 마감 후 로그 수집 → Claude Code 분석 → 자동 수정 → 텔레그램 보고
# Cloud Scheduler: 15:40 KST (월~금)
# ============================================================

set -uo pipefail  # -e 제거: 중간 실패 시 텔레그램 에러 보고 후 종료하도록 수동 처리

# ── 설정 ────────────────────────────────────────────────────
PROJECT_DIR="/home/f1227/easy-trader"
CONTAINER_NAME="easy-trader"
LOG_FILE="/tmp/easy_trader_$(date '+%Y%m%d').log"
ANALYSIS_FILE="/tmp/analysis_$(date '+%Y%m%d').txt"
TODAY=$(date '+%Y-%m-%d')
# .env 직접 로드
set -a
source /home/f1227/easy-trader/.env
set +a

BOT_TOKEN="${TELEGRAM_BOT_TOKEN}"
CHAT_ID="${TELEGRAM_CHAT_ID}"

# ── 함수 ────────────────────────────────────────────────────
send_telegram() {
    local msg="$1"
    curl -s -X POST "https://api.telegram.org/bot${BOT_TOKEN}/sendMessage" \
        -d chat_id="${CHAT_ID}" \
        -d parse_mode="Markdown" \
        -d text="${msg}" > /dev/null
}

die() {
    local msg="$1"
    echo "[ERROR] ${msg}" >&2
    send_telegram "❌ *[daily_review 오류] ${TODAY}*

${msg}"
    exit 1
}

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

# Claude 실행 전 git 스냅샷 (pre-existing 변경사항과 구분)
cd "${PROJECT_DIR}"
PRE_DIFF=$(git diff --name-only 2>/dev/null || true)

cat > /tmp/claude_prompt.txt << 'PROMPT_EOF'
오늘 장중 로그 파일을 읽고 아래 항목을 분석해줘.

[1] 시나리오 검증
- 08:50: 보유종목 감시목록 편입 / WS 구독 / 워밍업 정상 완료 여부
- 09:00: 보유종목 신호처리 시작 여부 (_trade_filter_done 무관)
- 09:20: 거래대금 필터 실행 및 신규 종목 편입 여부

[2] 에러/경고 분석
- ERROR, WARNING 로그 목록과 원인 추정

[3] 매매 결과 요약
- 진입/청산 종목, 손익

[4] 코드 수정 필요 여부
- 수정이 필요하면 engine.py를 직접 수정해줘
- 수정 후 변경 내용 요약해줘
- 수정 없으면 '수정 없음' 명시
PROMPT_EOF

# stdin을 /dev/null로 닫아 3초 대기 경고 방지
claude --print "$(cat /tmp/claude_prompt.txt)

로그:
$(cat "${LOG_FILE}")" < /dev/null > "${ANALYSIS_FILE}" 2>&1
CLAUDE_EXIT=$?

if [ ${CLAUDE_EXIT} -ne 0 ] || [ "$(wc -c < "${ANALYSIS_FILE}")" -lt 50 ]; then
    die "Claude 분석 실패 (exit=${CLAUDE_EXIT}, output=$(cat "${ANALYSIS_FILE}" | head -c 200))"
fi

# ── 3단계: 수정 여부 판단 ───────────────────────────────────
echo "[3/4] 수정 여부 확인 중..."

# Claude 실행 후 새로 생긴 변경사항만 감지 (pre-existing 제외)
POST_DIFF=$(git diff --name-only 2>/dev/null || true)
NEW_CHANGES=""
for f in ${POST_DIFF}; do
    if ! echo "${PRE_DIFF}" | grep -qx "${f}"; then
        NEW_CHANGES="${NEW_CHANGES} ${f}"
    fi
done
NEW_CHANGES=$(echo "${NEW_CHANGES}" | xargs)  # trim

if [ -n "${NEW_CHANGES}" ]; then
    echo "  → 코드 수정 감지: ${NEW_CHANGES}"
    MODIFIED=true
else
    echo "  → 수정 없음"
    MODIFIED=false
fi

# ── 4단계: 텔레그램 보고 ────────────────────────────────────
echo "[4/4] 텔레그램 보고 중..."

# 분석 결과 요약 (줄 단위로 잘라 한글 깨짐 방지)
SUMMARY=$(head -n 30 "${ANALYSIS_FILE}" | head -c 1500)

if [ "${MODIFIED}" = true ]; then
    DIFF_DETAIL=$(git diff --stat -- ${NEW_CHANGES} 2>/dev/null | head -20)
    send_telegram "🔧 *[Easy Trader 일일 리뷰] ${TODAY}*

📋 *분석 요약*
${SUMMARY}

✏️ *코드 수정됨*
\`\`\`
${DIFF_DETAIL}
\`\`\`

⚠️ 확인 후 재배포 필요: \`./rebuild.sh\`"

    # Claude가 수정한 파일만 커밋 (git add -A 사용 금지)
    git add -- ${NEW_CHANGES}
    git commit -m "auto: ${TODAY} 일일 리뷰 자동 수정"
    echo "  → git commit 완료 (재배포는 수동)"

else
    send_telegram "✅ *[Easy Trader 일일 리뷰] ${TODAY}*

📋 *분석 요약*
${SUMMARY}

수정 사항 없음"
fi

echo "완료."
