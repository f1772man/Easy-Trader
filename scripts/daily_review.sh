#!/bin/bash
# ============================================================
# daily_review.sh
# 장 마감 후 로그 수집 → Claude Code 분석 → 자동 수정 → 텔레그램 보고
# Cloud Scheduler: 15:40 KST (월~금)
# ============================================================

set -euo pipefail

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

# ── 1단계: 로그 수집 ────────────────────────────────────────
echo "[1/4] 오늘 로그 수집 중..."
# 수정
docker logs "${CONTAINER_NAME}" \
    --since "${TODAY}T08:40:00" \
    --until "${TODAY}T15:35:00" \
    > "${LOG_FILE}" 2>&1

LINE_COUNT=$(wc -l < "${LOG_FILE}")
echo "  → ${LINE_COUNT}줄 수집 완료: ${LOG_FILE}"

if [ "${LINE_COUNT}" -lt 10 ]; then
    send_telegram "⚠️ [daily_review] 로그가 너무 적습니다 (${LINE_COUNT}줄). 컨테이너 상태를 확인하세요."
    exit 1
fi

# ── 2단계: Claude Code 분석 ─────────────────────────────────
# ── 2단계: Claude Code 분석 ─────────────────────────────────
echo "[2/4] Claude Code 분석 중..."

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

cd "${PROJECT_DIR}"
claude --print "$(cat /tmp/claude_prompt.txt)

로그:
$(tail -n 500 ${LOG_FILE})" > "${ANALYSIS_FILE}" 2>&1

# ── 3단계: 수정 여부 판단 ───────────────────────────────────
echo "[3/4] 수정 여부 확인 중..."

# git diff로 실제 변경 감지
DIFF_OUTPUT=$(git diff --name-only 2>/dev/null || echo "")

if [ -n "${DIFF_OUTPUT}" ]; then
    echo "  → 코드 수정 감지: ${DIFF_OUTPUT}"
    MODIFIED=true
else
    echo "  → 수정 없음"
    MODIFIED=false
fi

# ── 4단계: 텔레그램 보고 ────────────────────────────────────
echo "[4/4] 텔레그램 보고 중..."

# 분석 결과 요약 (앞 800자)
SUMMARY=$(head -c 800 "${ANALYSIS_FILE}")

if [ "${MODIFIED}" = true ]; then
    # 수정된 경우: diff 포함 보고
    DIFF_DETAIL=$(git diff --stat 2>/dev/null | head -20)
    send_telegram "🔧 *[Easy Trader 일일 리뷰] ${TODAY}*

📋 *분석 요약*
${SUMMARY}

✏️ *코드 수정됨*
\`\`\`
${DIFF_DETAIL}
\`\`\`

⚠️ 확인 후 재배포 필요: \`./deploy.sh\`"

    # git commit (재배포는 수동)
    git add -A
    git commit -m "auto: ${TODAY} 일일 리뷰 자동 수정"
    echo "  → git commit 완료 (재배포는 수동)"

else
    send_telegram "✅ *[Easy Trader 일일 리뷰] ${TODAY}*

📋 *분석 요약*
${SUMMARY}

수정 사항 없음"
fi

echo "완료."
