"""
telegram.py
텔레그램 알림 모듈 - GAS sendTelegram을 Python으로 변환
"""

import os
import time
import logging
import requests
from typing import Union

logger = logging.getLogger(__name__)

MAX_LEN = 3900   # 텔레그램 메시지 최대 길이


def send_telegram(content: Union[str, dict]) -> None:
    """
    텔레그램 전송
    content: 문자열 또는 Daily 리포트 딕셔너리
    """
    token   = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        logger.warning("텔레그램 설정이 없습니다.")
        return

    blocks = _build_blocks(content)
    messages = _pack_blocks(blocks, MAX_LEN)

    url = f"https://api.telegram.org/bot{token}/sendMessage"

    for idx, msg in enumerate(messages):
        payload = {
            "chat_id": chat_id,
            "text": msg,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        try:
            res = requests.post(url, json=payload, timeout=10)
            if res.status_code != 200:
                logger.warning(f"[HTML 전송 실패] {res.status_code} {res.text}")
                # parse_mode 제거 후 재시도
                del payload["parse_mode"]
                requests.post(url, json=payload, timeout=10)

            if idx < len(messages) - 1:
                time.sleep(0.5)
        except Exception as e:
            logger.error(f"텔레그램 전송 오류: {e}")


# ── 매매 신호 전용 헬퍼 ──────────────────────────────

def notify_buy(symbol: str, name: str, price: int, reason: str, energy_score: int = 0) -> None:
    display = f"{_esc(name)}({_esc(symbol)})" if name else _esc(symbol)
    msg = (
        f"🟢 <b>[매수 체결]</b>\n"
        f"종목: <b>{display}</b>\n"
        f"가격: {price:,}원\n"
        f"사유: {_esc(reason)}\n"
        f"에너지 점수: {energy_score}점"
    )
    send_telegram(msg)


def notify_sell(symbol: str, name: str, price: int, reason: str, profit_pct: float) -> None:
    display = f"{_esc(name)}({_esc(symbol)})" if name else _esc(symbol)
    emoji = "🔴" if profit_pct < 0 else "🟡"
    msg = (
        f"{emoji} <b>[매도 체결]</b>\n"
        f"종목: <b>{display}</b>\n"
        f"가격: {price:,}원\n"
        f"수익률: {profit_pct:+.2f}%\n"
        f"사유: {_esc(reason)}"
    )
    send_telegram(msg)


def notify_error(msg: str) -> None:
    send_telegram(f"❌ <b>[오류]</b>\n{_esc(msg)}")


def notify_heartbeat(status: dict) -> None:
    msg = (
        f"💓 <b>[Heartbeat]</b>\n"
        f"엔진 상태: {'✅ 정상' if status.get('running') else '❌ 중지'}\n"
        f"마지막 틱: {status.get('last_tick', '-')}\n"
        f"보유 종목: {status.get('holding_count', 0)}개"
    )
    send_telegram(msg)


# ── 내부 함수 ────────────────────────────────────────

def _build_blocks(content: Union[str, dict]) -> list:
    """content → 블록 리스트"""
    if isinstance(content, str):
        return [content]

    # 딕셔너리(Daily 리포트) 처리
    blocks = ["📊 <b>[Daily Strategy 리포트]</b>"]
    titles = {
        "squeeze":          "🔋 [에너지 응축 (VCP)]",
        "breakout":         "🚀 [거래량 돌파]",
        "underpriced":      "📉 [낙폭과대]",
        "goldenCrossShort": "📗 [단기골든크로스]",
        "goldenCrossMid":   "📘 [중기골든크로스]",
        "goldenCrossLong":  "📕 [장기골든크로스]",
    }

    has_data = False
    for key, items in content.items():
        if not items:
            continue
        has_data = True
        blocks.append(f"<b>{_esc(titles.get(key, key))}</b>")
        for r in items:
            name  = r.get("name", "")
            score = float(r.get("score", 0))
            if key == "breakout":
                vol = r.get("volume", 0)
                vol_str = f"{vol/10000:.1f}만" if vol >= 10000 else str(vol)
                blocks.append(f"<b>{_esc(name)}</b> : 거래량 {_esc(vol_str)}주 ({score:.1f}배 폭발)")
            else:
                blocks.append(f"<b>{_esc(name)}</b> : {score:.1f}점")

    if not has_data:
        blocks.append("오늘 조건에 맞는 종목이 없습니다.")
    return blocks


def _pack_blocks(blocks: list, max_len: int) -> list:
    """블록들을 max_len 이하로 묶어서 메시지 리스트 반환"""
    messages = []
    current  = ""
    for block in blocks:
        candidate = (current + "\n\n" + block).strip()
        if len(candidate) > max_len:
            if current:
                messages.append(current.strip())
            current = block
        else:
            current = candidate
    if current:
        messages.append(current.strip())
    return messages


def _esc(text: str) -> str:
    """HTML 특수문자 이스케이프"""
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )
