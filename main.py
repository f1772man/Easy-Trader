"""
main.py
Flask 앱 + 백그라운드 트레이딩 엔진
"""

import os
import threading
import logging
from flask import Flask, jsonify
from dotenv import load_dotenv
load_dotenv()  # 가장 먼저
from logging_config import setup_logging
setup_logging()

"""
# 로그 설정
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
"""
logger = logging.getLogger(__name__)

from trader.engine import TradingEngine

app    = Flask(__name__)
engine = TradingEngine()


# ── 헬스 엔드포인트 ──────────────────────────────────
@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "engine_running": engine.is_running,
        "last_tick": engine.last_tick_time,
    }), 200


@app.route("/status")
def status():
    return jsonify(engine.get_status()), 200


@app.route("/stop", methods=["POST"])
def stop():
    engine.stop()
    return jsonify({"status": "stopped"}), 200


# ── 엔진 시작 ────────────────────────────────────────
if __name__ == "__main__":
    t = threading.Thread(target=engine.run, daemon=True)
    t.start()
    logger.info("✅ 백그라운드 엔진 스레드 시작")

    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
