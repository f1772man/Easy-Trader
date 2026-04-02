"""
firebase.py
- Realtime Database: system_status, trade_signals (heartbeat, 신호 기록)
- Firestore: positions (다중 종목 포지션 관리)
"""

import os
import logging
import time
from typing import Optional
from datetime import datetime, timedelta
import pytz
from google.cloud.firestore_v1.base_query import FieldFilter
from http.client import RemoteDisconnected

logger = logging.getLogger(__name__)

# Realtime Database
try:
    import firebase_admin
    from firebase_admin import credentials, db
    FIREBASE_AVAILABLE = True
except ImportError:
    FIREBASE_AVAILABLE = False
    logger.warning("firebase_admin 미설치 - Realtime DB 비활성화")

# Firestore
try:
    from google.cloud import firestore as _firestore
    FIRESTORE_AVAILABLE = True
except ImportError:
    FIRESTORE_AVAILABLE = False
    logger.warning("google-cloud-firestore 미설치 - Firestore 비활성화")

KST  = pytz.timezone('Asia/Seoul')
ROOT = "EASY_TRADER"


class FirebaseClient:
    def __init__(self):
        self._db  = None   # Realtime DB
        self._fs  = None   # Firestore
        self._error_count_today = 0
        self._recent_error = "None"
        self._init()

    def _init(self):
        # Realtime Database
        if FIREBASE_AVAILABLE:
            key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            db_url   = os.environ.get("FIREBASE_DATABASE_URL")
            try:
                if not firebase_admin._apps:
                    cred = credentials.Certificate(key_path) if key_path else credentials.ApplicationDefault()
                    firebase_admin.initialize_app(cred, {"databaseURL": db_url})
                self._db = db
                logger.info("✅ Firebase Realtime DB 연결 완료")
            except Exception as e:
                logger.error(f"Realtime DB 초기화 실패: {e}")

        # Firestore
        if FIRESTORE_AVAILABLE:
            try:
                self._fs = _firestore.Client()
                logger.info("✅ Firebase 연결 완료")
            except Exception as e:
                logger.error(f"Firestore 초기화 실패: {e}")

    def _ref(self, path: str):
        return self._db.reference(f"{ROOT}/{path}") if self._db else None

    def _rtdb_update(self, path: str, data: dict, retries: int = 2) -> bool:
        """Realtime DB update with silent retry on RemoteDisconnected."""
        ref = self._ref(path)
        if not ref:
            return False
        for attempt in range(retries + 1):
            try:
                ref.update(data)
                return True
            except (RemoteDisconnected, ConnectionResetError, OSError) as e:
                if attempt < retries:
                    logger.debug(f"[RTDB] 연결 끊김 → 재시도 {attempt + 1}/{retries} ({path}): {e}")
                    time.sleep(0.5 * (attempt + 1))
                else:
                    logger.warning(f"[RTDB] {path} 업데이트 실패 (재시도 {retries}회 소진): {e}")
                    return False
            except Exception as e:
                logger.error(f"[RTDB] {path} 업데이트 오류: {e}")
                return False
        return False

    # ── Heartbeat (Realtime DB) ───────────────────────
    def update_heartbeat(self, status: dict) -> bool:
        now = datetime.now(KST)
        return self._rtdb_update("system_status", {
            "engine_running": status.get("running", False),
            "api_connected":  True,
            "last_heartbeat": now.strftime("%Y-%m-%d %H:%M:%S"),
            "next_run":       (now + timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S"),
            "error_log": {
                "error_count_today": self._error_count_today,
                "recent_error":      self._recent_error,
            },
        })

    def log_error(self, error_msg: str) -> bool:
        self._error_count_today += 1
        self._recent_error = f"{datetime.now(KST).strftime('%H:%M:%S')} {error_msg[:100]}"
        return self._rtdb_update("system_status/error_log", {
            "error_count_today": self._error_count_today,
            "recent_error":      self._recent_error,
        })

    def reset_error_count(self) -> bool:
        self._error_count_today = 0
        self._recent_error = "None"
        return self._rtdb_update("system_status/error_log", {
            "error_count_today": 0,
            "recent_error": "None",
        })

    # ── 포지션 관리 (Firestore) ───────────────────────
    def save_trade_state(self, symbol: str, state: dict) -> bool:
        """Firestore positions/{symbol} — pos dict 그대로 저장"""
        if not self._fs:
            return False
        try:
            doc = {"symbol": symbol}
            doc.update(state)
            self._fs.collection("positions").document(symbol).set(doc)
            return True
        except Exception as e:
            logger.error(f"save_trade_state 실패: {e}")
            return False

    def get_trade_state(self, symbol: str) -> Optional[dict]:
        if not self._fs:
            return None
        try:
            doc = self._fs.collection("positions").document(symbol).get()
            if doc.exists:
                data = doc.to_dict()
                if data.get("is_holding"):
                    return data
            return None
        except Exception as e:
            logger.error(f"get_trade_state 실패: {e}")
            return None

    def delete_trade_state(self, symbol: str) -> bool:
        """매도 시 is_holding=False로 변경"""
        if not self._fs:
            return False
        now = datetime.now(KST)
        try:
            self._fs.collection("positions").document(symbol).update({
                "is_holding": False,
                "updated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
            })
            return True
        except Exception as e:
            logger.error(f"delete_trade_state 실패: {e}")
            return False

    # 위치: delete_trade_state() 바로 아래 추가
    def cleanup_sold_positions(self) -> bool:
        """is_holding=False 문서 전체 삭제 (날짜 변경 시 호출)"""
        if not self._fs:
            return False
        try:
            docs = self._fs.collection("positions").where(
                filter=FieldFilter("is_holding", "==", False)
            ).stream()
            for doc in docs:
                doc.reference.delete()
            logger.info("[포지션정리] is_holding=False 문서 삭제 완료")
            return True
        except Exception as e:
            logger.error(f"cleanup_sold_positions 실패: {e}")
            return False
    
    def get_sold_positions(self) -> list:
        """is_holding=False 종목 반환 (재시작 시 당일매도 복원용)"""
        if not self._fs:
            return []
        try:
            docs = self._fs.collection("positions").where(
                filter=FieldFilter("is_holding", "==", False)
            ).stream()
            result = []
            for doc in docs:
                data = doc.to_dict() or {}
                sym = data.get("symbol", doc.id)
                result.append({"symbol": sym})
            logger.info(f"[당일매도복원] {len(result)}개")
            return result
        except Exception as e:
            logger.error(f"get_sold_positions 실패: {e}")
            return []

    def get_all_positions(self) -> list:
        """positions 전체 반환 (재시작 시 복원용)"""
        if not self._fs:
            return []
        try:
            docs = self._fs.collection("positions").stream()
            result = []
            for doc in docs:
                data = doc.to_dict() or {}
                if not data.get("is_holding"):
                    continue
                sym = data.get("symbol", doc.id)
                entry = {"symbol": sym}
                entry.update({k: v for k, v in data.items() if k != "symbol"})
                result.append(entry)
            logger.info(f"[포지션복원] {len(result)}개")
            return result
        except Exception as e:
            logger.error(f"get_all_positions 실패: {e}")
            return []

    # ── 거래 내역 기록 (Firestore) ───────────────────
    def log_trade(self, action: str, symbol: str, data: dict) -> bool:
        """
        Firestore: trade_log/{ts}_{symbol}
        Realtime DB: EASY_TRADER/trade_signals/latest_signal (앱 실시간 표시용)
        """
        now = datetime.now(KST)
        ts  = int(time.time())
        key = f"{ts}_{symbol}"

        entry = {
            "type":       action,
            "symbol":     symbol,
            "name":       data.get("name", symbol),
            "price":      data.get("price", 0),
            "qty":        data.get("qty", 0),
            "reason":     data.get("reason", ""),
            "timestamp":  ts,
            "date":       now.strftime("%Y-%m-%d"),
            "time":       now.strftime("%H:%M:%S"),
        }
        if action == "BUY":
            entry["entry_price"] = data.get("entry_price", 0)
            entry["entry_time"]  = data.get("entry_time", "")
        if action == "SELL":
            entry["profit_pct"]  = data.get("profit_pct", 0)
            entry["entry_price"] = data.get("entry_price", 0)
            entry["entry_time"]  = data.get("entry_time", "")

        # Firestore에 영구 저장
        if self._fs:
            try:
                self._fs.collection("trade_log").document(key).set(entry)
            except Exception as e:
                logger.error(f"log_trade Firestore 실패: {e}")

        # Realtime DB latest_signal 갱신 (앱 실시간 표시용)
        self._rtdb_update("trade_signals/latest_signal", {
            "type":      action,
            "symbol":    symbol,
            "name":      data.get("name", symbol),
            "price":     data.get("price", 0),
            "reason":    data.get("reason", ""),
            "timestamp": ts,
        })

        return True
