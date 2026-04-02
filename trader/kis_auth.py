# -*- coding: utf-8 -*-
"""
kis_auth.py
KIS API 전체 통신 인프라 - Firestore & Docker 최적화 버전
"""

import asyncio
import json
import logging
import os
import time
from base64 import b64decode
from collections import namedtuple
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from io import StringIO

import pandas as pd
import requests
import websockets
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from google.cloud import firestore  # pip install google-cloud-firestore 필수

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────
# 1. Firestore 초기화 (ADC 방식)
# ──────────────────────────────────────────────────────
# 도커 실행 시 GOOGLE_APPLICATION_CREDENTIALS 환경변수가 설정되어 있으면 자동으로 읽음
try:
    db = firestore.Client()
    logger.info("✅ Firestore 클라이언트 초기화 성공")
except Exception as e:
    logger.error(f"❌ Firestore 초기화 실패: {e}")
    db = None

# ──────────────────────────────────────────────────────
# 설정 로드 및 전역 상태
# ──────────────────────────────────────────────────────
def _load_cfg() -> dict:
    return {
        "my_app":          os.environ.get("KIS_APP_KEY", ""),
        "my_sec":          os.environ.get("KIS_APP_SECRET", ""),
        "my_acct_stock":   os.environ.get("KIS_ACCOUNT_STOCK", ""),
        "my_acct_future":  os.environ.get("KIS_ACCOUNT_FUTURE", ""),
        "my_prod":         os.environ.get("KIS_PRODUCT_CODE", "01"),
        "my_htsid":        os.environ.get("KIS_HTS_ID", ""),
        "my_agent":        "Mozilla/5.0",
        "paper_app":       os.environ.get("KIS_PAPER_APP_KEY", os.environ.get("KIS_APP_KEY", "")),
        "paper_sec":       os.environ.get("KIS_PAPER_APP_SECRET", os.environ.get("KIS_APP_SECRET", "")),
        "my_paper_stock":  os.environ.get("KIS_PAPER_ACCOUNT_STOCK", ""),
        "my_paper_future": os.environ.get("KIS_PAPER_ACCOUNT_FUTURE", ""),
        "prod": "https://openapi.koreainvestment.com:9443",
        "vps":  "https://openapivts.koreainvestment.com:29443",
        "ops":  "ws://ops.koreainvestment.com:21000",
        "vops": "ws://ops.koreainvestment.com:31000",
    }

_cfg = _load_cfg()
_TRENV = tuple()
_last_auth_time = datetime.now()
_autoReAuth = False
_DEBUG = os.environ.get("KIS_DEBUG", "false").lower() == "true"
_isPaper = False
_smartSleep = 0.1
TOKEN_DOC = os.environ.get("FIREBASE_TOKEN_DOC", "config/kis_token")

BASE_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "text/plain",
    "charset": "UTF-8",
    "User-Agent": _cfg["my_agent"],
}

REAUTH_ERROR_CODES = {
    "401",
    "EGW00121",
    "EGW00122",
    "EGW00123",
    "EGW00205",
}

RATE_LIMIT_CODES = {
    "EGW00201",
}

RETRYABLE_CODES = {
    "EGW00001",
    "EGW00002",
    "EGW00301",
    "EGW00302",
    "500",
}

CONFIG_ERROR_CODES = {
    "EGW00102",
    "EGW00103",
    "EGW00104",
    "EGW00105",
    "EGW00304",
}

# ──────────────────────────────────────────────────────
# 환경 설정 함수
# ──────────────────────────────────────────────────────
def _setTRENV(cfg: dict):
    nt1 = namedtuple("KISEnv", ["my_app", "my_sec", "my_acct", "my_prod", "my_htsid", "my_token", "expires_at", "my_url", "my_url_ws"])
    global _TRENV
    _TRENV = nt1(**cfg)

def changeTREnv(token_key: str | None, svr: str = "prod", product: str = None, expires_at: str | None = None):
    global _isPaper, _smartSleep
    product = product or _cfg["my_prod"]
    cfg = {}

    if svr == "prod":
        cfg["my_app"], cfg["my_sec"], _isPaper, _smartSleep = _cfg["my_app"], _cfg["my_sec"], False, 0.05
    elif svr == "vps":
        cfg["my_app"], cfg["my_sec"], _isPaper, _smartSleep = _cfg["paper_app"], _cfg["paper_sec"], True, 0.5

    acct_map = {
        ("prod", "01"): "my_acct_stock", ("prod", "03"): "my_acct_future",
        ("prod", "08"): "my_acct_future", ("prod", "22"): "my_acct_stock",
        ("prod", "29"): "my_acct_stock", ("vps", "01"): "my_paper_stock",
        ("vps", "03"): "my_paper_future",
    }
    cfg["my_acct"] = _cfg.get(acct_map.get((svr, product), "my_acct_stock"), "")
    cfg["my_prod"], cfg["my_htsid"] = product, _cfg["my_htsid"]
    cfg["my_url"], cfg["my_token"] = _cfg[svr], token_key or ""
    cfg["expires_at"] = expires_at
    cfg["my_url_ws"] = _cfg["ops" if svr == "prod" else "vops"]
    _setTRENV(cfg)

def isPaperTrading(): return _isPaper
def getTREnv(): return _TRENV

# ──────────────────────────────────────────────────────
# Firestore 기반 토큰 핸들링
# ──────────────────────────────────────────────────────
def read_token_from_firestore():
    if not db: return None
    try:
        doc = db.document(TOKEN_DOC).get()
        return doc.to_dict() if doc.exists else None
    except Exception as e:
        logger.error(f"Firestore 읽기 오류: {e}"); return None

def save_token_to_firestore(token: str, expires_at: str):
    if not db: return
    try:
        db.document(TOKEN_DOC).set({
            'access_token': token,
            'expires_at': expires_at,
            'issued_at': datetime.now(timezone.utc).isoformat()
        })
    except Exception as e:
        logger.error(f"Firestore 저장 오류: {e}")

# ──────────────────────────────────────────────────────
# 인증 (Auth) 로직
# ──────────────────────────────────────────────────────
def auth(svr: str = "prod", product: str = None):
    product = product or _cfg["my_prod"]
    app_key = _cfg["my_app" if svr == "prod" else "paper_app"]
    app_secret = _cfg["my_sec" if svr == "prod" else "paper_sec"]

    my_token = None
    data = read_token_from_firestore()

    if data:
        token, expires_at = data.get('access_token'), data.get('expires_at')
        if token and expires_at:
            expire_dt = datetime.fromisoformat(expires_at.replace(' ', 'T'))
            if expire_dt.tzinfo is None: expire_dt = expire_dt.replace(tzinfo=ZoneInfo('Asia/Seoul'))
            # 만료 1시간 전까지 재사용
            if datetime.now(timezone.utc) < expire_dt - timedelta(hours=1):
                my_token = token
                expire_time_str = expires_at
                logger.info(f"✅ Firestore 토큰 재사용 (만료: {expires_at})")

    if my_token is None:
        logger.info("🔄 KIS API 토큰 신규 발급 시도...")
        url = f"{_cfg[svr]}/oauth2/tokenP"
        res = requests.post(url, data=json.dumps({"grant_type": "client_credentials", "appkey": app_key, "appsecret": app_secret}), 
                            headers={"Content-Type": "application/json"})
        
        if res.status_code == 200:
            result = res.json()
            my_token = result["access_token"]
            expires_in = int(result.get('expires_in', 72000))
            expire_time_str = (datetime.now(timezone.utc) + timedelta(seconds=expires_in)).isoformat()
            save_token_to_firestore(my_token, expire_time_str)
            logger.info("✅ 새 토큰 발급 및 Firestore 저장 완료")
        else:
            raise RuntimeError(f"❌ 토큰 발급 실패: {res.text}")

    changeTREnv(my_token, svr, product, expire_time_str)
    global _last_auth_time
    _last_auth_time = datetime.now()

def reAuth(svr: str = "prod", product: str = None):
    auth(svr, product)

def _make_headers(ptr_id: str, tr_cont: str, appendHeaders=None) -> dict:
    env = getTREnv()
    tr_id = "V" + ptr_id[1:] if ptr_id[0] in ("T", "J", "C") and isPaperTrading() else ptr_id

    headers = {
        **BASE_HEADERS,
        "authorization": f"Bearer {env.my_token}",
        "appkey": env.my_app,
        "appsecret": env.my_sec,
        "tr_id": tr_id,
        "custtype": "P",
        "tr_cont": tr_cont,
    }

    if appendHeaders:
        headers.update(appendHeaders)

    return headers

def smart_sleep(): time.sleep(_smartSleep)

# ──────────────────────────────────────────────────────
# REST API 래퍼 및 호출
# ──────────────────────────────────────────────────────
class APIResp:
    def __init__(self, resp):
        self._rescode, self._resp = resp.status_code, resp
        self._header, self._body = self._setHeader(), self._setBody()
        self._err_code, self._err_message = getattr(self._body, 'msg_cd', ''), getattr(self._body, 'msg1', '')

    def isOK(self):
        try: return self.getBody().rt_cd == "0"
        except: return False

    def getResCode(self): return self._rescode
    def getHeader(self): return self._header
    def getBody(self): return self._body
    def getErrorCode(self): return self._err_code
    def getErrorMessage(self): return self._err_message

    def _setHeader(self):
        fld = {k: v for k, v in self._resp.headers.items() if k.islower()}
        return namedtuple("header", fld.keys())(**fld)

    def _setBody(self):
        data = self._resp.json()
        return namedtuple("body", data.keys())(**data)

    def printError(self, url=""):
        logger.error(f"API Error {self._rescode} | url={url} | msg={self.getErrorMessage()}")

class APIRespError:
    def __init__(self, status_code, error_text):
        self.status_code = status_code
        self._rescode     = status_code
        self._err_code    = str(status_code)
        self._err_message = error_text

        # ─── 상세 에러 코드(msg_cd) 추출 로직 추가 ───
        if error_text and error_text.strip().startswith('{'):
            try:
                import json
                data = json.loads(error_text)
                # KIS 표준 에러 필드인 msg_cd를 추출하여 _err_code에 할당
                self._err_code = data.get("msg_cd", str(status_code))
                # 에러 메시지도 구체적인 내용(msg1)이 있다면 업데이트
                if "msg1" in data:
                    self._err_message = data["msg1"]
            except Exception:
                # 파싱 실패 시 초기 설정값 유지
                pass
        # ──────────────────────────────────────────────
    def isOK(self): return False
    def getResCode(self): return self._rescode
    def getErrorCode(self): return self._err_code
    def getErrorMessage(self): return self._err_message
    def printError(self, url=""):
        logger.error(f"API Error {self._rescode} | url={url} | msg={self._err_message}")
    def getBody(self):
        class EB:
            def __getattr__(self, n): return None
        return EB()
    def getHeader(self):
        class EH:
            tr_cont = ""
            def __getattr__(self, n): return ""
        return EH()

def _url_fetch(api_url, ptr_id, tr_cont, params, appendHeaders=None, postFlag=False, hashFlag=True):
    env = getTREnv()
    if not env:
        raise RuntimeError("KIS 인증 필요 (auth 먼저 호출)")

    # ──────────────────────────────────────────────────────
    # 1. expires_at 체크 및 자동 재발급 로직
    # ──────────────────────────────────────────────────────
    if env.expires_at:
        try:
            # ISO 포맷 문자열을 datetime 객체로 변환
            # 'Z'로 끝나는 UTC 시간이나 '+09:00' 같은 시차 정보를 모두 처리합니다.
            expire_dt = datetime.fromisoformat(env.expires_at.replace('Z', '+00:00'))
            
            # 현재 시간(UTC)과 비교하여 만료 10분 전이면 재발급
            # 22:26:31 만료라면, 22:16:31 이후부터 재발급을 시도합니다.
            if datetime.now(timezone.utc) >= expire_dt - timedelta(minutes=10):
                logger.warning(f"토큰 만료 임박(남은시간 < 10분). 재인증을 수행합니다.")
                reAuth(svr="vps" if isPaperTrading() else "prod", product=env.my_prod)
                env = getTREnv()  # 새 토큰이 담긴 환경으로 갱신
        except Exception as e:
            logger.error(f"만료 시간 파싱 중 오류 발생: {e}")

    url = f"{env.my_url}{api_url}"

    def request(headers):
        if postFlag:
            return requests.post(url, headers=headers, data=json.dumps(params), timeout=10)
        return requests.get(url, headers=headers, params=params, timeout=15)

    # 1차 요청
    headers = _make_headers(ptr_id, tr_cont, appendHeaders)

    from requests.exceptions import ReadTimeout, ConnectionError as ReqConnectionError

    MAX_RETRY = 3

    for attempt in range(1, MAX_RETRY + 1):
        try:
            raw = request(headers)
            res = APIResp(raw) if raw.status_code == 200 else APIRespError(raw.status_code, raw.text)

            if res.isOK():
                return res

            code = str(res.getErrorCode())
            msg = res.getErrorMessage()

            logger.warning(
                f"[KIS 실패] ptr_id={ptr_id} code={code} msg={msg} "
                f"status={res.getResCode()} attempt={attempt}/{MAX_RETRY}"
            )

            # 서버 5xx → 재시도
            if raw.status_code >= 500 and attempt < MAX_RETRY and code not in REAUTH_ERROR_CODES:
                sleep_sec = 0.4 * attempt
                logger.warning(f"[KIS 5xx 재시도] sleep={sleep_sec}s")
                time.sleep(sleep_sec)
                continue

            break

        except (ReadTimeout, ReqConnectionError) as e:
            logger.warning(
                f"[KIS timeout 재시도] ptr_id={ptr_id} attempt={attempt}/{MAX_RETRY} error={e}"
            )

            if attempt < MAX_RETRY:
                sleep_sec = 0.5 * attempt
                time.sleep(sleep_sec)
                continue

            logger.exception(f"[KIS 최종 실패] url={url} error={e}")
            return APIRespError(500, str(e))

        except Exception as e:
            logger.exception(f"[KIS 요청 예외] url={url} error={e}")
            return APIRespError(500, str(e))

    if res.isOK():
        return res

    code = str(res.getErrorCode())
    msg = res.getErrorMessage()

    logger.warning(
        f"[KIS 실패] ptr_id={ptr_id} code={code} msg={msg} status={res.getResCode()}"
    )

    # 설정 오류: 재시도 불가
    if code in CONFIG_ERROR_CODES:
        logger.error(f"[KIS 설정 오류] ptr_id={ptr_id} code={code} msg={msg}")
        return res

    # 인증 오류: reAuth 후 1회 재시도
    if code in REAUTH_ERROR_CODES or res.getResCode() == 401:
        logger.warning(f"[KIS 인증 오류] ptr_id={ptr_id} code={code} -> reAuth 후 재시도")

        try:
            reAuth(svr="vps" if isPaperTrading() else "prod", product=env.my_prod)
        except Exception as e:
            logger.exception(f"[reAuth 실패] ptr_id={ptr_id} error={e}")
            return res

        headers = _make_headers(ptr_id, tr_cont, appendHeaders)

        try:
            raw = request(headers)
            retry_res = APIResp(raw) if raw.status_code == 200 else APIRespError(raw.status_code, raw.text)
        except Exception as e:
            logger.exception(f"[재시도 요청 예외] ptr_id={ptr_id} error={e}")
            return APIRespError(500, str(e))

        if retry_res.isOK():
            logger.info(f"[KIS 재인증 후 성공] ptr_id={ptr_id}")
        else:
            logger.error(
                f"[KIS 재인증 후 실패] ptr_id={ptr_id} "
                f"code={retry_res.getErrorCode()} msg={retry_res.getErrorMessage()} "
                f"status={retry_res.getResCode()}"
            )
        return retry_res

    # rate limit: 짧게 대기 후 1회 재시도
    if code in RATE_LIMIT_CODES:
        logger.warning(f"[KIS 호출제한] ptr_id={ptr_id} code={code} -> sleep 후 재시도")
        time.sleep(0.5)

        try:
            headers = _make_headers(ptr_id, tr_cont, appendHeaders)
            raw = request(headers)
            return APIResp(raw) if raw.status_code == 200 else APIRespError(raw.status_code, raw.text)
        except Exception as e:
            logger.exception(f"[rate limit 재시도 예외] ptr_id={ptr_id} error={e}")
            return APIRespError(500, str(e))

    # 서버/게이트웨이 일시 오류: 1회 재시도
    if code in RETRYABLE_CODES:
        logger.warning(f"[KIS 일시 오류] ptr_id={ptr_id} code={code} -> 1회 재시도")

        try:
            headers = _make_headers(ptr_id, tr_cont, appendHeaders)
            raw = request(headers)
            return APIResp(raw) if raw.status_code == 200 else APIRespError(raw.status_code, raw.text)
        except Exception as e:
            logger.exception(f"[일시 오류 재시도 예외] ptr_id={ptr_id} error={e}")
            return APIRespError(500, str(e))

    # 기타 오류는 그대로 반환
    return res

# ──────────────────────────────────────────────────────
# WebSocket 섹션 (기존 로직 유지)
# ──────────────────────────────────────────────────────
_base_headers_ws = {"content-type": "utf-8"}

def auth_ws(svr="prod", product=None):
    product = product or _cfg["my_prod"]
    p = {"grant_type": "client_credentials", "appkey": _cfg["my_app" if svr=="prod" else "paper_app"], 
         "secretkey": _cfg["my_sec" if svr=="prod" else "paper_sec"]}
    res = requests.post(
        f"{_cfg[svr]}/oauth2/Approval",
        data=json.dumps(p),
        headers={**BASE_HEADERS},
    )
    if res.status_code == 200:
        _base_headers_ws["approval_key"] = res.json()["approval_key"]
        changeTREnv(None, svr, product, expires_at=None)
        logger.info("✅ WebSocket approval_key 발급 완료")
    else: raise RuntimeError("WS 인증 실패")

# ... (이하 WebSocket 관련 open_map, data_map, KISWebSocket 클래스 등 기존 코드와 동일) ...
# (지면 관계상 중복되는 데이터 처리 함수 및 클래스는 원본과 동일하게 유지하시면 됩니다)

open_map, data_map = {}, {}

def add_open_map(name, request, data, kwargs=None):
    if name not in open_map: open_map[name] = {"func": request, "items": [], "kwargs": kwargs}
    open_map[name]["items"] += data if isinstance(data, list) else [data]

def add_data_map(tr_id, columns=None, encrypt=None, key=None, iv=None):
    if tr_id not in data_map: data_map[tr_id] = {"columns": [], "encrypt": False, "key": None, "iv": None}
    if columns: data_map[tr_id]["columns"] = columns
    if encrypt: data_map[tr_id]["encrypt"] = encrypt
    if key: data_map[tr_id]["key"] = key
    if iv: data_map[tr_id]["iv"] = iv

def aes_cbc_base64_dec(key, iv, cipher_text):
    cipher = AES.new(key.encode("utf-8"), AES.MODE_CBC, iv.encode("utf-8"))
    return bytes.decode(unpad(cipher.decrypt(b64decode(cipher_text)), AES.block_size))

class KISWebSocket:
    def __init__(self, api_url, max_retries=3):
        self.api_url, self.max_retries, self.retry_count = api_url, max_retries, 0
        self.on_result, self.result_all_data = None, False

    async def __subscriber(self, ws):
        async for raw in ws:
            if raw[0] in ["0", "1"]:
                d1 = raw.split("|")
                tr_id, dm, d = d1[1], data_map[d1[1]], d1[3]
                if dm.get("encrypt") == "Y": d = aes_cbc_base64_dec(dm["key"], dm["iv"], d)
                df = pd.read_csv(StringIO(d), header=None, sep="^", names=dm["columns"], dtype=object)
                if self.on_result: self.on_result(ws, tr_id, df, dm)
            else:
                # 시스템 메시지 처리 (PingPong 등)
                rdic = json.loads(raw)
                tr_id = rdic["header"]["tr_id"]
                if tr_id == "PINGPONG": await ws.pong(raw)
                elif "body" in rdic and "output" in rdic["body"]:
                    add_data_map(tr_id, encrypt=rdic["header"].get("encrypt"), 
                                 key=rdic["body"]["output"].get("key"), iv=rdic["body"]["output"].get("iv"))

    async def __runner(self):
        url = f"{getTREnv().my_url_ws}{self.api_url}"
        while self.retry_count < self.max_retries:
            try:
                async with websockets.connect(url) as ws:
                    for name, obj in open_map.items():
                        for item in obj["items"]:
                            msg, cols = obj["func"]("1", item, **(obj["kwargs"] or {}))
                            add_data_map(msg["body"]["input"]["tr_id"], columns=cols)
                            await ws.send(json.dumps(msg))
                            smart_sleep()
                    await self.__subscriber(ws)
            except Exception as e:
                logger.warning(f"WS 재연결 시도 ({self.retry_count+1}): {e}")
                self.retry_count += 1
                await asyncio.sleep(2)

    def start(self, on_result, result_all_data=False):
        self.on_result, self.result_all_data = on_result, result_all_data
        asyncio.run(self.__runner())