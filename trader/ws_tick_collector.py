import asyncio
import json
import logging
import threading
from datetime import datetime, timezone, timedelta
from typing import Optional

from trader.kis_auth import auth_ws, getTREnv, _base_headers_ws

logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))

_CCNL_COLUMNS = [
    "MKSC_SHRN_ISCD", "STCK_CNTG_HOUR", "STCK_PRPR", "PRDY_VRSS_SIGN",
    "PRDY_VRSS", "PRDY_CTRT", "WGHN_AVRG_STCK_PRC", "STCK_OPRC",
    "STCK_HGPR", "STCK_LWPR", "ASKP1", "BIDP1", "CNTG_VOL", "ACML_VOL",
    "ACML_TR_PBMN", "SELN_CNTG_CSNU", "SHNU_CNTG_CSNU", "NTBY_CNTG_CSNU",
    "CTTR", "SELN_CNTG_SMTN", "SHNU_CNTG_SMTN", "CCLD_DVSN", "SHNU_RATE",
    "PRDY_VOL_VRSS_ACML_VOL_RATE", "OPRC_HOUR", "OPRC_VRSS_PRPR_SIGN",
    "OPRC_VRSS_PRPR", "HGPR_HOUR", "HGPR_VRSS_PRPR_SIGN", "HGPR_VRSS_PRPR",
    "LWPR_HOUR", "LWPR_VRSS_PRPR_SIGN", "LWPR_VRSS_PRPR", "BSOP_DATE",
    "NEW_MKOP_CLS_CODE", "TRHT_YN", "ASKP_RSQN1", "BIDP_RSQN1",
    "TOTAL_ASKP_RSQN", "TOTAL_BIDP_RSQN", "VOL_TNRT",
    "PRDY_SMNS_HOUR_ACML_VOL", "PRDY_SMNS_HOUR_ACML_VOL_RATE",
    "HOUR_CLS_CODE", "MRKT_TRTM_CLS_CODE", "VI_STND_PRC"
]


class WsTickCollector:
    def __init__(self, symbols: list):
        self._symbols = [s.strip() for s in symbols if s.strip()]
        self._lock = threading.RLock()

        self._1min_buffer: dict = {}
        self._5min_cache: dict = {}

        self._thread: Optional[threading.Thread] = None
        self._running: bool = False
        self._stop_event: Optional[asyncio.Event] = None

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._ws = None
        self._connected_event: Optional[asyncio.Event] = None

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._run, daemon=True, name="WsTickCollector"
        )
        self._thread.start()
        logger.info(f"[WS] 체결 수신 시작 ({len(self._symbols)}종목)")

    def stop(self, timeout: float = 5.0):
        self._running = False

        if self._loop and self._ws:
            try:
                asyncio.run_coroutine_threadsafe(self._ws.close(), self._loop)
            except Exception:
                pass

        if self._stop_event:
            try:
                self._loop.call_soon_threadsafe(self._stop_event.set)
            except Exception:
                pass

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
            if self._thread.is_alive():
                logger.warning("[WS] 스레드가 timeout 내 종료되지 않음")
            else:
                logger.info("[WS] 스레드 정상 종료 완료")

        self._ws = None
        self._loop = None
        self._connected_event = None
        logger.info("[WS] 체결 수신 중지")

    def get_5min(self, symbol: str) -> list:
        with self._lock:
            return list(self._5min_cache.get(symbol, []))

    def get_1min(self, symbol: str) -> list:
        with self._lock:
            return list(self._1min_buffer.get(symbol, []))

    def reset_day(self):
        with self._lock:
            self._1min_buffer.clear()
            self._5min_cache.clear()
        logger.info("[WS] 일자 변경 → 버퍼 초기화")

    def update_symbols(self, symbols: list):
        new_symbols = [s.strip() for s in symbols if s.strip()]

        with self._lock:
            old_symbols = list(self._symbols)

        if new_symbols == old_symbols:
            return

        old_set = set(old_symbols)
        new_set = set(new_symbols)

        added = sorted(new_set - old_set)
        removed = sorted(old_set - new_set)

        with self._lock:
            self._symbols = new_symbols

        # 아직 WS 연결 전이면 내부 목록만 바꾸고 종료
        if not self._running or not self._loop or not self._ws:
            logger.info(f"[WS] 감시목록 교체(비연결 상태) | 추가={added} | 제거={removed}")
            return

        futures = []

        for sym in added:
            futures.append(
                asyncio.run_coroutine_threadsafe(self._subscribe_symbol(sym), self._loop)
            )

        for sym in removed:
            futures.append(
                asyncio.run_coroutine_threadsafe(self._unsubscribe_symbol(sym), self._loop)
            )

        for fut in futures:
            try:
                fut.result(timeout=2)
            except Exception as e:
                logger.warning(f"[WS] 증분 구독 반영 실패: {e}")

        logger.info(f"[WS] 감시목록 증분 반영 | 추가={added} | 제거={removed}")

    def _run(self):
        loop = asyncio.new_event_loop()
        self._loop = loop
        asyncio.set_event_loop(loop)
        self._stop_event = asyncio.Event()
        self._connected_event = asyncio.Event()

        try:
            loop.run_until_complete(self._ws_main())
        except Exception as e:
            logger.error(f"[WS] 루프 오류: {e}")
        finally:
            self._ws = None
            self._stop_event = None
            self._connected_event = None
            self._loop = None
            loop.close()

    async def _subscribe_symbol(self, sym: str):
        if not self._ws:
            return

        msg = {
            "header": {
                "approval_key": _base_headers_ws.get("approval_key", ""),
                "custtype": "P",
                "tr_type": "1",
                "content-type": "utf-8",
            },
            "body": {
                "input": {
                    "tr_id": "H0STCNT0",
                    "tr_key": sym,
                }
            },
        }
        await self._ws.send(json.dumps(msg))
        await asyncio.sleep(0.05)

    async def _unsubscribe_symbol(self, sym: str):
        if not self._ws:
            return

        msg = {
            "header": {
                "approval_key": _base_headers_ws.get("approval_key", ""),
                "custtype": "P",
                "tr_type": "2",  # 운영 중 반드시 확인 필요
                "content-type": "utf-8",
            },
            "body": {
                "input": {
                    "tr_id": "H0STCNT0",
                    "tr_key": sym,
                }
            },
        }
        await self._ws.send(json.dumps(msg))
        await asyncio.sleep(0.05)

    async def _ws_main(self):
        import websockets

        try:
            auth_ws(svr="prod")
        except Exception as e:
            logger.error(f"[WS] auth_ws 실패: {e}")
            return

        url = f"{getTREnv().my_url_ws}/tryitout"
        max_retry = 10
        retry = 0

        while self._running and retry < max_retry:
            try:
                logger.info(f"[WS] 연결 시도 (retry={retry})")
                async with websockets.connect(url) as ws:
                    self._ws = ws
                    retry = 0

                    if self._connected_event:
                        self._connected_event.set()

                    # 현재 symbols 전체 재구독
                    with self._lock:
                        current_symbols = list(self._symbols)

                    for sym in current_symbols:
                        await self._subscribe_symbol(sym)

                    logger.info(f"[WS] {len(current_symbols)}종목 구독 완료")

                    async for raw in ws:
                        if not self._running or (self._stop_event and self._stop_event.is_set()):
                            await ws.close()
                            break

                        try:
                            if raw[0] not in ("0", "1"):
                                try:
                                    rdic = json.loads(raw)
                                    tr_id = rdic.get("header", {}).get("tr_id", "")
                                    if tr_id == "PINGPONG":
                                        await ws.pong(raw)
                                except Exception:
                                    pass
                            else:
                                self._handle_raw(raw)
                        except Exception as e:
                            logger.debug(f"[WS] 메시지 처리 오류: {e}")

            except Exception as e:
                retry += 1
                self._ws = None
                if self._connected_event:
                    self._connected_event.clear()
                wait = min(2 * retry, 30)
                logger.warning(f"[WS] 연결 끊김 ({retry}/{max_retry}): {e} → {wait}초 후 재시도")
                await asyncio.sleep(wait)

        if retry >= max_retry:
            logger.error("[WS] 최대 재시도 초과 — WebSocket 수신 중단")

    def _handle_raw(self, raw: str):
        if raw[0] not in ("0", "1"):
            return

        parts = raw.split("|")
        if len(parts) < 4:
            return

        tr_id = parts[1]
        if tr_id != "H0STCNT0":
            return

        data_str = parts[3]
        cnt = int(parts[2]) if parts[2].isdigit() else 1
        col_count = len(_CCNL_COLUMNS)
        values = data_str.split("^")

        for i in range(cnt):
            start = i * col_count
            end = start + col_count
            if end > len(values):
                break
            row = dict(zip(_CCNL_COLUMNS, values[start:end]))
            self._on_tick(row)

    def _on_tick(self, row):
        try:
            symbol = str(row.get("MKSC_SHRN_ISCD", "")).strip()
            hour_str = str(row.get("STCK_CNTG_HOUR", "")).strip().zfill(6)
            date_str = str(row.get("BSOP_DATE", "")).strip()
            price = int(row.get("STCK_PRPR", 0) or 0)
            cntg_vol = int(row.get("CNTG_VOL", 0) or 0)

            if not symbol or not price or not date_str:
                return
            if hour_str < "090000" or hour_str > "153000":
                return

            hhmm = hour_str[:4]
            time_key = f"{date_str}_{hhmm}"

            with self._lock:
                is_new = symbol not in self._1min_buffer
            if is_new:
                logger.info(f"[WS] 첫 체결 수신: {symbol} {price}원 {time_key}")

            self._update_1min(symbol, time_key, price, cntg_vol)
            self._update_5min(symbol, time_key)

        except Exception as e:
            logger.debug(f"[WS] _on_tick 오류: {e}")

    def _update_1min(self, symbol: str, time_key: str, price: int, vol: int):
        with self._lock:
            if symbol not in self._1min_buffer:
                self._1min_buffer[symbol] = []
            buf = self._1min_buffer[symbol]

            if buf and buf[-1][0] == time_key:
                row = buf[-1]
                row[2] = max(row[2], price)
                row[3] = min(row[3], price)
                row[4] = price
                row[5] += vol
            else:
                buf.append([time_key, price, price, price, price, vol])

    def _update_5min(self, symbol: str, time_key: str):
        with self._lock:
            buf = list(self._1min_buffer.get(symbol, []))

        if not buf:
            return

        hhmm = time_key.split("_")[1] if "_" in time_key else time_key[-4:]
        h, m = int(hhmm[:2]), int(hhmm[2:])
        bucket_m = (h * 60 + m) // 5 * 5
        cur_bucket = f"{time_key.split('_')[0]}_{bucket_m // 60:02d}{bucket_m % 60:02d}"

        buckets = {}
        for row in buf:
            t = row[0]
            hh = t.split("_")[1] if "_" in t else t[-4:]
            bh, bm = int(hh[:2]), int(hh[2:])
            bkt_m = (bh * 60 + bm) // 5 * 5
            bkt_key = f"{t.split('_')[0]}_{bkt_m // 60:02d}{bkt_m % 60:02d}"

            if bkt_key not in buckets:
                buckets[bkt_key] = [bkt_key, row[1], row[2], row[3], row[4], row[5]]
            else:
                b = buckets[bkt_key]
                b[2] = max(b[2], row[2])
                b[3] = min(b[3], row[3])
                b[4] = row[4]
                b[5] += row[5]

        completed = [v for k, v in sorted(buckets.items()) if k != cur_bucket]

        with self._lock:
            self._5min_cache[symbol] = completed