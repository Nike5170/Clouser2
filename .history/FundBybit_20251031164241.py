import asyncio
import aiohttp
import json
import time
import hmac
import hashlib
from urllib.parse import urlencode
from datetime import datetime

API_KEY    = "TKCSBRh0oWIXaS3SC0"
API_SECRET = "MgAq2McTsjs4w1iwpGKyRRynWe5BGNUo61m9"
SYMBOL     = "ICNTUSDT"
QTY        = 15
BASE       = "https://api.bybit.com"
WS_PUBLIC  = "wss://stream.bybit.com/v5/public/linear"

ENTRY_OFFSET_MS = 50
LOG_WINDOW_MS   = 2000  # 2 секунды до и после фандинга

def log(msg):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print(f"[{ts}] {msg}")

def sign(secret: str, params: dict) -> str:
    q = urlencode(params)
    return hmac.new(secret.encode(), q.encode(), hashlib.sha256).hexdigest()

async def signed_req(session, method, path, params=None):
    params = params or {}
    params["apiKey"] = API_KEY
    params["timestamp"] = int(time.time() * 1000)
    params["sign"] = sign(API_SECRET, params)
    url = BASE + path
    async with session.request(method, url, params=params) as r:
        text = await r.text()
        if r.status != 200:
            log(f"[ERROR] {r.status}: {text}")
            raise Exception(f"HTTP {r.status}: {text}")
        return json.loads(text)

async def market_order(session, side, qty, reduce=False):
    body = {
        "category": "linear",
        "symbol": SYMBOL,
        "side": side.upper(),
        "orderType": "Market",
        "qty": str(qty),
        "timeInForce": "ImmediateOrCancel",
        "reduceOnly": reduce
    }
    log(f"[SEND] MARKET {side} {qty} (reduceOnly={reduce})")
    res = await signed_req(session, "POST", "/v5/order/create", body)
    log(f"[ORDER] {side} {qty}: {res}")
    return res

async def get_position(session):
    body = {"category": "linear", "symbol": SYMBOL}
    res = await signed_req(session, "GET", "/v5/position/list", params={"category": "linear", "symbol": SYMBOL})
    for p in res.get("result", {}).get("list", []):
        if p.get("symbol") == SYMBOL:
            return float(p.get("size", 0)) * (1 if p.get("side") == "Buy" else -1)
    return 0.0

async def schedule_entry(session, next_T):
    now = int(time.time() * 1000)
    delay = max(0, (next_T - ENTRY_OFFSET_MS - now) / 1000.0)
    log(f"[READY] Entry in {delay*1000:.0f} ms")
    await asyncio.sleep(delay)
    log(f"[ENTRY] MARKET BUY {QTY}")
    await market_order(session, "Buy", QTY)

async def run():
    async with aiohttp.ClientSession() as session:
        # Получаем предыдущий funding через REST
        res = await signed_req(
            session,
            "GET",
            "/v5/market/funding/prev-funding-rate",
            params={"category": "linear", "symbol": SYMBOL}
        )
        item = res.get("result", {}).get("list", [{}])[0]
        next_funding = int(item.get("fundingTimestamp", int(time.time() * 1000)))
        entry_task = asyncio.create_task(schedule_entry(session, next_funding))
        entered = True

        async with session.ws_connect(WS_PUBLIC) as ws:
            await ws.send_json({
                "op": "subscribe",
                "args": [f"funding_rate.{SYMBOL}"]
            })

            async for msg in ws:
                if msg.type != aiohttp.WSMsgType.TEXT:
                    continue
                data = json.loads(msg.data)
                if "data" not in data:
                    continue

                for entry in data["data"]:
                    next_T = int(entry.get("nextFundingTime", 0))
                    now = int(time.time() * 1000)
                    ms_to_funding = next_T - now

                    # Логи за 2 сек до и после funding
                    if abs(ms_to_funding) <= LOG_WINDOW_MS:
                        rate = float(entry.get("fundingRate", 0))
                        log(f"[MARKPRICE] rate={rate:.6%}, nextFunding={next_T}, ms_to_funding={ms_to_funding}")

                    # Мгновенное закрытие позиции после funding
                    if entered and ms_to_funding < 0:
                        pos = await get_position(session)
                        if pos != 0:
                            side = "Sell" if pos > 0 else "Buy"
                            log(f"[CLOSE_TRIGGER] Closing position {pos} via {side}")
                            await market_order(session, side, abs(pos), reduce=True)
                            log("[EXIT] Position closed immediately after funding")
                        entered = False
                        if entry_task:
                            entry_task.cancel()
                            entry_task = None

                await asyncio.sleep(0.01)

if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log("Stopped manually.")
