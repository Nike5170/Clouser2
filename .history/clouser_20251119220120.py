import asyncio
import aiohttp
from datetime import datetime
from decimal import Decimal, ROUND_DOWN
import re
from binance import AsyncClient, BinanceSocketManager

positions_data = {}
SYMBOL_FILTERS = {} 

TELEGRAM_BOT_TOKEN = '8512455108:AAFph9GJ014m0NEmxvKG1EyV_QVfYdnOfkM'
API_KEY = "9TvpACxlJtkRD6s22omjR7DzoZaBMouRUgtNuZAsemjwr50SE0rHOfn1u742BAqV"
API_SECRET = "tv5mhBQCuQYWE8qfrmk7O7a7Wtq9bckZvgNgE29SEhGRb0L1998g3ktjpwJxZwi6"

API_KEYS = [
    {
        "key": "9TvpACxlJtkRD6s22omjR7DzoZaBMouRUgtNuZAsemjwr50SE0rHOfn1u742BAqV",
        "secret": "tv5mhBQCuQYWE8qfrmk7O7a7Wtq9bckZvgNgE29SEhGRb0L1998g3ktjpwJxZwi6",
        "tg_id": 5902293966  
    }
]
DEFAULT_CONFIG = {
    'STOP_LOSS_PCT': 0.006,
}

CUSTOM_CONFIG = {
    'BTCUSDT': {
        'STOP_LOSS_PCT': 0.003,

    },
    'ETHUSDT': {
        'STOP_LOSS_PCT': 0.003,

    },
    'SOLUSDT': {
        'STOP_LOSS_PCT': 0.004,

    }
}

def now():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

async def load_all_symbol_precisions():

    global SYMBOL_FILTERS

    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as r:
                data = await r.json()

    except Exception as e:
        await logger.log(f"❌ Ошибка загрузки exchangeInfo: {e}")

        return

    updated = 0
    total = 0

    for s in data["symbols"]:
        if (
            s["contractType"] != "PERPETUAL"
            or s["quoteAsset"] != "USDT"
            or s["status"] != "TRADING"
        ):
            continue

        symbol = s["symbol"]
        total += 1

        price_tick = None
        qty_step = None

        for f in s["filters"]:
            if f["filterType"] == "PRICE_FILTER":
                price_tick = float(f["tickSize"])
            elif f["filterType"] == "LOT_SIZE":
                qty_step = float(f["stepSize"])

        if not price_tick or not qty_step:
            continue


        price_decimals = count_decimals_from_step(price_tick)
        qty_decimals = count_decimals_from_step(qty_step)


        prev = SYMBOL_FILTERS.get(symbol)

        SYMBOL_FILTERS[symbol] = {
            "price_tick": price_tick,
            "qty_step": qty_step,
            "price_decimals": price_decimals,
            "qty_decimals": qty_decimals
        }

        if prev != SYMBOL_FILTERS[symbol]:
            updated += 1

    await logger.log(f"🔄 Обновление tickSize завершено: {updated}/{total} изменено.")


def count_decimals_from_step(step):
    s = '{:.18f}'.format(step).rstrip('0')
    if '.' in s:
        return len(s.split('.')[-1])
    return 0

async def symbol_precision_updater_daily():
    while True:
        await logger.log("⏳ Обновление tickSize…")

        await load_all_symbol_precisions()
        await asyncio.sleep(24 * 60 * 60) 

class AsyncLogger:
    def __init__(self):
        self.queue = None
        self.task = None

    async def start(self):
        self.queue = asyncio.Queue()
        self.task = asyncio.create_task(self._logger_worker())

    async def log(self, message: str, send_to_telegram: bool = True):
        await self.queue.put((f"[{now()}] {message}", send_to_telegram))

    async def _logger_worker(self):
        while True:
            msg, send_to_telegram = await self.queue.get()
            try:
                print(msg)
                if send_to_telegram:
                    await send_telegram_message(API_KEYS:{TG_CHAT_ID}, msg)
            except Exception as e:
                print(f"[{now()}] [LOGGER] Error: {e}")
            self.queue.task_done()

class PositionManager:
    def __init__(self, client, logger, symbol_filters, default_cfg, custom_cfg):
        self.client = client
        self.logger = logger
        self.filters = symbol_filters
        self.default_cfg = default_cfg
        self.custom_cfg = custom_cfg

    def get_positions(self):
        return positions_data_var.get()

    def get_pos(self, symbol):
        return self.get_positions().get(symbol)

    def quantize_tick(self, value, tick):
        return Decimal(str(value)).quantize(
            Decimal(str(tick)), rounding=ROUND_DOWN
        )

    def direction_from_qty(self, qty):
        return "BUY" if qty > 0 else "SELL"

    def opposite(self, side):
        return "SELL" if side == "BUY" else "BUY"

    # ----------------------------------
    #  NEW: создание "скелета" позиции
    # ----------------------------------

    def ensure_position(self, symbol, side):
        """Создаёт заготовку позиции, если её нет."""
        pos = self.get_pos(symbol)
        if pos:
            return pos

        self.get_positions()[symbol] = {
            'position_size': 0,
            'entry_price': None,
            'stop_order_id': None,
            'position_side': side,
            'closing': False,
        }
        return self.get_positions()[symbol]

    # ----------------------------------
    #  NEW: безопасная отмена стопа
    # ----------------------------------

    async def cancel_stop_loss(self, symbol):
        data = self.get_pos(symbol)
        if not data:
            return

        sl = data.get("stop_order_id")
        if not sl:
            return

        try:
            await self.client.futures_cancel_order(symbol=symbol, orderId=sl)
            asyncio.create_task(self.logger.log(
                f"[{symbol}] Cancel SL {sl}"
            ))
        except Exception as e:
            asyncio.create_task(self.logger.log(
                f"[{symbol}] Failed to cancel SL: {e}"
            ))

        data["stop_order_id"] = None

    # ----------------------------------
    #  NEW: универсальная переустановка SL
    # ----------------------------------

    async def recreate_stop_loss(self, symbol):
        """Отменяет старый SL и ставит новый."""
        data = self.get_pos(symbol)
        if not data:
            return

        await self.cancel_stop_loss(symbol)   # <-- повтор убран

        side = self.direction_from_qty(data["position_size"])
        stop_side = self.opposite(side)

        await self.place_stop_loss(symbol, stop_side)

    # ----------------------------------
    #  calc_stop_price (без изменений)
    # ----------------------------------

    def calc_stop_price(self, symbol, entry_price, side):
        cfg = self.custom_cfg.get(symbol, self.default_cfg)
        stop_pct = cfg['STOP_LOSS_PCT']

        sl = entry_price * (1 - stop_pct) if side == "BUY" else entry_price * (1 + stop_pct)

        tick = self.filters[symbol]["price_tick"]
        return self.quantize_tick(sl, tick)

    # ----------------------------------
    #  update_stop_loss_tracking (только упрощение)
    # ----------------------------------

    async def update_stop_loss_tracking(self, symbol, order):
        order_type = order.get('ot')
        order_status = order.get('x')
        order_id = order.get('i')
        order_cp = order.get('cp')
        order_sp = float(order.get('sp', 0))

        pos = self.get_pos(symbol)
        if not pos:
            return

        if order_status == 'CANCELED' and order_type == 'STOP_MARKET':
            if pos.get('stop_order_id') == order_id:
                await self.logger.log(f"[{symbol}] 🔄 SL {order_id} cancelled")
                pos['stop_order_id'] = None

        elif order_status == 'NEW' and order_type == 'STOP_MARKET':
            await self.logger.log(f"[{symbol}] 🆕 SL created: {order_id}")
            pos['stop_order_id'] = order_id
            pos['close_position'] = order_cp
            pos['stop_loss_pr'] = order_sp

    # ----------------------------------
    # process_order_trade_update (упрощено)
    # ----------------------------------

    async def process_order_trade_update(self, order):
        symbol = order['s']
        side = order['S']
        reduce_only = order.get('R', False)
        filled_qty = float(order['z'])
        order_type = order.get('ot', '')
        last_price = float(order.get('ap', order['L']))

        pos = self.ensure_position(symbol, side)

        if reduce_only:
            if order_type == 'STOP_MARKET':
                await self.logger.log(f"[{symbol}] ⛔ Stop-loss triggered! Position closed.")
                pos['stop_order_id'] = None
            else:
                await self.logger.log(f"[{symbol}] ⚠ Manual or external reduce-only close")

            await self.handle_closed_position(symbol)
            return

        # позиция уже есть → обновление
        if pos['position_size'] != 0:
            await self.update_existing_position(symbol, filled_qty, side)
        else:
            await self.logger.log(f"[{symbol}] ✅ New position opened: {filled_qty} @ {last_price}")
            await self.handle_new_position(symbol, filled_qty, last_price, side)

    # ----------------------------------
    # place_market_close (оставлена полностью)
    # ----------------------------------

    async def place_market_close(self, symbol):
        data = self.get_pos(symbol)
        if not data:
            return

        qty = data["position_size"]
        if qty == 0:
            return

        data['closing'] = True
        close_side = self.opposite(self.direction_from_qty(qty))

        await self.logger.log(f"[{symbol}] Closing market: qty={qty}")

        try:
            await self.client.futures_create_order(
                symbol=symbol,
                side=close_side,
                type='MARKET',
                quantity=abs(qty),
                reduceOnly=True
            )
            await self.logger.log(f"[{symbol}] Closed by MARKET")
        except Exception as e:
            await self.logger.log(f"[{symbol}] Market close failed: {e}")

        data['closing'] = False

    # ----------------------------------
    # place_stop_loss (без изменения логики)
    # ----------------------------------

    async def place_stop_loss(self, symbol, side):
        data = self.get_pos(symbol)
        if not data:
            return None

        if data.get('stop_order_id'):
            asyncio.create_task(self.logger.log(f"[{symbol}] Stop already exists."))
            return None

        price = self.quantize_tick(data['stop_loss_pr'], self.filters[symbol]["price_tick"])
        qty = self.quantize_tick(abs(data['position_size']), self.filters[symbol]["qty_step"])
        close_pos = data.get("close_position", False)

        params = {
            "symbol": symbol,
            "side": side,
            "type": "STOP_MARKET",
            "stopPrice": price
        }

        if close_pos:
            params["closePosition"] = True
        else:
            params["quantity"] = qty
            params["reduceOnly"] = True

        try:
            resp = await self.client.futures_create_order(**params)
            oid = resp["orderId"]
            data["stop_order_id"] = oid

            await self.logger.log(
                f"[{symbol}] SL set at {price} qty={qty} (close_pos={close_pos})"
            )
            return oid

        except Exception as e:
            if "immediately trigger" in str(e):
                await self.logger.log(f"[{symbol}] SL failed. Market close.")
                await self.place_market_close(symbol)
            else:
                await self.logger.log(f"[{symbol}] SL failed: {e}")
            return None

    # ----------------------------------
    # handle_new_position (упрощена через recreate_stop_loss)
    # ----------------------------------

    async def handle_new_position(self, symbol, qty, price, side):
        sl_price = self.calc_stop_price(symbol, price, side)

        self.get_positions()[symbol] = {
            'position_size': qty if side == 'BUY' else -qty,
            'entry_price': price,
            'stop_order_id': None,
            'position_side': side,
            'stop_loss_pr': sl_price,
            'closing': False
        }

        await self.logger.log(f"[{symbol}] SL for new position = {sl_price}")
        await self.place_stop_loss(symbol, self.opposite(side))

    # ----------------------------------
    # update_existing_position (повторы убраны)
    # ----------------------------------

    async def update_existing_position(self, symbol, order_qty, order_side):
        data = self.get_pos(symbol)
        old = data['position_size']
        delta = order_qty if order_side == "BUY" else -order_qty
        new = old + delta

        # логика изменения остаётся
        msg = (
            "🧭 Direction reversed" if old * new < 0 else
            "📈 Increased" if abs(new) > abs(old) else
            "📉 Decreased" if abs(new) < abs(old) else
            "🔁 Unchanged"
        )
        await self.logger.log(f"[{symbol}] {msg}. Δ={delta:+f}, new={new:+f}")

        data['position_size'] = new

        # позиция закрылась
        if new == 0:
            await self.handle_closed_position(symbol)
            return

        # позиция изменилась → SL нужно пересоздать
        data["close_position"] = True
        await self.recreate_stop_loss(symbol)

    # ----------------------------------
    # handle_closed_position (повторы убраны)
    # ----------------------------------

    async def handle_closed_position(self, symbol):
        data = self.get_pos(symbol)
        if not data:
            return

        # отмена стопа — теперь отдельная функция
        await self.cancel_stop_loss(symbol)

        await self.logger.log(f"[{symbol}] Position closed. Cleanup.")
        del self.get_positions()[symbol]

def escape_markdown(text):
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)
                 
async def send_telegram_message(tg_id: int, text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    async with aiohttp.ClientSession() as session:
        escaped_text = escape_markdown(text)
        payload = {"chat_id": tg_id, "text": escaped_text, "parse_mode": "MarkdownV2"}
        async with session.post(url, json=payload) as resp:
            if resp.status != 200:
                print(f"Failed to send telegram message: {await resp.text()}")

async def websocket_message_producer(client, queue: asyncio.Queue, logger):
    reconnect_delay = 1

    while True:
        try:
            bm = BinanceSocketManager(client)
            socket = bm.futures_user_socket()

            async with socket as websocket:
                asyncio.create_task(logger.log(f"📡 WebSocket producer connected."))
                reconnect_delay = 1  

                while True:
                    try:
                        msg = await websocket.recv()
                        asyncio.create_task(logger.log(f"WS MESSAGE: {msg}", send_to_telegram=False))
                        await queue.put(msg)  
                    except Exception as e:
                        asyncio.create_task(logger.log(f"[PRODUCER-INNER] Error receiving message: {e}"))
                        break  
        except Exception as e:
            asyncio.create_task(logger.log(f"[PRODUCER] WebSocket error: {e}, retrying in {reconnect_delay}s"))
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60) 


async def handle_private_messages(queue: asyncio.Queue, client, pm):
    while True:
        msg = await queue.get()

        if msg.get('e') == 'ORDER_TRADE_UPDATE':
            order = msg['o']
            order_type = order.get('ot')
            order_status = order.get('x')
            symbol = order['s'] 


            if order_type == 'STOP_MARKET' and order_status in ('CANCELED', 'NEW'):
                asyncio.create_task(pm.update_stop_loss_tracking(symbol, order))

            if order_status == 'TRADE' and order.get('X') == 'FILLED':
                await pm.process_order_trade_update(order)


async def telegram_command_listener():
    offset = 0
    while True:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
        params = {"timeout": 30, "offset": offset}
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, params=params, timeout=35) as resp:
                    data = await resp.json()
                    for update in data.get("result", []):
                        offset = update["update_id"] + 1
                        message = update.get("message", {})
                        chat_id = message.get("chat", {}).get("id")
                        text = message.get("text", "").strip().lower()

                        if not chat_id or not text:
                            continue

                        if text == "/start":
                            if chat_id in user_tasks:
                                await send_telegram_message(chat_id, "⚠️ Обработка уже запущена.")
                            else:
                                for api in API_KEYS:
                                    if api["tg_id"] == chat_id:
                                        task = asyncio.create_task(handle_api_for_user(api['key'], api['secret'], chat_id))
                                        user_tasks[chat_id] = task
                                        await send_telegram_message(chat_id, "✅ Обработка запущена.")
                                        break
                                else:
                                    await send_telegram_message(chat_id, "❌ Не найден API-ключ для этого Telegram ID.")

                        elif text == "/stop":
                            task = user_tasks.pop(chat_id, None)
                            if task:
                                await send_telegram_message(chat_id, "🛑 Останавливаю обработку...")
                                task.cancel()
                                try:
                                    await task 
                                except asyncio.CancelledError:
                                    pass
                                await send_telegram_message(chat_id, "✅ Обработка полностью остановлена и очищена.")
                            else:
                                await send_telegram_message(chat_id, "⚠️ Обработка ещё не была запущена.")
                        elif text == "/update_symbols":
                            for api in API_KEYS:
                                if api["tg_id"] == chat_id:
                                    await logger.log("⏳ Ручное обновление tickSize символов...", tg_id)
                                    await load_all_symbol_precisions()
                                    await send_telegram_message(chat_id, "✅ tickSize символов обновлены вручную.")
                                    break
                            else:
                                await send_telegram_message(chat_id, "❌ Не найден API-ключ для этого Telegram ID.")
                        else: 
                            for api in API_KEYS:
                                if api["tg_id"] == chat_id:
                                    await send_telegram_message(chat_id, "🤖 Используйте команды /start , /stop и /update_symbols.")

            except Exception as e:
                print(f"[{now()}] [TG-LISTENER] Ошибка: {e}")
                await asyncio.sleep(5)
                
                
async def handle_api_for_user(api_key, api_secret, tg_id):
    client = await AsyncClient.create(api_key, api_secret)
    queue = asyncio.Queue()

    pm = PositionManager(
        client=client,
        logger=logger,
        symbol_filters=SYMBOL_FILTERS,
        default_cfg=DEFAULT_CONFIG,     
        custom_cfg=CUSTOM_CONFIG        
    )
    producer_task = asyncio.create_task(websocket_message_producer(client, queue, logger))
    handler_task = asyncio.create_task(handle_private_messages(queue, client, pm))

    try:
        await logger.log(f"🚀 Запущена обработка для пользователя {tg_id}.")
        await asyncio.gather(producer_task, handler_task)
    except asyncio.CancelledError:
        await logger.log(f"🛑 Получен сигнал остановки для пользователя {tg_id}.", tg_id)
    finally:
        for t in [producer_task, handler_task]:
            t.cancel()
        await asyncio.gather(producer_task, handler_task, return_exceptions=True)

        try:
            await client.close_connection()
        except Exception:
            try:
                await client.session.close()
            except Exception:
                pass

        positions_data_var.set({})
        current_tg_id.set(None)

        await logger.log(f"✅ Пользователь {tg_id} полностью остановлен и очищен.", tg_id)

async def main():
    logger = AsyncLogger()
    await logger.start()
    await load_all_symbol_precisions()
    client = await AsyncClient.create(API_KEY, API_SECRET)
    pm = PositionManager(client, logger)

    queue = asyncio.Queue()
    asyncio.create_task(symbol_precision_updater_daily())
    asyncio.create_task(websocket_message_producer(client, queue, logger))
    asyncio.create_task(handle_private_messages(queue, pm))
    await telegram_command_listener()

if __name__ == "__main__":
    asyncio.run(main())