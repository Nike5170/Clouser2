import asyncio
import math
import aiohttp
import json
from datetime import datetime, timedelta
from binance import AsyncClient, BinanceSocketManager
import contextvars
import re
from decimal import Decimal, ROUND_DOWN


logger = AsyncLogger()
user_tasks = {}
current_tg_id = contextvars.ContextVar('current_tg_id', default=None)
positions_data_var = contextvars.ContextVar("positions_data_var")
SYMBOL_FILTERS = {} 

TELEGRAM_BOT_TOKEN = '8512455108:AAFph9GJ014m0NEmxvKG1EyV_QVfYdnOfkM'
API_KEYS = [
    {
        "key": "9TvpACxlJtkRD6s22omjR7DzoZaBMouRUgtNuZAsemjwr50SE0rHOfn1u742BAqV",
        "secret": "tv5mhBQCuQYWE8qfrmk7O7a7Wtq9bckZvgNgE29SEhGRb0L1998g3ktjpwJxZwi6",
        "tg_id": 5902293966  
    },
    {
        "key": "2WO25NiaXFuIZKcd4LrWkZkLpKNli45sNANYaTgonFkM6lWqpXhODyXZM9W3rXsS",
        "secret": "keyyCRTdXFiN7m6KXc2knNcYF0QMW2Z9zbOYJxiOjXhoRCJ4oG4fMQ4MLMFysiCE",
        "tg_id": 6360001973  
    },
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

    async def log(self, message: str, tg_id: int = None, send_to_telegram: bool = True):
        if tg_id is None:
            tg_id = current_tg_id.get()
        await self.queue.put((f"[{now()}] {message}", tg_id, send_to_telegram))

    async def _logger_worker(self):
        while True:
            msg, tg_id, send_to_telegram = await self.queue.get()
            try:
            
                print(msg)


                if tg_id is not None and send_to_telegram:
                    await send_telegram_message(tg_id, msg)

            except Exception as e:
                print(f"[{now()}] [LOGGER] Error writing log: {e}")
            self.queue.task_done()


class PositionManager:
    def __init__(self, client, logger, symbol_filters, default_cfg, custom_cfg):
        self.client = client
        self.logger = logger
        self.filters = symbol_filters
        self.default_cfg = default_cfg
        self.custom_cfg = custom_cfg

    # ----------------------------------------------------
    # Вспомогательные функции (внутренние)
    # ----------------------------------------------------

    def get_positions(self):
        return positions_data_var.get()

    def quantize_tick(self, value, tick):
        return Decimal(str(value)).quantize(
            Decimal(str(tick)),
            rounding=ROUND_DOWN
        )

    def direction_from_qty(self, qty):
        return "BUY" if qty > 0 else "SELL"

    def opposite(self, side):
        return "SELL" if side == "BUY" else "BUY"

    def calc_stop_price(self, symbol, entry_price, side):
        cfg = self.custom_cfg.get(symbol, self.default_cfg)
        stop_pct = cfg['STOP_LOSS_PCT']

        if side == "BUY":
            sl = entry_price * (1 - stop_pct)
        else:
            sl = entry_price * (1 + stop_pct)

        tick = self.filters[symbol]["price_tick"]
        return self.quantize_tick(sl, tick)

    # ----------------------------------------------------
    # ОСНОВНАЯ ЛОГИКА
    # ----------------------------------------------------
    async def update_stop_loss_tracking(self, symbol, order):
        order_type = order.get('ot')
        order_status = order.get('x')
        order_id = order.get('i')
        order_cp = order.get('cp')
        order_sp = float(order.get('sp', 0))

        if symbol not in get_positions_data():
            return

        if order_status == 'CANCELED' and order_type == 'STOP_MARKET':
            current_stop_id = get_positions_data()[symbol].get('stop_order_id')
            if current_stop_id == order_id:
                asyncio.create_task(logger.log(f"[{symbol}] 🔄 Stop-loss order {order_id} cancelled by Binance.", send_to_telegram=False))
                get_positions_data()[symbol]['stop_order_id'] = None

        elif order_status == 'NEW' and order_type == 'STOP_MARKET':
            asyncio.create_task(logger.log(f"[{symbol}] 🆕 New stop-loss order created: {order_id}", send_to_telegram=False))
            get_positions_data()[symbol]['stop_order_id'] = order_id
            get_positions_data()[symbol]['close_position'] = order_cp
            get_positions_data()[symbol]['stop_loss_pr'] = order_sp

    async def process_order_trade_update(self, order)
        symbol = order['s']
        side = order['S']
        reduce_only = order.get('R', False)
        filled_qty = float(order['z'])
        order_type = order.get('ot', '')  
        last_price = float(order.get('ap', order['L']))
        
        if symbol not in get_positions_data():
            get_positions_data()[symbol] = {
                'position_size': 0,
                'entry_price': None,
                'stop_order_id': None,
                'position_side': side,
                'closing': False,
            }

        data = get_positions_data()[symbol]

        if reduce_only:
            if order_type == 'STOP_MARKET':
                asyncio.create_task(logger.log(f"[{symbol}] ⛔️ Stop-loss triggered! Position closed by STOP_MARKET."))
                data['stop_order_id'] = None
            else:
                asyncio.create_task(logger.log(f"[{symbol}] ⚠️ Position closed manually or by another reduce-only order."))

            await handle_closed_position(symbol, client)
        else:

            if data['position_size'] != 0:
                await update_existing_position(symbol, filled_qty, side, client)
            else:
                asyncio.create_task(logger.log(f"[{symbol}] ✅ New position opened. Qty: {filled_qty}, Side: {side}, Price: {last_price}"))
                await handle_new_position(client, symbol, filled_qty, last_price, side)

    def reset_position_data(self, symbol):
        if symbol in get_positions_data():
            del get_positions_data()[symbol]       

    async def place_market_close(self, symbol):
        """Полное закрытие позиции — рыночным ордером."""
        data = self.get_positions().get(symbol)
        if not data:
            return

        qty = data['position_size']
        if qty == 0:
            return

        data['closing'] = True
        side = self.direction_from_qty(qty)
        close_side = self.opposite(side)

        asyncio.create_task(self.logger.log(
            f"[{symbol}] Closing position: qty={qty}, reduceOnly=True"
        ))

        try:
            await self.client.futures_create_order(
                symbol=symbol,
                side=close_side,
                type='MARKET',
                quantity=abs(qty),
                reduceOnly=True
            )
            asyncio.create_task(self.logger.log(f"[{symbol}] Position closed by market order."))
        except Exception as e:
            asyncio.create_task(self.logger.log(f"[{symbol}] Failed to close position: {e}"))

        data['closing'] = False

    async def place_stop_loss(self, symbol, side):
        """Создаёт STOP_MARKET."""
        data = self.get_positions().get(symbol)
        if not data:
            return None

        if data.get('stop_order_id'):
            asyncio.create_task(self.logger.log(f"[{symbol}] Stop already exists, skip."))
            return None

        price = data['stop_loss_pr']
        qty = abs(data['position_size'])
        close_pos = data.get("close_position", False)

        price = self.quantize_tick(price, self.filters[symbol]["price_tick"])
        qty = self.quantize_tick(qty, self.filters[symbol]["qty_step"])

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

            asyncio.create_task(self.logger.log(
                f"[{symbol}] Stop-loss set at {price} qty={qty} (close_pos={close_pos})"
            ))
            return oid

        except Exception as e:
            if "immediately trigger" in str(e):
                asyncio.create_task(self.logger.log(
                    f"[{symbol}] Stop placement failed (trigger), closing market."
                ))
                await self.place_market_close(symbol)
            else:
                asyncio.create_task(self.logger.log(
                    f"[{symbol}] Stop placement failed: {e}"
                ))
            return None

    async def handle_new_position(self, symbol, qty, price, side):
        """Первая сделка — создаём позицию и ставим стоп."""
        sl_price = self.calc_stop_price(symbol, price, side)

        self.get_positions()[symbol] = {
            'position_size': qty if side == 'BUY' else -qty,
            'entry_price': price,
            'stop_order_id': None,
            'position_side': side,
            'stop_loss_pr': sl_price,
            'closing': False
        }

        stop_side = self.opposite(side)
        asyncio.create_task(self.logger.log(
            f"[{symbol}] Setting SL at {sl_price}", send_to_telegram=False
        ))

        await self.place_stop_loss(symbol, stop_side)

    async def update_existing_position(self, symbol, order_qty, order_side):
        """Увеличение/уменьшение/реверс позиции."""
        data = self.get_positions()[symbol]
        old_size = data['position_size']
        new_fragment = order_qty if order_side == "BUY" else -order_qty
        new_size = old_size + new_fragment

        # Логи
        if old_size * new_size < 0:
            msg = "🧭 Direction reversed"
        elif abs(new_size) > abs(old_size):
            msg = "📈 Increased"
        elif abs(new_size) < abs(old_size):
            msg = "📉 Decreased"
        else:
            msg = "🔁 Unchanged"

        asyncio.create_task(self.logger.log(
            f"[{symbol}] {msg}\n   Fragment: {new_fragment:+f}, New size: {new_size:+f}"
        ))

        data['position_size'] = new_size

        # позиция изменилась — переставляем SL
        if new_size != 0:
            old_stop = data.get('stop_order_id')
            if old_stop:
                try:
                    await self.client.futures_cancel_order(symbol=symbol, orderId=old_stop)
                    asyncio.create_task(self.logger.log(
                        f"[{symbol}] Cancel SL {old_stop} due to position change."
                    ))
                except Exception as e:
                    asyncio.create_task(self.logger.log(
                        f"[{symbol}] Failed to cancel SL: {e}"
                    ))

                data['stop_order_id'] = None

            data["close_position"] = True
            side = self.direction_from_qty(new_size)
            stop_side = self.opposite(side)
            await self.place_stop_loss(symbol, stop_side)
            return

        # позиция стала 0 → закрытие
        await self.handle_closed_position(symbol)

    async def handle_closed_position(self, symbol):
        """Очистка позиции и отмена SL после полного закрытия."""
        data = self.get_positions().get(symbol)
        if not data:
            return

        # отмена SL если остался
        sl = data.get("stop_order_id")
        if sl:
            try:
                await self.client.futures_cancel_order(symbol=symbol, orderId=sl)
                asyncio.create_task(self.logger.log(
                    f"[{symbol}] Stop {sl} cancelled (position closed)."
                ))
            except Exception as e:
                asyncio.create_task(self.logger.log(
                    f"[{symbol}] Failed to cancel stop: {e}"
                ))

        asyncio.create_task(self.logger.log(f"[{symbol}] Position closed, cleanup."))

        del self.get_positions()[symbol]

def escape_markdown(text):
    """
    Экранирует спецсимволы для MarkdownV2 Telegram API.
    """
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)
    
async def send_telegram_message(tg_id: int, text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    async with aiohttp.ClientSession() as session:
        escaped_text = escape_markdown(text)
        payload = {
            "chat_id": tg_id,
            "text": escaped_text,
            "parse_mode": "MarkdownV2"
        }
        async with session.post(url, json=payload) as resp:
            if resp.status != 200:
                print(f"Failed to send telegram message to {tg_id}: {await resp.text()}")
                
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


async def handle_private_messages(queue: asyncio.Queue, client):
    while True:
        msg = await queue.get()


        if msg.get('e') == 'ORDER_TRADE_UPDATE':
            order = msg['o']
            order_type = order.get('ot')
            order_status = order.get('x')


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

    # ---------------------------------------------
    # Создаём PositionManager для управления позициями
    # ---------------------------------------------
    pm = PositionManager(
        client=client,
        logger=logger,
        symbol_filters=SYMBOL_FILTERS,  # если есть
        default_cfg=DEFAULT_CONFIG,     # если есть
        custom_cfg=CUSTOM_CONFIG        # если есть
    )

    token_tg = current_tg_id.set(tg_id)
    token_positions = positions_data_var.set({})

    # Передаём pm в обработчик сообщений WebSocket
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
    print(f"[{now()}] 🚀 Bot started.")
    await logger.start()

 
    await load_all_symbol_precisions()


    asyncio.create_task(symbol_precision_updater_daily())


    await telegram_command_listener()

if __name__ == "__main__":
    asyncio.run(main())
