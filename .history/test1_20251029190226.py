import asyncio
import aiohttp
import json
from datetime import datetime
from binance import AsyncClient, BinanceSocketManager
import contextvars
import re

user_tasks = {}
current_tg_id = contextvars.ContextVar('current_tg_id', default=None)
positions_data_var = contextvars.ContextVar("positions_data_var")

TELEGRAM_BOT_TOKEN = '7896630222:AAHqpViKZ9M2EvUIpJ1aVHpR23atyBOfy8E'
API_KEYS = [
    {
        "key": "9TvpACxlJtkRD6s22omjR7DzoZaBMouRUgtNuZAsemjwr50SE0rHOfn1u742BAqV",
        "secret": "tv5mhBQCuQYWE8qfrmk7O7a7Wtq9bckZvgNgE29SEhGRb0L1998g3ktjpwJxZwi6",
        "tg_id": 5902293966  # Telegram ID пользователя 1
    },
    {
        "key": "2WO25NiaXFuIZKcd4LrWkZkLpKNli45sNANYaTgonFkM6lWqpXhODyXZM9W3rXsS",
        "secret": "keyyCRTdXFiN7m6KXc2knNcYF0QMW2Z9zbOYJxiOjXhoRCJ4oG4fMQ4MLMFysiCE",
        "tg_id": 6360001973  # Telegram ID пользователя 2
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

    }
}


def now():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

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
                # Печатаем в консоль всегда
                print(msg)

                # Отправляем в Telegram только если разрешено
                if tg_id is not None and send_to_telegram:
                    await send_telegram_message(tg_id, msg)

            except Exception as e:
                print(f"[{now()}] [LOGGER] Error writing log: {e}")
            self.queue.task_done()

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
                reconnect_delay = 1  # сбрасываем задержку при успешном подключении

                while True:
                    try:
                        msg = await websocket.recv()
                        asyncio.create_task(logger.log(f"WS MESSAGE: {msg}", send_to_telegram=False))
                        await queue.put(msg)  # отправка сообщения в очередь
                    except Exception as e:
                        asyncio.create_task(logger.log(f"[PRODUCER-INNER] Error receiving message: {e}"))
                        break  # выход из внутреннего цикла для переподключения

        except Exception as e:
            asyncio.create_task(logger.log(f"[PRODUCER] WebSocket error: {e}, retrying in {reconnect_delay}s"))
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)  # увеличиваем задержку с лимитом


async def place_market_close(client, symbol, position_side):
    try:
        data = get_positions_data()[symbol]
        qty = data['position_size']
        if qty == 0:
            return
        data['closing'] = True
        side = 'SELL' if qty > 0 else 'BUY'
        asyncio.create_task(logger.log(f"[{symbol}] Closing position: qty={qty}, side={side}, reduceOnly=True"))
        await client.futures_create_order(
            symbol=symbol,
            side=side,
            type='MARKET',
            quantity=abs(qty),
            reduceOnly=True
        )
        asyncio.create_task(logger.log(f"[{symbol}] Position closed by market order."))
        data['closing'] = False

    except Exception as e:
        asyncio.create_task(logger.log(f"[{symbol}] Failed to close position: {e}"))


async def place_stop_loss(client, symbol, side, position_side):
    try:
        data = get_positions_data()[symbol]

        # Не ставим второй стоп, если уже есть
        if data.get('stop_order_id'):
            asyncio.create_task(logger.log(f"[{symbol}] Stop order already exists, skipping."))
            return None

        price = data['stop_loss_pr']
        quantity = data['position_size']
        if quantity == 0:
            return None

        close_position = data.get('close_position', False)

        # Формируем параметры запроса
        order_params = {
            "symbol": symbol,
            "side": side,
            "type": 'STOP_MARKET',
            "stopPrice": price,
            "timeInForce": 'GTC',
        }

        if close_position:
            order_params["closePosition"] = True
        else:
            order_params["quantity"] = abs(quantity)
            order_params["reduceOnly"] = True

        # Отправляем ордер
        resp = await client.futures_create_order(**order_params)

        order_id = resp['orderId']
        data['stop_order_id'] = order_id
        asyncio.create_task(logger.log(f"[{symbol}] Stop loss set at {price} (ClosePosition: {close_position})"))
        return order_id

    except Exception as e:
        if 'PlaceOrderError' in str(type(e)) or 'Order would immediately trigger' in str(e):
            asyncio.create_task(logger.log(f"[{symbol}] Stop loss placement failed with trigger error, closing position."))
            await place_market_close(client, symbol, get_positions_data()[symbol]['position_side'])
        else:
            asyncio.create_task(logger.log(f"[{symbol}] Stop loss placement failed: {e}"))
        return None

async def handle_private_messages(queue: asyncio.Queue, client, logger):
    #iteration = 0
    while True:
        msg = await queue.get()
        #iteration += 1
        #asyncio.create_task(logger.log(f"Handle iteration - {iteration}"))

        if msg.get('e') == 'ORDER_TRADE_UPDATE':
            order = msg['o']
            order_type = order.get('ot')
            order_status = order.get('x')

            # 💡 Обновляем stop_order_id, если это отмена или новый стоп-лосс
            if order_type == 'STOP_MARKET' and order_status in ('CANCELED', 'NEW'):
                asyncio.create_task(update_stop_loss_tracking(order['s'], order))

            # Обработка трейда только если ордер FILLED
            if order_status == 'TRADE' and order.get('X') == 'FILLED':
                await process_order_trade_update(client, order)


async def update_stop_loss_tracking(symbol: str, order: dict):
    order_type = order.get('ot')
    order_status = order.get('x')
    order_id = order.get('i')
    order_cp = order.get('cp')
    order_sp = order.get('sp')

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
        
async def process_order_trade_update(client, order):
    symbol = order['s']
    side = order['S']
    reduce_only = order.get('R', False)
    filled_qty = float(order['z'])
    order_type = order.get('ot', '')  # Тип оригинального ордера
    last_price = float(order.get('ap', order['L']))
    
    if symbol not in get_positions_data():
        get_positions_data()[symbol] = {
            'position_size': 0,
            'entry_price': None,
            'stop_order_id': None,
            'price_decimals': count_decimals(order['L']),
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
            
            
def count_decimals(price_str):
    if '.' in price_str:
        return len(price_str.split('.')[-1])
    return 0


async def handle_new_position(client, symbol, qty, price, side):
    config = CUSTOM_CONFIG.get(symbol, DEFAULT_CONFIG)
    stop_loss_pct = config['STOP_LOSS_PCT']
    decimals = get_positions_data()[symbol].get('price_decimals', 2)
    stop_loss_price = price * (1 - stop_loss_pct) if side == 'BUY' else price * (1 + stop_loss_pct)
    stop_loss_price = round(stop_loss_price, decimals)
    
    get_positions_data()[symbol].update({
        'position_size': qty if side == 'BUY' else -qty,
        'entry_price': price,
        'stop_order_id': None,
        'position_side': side,
        'closing': False,
        'stop_loss_pr': stop_loss_price
    })

    stop_side = 'SELL' if side == 'BUY' else 'BUY'
    asyncio.create_task(logger.log(f"[{symbol}] Setting stop loss at {stop_loss_price}", send_to_telegram=False))
    await place_stop_loss(client, symbol, stop_side, side)

async def update_existing_position(symbol, order_qty, order_side, client=None):
    data = get_positions_data()[symbol]
    old_size = data['position_size']

    # Переводим ордер в signed объём
    new_qty = order_qty if order_side == 'BUY' else -order_qty

    updated_size = old_size + new_qty

    # Проверка на разворот
    if old_size * updated_size < 0:
        direction = "🧭 Position direction reversed"
    elif abs(updated_size) > abs(old_size):
        direction = "📈 Position increased"
    elif abs(updated_size) < abs(old_size):
        direction = "📉 Position decreased"
    else:
        direction = "🔁 Position unchanged"

    asyncio.create_task(logger.log(f"[{symbol}] {direction}"))
    asyncio.create_task(logger.log(f"   New order qty: {new_qty:+f}, Updated position size: {updated_size:+f}"))

    # Обновляем данные позиции
    data['position_size'] = updated_size
    
    if updated_size == 0 and client is not None:
        asyncio.create_task(logger.log(f"[{symbol}] 📤 Position size is zero after update. Closing position record."))
        await handle_closed_position(symbol, client)
        
    if old_size != updated_size and updated_size != 0 and client is not None:
        # Отменяем текущий стоп, если есть
        stop_order_id = data.get('stop_order_id')
        if stop_order_id:
            try:
                await client.futures_cancel_order(symbol=symbol, orderId=stop_order_id)
                asyncio.create_task(logger.log(f"[{symbol}] ❌ Previous stop-loss {stop_order_id} cancelled due to position size change."))
            except Exception as e:
                asyncio.create_task(logger.log(f"[{symbol}] ⚠️ Failed to cancel stop-loss order {stop_order_id}: {e}"))

            data['stop_order_id'] = None

        # Определяем сторону стоп-ордера
        stop_side = 'SELL' if updated_size > 0 else 'BUY'
        position_side = 'LONG' if updated_size > 0 else 'SHORT'

        # Вызов функции постановки нового стопа
        await place_stop_loss(client, symbol, stop_side, position_side)  
        
async def handle_closed_position(symbol, client=None):
    if symbol in get_positions_data():
        stop_order_id = get_positions_data()[symbol].get('stop_order_id')
        if stop_order_id and client:
            try:
                await client.futures_cancel_order(symbol=symbol, orderId=stop_order_id)
                asyncio.create_task(logger.log(f"[{symbol}] 🧹 Stop-loss order {stop_order_id} cancelled manually.", send_to_telegram=False))
            except Exception as e:
                asyncio.create_task(logger.log(f"[{symbol}] ⚠️ Failed to cancel stop-loss order: {e}", send_to_telegram=False))
        
        asyncio.create_task(logger.log(f"[{symbol}] Position closed. Cleaning up."))
        reset_position_data(symbol)
        

def reset_position_data(symbol):
    if symbol in get_positions_data():
        del get_positions_data()[symbol]

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
                                    await task  # дождаться корректной очистки
                                except asyncio.CancelledError:
                                    pass
                                await send_telegram_message(chat_id, "✅ Обработка полностью остановлена и очищена.")
                            else:
                                await send_telegram_message(chat_id, "⚠️ Обработка ещё не была запущена.")
                        else: 
                            for api in API_KEYS:
                                if api["tg_id"] == chat_id:
                                    await send_telegram_message(chat_id, "🤖 Используйте команды /start и /stop.")

            except Exception as e:
                print(f"[{now()}] [TG-LISTENER] Ошибка: {e}")
                await asyncio.sleep(5)
                
                
logger = AsyncLogger()
                
async def handle_api_for_user(api_key, api_secret, tg_id):
    client = await AsyncClient.create(api_key, api_secret)
    queue = asyncio.Queue()

    # Устанавливаем tg_id в контекст
    token_tg = current_tg_id.set(tg_id)
    token_positions = positions_data_var.set({})

    # Запускаем обе задачи Binance
    producer_task = asyncio.create_task(websocket_message_producer(client, queue, logger))
    handler_task = asyncio.create_task(handle_private_messages(queue, client, logger))

    try:
        await logger.log(f"🚀 Запущена обработка для пользователя {tg_id}.")
        await asyncio.gather(producer_task, handler_task)
    except asyncio.CancelledError:
        # Получили отмену через /stop
        await logger.log(f"🛑 Получен сигнал остановки для пользователя {tg_id}.", tg_id)
    finally:
        # Отменяем обе задачи и ждём завершения
        for t in [producer_task, handler_task]:
            t.cancel()
        await asyncio.gather(producer_task, handler_task, return_exceptions=True)

        # Закрываем соединение с Binance
        try:
            await client.close_connection()
        except Exception:
            try:
                await client.session.close()
            except Exception:
                pass

        # Чистим контексты и позиции
        positions_data_var.set({})
        current_tg_id.set(None)

        await logger.log(f"✅ Пользователь {tg_id} полностью остановлен и очищен.", tg_id)
        
def get_positions_data():
    return positions_data_var.get()
    
async def main():
    print(f"[{now()}] 🚀 Bot started.")
    await logger.start()

    # запускаем long polling для Telegram
    await telegram_command_listener()
        
if __name__ == "__main__":
    asyncio.run(main())
