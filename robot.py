# robot.py
import time
import os
import logging
import traceback
import psycopg2
import psycopg2.extras
from psycopg2.extras import RealDictCursor
from threading import RLock
from QuikPy import QuikPy
import requests
from typing import Optional

# ─── Логирование ─────────────────────────────────────────────────────────────
LOG_PATH = os.path.join(os.path.dirname(__file__), "robot.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger("robot")

# ─── БД ──────────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "dbname":   "instrumentsdb",
    "user":     "postgres",
    "password": "1234",
    "host":     "localhost",
    "port":     5432,
}

# ─── Флаг остановки ──────────────────────────────────────────────────────────
STOP_FLAG = os.path.join(os.path.dirname(__file__), "stop.flag")

def should_stop() -> bool:
    return os.path.exists(STOP_FLAG)

def cleanup_flag():
    try:
        if os.path.exists(STOP_FLAG):
            os.remove(STOP_FLAG)
    except Exception:
        pass

# ─── Кэш стаканов ────────────────────────────────────────────────────────────
orderbook_cache: dict = {}   # key: isin -> {"bid": [...], "offer": [...]}
orderbook_lock  = RLock()


# ─── Callback: обновление стакана ─────────────────────────────────────────────
def on_quote_callback(data):
    q          = data.get("data") or {}
    sec_code   = q.get("sec_code")
    if not sec_code:
        return

    bids   = q.get("bid")   or []
    offers = q.get("offer") or []

    with orderbook_lock:
        old = orderbook_cache.get(sec_code)
        if old:
            old_bids   = old.get("bid")   or []
            old_offers = old.get("offer") or []
            if (len(bids) == len(old_bids) and len(offers) == len(old_offers)
                    and bids == old_bids and offers == old_offers):
                return   # ничего не изменилось
        orderbook_cache[sec_code] = {"bid": bids, "offer": offers}


# ─── Подписка на стаканы ──────────────────────────────────────────────────────
def subscribe_all_books(qp: QuikPy, instruments: list):
    """
    Подписываемся напрямую через process_request, минуя
    is_subscribed_level2_quotes() внутри subscribe_level2_quotes —
    это избегает конкуренции за Lock с CallbackThread.
    """
    for row in instruments:
        board = row["board"]
        isin  = row["isin"]
        try:
            qp.process_request({
                "data": f"{board}|{isin}",
                "id": 0,
                "cmd": "Subscribe_Level_II_Quotes",
                "t": ""
            })
            logger.info(f"📶 Подписка L2: {board}.{isin}")
        except Exception as e:
            logger.warning(f"⚠️ Подписка L2 {board}.{isin}: {e}")


def preload_orderbooks(qp: QuikPy, instruments: list):
    """Инициализируем стаканы сразу после подписки — до первого цикла."""
    for row in instruments:
        board = row["board"]
        isin  = row["isin"]
        try:
            resp = qp.get_quote_level2(board, isin)
            data = resp.get("data") or {}
            bids   = data.get("bid")   or []
            offers = data.get("offer") or []
            with orderbook_lock:
                orderbook_cache[isin] = {"bid": bids, "offer": offers}
            if bids or offers:
                logger.info(f"📖 Стакан загружен: {board}.{isin}  "
                            f"bid={len(bids)} offer={len(offers)}")
            else:
                logger.warning(f"⚠️ Пустой стакан при старте: {board}.{isin}")
        except Exception as e:
            logger.warning(f"⚠️ Ошибка инициализации стакана {board}.{isin}: {e}")


# ─── Форматирование стакана для Telegram ──────────────────────────────────────
def format_orderbook(isin: str, name: str, board: str) -> str:
    """
    Возвращает текст сообщения со стаканом для отправки в TG.
    Показывает до 5 лучших уровней bid и offer.
    """
    MAX_LEVELS = 5

    with orderbook_lock:
        ob = orderbook_cache.get(isin)

    if not ob:
        return f"📋 {name} ({isin})\nСтакан пуст или не загружен"

    bids   = ob.get("bid")   or []
    offers = ob.get("offer") or []

    # Лучший bid — последняя строка, лучший offer — первая строка
    best_bids   = bids[-MAX_LEVELS:][::-1]   # топ 5 покупок, от лучшей вниз
    best_offers = offers[:MAX_LEVELS]         # топ 5 продаж, от лучшей вверх

    lines = [f"📊 {name} | {isin} | {board}", ""]

    lines.append("  ПРОДАЖА (offer)")
    if best_offers:
        for lvl in reversed(best_offers):   # выводим от худшей к лучшей (сверху)
            price = lvl.get("price", "?")
            qty   = lvl.get("quantity", "?")
            lines.append(f"  {float(price):>12.4f}   {qty}")
    else:
        lines.append("  —")

    lines.append("─" * 28)

    lines.append("  ПОКУПКА  (bid)")
    if best_bids:
        for lvl in best_bids:
            price = lvl.get("price", "?")
            qty   = lvl.get("quantity", "?")
            lines.append(f"  {float(price):>12.4f}   {qty}")
    else:
        lines.append("  —")

    return "\n".join(lines)


# ─── Telegram ─────────────────────────────────────────────────────────────────
def send_telegram(tgapi: str, chat_id: str, text: str, proxy: Optional[dict]) -> bool:
    url     = f"https://api.telegram.org/bot{tgapi}/sendMessage"
    session = requests.Session()

    if proxy:
        user  = proxy.get("username", "")
        pwd   = proxy.get("password", "")
        host  = proxy["host"]
        port  = proxy["port"]
        auth  = f"{user}:{pwd}@" if user else ""
        p_url = f"socks5h://{auth}{host}:{port}"
        session.proxies = {"http": p_url, "https": p_url}

    try:
        resp = session.post(
            url,
            json={"chat_id": chat_id, "text": text},
            timeout=30
        )
        if resp.status_code == 200:
            return True
        logger.warning(f"⚠️ TG {resp.status_code}: {resp.text[:120]}")
        return False
    except Exception as e:
        logger.warning(f"⚠️ Ошибка TG ({chat_id}): {e}")
        return False
    finally:
        session.close()


# ─── Чтение из БД ─────────────────────────────────────────────────────────────
def fetch_instruments(conn) -> list:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM instruments ORDER BY name")
        return cur.fetchall()

def fetch_decay(conn) -> float:
    with conn.cursor() as cur:
        cur.execute("SELECT decay FROM decay WHERE id = 1")
        row = cur.fetchone()
    return float(row[0]) if row and row[0] is not None else 1.0

def fetch_active_proxy(conn) -> Optional[dict]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM proxies WHERE is_active = TRUE LIMIT 1")
        row = cur.fetchone()
    return dict(row) if row else None

def fetch_tg_enabled(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT tg_enabled FROM tg_settings WHERE id = 1")
        row = cur.fetchone()
    return bool(row[0]) if row else False


# ─── Обработка одной строки ───────────────────────────────────────────────────
def process_instrument(row: dict, proxy: Optional[dict], tg_enabled: bool = False):
    name  = row.get("name",  "—")
    isin  = row.get("isin",  "—")
    board = row.get("board", "—")

    logger.info(f"── {name} ({isin}) ──")
    for key, value in row.items():
        logger.info(f"   {key} = {value}")

    if tg_enabled:
        tgapi  = (row.get("tgapi")  or "").strip()
        tgchat = (row.get("tgchat") or "").strip()

        if tgapi and tgchat:
            msg = format_orderbook(isin, name, board)
            ok  = send_telegram(tgapi, tgchat, msg, proxy)
            if ok:
                logger.info(f"📨 TG → {tgchat}: стакан {name} отправлен")
        else:
            logger.info(f"   (TG не настроен для {name})")
    else:
        logger.info(f"   (TG отключён глобально)")


# ─── Основной цикл ────────────────────────────────────────────────────────────
def robot():
    cleanup_flag()
    logger.info("=== Робот запущен ===")

    qp   = None
    conn = None

    try:
        qp = QuikPy()
        # Вешаем колбэк на обновления стакана
        qp.on_quote.subscribe(on_quote_callback)
        logger.info("✅ Подключение к QUIK установлено")

        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        logger.info("✅ Подключение к БД установлено")

        # ── Одноразовое чтение настроек при старте ──────────────────────────
        try:
            proxy = fetch_active_proxy(conn)
            logger.info(f"🌐 Прокси: {proxy['host']}:{proxy['port']}" if proxy
                        else "🌐 Прокси не выбран")
        except Exception as e:
            logger.warning(f"⚠️ Прокси: {e}")
            proxy = None

        try:
            tg_enabled = fetch_tg_enabled(conn)
            logger.info(f"📣 Отправка TG: {'ВКЛ' if tg_enabled else 'ВЫКЛ'}")
        except Exception as e:
            logger.warning(f"⚠️ tg_enabled: {e}")
            tg_enabled = False

        # ── Подписка на стаканы ─────────────────────────────────────────────
        instruments = fetch_instruments(conn)
        logger.info(f"📋 Инструментов в БД: {len(instruments)}")

        subscribe_all_books(qp, instruments)

        # Небольшая пауза — даём QUIK время прислать первые данные
        time.sleep(2)

        preload_orderbooks(qp, instruments)

        # ── Основной цикл ───────────────────────────────────────────────────
        iteration = 0

        while not should_stop():
            iteration += 1
            logger.info(f"=== Итерация {iteration} ===")

            try:
                rows = fetch_instruments(conn)
            except Exception as e:
                logger.error(f"❌ Ошибка чтения инструментов: {e}")
                rows = []

            for row in rows:
                if should_stop():
                    break
                process_instrument(dict(row), proxy, tg_enabled)

            logger.info(f"✅ Итерация {iteration} завершена, строк: {len(rows)}")

            try:
                decay = fetch_decay(conn)
            except Exception as e:
                logger.warning(f"⚠️ decay: {e}")
                decay = 1.0

            logger.info(f"⏱ Задержка {decay} сек.")
            elapsed = 0.0
            while elapsed < decay and not should_stop():
                time.sleep(0.2)
                elapsed += 0.2

        logger.info("🟡 Получен сигнал остановки")

    except Exception:
        logger.error("❌ Критическая ошибка:\n" + traceback.format_exc())

    finally:
        if qp:
            try:
                # Отписываемся от всех стаканов
                if conn:
                    for row in fetch_instruments(conn):
                        try:
                            qp.process_request({
                                "data": f"{row['board']}|{row['isin']}",
                                "id": 0,
                                "cmd": "Unsubscribe_Level_II_Quotes",
                                "t": ""
                            })
                        except Exception:
                            pass
                qp.close_connection_and_thread()
                logger.info("🔒 Соединение с QUIK закрыто")
            except Exception as e:
                logger.warning(f"⚠️ Закрытие QUIK: {e}")
        if conn:
            try:
                conn.close()
                logger.info("🗄️ БД закрыта")
            except Exception:
                pass
        cleanup_flag()
        logger.info("=== Робот остановлен ===")


if __name__ == "__main__":
    robot()