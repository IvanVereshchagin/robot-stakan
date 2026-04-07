# robot.py
import time
import os
import logging
import traceback
import psycopg2
import psycopg2.extras
from psycopg2.extras import RealDictCursor
from QuikPy import QuikPy

import socks          # pip install PySocks
import socket
import requests       # pip install requests

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


# ─── Telegram ────────────────────────────────────────────────────────────────
def send_telegram(tgapi: str, chat_id: str, text: str, proxy: dict | None) -> bool:
    """
    Отправляет сообщение в Telegram.
    proxy — словарь с ключами host, port, username, password (или None).
    """
    url = f"https://api.telegram.org/bot{tgapi}/sendMessage"

    session = requests.Session()

    if proxy:
        user = proxy.get("username", "")
        pwd  = proxy.get("password", "")
        host = proxy["host"]
        port = proxy["port"]
        auth = f"{user}:{pwd}@" if user else ""
        proxy_url = f"socks5h://{auth}{host}:{port}"
        session.proxies = {"http": proxy_url, "https": proxy_url}
        logger.debug(f"   Прокси: {host}:{port}")

    try:
        resp = session.post(
            url,
            json={"chat_id": chat_id, "text": text},
            timeout=30
        )
        if resp.status_code == 200:
            return True
        else:
            logger.warning(f"⚠️ TG ответил {resp.status_code}: {resp.text[:120]}")
            return False
    except Exception as e:
        logger.warning(f"⚠️ Ошибка отправки TG ({chat_id}): {e}")
        return False
    finally:
        session.close()


# ─── Чтение из БД ────────────────────────────────────────────────────────────
def fetch_active_proxy(conn) -> dict | None:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM proxies WHERE is_active = TRUE LIMIT 1")
        row = cur.fetchone()
    return dict(row) if row else None


def fetch_tg_enabled(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT tg_enabled FROM tg_settings WHERE id = 1")
        row = cur.fetchone()
    return bool(row[0]) if row else False


def fetch_instruments(conn) -> list:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM instruments ORDER BY name")
        return cur.fetchall()

def fetch_decay(conn) -> float:
    with conn.cursor() as cur:
        cur.execute("SELECT decay FROM decay WHERE id = 1")
        row = cur.fetchone()
    return float(row[0]) if row and row[0] is not None else 1.0


# ─── Обработка одной строки ──────────────────────────────────────────────────
def process_instrument(row: dict, proxy: dict | None, tg_enabled: bool = False):
    name = row.get("name", "—")
    isin = row.get("isin", "—")

    logger.info(f"── Инструмент: {name} (ISIN: {isin}) ──")
    for key, value in row.items():
        logger.info(f"   {key} = {value}")

    # Отправка TG-уведомления если заданы оба поля
    tgapi  = (row.get("tgapi")  or "").strip()
    tgchat = (row.get("tgchat") or "").strip()

    if tg_enabled and tgapi and tgchat:
        msg = f"{isin} | {name}"
        ok  = send_telegram(tgapi, tgchat, msg, proxy)
        if ok:
            logger.info(f"📨 TG отправлено → {tgchat}: «{msg}»")
    elif not tg_enabled:
        logger.info(f"   (TG отключён глобально)")
    else:
        logger.info(f"   (TG не настроен для {name})")


# ─── Основной цикл ───────────────────────────────────────────────────────────
def robot():
    cleanup_flag()
    logger.info("=== Робот запущен ===")

    qp   = None
    conn = None

    try:
        qp = QuikPy()
        logger.info("✅ Подключение к QUIK установлено")

        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        logger.info("✅ Подключение к БД установлено")

        # Читаем прокси один раз при старте
        try:
            proxy = fetch_active_proxy(conn)
            if proxy:
                logger.info(f"🌐 Прокси: {proxy['host']}:{proxy['port']}")
            else:
                logger.info("🌐 Прокси не выбран — отправка напрямую")
        except Exception as e:
            logger.warning(f"⚠️ Не удалось прочитать прокси: {e}")
            proxy = None

        # Читаем признак отправки TG один раз при старте
        try:
            tg_enabled = fetch_tg_enabled(conn)
            logger.info(f"📣 Отправка TG: {'ВКЛ' if tg_enabled else 'ВЫКЛ'}")
        except Exception as e:
            logger.warning(f"⚠️ Не удалось прочитать tg_enabled: {e}")
            tg_enabled = False

        iteration = 0

        while not should_stop():
            iteration += 1
            logger.info(f"=== Итерация {iteration} ===")

            # 1. Читаем все инструменты из БД
            try:
                rows = fetch_instruments(conn)
            except Exception as e:
                logger.error(f"❌ Ошибка чтения инструментов: {e}")
                rows = []

            # 2. Обрабатываем каждую строку
            for row in rows:
                if should_stop():
                    break
                process_instrument(dict(row), proxy, tg_enabled)

            logger.info(f"✅ Итерация {iteration} завершена, строк: {len(rows)}")

            # 3. Читаем задержку из БД
            try:
                decay = fetch_decay(conn)
            except Exception as e:
                logger.warning(f"⚠️ Не удалось прочитать decay: {e}, использую 1.0")
                decay = 1.0

            logger.info(f"⏱ Задержка {decay} сек.")

            # 4. Ждём decay секунд с проверкой флага каждые 0.2с
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
                qp.close_connection_and_thread()
                logger.info("🔒 Соединение с QUIK закрыто")
            except Exception as e:
                logger.warning(f"⚠️ Ошибка при закрытии QUIK: {e}")
        if conn:
            try:
                conn.close()
                logger.info("🗄️ Соединение с БД закрыто")
            except Exception:
                pass
        cleanup_flag()
        logger.info("=== Робот остановлен ===")


if __name__ == "__main__":
    robot()