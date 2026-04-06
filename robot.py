# robot.py
import time
import os
import logging
import traceback
import psycopg2
import psycopg2.extras
from psycopg2.extras import RealDictCursor
from QuikPy import QuikPy

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


# ─── Чтение из БД ────────────────────────────────────────────────────────────
def fetch_instruments(conn) -> list:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM instruments ORDER BY name")
        return cur.fetchall()

def fetch_decay(conn) -> float:
    with conn.cursor() as cur:
        cur.execute("SELECT decay FROM decay WHERE id = 1")
        row = cur.fetchone()
    return float(row[0]) if row and row[0] is not None else 1.0


# ─── Обработка одной строки ───────────────────────────────────────────────────
def process_instrument(row: dict):
    logger.info(f"── Инструмент: {row['name']} (ISIN: {row['isin']}) ──")
    for key, value in row.items():
        logger.info(f"   {key} = {value}")


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
                process_instrument(row)

            logger.info(f"✅ Итерация {iteration} завершена, обработано строк: {len(rows)}")

            # 3. Читаем задержку из БД
            try:
                decay = fetch_decay(conn)
            except Exception as e:
                logger.warning(f"⚠️ Не удалось прочитать decay: {e}, использую 1.0")
                decay = 1.0

            logger.info(f"⏱ Задержка {decay} сек.")

            # 4. Ждём decay секунд, проверяя флаг каждые 0.2с
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