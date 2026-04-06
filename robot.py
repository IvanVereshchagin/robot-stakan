# robot.py
import time
import os
import logging
import traceback
from QuikPy import QuikPy

# ─── Логирование ─────────────────────────────────────────────────────────────
LOG_PATH = os.path.join(os.path.dirname(__file__), "robot.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8")]
)
logger = logging.getLogger("robot")

# ─── Флаг остановки (GUI создаёт файл stop.flag для сигнала) ─────────────────
STOP_FLAG = os.path.join(os.path.dirname(__file__), "stop.flag")

def should_stop() -> bool:
    return os.path.exists(STOP_FLAG)

def cleanup_flag():
    try:
        if os.path.exists(STOP_FLAG):
            os.remove(STOP_FLAG)
    except Exception:
        pass


# ─── Основной цикл ───────────────────────────────────────────────────────────
def robot():
    cleanup_flag()   # На старте всегда убираем старый флаг
    logger.info("=== Робот запущен ===")

    qp = None
    try:
        qp = QuikPy()
        logger.info("✅ Подключение к QUIK установлено")

        while not should_stop():
            try:
                qp.message_info("Ok")
                logger.info("📨 Отправлено сообщение Ok в QUIK")
            except Exception as e:
                logger.warning(f"⚠️ Ошибка при отправке сообщения: {e}")

            # Ждём 5 секунд, но проверяем флаг каждые 0.5с
            for _ in range(10):
                if should_stop():
                    break
                time.sleep(0.5)

        logger.info("🟡 Получен сигнал остановки, завершаю робот...")

    except Exception:
        logger.error("❌ Критическая ошибка:\n" + traceback.format_exc())

    finally:
        if qp:
            try:
                qp.close_connection_and_thread()
                logger.info("🔒 Соединение с QUIK закрыто")
            except Exception as e:
                logger.warning(f"⚠️ Ошибка при закрытии QUIK: {e}")
        cleanup_flag()
        logger.info("=== Робот остановлен ===")


if __name__ == "__main__":
    robot()