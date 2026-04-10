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
from decimal import Decimal
from datetime import datetime


def parse_trade_interval(interval_str: str):
    """
    '10:00-23:50' -> (time(10,00), time(23,50))
    Поддерживает и ночной интервал, например '23:00-02:00'.
    """
    try:
        raw = (interval_str or "").strip()
        left, right = raw.split("-", 1)
        start_t = datetime.strptime(left.strip(), "%H:%M").time()
        end_t   = datetime.strptime(right.strip(), "%H:%M").time()
        return start_t, end_t
    except Exception:
        return None, None


def is_now_in_trade_interval(interval_str: str) -> bool:
    """
    True  -> текущее системное время внутри trade_interval
    False -> вне интервала или формат кривой
    """
    start_t, end_t = parse_trade_interval(interval_str)
    if start_t is None or end_t is None:
        return False

    now_t = datetime.now().time().replace(second=0, microsecond=0)

    # обычный интервал
    if start_t <= end_t:
        return start_t <= now_t <= end_t

    # интервал через полночь, например 23:00-02:00
    return now_t >= start_t or now_t <= end_t


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
orderbook_cache: dict = {}
orderbook_lock  = RLock()

# ─── Кэш best_offer заявок ───────────────────────────────────────────────────
# key: isin -> {"order_num": str, "price": float, "qty": int}
best_offer_orders: dict = {}
best_offer_lock = RLock()

# ─── TRANS_ID префикс best_offer ─────────────────────────────────────────────
BEST_OFFER_TRANS_ID_PREFIX = "1212"
BATTLE_TRANS_ID_PREFIX = "1313"


battle_triggered: dict = {}
battle_lock = RLock()


def send_battle_order(qp: QuikPy, board: str, isin: str,
                      price: float, qty: int,
                      account: str, client_code: str) -> None:
    qp.send_transaction({
        "ACCOUNT":     account,
        "CLIENT_CODE": client_code,
        "TYPE":        "L",
        "OPERATION":   "S",
        "CLASSCODE":   board,
        "SECCODE":     isin,
        "PRICE":       str(price),
        "QUANTITY":    str(qty),
        "TRANS_ID":    BATTLE_TRANS_ID_PREFIX,
        "ACTION":      "NEW_ORDER",
    })
    logger.info(f"Battle order выставлен: {isin} SELL {qty} @ {price}")


# ─── Вспомогательная: парсинг datetime из QUIK ───────────────────────────────
def parse_quik_dt(d: dict) -> str:
    from datetime import datetime as _dt
    try:
        return _dt(
            d.get("year", 0), d.get("month", 0), d.get("day", 0),
            d.get("hour", 0), d.get("min",   0), d.get("sec",  0),
            d.get("ms",   0) * 1000
        ).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    except Exception:
        return "-"


# ─── Callback: обновление стакана ────────────────────────────────────────────
def on_quote_callback(data):
    q        = data.get("data") or {}
    sec_code = q.get("sec_code")
    if not sec_code:
        return
    bids   = q.get("bid")   or []
    offers = q.get("offer") or []
    with orderbook_lock:
        old = orderbook_cache.get(sec_code)
        if old:
            if (len(bids) == len(old.get("bid", [])) and
                    len(offers) == len(old.get("offer", [])) and
                    bids == old.get("bid") and offers == old.get("offer")):
                return
        orderbook_cache[sec_code] = {"bid": bids, "offer": offers}


# ─── Callback: заявка (OnOrder) ──────────────────────────────────────────────
def on_order_callback(data):
    order    = data.get("data") or {}
    trans_id = str(order.get("trans_id") or "")
    if not trans_id.startswith(BEST_OFFER_TRANS_ID_PREFIX):
        return

    ORDER_NUM    = str(order.get("order_num") or "")
    flags        = int(order.get("flags") or 0)
    IS_ACTIVE    = int(bool(flags & 0x1))
    IS_CANCELLED = int(bool(flags & 0x2))
    IS_SELL      = int(bool(flags & 0x4))
    PRICE        = order.get("price")
    QTY          = int(order.get("qty")     or 0)
    BALANCE      = int(order.get("balance") or 0)
    ISIN         = order.get("sec_code")
    BOARD        = order.get("class_code")
    ACCOUNT      = order.get("account")
    CLIENT_CODE  = order.get("client_code")
    DT_PLACE     = parse_quik_dt(order.get("datetime")          or {})
    DT_KILL      = parse_quik_dt(order.get("withdraw_datetime") or {})

    # Обновляем in-memory кэш
    with best_offer_lock:
        if IS_ACTIVE:
            best_offer_orders[ISIN] = {
                "order_num": ORDER_NUM,
                "price":     float(PRICE or 0),
                "qty":       QTY,
                "balance":   BALANCE,
            }
        else:
            best_offer_orders.pop(ISIN, None)

    # Пишем в БД своим соединением — колбэк живёт в CallbackThread
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO best_offer_orders (
                        order_num, is_active, is_cancelled, is_sell,
                        price, qty, balance, trans_id,
                        isin, board, account, client_code, dt_place, dt_kill
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (order_num) DO UPDATE SET
                        is_active    = EXCLUDED.is_active,
                        is_cancelled = EXCLUDED.is_cancelled,
                        price        = EXCLUDED.price,
                        qty          = EXCLUDED.qty,
                        balance      = EXCLUDED.balance,
                        dt_kill      = EXCLUDED.dt_kill
                """, (
                    ORDER_NUM, IS_ACTIVE, IS_CANCELLED, IS_SELL,
                    PRICE, QTY, BALANCE, trans_id,
                    ISIN, BOARD, ACCOUNT, CLIENT_CODE, DT_PLACE, DT_KILL
                ))
        status = "активна" if IS_ACTIVE else "снята"
        logger.info(f"📋 Заявка {ORDER_NUM} ({ISIN}) {status} @ {PRICE} x {QTY}")
    except Exception as e:
        logger.warning(f"⚠️ Ошибка записи заявки {ORDER_NUM}: {e}")


# ─── Callback: сделка (OnTrade) ──────────────────────────────────────────────
def on_trade_callback(data):
    trade    = data.get("data") or {}
    trans_id = str(trade.get("trans_id") or "")
    if not trans_id.startswith(BEST_OFFER_TRANS_ID_PREFIX):
        return

    TRADE_NUM = str(trade.get("trade_num") or "")
    ORDER_NUM = str(trade.get("order_num") or "")
    PRICE     = trade.get("price")
    QTY       = int(trade.get("qty") or 0)
    flags     = int(trade.get("flags") or 0)
    IS_SELL   = int(bool(flags & 0x4))
    ISIN      = trade.get("sec_code")
    BOARD     = trade.get("class_code")
    ACCOUNT   = trade.get("account")
    DT_TRADE  = parse_quik_dt(trade.get("datetime") or {})

    # Снимаем из кэша — process_instrument перевыставит на следующей итерации
    with best_offer_lock:
        best_offer_orders.pop(ISIN, None)

    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO best_offer_trades (
                        trade_num, order_num, price, qty, is_sell,
                        trans_id, isin, board, account, dt_trade
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (trade_num) DO UPDATE SET
                        qty   = EXCLUDED.qty,
                        price = EXCLUDED.price
                """, (
                    TRADE_NUM, ORDER_NUM, PRICE, QTY, IS_SELL,
                    trans_id, ISIN, BOARD, ACCOUNT, DT_TRADE
                ))
        logger.info(f"💰 Сделка {TRADE_NUM} ({ISIN}) @ {PRICE} x {QTY} — перевыставим на след. итерации")
    except Exception as e:
        logger.warning(f"⚠️ Ошибка записи сделки {TRADE_NUM}: {e}")


# ─── Подписка / инициализация стаканов ───────────────────────────────────────
def subscribe_all_books(qp: QuikPy, instruments: list):
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
    for row in instruments:
        board = row["board"]
        isin  = row["isin"]
        try:
            resp   = qp.get_quote_level2(board, isin)
            d      = resp.get("data") or {}
            bids   = d.get("bid")   or []
            offers = d.get("offer") or []
            with orderbook_lock:
                orderbook_cache[isin] = {"bid": bids, "offer": offers}
            if bids or offers:
                logger.info(f"📖 Стакан {board}.{isin}: bid={len(bids)} offer={len(offers)}")
            else:
                logger.warning(f"⚠️ Пустой стакан при старте: {board}.{isin}")
        except Exception as e:
            logger.warning(f"⚠️ Ошибка инициализации стакана {board}.{isin}: {e}")


# ─── Telegram ────────────────────────────────────────────────────────────────
def send_telegram(tgapi: str, chat_id: str, text: str, proxy: Optional[dict]) -> bool:
    url     = f"https://api.telegram.org/bot{tgapi}/sendMessage"
    session = requests.Session()
    if proxy:
        user  = proxy.get("username", "")
        pwd   = proxy.get("password", "")
        auth  = f"{user}:{pwd}@" if user else ""
        p_url = f"socks5h://{auth}{proxy['host']}:{proxy['port']}"
        session.proxies = {"http": p_url, "https": p_url}
    try:
        resp = session.post(url, json={"chat_id": chat_id, "text": text}, timeout=30)
        if resp.status_code == 200:
            return True
        logger.warning(f"⚠️ TG {resp.status_code}: {resp.text[:120]}")
        return False
    except Exception as e:
        logger.warning(f"⚠️ Ошибка TG ({chat_id}): {e}")
        return False
    finally:
        session.close()


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


# ─── Расчёт bid_curr ─────────────────────────────────────────────────────────
def calc_bid_curr(isin: str, price_limit: float) -> int:
    if not price_limit or price_limit <= 0:
        return 0
    with orderbook_lock:
        ob = orderbook_cache.get(isin)
    if not ob:
        return 0
    total = 0
    for lvl in (ob.get("bid") or []):
        try:
            if float(lvl.get("price", 0)) >= price_limit:
                total += int(lvl.get("quantity", 0))
        except (ValueError, TypeError):
            pass
    return total

def update_bid_curr(conn, isin: str, bid_curr: int):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE instruments SET bid_curr = %s WHERE isin = %s",
            (bid_curr, isin)
        )


# ─── Алерты по стакану ───────────────────────────────────────────────────────
def check_big_bid_alerts(isin: str, price_limit: float, big_bid_alert_qty: int) -> list:
    if not big_bid_alert_qty or big_bid_alert_qty <= 0:
        return []
    if not price_limit or price_limit <= 0:
        return []
    with orderbook_lock:
        ob = orderbook_cache.get(isin)
    if not ob:
        return []
    hits = []
    for lvl in (ob.get("bid") or []):
        try:
            p = float(lvl.get("price", 0))
            q = int(lvl.get("quantity", 0))
            if p >= price_limit and q >= big_bid_alert_qty:
                hits.append((p, q))
        except (ValueError, TypeError):
            pass
    hits.sort(key=lambda x: x[0], reverse=True)
    return hits


# ─── Best offer: шаг цены ────────────────────────────────────────────────────
def get_price_step(qp: QuikPy, board: str, isin: str) -> float:
    try:
        r   = qp.get_param_ex(board, isin, "SEC_PRICE_STEP")
        val = r["data"].get("param_value") or "0.01"
        s   = float(str(val).replace(",", "."))
        return s if s > 0 else 0.01
    except Exception:
        return 0.01


# ─── Best offer: выставить / снять заявку ────────────────────────────────────
def send_best_offer_order(qp: QuikPy, board: str, isin: str,
                          price: float, qty: int,
                          account: str, client_code: str) -> None:
    qp.send_transaction({
        "ACCOUNT":     account,
        "CLIENT_CODE": client_code,
        "TYPE":        "L",
        "OPERATION":   "S",
        "CLASSCODE":   board,
        "SECCODE":     isin,
        "PRICE":       str(price),
        "QUANTITY":    str(qty),
        "TRANS_ID":    BEST_OFFER_TRANS_ID_PREFIX,
        "ACTION":      "NEW_ORDER",
    })
    logger.info(f"📤 Best offer выставлен: {isin} SELL {qty} @ {price}")


def cancel_best_offer_order(qp: QuikPy, board: str, isin: str,
                            order_num: str, account: str) -> None:
    qp.send_transaction({
        "ACCOUNT":   account,
        "CLASSCODE": board,
        "SECCODE":   isin,
        "ORDER_KEY": order_num,
        "TRANS_ID":  BEST_OFFER_TRANS_ID_PREFIX,
        "ACTION":    "KILL_ORDER",
    })
    logger.info(f"❌ Best offer снят: {isin} order_num={order_num}")




# ─── Best offer: снять все при остановке / очистить БД ───────────────────────
def cancel_all_active_best_offers(qp: QuikPy, conn) -> None:
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM best_offer_orders WHERE is_active = 1")
            rows = cur.fetchall()
    except Exception as e:
        logger.warning(f"⚠️ Не удалось прочитать active best_offer orders: {e}")
        return

    if not rows:
        logger.info("✅ Активных best_offer заявок нет — снимать нечего")
        return

    for row in rows:
        try:
            cancel_best_offer_order(qp, row["board"], row["isin"],
                                    row["order_num"], row["account"])
        except Exception as e:
            logger.warning(f"⚠️ Ошибка снятия {row['order_num']}: {e}")

    time.sleep(1.5)  # даём QUIK обработать снятие


def cleanup_best_offer_db(conn) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM best_offer_orders WHERE is_active = 1")
            still_active = cur.fetchone()[0]
        if still_active:
            logger.warning(
                f"⚠️ После снятия в БД ещё {still_active} активных заявок — чистим"
            )
        with conn.cursor() as cur:
            cur.execute("DELETE FROM best_offer_orders")
            cur.execute("DELETE FROM best_offer_trades")
        logger.info("🧹 Таблицы best_offer_orders и best_offer_trades очищены")
    except Exception as e:
        logger.warning(f"⚠️ Ошибка очистки best_offer таблиц: {e}")

# ─── Проверка заявок при старте ──────────────────────────────────────────────
def startup_check_best_offers(qp: QuikPy, conn) -> None:
    """
    Выполняется один раз перед основным циклом.
    Читает все записи best_offer_orders из БД, для каждой запрашивает
    статус заявки у QUIK и снимает её если она ещё активна в рынке.
    После этого чистит таблицы best_offer_orders и best_offer_trades.
    """
    logger.info("🔍 Стартовая проверка заявок best_offer в БД...")
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM best_offer_orders")
            rows = cur.fetchall()
    except Exception as e:
        logger.warning(f"⚠️ Стартовая проверка: не удалось прочитать best_offer_orders: {e}")
        return

    if not rows:
        logger.info("✅ Стартовая проверка: таблица best_offer_orders пуста — ок")
        return

    logger.info(f"⚠️ Стартовая проверка: найдено {len(rows)} записей в best_offer_orders — проверяем в QUIK")

    cancelled = 0
    for row in rows:
        order_num = str(row.get("order_num") or "")
        board     = str(row.get("board")     or "")
        isin      = str(row.get("isin")      or "")
        account   = str(row.get("account")   or "")

        if not order_num:
            continue

        # Спрашиваем QUIK актуальный статус заявки
        try:
            resp  = qp.get_order_by_number(board, int(order_num))
            flags = int((resp.get("data") or {}).get("flags") or 0)
            is_active_in_market = bool(flags & 0x1)
        except Exception as e:
            # Если QUIK не знает заявку — считаем её неактивной, просто пропускаем
            logger.warning(f"⚠️ Стартовая проверка: get_order_by_number({order_num}) → {e}")
            is_active_in_market = False

        if is_active_in_market:
            try:
                cancel_best_offer_order(qp, board, isin, order_num, account)
                cancelled += 1
                logger.info(f"   ❌ Снята живая заявка {order_num} ({isin})")
            except Exception as e:
                logger.warning(f"⚠️ Стартовая проверка: не удалось снять {order_num}: {e}")
        else:
            logger.info(f"   ✅ Заявка {order_num} ({isin}) уже неактивна в рынке")

    if cancelled:
        logger.info(f"⏳ Ждём 1.5 сек чтобы QUIK обработал снятие...")
        time.sleep(1.5)

    cleanup_best_offer_db(conn)
    logger.info("✅ Стартовая проверка завершена")


# ─── Обработка одной строки ──────────────────────────────────────────────────
def process_instrument(conn, qp: QuikPy, row: dict,
                       proxy: Optional[dict], tg_enabled: bool = False):
    name  = row.get("name",  "—")
    isin  = row.get("isin",  "—")
    board = row.get("board", "—")

    logger.info(f"── {name} ({isin}) ──")

    # Пропускаем если condition != ON
    if (row.get("condition") or "").strip().upper() != "ON":
        logger.info(f"   ⏭ Пропускаем — condition = {row.get('condition')}")
        with best_offer_lock:
            active = best_offer_orders.get(isin)
        if active:
            account = (row.get("account") or "").strip()
            logger.info(f"   Снимаем best_offer т.к. condition=OFF: {isin}")
            cancel_best_offer_order(qp, board, isin, active["order_num"], account)
        return

    for key, value in row.items():
        logger.info(f"   {key} = {value}")

    price_limit       = float(row.get("price_limit")    or 0)
    bid_limit         = int(row.get("bid_limit")         or 0)
    big_bid_alert_qty = int(row.get("big_bid_alert_qty") or 0)
    tgapi             = (row.get("tgapi")  or "").strip()
    tgchat            = (row.get("tgchat") or "").strip()
    tg_ready          = tg_enabled and bool(tgapi) and bool(tgchat)
    account           = (row.get("account")     or "").strip()
    client_code       = (row.get("client_code") or "").strip()
    battle_regime_on  = (row.get("battle_regime") or "").strip().upper() == "ON"
    trade_interval = str(row.get("trade_interval") or "10:00-23:50").strip()

    # ── 1. bid_curr ───────────────────────────────────────────────────────────
    try:
        bid_curr = calc_bid_curr(isin, price_limit)
        update_bid_curr(conn, isin, bid_curr)
        logger.info(f"   bid_curr = {bid_curr}  (price_limit >= {price_limit})")
    except Exception as e:
        logger.warning(f"⚠️ bid_curr для {name}: {e}")
        bid_curr = 0

    # ── 2. Алерт bid_curr >= bid_limit ───────────────────────────────────────
    if bid_limit > 0 and bid_curr >= bid_limit:
        msg = (
            f"🔔 {name} | {isin}\n"
            f"Сумма бидов ≥ {price_limit}: {bid_curr} шт\n"
            f"Превышает лимит: {bid_limit} шт"
        )
        logger.info(f"   🔔 bid_curr ({bid_curr}) >= bid_limit ({bid_limit})")
        if tg_ready:
            if send_telegram(tgapi, tgchat, msg, proxy):
                logger.info(f"📨 TG → {tgchat}: алерт bid_limit")
        else:
            logger.info("   (TG отключён или не настроен)")


    # ── 2.1 Battle regime: sell при превышении bid_limit ─────────────────────
   # ── 2.1 Battle regime: sell при превышении bid_limit ─────────────────────
    if battle_regime_on and price_limit > 0 and bid_limit > 0 and account:

        if not is_now_in_trade_interval(trade_interval):
            logger.info(
                f"   ⚔️ Battle regime: текущее время вне trade_interval "
                f"({trade_interval}) — заявку не выставляем"
            )
            with battle_lock:
                battle_triggered[isin] = False

        elif bid_curr > bid_limit:
            with battle_lock:
                already_triggered = battle_triggered.get(isin, False)

            if not already_triggered:
                logger.info(
                    f"   ⚔️ Battle regime: bid_curr ({bid_curr}) > bid_limit ({bid_limit}) "
                    f"→ выставляем SELL {bid_limit} @ {price_limit}"
                )
                try:
                    send_battle_order(
                        qp=qp,
                        board=board,
                        isin=isin,
                        price=price_limit,
                        qty=bid_limit,
                        account=account,
                        client_code=client_code,
                    )
                    with battle_lock:
                        battle_triggered[isin] = True
                except Exception as e:
                    logger.warning(f"⚠️ Battle regime {isin}: не удалось выставить заявку: {e}")
            else:
                logger.info(
                    f"   ⚔️ Battle regime: превышение уже обработано ранее, повторно не выставляем"
                )

        else:
            with battle_lock:
                if battle_triggered.get(isin):
                    logger.info(
                        f"   ⚔️ Battle regime: bid_curr снова <= bid_limit, сбрасываем триггер"
                    )
                battle_triggered[isin] = False
    else:
        with battle_lock:
            battle_triggered[isin] = False

    # ── 3. Алерт крупные биды ────────────────────────────────────────────────
    if big_bid_alert_qty > 0:
        hits = check_big_bid_alerts(isin, price_limit, big_bid_alert_qty)
        if hits:
            lines = [f"🐋 {name} | {isin}", f"Крупные биды ≥ {price_limit}:"]
            for p, q in hits:
                lines.append(f"  цена {p:.4f} — {q} шт")
            msg = "\n".join(lines)
            logger.info(f"   🐋 Крупные биды: {hits}")
            if tg_ready:
                if send_telegram(tgapi, tgchat, msg, proxy):
                    logger.info(f"📨 TG → {tgchat}: алерт big_bid")
            else:
                logger.info("   (TG отключён или не настроен)")

    # ── 4. Best offer логика ──────────────────────────────────────────────────
    best_offer_on  = (row.get("best_offer") or "").strip().upper() == "ON"
    best_offer_qty = int(row.get("best_offer_qty") or 0)
    best_offer_limit = float(row.get("best_offer_limit") or 0)
    

    # 4.0 — выключен → снять заявку если есть
    if not best_offer_on:
        with best_offer_lock:
            active = best_offer_orders.get(isin)
        if active:
            logger.info(f"   Best offer ВЫКЛ — снимаем {active['order_num']}")
            cancel_best_offer_order(qp, board, isin, active["order_num"], account)
        return
    
    if not is_now_in_trade_interval(trade_interval):
        with best_offer_lock:
            active = best_offer_orders.get(isin)

        if active:
            logger.info(
                f"   Best offer: текущее время вне trade_interval "
                f"({trade_interval}) — снимаем заявку {active['order_num']}"
            )
            cancel_best_offer_order(qp, board, isin, active["order_num"], account)
        else:
            logger.info(
                f"   Best offer: текущее время вне trade_interval "
                f"({trade_interval}) — новую заявку не ставим"
            )
        return

    if not (best_offer_qty > 0 and account):
        logger.warning(f"   Best offer: аккаунт или qty не заданы для {name}")
        return

    # 4.1 — читаем стакан
    with orderbook_lock:
        ob = orderbook_cache.get(isin) or {}
    offers = ob.get("offer") or []

    if not offers:
        logger.info(f"   Best offer: стакан офферов пуст — пропускаем")
        return

    best_ask   = float(offers[0].get("price", 0))
    price_step = get_price_step(qp, board, isin)

    with best_offer_lock:
        active = best_offer_orders.get(isin)

    our_price   = round(float(active["price"]),   8) if active else None
    our_balance = int(active.get("balance") or active.get("qty") or 0) if active else 0
    best_ask_r  = round(best_ask, 8)

    # 4.2 — наша заявка стоит по цене лучшего оффера (мы best offer)
        # 4.2 — наша заявка стоит по цене лучшего оффера
    if active and our_price is not None and abs(our_price - best_ask_r) < price_step * 0.01:

        # Если лимит изменили из GUI и он стал выше/равен нашей цене — снимаем и ждём
        if best_offer_limit > 0 and our_price <= best_offer_limit:
            logger.info(
                f"   Best offer: текущая цена {our_price} <= лимит {best_offer_limit} — снимаем и ждём"
            )
            cancel_best_offer_order(qp, board, isin, active["order_num"], account)
            return

        # Стоим лучшим оффером полным объёмом — ничего не делаем
        if our_balance == best_offer_qty:
            logger.info(
                f"   Best offer: ✅ уже стоим лучшим оффером @ {our_price}, "
                f"balance={our_balance} — ок"
            )
            return

        # Частично исполнили: надо восстановить полный объём best_offer_qty
        if 0 < our_balance < best_offer_qty:
            logger.info(
                f"   Best offer: частично съели заявку "
                f"({our_balance} из {best_offer_qty}) @ {our_price} — восстанавливаем объём"
            )
            cancel_best_offer_order(qp, board, isin, active["order_num"], account)
            time.sleep(0.3)
            send_best_offer_order(
                qp, board, isin, our_price,
                best_offer_qty, account, client_code
            )
            return

        # На всякий случай: если balance пришёл 0 или кривой, ничего резко не делаем.
        # Полное исполнение обработается через callbacks и следующую итерацию.
        logger.info(
            f"   Best offer: balance={our_balance}, ждём подтверждения из QUIK"
        )
        return

    # 4.3 — нашей заявки по цене best_ask нет → цель = best_ask - шаг
    target_price = round(best_ask - price_step, 8)

    # Правило 6: ниже лимита — не выставляем, снимаем если стоим
    if best_offer_limit > 0 and target_price <= best_offer_limit:
        logger.info(
            f"   Best offer: target {target_price} <= лимит {best_offer_limit} — не выставляем"
        )
        if active:
            cancel_best_offer_order(qp, board, isin, active["order_num"], account)
        return

    if active:
        if abs(our_price - target_price) < price_step * 0.01:
            # Уже стоим по нужной цене — ничего не делаем
            logger.info(f"   Best offer: ✅ стоим @ {our_price} (target={target_price}) — ок")
            return
        # Правило 5b: наша заявка по другой цене → снимаем и перевыставляем
        logger.info(
            f"   Best offer: цена изменилась {our_price} → {target_price}, перевыставляем"
        )
        cancel_best_offer_order(qp, board, isin, active["order_num"], account)
        time.sleep(0.3)

    # Правило 5: выставляем по best_ask - шаг
    logger.info(f"   Best offer: выставляем {best_offer_qty} @ {target_price}")
    send_best_offer_order(qp, board, isin, target_price,
                          best_offer_qty, account, client_code)


# ─── Основной цикл ───────────────────────────────────────────────────────────
def robot():
    cleanup_flag()
    logger.info("=== Робот запущен ===")

    qp   = None
    conn = None

    try:
        qp = QuikPy()
        qp.on_quote.subscribe(on_quote_callback)
        qp.on_order.subscribe(on_order_callback)
        qp.on_trade.subscribe(on_trade_callback)
        logger.info("✅ Подключение к QUIK установлено")

        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        logger.info("✅ Подключение к БД установлено")

        # Одноразовые настройки при старте
        try:
            proxy = fetch_active_proxy(conn)
            logger.info(
                f"🌐 Прокси: {proxy['host']}:{proxy['port']}" if proxy
                else "🌐 Прокси не выбран"
            )
        except Exception as e:
            logger.warning(f"⚠️ Прокси: {e}")
            proxy = None

        try:
            tg_enabled = fetch_tg_enabled(conn)
            logger.info(f"📣 Отправка TG: {'ВКЛ' if tg_enabled else 'ВЫКЛ'}")
        except Exception as e:
            logger.warning(f"⚠️ tg_enabled: {e}")
            tg_enabled = False

        # Подписка на стаканы
        instruments = fetch_instruments(conn)
        logger.info(f"📋 Инструментов в БД: {len(instruments)}")
        subscribe_all_books(qp, instruments)
        time.sleep(2)
        preload_orderbooks(qp, instruments)

        # Стартовая проверка остатков заявок от прошлого сеанса
        startup_check_best_offers(qp, conn)

        # Основной цикл
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
                process_instrument(conn, qp, dict(row), proxy, tg_enabled)

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
                    cancel_all_active_best_offers(qp, conn)
                    cleanup_best_offer_db(conn)

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