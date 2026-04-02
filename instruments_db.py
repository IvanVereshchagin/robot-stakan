# instruments_db.py
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

DB_CONFIG = {
    "dbname":   "instrumentsdb",
    "user":     "postgres",
    "password": "1234",
    "host":     "localhost",
    "port":     5432,
}
ADMIN = {**DB_CONFIG, "dbname": "postgres"}

# ─── Глобальное соединение ───────────────────────────────────────────────────
_conn = None

def get_connection():
    global _conn
    try:
        if _conn is None or _conn.closed:
            raise Exception
        with _conn.cursor() as cur:
            cur.execute("SELECT 1")
    except Exception:
        try:
            if _conn: _conn.close()
        except Exception:
            pass
        _conn = psycopg2.connect(**DB_CONFIG)
        _conn.autocommit = True
    return _conn


# ─── DDL ─────────────────────────────────────────────────────────────────────
DDL_STATEMENTS = [
    # Основная таблица
    """
    CREATE TABLE IF NOT EXISTS instruments (
        id               BIGSERIAL PRIMARY KEY,
        name             TEXT    NOT NULL,
        isin             TEXT    NOT NULL,
        board            TEXT    NOT NULL,
        condition        TEXT    NOT NULL DEFAULT 'OFF',
        battle_regime    TEXT    NOT NULL DEFAULT 'OFF',
        trade_interval   TEXT    NOT NULL DEFAULT '10:00-23:50',
        best_offer_qty   INT     NOT NULL DEFAULT 0,
        best_offer       TEXT    NOT NULL DEFAULT 'OFF',
        price_limit      INT     NOT NULL DEFAULT 0,
        bid_limit        INT     NOT NULL DEFAULT 0,
        bid_curr         INT     NOT NULL DEFAULT 0,
        trades_limit     INT     NOT NULL DEFAULT 0,
        trades_curr      INT     NOT NULL DEFAULT 0,
        big_bid_alert_qty INT    NOT NULL DEFAULT 0
    );
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_instruments_isin ON instruments (isin);",

    # ALTER TABLE — добавляем колонки если таблица уже существовала без них
    "ALTER TABLE instruments ADD COLUMN IF NOT EXISTS condition        TEXT NOT NULL DEFAULT 'OFF';",
    "ALTER TABLE instruments ADD COLUMN IF NOT EXISTS battle_regime    TEXT NOT NULL DEFAULT 'OFF';",
    "ALTER TABLE instruments ADD COLUMN IF NOT EXISTS trade_interval   TEXT NOT NULL DEFAULT '10:00-23:50';",
    "ALTER TABLE instruments ADD COLUMN IF NOT EXISTS best_offer_qty   INT  NOT NULL DEFAULT 0;",
    "ALTER TABLE instruments ADD COLUMN IF NOT EXISTS best_offer       TEXT NOT NULL DEFAULT 'OFF';",
    "ALTER TABLE instruments ADD COLUMN IF NOT EXISTS price_limit      INT  NOT NULL DEFAULT 0;",
    "ALTER TABLE instruments ADD COLUMN IF NOT EXISTS bid_limit        INT  NOT NULL DEFAULT 0;",
    "ALTER TABLE instruments ADD COLUMN IF NOT EXISTS bid_curr         INT  NOT NULL DEFAULT 0;",
    "ALTER TABLE instruments ADD COLUMN IF NOT EXISTS trades_limit     INT  NOT NULL DEFAULT 0;",
    "ALTER TABLE instruments ADD COLUMN IF NOT EXISTS trades_curr      INT  NOT NULL DEFAULT 0;",
    "ALTER TABLE instruments ADD COLUMN IF NOT EXISTS big_bid_alert_qty INT NOT NULL DEFAULT 0;",
]

# Поля, которые пользователь может редактировать
ALLOWED_FIELDS = {
    "condition", "battle_regime", "trade_interval",
    "best_offer_qty", "best_offer", "price_limit",
    "bid_limit", "trades_limit", "big_bid_alert_qty",
}


# ─── Инициализация ───────────────────────────────────────────────────────────
def _create_db_if_missing():
    conn = psycopg2.connect(**ADMIN)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_CONFIG["dbname"],))
            if cur.fetchone():
                return
            cur.execute(
                sql.SQL("CREATE DATABASE {} ENCODING 'UTF8'").format(
                    sql.Identifier(DB_CONFIG["dbname"])
                )
            )
            print(f"✅ Создана БД {DB_CONFIG['dbname']}")
    finally:
        conn.close()


def _init_schema():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn:
            with conn.cursor() as cur:
                for ddl in DDL_STATEMENTS:
                    cur.execute(ddl)
    finally:
        conn.close()


def init_db():
    _create_db_if_missing()
    _init_schema()


# ─── CRUD ─────────────────────────────────────────────────────────────────────
def insert_instrument(name: str, isin: str, board: str) -> bool:
    """Возвращает False если ISIN уже есть."""
    con = get_connection()
    try:
        with con.cursor() as cur:
            cur.execute(
                "INSERT INTO instruments (name, isin, board) VALUES (%s, %s, %s)",
                (name.strip(), isin.strip(), board.strip())
            )
        return True
    except psycopg2.errors.UniqueViolation:
        return False


def fetch_all_instruments() -> list:
    con = get_connection()
    with con.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM instruments ORDER BY name")
        return cur.fetchall()


def update_field(isin: str, field: str, value) -> None:
    """Обновляет одно разрешённое поле по ISIN."""
    if field not in ALLOWED_FIELDS:
        raise ValueError(f"Поле '{field}' нельзя редактировать.")
    con = get_connection()
    with con.cursor() as cur:
        cur.execute(
            sql.SQL("UPDATE instruments SET {} = %s WHERE isin = %s").format(
                sql.Identifier(field)
            ),
            (value, isin)
        )


def delete_instrument(isin: str) -> None:
    con = get_connection()
    with con.cursor() as cur:
        cur.execute("DELETE FROM instruments WHERE isin = %s", (isin,))