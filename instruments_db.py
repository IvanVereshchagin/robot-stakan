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
        big_bid_alert_qty INT    NOT NULL DEFAULT 0,
        tgapi             TEXT   NOT NULL DEFAULT '',
        tgchat            TEXT   NOT NULL DEFAULT '',
        account           TEXT   NOT NULL DEFAULT '',
        client_code       TEXT   NOT NULL DEFAULT ''
    );
    """,
    "DROP INDEX IF EXISTS uq_instruments_isin;",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_instruments_isin_name ON instruments (isin, name);",

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
    "ALTER TABLE instruments ADD COLUMN IF NOT EXISTS tgapi TEXT NOT NULL DEFAULT '';",
    "ALTER TABLE instruments ADD COLUMN IF NOT EXISTS tgchat TEXT NOT NULL DEFAULT '';",
    "ALTER TABLE instruments ADD COLUMN IF NOT EXISTS account TEXT NOT NULL DEFAULT '';",
    "ALTER TABLE instruments ADD COLUMN IF NOT EXISTS client_code TEXT NOT NULL DEFAULT '';",

    # Аккаунты
    """
    CREATE TABLE IF NOT EXISTS accounts (
        id      BIGSERIAL PRIMARY KEY,
        account TEXT NOT NULL UNIQUE
    );
    """,

    # Коды клиентов
    """
    CREATE TABLE IF NOT EXISTS client_codes (
        id          BIGSERIAL PRIMARY KEY,
        client_code TEXT NOT NULL UNIQUE
    );
    """,

    # Задержка цикла робота
    """
    CREATE TABLE IF NOT EXISTS decay (
        id    INT PRIMARY KEY,
        decay NUMERIC NOT NULL DEFAULT 1.0
    );
    """,
    """
    INSERT INTO decay (id, decay)
    SELECT 1, 1.0
    WHERE NOT EXISTS (SELECT 1 FROM decay WHERE id = 1);
    """,

    # Прокси
    """
    CREATE TABLE IF NOT EXISTS proxies (
        id       BIGSERIAL PRIMARY KEY,
        host     TEXT NOT NULL,
        port     INT  NOT NULL,
        username TEXT NOT NULL DEFAULT '',
        password TEXT NOT NULL DEFAULT '',
        is_active BOOLEAN NOT NULL DEFAULT FALSE
    );
    """,

    # Telegram: API tokens
    """
    CREATE TABLE IF NOT EXISTS tgapi (
        id    BIGSERIAL PRIMARY KEY,
        tgapi TEXT NOT NULL UNIQUE
    );
    """,

    # Telegram: Chat IDs
    """
    CREATE TABLE IF NOT EXISTS tgchat (
        id     BIGSERIAL PRIMARY KEY,
        tgchat TEXT NOT NULL UNIQUE
    );
    """,
]

# Поля, которые пользователь может редактировать
ALLOWED_FIELDS = {
    "condition", "battle_regime", "trade_interval",
    "best_offer_qty", "best_offer", "price_limit",
    "bid_limit", "trades_limit", "big_bid_alert_qty",
    "tgapi", "tgchat", "account", "client_code",
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
    """Возвращает False если пара (isin, name) уже существует."""
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

# ─── Telegram API ─────────────────────────────────────────────────────────────
def fetch_tgapi() -> list:
    con = get_connection()
    with con.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT tgapi FROM tgapi ORDER BY id")
        return [r["tgapi"] for r in cur.fetchall()]

def insert_tgapi(value: str) -> bool:
    """False если уже существует."""
    con = get_connection()
    try:
        with con.cursor() as cur:
            cur.execute("INSERT INTO tgapi (tgapi) VALUES (%s)", (value.strip(),))
        return True
    except psycopg2.errors.UniqueViolation:
        return False

def delete_tgapi(value: str) -> None:
    con = get_connection()
    with con.cursor() as cur:
        cur.execute("DELETE FROM tgapi WHERE tgapi = %s", (value.strip(),))


# ─── Telegram Chat ID ─────────────────────────────────────────────────────────
def fetch_tgchat() -> list:
    con = get_connection()
    with con.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT tgchat FROM tgchat ORDER BY id")
        return [r["tgchat"] for r in cur.fetchall()]

def insert_tgchat(value: str) -> bool:
    """False если уже существует."""
    con = get_connection()
    try:
        with con.cursor() as cur:
            cur.execute("INSERT INTO tgchat (tgchat) VALUES (%s)", (value.strip(),))
        return True
    except psycopg2.errors.UniqueViolation:
        return False

def delete_tgchat(value: str) -> None:
    con = get_connection()
    with con.cursor() as cur:
        cur.execute("DELETE FROM tgchat WHERE tgchat = %s", (value.strip(),))

# ─── Accounts ─────────────────────────────────────────────────────────────────
def fetch_accounts() -> list:
    con = get_connection()
    with con.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT account FROM accounts ORDER BY account")
        return [r["account"] for r in cur.fetchall()]

def insert_account(value: str) -> bool:
    con = get_connection()
    try:
        with con.cursor() as cur:
            cur.execute("INSERT INTO accounts (account) VALUES (%s)", (value.strip(),))
        return True
    except psycopg2.errors.UniqueViolation:
        return False

def delete_account(value: str) -> None:
    con = get_connection()
    with con.cursor() as cur:
        cur.execute("DELETE FROM accounts WHERE account = %s", (value.strip(),))


# ─── Client codes ─────────────────────────────────────────────────────────────
def fetch_client_codes() -> list:
    con = get_connection()
    with con.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT client_code FROM client_codes ORDER BY client_code")
        return [r["client_code"] for r in cur.fetchall()]

def insert_client_code(value: str) -> bool:
    con = get_connection()
    try:
        with con.cursor() as cur:
            cur.execute("INSERT INTO client_codes (client_code) VALUES (%s)", (value.strip(),))
        return True
    except psycopg2.errors.UniqueViolation:
        return False

def delete_client_code(value: str) -> None:
    con = get_connection()
    with con.cursor() as cur:
        cur.execute("DELETE FROM client_codes WHERE client_code = %s", (value.strip(),))

# ─── Decay (задержка цикла) ───────────────────────────────────────────────────
def fetch_decay() -> float:
    con = get_connection()
    with con.cursor() as cur:
        cur.execute("SELECT decay FROM decay WHERE id = 1")
        row = cur.fetchone()
    return float(row[0]) if row and row[0] is not None else 1.0

def update_decay(value: float) -> None:
    con = get_connection()
    with con.cursor() as cur:
        cur.execute(
            "INSERT INTO decay (id, decay) VALUES (1, %s) "
            "ON CONFLICT (id) DO UPDATE SET decay = EXCLUDED.decay",
            (value,)
        )

# ─── Proxies ──────────────────────────────────────────────────────────────────
def fetch_proxies() -> list:
    con = get_connection()
    with con.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM proxies ORDER BY id")
        return cur.fetchall()

def fetch_active_proxy() -> dict | None:
    """Возвращает активный прокси или None."""
    con = get_connection()
    with con.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM proxies WHERE is_active = TRUE LIMIT 1")
        return cur.fetchone()

def insert_proxy(host: str, port: int, username: str = "", password: str = "") -> int:
    """Добавляет прокси, возвращает его id."""
    con = get_connection()
    with con.cursor() as cur:
        cur.execute(
            "INSERT INTO proxies (host, port, username, password) "
            "VALUES (%s, %s, %s, %s) RETURNING id",
            (host.strip(), port, username.strip(), password.strip())
        )
        return cur.fetchone()[0]

def delete_proxy(proxy_id: int) -> None:
    con = get_connection()
    with con.cursor() as cur:
        cur.execute("DELETE FROM proxies WHERE id = %s", (proxy_id,))

def set_active_proxy(proxy_id: int | None) -> None:
    """Снимает активность со всех, ставит на proxy_id (None = никакой)."""
    con = get_connection()
    with con.cursor() as cur:
        cur.execute("UPDATE proxies SET is_active = FALSE")
        if proxy_id is not None:
            cur.execute(
                "UPDATE proxies SET is_active = TRUE WHERE id = %s",
                (proxy_id,)
            )