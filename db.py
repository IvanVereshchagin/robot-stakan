# instruments_db.py
import os
import psycopg2
import psycopg2.extras
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

# ─── Параметры подключения ───────────────────────────────────────────────────
ADMIN = {                       # суперюзер — только для создания БД
    "dbname":   "postgres",
    "user":     "postgres",
    "password": "1234",
    "host":     "localhost",
    "port":     5432,
}

DBNAME = "instrumentsdb"        # имя нашей БД

DB_CONFIG = {                   # рабочее подключение
    "dbname":   DBNAME,
    "user":     "postgres",
    "password": "1234",
    "host":     "localhost",
    "port":     5432,
}

os.environ["PGPASSFILE"]    = "NUL"
os.environ["PGSERVICEFILE"] = "NUL"
os.environ["PGSERVICE"]     = ""

# ─── Глобальное соединение (переиспользуется в GUI) ──────────────────────────
_conn = None

def get_connection():
    global _conn
    try:
        if _conn is None or _conn.closed:
            raise Exception("need new")
        with _conn.cursor() as cur:
            cur.execute("SELECT 1")
    except Exception:
        try:
            if _conn:
                _conn.close()
        except Exception:
            pass
        _conn = psycopg2.connect(**DB_CONFIG)
        _conn.autocommit = True
    return _conn


# ─── DDL ─────────────────────────────────────────────────────────────────────
DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS instruments (
        id    BIGSERIAL PRIMARY KEY,
        name  TEXT NOT NULL,
        isin  TEXT NOT NULL,
        board TEXT NOT NULL
    );
    """,
    # уникальность по ISIN — чтобы не добавить дважды
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_instruments_isin
    ON instruments (isin);
    """,
]


# ─── Инициализация ───────────────────────────────────────────────────────────
def _create_db_if_missing():
    conn = psycopg2.connect(**ADMIN)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s", (DBNAME,)
            )
            if cur.fetchone():
                print(f"✓ БД {DBNAME} уже существует")
                return
            cur.execute(
                sql.SQL(
                    "CREATE DATABASE {} WITH OWNER {} ENCODING 'UTF8'"
                ).format(
                    sql.Identifier(DBNAME),
                    sql.Identifier(ADMIN["user"])
                )
            )
            print(f"✅ Создана БД {DBNAME}")
    finally:
        conn.close()


def _init_schema():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn:
            with conn.cursor() as cur:
                for ddl in DDL_STATEMENTS:
                    cur.execute(ddl)
        print("✅ Схема instruments создана/актуализирована.")
    finally:
        conn.close()


def init_db():
    """Вызывать один раз при старте приложения."""
    _create_db_if_missing()
    _init_schema()


# ─── CRUD ─────────────────────────────────────────────────────────────────────
def insert_instrument(name: str, isin: str, board: str) -> bool:
    """
    Добавляет инструмент. Возвращает True при успехе,
    False если ISIN уже существует.
    """
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


def fetch_all_instruments() -> list[dict]:
    """Возвращает все инструменты, отсортированные по имени."""
    con = get_connection()
    with con.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            'SELECT name, isin, board FROM instruments ORDER BY name'
        )
        return cur.fetchall()


def delete_instrument(isin: str) -> None:
    """Удаляет инструмент по ISIN."""
    con = get_connection()
    with con.cursor() as cur:
        cur.execute("DELETE FROM instruments WHERE isin = %s", (isin,))