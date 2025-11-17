"""
Copy candles_15min, candles_5min and daily_bars
from a SQLite DB into a PostgreSQL DB.

Requirements:
    pip install psycopg2-binary
"""

import sqlite3
import psycopg2
from psycopg2.extras import execute_batch


SQLITE_DB_PATH = r"e:/database/market_data.db"   # <-- change this
PG_DSN = "dbname=market user=postgres password=admin host=localhost"  # <-- change this


# --- Postgres DDL ----------------------------------------------------------

CREATE_CANDLES_15MIN = """
CREATE TABLE IF NOT EXISTS candles_15min (
    id          INTEGER PRIMARY KEY,
    symbol      TEXT NOT NULL,
    timestamp   TIMESTAMP NOT NULL,
    open        DOUBLE PRECISION NOT NULL,
    high        DOUBLE PRECISION NOT NULL,
    low         DOUBLE PRECISION NOT NULL,
    close       DOUBLE PRECISION NOT NULL,
    volume      INTEGER NOT NULL,
    vwap        DOUBLE PRECISION,
    ema_8       DOUBLE PRECISION,
    ema_20      DOUBLE PRECISION,
    ema_39      DOUBLE PRECISION,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_15min_symbol_timestamp
    ON candles_15min(symbol, timestamp);
"""

CREATE_CANDLES_5MIN = """
CREATE TABLE IF NOT EXISTS candles_5min (
    id          INTEGER PRIMARY KEY,
    symbol      TEXT NOT NULL,
    timestamp   TIMESTAMP NOT NULL,
    open        DOUBLE PRECISION NOT NULL,
    high        DOUBLE PRECISION NOT NULL,
    low         DOUBLE PRECISION NOT NULL,
    close       DOUBLE PRECISION NOT NULL,
    volume      INTEGER NOT NULL,
    vwap        DOUBLE PRECISION,
    ema_8       DOUBLE PRECISION,
    ema_20      DOUBLE PRECISION,
    ema_39      DOUBLE PRECISION,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_5min_symbol_timestamp
    ON candles_5min(symbol, timestamp);
"""

CREATE_DAILY_BARS = """
CREATE TABLE IF NOT EXISTS daily_bars (
    id           INTEGER PRIMARY KEY,
    symbol       TEXT NOT NULL,
    timestamp    DATE NOT NULL,
    open         DOUBLE PRECISION NOT NULL,
    high         DOUBLE PRECISION NOT NULL,
    low          DOUBLE PRECISION NOT NULL,
    close        DOUBLE PRECISION NOT NULL,
    volume       INTEGER NOT NULL,
    vwap         DOUBLE PRECISION,
    vwap_upper   DOUBLE PRECISION,
    vwap_lower   DOUBLE PRECISION,
    session_high DOUBLE PRECISION,
    session_low  DOUBLE PRECISION,
    poc          DOUBLE PRECISION,
    atr          DOUBLE PRECISION,
    rsi_10       DOUBLE PRECISION,
    momentum_10  DOUBLE PRECISION,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_daily_symbol_timestamp
    ON daily_bars(symbol, timestamp);
"""


def create_pg_schema(pg_conn):
    """Create tables + indexes in Postgres if they don't exist."""
    with pg_conn.cursor() as cur:
        cur.execute(CREATE_CANDLES_15MIN)
        cur.execute(CREATE_CANDLES_5MIN)
        cur.execute(CREATE_DAILY_BARS)
    pg_conn.commit()


# --- Generic copy helper ---------------------------------------------------

def copy_table(sqlite_conn, pg_conn, table_name, columns, batch_size=1000):
    """
    Copy one table from SQLite to Postgres.

    :param sqlite_conn: sqlite3.Connection
    :param pg_conn: psycopg2 connection
    :param table_name: table name (same in both DBs)
    :param columns: list of column names (same in both DBs)
    """
    col_list = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))

    sqlite_cur = sqlite_conn.cursor()
    sqlite_cur.execute(f"SELECT {col_list} FROM {table_name}")

    pg_cur = pg_conn.cursor()

    while True:
        rows = sqlite_cur.fetchmany(batch_size)
        if not rows:
            break
        execute_batch(
            pg_cur,
            f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})",
            rows,
            page_size=batch_size,
        )
        pg_conn.commit()

    sqlite_cur.close()
    pg_cur.close()


def main():
    # 1. Connect to both databases
    sqlite_conn = sqlite3.connect(SQLITE_DB_PATH)
    pg_conn = psycopg2.connect(PG_DSN)

    try:
        # 2. Ensure Postgres schema exists
        create_pg_schema(pg_conn)

        # 3. Define columns for each table
        candles_cols = [
            "id", "symbol", "timestamp", "open", "high", "low", "close",
            "volume", "vwap", "ema_8", "ema_20", "ema_39", "created_at",
        ]

        daily_cols = [
            "id", "symbol", "timestamp", "open", "high", "low", "close",
            "volume", "vwap", "vwap_upper", "vwap_lower",
            "session_high", "session_low", "poc", "atr",
            "rsi_10", "momentum_10", "created_at",
        ]

        # 4. Copy each table
        print("Copying candles_15min ...")
        copy_table(sqlite_conn, pg_conn, "candles_15min", candles_cols)

        print("Copying candles_5min ...")
        copy_table(sqlite_conn, pg_conn, "candles_5min", candles_cols)

        print("Copying daily_bars ...")
        copy_table(sqlite_conn, pg_conn, "daily_bars", daily_cols)

        print("Done.")
    finally:
        sqlite_conn.close()
        pg_conn.close()


if __name__ == "__main__":
    main()
