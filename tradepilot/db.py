import duckdb
from tradepilot.config import DB_PATH

DB_PATH.parent.mkdir(parents=True, exist_ok=True)

_conn = None


def get_conn() -> duckdb.DuckDBPyConnection:
    global _conn
    if _conn is None:
        _conn = duckdb.connect(str(DB_PATH))
        _init_tables(_conn)
    return _conn


def _init_tables(conn: duckdb.DuckDBPyConnection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_daily (
            stock_code VARCHAR, date DATE,
            open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
            volume BIGINT, amount DOUBLE, turnover DOUBLE,
            PRIMARY KEY (stock_code, date)
        );
        CREATE TABLE IF NOT EXISTS index_daily (
            index_code VARCHAR, date DATE,
            open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
            volume BIGINT, amount DOUBLE,
            PRIMARY KEY (index_code, date)
        );
        CREATE TABLE IF NOT EXISTS etf_flow (
            etf_code VARCHAR, date DATE,
            net_inflow DOUBLE, volume BIGINT,
            PRIMARY KEY (etf_code, date)
        );
        CREATE TABLE IF NOT EXISTS margin_data (
            date DATE, stock_code VARCHAR,
            margin_balance DOUBLE, margin_buy DOUBLE,
            PRIMARY KEY (date, stock_code)
        );
        CREATE TABLE IF NOT EXISTS northbound_flow (
            date DATE,
            net_buy DOUBLE, buy_amount DOUBLE, sell_amount DOUBLE,
            PRIMARY KEY (date)
        );
        CREATE TABLE IF NOT EXISTS stock_valuation (
            stock_code VARCHAR, date DATE,
            pe_ttm DOUBLE, pb DOUBLE, ps DOUBLE, market_cap DOUBLE,
            PRIMARY KEY (stock_code, date)
        );
        CREATE TABLE IF NOT EXISTS sector_data (
            sector VARCHAR, date DATE,
            avg_pe DOUBLE, avg_pb DOUBLE,
            change_1d DOUBLE, change_5d DOUBLE, change_20d DOUBLE, change_60d DOUBLE,
            PRIMARY KEY (sector, date)
        );
        CREATE TABLE IF NOT EXISTS portfolio (
            id INTEGER PRIMARY KEY,
            stock_code VARCHAR, stock_name VARCHAR,
            buy_date DATE, buy_price DOUBLE, quantity INTEGER,
            status VARCHAR DEFAULT 'open'
        );
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY,
            date DATE, stock_code VARCHAR, stock_name VARCHAR,
            direction VARCHAR, price DOUBLE, quantity INTEGER,
            reason VARCHAR
        );
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY,
            date DATE, stock_code VARCHAR,
            signal_type VARCHAR, signal_name VARCHAR,
            direction VARCHAR, strength INTEGER,
            description VARCHAR
        );
    """)
