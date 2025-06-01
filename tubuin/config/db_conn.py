# tubuin\config\db_conn.py
import psycopg
from contextlib import contextmanager
from config.db_conn_info import DB_CONN_INFO

def make_conn():
    return psycopg.connect(
        DB_CONN_INFO,
    )

@contextmanager
def db_conn():
    conn = make_conn()
    try:
        yield conn
    finally:
        conn.close()