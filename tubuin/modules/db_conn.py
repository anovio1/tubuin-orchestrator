#db_conn.py
import psycopg
from contextlib import contextmanager
from config.db_conn_info import DB_CONN_INFO

# DB_CONN_INFO = (
#     f"host={os.environ['DB_HOST']} "
#     f"port={os.environ['DB_PORT']} "
#     f"dbname={os.environ['DB_NAME']} "
#     f"user={os.environ['DB_USER']} "
#     f"password={os.environ['DB_PASS']}"
# )

def make_conn():
    return psycopg.connect(
        dbname=DB_CONN_INFO.dbname,
        user=DB_CONN_INFO.user,
        password=DB_CONN_INFO.password,
        host=DB_CONN_INFO.host,
        port=DB_CONN_INFO.port,
        autocommit=False
    )

@contextmanager
def db_conn():
    conn = make_conn()
    try:
        yield conn
    finally:
        conn.close()