from sql_command import SQLCommand
from config.db_conn import db_conn


class SQLRunner:
    def __init__ (self, db_conn_factory = db_conn):
        self._conn_factory = db_conn_factory
    
    def run(self, command: SQLCommand):
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(command.query, command.params)
                    if command.commit:
                        conn.commit()
                        return cur.rowcount
                except Exception as e:
                    conn.rollback()
                    conn.close()
                    return 0
        return 0