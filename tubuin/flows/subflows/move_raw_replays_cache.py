# move_raw_replays_cache.py
from prefect import flow, task, get_run_logger
import os
from config import db_conn
from sql.commands import MoveRawReplays

@flow(name="Move Raw Replays Cache Flow")
def move_raw_replays_flow() -> int:
    logger = get_run_logger()
    affected_rows = 0
    try:
        logger.info("Move Raw Replays: Connecting to PostgreSQL...")
        with db_conn() as conn:
            with conn.cursor() as cur:

                cur.execute(MoveRawReplays().query)
                conn.commit()
                affected_rows = cur.rowcount
                logger.info(f"\t Rows Inserted cur.rowcount {cur.rowcount}")
                logger.info("Move Raw Replays: Executed successfully.")
    except Exception as e:
        conn.rollback()
        conn.close()
        logger.error(f"Move Raw Replays: Error {e}")
        raise
    finally:
        logger.info(f"Move Raw Replays: Affected Rows: {affected_rows}")
        return affected_rows


if __name__ == "__main__":
    count = move_raw_replays_flow()
    print(f"Moved {count} rows")
