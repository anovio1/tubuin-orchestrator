# update_derived_replays.py
from prefect import flow, task, get_run_logger
import os
from config import db_conn
from datetime import datetime, timedelta, timezone
from sql.commands import UpdateDerivedReplays


@task(retries=3, retry_delay_seconds=10)
def update_derived_replays(t_from: datetime):
    logger = get_run_logger()
    try:
        logger.info("Update Derived Replays: Connecting to PostgreSQL...")
        with db_conn() as conn:
            with conn.cursor() as cur:
                query = UpdateDerivedReplays().query
                params = {
                    "timestamptz_from": t_from,
                }
                logger.info(f"\t Update Derived Replays:  from: {t_from}")
                cur.execute(query, params)
                conn.commit()
                logger.info(f"\t Update Derived Replays: Rows Inserted cur.rowcount {cur.rowcount}")
                logger.info("Update Derived Replays: Executed successfully.")
    except Exception as e:
        conn.rollback()
        conn.close()
        logger.error(f"Update Derived Replays: Error {e}")
        raise


@flow(name="Update Derived Replays Flow")
def update_derived_replays_flow():
    now_utc = datetime.now(timezone.utc)
    from_date = now_utc - timedelta(days=7)       # 7 days ago
    update_derived_replays(from_date)


if __name__ == "__main__":
    update_derived_replays_flow()
