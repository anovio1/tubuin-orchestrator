# tubuin\flows\subflows\update_derived_match_players_unlogged.py
from prefect import flow, task, get_run_logger
import os
from config import db_conn
from datetime import datetime, timedelta, timezone
from sql.commands import UpdateDerivedMatchPlayers


@task(retries=3, retry_delay_seconds=10)
def update_derived_match_players_unlogged(t_from: datetime, t_to: datetime):
    logger = get_run_logger()
    conn = None  # Initialize conn to None
    try:
        logger.info("Update Derived Match Players Unlogged: Connecting to PostgreSQL...")
        with db_conn() as conn:  # Attempt to get the connection object
        # At this point, if db_conn() failed, conn is still None and we'd jump to except/finally
        # If db_conn() succeeded, conn is a connection object.
            with conn.cursor() as cur: # Use cursor as a context manager
                try:
                    query = UpdateDerivedMatchPlayers().query
                    params = {
                        "timestamptz_from": t_from,
                        "timestamptz_to": t_to,
                    }
                    logger.info(f"\t Update Derived Match Players Unlogged:  from: {t_from} - {t_to}")
                    cur.execute(query, params)
                    print(query)
                    conn.commit() # Commit if all successful
                    logger.info(f"\t Update Derived Match Players Unlogged: Rows Inserted cur.rowcount {cur.rowcount}")
                    logger.info("Update Derived Match Players Unlogged: Executed successfully.")
                except Exception as e:
                    logger.error(f"Update Derived Match Players Unlogged: Error {e}")
                    conn.rollback()
                finally:
                    conn.close()
                    
    except Exception as e:
        logger.error(f"Update Derived Match Players Unlogged: Error {e}")
        if conn: # Only try to rollback if conn was successfully assigned
            try:
                conn.rollback()
                logger.info("Transaction rolled back.")
            except Exception as rb_e:
                logger.error(f"Error during rollback: {rb_e}")
        raise # Re-raise the original error for Prefect to handle

    finally:
        if conn: # Only try to close if conn was successfully assigned
            try:
                conn.close()
                logger.info("Database connection closed.")
            except Exception as cl_e:
                logger.error(f"Error during connection close: {cl_e}")


@flow(name="Update Derived Match Players Unlogged Flow")
def update_derived_match_players_flow():
    now_utc = datetime.now(timezone.utc)
    from_date = now_utc - timedelta(days=7)       # 7 days ago
    to_date   = now_utc + timedelta(days=1)       # tomorrow
    update_derived_match_players_unlogged(from_date, to_date)


if __name__ == "__main__":
    update_derived_match_players_flow()
