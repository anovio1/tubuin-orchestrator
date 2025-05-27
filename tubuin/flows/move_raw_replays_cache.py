# from prefect import flow, task, get_run_logger
# import psycopg2
# from psycopg2.extras import RealDictCursor
# import os


# # Optional: Load from environment or .env
# DB_CONN = os.getenv("DB_CONN", "dbname=yourdb user=youruser password=yourpass host=localhost")

# @task(retries=3, retry_delay_seconds=10)
# def move_old_replays():
#     logger = get_run_logger()
#     try:
#         logger.info("Connecting to PostgreSQL...")
#         with psycopg2.connect(DB_CONN) as conn:
#             with conn.cursor(cursor_factory=RealDictCursor) as cur:
#                 logger.info("Executing: SELECT move_old_replays()")
#                 cur.execute("SELECT move_old_replays();")
#                 logger.info("Replay move executed successfully.")
#     except Exception as e:
#         logger.error(f"Error moving replays: {e}")
#         raise

# @flow(name="Replay Cleanup Flow")
# def run_cleanup():
#     move_old_replays()

# if __name__ == "__main__":
#     run_cleanup()