# tubuin\flows\subflows\analytics_skills_and_player_pages.py
from prefect import flow, task, get_run_logger
from datetime import timedelta, datetime, timezone
from config import db_conn, sftp_ssh_conn
import json
from pathlib import Path

from sql.commands import (
    UpdateAnalyticsSkillSnapshots,
    RefreshMaterialSkillDeltas,
    UpdateAnalyticsPlayerPageJson,
)
from logic.utils.sftp import upload_gzipped_and_decompress_remotely

from .create_player_artifacts_from_db_flow import (
    create_files_from_analytics_json_flow,
)


@task
def update_analytics_match_skill_snapshots() -> int:
    # IMPROVEMENT: the task itself should be smarter. It should fetch the watermark first.
    # If the watermark is NULL, it should pass a default "beginning of time" parameter (1970-01-01)
    # to the SQL script. The SQL script itself should be parameterized to accept this start date.
    logger = get_run_logger()
    affected_rows = 0

    try:
        logger.info("🔄 Updating analytics.match_skill_snapshots...")
        with db_conn() as conn:
            with conn.cursor() as cur:
                query = UpdateAnalyticsSkillSnapshots().query

                cur.execute(query)
                conn.commit()

                affected_rows = cur.rowcount
                logger.info(
                    f"✅ Inserted {cur.rowcount} rows into match_skill_snapshots."
                )
    except Exception as e:
        logger.error(f"❌ Error during match_skill_snapshots update: {e}")
        conn.rollback()
        conn.close()
        raise
    finally:
        logger.info(f"ℹ️ Total rows affected: {affected_rows}")
        return affected_rows


@task
def update_match_skill_deltas():
    # IMPROVEMENT: The "materialized view" should not be a MATERIALIZED VIEW object in Postgres.
    # It should be a regular table (derived.match_skill_deltas) that is updated incrementally.
    # The task should execute a script that finds only the users whose skills have changed
    logger = get_run_logger()

    try:
        logger.info("🔄 Refreshing derived.match_skill_deltas...")
        with db_conn() as conn:
            with conn.cursor() as cur:
                query = RefreshMaterialSkillDeltas().query

                cur.execute(query)
                conn.commit()

                logger.info(f"✅ Refreshed match_skill_deltas.")
    except Exception as e:
        logger.error(f"❌ Error during match_skill_deltas refresh: {e}")
        conn.rollback()
        raise
    finally:
        logger.info(f"ℹ️ Total rows affected:")


# --- Populate Postgres Table analytics.player_page_json ---
@task(name="Populate analytics.player_page_json", retries=1, retry_delay_seconds=60)
def populate_analytics_player_json_task(from_date: str, to_date: str):
    logger = get_run_logger()
    conn = None  # Initialize conn to None
    try:
        logger.info("Update Analytics Player Page Json: Connecting to PostgreSQL...")
        with db_conn() as conn:  # Attempt to get the connection object
            # At this point, if db_conn() failed, conn is still None and we'd jump to except/finally
            # If db_conn() succeeded, conn is a connection object.
            with conn.cursor() as cur:  # Use cursor as a context manager
                try:
                    query = UpdateAnalyticsPlayerPageJson().query
                    params = {
                        "timestamptz_from": from_date,
                        "timestamptz_to": to_date,
                    }
                    logger.info(
                        f"\t Update Analytics Player Page Json:  {from_date} - {to_date}"
                    )
                    cur.execute(query, params)
                    print(query)
                    conn.commit()  # Commit if all successful
                    logger.info(
                        f"\t Update Analytics Player Page Json: Rows Inserted cur.rowcount {cur.rowcount}"
                    )
                    logger.info(
                        "Update Analytics Player Page Json: Executed successfully."
                    )
                except Exception as e:
                    logger.error(f"Update Analytics Player Page Json: Error {e}")
                    conn.rollback()

    except Exception as e:
        logger.error(f"Update Analytics Player Page Json failed: {e}")
        raise  # Re-raise for Prefect


@task(name="Upload Artifacts (from DB JSON flow)", retries=2, retry_delay_seconds=30)
def upload_artifacts_from_db_json_task(jsonl_path: str, index_path: str):
    logger = get_run_logger()
    logger.info("Upload Artifacts (from DB JSON flow)...")
    try:
        logger.info("in Upload Artifacts try.")
        with sftp_ssh_conn() as (sftp, ssh):
            logger.info("SFTP connection established.")
            upload_gzipped_and_decompress_remotely(sftp, ssh, Path(jsonl_path), Path(f"/home/nodeuser/apps/go-api/private/{Path(jsonl_path).name}"))
            upload_gzipped_and_decompress_remotely(sftp, ssh, Path(index_path), Path(f"/home/nodeuser/apps/go-api/private/{Path(index_path).name}"))

    except Exception as e:
        logger.error(f"❌ Error during artifact upload: {e}")
        raise

    return True


# @task
# def write_json_output(output_path="output/skill_deltas.json"):
#     return
# query = "SELECT * FROM derived.match_skill_deltas;"
# with psycopg2.connect(**DB_CONFIG) as conn:
#     with conn.cursor() as cur:
#         cur.execute(query)
#         columns = [desc[0] for desc in cur.description]
#         rows = [dict(zip(columns, row)) for row in cur.fetchall()]

# Path(output_path).parent.mkdir(parents=True, exist_ok=True)
# with open(output_path, "w") as f:
#     json.dump(rows, f, indent=2)
# return f"JSON written to {output_path}"


@task
def flow_wrapper_create_files_from_analytics(from_date, to_date):
    return create_files_from_analytics_json_flow(from_date, to_date)


@flow(name="Analytics Skills And Player Pages Flow")
def main_flow():
    logger = get_run_logger()
    logger.info("🟢 Starting Skill: Deltas Refresh and Export")

    # Step 1: update analytics.match_skill_snapshots
    logger.info("▶️ Step 1: Update analytics.match_skill_snapshots")
    snapshots_future = update_analytics_match_skill_snapshots.submit()

    # Step 2: update derived.match_skill_deltas
    logger.info("▶️ Step 2: Update derived.match_skill_deltas")
    mat_skill_deltas_future = update_match_skill_deltas.submit(
        wait_for=[snapshots_future]  # This dependency is explicit and correct.
    )

    # A future improvement is to make this more flexible. We could pass in a short
    # date range (like "last 7 days") to calculate recent stats like win/loss,
    # while other parts of the query (like skill history) would ignore that and
    # always pull from their own long-term history tables.
    #
    # This would allow us to create different data "slices" on demand and would be
    # the final step before moving each major component into its own dedicated analytics table.

    # Step 3: Set window which matches front end view
    logger.info("▶️ Step 3: Setting window to 6 months for player page data")
    today = datetime.now(timezone.utc).date()
    to_date_param = today  # 1 day is added in sql to ensure complete data
    from_date_param = today - timedelta(weeks=24)  # Approx 6 months
    logger.info(f"\t Window set from {from_date_param} to {to_date_param}")

    # Step 4: populate/update analytics.player_page_json
    logger.info("▶️ Step 4: Populate/Update analytics.player_page_json")
    analytics_player_json_future = populate_analytics_player_json_task.submit(
        from_date=from_date_param.isoformat(),
        to_date=to_date_param.isoformat(),
        wait_for=[mat_skill_deltas_future],
    )

    # Step 5: create artifacts from analytics.player_page_json
    #   JSONL and Index from analytics table
    logger.info("▶️ Step 5: Create artifacts from analytics.player_page_json")
    paths_future = flow_wrapper_create_files_from_analytics.submit(
        from_date=from_date_param.isoformat(),
        to_date=to_date_param.isoformat(),
        wait_for=[analytics_player_json_future],
    )
    
    paths = paths_future.result()

    # Step 6: Upload artifacts to Production
    #   CRITICAL
    logger.info("▶️ Step 6: Upload artifacts to Production")
    upload_artifacts_future = upload_artifacts_from_db_json_task.submit(
        jsonl_path=paths[0],  # Pass the first element of the result
        index_path=paths[1],  # Pass the second element
        wait_for=[paths_future],
    )

    upload_artifacts_future.wait()

    # write_json_output_future = write_json_output(wait_for=[mat_skill_deltas_future])
    # write_json_output_future.result()

    logger.info("✅ Main Player Data Pipeline finished.")


if __name__ == "__main__":
    main_flow()
