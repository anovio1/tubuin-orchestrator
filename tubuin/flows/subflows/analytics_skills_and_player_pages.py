# tubuin\flows\subflows\analytics_skills_and_player_pages.py
from prefect import flow, task, get_run_logger
from datetime import timedelta, datetime, timezone
from config import db_conn
import json
from pathlib import Path

from sql.commands import (
    UpdateAnalyticsSkillSnapshots,
    RefreshMaterialSkillDeltas,
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
    # This makes the task resilient to a cold start.
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
    # (by looking at new snapshots) and performs a targeted INSERT ... ON CONFLICT DO UPDATE
    # for just those users. This is far more scalable
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


@task
def write_json_output(output_path="output/skill_deltas.json"):
    return
    query = "SELECT * FROM derived.match_skill_deltas;"
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            columns = [desc[0] for desc in cur.description]
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(rows, f, indent=2)
    return f"JSON written to {output_path}"


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

    write_json_output_future = write_json_output(wait_for=[mat_skill_deltas_future])
    write_json_output_future.result()

    logger.info("✅ Main Player Data Pipeline finished.")


if __name__ == "__main__":
    main_flow()
