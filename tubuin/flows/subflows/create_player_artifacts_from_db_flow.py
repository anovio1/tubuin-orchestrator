# tubuin\flows\subflows\create_player_artifacts_from_db_flow.py
from typing import Any, Dict, Generator, List
import orjson
import msgpack
from datetime import date, timedelta, datetime, timezone
import os
from prefect import flow, task, get_run_logger
from config import db_conn

# --- Config ---
MPK_FILENAME = "players.mpk"
INDEX_FILENAME = "players.index.json"
OUTPUT_DIR = "./output_artifacts"


# --- Dense Skill History ---
def format_dense_skill_history_in_python(
    skill_history_raw_map: Dict[str, List[Dict[str, Any]]],
    overall_from_date: date,
    overall_to_date: date,
) -> Dict[str, List[Any]]:
    """
    Processes a raw, sparse skill history map into a dense grid format
    with forward-filled values.
    """
    if not skill_history_raw_map:
        return {"labels": [], "datasets": []}

    # 1. Generate global date labels for the entire period
    all_processing_dates_str = []
    current_scan_date = overall_from_date
    while current_scan_date <= overall_to_date:
        all_processing_dates_str.append(current_scan_date.strftime("%Y-%m-%d"))
        current_scan_date += timedelta(days=1)

    if not all_processing_dates_str:
        return {"labels": [], "datasets": []}

    # 2. Process each category's sparse data into a dense dataset
    final_datasets = []
    for mode, daily_entries in sorted(skill_history_raw_map.items()):
        if not daily_entries:
            continue

        # Create a lookup map for this mode's data points for efficiency
        mode_skill_on_date = {entry["date"]: entry["skill"] for entry in daily_entries}

        dataset_data = []
        last_known_skill = 0.0
        initial_skill_found = False

        # 3. Iterate through the global timeline and fill in the data
        for date_str in all_processing_dates_str:
            if date_str in mode_skill_on_date:
                last_known_skill = mode_skill_on_date[date_str]
                initial_skill_found = True
                dataset_data.append(last_known_skill)
            else:
                # Carry forward the last known value, but only after the first
                # actual data point has been found.
                dataset_data.append(last_known_skill if initial_skill_found else 0.0)

        final_datasets.append({"label": mode, "data": dataset_data})

    return {"labels": all_processing_dates_str, "datasets": final_datasets}


@task(name="Write Artifacts (Streaming)", retries=1)
def write_artifacts_task(from_date_obj: date, to_date_obj: date):
    logger = get_run_logger()
    logger.info(f"> Write Artifacts (Streaming).")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    jsonl_path = os.path.join(OUTPUT_DIR, MPK_FILENAME)
    index_path = os.path.join(OUTPUT_DIR, INDEX_FILENAME)

    query = """
        SELECT user_id, json_data
        FROM analytics.player_page_json_v4
        WHERE data_start_date = %s AND data_end_date = %s;
    """

    index_data = {}
    current_offset = 0
    processed_users_count = 0

    logger.info("Starting artifact writing process...")
    try:
        with db_conn() as conn:
            with conn.cursor(name="player_json_stream") as cur:
                cur.itersize = 1000
                logger.info(
                    f"Executing query to fetch data for window: {from_date_obj} to {to_date_obj}"
                )
                cur.execute(query, (from_date_obj, to_date_obj))

                with open(jsonl_path, "wb") as f_mpk:
                    for row in cur:
                        user_id = row[0]
                        player_json_dict = row[1]

                        if not player_json_dict:
                            logger.warning(
                                f"Skipping user {user_id} due to missing JSON data."
                            )
                            continue

                        try:
                            # skill_history_raw = player_json_dict.pop(
                            #     "skillHistoryRaw", {}
                            # )
                            # final_skill_history = format_dense_skill_history_in_python(
                            #     skill_history_raw, from_date_obj, to_date_obj
                            # )
                            # player_json_dict["skillHistory"] = final_skill_history
                            
                            packed_bytes = msgpack.packb(player_json_dict)
                            assert(isinstance(packed_bytes, bytes))

                            line_length = len(packed_bytes)
                            f_mpk.write(packed_bytes)
                            

                            # json_line_bytes = orjson.dumps(player_json_dict) + b"\n"
                            # line_length = len(json_line_bytes)
                            # f_jsonl.write(json_line_bytes)

                            index_data[str(user_id)] = [current_offset, line_length]
                            current_offset += line_length
                            processed_users_count += 1

                        except Exception as e:
                            logger.error(
                                f"❌ Failed to serialize/write for user {user_id}: {e}"
                            )

        with open(index_path, "wb") as f_index:
            f_index.write(orjson.dumps(index_data))

        logger.info(f"✅ Wrote {jsonl_path} with {processed_users_count} entries.")
        logger.info(f"✅ Wrote index to {index_path} for {len(index_data)} users.")
        return jsonl_path, index_path
    except Exception as e:
        logger.error(f"Database streaming failed: {e}")
        raise


@flow(name="Create Player Artifacts from Analytics DB")
def create_files_from_analytics_json_flow(
    from_date: str = (
        datetime.now(timezone.utc).date() - timedelta(weeks=24)
    ).isoformat(),
    to_date: str = (datetime.now(timezone.utc).date()).isoformat(),
):
    logger = get_run_logger()
    logger.info(f"Artifacts Flow: Starting for period {from_date} to {to_date}")

    from_date_obj = date.fromisoformat(from_date)
    to_date_obj = date.fromisoformat(to_date)

    # Process the data in chunks to avoid loading everything into memory
    # The query must look for an EXACT match on the date window
    jsonl_path, index_path = write_artifacts_task.submit(
        from_date_obj=from_date_obj,
        to_date_obj=to_date_obj
    ).result()

    logger.info(f"✅ Artifacts Flow: Finished successfully.")
    return jsonl_path, index_path


if __name__ == "__main__":
    create_files_from_analytics_json_flow(
        from_date=(
            datetime.now(timezone.utc).date() - timedelta(weeks=24)
        ).isoformat(),  # ~ 6 months
        to_date=(datetime.now(timezone.utc).date()).isoformat(),
    )
