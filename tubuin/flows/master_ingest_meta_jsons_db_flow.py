# tubuin\flows\master_ingest_meta_jsons_db_flow.py
from prefect import flow, task, get_run_logger
from logic.replay_meta_jsons_ingest import process_meta_jsons_folder
from config import db_conn
from pathlib import Path


@flow(name="Master: Ingest meta jsons to db")
def ingest_meta_json_to_db_flow():
    logger = get_run_logger()
    logger.info("Starting Master: Ingest meta jsons to db")

    with db_conn() as conn:
        with conn.cursor() as cursor:
            try:
                process_meta_jsons_folder(
                    cursor,
                    metas_dir=Path("V:/Github/BAR-ReplayDownloader/metas"),
                    processed_dir=Path("V:/Github/BAR-ReplayDownloader/metas/processed"),
                    naughty_dir=Path("V:/Github/BAR-ReplayDownloader/metas/naughty"),
                )
            except Exception as e:
                print("An error occurred: %s", str(e))

if __name__ == "__main__":
    ingest_meta_json_to_db_flow()
