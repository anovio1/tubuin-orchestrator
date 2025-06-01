# pgsql_replaysCacheIngest.py
# This script reads metas and pushes to pgsql, moves processed metas to a "processed" subdirectory.
# TO DO: upon completion, batches to msgpack?

import os
import argparse
from pathlib import Path

# import logging
import orjson
# from watchdog.observers import Observer
# from watchdog.events import FileSystemEventHandler
# import time

# --- Configuration ---
MIN_AGE_SEC = 5.0  # Minimum age of file in seconds before processing
MOVE_ON_SUCCESS = True  # Move processed metas to a "processed" subdirectory

# === Console Message Settings ===
DEBUG = False   # Verbose output for debugging
PRINT_MESSAGES = True
VERBOSE = False

# --- Logging setup ---
# logging.basicConfig(
#     filename="pgsql_replaysCacheIngest.log",
#     level=logging.WARNING,
#     format="%(asctime)s [%(levelname)s] %(message)s",
#     datefmt="%Y-%m-%d %H:%M:%S",
# )

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb
from psycopg.types.numeric import Float4, Int4
from datetime import datetime, timezone

# class MetaFileHandler(FileSystemEventHandler):
#     def __init__(self, cursor, processed_dir):
#         self.cursor = cursor
#         self.processed_dir = processed_dir

#     def on_created(self, event):
#         if not event.is_directory and event.src_path.endswith(".json"):
#             if file_is_old_enough(event.src_path, min_age_sec=MIN_AGE_SEC):
#                 print(f"{formatted_log_time()} Detected new file: {event.src_path}")
#                 try:
#                     process_json_file(event.src_path, self.cursor, self.processed_dir)
#                 except Exception as e:
#                     print(f"Error processing {event.src_path}: {e}")

# def watch_folder(cursor, metas_dir):
#     observer = Observer()
#     event_handler = MetaFileHandler(cursor)
#     observer.schedule(event_handler, metas_dir, recursive=False)
#     observer.start()
#     print("Watching for new meta files...")
#     try:
#         while True:
#             time.sleep(1)
#     except KeyboardInterrupt:
#         observer.stop()
#     observer.join()

def formatted_log_time():
    now = datetime.now()
    s = now.strftime("%I:%M:%S %p")  # e.g. "03:27:45 PM"
    return s.lstrip("0")         # "3:07:05 PM"


def build_query(table, columns, conflict_column=None):
    """
    Returns a psycopg2.sql.Composed INSERT ... query with optional ON CONFLICT DO NOTHING.
    """
    if DEBUG: print(f"    => build_query table: {table} columns: {columns} conflict_column: {conflict_column}")
    
    table_ident = sql.Identifier(*table.split("."))
    col_idents  = [sql.Identifier(c) for c in columns]
    conflict_clause = sql.SQL("ON CONFLICT ({}) DO NOTHING").format(sql.Identifier(conflict_column)) if conflict_column else sql.SQL("")

    query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) {}").format(
        table_ident,
        sql.SQL(", ").join(col_idents),
        sql.SQL(", ").join(sql.Placeholder() * len(col_idents)),
        conflict_clause,
    )

    return query


def transform_data_for_replay_cache_query(meta):
    replay_id = meta.get("id")
    if DEBUG: print(f"    => transform_data_for_replay_cache_query meta: {replay_id}")
    start_time = datetime.fromisoformat(meta["startTime"].replace("Z", "+00:00"))
    raw_jsonb = Jsonb(meta)

    return (replay_id, start_time, raw_jsonb)


def push_to_replay_cache(meta, cursor):
    replay_id = meta.get('id', 'unknown')
    status = ""
    rowsInserted = 0
    rowsSkipped = 0

    if DEBUG or PRINT_MESSAGES:
        base = f"    => push_to_replay_cache replay_id: {replay_id}"

    transformed_data = transform_data_for_replay_cache_query(meta)
    query = build_query("raw.replays_cache", ["replay_id", "start_time", "raw_jsonb"], "replay_id")

    try:
        cursor.execute(query, transformed_data)
        if cursor.rowcount == 0:    # rows affected the INSERT hit the ON CONFLICT and did nothing
            status = "already exists, skipped"
            rowsSkipped += 1

            if DEBUG:
                print(f"{base} {status}")
            # logging.info(f"replay_id {replay_id} already exists, skipped")
        else: 
            status = f"{cursor.rowcount} row(s) inserted"
            rowsInserted += cursor.rowcount

            if DEBUG:
                print(f"{base} {status}")
            # logging.info(f"Inserted replay_id {replay_id}")
        cursor.connection.commit()

    except psycopg.Error as e:
        status = f"error occurred, rolling back Error: {e}"

        if DEBUG:
            print(f"{base} {status}")

        cursor.connection.rollback()
        # logging.error(
        #     "Failed to insert meta %s: %s", meta.get("id"), str(e)
        # )
        return (False, rowsInserted, rowsSkipped, status)
    
    except Exception as e:
        return (False, rowsInserted, rowsSkipped, status)

    return (True, rowsInserted, rowsSkipped, status)

def file_is_old_enough(path: str, min_age_sec: float = 2.0) -> bool:
    """
    Return True if the file’s last-modified time is at least min_age_sec seconds ago.
    """
    mtime = os.path.getmtime(path)
    # Convert to a datetime
    modified_dt = datetime.fromtimestamp(mtime)
    age = datetime.now() - modified_dt
    return age.total_seconds() >= min_age_sec

# Static
def get_meta_files(directory):
    return [
        f
        for f in os.listdir(directory)
        if f.endswith(".json")
    ]

def process_json_file(filepath, cursor, processed_dir, naughty_dir):
    if not filepath.endswith(".json"):
        return False

    if(DEBUG or (PRINT_MESSAGES and VERBOSE)): base = f"{formatted_log_time()} Processing file: {os.path.basename(filepath)}"

    if not file_is_old_enough(filepath, min_age_sec=MIN_AGE_SEC):
        if(DEBUG or (PRINT_MESSAGES and VERBOSE)):
            status = f"  ...age < {MIN_AGE_SEC}, skipped"#.ljust(28)
            print(f"{base} {status}")
        return False
    else:
        if(DEBUG or (PRINT_MESSAGES and VERBOSE)): 
            status = f"  ...pushing"#.ljust(28)

    with open(filepath, "rb") as f:
            try:
                meta = orjson.loads(f.read())
            except orjson.JSONDecodeError as e:
                if(DEBUG or (PRINT_MESSAGES and VERBOSE)): print(f"Error decoding JSON from {os.path.basename(filepath)}: {e}")
                return False
            is_success, rowsInserted, rowsSkipped, push_status = push_to_replay_cache(meta, cursor)
            
    if (DEBUG or (PRINT_MESSAGES and VERBOSE)): print(f"{base} {status} ...{push_status}")

    if (MOVE_ON_SUCCESS and is_success):
        # move meta file to processed directory
        try:
            filename = os.path.basename(filepath)
            os.rename(
                os.path.join(filepath),
                os.path.join(processed_dir, os.path.basename(filename))
            )
        except FileExistsError:
            print(f"{filename} already in processed directory, moving to naughty.")
            try:
                os.rename(
                    os.path.join(filepath),
                    os.path.join(naughty_dir, os.path.basename(filename))
                )
            except Exception as e:
                print(f"meta_jsons {filename} naughty_move error: {e}")
        except Exception as e:
            print(f"meta_jsons {filename} move error: {e}")

    return (rowsInserted, rowsSkipped)

def process_meta_jsons_folder(cursor, metas_dir: Path, processed_dir: Path, naughty_dir: Path): #, startListener: bool):
    print("Meta files in:", metas_dir)
    metas_dir.mkdir(exist_ok=True)
    print("Processed files will go to:", processed_dir)
    processed_dir.mkdir(exist_ok=True)

    filenames = get_meta_files(metas_dir)
    if DEBUG: print(f"filename count: {len(filenames)}")

    totalRowsInserted = 0
    totalRowsSkipped = 0
    for filename in filenames:
        full_path = os.path.join(metas_dir, filename)
        rowsInserted, rowsSkipped = process_json_file(full_path, cursor, processed_dir, naughty_dir)
        totalRowsInserted += rowsInserted
        totalRowsSkipped += rowsSkipped

    print(f"{formatted_log_time()} Total: {len(filenames)} Inserted: {totalRowsInserted} Skipped: {totalRowsSkipped} MOVE_ON_SUCCESS: {MOVE_ON_SUCCESS}")
    # # Then watch for new files
    # if startListener:
    #     watch_folder(cursor)


if __name__ == "__main__":
    from config import db_conn
    parser = argparse.ArgumentParser(description="Ingest meta files.")
    # parser.add_argument(
    #     "--t", "--trigger", 
    #     dest="start_listener",
    #     action="store_true",
    #     help="Start the folder listener after processing existing files"
    # )
    parser.add_argument(
        "--metas-dir", type=Path, default=Path("V:/Github/BAR-ReplayDownloader/metas"),
        dest="metas_dir",
        help="Directory where raw meta JSON files are stored"
    )
    parser.add_argument(
        "--processed-dir", type=Path, default=Path("V:/Github/BAR-ReplayDownloader/metas/processed"),
        dest="processed_dir",
        help="Where to move processed files. Defaults to <metas>/processed"
    )
    parser.add_argument(
        "--naughty-dir", type=Path, default=Path("V:/Github/BAR-ReplayDownloader/metas/naughty"),
        dest="naughty_dir",
        help="Move duplicate metas files. Defaults to <metas>/naughty"
    )

    args = parser.parse_args()
    with db_conn() as conn:
        with conn.cursor() as cursor:
            try:
                process_meta_jsons_folder(cursor, metas_dir=Path(args.metas_dir), processed_dir=Path(args.processed_dir), naughty_dir = Path(args.naughty_dir)) #, startListener=args.start_listener)
            except Exception as e:
                print("An error occurred: %s", str(e))
            except KeyboardInterrupt:
                print("\n=> Interrupted by user. Shutting down...")
            finally:
                cursor.close()
                conn.close()
                print("meta file Ingest exited")
