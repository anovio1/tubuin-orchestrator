# tubuin\flows\subflows\replay_meta_ingress_flow.py
from prefect import flow, task, get_run_logger
from flows.subflows.move_raw_replays_cache import move_raw_replays_flow
from flows.subflows.update_derived_match_players_unlogged import update_derived_match_players_flow
from flows.subflows.update_derived_replays import update_derived_replays_flow

@task(name="Move Raw.Replays_Cache", retries=2, retry_delay_seconds=10)
def move_raw_replays_cache_task():
    # move metas from raw.replays_cache into raw.replays after they are 24 hours old
    move_raw_replays_flow()


@task(name="Update Derived.Match_Players_Unlogged", retries=1, retry_delay_seconds=10)
def update_derived_match_players_task():
    # update raw.match_players_unlogged
    update_derived_match_players_flow()



@task(name="Update Derived.Replays", retries=1, retry_delay_seconds=10)
def update_derived_replays_task():
    # update raw.match_players_unlogged
    update_derived_replays_flow()



@flow(name="Replay Meta Ingress Flow")
def replay_meta_ingress_flow():
    logger = get_run_logger()
    logger.info("🟢 Starting Replay Meta Ingress Flow")

    # 1) Move raw.replay_cache older than 24h into raw.replays
    logger.info("▶️ Step 1 move_raw_replays_cache")
    raw_cache_move = move_raw_replays_cache_task.submit()
    raw_cache_move.result()

    # 2) Update derived.match_players_unlogged
    logger.info("▶️ Step 2 update_derived_match_players_flow")
    match_players_update = update_derived_match_players_task.submit(wait_for=[raw_cache_move])
    match_players_update.result()
    
    # 3) Update derived.replays
    logger.info("▶️ Step 3 update_derived_replays task")
    replays_update = update_derived_replays_task.submit(wait_for=[match_players_update])
    replays_update.result()


    logger.info("✅ All steps complete")


if __name__ == "__main__":
    replay_meta_ingress_flow()