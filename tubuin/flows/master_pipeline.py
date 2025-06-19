# File: tubuin/flows/master_pipeline.py

from prefect import flow, get_run_logger, task

from .subflows.replay_meta_ingress_flow import replay_meta_ingress_flow as ingress_flow
from .subflows.analytics_skills_and_player_pages import main_flow as analytics_flow

@task
def ingress_flow_wrapper():
    return ingress_flow()

@task
def analytics_flow_wrapper():
    return analytics_flow()

@flow(name="Master Data Pipeline")
def master_data_pipeline():
    """
    This flow ensures that the raw data is ingested and prepared
    before the analytics and player page artifacts are built.
    """
    logger = get_run_logger()
    logger.info("> Starting Master Data Pipeline ")

    # --- Step 1: Run the Ingress Flow ---
    # Prepare derived tables
    logger.info("> Kicking off: Replay Meta Ingress Flow...")
    ingress_flow_wrapper_future = ingress_flow_wrapper.submit()
    ingress_flow_wrapper_future.result()

    
    # --- Step 2: Analytics ---
    logger.info("> Kicking off: Analytics and Player Pages Flow...")
    analytics_flow_wrapper(wait_for=[ingress_flow_wrapper_future])

    # We can wait for the final step to complete before marking the master flow as finished.

    logger.info("✅✅✅ Master Data Pipeline Finished Successfully ✅✅✅")


if __name__ == "__main__":
    master_data_pipeline()