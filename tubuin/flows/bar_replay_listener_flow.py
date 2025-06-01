# tubuin\flows\bar_replay_listener_flow.py
from prefect import flow, task, get_run_logger
from logic.listener_new import scrape_replays, Config, HTTPReplayDownloader, HTTPMetadataFetcher
from datetime import datetime, timedelta
import requests
from requests.adapters import HTTPAdapter
from pathlib import Path

@task
def run_scrape_replays():
    logger = get_run_logger()
    logger.info("Starting BAR replay scraping...")
    
    try:
        cfg = Config(
            download_folder=Path("V:/Github/BAR-ReplayDownloader/Replays"),
            metas_folder=Path("V:/Github/BAR-ReplayDownloader/metas"),
            from_date=(datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d"),
            to_date=(datetime.today() + timedelta(days=2)).strftime("%Y-%m-%d"),
            listen_interval=1,
            results_per_page_limit=500,
            listen_max_empty_pages=5,
            logger=logger
        )

        cfg.download_folder.mkdir(parents=True, exist_ok=True)
        cfg.metas_folder.mkdir(parents=True, exist_ok=True)


        # Prepare HTTP session
        session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=cfg.pool_connections,
            pool_maxsize=cfg.pool_maxsize,
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        downloader = HTTPReplayDownloader(cfg, session)
        fetcher = HTTPMetadataFetcher(cfg, session)

        scrape_replays(cfg, downloader, fetcher, session)  # or pass individual params if needed
        logger.info("Replay scraping completed successfully.")
    except Exception as e:
        logger.error(f"Replay scraping failed: {e}", exc_info=True)
        raise

@flow(log_prints=True, name="BAR Replay Listener Flow")
def scrape_flow():
    run_scrape_replays()

if __name__ == "__main__":
    scrape_flow()