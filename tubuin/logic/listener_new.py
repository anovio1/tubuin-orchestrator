# listener_new.py
"""
Refactored BAR Replay Listener
- Uses Command Pattern to separate concerns (search, filter, fetch, download).
- Defines interfaces for downloading and metadata fetching via ABCs.
- Central `scrape_replays` orchestrates the workflow using injected implementations.
"""
import argparse
import itertools
import json
import logging
import re
import sys
import time
import functools
import tempfile
import shutil
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Generic, TypeVar, List, Optional, Set, Tuple

import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib.parse import quote

try:
    from prefect import get_run_logger
except ImportError:

    class SuppressConsoleInfoFilter(logging.Filter):
        def filter(self, record):
            if record.levelno == logging.INFO and "Downloaded" in record.getMessage():
                return False
            return True

    def get_run_logger():
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)

        if not logger.handlers:
            formatter = logging.Formatter(
                "%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"
            )

            file_handler = logging.FileHandler("listener_py.log", encoding="utf-8")
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            console_handler.addFilter(SuppressConsoleInfoFilter())
            logger.addHandler(console_handler)

        return logger


@dataclass
class Config:
    download_folder: Path
    metas_folder: Path
    from_date: str
    to_date: str
    listen_interval: int
    results_per_page_limit: int
    listen_max_empty_pages: int
    logger: logging.Logger | logging.LoggerAdapter
    skip_download: bool = False
    force_meta: bool = False
    downloaded_jsonl: str = "downloaded.jsonl"
    pool_connections: int = 10
    pool_maxsize: int = 20
    base_api: str = "https://api.bar-rts.com"
    base_download_url: str = (
        "https://storage.uk.cloud.ovh.net/v1/AUTH_10286efc0d334efd917d476d7183232e/BAR/demos"
    )
    sandbox: bool = False

    @property
    def listen_endpoint(self) -> str:
        return f"{self.base_api}/replays"


@dataclass
class Summary:
    label: str
    total: int
    ok: int
    fail: int
    elapsed: float


class Summarizer:
    def __init__(self, config, width: int = 100):
        self.config = config
        self.width = width

    def report(self, summary: Summary) -> None:
        rps = summary.total / summary.elapsed if summary.elapsed > 0 else 0.0
        msg = (
            f"[~] {summary.label} | "
            f"✅ {summary.ok} ❌ {summary.fail} | "
            f"RPS: {rps:.2f} | "
            f"Time: {summary.elapsed:.2f}s"
        )
        # pad to width so it overwrites old console lines cleanly
        self.config.logger.info(msg)


def cancel_futures_on_interrupt(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except KeyboardInterrupt:
            self.logger.debug("[cancel_futures_on_interrupt] Caught Ctrl+C!")
            # tear down the pool immediately
            executor = getattr(self, "executor", None)
            if executor:
                executor.shutdown(wait=False, cancel_futures=True)
            # cancel outstanding futures
            for f in getattr(self, "futures", []):
                f.cancel()
            # user feedback & exit
            self.logger.info(f"{self.__class__.__name__} interrupted by user.")
            sys.exit(1)

    return wrapper


def setup_logger(cfg) -> logging.Logger:
    logger = logging.getLogger("listener")
    logger.setLevel(logging.DEBUG)
    if not logger.handlers:
        fmt = "%(asctime)s [%(levelname)s] %(message)s"
        datefmt = "%Y-%m-%d %H:%M:%S"
        fh = logging.FileHandler(f"listener_new{cfg.sandbox}.log", encoding="utf-8")
        fh.setFormatter(logging.Formatter(fmt, datefmt))
        logger.addHandler(fh)
        ch = logging.StreamHandler()
        ch.setFormatter(logging.Formatter(fmt, datefmt))
        logger.addHandler(ch)
    return logger


class ReplayDownloader(ABC):
    @abstractmethod
    def download(self, filename: str, start_time: str) -> Tuple[str, Path, str]:
        """
        Download a replay file and return (status, folder, filename).
        Status is one of "ok", "exists", or "fail".
        """
        ...


class MetadataFetcher(ABC):
    @abstractmethod
    def fetch(self, replay_id: str) -> Tuple[Optional[str], Optional[dict]]:
        """
        Fetch metadata for a replay ID.
        Returns (filename, metadata) or (None, None) on failure.
        """
        ...


# --- Utilities ---


def countdown_sleep(seconds: int, message: str = "Waiting to check") -> None:
    ellipsis = itertools.cycle([".", "..", "..."])
    for remaining in range(seconds, 0, -1):
        dots = next(ellipsis)
        text = f"[⏳] {message}{dots:<3} next in {remaining:2d}s"
        print(text.ljust(80), end="\r", flush=True)
        time.sleep(1)
    print(" " * 80, end="\r")


def ensure_date_folder(base: Path, date_str: str) -> Path:
    folder = base / f"L{date_str}Replays"
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def is_listener_folder(name: str) -> bool:
    return bool(re.fullmatch(r"L\d{4}-\d{2}-\d{2}Replays", name))


def load_all_seen_ids(config: Config) -> Set[str]:
    """
    Scans subfolders of DOWNLOAD_FOLDER for downloaded.jsonl files,
    reads each line as JSON, and collects all gameId values into a set.
    Returns the set of seen replay IDs.
    """

    seen: Set[str] = set()

    for subdir in config.download_folder.iterdir():
        if subdir.is_dir() and is_listener_folder(subdir.name):
            jsonl_file = subdir / config.downloaded_jsonl
            if not jsonl_file.exists():
                config.logger.error(
                    f"load_all_seen_ids: jsonl_file does not exist for {subdir.name}"
                )
                continue
            for line in jsonl_file.read_text(encoding="utf-8").splitlines():
                try:
                    seen.add(json.loads(line).get("gameId"))
                except Exception as e:
                    config.logger.error(f"load_all_seen_ids: while reading line: {e}")
                    continue
    return seen


def append_to_downloaded_log(config: Config, replay_id: str, folder: Path) -> None:
    log_file = folder / config.downloaded_jsonl
    try:
        with open(log_file, "a", encoding="utf-8") as f:
            json.dump({"gameId": replay_id}, f)
            f.write("\n")
    except Exception as e:
        config.logger.error(f"Failed to write to {log_file}: {e}")


class HTTPReplayDownloader(ReplayDownloader):
    def __init__(self, config: Config, session: requests.Session):
        self.config = config
        self.session = session

    def download(self, filename: str, start_time: str) -> Tuple[str, Path, str]:
        date_only = start_time.split("T")[0]
        folder = ensure_date_folder(self.config.download_folder, date_only)
        target = folder / filename
        if target.exists():
            return "exists", folder, filename
        url = f"{self.config.base_download_url}/{quote(filename)}"
        r = None
        try:
            r = self.session.get(url, stream=True, timeout=10)
            r.raise_for_status()
            with open(target, "wb") as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
            return "ok", folder, filename
        except Exception as e:
            self.config.logger.error(
                f"Download failed for {filename}: {e}", exc_info=True
            )
            return "fail", folder, filename
        finally:
            if r is not None:
                r.close()


class HTTPMetadataFetcher(MetadataFetcher):
    def __init__(self, config: Config, session: requests.Session):
        self.config = config
        self.session = session

    def fetch(self, replay_id: str) -> Tuple[Optional[str], Optional[dict]]:
        if not replay_id:
            return None, None
        url = f"{self.config.base_api}/replays/{replay_id}"
        try:
            r = self.session.get(url, timeout=10)
            r.raise_for_status()
            meta = r.json()
            fname = meta.get("fileName")
            return (fname, meta) if fname else (None, None)
        except Exception as e:
            self.config.logger.error(f"Metadata fetch failed for {replay_id}: {e}")
            return None, None


T = TypeVar("T")


class Command(Generic[T], ABC):
    @abstractmethod
    def execute(self) -> T: ...


class SearchReplaysCommand(Command[List[dict]]):
    def __init__(self, config: Config, page: int, session: requests.Session):
        self.config = config
        self.page = page
        self.session = session

    def execute(self) -> List[dict]:
        try:
            params = {
                "page": self.page,
                "limit": self.config.results_per_page_limit,
                "hasBots": "false",
                "endedNormally": "true",
                "date": [self.config.from_date, self.config.to_date],
            }
            self.config.logger.debug(params)
            r = self.session.get(
                self.config.listen_endpoint,
                params=params,
                timeout=15,
            )
            r.raise_for_status()
            return r.json().get("data", []) or []
        except Exception as e:
            self.config.logger.error(f"Search failed on page {self.page}: {e}")
            return []


class FilterNewReplaysCommand(Command):
    def __init__(
        self,
        data: List[dict],
        seen_ids: Set[str],
        force_meta: bool,
    ):
        self.data = data
        self.seen_ids = seen_ids
        self.force_meta = force_meta

    def execute(self) -> Tuple[List[dict], int]:
        new: List[dict] = []
        skipped = 0
        for r in self.data:
            rid = r.get("id")
            if rid is None:
                continue
            if not self.force_meta and rid in self.seen_ids:
                skipped += 1
                continue
            new.append(r)
            self.seen_ids.add(rid)
        return new, skipped


class ParallelFetchMetadataCommand(Command[List[Tuple[str, str, str]]]):
    def __init__(
        self,
        replays: List[dict],
        fetcher: MetadataFetcher,
        config: Config,
    ):
        self.replays = replays
        self.fetcher = fetcher
        self.config = config

        self.executor = None
        self.futures = {}

    @cancel_futures_on_interrupt
    def execute(self) -> List[Tuple[str, str, str]]:
        results: List[Tuple[str, str, str]] = []
        with ThreadPoolExecutor(max_workers=self.config.pool_maxsize) as exec:
            self.executor = exec
            self.futures = {
                exec.submit(self.fetcher.fetch, r["id"]): r for r in self.replays
            }
            for fut in tqdm(
                as_completed(self.futures),
                total=len(self.futures),
                desc="Fetching metadata",
                ncols=80,
            ):
                r = self.futures[fut]
                rid = r.get("id")
                assert(isinstance(rid, str))
                try:
                    fname, meta = fut.result()
                except Exception as e:
                    self.config.logger.error(f"Future error: {e}")
                    continue
                if not fname or not meta:
                    continue
                start = meta.get("startTime", "")
                # save metadata file
                mpath = self.config.metas_folder / f"{rid}.json"
                mpath.parent.mkdir(parents=True, exist_ok=True)
                with open(mpath, "w", encoding="utf-8") as mf:
                    json.dump(meta, mf, ensure_ascii=False)
                results.append((rid, fname, start))
        return results


class ParallelDownloadCommand(Command):
    def __init__(
        self,
        downloads: List[Tuple[str, str, str]],
        downloader: ReplayDownloader,
        config: Config,
    ):
        self.downloads = downloads
        self.downloader = downloader
        self.config = config

        self.executor = None
        self.futures = {}

    @cancel_futures_on_interrupt
    def execute(self) -> Dict[str, int]:
        counts = {"ok": 0, "exists": 0, "fail": 0}
        with ThreadPoolExecutor(max_workers=self.config.pool_maxsize) as exec:
            self.executor = exec
            self.futures = {
                exec.submit(self.downloader.download, fn, st): (rid, fn)
                for rid, fn, st in self.downloads
            }
            for fut in tqdm(
                as_completed(self.futures),
                total=len(self.futures),
                desc="Downloading files",
                ncols=80,
            ):
                rid, fn = self.futures[fut]
                try:
                    status, folder, _ = fut.result()
                except Exception as e:
                    self.config.logger.error(f"Download future error: {e}")
                    status = "fail"
                    folder = self.config.download_folder
                counts[status] += 1
                if status in ("ok", "exists"):
                    append_to_downloaded_log(self.config, rid, folder)
        return counts


def scrape_replays(
    config: Config,
    downloader: ReplayDownloader,
    fetcher: MetadataFetcher,
    session: requests.Session,
    run_endless: bool = False,
) -> None:
    logger = config.logger
    logger.info("[scrape_replays] Starting listener...")
    summarizer = Summarizer(config)

    seen_ids = load_all_seen_ids(config)
    page = 0
    empty = 0

    try:
        while run_endless or empty < config.listen_max_empty_pages:
            try:
                page += 1
                # 1) Search
                logger.info(f"Requesting Page:{page} Totals: Empty:{empty}")
                raw = SearchReplaysCommand(config, page, session).execute()
                if not raw:
                    empty += 1
                    logger.info(
                        f"Page {page} Empty response ({empty}/{config.listen_max_empty_pages})"
                    )
                    countdown_sleep(config.listen_interval)
                    continue

                # 2) Filter
                new_replays, skipped = FilterNewReplaysCommand(
                    raw, seen_ids, config.force_meta
                ).execute()
                if not new_replays:
                    empty += 1
                    logger.info(
                        f"Page {page} No new replays found ({empty}/{config.listen_max_empty_pages})"
                    )
                    countdown_sleep(config.listen_interval)
                    continue

                firstDate = new_replays[0]["startTime"][:10]
                lastDate = new_replays[-1]["startTime"][:10]
                empty = 0
                logger.info(
                    f"Found {len(new_replays)} new replay(s) Skipped Seen: {skipped} {firstDate} - {lastDate}"
                )

                # 3) Fetch All Metadata
                fetch_start = time.perf_counter()
                metas = ParallelFetchMetadataCommand(
                    new_replays, fetcher, config
                ).execute()
                fetch_elapsed = time.perf_counter() - fetch_start

                total_to_fetch = len(new_replays)  # items you tried
                ok = len(metas)
                fail = total_to_fetch - ok
                summarizer.report(
                    Summary("Metadata", total_to_fetch, ok, fail, fetch_elapsed)
                )

                # 4) Download All
                if not config.skip_download:
                    dl_start = time.perf_counter()
                    dl_counts = ParallelDownloadCommand(
                        metas, downloader, config
                    ).execute()
                    dl_elapsed = time.perf_counter() - dl_start

                    summarizer.report(
                        Summary(
                            "Download",
                            dl_counts["ok"] + dl_counts["fail"] + dl_counts["exists"],
                            dl_counts["ok"] + dl_counts["exists"],
                            dl_counts["fail"],
                            dl_elapsed,
                        )
                    )

                else:
                    logger.info("Skipping downloads")

                countdown_sleep(config.listen_interval)

            except KeyboardInterrupt:
                logger.info("\n[scrape_replays] Interrupted by user, shutting down.")
                sys.exit(0)

            except Exception as e:
                logger.error("Unhandled error in scrape_replays loop", exc_info=True)
                countdown_sleep(config.listen_interval)

    except Exception as e:
        logger.error(f"[scrape_replays] Error in while loop: {e}")

    logger.info("Listener stopped.")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Listen for new BAR replays.")
    p.add_argument("--download-folder", type=Path, default="Replays")
    p.add_argument("--metas-folder", type=Path, default="metas")
    p.add_argument(
        "--from-date",
        type=str,
        default=(datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d"),
    )
    p.add_argument(
        "--to-date",
        type=str,
        default=(datetime.today() + timedelta(days=2)).strftime("%Y-%m-%d"),
    )
    p.add_argument("--listen-interval", type=int, default=1)
    p.add_argument("--results-per-page-limit", type=int, default=500)
    p.add_argument("--listen-max-empty-pages", type=int, default=5)
    p.add_argument("--skip-download", action="store_true")
    p.add_argument("--force-meta", action="store_true")
    p.add_argument(
        "--listen",
        action="store_true",
        help="Run endlessly ignoring max-empty-pages limit",
    )
    p.add_argument(
        "--sandbox",
        action="store_true",
        help="Run using real downloads/network, but write to safe folders",
    )
    args = p.parse_args()

    cfg = Config(
        download_folder=args.download_folder,
        metas_folder=args.metas_folder,
        from_date=args.from_date,
        to_date=args.to_date,
        listen_interval=args.listen_interval,
        results_per_page_limit=args.results_per_page_limit,
        listen_max_empty_pages=args.listen_max_empty_pages,
        skip_download=args.skip_download,
        force_meta=args.force_meta,
        sandbox=args.sandbox,
        logger=get_run_logger(),
    )

    if args.sandbox:
        cfg.download_folder = (
            args.download_folder.parent / f"{args.download_folder.name}_staging"
        )
        cfg.metas_folder = (
            args.metas_folder.parent / f"{args.metas_folder.name}_staging"
        )
        cfg.logger.info(
            f"[SANDBOX MODE] Writing to: - DL: {cfg.download_folder} - M: {cfg.metas_folder}"
        )

    cfg.download_folder.mkdir(parents=True, exist_ok=True)
    cfg.metas_folder.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=cfg.pool_connections,
        pool_maxsize=cfg.pool_maxsize,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    downloader = HTTPReplayDownloader(cfg, session)
    fetcher = HTTPMetadataFetcher(cfg, session)

    scrape_replays(cfg, downloader, fetcher, session, run_endless=args.listen)
