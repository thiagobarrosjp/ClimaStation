"""
ClimaStation DWD Repository Crawler

SCRIPT IDENTIFICATION: DWD10TAH3W (DWD Crawler)

PURPOSE:
Modernized crawler for the official DWD climate data repository that integrates
with the ClimaStation architecture. Recursively crawls configured subpaths under
specified dataset root to identify downloadable .zip archives for the download
pipeline.

RESPONSIBILITIES:
- Traverse only dataset-defined subpaths up to configurable depth
- Discover and emit individual .zip file URLs
- Stream output as JSONL for memory efficiency
- Integrate with configuration system and enhanced logging
- Support progress tracking, retries, and error recovery
- Follow dependency injection patterns for reusability

USAGE:
    from app.pipeline.crawler import crawl_dwd_repository
    from app.utils.enhanced_logger import get_logger
    from app.utils.config_manager import load_config

    logger = get_logger("CRAWLER")
    config = load_config("10_minutes_air_temperature", logger)

    result = crawl_dwd_repository(config, logger, throttle=1.0)
    # result.output_files["urls"] points to dwd_10_minutes_air_temperature_urls.jsonl
"""
import requests
from bs4 import BeautifulSoup, Tag
from urllib.parse import urljoin
from dataclasses import dataclass
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

from app.utils.http_headers import default_headers
from app.utils.enhanced_logger import StructuredLoggerAdapter
from app.utils.config_manager import ConfigurationError

@dataclass
class CrawlResult:
    url_records: List[Dict[str, Any]]
    crawled_count: int
    elapsed_time: float
    output_files: Dict[str, Path]

class DWDRepositoryCrawler:
    def __init__(
        self,
        config: Dict[str, Any],
        logger: StructuredLoggerAdapter,
        throttle: Optional[float] = None,
    ):
        self.config = config
        self.logger = logger

        crawler_cfg = config.get('crawler', {})

        # 1) Base URL
        self.base_url = crawler_cfg.get(
            'base_url',
            'https://opendata.dwd.de/climate_environment/CDC/observations_germany/'
        )

        # 2) Timing & retry
        self.max_depth = crawler_cfg.get('max_depth', 10)
        self.request_timeout = crawler_cfg.get('request_timeout_seconds', 30)
        cfg_delay = crawler_cfg.get('request_delay_seconds', 0.1)
        self.request_delay = throttle if throttle is not None else cfg_delay
        self.max_retries = crawler_cfg.get('max_retries', 3)

        # 3) Dataset‐specific root path
        dataset_path = config.get('base_path') or crawler_cfg.get('dataset_path')
        if not dataset_path:
            raise ConfigurationError("Missing 'base_path' in configuration")
        if not dataset_path.endswith('/'):
            dataset_path += '/'
        self.start_url = urljoin(self.base_url, dataset_path)

        # 4) Subpaths under dataset root
        self.subpaths = crawler_cfg.get('subpaths')
        if not isinstance(self.subpaths, list) or not self.subpaths:
            raise ConfigurationError("Missing 'crawler.subpaths' list in configuration")

        # 5) Output directory and file
        paths = config.get('dwd_paths', {}) or {}
        base_output = Path(paths.get('crawl_data', 'data/dwd/1_crawl_dwd'))
        dataset_name = config.get('name')
        if not isinstance(dataset_name, str) or not dataset_name:
            raise ConfigurationError("Missing top-level 'name' in configuration")
        self.output_dir = base_output / dataset_name
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.output_file_path = self.output_dir / f"dwd_{dataset_name}_urls.jsonl"

        # Internal state
        self.url_records: List[Dict[str, Any]] = []
        self.crawled_count = 0
        self.start_time = 0.0

        self.logger.info(
            "DWD crawler initialized",
            extra={
                "component": "CRAWLER",
                "structured_data": {
                    "start_url": self.start_url,
                    "subpaths": self.subpaths,
                    "output_file": str(self.output_file_path),
                    "throttle": self.request_delay
                }
            }
        )

    def _make_request(self, url: str, retries: int = 0) -> Optional[requests.Response]:
        try:
            resp = requests.get(url, headers=default_headers(), timeout=self.request_timeout)
            resp.raise_for_status()
            self.logger.info(f"Visited URL: {url}", extra={
                "component": "CRAWLER", "structured_data": {"url": url}
            })
            if self.request_delay > 0:
                self.logger.debug(f"Sleeping for {self.request_delay:.1f}s before next crawl request...")
                time.sleep(self.request_delay)
            return resp
        except requests.exceptions.RequestException as e:
            if retries < self.max_retries:
                self.logger.warning(f"Retry {retries+1} for URL: {url}", extra={
                    "component": "CRAWLER", "structured_data": {"url": url, "retry": retries+1}
                })
                time.sleep(2 ** retries)
                return self._make_request(url, retries + 1)
            self.logger.error(f"Failed request after retries: {url}", extra={
                "component": "CRAWLER", "structured_data": {"url": url, "error": str(e)}
            })
            return None

    def _parse_listing(self, response: requests.Response) -> Tuple[List[str], List[str]]:
        """
        Parses an HTML directory listing, returning (subdirs, zip_files)
        where both lists are List[str].
        """
        soup = BeautifulSoup(response.text, "html.parser")
        hrefs: List[str] = []
        # Use tag['href'] directly (BeautifulSoup Tag supports __getitem__)
        for tag in soup.find_all("a", href=True):
            href = tag['href']  # type: ignore[attr-defined]
            if isinstance(href, str) and href not in ('../', '/'):
                hrefs.append(href)

        subdirs = [h for h in hrefs if h.endswith("/")]
        zip_files = [h for h in hrefs if h.lower().endswith(".zip")]
        return subdirs, zip_files

    def _crawl_directory(self, url: str, path_parts: List[str], fp) -> None:
        self.crawled_count += 1

        # Depth guard
        if len(path_parts) > self.max_depth:
            return

        resp = self._make_request(url)
        if not resp:
            return

        subdirs, zip_files = self._parse_listing(resp)

        # Emit records for each ZIP
        for fname in zip_files:
            file_url = urljoin(url, fname)
            record = {
                "url": file_url,
                "relative_path": "/".join(path_parts + [fname]),
                "filename": fname
            }
            fp.write(json.dumps(record) + "\n")
            self.url_records.append(record)

        # Recurse only into allowed subdirs
        for sub in subdirs:
            if sub in self.subpaths:
                next_url = urljoin(url, sub)
                self._crawl_directory(next_url, path_parts + [sub.rstrip("/")], fp)

    def crawl_repository(self) -> CrawlResult:
        self.start_time = time.time()
        self.logger.info("Starting crawl", extra={
            "component": "CRAWLER", "structured_data": {"start_url": self.start_url}
        })

        with open(self.output_file_path, "w", encoding="utf-8") as fp:
            for sub in self.subpaths:
                self._crawl_directory(urljoin(self.start_url, sub), [sub.rstrip("/")], fp)

        elapsed = time.time() - self.start_time
        self.logger.info("Crawl completed", extra={
            "component": "CRAWLER",
            "structured_data": {
                "zip_count": len(self.url_records),
                "requests": self.crawled_count,
                "elapsed_time": round(elapsed, 2)
            }
        })

        return CrawlResult(
            url_records=self.url_records,
            crawled_count=self.crawled_count,
            elapsed_time=elapsed,
            output_files={"urls": self.output_file_path}
        )

def crawl_dwd_repository(
    config: Dict[str, Any],
    logger: StructuredLoggerAdapter,
    throttle: Optional[float] = None,
) -> CrawlResult:
    return DWDRepositoryCrawler(config, logger, throttle=throttle).crawl_repository()

