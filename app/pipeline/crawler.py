"""
ClimaStation DWD Repository Crawler

SCRIPT IDENTIFICATION: DWD10TAH3W (DWD Crawler)

PURPOSE:
Modernized crawler for the official DWD climate data repository that integrates
with the ClimaStation architecture. Recursively crawls all subdirectories under
a specified dataset root to identify downloadable .zip archives for the download
pipeline.

RESPONSIBILITIES:
- Recursively traverse the DWD dataset base URL up to configurable depth
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

    # Crawl repository
    result = crawl_dwd_repository(config, logger)
    # result.output_files["urls"] points to dwd_urls.jsonl

OUTPUT FORMAT:
- dwd_urls.jsonl: One JSON record per line with fields:
    {
      "url": "<full_download_url>",
      "relative_path": "<relative/path/from_root>",
      "filename": "<file_name.zip>"
    }

INTEGRATION:
- Uses config_manager for dataset and path settings
- Uses enhanced_logger with CRAWLER component
- Compatible with run_pipeline.py in crawl mode
"""

import requests
from bs4 import BeautifulSoup, Tag
from urllib.parse import urljoin
from dataclasses import dataclass
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

from app.utils.http_headers import default_headers
from app.utils.enhanced_logger import StructuredLoggerAdapter
from app.utils.config_manager import ConfigurationError

@dataclass
class CrawlResult:
    """
    Results from a DWD repository crawl.
    """
    url_records: List[Dict[str, Any]]
    crawled_count: int
    elapsed_time: float
    output_files: Dict[str, Path]

class DWDRepositoryCrawler:
    """
    Recursively discovers all ZIP file URLs under the DWD dataset root
    and writes them out as one JSONL record per file.
    """
    def __init__(self, config: Dict[str, Any], logger: StructuredLoggerAdapter):
        self.config = config
        self.logger = logger

        crawler_cfg = config.get('crawler', {})
        self.base_url = crawler_cfg.get(
            'base_url',
            'https://opendata.dwd.de/climate_environment/CDC/observations_germany/'
        )
        self.max_depth = crawler_cfg.get('max_depth', 10)
        self.request_timeout = crawler_cfg.get('request_timeout_seconds', 30)
        self.request_delay = crawler_cfg.get('request_delay_seconds', 0.1)
        self.max_retries = crawler_cfg.get('max_retries', 3)

        # Determine output directory (data/dwd/1_crawl_dwd/{dataset_name})
        paths = config.get('dwd_paths', {})
        base_output = Path(paths.get('crawl_data', 'data/dwd/1_crawl_dwd'))
        dataset_name = config.get('name')
        if not dataset_name:
            raise ConfigurationError("Configuration must include top-level 'name' for dataset")
        self.output_dir = base_output / dataset_name
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Internal state
        self.url_records: List[Dict[str, Any]] = []
        self.crawled_count = 0
        self.start_time = 0.0

        self.logger.info(
            "DWD crawler initialized",
            extra={
                "component": "CRAWLER",
                "structured_data": {
                    "base_url": self.base_url,
                    "output_dir": str(self.output_dir),
                    "max_depth": self.max_depth
                }
            }
        )

    def _make_request(self, url: str, retries: int = 0) -> Any:
        try:
            resp = requests.get(url, timeout=self.request_timeout)
            resp.raise_for_status()
            if self.request_delay > 0:
                time.sleep(self.request_delay)
            return resp
        except requests.exceptions.RequestException as e:
            if retries < self.max_retries:
                self.logger.warning(
                    f"Retry {retries+1} for URL: {url}",
                    extra={
                        "component": "CRAWLER",
                        "structured_data": {"url": url, "retry": retries+1, "error": str(e)}
                    }
                )
                time.sleep(2 ** retries)
                return self._make_request(url, retries + 1)
            self.logger.error(
                f"Failed request after retries: {url}",
                extra={"component": "CRAWLER", "structured_data": {"url": url, "error": str(e)}}
            )
            return None

    def _parse_listing(self, response: Any) -> Tuple[List[str], List[str]]:
        soup = BeautifulSoup(response.text, "html.parser")
        # Extract only string hrefs from <a> tags
        hrefs = [
            tag['href']
            for tag in soup.find_all("a", href=True)
            if isinstance(tag, Tag)
               and isinstance(tag['href'], str)
               and tag['href'] not in ('../', '/')
        ]
        # Use explicit str conversions to satisfy typing
        subdirs = [h for h in hrefs if str(h).endswith("/")]
        zip_files = [h for h in hrefs if str(h).lower().endswith(".zip")]
        return list(map(str, subdirs)), list(map(str, zip_files))

    def _crawl_directory(self, url: str, path_parts: List[str], fp) -> None:
        self.crawled_count += 1
        rel_path = "/".join(path_parts)

        if len(path_parts) > self.max_depth:
            self.logger.warning(
                f"Max depth {self.max_depth} reached at {rel_path}",
                extra={"component": "CRAWLER", "structured_data": {"path": rel_path, "depth": len(path_parts)}}
            )
            return

        if self.crawled_count % 50 == 0:
            elapsed = time.time() - self.start_time
            self.logger.info(
                f"Crawled {self.crawled_count} dirs in {elapsed:.2f}s",
                extra={"component": "CRAWLER", "structured_data": {"count": self.crawled_count}}
            )

        resp = self._make_request(url)
        if resp is None:
            return

        try:
            subdirs, zips = self._parse_listing(resp)
        except Exception as e:
            self.logger.error(
                f"Parse error at {url}: {e}",
                extra={"component": "CRAWLER"}
            )
            return

        # Write out each ZIP as a JSONL line
        for fname in zips:
            file_url = urljoin(url, fname)
            record = {
                "url": file_url,
                "relative_path": f"{rel_path}/{fname}",
                "filename": fname
            }
            fp.write(json.dumps(record) + "\n")
            self.url_records.append(record)
            self.logger.debug(
                f"Discovered ZIP: {record['relative_path']}",
                extra={"component": "CRAWLER", "structured_data": record}
            )

        # Recurse into subdirectories
        for sub in subdirs:
            sub_name = sub.rstrip("/")
            next_url = urljoin(url, sub)
            self._crawl_directory(next_url, path_parts + [sub_name], fp)

    def crawl_repository(self) -> CrawlResult:
        self.start_time = time.time()
        self.logger.info(
            "Starting crawl",
            extra={"component": "CRAWLER", "structured_data": {"base_url": self.base_url}}
        )

        resp = self._make_request(self.base_url)
        if resp is None:
            raise Exception(f"Cannot access base URL: {self.base_url}")

        subdirs, _ = self._parse_listing(resp)
        top_levels = [d.rstrip("/") for d in subdirs]

        urls_path = self.output_dir / "dwd_urls.jsonl"
        with open(urls_path, "w", encoding="utf-8") as fp:
            for d in top_levels:
                dir_url = urljoin(self.base_url, f"{d}/")
                self._crawl_directory(dir_url, [d], fp)

        elapsed = time.time() - self.start_time
        self.logger.info(
            "Crawl completed",
            extra={
                "component": "CRAWLER",
                "structured_data": {
                    "zip_count": len(self.url_records),
                    "requests": self.crawled_count,
                    "elapsed_time": round(elapsed, 2)
                }
            }
        )

        return CrawlResult(
            url_records=self.url_records,
            crawled_count=self.crawled_count,
            elapsed_time=elapsed,
            output_files={"urls": urls_path}
        )

def crawl_dwd_repository(config: Dict[str, Any], logger: StructuredLoggerAdapter) -> CrawlResult:
    crawler = DWDRepositoryCrawler(config, logger)
    return crawler.crawl_repository()
