"""
ClimaStation DWD Repository Crawler (updated for NEW YAML contract)

SCRIPT IDENTIFICATION: DWD10TAH3W (DWD Crawler)

PUBLIC API (stable):
- crawl_dwd_repository(config: Dict[str, Any],
                       logger: logging.Logger | logging.LoggerAdapter,
                       throttle: Optional[float] = None) -> CrawlResult

Notes:
- Parses simple directory listings without third-party HTML parsers.
- **Single JSONL output** written/merged **only** to `crawler.output_urls_jsonl` when provided.
- Idempotent behavior (de-dupe by URL on re-runs).
- Honors an optional crawl limit provided via `crawler.max_items` or `runner_args.limit`.
"""
from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin

import requests
import logging

from app.utils.http_headers import default_headers


@dataclass
class CrawlResult:
    url_records: List[Dict[str, Any]]
    files_found: int
    files_written: int
    files_skipped: int
    errors: List[str]
    crawled_count: int
    elapsed_time: float
    output_files: Dict[str, Path]


# ------------------------------
# Utilities
# ------------------------------

def _get_cfg(cfg: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    cur: Any = cfg
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _ensure_jsonl_idempotent(target: Path, records: List[Dict[str, Any]], logger: logging.Logger) -> Tuple[int, int]:
    """Merge with existing JSONL by URL, write atomically, return (written, skipped)."""
    existing_urls: Set[str] = set()
    if target.exists():
        try:
            with target.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                        url = obj.get("url")
                        if isinstance(url, str):
                            existing_urls.add(url)
                    except json.JSONDecodeError:
                        # ignore bad line but keep going
                        continue
        except Exception as e:
            logger.warning(
                "Failed reading existing JSONL; continuing fresh",
                extra={"component": "CRAWLER", "structured_data": {"jsonl_path": str(target), "error": str(e)}},
            )

    unique: List[Dict[str, Any]] = []
    skipped = 0
    for rec in records:
        url = rec.get("url")
        if not isinstance(url, str):
            continue
        if url in existing_urls:
            skipped += 1
            continue
        existing_urls.add(url)
        unique.append(rec)

    # Atomic write (append only new lines)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".part")
    try:
        # Strategy: copy existing file, then append unique; else create new
        if target.exists():
            tmp.write_bytes(target.read_bytes())
        with tmp.open("a", encoding="utf-8") as fh:
            for rec in unique:
                fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
        tmp.replace(target)
    finally:
        if tmp.exists() and tmp != target:
            try:
                pass
            except Exception:
                pass

    return len(unique), skipped


# ------------------------------
# Core crawler
# ------------------------------

class DWDRepositoryCrawler:
    def __init__(self, config: Dict[str, Any], logger: logging.Logger, throttle: Optional[float] = None):
        self.config = config or {}
        self.logger = logger

        crawler_cfg = _get_cfg(self.config, "crawler", default={}) or {}

        # Base URL (NEW: crawler.base_url)
        self.base_url: str = str(
            crawler_cfg.get("base_url", "https://opendata.dwd.de/climate_environment/CDC/")
        )

        # Dataset root path (NEW: crawler.root_path). Fallbacks for legacy compat.
        root_path: Optional[str] = (
            str(crawler_cfg.get("root_path")) if crawler_cfg.get("root_path") is not None else None
        )
        if root_path is None:
            # Legacy fallbacks
            root_path = (
                self.config.get("base_path")
                or _get_cfg(self.config, "source", "base_path")
                or crawler_cfg.get("dataset_path")
                or ""
            )
        root_path = str(root_path)
        if root_path and not root_path.endswith("/"):
            root_path += "/"
        self.start_url = urljoin(self.base_url.rstrip("/") + "/", root_path.lstrip("/"))

        # Subfolders (NEW: crawler.subfolders as mapping) → allowed first-level subpaths list
        subfolders = crawler_cfg.get("subfolders")
        subpaths_cfg = crawler_cfg.get("subpaths")  # legacy list compat
        if isinstance(subfolders, dict) and subfolders:
            subpaths: List[str] = [str(v) for v in subfolders.values()]
        elif isinstance(subpaths_cfg, list):
            subpaths = [str(s) for s in subpaths_cfg]
        else:
            subpaths = []
        self.subpaths: List[str] = [s if s.endswith("/") else s + "/" for s in subpaths]
        if not self.subpaths:
            self.logger.warning(
                "No crawler.subfolders configured; crawl will only scan the dataset root for .zip files",
                extra={"component": "CRAWLER", "structured_data": {"start_url": self.start_url}},
            )

        # Crawl behavior
        self.max_depth = int(crawler_cfg.get("max_depth", 8))
        self.request_timeout = int(crawler_cfg.get("request_timeout_seconds", 30))
        cfg_delay = float(crawler_cfg.get("request_delay_seconds", 0.0))
        self.request_delay = float(throttle) if throttle is not None else cfg_delay
        self.max_retries = int(crawler_cfg.get("max_retries", 2))

        # LIMIT: prefer crawler.max_items; fallback to runner_args.limit if present
        limit_cfg = crawler_cfg.get("max_items")
        if limit_cfg is None:
            limit_cfg = _get_cfg(self.config, "runner_args", "limit", default=None)
        self.max_items: Optional[int] = int(limit_cfg) if limit_cfg is not None else None

        # Output path (NEW: crawler.output_urls_jsonl)
        out_path_cfg = crawler_cfg.get("output_urls_jsonl")
        if isinstance(out_path_cfg, str) and out_path_cfg.strip():
            self.output_file_path = Path(out_path_cfg)
        else:
            # Robust fallback: write to <dwd_paths.crawl_data>/<dataset_name>_urls.jsonl
            dataset_name = str(self.config.get("name") or self.config.get("dataset", "dataset"))
            base_output = Path(_get_cfg(self.config, "dwd_paths", "crawl_data", default="data/dwd/1_crawl_dwd"))
            self.output_file_path = base_output / f"{dataset_name}_urls.jsonl"
        self.output_file_path.parent.mkdir(parents=True, exist_ok=True)

        # State
        self._seen_urls: Set[str] = set()
        self.url_records: List[Dict[str, Any]] = []
        self.crawled_count = 0
        self._stop_flag = False

        self.logger.info(
            "Crawler initialized",
            extra={
                "component": "CRAWLER",
                "structured_data": {
                    "start_url": self.start_url,
                    "subpaths": self.subpaths,
                    "output_file": str(self.output_file_path),
                    "throttle": self.request_delay,
                    "limit": self.max_items,
                },
            },
        )

    # --------------------------
    # Networking & parsing
    # --------------------------

    def _sleep(self):
        if self.request_delay and self.request_delay > 0:
            time.sleep(self.request_delay)

    def _request(self, url: str, attempt: int = 0) -> Optional[requests.Response]:
        try:
            resp = requests.get(url, headers=default_headers(), timeout=self.request_timeout)
            self.crawled_count += 1
            if resp.status_code != 200:
                # Explicit status + URL in logs
                self.logger.error(
                    "HTTP request failed",
                    extra={"component": "CRAWLER", "structured_data": {"url": url, "status": resp.status_code}},
                )
                if attempt < self.max_retries:
                    time.sleep(min(2 ** attempt, 4))
                    return self._request(url, attempt + 1)
                return None
            self._sleep()
            return resp
        except Exception as e:
            if attempt < self.max_retries:
                self.logger.warning(
                    "Retrying request",
                    extra={"component": "CRAWLER", "structured_data": {"url": url, "attempt": attempt + 1}},
                )
                time.sleep(min(2 ** attempt, 4))
                return self._request(url, attempt + 1)
            self.logger.error(
                "Request exception",
                extra={"component": "CRAWLER", "structured_data": {"url": url, "error": str(e)}},
            )
            return None

    @staticmethod
    def _parse_listing_html(html: str) -> Tuple[List[str], List[str]]:
        """Return (subdirs, zip_names) from a simple Apache-style listing."""
        hrefs = re.findall(r'href=["\']([^"\']+)', html, flags=re.IGNORECASE)
        hrefs = [h for h in hrefs if h not in ("../", "/")]
        subdirs = [h for h in hrefs if h.endswith("/")]
        zips = [h for h in hrefs if h.lower().endswith(".zip")]
        return subdirs, zips

    # --------------------------
    # Crawl helpers
    # --------------------------

    def _emit_zip(self, base_url: str, rel_parts: List[str], filename: str):
        if self._stop_flag:
            return
        url = urljoin(base_url, filename)
        if url in self._seen_urls:
            return
        self._seen_urls.add(url)
        rec = {
            "url": url,
            "relative_path": "/".join(rel_parts + [filename]),
            "filename": filename,
        }
        self.url_records.append(rec)
        # Respect limit eagerly
        if self.max_items is not None and len(self.url_records) >= self.max_items:
            self._stop_flag = True

    def _crawl_dir(self, url: str, path_parts: List[str], depth: int):
        if self._stop_flag or depth > self.max_depth:
            return
        resp = self._request(url)
        if not resp:
            return
        subdirs, zips = self._parse_listing_html(resp.text)
        for fn in zips:
            if self._stop_flag:
                break
            self._emit_zip(url, path_parts, fn)
        for sd in subdirs:
            if self._stop_flag:
                break
            # Only recurse into explicitly allowed subpaths at the first level
            if depth == 0 and self.subpaths and sd not in self.subpaths:
                continue
            next_url = urljoin(url, sd)
            self._crawl_dir(next_url, path_parts + [sd.rstrip("/")], depth + 1)

    # --------------------------
    # Crawl entry
    # --------------------------

    def crawl_repository(self) -> CrawlResult:
        start = time.time()
        errors: List[str] = []

        # Start from each configured subpath, or root if none given
        roots = self.subpaths or [""]
        for sub in roots:
            if self._stop_flag:
                break
            root_url = urljoin(self.start_url, sub)
            parts = [p for p in [sub.rstrip("/")] if p]
            try:
                self._crawl_dir(root_url, parts, depth=0)
            except Exception as e:
                msg = f"crawl_error:{root_url}:{e}"
                errors.append(msg)
                self.logger.error(
                    "Unhandled crawl exception",
                    extra={"component": "CRAWLER", "structured_data": {"root_url": root_url, "error": str(e)}},
                )

        # If a limit was set, ensure we hard-cap before writing
        limit_applied = 0
        if self.max_items is not None and len(self.url_records) > self.max_items:
            limit_applied = self.max_items
            self.url_records = self.url_records[: self.max_items]
        elif self.max_items is not None:
            limit_applied = min(self.max_items, len(self.url_records))

        # Write/merge JSONL idempotently to the **single required path**
        written, skipped = _ensure_jsonl_idempotent(self.output_file_path, self.url_records, self.logger)

        elapsed = time.time() - start
        self.logger.info(
            "Crawl completed",
            extra={
                "component": "CRAWLER",
                "structured_data": {
                    "files_found": len(self.url_records),
                    "files_written": written,
                    "files_skipped": skipped,
                    "limit_applied": limit_applied if self.max_items is not None else None,
                    "requests": self.crawled_count,
                    "elapsed_seconds": round(elapsed, 2),
                    "resolved_listing_url": self.start_url,
                    "output_file": str(self.output_file_path),
                },
            },
        )

        return CrawlResult(
            url_records=self.url_records,
            files_found=len(self.url_records),
            files_written=written,
            files_skipped=skipped,
            errors=errors,
            crawled_count=self.crawled_count,
            elapsed_time=elapsed,
            output_files={"urls": self.output_file_path},
        )


# ------------------------------
# Public API
# ------------------------------

def crawl_dwd_repository(
    config: Dict[str, Any],
    logger: logging.Logger | logging.LoggerAdapter,
    throttle: Optional[float] = None,
) -> CrawlResult:
    """Discover per-file ZIP URLs under the dataset root and write a single JSONL.

    The function **does not raise** on expected failures; it logs a summary and
    returns a CrawlResult with counts and the JSONL output path.
    """
    # Normalize to a plain Logger (avoid adapter type issues)
    base_logger = logging.getLogger("pipeline.crawler")
    if isinstance(logger, logging.LoggerAdapter):
        norm_logger = logger.logger
    elif isinstance(logger, logging.Logger):
        norm_logger = logger
    else:
        norm_logger = base_logger

    crawler = DWDRepositoryCrawler(config=config, logger=norm_logger, throttle=throttle)
    return crawler.crawl_repository()
