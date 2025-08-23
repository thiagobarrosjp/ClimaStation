"""
ClimaStation DWD Repository Crawler (canonicalize ONLY emitted file URLs)

SCRIPT IDENTIFICATION: DWD10TAH3W (DWD Crawler)

PUBLIC API (stable):
- crawl_dwd_repository(config: Dict[str, Any],
                       logger: logging.Logger | logging.LoggerAdapter,
                       throttle: Optional[float] = None) -> CrawlResult

Notes:
- Parses simple directory listings without third-party HTML parsers.
- Emits a single JSONL manifest to `crawler.output_urls_jsonl` when provided.
- Idempotent behavior (sorted + de-duped by (relative_path, filename)).
- Honors an optional crawl limit provided via `crawler.max_items` or `runner_args.limit`.
- Emits a deterministic *sample* JSONL with the first N lines after final sort & de-dupe.
- 2025-08-20 update: **Canonicalization is applied ONLY to emitted file URLs**.
  Crawling/fetching uses the discovered URLs as-is (useful for offline tests on a local server).
  Non-zip files are included as long as they are files (href not ending with '/').
"""
from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse, urlunparse
import posixpath

import requests
import logging

from app.utils.http_headers import default_headers


@dataclass
class CrawlResult:
    """Result of a crawl: counts, paths and status."""
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


def _sort_and_dedupe(records: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
    """Return (unique_sorted_records, skipped_due_to_dupes).

    De-duplicates by (relative_path, filename) keeping the first occurrence,
    then sorts by the same pair.
    """
    seen_keys: Set[Tuple[str, str]] = set()
    unique: List[Dict[str, Any]] = []
    skipped = 0
    for rec in records:
        rp = str(rec.get("relative_path", ""))
        fn = str(rec.get("filename", ""))
        key = (rp, fn)
        if key in seen_keys:
            skipped += 1
            continue
        seen_keys.add(key)
        unique.append(rec)

    unique.sort(key=lambda r: (str(r.get("relative_path", "")), str(r.get("filename", ""))))
    return unique, skipped


def _atomic_write_jsonl(target: Path, records: List[Dict[str, Any]], logger: logging.Logger) -> int:
    """Write *records* to *target* as JSONL atomically. Returns lines written.

    Uses UTF-8 with LF newlines and an atomic os.replace() from a .tmp path.
    Ensures deterministic key order (insertion order) and no extraneous spaces.
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    try:
        with tmp.open("w", encoding="utf-8", newline="\n") as fh:
            for rec in records:
                # rec is constructed with fixed key order: url, relative_path, filename, (optional dataset_key)
                fh.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")
        os.replace(tmp, target)
    finally:
        if tmp.exists():
            try:
                tmp.unlink(missing_ok=True)  # type: ignore[arg-type]
            except Exception:
                pass
    logger.debug(
        "Wrote JSONL",
        extra={"component": "CRAWLER", "structured_data": {"path": str(target), "lines": len(records)}},
    )
    return len(records)


def _get_sample_count_from_env(default: int = 100) -> int:
    raw = os.getenv("CLIMASTATION_CRAWLER_SAMPLE_COUNT", "")
    try:
        n = int(raw)
        if n >= 1:
            return n
    except Exception:
        pass
    return default


# ------------------------------
# Core crawler
# ------------------------------

class DWDRepositoryCrawler:
    """Crawl DWD directory pages and producte URL manifests."""
    def __init__(self, config: Dict[str, Any], logger: logging.Logger, throttle: Optional[float] = None):
        
        self.config = config or {}
        self.logger = logger

        crawler_cfg = _get_cfg(self.config, "crawler", default={}) or {}

        # Base URL (NEW: crawler.base_url) + optional env override for fixtures
        default_base = str(crawler_cfg.get("base_url", "https://opendata.dwd.de/climate_environment/CDC/"))
        env_start = os.getenv("CLIMASTATION_CRAWLER_BASE_URL")
        self.base_url: str = env_start if env_start else default_base

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
        self.dataset_root_path = root_path

        # Compute starting URL
        # If an env override is provided, use it AS-IS (plus trailing slash) to support offline fixtures.
        if env_start:
            self.start_url = env_start if env_start.endswith("/") else env_start + "/"
        else:
            self.start_url = urljoin(self.base_url.rstrip("/") + "/", root_path.lstrip("/"))
            if not self.start_url.endswith("/"):
                self.start_url += "/"

        # Optional canonical base (scheme+host), used ONLY when emitting file URLs
        self.canonical_base: Optional[str] = os.getenv("CLIMASTATION_CANONICAL_BASE_URL") or None

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
                "No crawler.subfolders configured; crawl will scan the dataset root",
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
            dataset_name = str(self.config.get("name") or self.config.get("dataset", "dataset"))
            base_output = Path(_get_cfg(self.config, "dwd_paths", "crawl_data", default="data/dwd/1_crawl_dwd"))
            self.output_file_path = base_output / f"{dataset_name}_urls.jsonl"
        self.output_file_path.parent.mkdir(parents=True, exist_ok=True)

        # Sample path (computed at write time since N may vary)
        self.dataset_name = str(self.config.get("name") or self.config.get("dataset", "dataset"))
        self.crawl_output_dir = self.output_file_path.parent

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
                    "canonical_base": self.canonical_base or "none",
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
        """Return (subdirs, files) from a simple Apache-style listing.

        subdirs: hrefs that end with '/'
        files: every other href (not '../' or absolute '/')
        """
        hrefs = re.findall(r'href=["\']([^"\']+)', html, flags=re.IGNORECASE)
        hrefs = [h for h in hrefs if h not in ("../", "/")]
        subdirs = [h for h in hrefs if h.endswith("/")]
        files = [h for h in hrefs if not h.endswith("/")]
        return subdirs, files

    # --------------------------
    # Crawl helpers
    # --------------------------

    def _emit_file(self, next_url: str) -> None:
        """Emit a manifest record for a *file* discovered at `next_url`.

        Canonicalization is applied ONLY here, never to fetch URLs.
        """
        if self._stop_flag:
            return

        r = urlparse(next_url)
        # Skip directories; only files
        if r.path.endswith("/"):
            return

        # Compute fields from the resolved path
        filename = posixpath.basename(r.path)
        rel_dir = posixpath.dirname(r.path)
        relative_path = (rel_dir + "/") if rel_dir else ""

        # Rebase scheme+netloc if canonical base is provided, keep path
        cr = urlparse(os.environ.get("CLIMASTATION_CANONICAL_BASE_URL", ""))
        final_url = urlunparse((cr.scheme or "https", cr.netloc or r.netloc, r.path, "", "", ""))

        # De-dupe early by full URL to reduce downstream work
        if final_url in self._seen_urls:
            return
        self._seen_urls.add(final_url)

        rec = {
            "url": final_url,
            "relative_path": relative_path,
            "filename": filename,
        }
        dataset_key = str(self.config.get("name") or self.config.get("dataset", ""))
        if dataset_key:
            rec["dataset_key"] = dataset_key

        self.url_records.append(rec)
        if self.max_items is not None and len(self.url_records) >= self.max_items:
            self._stop_flag = True

    def _crawl_dir(self, url: str, depth: int):
        if self._stop_flag or depth > self.max_depth:
            return
        resp = self._request(url)
        if not resp:
            return
        subdirs, files = self._parse_listing_html(resp.text)
        # Emit files using *as-discovered* absolute URL (do not canonicalize for fetch)
        for fn in files:
            if self._stop_flag:
                break
            next_url = urljoin(url, fn)
            self._emit_file(next_url)
        # Recurse into subdirectories using as-is URLs
        for sd in subdirs:
            if self._stop_flag:
                break
            if depth == 0 and self.subpaths and sd not in self.subpaths:
                continue
            next_url = urljoin(url, sd)
            # Only enqueue (do not emit records for directories)
            self._crawl_dir(next_url, depth + 1)

    # --------------------------
    # Crawl entry
    # --------------------------

    def crawl_repository(self) -> CrawlResult:
        """Ccrawl DWD listings and emit deterministic URL manifest(s)."""
        start = time.time()
        errors: List[str] = []

        # Start from each configured subpath, or root if none given
        roots = self.subpaths or [""]
        for sub in roots:
            if self._stop_flag:
                break
            root_url = urljoin(self.start_url, sub)
            try:
                self._crawl_dir(root_url, depth=0)
            except Exception as e:
                msg = f"crawl_error:{root_url}:{e}"
                errors.append(msg)
                self.logger.error(
                    "Unhandled crawl exception",
                    extra={"component": "CRAWLER", "structured_data": {"root_url": root_url, "error": str(e)}},
                )

        # If a limit was set, ensure we hard-cap before writing (keeps discovery order before sort)
        limit_applied = 0
        if self.max_items is not None and len(self.url_records) > self.max_items:
            limit_applied = self.max_items
            self.url_records = self.url_records[: self.max_items]
        elif self.max_items is not None:
            limit_applied = min(self.max_items, len(self.url_records))

        # Finalize: sort & unique by (relative_path, filename)
        unique_sorted, skipped = _sort_and_dedupe(self.url_records)

        # Write full manifest atomically
        written_full = _atomic_write_jsonl(self.output_file_path, unique_sorted, self.logger)

        # Prepare and write deterministic sample (first N lines of final order)
        sample_n = _get_sample_count_from_env(100)
        sample_records = unique_sorted[:sample_n]
        sample_path = self.crawl_output_dir / f"{self.dataset_name}_urls_sample{sample_n}.jsonl"
        written_sample = _atomic_write_jsonl(sample_path, sample_records, self.logger)

        elapsed = time.time() - start
        self.logger.info(
            "Crawl completed",
            extra={
                "component": "CRAWLER",
                "structured_data": {
                    "canonical_base": self.canonical_base or "none",
                    "files_found": len(self.url_records),
                    "files_written": written_full,
                    "files_skipped": skipped,
                    "limit_applied": limit_applied if self.max_items is not None else None,
                    "requests": self.crawled_count,
                    "elapsed_seconds": round(elapsed, 2),
                    "resolved_listing_url": self.start_url,
                    "output_file": str(self.output_file_path),
                    "sample_file": str(sample_path),
                    "sample_count": written_sample,
                },
            },
        )

        return CrawlResult(
            url_records=unique_sorted,  # return final (sorted+unique) sequence
            files_found=len(self.url_records),
            files_written=written_full,
            files_skipped=skipped,
            errors=errors,
            crawled_count=self.crawled_count,
            elapsed_time=elapsed,
            output_files={"urls": self.output_file_path, "sample": sample_path},
        )


# ------------------------------
# Public API
# ------------------------------

def crawl_dwd_repository(
    config: Dict[str, Any],
    logger: logging.Logger | logging.LoggerAdapter,
    throttle: Optional[float] = None,
) -> CrawlResult:
    """Discover per-file URLs under the dataset root and write a single JSONL.

    The function **does not raise** on expected failures; it logs a summary and
    returns a CrawlResult with counts and the JSONL output path.
    """
    base_logger = logging.getLogger("pipeline.crawler")
    if isinstance(logger, logging.LoggerAdapter):
        norm_logger = logger.logger
    elif isinstance(logger, logging.Logger):
        norm_logger = logger
    else:
        norm_logger = base_logger

    crawler = DWDRepositoryCrawler(config=config, logger=norm_logger, throttle=throttle)
    return crawler.crawl_repository()
