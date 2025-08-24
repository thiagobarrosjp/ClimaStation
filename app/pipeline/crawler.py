# ---------------------------------------------------------------------------
# app/pipeline/crawler.py
# ---------------------------------------------------------------------------
"""
ClimaStation DWD Repository Crawler — URL-agnostic (no env vars)

PUBLIC API (stable):
- crawl_dwd_repository(
      config: Dict[str, Any],
      logger: logging.Logger | logging.LoggerAdapter,
      *,
      base_url: str,
      canonical_base_url: str,
      include_extensions: list[str],
      sample_size: int = 100,
      throttle: Optional[float] = None,
      limit: Optional[int] = None,
  ) -> CrawlResult

Notes:
- Parses simple directory listings without third-party HTML parsers.
- Emits a single JSONL manifest and a deterministic sample JSONL.
- Canonicalization applies ONLY to emitted file URLs: scheme+host taken from
  canonical_base_url; path is the dataset-relative path discovered during crawl.
- Relative paths are computed relative to the provided base_url path.
- Includes only files whose filename ends with any of include_extensions.
- Deterministic, atomic writes; sorted & de-duped by (relative_path, filename).
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
    """Return (unique_sorted_records, skipped_due_to_dupes)."""
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
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    try:
        with tmp.open("w", encoding="utf-8", newline="\n") as fh:
            for rec in records:
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


# ------------------------------
# Core crawler
# ------------------------------

class DWDRepositoryCrawler:
    """Crawl Apache-style directory listings and produce URL manifests."""

    def __init__(
        self,
        *,
        config: Dict[str, Any],
        logger: logging.Logger,
        base_url: str,
        canonical_base_url: str,
        include_extensions: List[str],
        sample_size: int = 100,
        throttle: Optional[float] = None,
        limit: Optional[int] = None,
    ) -> None:
        self.config = config or {}
        self.logger = logger

        # Crawl roots
        self.base_url = base_url if base_url.endswith("/") else base_url + "/"
        self.base_path = urlparse(self.base_url).path
        if not self.base_path.endswith("/"):
            self.base_path += "/"

        # Canonical (scheme+host and optional canonical root path)
        self.canonical = urlparse(canonical_base_url)
        if not self.canonical.scheme or not self.canonical.netloc:
            raise ValueError("canonical_base_url must include scheme and host")
        self.canonical_root = canonical_base_url if canonical_base_url.endswith("/") else canonical_base_url + "/"

        # Filters & behavior
        self.include_ext = [e.lower() for e in (include_extensions or [])]
        self.sample_size = int(sample_size) if int(sample_size) > 0 else 100

        crawler_cfg = _get_cfg(self.config, "crawler", default={}) or {}
        # Support nested crawler.request.* with fallback to legacy flat keys
        req_cfg = crawler_cfg.get("request", {}) or {}
        self.request_timeout = int(req_cfg.get("timeout_seconds", crawler_cfg.get("request_timeout_seconds", 30)))
        cfg_delay = float(req_cfg.get("request_delay_seconds", crawler_cfg.get("request_delay_seconds", 0.0)))
        self.request_delay = float(throttle) if throttle is not None else cfg_delay
        self.max_retries = int(req_cfg.get("max_retries", crawler_cfg.get("max_retries", 2)))
        # Depth limit
        self.max_depth = int(crawler_cfg.get("max_depth", 8))

        # LIMIT: prefer explicit limit parameter, fallback to config
        if limit is not None:
            self.max_items: Optional[int] = int(limit)
        else:
            max_items_cfg = crawler_cfg.get("max_items")
            self.max_items = int(max_items_cfg) if max_items_cfg is not None else None

        # Subfolders -> allowed first-level subpaths list
        subfolders = crawler_cfg.get("subfolders")
        if isinstance(subfolders, dict) and subfolders:
            subpaths: List[str] = [str(v) for v in subfolders.values()]
        else:
            subpaths = []
        self.subpaths: List[str] = [s if s.endswith("/") else s + "/" for s in subpaths]

        # Output paths
        out_path_cfg = crawler_cfg.get("output_urls_jsonl")
        if isinstance(out_path_cfg, str) and out_path_cfg.strip():
            self.output_file_path = Path(out_path_cfg)
        else:
            dataset_name = str(self.config.get("name") or self.config.get("dataset", "dataset"))
            base_output = Path(_get_cfg(self.config, "dwd_paths", "crawl_data", default="data/dwd/1_crawl_dwd"))
            self.output_file_path = base_output / f"{dataset_name}_urls.jsonl"
        self.output_file_path.parent.mkdir(parents=True, exist_ok=True)
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
                    "base_url": self.base_url,
                    "subpaths": self.subpaths or ["<root>"] ,
                    "output_file": str(self.output_file_path),
                    "throttle": self.request_delay,
                    "limit": self.max_items,
                    "canonical_base_url": urlunparse((self.canonical.scheme, self.canonical.netloc, "", "", "", "")),
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
        """Return (subdirs, files) from a simple Apache-style listing."""
        hrefs = re.findall(r'href=["\']([^"\']+)', html, flags=re.IGNORECASE)
        hrefs = [h for h in hrefs if h not in ("../", "/")]
        subdirs = [h for h in hrefs if h.endswith("/")]
        files = [h for h in hrefs if not h.endswith("/")]
        return subdirs, files

    # --------------------------
    # Helpers
    # --------------------------

    def _emit_file(self, listing_url: str, href: str) -> None:
        if self._stop_flag:
            return
        next_url = urljoin(listing_url, href)
        r = urlparse(next_url)
        if r.path.endswith("/"):
            return  # directories only

        filename = posixpath.basename(r.path)
        if self.include_ext:
            if not any(filename.lower().endswith(ext) for ext in self.include_ext):
                return

        # Dataset-relative path: strip the base_url path prefix
        rel_path_full = r.path
        base_path = self.base_path
        if rel_path_full.startswith(base_path):
            rel_rel = rel_path_full[len(base_path):]
        else:
            # Fallback: try to find the longest common prefix boundary at '/'
            rel_rel = rel_path_full.lstrip("/")
        rel_dir = posixpath.dirname(rel_rel)
        relative_path = (rel_dir + "/") if rel_dir else ""

        # Canonical URL = canonical scheme+host + canonical_root path + rel_rel
        canon_parsed = self.canonical
        final_path = urljoin(self.canonical_root, rel_rel)
        final_r = urlparse(final_path)
        final_url = urlunparse((canon_parsed.scheme, canon_parsed.netloc, final_r.path, "", "", ""))

        # Early de-dupe by final URL
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

    def _crawl_dir(self, url: str, depth: int) -> None:
        if self._stop_flag or depth > self.max_depth:
            return
        resp = self._request(url)
        if not resp:
            return
        subdirs, files = self._parse_listing_html(resp.text)
        for fn in files:
            if self._stop_flag:
                break
            self._emit_file(url, fn)
        for sd in subdirs:
            if self._stop_flag:
                break
            if depth == 0 and self.subpaths and sd not in self.subpaths:
                continue
            self._crawl_dir(urljoin(url, sd), depth + 1)

    # --------------------------
    # Entry
    # --------------------------

    def crawl_repository(self) -> CrawlResult:
        start = time.time()
        errors: List[str] = []

        roots = self.subpaths or [""]
        for sub in roots:
            if self._stop_flag:
                break
            root_url = urljoin(self.base_url, sub)
            try:
                self._crawl_dir(root_url, depth=0)
            except Exception as e:
                errors.append(f"crawl_error:{root_url}:{e}")
                self.logger.error(
                    "Unhandled crawl exception",
                    extra={"component": "CRAWLER", "structured_data": {"root_url": root_url, "error": str(e)}},
                )

        if self.max_items is not None and len(self.url_records) > self.max_items:
            self.url_records = self.url_records[: self.max_items]

        unique_sorted, skipped = _sort_and_dedupe(self.url_records)

        written_full = _atomic_write_jsonl(self.output_file_path, unique_sorted, self.logger)

        sample_n = int(self.sample_size)
        sample_records = unique_sorted[:sample_n]
        sample_path = self.crawl_output_dir / f"{self.dataset_name}_urls_sample{sample_n}.jsonl"
        _ = _atomic_write_jsonl(sample_path, sample_records, self.logger)

        elapsed = time.time() - start
        self.logger.info(
            "Crawl completed",
            extra={
                "component": "CRAWLER",
                "structured_data": {
                    "files_found": len(self.url_records),
                    "files_written": written_full,
                    "files_skipped": skipped,
                    "limit_applied": self.max_items,
                    "requests": self.crawled_count,
                    "elapsed_seconds": round(elapsed, 2),
                    "resolved_listing_url": self.base_url,
                    "output_file": str(self.output_file_path),
                    "sample_file": str(sample_path),
                    "sample_count": sample_n,
                },
            },
        )

        return CrawlResult(
            url_records=unique_sorted,
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
    *,
    base_url: str,
    canonical_base_url: str,
    include_extensions: List[str],
    sample_size: int = 100,
    throttle: Optional[float] = None,
    limit: Optional[int] = None,
) -> CrawlResult:
    """Discover per-file URLs under the dataset root and write JSONL manifests."""
    base_logger = logging.getLogger("pipeline.crawler")
    if isinstance(logger, logging.LoggerAdapter):
        norm_logger = logger.logger
    elif isinstance(logger, logging.Logger):
        norm_logger = logger
    else:
        norm_logger = base_logger

    crawler = DWDRepositoryCrawler(
        config=config,
        logger=norm_logger,
        base_url=base_url,
        canonical_base_url=canonical_base_url,
        include_extensions=include_extensions,
        sample_size=sample_size,
        throttle=throttle,
        limit=limit,
    )
    return crawler.crawl_repository()