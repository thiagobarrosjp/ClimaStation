#!/usr/bin/env python3
"""
ClimaStation Downloader Module (corrected)

PUBLIC API (stable):
- load_urls_from_jsonl(urls_file: Path, logger: ComponentLogger, limit: Optional[int] = None, filter_subfolder: Optional[str] = None) -> List[Dict[str, Any]]
- run_downloader(config: Dict[str, Any], logger: ComponentLogger, max_downloads: Optional[int] = None, throttle: Optional[float] = None) -> ProcessingResult

Notes:
- Uses absolute imports only. No third‑party deps.
- Does not import runtime-only reference modules.
- Graceful error handling; no unexpected raises.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.utils.http_headers import default_headers
from app.utils.file_operations import download_file

# Use the current logger class name at runtime, but keep the public type alias
from app.utils.enhanced_logger import ComponentLogger
# ------------------------------
# Result type (reference-compatible)
# ------------------------------

@dataclass
class ProcessingResult:
    success: bool
    files_processed: int
    files_failed: int
    output_files: List[Path]
    errors: List[str]
    metadata: Dict[str, Any]
    warnings: Optional[List[str]] = None


# ------------------------------
# Helpers
# ------------------------------

def _derive_crawl_urls_path(config: Dict[str, Any]) -> Path:
    """Resolve crawler JSONL path the same way the runner expects.

    Prefers canonical `dwd_{dataset}_urls.jsonl` and falls back to legacy name.
    """
    dwd_paths = config.get("dwd_paths") or {}
    crawl_root = Path(dwd_paths.get("crawl_data", "data/dwd/1_crawl_dwd"))
    dataset = str(config.get("name") or config.get("dataset", "dataset"))
    canonical = crawl_root / dataset / f"dwd_{dataset}_urls.jsonl"
    legacy = crawl_root / dataset / "dwd_urls.jsonl"
    return canonical if canonical.exists() else (legacy if legacy.exists() else canonical)


def _get_download_root(config: Dict[str, Any]) -> Path:
    dwd_paths = config.get("dwd_paths") or {}
    return Path(dwd_paths.get("download_data", "data/dwd/2_downloaded_files"))


# ------------------------------
# Public: planner
# ------------------------------

def load_urls_from_jsonl(
    urls_file: Path,
    logger: ComponentLogger,
    limit: Optional[int] = None,
    filter_subfolder: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Stream-read JSONL, optionally filter by leading subfolder, return dicts.

    - Skips malformed lines but continues.
    - Filter includes only records whose `relative_path` **starts with** `filter_subfolder`.
    - Truncates to `limit` after filtering.
    """
    total_lines = 0
    kept = 0
    filtered_out = 0
    records: List[Dict[str, Any]] = []

    if not urls_file.exists():
        logger.error("URLs file not found", extra={
            "component": "DOWNLOAD",
            "structured_data": {"urls_file": str(urls_file)},
        })
        return records

    logger.info("Loading URLs from JSONL", extra={
        "component": "DOWNLOAD",
        "structured_data": {
            "urls_file": str(urls_file),
            "limit": limit,
            "filter_subfolder": filter_subfolder,
        },
    })

    try:
        with urls_file.open("r", encoding="utf-8") as fh:
            for idx, line in enumerate(fh, 1):
                total_lines += 1
                s = line.strip()
                if not s:
                    continue
                try:
                    obj = json.loads(s)
                except json.JSONDecodeError as e:
                    logger.warning("Invalid JSONL line; skipping", extra={
                        "component": "DOWNLOAD",
                        "structured_data": {"line": idx, "error": str(e)},
                    })
                    continue

                # Minimal validation
                url = obj.get("url")
                rel = obj.get("relative_path")
                fn = obj.get("filename")
                if not (isinstance(url, str) and isinstance(rel, str) and isinstance(fn, str)):
                    filtered_out += 1
                    continue

                if filter_subfolder is not None and filter_subfolder != "":
                    if not rel.startswith(filter_subfolder):
                        filtered_out += 1
                        continue

                records.append({"url": url, "relative_path": rel, "filename": fn})
                kept += 1
                if limit is not None and kept >= max(0, int(limit)):
                    break
    except Exception as e:
        logger.error("Error reading URLs file", extra={
            "component": "DOWNLOAD",
            "structured_data": {"urls_file": str(urls_file), "error": str(e)},
        })
        # Return what we have so far

    logger.info("Planner summary", extra={
        "component": "DOWNLOAD",
        "structured_data": {
            "lines_read": total_lines,
            "kept": len(records),
            "filtered_out": filtered_out,
            "returned": len(records),
        },
    })
    return records


# ------------------------------
# Internal: per-file retries (delegates to utils.download_file)
# ------------------------------

def _download_with_retry(
    url: str,
    destination: Path,
    config: Dict[str, Any],
    logger: ComponentLogger,
) -> bool:
    max_retries = int((config.get("downloader") or {}).get("max_retries", 3))
    base_delay = float((config.get("downloader") or {}).get("retry_delay_seconds", 1))

    # Ensure parent exists
    destination.parent.mkdir(parents=True, exist_ok=True)

    for attempt in range(1, max_retries + 2):
        try:
            logger.info("Download attempt", extra={
                "component": "DOWNLOAD",
                "structured_data": {"url": url, "destination": str(destination), "attempt": attempt},
            })
            ok = download_file(url, destination, config, logger)
            if ok:
                return True
            # download_file returned False → retryable
            logger.warning("Download failed; will retry if attempts remain", extra={
                "component": "DOWNLOAD",
                "structured_data": {"url": url, "attempt": attempt},
            })
        except Exception as e:
            logger.warning("Exception during download attempt", extra={
                "component": "DOWNLOAD",
                "structured_data": {"url": url, "attempt": attempt, "error": str(e)},
            })
        if attempt <= max_retries:
            time.sleep(base_delay * (2 ** (attempt - 1)))
    logger.error("All retry attempts exhausted", extra={
        "component": "DOWNLOAD",
        "structured_data": {"url": url},
    })
    return False


# ------------------------------
# Public: runner entry
# ------------------------------

def run_downloader(
    config: Dict[str, Any],
    logger: ComponentLogger,
    max_downloads: Optional[int] = None,
    throttle: Optional[float] = None,
) -> ProcessingResult:
    """Run filtered, resumable downloads; return a ProcessingResult.

    - Resolves URLs JSONL path in the same way as the runner.
    - Skips files that already exist (assumes complete when present).
    - Respects optional throttle seconds between files.
    - Never raises for expected failures.
    """
    start = time.time()

    dataset = str(config.get("name") or config.get("dataset", ""))
    if not dataset:
        msg = "Missing dataset name in config"
        logger.error(msg, extra={"component": "DOWNLOAD"})
        return ProcessingResult(
            success=False,
            files_processed=0,
            files_failed=0,
            output_files=[],
            errors=[msg],
            metadata={"elapsed_time": 0.0},
        )

    # Hint: propagate UA for transparency (downstream download_file already sets headers via default_headers)
    ua = default_headers().get("User-Agent", "")
    logger.info("Downloader starting", extra={
        "component": "DOWNLOAD",
        "structured_data": {"dataset": dataset, "user_agent": ua, "limit": max_downloads, "throttle": throttle},
    })

    urls_path = _derive_crawl_urls_path(config)
    subfolder = (config.get("downloader") or {}).get("subfolder")

    candidates = load_urls_from_jsonl(urls_path, logger, limit=max_downloads, filter_subfolder=subfolder)

    files_processed = 0
    files_failed = 0
    skipped_existing = 0
    output_files: List[Path] = []
    errors: List[str] = []

    if not candidates:
        elapsed = time.time() - start
        logger.info("No candidates to download", extra={
            "component": "DOWNLOAD",
            "structured_data": {"elapsed_time": round(elapsed, 2), "urls_file": str(urls_path)},
        })
        return ProcessingResult(
            success=False,
            files_processed=0,
            files_failed=0,
            output_files=[],
            errors=["No files to download after filtering"],
            metadata={"elapsed_time": elapsed, "files_skipped": 0},
        )

    download_root = _get_download_root(config)

    for idx, rec in enumerate(candidates, 1):
        url = rec["url"]
        rel = rec["relative_path"]
        fname = rec["filename"]
        dest = download_root / Path(rel).parent / fname

        # Check existing
        if dest.exists():
            skipped_existing += 1
            logger.info("Skipping existing file", extra={
                "component": "DOWNLOAD",
                "structured_data": {"url": url, "destination": str(dest)},
            })
        else:
            logger.info("Downloading file", extra={
                "component": "DOWNLOAD",
                "structured_data": {
                    "index": idx,
                    "total": len(candidates),
                    "url": url,
                    "destination": str(dest),
                },
            })
            ok = _download_with_retry(url, dest, config, logger)
            if ok:
                files_processed += 1
                output_files.append(dest)
            else:
                files_failed += 1
                errors.append(url)

            if throttle and throttle > 0:
                time.sleep(throttle)

    elapsed = time.time() - start
    total = len(candidates)
    success = files_failed == 0 and files_processed > 0
    success_rate = round(100 * files_processed / max(total, 1), 1)

    logger.info("Download summary", extra={
        "component": "DOWNLOAD",
        "structured_data": {
            "total": total,
            "processed": files_processed,
            "failed": files_failed,
            "skipped": skipped_existing,
            "elapsed_seconds": round(elapsed, 2),
            "download_root": str(download_root),
            "success_rate": success_rate,
        },
    })

    warnings: List[str] = []
    if skipped_existing:
        warnings.append(f"{skipped_existing} files were skipped (already present)")
    if files_failed:
        warnings.append(f"{files_failed} files failed to download")

    return ProcessingResult(
        success=success,
        files_processed=files_processed,
        files_failed=files_failed,
        output_files=output_files,
        errors=errors,
        metadata={
            "elapsed_time": elapsed,
            "files_skipped": skipped_existing,
            "download_root": str(download_root),
            "success_rate": success_rate,
        },
        warnings=warnings or None,
    )
