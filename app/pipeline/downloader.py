#!/usr/bin/env python3
"""
ClimaStation Downloader Module

SCRIPT IDENTIFICATION:
    Name: downloader.py
    Purpose: Handles the downloading of DWD ZIP files from the list provided by the crawler,
             with support for custom User-Agent and optional throttling between files.
    Author: ClimaStation Pipeline System
    Version: 1.1.0
    Created: 2025-01-29
    Updated: 2025-07-30

PURPOSE:
    Orchestrate filtered, limited downloads of .zip archives discovered by the crawler.
    Applies subfolder filtering, retry logic, custom User-Agent header, preserves
    folder hierarchy under data/dwd/2_downloaded_files/, and supports optional
    throttling between file downloads.

RESPONSIBILITIES:
    - Stream the `dwd_urls.jsonl` for the given dataset & subfolder
    - Skip already-downloaded files
    - Retry downloads up to 3× with exponential backoff
    - Preserve relative directory structure
    - Log successes, skips, and failures
    - Honor an optional max-downloads limit
    - Use a custom User-Agent header on all HTTP requests
    - Throttle between file downloads if requested

USAGE:
    from app.pipeline.downloader import run_downloader
    result = run_downloader(config, logger, max_downloads=50, throttle=0.3)

COMPONENT CODE: DOWNLOAD
"""

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from dataclasses import dataclass
from urllib.parse import urljoin

from app.utils.http_headers import default_headers
from app.utils.enhanced_logger import StructuredLoggerAdapter
from app.utils.file_operations import download_file
from app.utils.config_manager import ConfigurationError

@dataclass
class ProcessingResult:
    success: bool
    files_processed: int
    files_failed: int
    output_files: List[Path]
    errors: List[str]
    metadata: Dict[str, Any]
    warnings: Optional[List[str]] = None

# Root where downloads are stored
DOWNLOAD_ROOT = Path("data/dwd/2_downloaded_files")

def load_urls_from_jsonl(
    urls_file: Path,
    logger: StructuredLoggerAdapter,
    limit: Optional[int] = None,
    filter_subfolder: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Stream-read JSONL, filter by subfolder in `relative_path`, return dicts.
    """
    records: List[Dict[str, Any]] = []
    if not urls_file.exists():
        logger.error("URLs file not found", extra={
            "component": "DOWNLOAD",
            "structured_data": {"urls_file": str(urls_file)}
        })
        return records

    logger.info("Loading URLs from JSONL", extra={
        "component": "DOWNLOAD",
        "structured_data": {
            "urls_file": str(urls_file),
            "limit": limit,
            "filter_subfolder": filter_subfolder
        }
    })

    with urls_file.open("r", encoding="utf-8") as f:
        for idx, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                rel = rec.get("relative_path", "")
                if filter_subfolder and filter_subfolder not in rel:
                    continue
                records.append(rec)
                if limit and len(records) >= limit:
                    logger.info(f"Reached download limit of {limit}", extra={
                        "component": "DOWNLOAD",
                        "structured_data": {"limit": limit}
                    })
                    break
            except json.JSONDecodeError as e:
                logger.warning(f"Invalid JSON line #{idx}", extra={
                    "component": "DOWNLOAD",
                    "structured_data": {"line_number": idx, "error": str(e)}
                })

    logger.info(f"Loaded {len(records)} URLs to download", extra={
        "component": "DOWNLOAD",
        "structured_data": {
            "total": len(records),
            "limit_applied": limit is not None
        }
    })
    return records

def download_with_retry(
    url: str,
    destination: Path,
    config: Dict[str, Any],
    logger: StructuredLoggerAdapter
) -> bool:
    """
    Download a file with exponential-backoff retry logic using the custom User-Agent.
    """
    max_retries = config.get('downloader', {}).get('max_retries', 3)
    base_delay = config.get('downloader', {}).get('retry_delay_seconds', 1)
    headers = default_headers()

    for attempt in range(1, max_retries + 2):
        try:
            logger.info(f"Attempt {attempt}/{max_retries+1}", extra={
                "component": "DOWNLOAD",
                "structured_data": {
                    "url": url,
                    "destination": str(destination),
                    "attempt": attempt
                }
            })
            destination.parent.mkdir(parents=True, exist_ok=True)
            success = download_file(
                url,
                destination,
                config,
                logger
            )
            if success:
                return True
            logger.warning("Download failed, will retry", extra={
                "component": "DOWNLOAD",
                "structured_data": {"url": url, "attempt": attempt}
            })
        except Exception as e:
            logger.warning(f"Error on attempt {attempt}: {e}", extra={
                "component": "DOWNLOAD",
                "structured_data": {"url": url, "error": str(e)}
            })
        if attempt <= max_retries:
            delay = base_delay * (2 ** (attempt - 1))
            time.sleep(delay)

    logger.error("All retry attempts failed", extra={
        "component": "DOWNLOAD",
        "structured_data": {"url": url}
    })
    return False

def run_downloader(
    config: Dict[str, Any],
    logger: StructuredLoggerAdapter,
    max_downloads: Optional[int] = None,
    throttle: Optional[float] = None
) -> ProcessingResult:
    """
    Orchestrate the filtered, limited download of ZIP files,
    applying optional throttle between files.
    """
    start = time.time()
    ds_name = config.get('name')
    if not ds_name:
        raise ConfigurationError("Missing dataset name in config")

    # Inject custom User-Agent into config for downstream helpers
    ua = default_headers()["User-Agent"]
    config.setdefault('downloader', {})['user_agent'] = ua
    logger.info("Using custom User-Agent for downloads", extra={
        "component": "DOWNLOAD",
        "structured_data": {"user_agent": ua}
    })

    # Locate crawler output
    crawl_base = Path(config['dwd_paths']['crawl_data'])
    urls_file = crawl_base / ds_name / "dwd_urls.jsonl"
    subfolder = config.get('downloader', {}).get('subfolder')

    logger.info("Starting run_downloader", extra={
        "component": "DOWNLOAD",
        "structured_data": {
            "dataset": ds_name,
            "urls_file": str(urls_file),
            "filter_subfolder": subfolder,
            "max_downloads": max_downloads,
            "throttle": throttle
        }
    })

    records = load_urls_from_jsonl(urls_file, logger, max_downloads, subfolder)
    files_processed = 0
    files_failed = 0
    output_files: List[Path] = []
    errors: List[str] = []
    warnings: List[str] = []

    if not records:
        msg = "No files to download after filtering"
        logger.error(msg, extra={"component": "DOWNLOAD"})
        return ProcessingResult(False, 0, 0, [], [msg], {'elapsed_time': time.time() - start})

    for idx, rec in enumerate(records, 1):
        url = rec["url"]
        rel = rec["relative_path"]
        fname = rec["filename"]
        dest = DOWNLOAD_ROOT / Path(rel).parent / fname

        # Skip if already exists
        if dest.exists():
            logger.info("Skipping existing file", extra={
                "component": "DOWNLOAD",
                "structured_data": {"url": url, "destination": str(dest)}
            })
            continue

        logger.info(f"Downloading {idx}/{len(records)}", extra={
            "component": "DOWNLOAD",
            "structured_data": {"url": url}
        })

        ok = download_with_retry(url, dest, config, logger)
        if ok:
            files_processed += 1
            output_files.append(dest)
        else:
            files_failed += 1
            errors.append(url)

        # Throttle between separate files
        if throttle:
            logger.info(f"Throttling {throttle}s before next file", extra={
                "component": "DOWNLOAD",
                "structured_data": {"throttle_s": throttle}
            })
            time.sleep(throttle)

    elapsed = time.time() - start
    total = len(records)
    skipped = total - files_processed - files_failed
    success = files_failed == 0 and files_processed > 0
    success_rate = round(100 * files_processed / max(total, 1), 1)

    logger.info("Download run complete", extra={
        "component": "DOWNLOAD",
        "structured_data": {
            "total": total,
            "processed": files_processed,
            "failed": files_failed,
            "skipped": skipped,
            "elapsed_time": elapsed,
            "success_rate": success_rate,
            "download_root": str(DOWNLOAD_ROOT)
        }
    })

    if skipped:
        warnings.append(f"{skipped} files were skipped")
    if files_failed:
        warnings.append(f"{files_failed} files failed")

    return ProcessingResult(
        success=success,
        files_processed=files_processed,
        files_failed=files_failed,
        output_files=output_files,
        errors=errors,
        metadata={
            'elapsed_time': elapsed,
            'files_skipped': skipped,
            'download_root': str(DOWNLOAD_ROOT),
            'success_rate': success_rate
        },
        warnings=warnings or None
    )
