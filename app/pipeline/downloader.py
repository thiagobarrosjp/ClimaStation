#!/usr/bin/env python3
"""
ClimaStation Downloader Module — JSONL → files (mirrored DWD tree)

PUBLIC API (preserve):
- load_urls_from_jsonl(urls_file: Path, logger, limit: Optional[int] = None, filter_subfolder: Optional[str] = None) -> List[Dict[str, Any]]
- run_downloader(config: Dict[str, Any], logger, max_downloads: Optional[int] = None, throttle: Optional[float] = None) -> ProcessingResult

Contract highlights:
- Resolve input JSONL via cfg["crawler"]["output_urls_jsonl"]. If missing/not found, log the absolute
  path attempted and return an empty plan (no crash).
- Destination path MUST mirror the DWD repository:
    <downloader.root_dir> / <crawler.root_path> / <relative_path>
  where `crawler.root_path` is treated as a posix-like relative path (leading "/" stripped).
- JSONL schema: {"url": str, "relative_path": str, "filename": str}
- Filtering: if `filter_subfolder` is provided, keep only entries whose `relative_path` startswith it.
- Resume/skip: if destination exists AND size > 0 → log skip and continue.
- Optional `throttle` seconds sleep between candidates.
- Use pathlib.Path for all path joins and directory creation.
- No new dependencies; absolute imports only.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

# Absolute, public utils
from app.utils.file_operations import download_file
from app.utils.enhanced_logger import StructuredLoggerAdapter
import logging


# ------------------------------
# Result type (compatible with runner expectations)
# ------------------------------
@dataclass
class ProcessingResult:
    success: bool
    files_processed: int
    files_failed: int
    output_files: List[Path]
    errors: List[Dict[str, Any]]
    metadata: Dict[str, Any]
    warnings: Optional[List[str]] = None


# ------------------------------
# Public: JSONL loader / planner
# ------------------------------
def load_urls_from_jsonl(
    urls_file: Path,
    logger: logging.Logger | logging.LoggerAdapter,
    limit: Optional[int] = None,
    filter_subfolder: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Read URLs JSONL line-by-line with optional filtering.

    - Skips malformed lines; logs how many were skipped.
    - If `filter_subfolder` is set, include only entries where `relative_path` starts with it (case-sensitive).
    - Apply `limit` *after* filtering.
    """
    total_lines = 0
    filtered_out = 0
    malformed = 0
    kept = 0
    records: List[Dict[str, Any]] = []

    if not isinstance(urls_file, Path):
        urls_file = Path(str(urls_file))

    if not urls_file.exists():
        # Log absolute path per requirement and return empty plan
        try:
            abs_path = urls_file.resolve()
        except Exception:
            abs_path = urls_file
        if isinstance(logger, logging.LoggerAdapter):
            logger.info(
                "URLs JSONL not found",
                extra={
                    "component": "DOWNLOAD",
                    "structured_data": {"urls_file": str(abs_path)},
                },
            )
        else:
            logger.info(
                "URLs JSONL not found",
                extra={
                    "component": "DOWNLOAD",
                    "structured_data": {"urls_file": str(abs_path)},
                },
            )
        return records

    logger.info(
        "Loading URLs from JSONL",
        extra={
            "component": "DOWNLOAD",
            "structured_data": {
                "urls_file": str(urls_file.resolve()),
                "limit": limit,
                "filter_subfolder": filter_subfolder,
            },
        },
    )

    try:
        with urls_file.open("r", encoding="utf-8") as fh:
            for idx, line in enumerate(fh, 1):
                total_lines += 1
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    malformed += 1
                    continue

                url = obj.get("url")
                rel = obj.get("relative_path")
                fn = obj.get("filename")
                if not isinstance(url, str) or not isinstance(rel, str) or not isinstance(fn, str):
                    malformed += 1
                    continue

                # Apply optional subfolder filter
                if filter_subfolder and not rel.startswith(filter_subfolder):
                    filtered_out += 1
                    continue

                records.append({"url": url, "relative_path": rel, "filename": fn})
                kept += 1
                if limit is not None and kept >= limit:
                    break

    except Exception as e:
        if isinstance(logger, logging.LoggerAdapter):
            logger.error(
                "Error reading URLs JSONL",
                extra={"component": "DOWNLOAD", "structured_data": {"urls_file": str(urls_file), "error": str(e)}},
            )
        else:
            logger.error(
                "Error reading URLs JSONL",
                extra={"component": "DOWNLOAD", "structured_data": {"urls_file": str(urls_file), "error": str(e)}},
            )
        # Return whatever we accumulated

    logger.info(
        "Planner summary",
        extra={
            "component": "DOWNLOAD",
            "structured_data": {
                "lines_read": total_lines,
                "malformed": malformed,
                "filtered_out": filtered_out,
                "returned": len(records),
            },
        },
    )
    return records


# ------------------------------
# Internal: single download with retry delegated to utils.download_file
# ------------------------------
def _attempt_download(url: str, destination: Path, config: Dict[str, Any], logger: logging.Logger | logging.LoggerAdapter) -> bool:
    # Ensure parent exists before handing off
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        # Adapt to the expected logger type of download_file (StructuredLoggerAdapter)
        if isinstance(logger, logging.LoggerAdapter):
            base = logger.logger
        elif isinstance(logger, logging.Logger):
            base = logger
        else:
            base = logging.getLogger("pipeline.downloader")
        adapted = StructuredLoggerAdapter(base, {})
        return download_file(url, destination, config, adapted)
    except Exception as e:
        msg = f"Unexpected exception during download: {e}"
        if isinstance(logger, logging.LoggerAdapter):
            logger.error(msg)
        else:
            logger.error(msg)
        return False


# ------------------------------
# Internal: config helpers
# ------------------------------
def _get_cfg(cfg: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    cur: Any = cfg
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _get_download_root(cfg: Dict[str, Any]) -> Path:
    """Root folder where downloads are mirrored. NEW key: downloader.root_dir; fallbacks supported."""
    root = _get_cfg(cfg, "downloader", "root_dir") or _get_cfg(cfg, "dwd_paths", "download_root") or "data/dwd/2_downloaded_files"
    return Path(str(root))


def _get_dwd_root(cfg: Dict[str, Any]) -> Path:
    """Dataset root (relative, posix-like) coming from crawler.root_path (or legacy fallbacks)."""
    root_path = _get_cfg(cfg, "crawler", "root_path")
    if root_path is None:
        root_path = cfg.get("base_path") or _get_cfg(cfg, "source", "base_path") or _get_cfg(cfg, "crawler", "dataset_path") or ""
    root_path = str(root_path).lstrip("/")
    return Path(root_path)


# ------------------------------
# Public: runner entry
# ------------------------------
def run_downloader(
    config: Dict[str, Any],
    logger: logging.Logger | logging.LoggerAdapter,
    max_downloads: Optional[int] = None,
    throttle: Optional[float] = None,
) -> ProcessingResult:
    # Use the centralized named logger that propagates to root
    logger = logging.getLogger("pipeline.downloader")
    start_ts = time.time()

    dataset = str(config.get("name") or config.get("dataset") or "")
    root_dir = _get_download_root(config)
    dwd_root = _get_dwd_root(config)

    logger.info(
        "Downloader start",
        extra={
            "component": "DOWNLOAD",
            "structured_data": {
                "dataset": dataset or None,
                "download_root": str((root_dir / dwd_root).resolve()),
                "max_downloads": max_downloads,
                "throttle": throttle,
            },
        },
    )

    # Planner: load candidates from crawler output
    urls_jsonl = _get_cfg(config, "crawler", "output_urls_jsonl")
    url_path = Path(str(urls_jsonl)) if urls_jsonl else Path("missing.jsonl")
    candidates = load_urls_from_jsonl(url_path, logger, limit=max_downloads)

    downloaded_ok = 0
    skipped_existing = 0
    failed = 0
    attempted = 0
    warnings: List[str] = []
    errors: List[Dict[str, Any]] = []

    output_files: List[Path] = []

    total_candidates = len(candidates)

    for idx, rec in enumerate(candidates, 1):
        url = rec["url"]
        rel = Path(rec["relative_path"])  # safe-join component

        # Destination path mirrors DWD tree: <root_dir>/<dwd_root>/<relative_path>
        dest = root_dir / dwd_root / rel

        # Resume/skip: existing file with size > 0
        if dest.exists():
            try:
                if dest.stat().st_size > 0:
                    skipped_existing += 1
                    attempted += 1
                    # Required plain-text skip line with destination path
                    logger.info("Skip existing file: %s", str(dest))
                    if throttle and throttle > 0:
                        time.sleep(throttle)
                    continue
            except Exception:
                # If stat() fails, fall through to attempt download
                pass

        # Required plain-text start line with url and destination
        logger.info("Starting download: %s -> %s", url, str(dest))

        attempted += 1
        ok = _attempt_download(url, dest, config, logger)
        if ok:
            downloaded_ok += 1
            output_files.append(dest)
            # Required plain-text completion line with destination
            logger.info("Download completed: %s", str(dest))
        else:
            failed += 1
            errors.append({"url": url, "destination": str(dest), "reason": "download_failed"})

        if throttle and throttle > 0:
            time.sleep(throttle)

    elapsed = time.time() - start_ts

    success = failed == 0
    return ProcessingResult(
        success=success,
        files_processed=downloaded_ok,
        files_failed=failed,
        output_files=output_files,
        errors=errors,
        metadata={
            "elapsed_time": elapsed,
            "files_skipped": skipped_existing,
            "attempted": attempted,
            "download_root": str((root_dir / dwd_root).resolve()),
            "success_rate": round(100.0 * (downloaded_ok / max(total_candidates, 1)), 1),
        },
        warnings=warnings or None,
    )
