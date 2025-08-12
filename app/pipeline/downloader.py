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
from app.utils.enhanced_logger import ComponentLogger


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
# Helpers
# ------------------------------

def _cfg_get(d: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    cur: Any = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _resolve_urls_file_from_cfg(cfg: Dict[str, Any]) -> Path:
    path = _cfg_get(cfg, "crawler", "output_urls_jsonl")
    if isinstance(path, str) and path:
        return Path(path)
    # Fall back to a sensible default location if key missing — but still try to be deterministic
    # NOTE: This path may not exist; callers/logging must handle this gracefully.
    dataset = str(cfg.get("name") or cfg.get("dataset") or "dataset")
    return Path("data/dwd/1_crawl_dwd") / f"{dataset}_urls.jsonl"


def _get_download_root(cfg: Dict[str, Any]) -> Path:
    root = _cfg_get(cfg, "downloader", "root_dir")
    if isinstance(root, str) and root:
        return Path(root)
    # Defensive fallback
    return Path("data/dwd/2_downloaded_files/")


def _get_dwd_root(cfg: Dict[str, Any]) -> Path:
    """Return normalized DWD root path from cfg["crawler"]["root_path"].

    Treat as posix-like relative; strip any leading '/'.
    """
    raw = _cfg_get(cfg, "crawler", "root_path")
    if isinstance(raw, str) and raw:
        return Path(str(raw).lstrip("/"))
    return Path("")


# ------------------------------
# Public: JSONL loader / planner
# ------------------------------

def load_urls_from_jsonl(
    urls_file: Path,
    logger: ComponentLogger,
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
        logger.error(
            "URLs file not found; skipping download planning",
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
                s = line.strip()
                if not s:
                    continue
                try:
                    obj = json.loads(s)
                except json.JSONDecodeError:
                    malformed += 1
                    continue

                url = obj.get("url")
                rel = obj.get("relative_path")
                fn = obj.get("filename")
                if not (isinstance(url, str) and isinstance(rel, str) and isinstance(fn, str)):
                    filtered_out += 1
                    continue

                if filter_subfolder:
                    if not rel.startswith(filter_subfolder):
                        filtered_out += 1
                        continue

                records.append({"url": url, "relative_path": rel, "filename": fn})
                kept += 1
                if limit is not None and kept >= max(0, int(limit)):
                    break
    except Exception as e:
        logger.error(
            "Error reading URLs file",
            extra={
                "component": "DOWNLOAD",
                "structured_data": {"urls_file": str(urls_file.resolve()), "error": str(e)},
            },
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

def _attempt_download(url: str, destination: Path, config: Dict[str, Any], logger: ComponentLogger) -> bool:
    # Ensure parent exists before handing off
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        return download_file(url, destination, config, logger)
    except Exception as e:
        logger.error(
            "Unexpected exception during download",
            extra={
                "component": "DOWNLOAD",
                "structured_data": {"url": url, "destination": str(destination), "error": str(e)},
            },
        )
        return False


# ------------------------------
# Public: main entry
# ------------------------------

def run_downloader(
    config: Dict[str, Any],
    logger: ComponentLogger,
    max_downloads: Optional[int] = None,
    throttle: Optional[float] = None,
) -> ProcessingResult:
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
                "limit": max_downloads,
                "throttle": throttle,
                "root_dir": str(root_dir),
                "dwd_root": str(dwd_root),
            },
        },
    )

    urls_path = _resolve_urls_file_from_cfg(config)
    # Accept optional subfolder directive injected by runner
    subfolder = _cfg_get(config, "downloader", "subfolder")

    # Build candidate list (limit applies after filtering inside loader)
    candidates = load_urls_from_jsonl(urls_path, logger, limit=max_downloads, filter_subfolder=subfolder)

    # Early exit if nothing to do
    if not candidates:
        elapsed = time.time() - start_ts
        logger.info(
            "No candidates to download",
            extra={
                "component": "DOWNLOAD",
                "structured_data": {"urls_file": str(urls_path.resolve()), "elapsed_seconds": round(elapsed, 2)},
            },
        )
        return ProcessingResult(
            success=False,
            files_processed=0,
            files_failed=0,
            output_files=[],
            errors=[{"reason": "no_candidates", "urls_file": str(urls_path.resolve())}],
            metadata={"elapsed_time": elapsed, "files_skipped": 0, "attempted": 0},
        )

    attempted = 0
    downloaded_ok = 0
    skipped_existing = 0
    failed = 0
    output_files: List[Path] = []
    errors: List[Dict[str, Any]] = []

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
                    logger.info(
                        "Skip existing file",
                        extra={
                            "component": "DOWNLOAD",
                            "structured_data": {"url": url, "destination": str(dest)},
                        },
                    )
                    if throttle and throttle > 0:
                        time.sleep(throttle)
                    continue
            except Exception:
                # If stat() fails, fall through to attempt download
                pass

        logger.info(
            "Downloading",
            extra={
                "component": "DOWNLOAD",
                "structured_data": {
                    "index": idx,
                    "total": total_candidates,
                    "url": url,
                    "destination": str(dest),
                },
            },
        )

        attempted += 1
        ok = _attempt_download(url, dest, config, logger)
        if ok:
            downloaded_ok += 1
            output_files.append(dest)
        else:
            failed += 1
            errors.append({"url": url, "destination": str(dest), "reason": "download_failed"})

        if throttle and throttle > 0:
            time.sleep(throttle)

    elapsed = time.time() - start_ts
    success = failed == 0 and downloaded_ok > 0

    logger.info(
        "Download summary",
        extra={
            "component": "DOWNLOAD",
            "structured_data": {
                "urls_file": str(urls_path.resolve()),
                "download_root": str((root_dir / dwd_root).resolve()),
                "total_candidates": total_candidates,
                "attempted": attempted,
                "downloaded_ok": downloaded_ok,
                "skipped_existing": skipped_existing,
                "failed": failed,
                "elapsed_seconds": round(elapsed, 2),
            },
        },
    )

    warnings: List[str] = []
    if skipped_existing:
        warnings.append(f"{skipped_existing} file(s) skipped (already present)")
    if failed:
        warnings.append(f"{failed} file(s) failed to download")

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
