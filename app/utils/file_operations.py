"""
ClimaStation File Operations Utilities

SCRIPT IDENTIFICATION: DWD10TAH3F (File Operations)

PURPOSE:
Robust file handling utilities for downloading, extracting, and validating DWD
climate data files. Provides standardized file operations with proper error
handling, retry logic, and comprehensive logging integration.

PUBLIC API (stable):
- download_file(url: str, destination: Path, config: Dict[str, Any], logger: StructuredLoggerAdapter) -> bool
- extract_zip(zip_path: Path, extract_to: Path, config: Dict[str, Any], logger: StructuredLoggerAdapter) -> List[Path]
- validate_file_structure(file_path: Path, expected_columns: List[str], config: Dict[str, Any], logger: StructuredLoggerAdapter) -> bool

NOTES:
- Uses absolute imports consistent with the project layout.
- No third‑party deps beyond stdlib + requests.

Minimal smoke tests (not executed by default):
"""
# Smoke-test snippet (kept in a guarded block for reference only)
#
# if __name__ == "__main__":
#     from app.utils.enhanced_logger import get_logger
#     import tempfile, csv, zipfile
#     from pathlib import Path
#
#     logger = get_logger("SMOKE")
#     cfg = {}
#
#     # validate_file_structure: create tiny semicolon CSV
#     tmpdir = Path(tempfile.mkdtemp())
#     csv_path = tmpdir / "sample.csv"
#     with csv_path.open("w", encoding="utf-8") as f:
#         f.write("A;B\n1;2\n")
#     assert validate_file_structure(csv_path, ["A", "B"], cfg, logger) is True
#
#     # extract_zip: zip a small file
#     zip_path = tmpdir / "t.zip"
#     with zipfile.ZipFile(zip_path, "w") as z:
#         z.writestr("foo.txt", "bar")
#     out_dir = tmpdir / "out"
#     extracted = extract_zip(zip_path, out_dir, cfg, logger)
#     assert extracted and (out_dir / "foo.txt").exists()
#
#     # download_file: simulate skip-if-exists (no network)
#     # We simulate by creating destination and setting a fake Content-Length check to be skipped
#     # In practice, call download_file() with a real URL. Here we ensure path creation/skip branch doesn't error.
#     dest = tmpdir / "already_there.bin"
#     dest.write_bytes(b"x" * 10)
#     # Direct call would try network; this snippet is illustrative only.

from __future__ import annotations

import csv
import os
import shutil
import time
import zipfile
from pathlib import Path
from typing import Any, Dict, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Compatibility import: support both legacy StructuredLoggerAdapter and new ComponentLogger
try:
    from app.utils.enhanced_logger import StructuredLoggerAdapter  # type: ignore
except Exception:  # pragma: no cover - fallback for newer logger module
    try:
        from app.utils.enhanced_logger import ComponentLogger as StructuredLoggerAdapter  # type: ignore
    except Exception:
        # Last-resort fallback to stdlib logger to avoid import errors in type-checkers/IDEs
        from logging import Logger as StructuredLoggerAdapter  # type: ignore
from app.utils.http_headers import default_headers


# ------------------------------
# Helpers
# ------------------------------

def _get_timeout_seconds(config: Dict[str, Any]) -> int:
    # Prefer explicit network timeout; fall back to a sane default
    try:
        network = config.get("network", {})
        if isinstance(network.get("timeout_seconds"), (int, float)):
            return int(network["timeout_seconds"]) or 60
    except Exception:
        pass
    # Fallback to processing timeout (minutes) if present; otherwise 60s
    try:
        proc = config.get("processing", {})
        if isinstance(proc.get("worker_timeout_minutes"), (int, float)):
            return max(30, int(proc["worker_timeout_minutes"] * 60))
    except Exception:
        pass
    return 60


def _get_retry_config(config: Dict[str, Any]) -> tuple[int, int]:
    fh = config.get("failure_handling", {}) if isinstance(config, dict) else {}
    max_retries = int(fh.get("max_retries", 3))
    retry_delay = int(fh.get("retry_delay_seconds", 5))
    return max_retries, retry_delay


# ------------------------------
# Public API
# ------------------------------

def download_file(url: str, destination: Path, config: Dict[str, Any], logger: StructuredLoggerAdapter) -> bool:
    """Download a file with retries, resume support, and atomic write.

    - Uses default_headers() for HTTP requests.
    - Creates parent directories if needed.
    - Streams to a temporary ".part" file then atomically renames.
    - If local file exists and appears complete (via HEAD content-length), it is skipped.
    - Returns True/False; expected failures are logged without raising.
    """
    try:
        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = destination.with_suffix(destination.suffix + ".part")

        timeout_seconds = _get_timeout_seconds(config)
        max_retries, retry_delay = _get_retry_config(config)

        session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        base_headers = {**default_headers(), "Accept": "*/*"}

        # Try a HEAD to determine remote size for skip/validation
        remote_size: int | None = None
        try:
            head = session.head(url, headers=base_headers, timeout=timeout_seconds, allow_redirects=True)
            if head.ok:
                cl = head.headers.get("content-length")
                if cl and cl.isdigit():
                    remote_size = int(cl)
        except Exception as e:
            logger.debug("HEAD request failed; continuing without size check", extra={
                "component": "DOWNLOAD",
                "structured_data": {"url": url, "error": str(e)},
            })

        # Skip if destination exists and size matches remote_size
        if destination.exists() and remote_size is not None and destination.stat().st_size == remote_size:
            logger.info("Download skipped (already complete)", extra={
                "component": "DOWNLOAD",
                "structured_data": {
                    "url": url,
                    "destination": str(destination),
                    "size_bytes": remote_size,
                    "reason": "size_match",
                },
            })
            return True

        logger.info("Starting download", extra={
            "component": "DOWNLOAD",
            "structured_data": {
                "url": url,
                "destination": str(destination),
                "timeout_seconds": timeout_seconds,
                "max_retries": max_retries,
                "remote_size": remote_size,
            },
        })

        # Retry loop for GET
        for attempt in range(1, max_retries + 2):
            try:
                # Determine resume offset from temporary file if present
                resume_from = tmp_path.stat().st_size if tmp_path.exists() else 0
                headers = dict(base_headers)
                mode = "ab" if resume_from > 0 else "wb"
                if resume_from > 0:
                    headers["Range"] = f"bytes={resume_from}-"

                resp = session.get(url, headers=headers, timeout=timeout_seconds, stream=True)
                # If server ignored our Range and sent 200, start over
                if resp.status_code == 200 and "Range" in headers:
                    resume_from = 0
                    mode = "wb"

                resp.raise_for_status()

                start_time = time.time()
                bytes_written = resume_from

                with tmp_path.open(mode) as fh:
                    for chunk in resp.iter_content(chunk_size=1024 * 64):  # 64KB chunks
                        if chunk:
                            fh.write(chunk)
                            bytes_written += len(chunk)

                # If we know expected size, verify
                final_size = tmp_path.stat().st_size
                if remote_size is not None and final_size != remote_size:
                    raise IOError(f"size_mismatch: got={final_size} expected={remote_size}")

                # Basic ZIP integrity check (only when destination looks like zip)
                if destination.suffix.lower() == ".zip":
                    try:
                        with zipfile.ZipFile(tmp_path, "r") as zf:
                            bad = zf.testzip()
                            if bad is not None:
                                raise zipfile.BadZipFile(f"bad member: {bad}")
                    except zipfile.BadZipFile as e:
                        raise IOError(f"zip_corrupt: {e}")

                os.replace(tmp_path, destination)

                elapsed = time.time() - start_time
                logger.info("Download completed", extra={
                    "component": "DOWNLOAD",
                    "structured_data": {
                        "destination": str(destination),
                        "size_bytes": destination.stat().st_size,
                        "duration_seconds": round(elapsed, 2),
                        "attempt": attempt,
                    },
                })
                return True

            except Exception as e:
                # On failure, optionally wait and retry
                will_retry = attempt <= max_retries
                level = logger.warning if will_retry else logger.error
                level(
                    "Download attempt failed",
                    extra={
                        "component": "DOWNLOAD",
                        "structured_data": {
                            "url": url,
                            "destination": str(destination),
                            "attempt": attempt,
                            "max_retries": max_retries,
                            "error": str(e),
                        },
                    },
                )
                if will_retry:
                    time.sleep(retry_delay)
                else:
                    # Cleanup temp file on terminal failure
                    try:
                        if tmp_path.exists():
                            tmp_path.unlink()
                    except Exception:
                        pass
                    return False
        return False
    except Exception as e:
        logger.error("Download failed with unexpected error", extra={
            "component": "DOWNLOAD",
            "structured_data": {"url": url, "destination": str(destination), "error": str(e)},
        })
        # Best-effort cleanup of temp file
        try:
            tmp = destination.with_suffix(destination.suffix + ".part")
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass
        return False


def extract_zip(zip_path: Path, extract_to: Path, config: Dict[str, Any], logger: StructuredLoggerAdapter) -> List[Path]:
    """Extract a ZIP archive using stdlib zipfile.

    - Creates `extract_to` if missing.
    - Returns a list of extracted file paths.
    - On error, logs and returns an empty list (does not raise).
    """
    extracted: List[Path] = []
    try:
        zip_path = Path(zip_path)
        extract_to = Path(extract_to)

        if not zip_path.exists():
            logger.error("ZIP file not found", extra={
                "component": "EXTRACT",
                "structured_data": {"zip_path": str(zip_path)},
            })
            return []
        if zip_path.suffix.lower() != ".zip":
            logger.error("Not a ZIP archive", extra={
                "component": "EXTRACT",
                "structured_data": {"zip_path": str(zip_path)},
            })
            return []

        extract_to.mkdir(parents=True, exist_ok=True)
        logger.info("Starting ZIP extraction", extra={
            "component": "EXTRACT",
            "structured_data": {
                "zip_path": str(zip_path),
                "extract_to": str(extract_to),
                "zip_size": zip_path.stat().st_size,
            },
        })

        with zipfile.ZipFile(zip_path, "r") as zf:
            bad = zf.testzip()
            if bad is not None:
                logger.error("ZIP integrity check failed", extra={
                    "component": "EXTRACT",
                    "structured_data": {"zip_path": str(zip_path), "bad_member": bad},
                })
                return []

            start = time.time()
            for info in zf.infolist():
                if info.is_dir():
                    # Ensure dir exists for completeness
                    (extract_to / info.filename).mkdir(parents=True, exist_ok=True)
                    continue
                target = extract_to / info.filename
                target.parent.mkdir(parents=True, exist_ok=True)
                with zf.open(info) as src, target.open("wb") as dst:
                    shutil.copyfileobj(src, dst)
                # Try to preserve timestamps
                try:
                    ts = time.mktime(info.date_time + (0, 0, -1))
                    os.utime(target, (ts, ts))
                except Exception:
                    pass
                extracted.append(target)

        logger.info("ZIP extraction completed", extra={
            "component": "EXTRACT",
            "structured_data": {
                "zip_path": str(zip_path),
                "extracted_count": len(extracted),
                "extract_to": str(extract_to),
                "duration_seconds": round(time.time() - start, 2),
            },
        })
        return extracted
    except Exception as e:
        logger.error("ZIP extraction failed", extra={
            "component": "EXTRACT",
            "structured_data": {"zip_path": str(zip_path), "error": str(e), "extracted_count": len(extracted)},
        })
        # Best-effort cleanup of partially extracted files
        for p in extracted:
            try:
                if p.exists():
                    p.unlink()
            except Exception:
                pass
        return []


def validate_file_structure(
    file_path: Path,
    expected_columns: List[str],
    config: Dict[str, Any],
    logger: StructuredLoggerAdapter,
) -> bool:
    """Validate a semicolon-delimited text file for header + at least one data row.

    - Tries UTF-8 first, falls back to Latin-1 (ISO-8859-1). Also tries CP1252 as a last resort.
    - Uses csv.reader with delimiter=";" (DWD standard) and order-insensitive header check.
    - Logs specific reasons on failure and returns True/False.
    """
    try:
        file_path = Path(file_path)
        if not file_path.exists():
            logger.error("File not found", extra={
                "component": "VALIDATE",
                "structured_data": {"file_path": str(file_path)},
            })
            return False
        if file_path.stat().st_size == 0:
            logger.error("File is empty", extra={
                "component": "VALIDATE",
                "structured_data": {"file_path": str(file_path)},
            })
            return False

        encodings = ["utf-8", "latin-1", "cp1252"]
        used_enc = None
        for enc in encodings:
            try:
                with file_path.open("r", encoding=enc, newline="") as fh:
                    reader = csv.reader(fh, delimiter=";")
                    try:
                        header = next(reader)
                    except StopIteration:
                        header = []
                    if not header:
                        # empty header under this encoding, try next
                        continue
                    used_enc = enc
                    # Normalize header
                    header_norm = [h.strip() for h in header]

                    # Check all expected columns are present (order-insensitive)
                    missing = [col for col in expected_columns if col not in header_norm]
                    if missing:
                        logger.error("Missing expected columns", extra={
                            "component": "VALIDATE",
                            "structured_data": {
                                "file_path": str(file_path),
                                "missing": missing,
                                "found": header_norm,
                            },
                        })
                        return False

                    # Ensure at least one data row (non-empty)
                    data_rows = 0
                    for row in reader:
                        # Skip completely empty rows
                        if not any(cell.strip() for cell in row):
                            continue
                        data_rows += 1
                        break

                    if data_rows == 0:
                        logger.error("No data rows found", extra={
                            "component": "VALIDATE",
                            "structured_data": {"file_path": str(file_path)},
                        })
                        return False

                    logger.info("File validation successful", extra={
                        "component": "VALIDATE",
                        "structured_data": {
                            "file_path": str(file_path),
                            "encoding": used_enc,
                            "column_count": len(header_norm),
                            "expected_columns": expected_columns,
                        },
                    })
                    return True
            except UnicodeDecodeError:
                # try next encoding
                continue
            except Exception as e:
                logger.error("Parse error during validation", extra={
                    "component": "VALIDATE",
                    "structured_data": {"file_path": str(file_path), "error": str(e), "encoding": enc},
                })
                return False

        logger.error("Failed to decode file with supported encodings", extra={
            "component": "VALIDATE",
            "structured_data": {"file_path": str(file_path), "encodings_tried": encodings},
        })
        return False

    except Exception as e:
        logger.error("File validation failed with unexpected error", extra={
            "component": "VALIDATE",
            "structured_data": {"file_path": str(file_path), "error": str(e)},
        })
        return False
