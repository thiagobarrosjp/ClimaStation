#!/usr/bin/env python3
"""
ClimaStation — Crawler URLs Manifest Validator (deterministic, offline)

Validates a JSONL manifest (one JSON object per line) emitted by the crawler
against the stage contract and schema rules — without importing jsonschema and
without any network calls. Processes the file in a streaming manner.

Exit codes:
  0 — all checks pass
  4 — any validation failure
  2 — unexpected exception (traceback logged)

See also (reference-only, do not import here):
- docs/dwd/contracts/crawler.md
- schemas/dwd/crawler_urls.schema.json
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, cast
from urllib.parse import urlparse
import posixpath
import traceback

# --- Logging helper ---------------------------------------------------------

def _get_logger() -> logging.Logger:
    """Return the shared validator logger.

    Tries `app.utils.enhanced_logger.get_logger("validator.crawler_urls")` first.
    Falls back to a standard console logger (single handler, INFO level).
    """
    logger_name = "validator.crawler_urls"
    logger: Optional[logging.Logger] = None

    try:
        # Lazy import to avoid hard dependency during offline runs.
        from app.utils.enhanced_logger import get_logger as _get  # type: ignore

        try:
            logger = _get("validator.crawler_urls")  # may return None
        except Exception:
            logger = None
    except Exception:
        logger = None

    if logger is None:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)
        # Avoid duplicate handlers when re-run in the same interpreter.
        if not logger.handlers:
            handler = logging.StreamHandler(stream=sys.stdout)
            fmt = (
                "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
            )
            handler.setFormatter(logging.Formatter(fmt))
            logger.addHandler(handler)
    return logger


# --- Data structures --------------------------------------------------------

@dataclass
class DuplicateRecord:
    relative_path: str
    filename: str
    first_seen_line: int
    duplicate_line: int


# --- Validation logic -------------------------------------------------------

def _is_nonempty_str(value: Any) -> bool:
    return isinstance(value, str) and len(value) > 0


def _validate_record(obj: Any, line_no: int) -> Tuple[bool, Optional[str], Optional[str], List[str]]:
    """Validate a single JSONL object per the contract (relative_path version).

    Returns:
        (is_valid, relative_path_trimmed, filename, errors)
        - relative_path_trimmed & filename are only set when record-level validation passes.
    """
    errors: List[str] = []

    if not isinstance(obj, dict):
        return False, None, None, ["line is not a JSON object"]

    # Required keys exist
    required = ["url", "relative_path", "filename"]
    for k in required:
        if k not in obj:
            errors.append(f"missing key: {k}")
    if errors:
        return False, None, None, errors

    url_val = obj.get("url")
    rel_path_raw = obj.get("relative_path")
    filename_val = obj.get("filename")

    # Basic type checks (collect errors first)
    if not isinstance(url_val, str):
        errors.append("url must be a string")
    if not isinstance(rel_path_raw, str):
        errors.append("relative_path must be a string")
    if not isinstance(filename_val, str):
        errors.append("filename must be a string")

    if errors:
        return False, None, None, errors

    # From here on, types are known — cast for the type checker
    url_s: str = cast(str, url_val)
    rel_path_in: str = cast(str, rel_path_raw)
    filename_s: str = cast(str, filename_val)

    # --- url checks ---
    if not url_s.startswith("https://"):
        errors.append("url must start with 'https://'")
    parsed = urlparse(url_s)
    if not parsed.scheme or not parsed.netloc:
        errors.append("url is not a valid absolute URI")
    # files-only: path must not end with '/'
    if parsed.path.endswith("/"):
        errors.append("url path ends with '/' (directory)")

    # --- filename checks ---
    if len(filename_s) == 0:
        errors.append("filename must be non-empty")
    if "/" in filename_s:
        errors.append("filename must not contain '/' characters")

    # if URL looks ok, compare basename to filename
    if parsed.path and not parsed.path.endswith("/"):
        base = posixpath.basename(parsed.path)
        if base and filename_s and base != filename_s:
            errors.append(
                f"url basename '{base}' does not match filename '{filename_s}'"
            )

    # --- relative_path checks ---
    rel_path = rel_path_in.strip()  # trim before validation (per spec)
    if rel_path != "" and rel_path.startswith("/"):
        errors.append("relative_path must not start with '/'")
    if rel_path != "" and not rel_path.endswith("/"):
        errors.append("relative_path must be empty or end with '/'")

    # --- optional keys (basic type/null checks only) ---
    if "dataset_key" in obj:
        if not _is_nonempty_str(obj["dataset_key"]):
            errors.append("dataset_key, if present, must be a non-empty string")
    if "size_bytes" in obj:
        sb = obj["size_bytes"]
        if not (sb is None or (isinstance(sb, int) and not isinstance(sb, bool) and sb >= 0)):
            errors.append("size_bytes must be int >= 0 or null")
    if "last_modified" in obj:
        lm = obj["last_modified"]
        if not (lm is None or isinstance(lm, str)):
            errors.append("last_modified must be string or null")
    if "checksum" in obj:
        cs = obj["checksum"]
        if not (cs is None or isinstance(cs, str)):
            errors.append("checksum must be string or null")

    if errors:
        return False, None, None, errors
    return True, rel_path, filename_s, []


# --- Report writer ----------------------------------------------------------

def _atomic_write_json(report_path: Path, data: Dict[str, Any], logger: logging.Logger) -> None:
    tmp_path = report_path.with_suffix(report_path.suffix + ".tmp")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with tmp_path.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=False)
        f.write("\n")
    os.replace(tmp_path, report_path)  # atomic on POSIX & Windows
    logger.info(f"Wrote validation report → {report_path}")


# --- CLI --------------------------------------------------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Validate crawler urls.jsonl manifest (offline, deterministic)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--input",
        required=False,
        default="data/dwd/1_crawl_dwd/10_minutes_air_temperature_urls.jsonl",
        help="Path to urls.jsonl",
    )
    p.add_argument(
        "--schema",
        default="schemas/dwd/crawler_urls.schema.json",
        help="Path to schema (read for metadata only)",
    )
    p.add_argument(
        "--report",
        default=None,
        help="Path to write validation report JSON (default: <input>.validation.json)",
    )
    return p.parse_args(argv)


# --- Main processing --------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> int:
    logger = _get_logger()
    t0 = time.time()

    try:
        args = parse_args(argv)
        input_path = Path(args.input).expanduser().resolve()
        schema_path = Path(args.schema).expanduser()
        report_path = (
            Path(args.report).expanduser() if args.report else Path(str(input_path) + ".validation.json")
        )
        report_path = report_path.resolve()

        logger.info(
            "Starting crawler URLs validation\n  input = %s\n  schema = %s\n  report = %s",
            str(input_path), str(schema_path.resolve()), str(report_path),
        )

        if not input_path.exists():
            logger.error(f"Input file not found: {input_path}")
            return 2

        total_lines = 0
        valid_lines = 0
        invalid_lines = 0
        errors_sample: List[Dict[str, Any]] = []

        # File-level invariants
        duplicates: List[DuplicateRecord] = []
        seen: Dict[Tuple[str, str], int] = {}
        MAX_DUP_SAMPLES = 20
        MAX_ERROR_SAMPLES = 25

        # Sorting detection across valid records only
        unsorted = {"is_unsorted": False, "first_bad_pair": None}  # type: ignore[dict-item]
        prev_pair: Optional[Tuple[str, str]] = None
        prev_line_no: Optional[int] = None

        with input_path.open("r", encoding="utf-8") as f:
            for line_no, line in enumerate(f, start=1):
                total_lines += 1
                s = line.strip()
                if s == "":
                    invalid_lines += 1
                    if len(errors_sample) < MAX_ERROR_SAMPLES:
                        errors_sample.append({"line": line_no, "error": "blank line"})
                    logger.warning(f"Line {line_no}: blank line")
                    continue
                try:
                    obj = json.loads(s)
                except json.JSONDecodeError as e:
                    invalid_lines += 1
                    if len(errors_sample) < MAX_ERROR_SAMPLES:
                        errors_sample.append({"line": line_no, "error": f"invalid JSON: {e.msg}"})
                    logger.warning(f"Line {line_no}: invalid JSON — {e.msg}")
                    continue

                ok, rel_path, filename, errs = _validate_record(obj, line_no)
                if not ok:
                    invalid_lines += 1
                    if len(errors_sample) < MAX_ERROR_SAMPLES:
                        errors_sample.append({"line": line_no, "error": "; ".join(errs)})
                    logger.warning(f"Line {line_no}: invalid — {'; '.join(errs)}")
                    continue

                # Valid record beyond this point
                valid_lines += 1

                # Unsorted detection (first adjacent pair only)
                pair = (rel_path or "", filename or "")
                if prev_pair is not None and not unsorted["is_unsorted"]:
                    if pair < prev_pair:
                        unsorted["is_unsorted"] = True
                        unsorted["first_bad_pair"] = {
                            "prev": {
                                "line": prev_line_no,
                                "relative_path": prev_pair[0],
                                "filename": prev_pair[1],
                            },
                            "curr": {
                                "line": line_no,
                                "relative_path": pair[0],
                                "filename": pair[1],
                            },
                        }
                        # Do not early-exit; continue to gather other stats deterministically
                prev_pair = pair
                prev_line_no = line_no

                # Duplicates (collect up to MAX_DUP_SAMPLES)
                if pair in seen:
                    if len(duplicates) < MAX_DUP_SAMPLES:
                        duplicates.append(
                            DuplicateRecord(
                                relative_path=pair[0],
                                filename=pair[1],
                                first_seen_line=seen[pair],
                                duplicate_line=line_no,
                            )
                        )
                else:
                    seen[pair] = line_no

        status = "ok"
        if invalid_lines > 0 or unsorted["is_unsorted"] or len(duplicates) > 0:
            status = "failed"

        report: Dict[str, Any] = {
            "input_path": str(input_path),
            "schema_path": str(schema_path),
            "total_lines": total_lines,
            "valid_lines": valid_lines,
            "invalid_lines": invalid_lines,
            "duplicates": [
                {
                    "relative_path": d.relative_path,
                    "filename": d.filename,
                    "first_seen_line": d.first_seen_line,
                    "duplicate_line": d.duplicate_line,
                }
                for d in duplicates
            ],
            "unsorted": unsorted,
            "errors_sample": errors_sample,
            "status": status,
            "elapsed_seconds": round(time.time() - t0, 6),
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }

        _atomic_write_json(report_path, report, logger)

        logger.info(
            "Finished validation: total=%d valid=%d invalid=%d duplicates=%d unsorted=%s status=%s elapsed=%.3fs",
            total_lines,
            valid_lines,
            invalid_lines,
            len(duplicates),
            str(unsorted["is_unsorted"]).lower(),
            status,
            time.time() - t0,
        )

        return 0 if status == "ok" else 4

    except SystemExit as se:
        # argparse may raise SystemExit; propagate its code
        raise se
    except Exception:
        # Unexpected exception
        tb = traceback.format_exc()
        try:
            logger = _get_logger()
            logger.error("Unexpected error during validation. See traceback below:")
            logger.error(tb)
        except Exception:
            # As a last resort, print to stderr
            sys.stderr.write(tb + "\n")
        return 2


if __name__ == "__main__":
    sys.exit(main())
