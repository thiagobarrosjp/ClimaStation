#!/usr/bin/env python3
"""
ClimaStation — refresh_fixture.py

Small CLI to create/update a test fixture JSONL by copying the first N lines
from a full crawler manifest JSONL. Supports atomic write and optional
post-write validation via the validator CLI.

Exit codes:
  0 — success
  2 — unexpected error
  4 — validator failed (when --validate is used)

Usage example:
    python app/tools/refresh_fixture.py \
      --input data/dwd/1_crawl_dwd/10_minutes_air_temperature_urls.jsonl \
      --output tests/dwd/fixtures/10_minutes_air_temperature_urls_sample100.jsonl \
      --count 100 \
      --validate
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
import subprocess
from typing import Optional


# ------------------------------ CLI ----------------------------------------

def _positive_int(value: str) -> int:
    try:
        iv = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("--count must be an integer >= 1")
    if iv < 1:
        raise argparse.ArgumentTypeError("--count must be >= 1")
    return iv


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Copy the first N lines from a crawler manifest JSONL into a fixture JSONL (atomic write).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--input",
        required=True,
        help="Path to the full manifest JSONL (source)",
    )
    p.add_argument(
        "--output",
        required=True,
        help="Path to the fixture JSONL to write (destination)",
    )
    p.add_argument(
        "--count",
        type=_positive_int,
        default=100,
        help="Number of lines to copy (>= 1)",
    )
    p.add_argument(
        "--schema",
        default="schemas/dwd/crawler_urls.schema.json",
        help="Path to schema to pass to the validator when --validate is used",
    )
    p.add_argument(
        "--validate",
        action="store_true",
        help="Run the crawler URLs validator on the output file after writing",
    )
    return p.parse_args(argv)


# --------------------------- Core behavior ---------------------------------

def _stream_copy_first_n(src: Path, dst: Path, n: int) -> int:
    """Stream the first N lines from src to dst, normalizing to LF and ensuring
    the file ends with a newline. Returns the number of lines written.
    """
    # Ensure parent directory exists
    dst.parent.mkdir(parents=True, exist_ok=True)

    tmp_path = Path(str(dst) + ".tmp")  # <output>.tmp (Windows-safe)

    lines_written = 0
    with src.open("r", encoding="utf-8", newline=None) as fin, \
         tmp_path.open("w", encoding="utf-8", newline="\n") as fout:
        for line in fin:
            # Normalize to a single LF and strip trailing CR/LF variations
            s = line.rstrip("\r\n")
            fout.write(s + "\n")
            lines_written += 1
            if lines_written >= n:
                break

    # Atomic replace: tmp -> final (POSIX & Windows)
    os.replace(tmp_path, dst)
    return lines_written


def _run_validator(output_path: Path, schema_path: Path) -> int:
    """Run the validator CLI against the output file.
    Returns the validator's process return code.
    """
    cmd = [
        sys.executable,
        "app/tools/validate_crawler_urls.py",
        "--input",
        str(output_path),
        "--schema",
        str(schema_path),
    ]
    proc = subprocess.run(cmd, capture_output=False)
    return proc.returncode


def main(argv: Optional[list[str]] = None) -> int:
    try:
        args = parse_args(argv)
        src = Path(args.input).expanduser().resolve()
        dst = Path(args.output).expanduser().resolve()
        schema = Path(args.schema).expanduser()

        if not src.exists():
            print(f"ERROR: input file not found: {src}")
            return 2
        if not src.is_file():
            print(f"ERROR: input is not a file: {src}")
            return 2

        # Copy first N lines with atomic write
        lines = _stream_copy_first_n(src, dst, args.count)

        # Optionally validate
        validation_status = "skipped"
        validator_rc = 0
        if args.validate:
            validator_rc = _run_validator(dst, schema)
            if validator_rc == 0:
                validation_status = "passed"
            else:
                validation_status = f"failed (rc={validator_rc})"

        # Summary to stdout
        print("Fixture refresh summary:")
        print(f"  input      = {src}")
        print(f"  output     = {dst}")
        print(f"  lines      = {lines}")
        print(f"  validate   = {validation_status}")

        if args.validate and validator_rc != 0:
            # Per spec: propagate exit code 4 on validator failure
            return 4

        return 0

    except SystemExit:
        # Let argparse propagate its own exit code
        raise
    except Exception as e:
        # Keep it simple; no external logger
        print(f"Unexpected error: {e}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
