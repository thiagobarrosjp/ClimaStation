<!-- docs/contracts/extractor.md -->

# Extractor — Contract (v1)

**Stage:** `extractor`  
**Dataset:** `10_minutes_air_temperature` (v0 target)  
**Last updated:** 2025-09-14

## Purpose
Open one DWD ZIP, find its single data TXT, and stream **data lines + provenance** in a deterministic, memory-light way for the parser.

## Inputs
- **CLI (optional; via `run_pipeline.py`)**
  - `--mode extract`
  - `--zip <path>` (ZIP file previously downloaded)
  - `--out <optional: lines.jsonl>` (for debugging)
- **Programmatic**
  ```python
  from dataclasses import dataclass
  from pathlib import Path
  from typing import Iterable

  @dataclass
  class SourceMeta:
      source_filename: str        # e.g. 10minutenwerte_TU_00003_19930428_19991231_hist.zip
      source_url: str | None      # when known from urls.jsonl
      file_sha256: str            # checksum of ZIP
      station_id: str             # zero-padded, e.g. "00003"

  def extract_lines(zip_path: Path) -> tuple[SourceMeta, Iterable[tuple[int, str]]]: ...
  ```

## Outputs
- **Primary (programmatic):** `(SourceMeta, Iterable[(row_no, line)])`, where `line` is the raw, semicolon-delimited payload; header/comment lines are skipped.
- **Optional (CLI):** `lines.jsonl` for inspection:
  ```json
  {"row_no":1,"line":"199306100430;...;QN=1;...","source_filename":"...","station_id":"00003"}
  ```

## Exit codes (CLI)
`0` OK · `1` invalid args · `2` unreadable ZIP/TXT · `4` invariants failed

## Invariants (auto-checked)
- **Data rows only:** header/comment lines removed; original order preserved.
- **Deterministic station:** `station_id` parsed the same way every time (from filename or TXT column; rule is fixed).
- **Stable checksum:** `file_sha256` computed once; stable across runs.
- **Idempotent write:** when `--out` is used, temp-file → atomic rename; no partials left behind.

## Validation & Tests
- **Property tests:** header skip; row count > 0 for known fixture; stable `station_id` extraction.
- **Golden test:** tiny offline ZIP fixture ⇒ exact `lines.jsonl` bytes (when `--out` used).
- **CI gate:** Black · Ruff · MyPy · Pytest must pass before merge.

## Required logging (per run)
```
stage=extractor zip=<path> station=<id> rows=<n> duration_ms=<int>
```

## Programmatic interface (internal)
```
ExtractorAPI.extract_lines(zip_path: Path) -> tuple[SourceMeta, Iterable[tuple[int, str]]]
```