<!-- docs/contracts/writer.md -->

# Writer — Contract (v1)

**Stage:** `writer`  
**Dataset:** `10_minutes_air_temperature` (v0 target)  
**Last updated:** 2025-09-14

## Purpose
Consume normalized rows, enforce uniqueness/sort, and write **Parquet** with a tiny deterministic **manifest** for tests.

## Inputs
- **Programmatic**
  ```python
  from pathlib import Path
  from typing import Iterable

  # from parser contract:
  # class ParsedRow(TypedDict, ...): ...

  def write_parquet(rows: Iterable[ParsedRow], out_root: Path) -> "WriterReport": ...
  ```

- **Schema module (Arrow):** see “Schema placement & naming” below.

## Outputs
- **Parquet layout**
  ```
  parsed/dwd/10_minutes_air_temperature/
    station_id=<id>/year=<YYYY>/<id>_<YYYY>.parquet
  ```
- **Per-file manifest (JSON)**
  ```json
  {
    "dataset_key":"10_minutes_air_temperature",
    "station_id":"00003",
    "year":1993,
    "row_count":123456,
    "min_ts":"1993-04-28T00:00:00Z",
    "max_ts":"1993-12-31T23:50:00Z",
    "schema_fingerprint":"sha256:…",
    "first_100_rows_csv_sha256":"…"
  }
  ```
- **Report:** counts and file paths.

## Exit codes (CLI, optional)
`0` OK · `1` invalid args · `3` disk/write failure · `4` invariants failed

## Invariants (auto-checked)
- **Uniqueness:** drop duplicates by `(station_id, timestamp_utc)` when `timestamp_utc` is not null.
- **Sorted:** chronological within each Parquet file.
- **Atomic writes:** temp → rename; idempotent re-runs.
- **Deterministic encoding:** fixed row-group size; stable Arrow schema; stable compression.

## Validation & Tests
- **Property tests:** uniqueness & order, schema adherence, re-run idempotency.
- **Golden test:** compare **manifest JSON** (not Parquet bytes) + verify `schema_fingerprint`.
- **CI gate:** Black · Ruff · MyPy · Pytest must pass before merge.

## Required logging (per run)
```
stage=writer station=<id> year=<YYYY> rows_in=<n> rows_written=<n> files=<k> duration_ms=<int>
```

## Programmatic interface (internal)
```
WriterAPI.write_parquet(rows, out_root) -> WriterReport
```