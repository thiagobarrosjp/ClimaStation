<!-- docs/contracts/parser.md -->

# Parser — Contract (v1)

**Stage:** `parser`  
**Dataset:** `10_minutes_air_temperature` (v0 target)  
**Last updated:** 2025-09-14

## Purpose
Transform extracted lines into **normalized records**: units converted, timestamps resolved (UTC when possible), sentinels → nulls, QN → `quality_level`, plus provenance.

## Inputs
- **Programmatic**
  ```python
  from datetime import datetime
  from typing import Iterable

  # from extractor contract:
  # class SourceMeta: ...

  def parse_air_temperature_10min(
      lines: Iterable[tuple[int, str]],
      meta: SourceMeta,
      *,
      now_utc: datetime,  # used for 'ingested_at'
  ) -> Iterable["ParsedRow"]: ...
  ```

- **Config (read-only):** dataset rules (sentinels, column mapping, unit scales).

### ParsedRow (v1) — one record per `(station_id, instant)`
```python
from typing import TypedDict, Literal, Optional

class ParsedRow(TypedDict, total=False):
    station_id: str

    # time
    timestamp_utc: Optional[str]      # ISO8601 "YYYY-MM-DDTHH:MM:SSZ" when resolvable
    timestamp_local: str              # raw local stamp "YYYYMMDDHHMM"
    time_ref: Literal["UTC","MEZ","unknown"]

    # measurements (v0; extendable)
    temperature_2m_c: Optional[float]
    temperature_0p05m_c: Optional[float]
    humidity_rel_pct: Optional[float]
    dewpoint_2m_c: Optional[float]
    pressure_station_hpa: Optional[float]

    # quality / window
    quality_level: Optional[int]      # from DWD QN
    parameter_window_found: bool      # true if within dataset’s time window

    # provenance
    source_filename: str
    source_url: Optional[str]
    source_row: int
    file_sha256: str
    ingested_at: str                  # ISO UTC
```

## Outputs
- **Primary (programmatic):** `Iterable[ParsedRow]` (streaming).
- **Optional (debug):** `parsed_sample.jsonl` (first N rows) for fixtures/tests.

## Exit codes (CLI, optional)
`0` OK · `1` invalid args · `2` bad input line/encoding · `4` invariants failed

## Invariants (auto-checked)
- **No guessing:** if timezone cannot be resolved ⇒ keep `timestamp_local`, set `time_ref="unknown"`, `timestamp_utc=None`.
- **Sentinels → nulls:** `-999`, `-9999`, empty ⇒ `None`.
- **Units normalized:** e.g., 0.1 °C ⇒ °C; hPa intact.
- **QN mapping:** DWD `QN` ⇒ `quality_level` (int).
- **Deterministic transform:** same inputs ⇒ identical `ParsedRow` sequence.
- **No enrichment in v0:** do not join station registries here (that’s Enricher later).

## Validation & Tests
- **Property tests:** required keys present; ≥1 row; UTC parsing when `time_ref=UTC`; **no duplicate `(station_id, timestamp_utc)`** among non-null UTC rows.
- **Golden test:** fixture `lines.jsonl` ⇒ stable `parsed_sample.jsonl` (e.g., SHA-256 of first 100 rows).
- **CI gate:** Black · Ruff · MyPy · Pytest must pass before merge.

## Required logging (per run)
```
stage=parser station=<id> in_rows=<n> out_rows=<n> utc_resolved=<n> nulls_converted=<n> duration_ms=<int>
```

## Programmatic interface (internal)
```
ParserAPI.parse_air_temperature_10min(lines, meta, *, now_utc) -> Iterable[ParsedRow]
```