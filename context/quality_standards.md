# Quality Standards (Pipeline-wide)

This document defines the minimum bar every stage must meet, plus tiny per-stage checklists and file pointers for schemas and tests.

## Stage Quality Standard (applies to every stage)

We enforce the same five checks for each stage (Extractor, Parser, Writer). Originally captured as a short list in the README, now made executable with concrete pointers. :contentReference[oaicite:0]{index=0}

| # | Standard | What it means here | Where enforced |
|---|---|---|---|
| 1 | **Invariants defined** (record/file/run) | One-liners per stage below; must be explicit in the contract docs | `docs/contracts/*.md` + tests |
| 2 | **Schema provided** | JSON Schema for JSON artifacts; Arrow schema module for Parquet tables | See **Schemas & fingerprints** |
| 3 | **Property tests** | File/run rules (e.g., skip headers, no dup keys, sorted writes, idempotency) | `tests/**/test_*.py` |
| 4 | **Golden test** | Deterministic output for frozen inputs (bytes or manifest hash) | `tests/**/golden_*.py` + fixtures |
| 5 | **CI gate blocks merge** | Black · Ruff · MyPy · Pytest must pass; job required on `main` | `.github/workflows/tests.yaml` (job: `ci-quality`) |

---

## Per-stage checklists (must-haves)

### Extractor
- **Invariants:** data rows only (headers/comments removed); deterministic `station_id`; stable `file_sha256`; atomic writes when `--out` is used.
- **Property tests:** `header_skipped`, `rowcount_gt_zero`, `station_id_deterministic`.
- **Golden test:** tiny offline ZIP → exact `lines.jsonl` bytes (when `--out` used).
- **Contract doc:** `docs/contracts/extractor.md` (authoritative I/O & logging keys).

### Parser
- **Invariants:** no guessing TZ (`time_ref="unknown"` if unresolved, `timestamp_utc=None`); sentinels→null; unit normalization; QN→`quality_level`; deterministic transform; no enrichment in v0.
- **Property tests:** `required_keys_present`, `utc_resolves_when_possible`, **no duplicate `(station_id, timestamp_utc)`** among non-null UTC rows.
- **Golden test:** `lines.jsonl` fixture → `parsed_sample.jsonl` (hash of first 100 rows).
- **Contract doc:** `docs/contracts/parser.md`.

### Writer
- **Invariants:** uniqueness on `(station_id, timestamp_utc)`; chronological order within file; atomic writes; deterministic encoding (fixed row group size, stable compression).
- **Property tests:** `dedup_ok`, `sorted_by_time`, `schema_matches`, `rerun_idempotent`.
- **Golden test:** compare **manifest JSON** (not Parquet bytes) + `schema_fingerprint`.
- **Contract doc:** `docs/contracts/writer.md`.

---

## Schemas & fingerprints

- **Parsed record (Arrow, for Parquet):**  
  `schemas/dwd/record_10_minutes_air_temperature.py`  
  Exports: `arrow_schema()` and `schema_fingerprint()`.

- **Writer manifest (JSON Schema):**  
  `schemas/dwd/writer_manifest.schema.json`  
  Keys: `dataset_key`, `station_id`, `year`, `row_count`, `min_ts`, `max_ts`, `schema_fingerprint`, `first_100_rows_csv_sha256`.

> Keep JSON Schema for any JSONL/JSON artifacts (e.g., writer manifest). Keep Arrow schema in Python for typed Parquet writing and stable fingerprints.

---

## Test matrix (minimal)

| Stage | Property tests (examples) | Golden |
|---|---|---|
| Extractor | `test_header_skipped`, `test_station_id_deterministic` | `golden_lines_jsonl_bytes` |
| Parser | `test_required_keys`, `test_no_dup_keys`, `test_utc_resolution_rules` | `golden_parsed_first100_sha256` |
| Writer | `test_dedup_and_sort`, `test_schema_matches`, `test_rerun_idempotent` | `golden_manifest_compare` |

Naming convention: **one assertion per test** where practical; use explicit names matching the table.

---

## CI gate (required)

- Workflow: `.github/workflows/tests.yaml` (job id **`ci-quality`**)
- Steps: `black --check .`, `ruff .`, `mypy .`, `pytest -q`
- Fixtures: golden tests run **offline** against frozen inputs
- Branch protection: require **`ci-quality`** for `main`

---

## Logging keys (per run)

- **Extractor:** `stage=extractor zip=<path> station=<id> rows=<n> duration_ms=<int>`
- **Parser:** `stage=parser station=<id> in_rows=<n> out_rows=<n> utc_resolved=<n> nulls_converted=<n> duration_ms=<int>`
- **Writer:** `stage=writer station=<id> year=<YYYY> rows_in=<n> rows_written=<n> files=<k> duration_ms=<int>`

---

## How to add a new stage/dataset (2-step recipe)

1) **Docs first:** create `docs/contracts/<stage>.md` with I/O + invariants; add schema file (JSON or Arrow) and expose a fingerprint function.  
2) **Tests before code:** land property + golden tests with a tiny offline fixture; wire into `ci-quality`.

