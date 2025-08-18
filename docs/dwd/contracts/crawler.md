<!-- docs/contracts/crawler.md -->

# Crawler — Contract (v1)

**Stage:** `crawler`  
**Last updated:** 2025-08-18

## Purpose
Discover and list all downloadable DWD files for a configured dataset; emit a **deterministic** `urls.jsonl` manifest used by the downloader.

## Inputs
- **CLI (via `run_pipeline.py`)**
  - `--mode crawl`
  - `--dataset <key>` (e.g., `10_minutes_air_temperature`)
  - `--outdir <dir>` (e.g., `data/dwd/1_crawl_dwd/`)
  - `--throttle <seconds>` (default `0.3`)
  - `--limit <n>` (optional, debug)
- **Config:** dataset rules (roots/subpaths/patterns), e.g., `app/config/datasets/<key>.yaml`.

## Outputs
- `<outdir>/urls.jsonl` — one JSON object per line with **stable fields**:
```json
{"url": "https://…", "relative_path": "…/", "filename": "file.zip", "dataset_key": "…"}
```
- Central run log: `data/dwd/0_debug/pipeline.log`.

## Exit codes
- `0` OK
- `1` Invalid arguments / missing config
- `2` Persistent HTTP/IO failure after retries (no partial output left behind)
- `4` Invariant/schema validation failure (when validation is enabled)

## Invariants (must always hold; auto-checked)
- **Files-only:** every line represents a downloadable file (no directories).
- **Absolute HTTPS:** `url` is absolute and starts with `https://`.
- **Stable order:** sorted by `relative_path`, then `filename`.
- **No duplicates:** unique `(relative_path, filename)` pairs.
- **Deterministic:** same inputs ⇒ byte-identical `urls.jsonl`.
- **Idempotent write:** temp → rename; re-runs replace prior file; no partials.

## Schema (record-level)
- `schemas/crawler_urls.schema.json` (required fields above).

## Validation & Tests
- **Property tests:** files-only, sorted, unique, absolute HTTPS.
- **Golden test:** tiny cached index/config ⇒ exact `urls.jsonl` bytes.
- **CI gate:** Black · Ruff · MyPy · Pytest must pass before merge.

## Required logging (per run)
```
stage=crawler dataset=<key> visited=<n> emitted=<n> throttle=<s> limit=<n|none>
duration_ms=<int> retries=<n> errors=[…]
```

## Programmatic interface (internal)
```
CrawlerAPI.expand(dataset_key: str, outdir: Path) -> UrlsManifest
```
