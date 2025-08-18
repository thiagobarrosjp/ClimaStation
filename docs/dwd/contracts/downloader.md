<!-- docs/contracts/downloader.md -->

# Downloader — Contract (v1)

**Stage:** `downloader`  
**Last updated:** 2025-08-18

## Purpose
Download all files listed in `urls.jsonl` into the canonical download tree, **safely and idempotently**.

## Inputs
- **CLI (via `run_pipeline.py`)**
  - `--mode download`
  - `--urls <path>` (default `data/dwd/1_crawl_dwd/urls.jsonl`)
  - `--download-dir <dir>` (e.g., `data/dwd/2_downloaded_files/`)
  - `--retries <n>` (default `3`), `--backoff <sec>` (default `1.0`), `--throttle <sec>`
- **Manifest:** `urls.jsonl` produced by the crawler.

## Outputs
- Files saved at:
```
<download-dir>/<relative_path>/<filename>
```
- (Optional) checksum sidecars: `<filename>.sha256`.
- Central run log: `data/dwd/0_debug/pipeline.log`.

## Exit codes
- `0` OK
- `1` Invalid arguments / unreadable `urls.jsonl`
- `2` Persistent HTTP failure after max retries (partials cleaned)
- `3` Disk/write failure (no partials left behind)
- `4` Invariant validation failure (when validation is enabled)

## Invariants (must always hold; auto-checked)
- **Idempotent skips:** if destination exists and matches size/checksum (when available) → **skip**.
- **No partial files:** failed downloads clean up `.part` files.
- **Correct layout:** saved path mirrors `relative_path` and `filename` from `urls.jsonl`.
- **Polite/robust:** throttle honored; retries with backoff + jitter for transient errors.
- **Deterministic ledger (optional):** emit a small JSON “download ledger” for testing.

## Validation & Tests
- **Property tests:** idempotent re-run (no new writes), no partials, correct directory layout (use local fixtures).
- **Golden test:** exact bytes of tiny fixture files **or** byte-compared **download ledger**.
- **CI gate:** Black · Ruff · MyPy · Pytest must pass before merge.

## Required logging (per file & summary)
```
stage=downloader url=<…> saved_to=<path> bytes=<int> attempts=<n> status=<ok|skipped|failed>
run_summary downloaded=<n> skipped=<n> failed=<n> duration_ms=<int>
```

## Programmatic interface (internal)
```
DownloaderAPI.fetch(urls_path: Path, download_dir: Path) -> DownloadReport
```
