# Available Functions (Reference Index)

> Scope: This index lists **only the callable entry points other modules should use**.  
> Keep it short and stable. Omit private helpers and internal class methods.  
> **Last updated:** 2025-08-10

---

## Configuration Management (`app/utils/config_manager.py`)
- `load_config(dataset_name: str, logger: Logger, config_root: Optional[Path] = None) -> Dict[str, Any]`  
  Load merged base + dataset configuration.
- `get_data_paths(logger: Logger, config_root: Optional[Path] = None) -> Dict[str, Path]`  
  Return all configured data paths as `Path` objects.
- `get_dwd_paths(logger: Logger, config_root: Optional[Path] = None) -> Dict[str, Path]`  
  Return DWD-specific paths block as `Path` objects.
- `clear_config_cache() -> None`  
  Clear cached configurations.

---

## Logging (`app/utils/enhanced_logger.py`)
- `setup_logger(component_id: str, name: Optional[str] = None, config: Optional[LogConfig] = None) -> ComponentLogger`  
  Create and register a component logger with standardized handlers/format.
- `get_logger(component_id: str, name: Optional[str] = None) -> Optional[ComponentLogger]`  
  Retrieve an existing component logger from the registry.
- `configure_root_logger(config: Optional[LogConfig] = None) -> None`  
  Configure the application root logger.
- `shutdown_logging() -> None`  
  Cleanly shut down all loggers/handlers.

---

## File Operations (`app/utils/file_operations.py`)
- `download_file(url: str, destination: Path, config: Dict[str, Any], logger: ComponentLogger) -> bool`  
  Download a file with retries and validation.
- `extract_zip(zip_path: Path, extract_to: Path, config: Dict[str, Any], logger: ComponentLogger) -> List[Path]`  
  Extract a ZIP and return extracted paths.
- `validate_file_structure(file_path: Path, expected_columns: List[str], config: Dict[str, Any], logger: ComponentLogger) -> bool`  
  Validate semicolon-delimited CSV/TXT structure.

---

## HTTP Headers (`app/utils/http_headers.py`)
- `default_headers() -> Dict[str, str]`  
  Default headers for HTTP requests.

---

## Progress Tracking (`app/utils/progress_tracker.py`)

- `ProcessingStatus(Enum)`  
  States: `PENDING`, `PROCESSING`, `SUCCESS`, `FAILED`.

- `ProgressTracker(db_path: Path, timeout: float = 30.0)`  
  SQLite-backed tracker; creates DB, tables, indexes.

- `ProgressTracker.register_files(dataset: str, file_paths: List[Path]) -> int`  
  Register files with `PENDING` status (idempotent).

- `ProgressTracker.start_processing(dataset: str, file_path: Path, worker_id: str) -> bool`  
  Transition `PENDING → PROCESSING` atomically for one file.

- `ProgressTracker.mark_success(dataset: str, file_path: Path, records_processed: Optional[int] = None) -> bool`  
  Set `SUCCESS`, stamp `end_time`.

- `ProgressTracker.mark_failed(dataset: str, file_path: Path, error_message: str) -> bool`  
  Set `FAILED`, record error + `end_time`.

- `ProgressTracker.get_dataset_status(dataset: str) -> Dict[str, int]`  
  Counts by status.

- `ProgressTracker.get_failed_files(dataset: str, limit: Optional[int] = None) -> List[Tuple[str, str, Optional[str]]]`  
  `(file_path, error_message, end_time)` list, newest first.

- `ProgressTracker.get_processing_summary(dataset: str) -> Dict[str, Any]`  
  Aggregates: totals, rates, timing, worker stats, recent daily stats.

- `ProgressTracker.get_stuck_files(dataset: str, timeout_hours: float = 2.0) -> List[Tuple[str, str, str]]`  
  Files in `PROCESSING` longer than threshold.

- `ProgressTracker.reset_stuck_files(dataset: str, timeout_hours: float = 2.0) -> int`  
  Reset stuck `PROCESSING` → `PENDING`.

- `ProgressTracker.reset_failed_files(dataset: str, error_pattern: Optional[str] = None) -> int`  
  Reset `FAILED` → `PENDING` (optional LIKE filter).

- `ProgressTracker.reset_dataset(dataset: str) -> bool`  
  Reset all files in dataset to `PENDING`.

- `ProgressTracker.get_pending_files(dataset: str, limit: Optional[int] = None) -> List[str]`  
  Pending file paths (oldest first).

- `ProgressTracker.cleanup_old_records(days_old: int = 30) -> int`  
  Delete old `SUCCESS`/`FAILED` records.

- `ProgressTracker.get_database_stats() -> Dict[str, Any]`  
  DB totals, size, datasets, date range.

---

## Crawler (`app/pipeline/crawler.py`)
- `crawl_dwd_repository(config: Dict[str, Any], logger: ComponentLogger, throttle: Optional[float] = None) -> CrawlResult`  
  Discover per-file ZIP URLs under the dataset’s configured root; write/merge an idempotent JSONL of URL records; return counts and output path(s). :contentReference[oaicite:1]{index=1}

---

## Downloader (`app/pipeline/downloader.py`)
- `run_downloader(config: Dict[str, Any], logger: ComponentLogger, max_downloads: Optional[int] = None, throttle: Optional[float] = None) -> ProcessingResult`  
  Orchestrate filtered, resumable ZIP downloads; optional per-file throttle. :contentReference[oaicite:2]{index=2}
- `load_urls_from_jsonl(urls_file: Path, logger: ComponentLogger, limit: Optional[int] = None, filter_subfolder: Optional[str] = None) -> List[Dict[str, Any]]`  
  Read and optionally filter URL entries from the crawler’s JSONL output. :contentReference[oaicite:3]{index=3}

---

## Pipeline Runner (`app/main/run_pipeline.py`)
- `run_crawl_mode(dataset_name: str, logger: ComponentLogger, dry_run: bool = False, throttle: Optional[float] = None) -> int`  
  Execute crawl mode for a dataset (config load, crawl, summary/exit code).
- `run_download_mode(dataset_name: str, logger: ComponentLogger, dry_run: bool = False, subfolder: Optional[str] = None, max_downloads: Optional[int] = None, throttle: Optional[float] = None) -> int`  
  Execute download mode (config load, optional subfolder filter, run downloader, summary/exit code).
- `main() -> int`  
  CLI entry-point for the pipeline.
