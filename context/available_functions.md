# Available Functions (Reference Index)

> Scope: This index lists **only the callable entry points other modules should use**.  
> Keep it short and stable. Omit private helpers and internal class methods.  
> **Last updated:** 2025-08-31

---

## app/main/run_pipeline.py
- `main() -> int` — Pipeline CLI entry point.
- `run_crawl_mode(dataset_name: str, logger: ComponentLogger, *, source: str=…, dry_run: bool=…, subfolder: Optional[str]=…, throttle: Optional[float]=…, limit: Optional[int]=…, outdir: Optional[str]=…, validate: bool=…) -> int` — Execute crawl mode: discover URLs and write JSONL manifests.
- `run_download_mode(dataset_name: str, logger: ComponentLogger, dry_run: bool=…, subfolder: Optional[str]=…, max_downloads: Optional[int]=…, throttle: Optional[float]=…) -> int` — Execute download mode: plan and download files.
- `serve_directory_http(root: Path) -> Iterator[tuple[str, int]]` — Context manager that serves *root* via a local HTTP server on 127.0.0.1:PORT.

## app/pipeline/crawler.py
- `crawl_dwd_repository(config: Dict[str, Any], logger: logging.Logger | logging.LoggerAdapter, *, base_url: str, canonical_base_url: str, include_extensions: List[str], sample_size: int=…, throttle: Optional[float]=…, limit: Optional[int]=…) -> CrawlResult` — Discover per-file URLs under the dataset root and write JSONL manifests.
- `CrawlResult` — Result of a crawl: counts, paths and status.
- `DWDRepositoryCrawler` — Crawl Apache-style directory listings and produce URL manifests.

## app/pipeline/downloader.py
- `load_urls_from_jsonl(urls_file: Path, logger: logging.Logger | logging.LoggerAdapter, limit: Optional[int]=…, filter_subfolder: Optional[str]=…) -> List[Dict[str, Any]]` — Read URLs JSONL line-by-line with optional filtering.
- `ProcessingResult` — Processing result.
- `run_downloader(config: Dict[str, Any], logger: logging.Logger | logging.LoggerAdapter, max_downloads: Optional[int]=…, throttle: Optional[float]=…) -> ProcessingResult` — Run downloader.

## app/processors/base_processor.py
- `BaseProcessor` — Abstract base class for all dataset processors.
- `ProcessingMode` — Processing mode enumeration.
- `ProcessingResult` — Result of processing a single file.
- `ProcessingStats` — Overall processing statistics.

## app/translations/meteorological/__init__.py
- `load_all_meteorological_translations() -> Dict[str, Any]` — Load all meteorological translation files.

## app/translations/translation_manager.py
- `TranslationManager` — Manages translations and metadata lookups for ClimaStation pipeline.

## app/utils/config_manager.py
- `ConfigManager` — Manages loading and accessing configuration files for the ClimaStation platform.

## app/utils/enhanced_logger.py
- `ComponentLogger` — Thin subclass for typing/compat only.
- `ComponentLoggerAdapter` — Compatibility adapter that preserves extra fields if callers use it.
- `configure_root_logger(config: Optional[Any]=…) -> None` — Deprecated shim. Prefer `configure_session_file_logging(...)`.
- `configure_session_file_logging(log_path: str=…, level: int | str=…) -> logging.Logger` — Configure the *root* logger once per process to write all logs into a single file.
- `get_logger(component_id: str, name: Optional[str]=…) -> Optional[ComponentLogger]` — Compatibility accessor; always returns the named logger (never None).
- `log_function_entry(logger: logging.Logger, func_name: str, **kwargs) -> None` — Log function entry.
- `log_function_exit(logger: logging.Logger, func_name: str, result: Any | None=…, duration: float | None=…) -> None` — Log function exit.
- `setup_logger(component_id: str, name: Optional[str]=…, config: Optional[Any]=…) -> ComponentLogger` — Return a named logger; formatting/handlers are controlled by the root.
- `shutdown_logging() -> None` — Flush and close the single session file handler without disturbing console handlers.

## app/utils/file_operations.py
- `download_file(url: str, destination: Path, config: Dict[str, Any], logger: StructuredLoggerAdapter) -> bool` — Download a file with retries, resume support, and atomic write.
- `extract_zip(zip_path: Path, extract_to: Path, config: Dict[str, Any], logger: StructuredLoggerAdapter) -> List[Path]` — Extract a ZIP archive using stdlib zipfile.
- `validate_file_structure(file_path: Path, expected_columns: List[str], config: Dict[str, Any], logger: StructuredLoggerAdapter) -> bool` — Validate a semicolon-delimited text file for header + at least one data row.

## app/utils/http_headers.py
- `default_headers()` — Return default HTTP headers for outbound requests.

## app/utils/progress_tracker.py
- `ProcessingStatus` — Enumeration of possible file processing states.
- `ProgressTracker` — SQLite-based progress tracking for file processing operations.
