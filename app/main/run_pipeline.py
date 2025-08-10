"""
ClimaStation Pipeline Runner

PUBLIC ENTRY POINTS (stable):
- run_crawl_mode(dataset_name: str, logger: ComponentLogger, dry_run: bool = False, throttle: Optional[float] = None) -> int
- run_download_mode(dataset_name: str, logger: ComponentLogger, dry_run: bool = False, subfolder: Optional[str] = None, max_downloads: Optional[int] = None, throttle: Optional[float] = None) -> int
- main() -> int

Notes:
- Wires CLI → crawler/downloader. No business logic here.
- Dry-run modes must not hit network or write files.
- Uses absolute imports and is tolerant of logger/config API variations.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional
import traceback
import logging

# ------------------------------
# Imports
# ------------------------------

from app.utils.enhanced_logger import get_logger, ComponentLogger

# Config loading: prefer load_config; fallback to ConfigManager
try:
    from app.utils.config_manager import load_config  # type: ignore
except Exception:  # pragma: no cover
    load_config = None  # type: ignore

try:
    from app.utils.config_manager import ConfigManager  # type: ignore
except Exception:  # pragma: no cover
    ConfigManager = None  # type: ignore

from app.pipeline.crawler import crawl_dwd_repository
from app.pipeline.downloader import run_downloader, load_urls_from_jsonl


# ------------------------------
# Helpers
# ------------------------------

def _safe_int(val: Optional[int]) -> Optional[int]:
    try:
        return int(val) if val is not None else None
    except Exception:
        return None


def _derive_crawl_paths(config: Dict[str, Any]) -> Dict[str, Path]:
    """Return key paths used by the crawler/downloader with robust defaults."""
    dwd_paths = (config.get("dwd_paths") or {})
    crawl_root = Path(dwd_paths.get("crawl_data", "data/dwd/1_crawl_dwd"))
    dataset_name = str(config.get("name") or config.get("dataset", "dataset"))
    return {
        "crawl_root": crawl_root,
        "dataset_dir": crawl_root / dataset_name,
        "crawler_jsonl_canonical": (crawl_root / dataset_name / f"dwd_{dataset_name}_urls.jsonl"),
        "crawler_jsonl_legacy": (crawl_root / dataset_name / "dwd_urls.jsonl"),
    }


def _resolve_urls_file(config: Dict[str, Any]) -> Path:
    paths = _derive_crawl_paths(config)
    # Prefer canonical (crawler) name; fall back to legacy (downloader) name
    if paths["crawler_jsonl_canonical"].exists():
        return paths["crawler_jsonl_canonical"]
    return paths["crawler_jsonl_canonical"]  # still prefer canonical even if absent (planning)


def _load_config(dataset_name: str, logger: ComponentLogger) -> Dict[str, Any]:
    """Load dataset config using available API, with safe defaults."""
    try:
        if callable(load_config):  # type: ignore[arg-type]
            cfg = load_config(dataset_name, logger)  # type: ignore
        elif ConfigManager is not None:
            cm = ConfigManager(config_dir="app/config/datasets")
            # Merge base + dataset if base exists; otherwise dataset only
            try:
                base_cfg = cm.get_base_config()  # type: ignore[attr-defined]
            except Exception:
                base_cfg = {}
            ds_cfg = cm.get_dataset_config(dataset_name)  # type: ignore[attr-defined]
            cfg = {**(base_cfg or {}), **(ds_cfg or {})}
        else:
            cfg = {}
    except Exception as e:  # robust fallback
        if hasattr(logger, "error"):
            logger.error("Failed to load config; using minimal defaults", extra={
                "component": "PIPELINE",
                "structured_data": {"dataset": dataset_name, "error": str(e)},
            })
        cfg = {}

    # Ensure required keys the rest of the pipeline expects
    # Normalize to a plain dict[str, Any] for static type checkers and safety
    from typing import cast as _cast
    if not isinstance(cfg, dict):
        try:
            cfg = dict(cfg)  # type: ignore[arg-type]
        except Exception:
            cfg = {}
    # Force string keys to avoid mypy/pyright inferring bytes keys
    try:
        cfg = {str(k): v for k, v in dict(cfg).items()}  # type: ignore[call-arg]
    except Exception:
        cfg = {}
    cfg = _cast(Dict[str, Any], cfg)

    cfg.setdefault("name", dataset_name)
    cfg.setdefault("dwd_paths", {})
    cfg["dwd_paths"].setdefault("crawl_data", "data/dwd/1_crawl_dwd")

    # Attempt to provide crawler scoping if available in config
    # Prefer consolidated `paths` map (subfolders) → derive base_path + subpaths
    paths = cfg.get("paths")
    crawler_cfg = cfg.setdefault("crawler", {})
    if isinstance(paths, dict) and paths:
        # Compute common parent and first-level subpaths
        try:
            # Convert to strings and compute parent
            parts = [str(v) for v in paths.values()]
            # Heuristic: find common prefix directory
            common_parent = str(Path(parts[0]).parent)
            for p in parts[1:]:
                while not str(Path(p)).startswith(common_parent):
                    common_parent = str(Path(common_parent).parent)
                    if common_parent == "/":
                        break
            base_path = common_parent.strip("/") + "/"
            subpaths = [Path(str(v)).name.strip("/") + "/" for v in parts]
            cfg["base_path"] = base_path
            crawler_cfg.setdefault("dataset_path", base_path)
            crawler_cfg.setdefault("subpaths", subpaths)
        except Exception:
            # Best-effort only; crawler will log if base_path missing
            pass

    return cfg


# ------------------------------
# Public entry points
# ------------------------------

def run_crawl_mode(
    dataset_name: str,
    logger: ComponentLogger,
    dry_run: bool = False,
    throttle: Optional[float] = None,
) -> int:
    """Execute crawl mode (discover ZIP URLs and write JSONL)."""
    try:
        logger.info("Runner: crawl mode start", extra={
            "component": "PIPELINE",
            "structured_data": {
                "mode": "crawl",
                "dataset": dataset_name,
                "dry_run": dry_run,
                "throttle": throttle,
            },
        })

        config = _load_config(dataset_name, logger)
        paths = _derive_crawl_paths(config)

        if dry_run:
            logger.info("Dry-run: would crawl dataset", extra={
                "component": "PIPELINE",
                "structured_data": {
                    "dataset": dataset_name,
                    "start_url_hint": config.get("base_path") or (config.get("source", {}) or {}).get("base_path"),
                    "output_jsonl": str(paths["crawler_jsonl_canonical"]),
                    "throttle": throttle,
                },
            })
            return 0

        result = crawl_dwd_repository(config, logger, throttle=throttle)
        logger.info("Crawl summary", extra={
            "component": "PIPELINE",
            "structured_data": {
                "files_found": result.files_found,
                "files_written": result.files_written,
                "files_skipped": result.files_skipped,
                "requests": result.crawled_count,
                "output_file": str(result.output_files.get("urls")),
            },
        })
        return 0

    except Exception as e:
        logger.error("Crawl mode failed", extra={
            "component": "PIPELINE",
            "structured_data": {"dataset": dataset_name, "error": str(e)},
        })
        if getattr(logger, "level", 99) <= logging.DEBUG:
            traceback.print_exc()
        return 1


def run_download_mode(
    dataset_name: str,
    logger: ComponentLogger,
    dry_run: bool = False,
    subfolder: Optional[str] = None,
    max_downloads: Optional[int] = None,
    throttle: Optional[float] = None,
) -> int:
    """Execute download mode (plan or run downloads)."""
    try:
        logger.info("Runner: download mode start", extra={
            "component": "PIPELINE",
            "structured_data": {
                "mode": "download",
                "dataset": dataset_name,
                "dry_run": dry_run,
                "subfolder": subfolder,
                "limit": max_downloads,
                "throttle": throttle,
            },
        })

        config = _load_config(dataset_name, logger)
        # Communicate subfolder preference to downstream when not None
        if subfolder:
            cfg_dl = config.setdefault("downloader", {})
            cfg_dl["subfolder"] = subfolder

        if dry_run:
            urls_path = _resolve_urls_file(config)
            # Plan only: read JSONL and show first N
            limit = _safe_int(max_downloads)
            candidates: List[Dict[str, Any]] = load_urls_from_jsonl(urls_path, logger, limit=limit, filter_subfolder=subfolder)
            preview = candidates[: min(3, len(candidates))]
            logger.info("Dry-run plan (download)", extra={
                "component": "PIPELINE",
                "structured_data": {
                    "urls_file": str(urls_path),
                    "count": len(candidates),
                    "preview": [c.get("filename") for c in preview],
                },
            })
            return 0

        # Note: subfolder filtering currently applied only during dry-run planning
        if subfolder:
            logger.info("Note: subfolder filter is applied only in dry-run planning for now", extra={
                "component": "PIPELINE",
                "structured_data": {"subfolder": subfolder},
            })

        result = run_downloader(config, logger, max_downloads=max_downloads, throttle=throttle)
        logger.info("Download summary", extra={
            "component": "PIPELINE",
            "structured_data": {
                "processed": result.files_processed,
                "failed": result.files_failed,
                "skipped": result.metadata.get("files_skipped", 0),
                "success": result.success,
            },
        })
        return 0 if result.success else 1

    except Exception as e:
        logger.error("Download mode failed", extra={
            "component": "PIPELINE",
            "structured_data": {"dataset": dataset_name, "error": str(e)},
        })
        if getattr(logger, "level", 99) <= logging.DEBUG:
            traceback.print_exc()
        return 1


# ------------------------------
# CLI
# ------------------------------

def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="ClimaStation pipeline runner")
    p.add_argument("--mode", choices=["crawl", "download"], required=True)
    p.add_argument("--dataset", required=True)
    p.add_argument("--dry-run", action="store_true", dest="dry_run")
    p.add_argument("--subfolder", type=str, default=None)
    p.add_argument("--limit", type=int, default=None, help="Limit items (planning or max downloads)")
    p.add_argument("--throttle", type=float, default=None, help="Seconds to sleep between requests/downloads")
    return p


def main() -> int:
    parser = _build_arg_parser()
    args = parser.parse_args()

    # Per requirements: use component name "pipeline.runner"
    logger_opt = get_logger("pipeline.runner")
    if logger_opt is None:
        # Fallback to stdlib logger but keep ComponentLogger typing via cast
        from typing import cast as _cast
        std = logging.getLogger("pipeline.runner")
        if not std.handlers:
            std.addHandler(logging.StreamHandler(sys.stdout))
        std.setLevel(logging.INFO)
        logger: ComponentLogger = _cast(ComponentLogger, std)
    else:
        logger: ComponentLogger = logger_opt   

    if args.mode == "crawl":
        return run_crawl_mode(dataset_name=args.dataset, logger=logger, dry_run=args.dry_run, throttle=args.throttle)
    elif args.mode == "download":
        return run_download_mode(
            dataset_name=args.dataset,
            logger=logger,
            dry_run=args.dry_run,
            subfolder=args.subfolder,
            max_downloads=args.limit,
            throttle=args.throttle,
        )
    else:
        # Should not happen due to choices enforcement
        logger.error("Unknown mode requested", extra={"component": "PIPELINE", "structured_data": {"mode": args.mode}})
        return 1


if __name__ == "__main__":
    sys.exit(main())
