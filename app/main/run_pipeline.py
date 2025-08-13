"""
ClimaStation Pipeline Runner (updated for new YAML contract)

PUBLIC ENTRY POINTS (stable):
- run_crawl_mode(dataset_name: str, logger: ComponentLogger, dry_run: bool = False, subfolder: Optional[str] = None, throttle: Optional[float] = None) -> int
- run_download_mode(dataset_name: str, logger: ComponentLogger, dry_run: bool = False, subfolder: Optional[str] = None, max_downloads: Optional[int] = None, throttle: Optional[float] = None) -> int
- main() -> int

Notes:
- Wires CLI → crawler/downloader. No business logic here.
- Dry-run modes must not hit network or write files.
- Uses absolute imports and is tolerant of logger/config API variations.
- **Aligned to NEW YAML keys** (crawler.base_url, crawler.root_path, crawler.subfolders, crawler.output_urls_jsonl, downloader.root_dir).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import traceback
import logging
import json


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
from app.utils.enhanced_logger import configure_session_file_logging

# ------------------------------
# Helpers
# ------------------------------

def _safe_int(val: Optional[int]) -> Optional[int]:
    try:
        return int(val) if val is not None else None
    except Exception:
        return None


def _get(cfg: Dict[str, Any], path: str) -> Any:
    """Safe dotted-path getter (e.g., 'crawler.base_url')."""
    cur: Any = cfg
    for key in path.split("."):
        if not isinstance(cur, dict) or key not in cur:
            return None
        cur = cur[key]
    return cur


def _validate_new_contract(cfg: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Verify presence of required keys from the NEW YAML contract."""
    required = [
        "crawler.base_url",
        "crawler.root_path",
        "crawler.subfolders",
        "crawler.output_urls_jsonl",
        "downloader.root_dir",
    ]
    missing: List[str] = [p for p in required if _get(cfg, p) in (None, "")]
    return (len(missing) == 0, missing)


def _load_config(dataset_name: str, logger: ComponentLogger) -> Dict[str, Any]:
    """Load dataset config using available API, with safe defaults, **without** inventing legacy keys."""
    try:
        if callable(load_config):  # type: ignore[arg-type]
            cfg = load_config(dataset_name, logger)  # type: ignore
        elif ConfigManager is not None:
            cm = ConfigManager(config_dir="app/config/datasets")
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

    # Normalize structure and inject name
    if not isinstance(cfg, dict):
        try:
            cfg = dict(cfg)  # type: ignore[arg-type]
        except Exception:
            cfg = {}
    cfg = {str(k): v for k, v in cfg.items()}
    cfg.setdefault("name", dataset_name)
    return cfg


def _resolve_urls_file(cfg: Dict[str, Any]) -> Path:
    path = _get(cfg, "crawler.output_urls_jsonl")
    if isinstance(path, str) and path:
        return Path(path)
    # Fallback: legacy location (kept only for robustness)
    dataset_name = str(cfg.get("name", cfg.get("dataset", "dataset")))
    return Path("data/dwd/1_crawl_dwd") / dataset_name / f"dwd_{dataset_name}_urls.jsonl"


def _merge_jsonl_append_unique(src: Path, dest: Path, logger: ComponentLogger) -> Tuple[int, int]:
    """Append unique records from src into dest by URL; return (written, skipped)."""
    if not src.exists():
        return (0, 0)

    existing_urls: set[str] = set()
    if dest.exists():
        try:
            with dest.open("r", encoding="utf-8") as fh:
                for line in fh:
                    try:
                        u = json.loads(line.strip()).get("url")
                        if isinstance(u, str):
                            existing_urls.add(u)
                    except Exception:
                        continue
        except Exception as e:
            logger.warning("Could not read existing output JSONL; proceeding fresh", extra={
                "component": "PIPELINE",
                "structured_data": {"output": str(dest), "error": str(e)},
            })

    written = 0
    skipped = 0
    dest.parent.mkdir(parents=True, exist_ok=True)
    with src.open("r", encoding="utf-8") as rfh, dest.open("a", encoding="utf-8") as wfh:
        for line in rfh:
            try:
                obj = json.loads(line.strip())
                url = obj.get("url")
                if not isinstance(url, str):
                    continue
                if url in existing_urls:
                    skipped += 1
                    continue
                existing_urls.add(url)
                wfh.write(json.dumps(obj, ensure_ascii=False) + "\n")
                written += 1
            except Exception:
                continue
    return (written, skipped)


def _translate_config_for_legacy_crawler(cfg: Dict[str, Any], dataset_name: str, subfolder: Optional[str]) -> Dict[str, Any]:
    """Build a compatibility config for the existing crawler implementation.

    - Maps NEW YAML keys to what the crawler expects today.
    - We keep writes idempotent but later merge to the NEW `crawler.output_urls_jsonl` path.
    """
    base_url = _get(cfg, "crawler.base_url") or "https://opendata.dwd.de/climate_environment/CDC/"
    root_path = _get(cfg, "crawler.root_path") or ""
    subfolders = _get(cfg, "crawler.subfolders") or {}
    # Choose allowed first-level subpaths
    subpaths_list: List[str] = []
    if isinstance(subfolders, dict) and subfolders:
        if subfolder:
            # Allow both key or raw value; prefer mapping by key
            if subfolder in subfolders:
                subpaths_list = [str(subfolders[subfolder])]
            else:
                # If user provided actual folder name, use as-is
                subpaths_list = [str(subfolder)]
        else:
            subpaths_list = [str(v) for v in subfolders.values()]
    # Ensure trailing slashes
    subpaths_list = [s if s.endswith("/") else s + "/" for s in subpaths_list]

    compat: Dict[str, Any] = {
        "name": dataset_name,
        # Old crawler reads these fields
        "crawler": {
            "base_url": base_url,
            "dataset_path": str(root_path),
            "subpaths": subpaths_list,
        },
        # Legacy output root where crawler will write; we'll merge into the new path afterwards
        "dwd_paths": {
            "crawl_data": str(Path(_resolve_urls_file(cfg)).parent),
        },
    }
    return compat


# ------------------------------
# Public entry points
# ------------------------------

def run_crawl_mode(
    dataset_name: str,
    logger: ComponentLogger,
    dry_run: bool = False,
    subfolder: Optional[str] = None,
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
                "subfolder": subfolder,
                "throttle": throttle,
            },
        })

        cfg = _load_config(dataset_name, logger)
        ok, missing = _validate_new_contract(cfg)
        if not ok:
            logger.error(
                "Missing required configuration keys; update dataset YAML to the new contract",
                extra={
                    "component": "PIPELINE",
                    "structured_data": {
                        "expected_keys": [
                            "crawler.base_url",
                            "crawler.root_path",
                            "crawler.subfolders",
                            "crawler.output_urls_jsonl",
                            "downloader.root_dir",
                        ],
                        "missing": missing,
                    },
                },
            )
            return 1

        base_url: str = _get(cfg, "crawler.base_url")  # type: ignore[assignment]
        root_path: str = _get(cfg, "crawler.root_path")  # type: ignore[assignment]
        subfolders: Dict[str, str] = _get(cfg, "crawler.subfolders") or {}
        output_jsonl = _resolve_urls_file(cfg)

        # Resolve subfolder value (if provided via CLI)
        subfolder_value: Optional[str] = None
        if subfolder:
            subfolder_value = subfolders.get(subfolder, subfolder)

        # Build root listing URL per requirements
        root_listing_url = base_url.rstrip("/") + "/" + root_path.lstrip("/")
        if subfolder_value:
            root_listing_url = root_listing_url.rstrip("/") + "/" + subfolder_value.lstrip("/")

        if dry_run:
            logger.info("Dry-run: would crawl dataset", extra={
                "component": "PIPELINE",
                "structured_data": {
                    "dataset": dataset_name,
                    "root_listing_url": root_listing_url,
                    "output_jsonl": str(output_jsonl),
                    "throttle": throttle,
                },
            })
            return 0

        # Translate config for legacy crawler and run
        compat_cfg = _translate_config_for_legacy_crawler(cfg, dataset_name, subfolder=subfolder)
        result = crawl_dwd_repository(compat_cfg, logger, throttle=throttle)

        # Merge/copy into required NEW output path
        crawler_jsonl = Path(result.output_files.get("urls") or "")
        written, skipped = _merge_jsonl_append_unique(crawler_jsonl, output_jsonl, logger)

        logger.info("Crawl summary", extra={
            "component": "PIPELINE",
            "structured_data": {
                "files_found": result.files_found,
                "files_written": result.files_written,
                "files_skipped": result.files_skipped,
                "requests": result.crawled_count,
                "crawler_output": str(crawler_jsonl) if crawler_jsonl else None,
                "merged_into": str(output_jsonl),
                "merge_appended": written,
                "merge_skipped": skipped,
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

        cfg = _load_config(dataset_name, logger)
        ok, missing = _validate_new_contract(cfg)
        if not ok:
            logger.error(
                "Missing required configuration keys; update dataset YAML to the new contract",
                extra={
                    "component": "PIPELINE",
                    "structured_data": {
                        "expected_keys": [
                            "crawler.base_url",
                            "crawler.root_path",
                            "crawler.subfolders",
                            "crawler.output_urls_jsonl",
                            "downloader.root_dir",
                        ],
                        "missing": missing,
                    },
                },
            )
            return 1

        # Communicate subfolder preference for downloader (optional)
        if subfolder:
            cfg_dl = cfg.setdefault("downloader", {})
            cfg_dl["subfolder"] = subfolder

        if dry_run:
            urls_path = _resolve_urls_file(cfg)
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

        result = run_downloader(cfg, logger, max_downloads=max_downloads, throttle=throttle)
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
    configure_session_file_logging("data/dwd/0_debug/pipeline.log")
    
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
        return run_crawl_mode(
            dataset_name=args.dataset,
            logger=logger,
            dry_run=args.dry_run,
            subfolder=args.subfolder,
            throttle=args.throttle,
        )
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
