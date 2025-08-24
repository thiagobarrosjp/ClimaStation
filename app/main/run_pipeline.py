# app/main/run_pipeline.py
"""
ClimaStation Pipeline Runner — URL-agnostic crawl with online/offline switch

PUBLIC ENTRY POINTS (stable):
- run_crawl_mode(dataset_name: str, logger: ComponentLogger, *, source: str = "online", dry_run: bool = False, subfolder: Optional[str] = None, throttle: Optional[float] = None, limit: Optional[int] = None, outdir: Optional[str] = None) -> int
- run_download_mode(dataset_name: str, logger: ComponentLogger, dry_run: bool = False, subfolder: Optional[str] = None, max_downloads: Optional[int] = None, throttle: Optional[float] = None) -> int
- main() -> int

Notes:
- Adds --source {online,offline} for crawl mode. Offline spins up a local HTTP server
  that serves a golden directory and auto-shuts down after crawling.
- Eliminates reliance on env vars for crawler URL selection.
- Passes explicit parameters to crawler: base_url, canonical_base_url, include_extensions,
  sample_size, throttle, and limit.
"""
from __future__ import annotations

import argparse
import contextlib
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING, Iterator
import traceback
import logging
import json
import os

from urllib.parse import urljoin

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
if TYPE_CHECKING:
    from app.pipeline.crawler import CrawlResult as CrawlerCrawlResult
else:
    CrawlerCrawlResult = Any  # runtime-friendly

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
    """Load dataset config using available API, with safe defaults."""
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
    dataset_name = str(cfg.get("name", cfg.get("dataset", "dataset")))
    return Path("data/dwd/1_crawl_dwd") / f"{dataset_name}_urls.jsonl"


# ------------------------------
# Local HTTP server for offline mode
# ------------------------------

@contextlib.contextmanager
def serve_directory_http(root: Path) -> Iterator[tuple[str, int]]:
    """Context manager that serves *root* via a local HTTP server on 127.0.0.1:PORT.

    Yields (host, port). Auto-shuts down afterwards.
    """
    import http.server
    import socketserver

    # Find a free port
    import socket as _socket
    with _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]

    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(root), **kwargs)

    httpd = socketserver.TCPServer(("127.0.0.1", port), Handler)

    try:
        import threading
        t = threading.Thread(target=httpd.serve_forever, daemon=True)
        t.start()
        yield ("127.0.0.1", port)
    finally:
        try:
            httpd.shutdown()
        except Exception:
            pass
        try:
            httpd.server_close()
        except Exception:
            pass


# ------------------------------
# Public entry points
# ------------------------------

def run_crawl_mode(
    dataset_name: str,
    logger: ComponentLogger,
    *,
    source: str = "online",
    dry_run: bool = False,
    subfolder: Optional[str] = None,
    throttle: Optional[float] = None,
    limit: Optional[int] = None,
    outdir: Optional[str] = None,
) -> int:
    """Execute crawl mode: discover URLs and write JSONL manifests."""
    try:
        logger.info("Runner: crawl mode start", extra={
            "component": "PIPELINE",
            "structured_data": {
                "mode": "crawl",
                "dataset": dataset_name,
                "source": source,
                "dry_run": dry_run,
                "subfolder": subfolder,
                "throttle": throttle,
                "limit": limit,
                "outdir": outdir or None,
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

        # Resolve subfolder mapping (first-level subpaths)
        subfolders_map: Dict[str, str] = _get(cfg, "crawler.subfolders") or {}
        subfolder_value: Optional[str] = None
        if subfolder:
            subfolder_value = subfolders_map.get(subfolder, subfolder)

        # ONLINE base url (joined): base_url + root_path
        base_url_online = (
            (_get(cfg, "crawler.base_url") or "").rstrip("/")
            + "/"
            + str(_get(cfg, "crawler.root_path") or "").lstrip("/")
        )
        if not base_url_online.endswith("/"):
            base_url_online += "/"

        # Build per-source base_url for crawling
        canonical_base_url: str = _get(cfg, "crawler.canonical_base_url") or "https://opendata.dwd.de/"
        include_extensions: List[str] = list(_get(cfg, "crawler.include_extensions") or [".zip"])  # type: ignore[list-item]
        include_extensions = [e.lower() for e in include_extensions]
        sample_size: int = int(_get(cfg, "crawler.sample_size") or 100)

        output_jsonl = _resolve_urls_file(cfg)

        # Determine base_url for crawler
        if source == "online":
            crawl_base_url = base_url_online
            if subfolder_value:
                crawl_base_url = urljoin(crawl_base_url, subfolder_value if subfolder_value.endswith("/") else subfolder_value + "/")
        elif source == "offline":
            offline_root = _get(cfg, "crawler.offline_server_root")
            offline_relpath = _get(cfg, "crawler.offline_relpath")
            if not offline_root or not offline_relpath:
                logger.error("Offline source requested but 'offline_server_root' or 'offline_relpath' missing in YAML", extra={
                    "component": "PIPELINE",
                    "structured_data": {"offline_server_root": offline_root, "offline_relpath": offline_relpath},
                })
                return 1
            root_path = Path(offline_root)
            if not root_path.exists():
                logger.error(f"Offline server root does not exist: {root_path}")
                return 1

            with serve_directory_http(root_path) as (host, port):
                base = f"http://{host}:{port}/"
                crawl_base_url = base + str(offline_relpath).lstrip("/")
                if not crawl_base_url.endswith("/"):
                    crawl_base_url += "/"
                if subfolder_value:
                    crawl_base_url = urljoin(crawl_base_url, subfolder_value if subfolder_value.endswith("/") else subfolder_value + "/")

                if dry_run:
                    logger.info("Dry-run (offline): would crawl dataset", extra={
                        "component": "PIPELINE",
                        "structured_data": {
                            "dataset": dataset_name,
                            "root_listing_url": crawl_base_url,
                            "output_jsonl": str(output_jsonl),
                            "throttle": throttle,
                            "limit": limit,
                            "outdir": outdir or None,
                        },
                    })
                    return 0

                result: CrawlerCrawlResult = crawl_dwd_repository(
                    config=cfg,
                    logger=logger,
                    base_url=crawl_base_url,
                    canonical_base_url=canonical_base_url,
                    include_extensions=include_extensions,
                    sample_size=sample_size,
                    throttle=throttle,
                    limit=limit,
                )
        else:
            logger.error(f"Unknown source: {source}")
            return 1

        # ONLINE execution path (not inside context manager)
        if source == "online":
            if dry_run:
                logger.info("Dry-run: would crawl dataset", extra={
                    "component": "PIPELINE",
                    "structured_data": {
                        "dataset": dataset_name,
                        "root_listing_url": crawl_base_url,
                        "output_jsonl": str(output_jsonl),
                        "throttle": throttle,
                        "limit": limit,
                        "outdir": outdir or None,
                    },
                })
                return 0

            result: CrawlerCrawlResult = crawl_dwd_repository(
                config=cfg,
                logger=logger,
                base_url=crawl_base_url,
                canonical_base_url=canonical_base_url,
                include_extensions=include_extensions,
                sample_size=sample_size,
                throttle=throttle,
                limit=limit,
            )

        # If --outdir provided, move the two crawler-native files there (atomic)
        if outdir:
            try:
                out_dir = Path(outdir)
                src_urls = Path(result.output_files.get("urls") or "")
                src_sample = Path(result.output_files.get("sample") or "")
                if not src_urls.exists() or not src_sample.exists():
                    raise FileNotFoundError("crawler outputs not found to move")
                out_dir.mkdir(parents=True, exist_ok=True)
                dest_urls = out_dir / src_urls.name
                dest_sample = out_dir / src_sample.name
                os.replace(src_urls, dest_urls)
                os.replace(src_sample, dest_sample)
                logger.info("Moved crawler outputs to custom outdir", extra={
                    "component": "PIPELINE",
                    "structured_data": {
                        "outdir": str(out_dir.resolve()),
                        "urls": str(dest_urls.resolve()),
                        "sample": str(dest_sample.resolve()),
                    },
                })
            except Exception as move_err:
                logger.error("Failed to place outputs into --outdir", extra={
                    "component": "PIPELINE",
                    "structured_data": {
                        "outdir": outdir,
                        "error": str(move_err),
                    },
                })
                return 1

        logger.info("Crawl summary", extra={
            "component": "PIPELINE",
            "structured_data": {
                "files_found": result.files_found,
                "files_written": result.files_written,
                "files_skipped": result.files_skipped,
                "requests": result.crawled_count,
                "crawler_output": str(result.output_files.get("urls")),
                "sample_output": str(result.output_files.get("sample")),
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
    """Execute download mode: plan and download files."""
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
    p.add_argument("--limit", type=int, default=None, help="Limit items (crawl or max downloads)")
    p.add_argument("--throttle", type=float, default=None, help="Seconds to sleep between requests/downloads")
    p.add_argument("--outdir", type=str, default=None, help="If set (crawl mode only), move crawler outputs to this directory")
    p.add_argument("--source", choices=["online", "offline"], default="online", help="Crawl source: real DWD (online) or golden server (offline)")
    return p


def main() -> int:
    """
    Pipeline CLI entry point.
    """
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
            source=args.source,
            dry_run=args.dry_run,
            subfolder=args.subfolder,
            throttle=args.throttle,
            limit=args.limit,
            outdir=args.outdir,
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
        logger.error("Unknown mode requested", extra={"component": "PIPELINE", "structured_data": {"mode": args.mode}})
        return 1


if __name__ == "__main__":
    sys.exit(main())