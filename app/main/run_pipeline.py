import argparse
import sys
import os
from pathlib import Path
from typing import Optional
import traceback

# Add project root to Python path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Import ClimaStation utilities
try:
    from app.pipeline.downloader import run_downloader
    from app.pipeline.crawler import crawl_dwd_repository
    from app.utils.config_manager import load_config, ConfigurationError
    from app.utils.enhanced_logger import get_logger, StructuredLoggerAdapter
except ImportError as e:
    print(f"❌ Failed to import ClimaStation utilities: {e}")
    print("Make sure you're running from the project root directory.")
    sys.exit(1)


def setup_argument_parser():
    parser = argparse.ArgumentParser(description="Run the data processing pipeline.")
    parser.add_argument(
        '--dataset',
        type=str,
        required=True,
        help="Name of the dataset configuration to use"
    )
    parser.add_argument(
        '--mode',
        type=str,
        choices=['crawl', 'download', 'process'],
        required=True,
        help="Mode of operation: crawl, download, or process"
    )
    parser.add_argument(
        '--dry-run',
        dest='dry_run',
        action='store_true',
        help="Validate setup without performing actual operations"
    )
    parser.add_argument(
        '--subfolder',
        type=str,
        choices=['meta_data', 'historical', 'recent', 'now'],
        help="Subfolder under dataset to download (required for download mode)"
    )
    parser.add_argument(
        '--max-downloads',
        dest='max_downloads',
        type=int,
        help="Maximum number of files to download (optional)"
    )
    parser.add_argument(
        '--throttle',
        dest='throttle',
        type=float,
        help="Seconds to wait between each HTTP request (optional)"
    )
    return parser


def run_crawl_mode(
    dataset_name: str,
    logger: StructuredLoggerAdapter,
    dry_run: bool = False,
    throttle: Optional[float] = None
) -> int:
    """
    Execute crawl mode to discover DWD repository structure.
    """
    try:
        logger.info("Starting crawl mode", extra={
            "component": "CRAWLER",
            "structured_data": {
                "dataset": dataset_name,
                "mode": "crawl",
                "dry_run": dry_run,
                "throttle": throttle
            }
        })

        # Load dataset configuration
        logger.info("Loading dataset configuration", extra={
            "component": "CRAWLER",
            "structured_data": {"dataset": dataset_name}
        })
        config = load_config(dataset_name, logger.logger)
        config['name'] = dataset_name

        # Derive crawler scope from the dataset's `paths:` section
        dataset_paths = config.get('paths', {})
        if not dataset_paths:
            raise ConfigurationError("Dataset YAML must include a top-level 'paths:' mapping")

        # Compute the common parent (e.g. "10_minutes/air_temperature")
        common_parent = os.path.commonpath([str(p) for p in dataset_paths.values()])

        # INFORM crawler of dataset root under base_url
        config['base_path'] = common_parent.rstrip('/') + '/'

        # Inject into crawler config for fallback
        crawler_cfg = config.setdefault('crawler', {})
        crawler_cfg['dataset_path'] = common_parent.rstrip('/') + '/'
        # subpaths are individual folder names
        crawler_cfg['subpaths'] = [
            Path(str(p)).name.rstrip('/') + '/' for p in dataset_paths.values()
        ]

        logger.info("Configuration loaded successfully", extra={
            "component": "CRAWLER",
            "structured_data": {
                "dataset": dataset_name,
                "base_path": config['base_path'],
                "subpaths": crawler_cfg['subpaths']
            }
        })

        if dry_run:
            print("✅ Dry run completed - configuration is valid")
            return 0

        # Execute crawling
        logger.info("Starting DWD repository crawl", extra={
            "component": "CRAWLER",
            "structured_data": {"dataset": dataset_name}
        })
        result = crawl_dwd_repository(config, logger, throttle=throttle)

        # Print success summary
        print("\n" + "="*60)
        if result.url_records:
            print("🎉 CRAWL COMPLETED SUCCESSFULLY")
        else:
            print("⚠️  CRAWL COMPLETED WITH NO DATA FOUND")
        print("="*60)
        print(f"📁 URLs discovered: {len(result.url_records):,}")
        print(f"⏱️  Elapsed time: {result.elapsed_time:.2f} seconds")
        print(f"💾 URL records file: {result.output_files['urls']}" )

        logger.info("Crawl mode completed", extra={
            "component": "CRAWLER",
            "structured_data": {
                "dataset": dataset_name,
                "urls_file": str(result.output_files['urls']),
                "url_records": len(result.url_records),
                "elapsed_time": result.elapsed_time
            }
        })
        return 0

    except ConfigurationError as e:
        logger.error(f"Configuration error in crawl mode: {e}", extra={
            "component": "CRAWLER",
            "structured_data": {"dataset": dataset_name, "error": str(e)}
        })
        print(f"❌ Configuration error: {e}")
        return 1

    except Exception as e:
        logger.error(f"Crawl mode failed: {e}", extra={
            "component": "CRAWLER",
            "structured_data": {"dataset": dataset_name, "error": str(e)}
        })
        print(f"❌ Crawl failed: {e}")
        if logger.logger.level <= 10:  # DEBUG
            traceback.print_exc()
        return 1


def run_download_mode(
    dataset_name: str,
    logger: StructuredLoggerAdapter,
    dry_run: bool = False,
    subfolder: Optional[str] = None,
    max_downloads: Optional[int] = None,
    throttle: Optional[float] = None
) -> int:
    """
    Execute download mode to fetch DWD data files.
    """
    if not subfolder:
        print("❌ Error: --subfolder is required for download mode")
        return 1

    try:
        logger.info("Starting download mode", extra={
            "component": "DOWNLOADER",
            "structured_data": {
                "dataset": dataset_name,
                "mode": "download",
                "dry_run": dry_run,
                "subfolder": subfolder,
                "max_downloads": max_downloads,
                "throttle": throttle
            }
        })

        # Load dataset configuration
        logger.info("Loading dataset configuration", extra={
            "component": "DOWNLOADER",
            "structured_data": {"dataset": dataset_name}
        })
        config = load_config(dataset_name, logger.logger)
        config['name'] = dataset_name
        downloader_cfg = config.setdefault('downloader', {})
        downloader_cfg['subfolder'] = subfolder

        logger.info("Configuration loaded successfully", extra={
            "component": "DOWNLOADER",
            "structured_data": {
                "dataset": dataset_name,
                "downloader_config": downloader_cfg,
                "max_downloads": max_downloads,
                "throttle": throttle
            }
        })

        if dry_run:
            print("✅ Dry run completed - configuration is valid")
            return 0

        # Execute file downloading
        result = run_downloader(config, logger, max_downloads, throttle)

        # Print summary
        print("\n" + "="*60)
        if result.success:
            print("🎉 DOWNLOAD COMPLETED SUCCESSFULLY")
        else:
            print("⚠️  DOWNLOAD COMPLETED WITH ISSUES")
        print("="*60)
        print(f"📁 Files processed: {result.files_processed:,}")
        print(f"❌ Files failed: {result.files_failed:,}")
        print(f"📊 Files skipped: {result.metadata.get('files_skipped', 0):,}")
        print(f"⏱️  Elapsed time: {result.metadata.get('elapsed_time', 0):.2f} seconds")
        print(f"📈 Success rate: {result.metadata.get('success_rate', 0):.1f}%")
        print(f"💾 Download root: {result.metadata.get('download_root')}")

        if result.warnings:
            print("\n⚠️  Warnings:")
            for w in result.warnings:
                print(f"   - {w}")

        if result.errors:
            print("\n❌ Errors:")
            for e in result.errors[:5]:
                print(f"   - {e}")
            if len(result.errors) > 5:
                print(f"   ... and {len(result.errors) - 5} more errors")

        logger.info("Download mode completed", extra={
            "component": "DOWNLOADER",
            "structured_data": {
                "dataset": dataset_name,
                "files_processed": result.files_processed,
                "files_failed": result.files_failed,
                "files_skipped": result.metadata.get('files_skipped', 0),
                "elapsed_time": result.metadata.get('elapsed_time', 0),
                "success_rate": result.metadata.get('success_rate', 0)
            }
        })
        return 0 if result.success else 1

    except ConfigurationError as e:
        logger.error(f"Configuration error in download mode: {e}", extra={
            "component": "DOWNLOADER",
            "structured_data": {"dataset": dataset_name, "error": str(e)}
        })
        print(f"❌ Configuration error: {e}")
        return 1

    except Exception as e:
        logger.error(f"Download mode failed: {e}", extra={
            "component": "DOWNLOADER",
            "structured_data": {"dataset": dataset_name, "error": str(e)}
        })
        print(f"❌ Download failed: {e}")
        if logger.logger.level <= 10:  # DEBUG
            traceback.print_exc()
        return 1


def main():
    parser = setup_argument_parser()
    args = parser.parse_args()

    logger = get_logger("PIPELINE")

    if args.mode == 'crawl':
        sys.exit(run_crawl_mode(
            dataset_name=args.dataset,
            logger=logger,
            dry_run=args.dry_run,
            throttle=args.throttle
        ))
    elif args.mode == 'download':
        sys.exit(run_download_mode(
            dataset_name=args.dataset,
            logger=logger,
            dry_run=args.dry_run,
            subfolder=args.subfolder,
            max_downloads=args.max_downloads,
            throttle=args.throttle
        ))
    elif args.mode == 'process':
        # Process mode logic here
        print("Process mode is not yet implemented.")
        sys.exit(1)
    else:
        print("Invalid mode specified")
        sys.exit(1)


if __name__ == "__main__":
    main()
