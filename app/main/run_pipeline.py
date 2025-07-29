import argparse
import sys
from pathlib import Path
from typing import Optional
import traceback

# Add project root to Python path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Import ClimaStation utilities
try:
    from app.pipeline.downloader import run_downloader
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
        choices=['download', 'process'],
        required=True,
        help="Mode of operation: download or process"
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help="Validate setup without performing actual operations"
    )
    parser.add_argument(
        '--limit',
        type=int,
        help="Limit number of files to process (useful for testing)"
    )
    return parser

def run_download_mode(dataset_name: str, logger: StructuredLoggerAdapter, dry_run: bool = False, limit: Optional[int] = None) -> int:
    """
    Execute download mode to fetch DWD data files.
    
    Args:
        dataset_name: Name of the dataset configuration to use
        logger: Structured logger instance
        dry_run: If True, validate setup without downloading
        limit: Optional limit on number of files to download
        
    Returns:
        Exit code (0 for success, 1 for failure)
    """
    try:
        logger.info("Starting download mode", extra={
            "component": "DOWNLOADER",
            "structured_data": {
                "dataset": dataset_name,
                "mode": "download",
                "dry_run": dry_run,
                "limit": limit
            }
        })
        
        # Load dataset configuration
        logger.info("Loading dataset configuration", extra={
            "component": "DOWNLOADER",
            "structured_data": {"dataset": dataset_name}
        })
        
        config = load_config(dataset_name, logger.logger)
        
        logger.info("Configuration loaded successfully", extra={
            "component": "DOWNLOADER",
            "structured_data": {
                "dataset": dataset_name,
                "downloader_config": config.get('downloader', {}),
                "limit": limit
            }
        })
        
        if dry_run:
            logger.info("Dry run mode - configuration validated successfully", extra={
                "component": "DOWNLOADER",
                "structured_data": {"dataset": dataset_name}
            })
            print("✅ Dry run completed - configuration is valid")
            return 0
        
        # Execute file downloading
        logger.info("Starting DWD file download", extra={
            "component": "DOWNLOADER",
            "structured_data": {"dataset": dataset_name, "limit": limit}
        })
        
        result = run_downloader(config, logger, limit)
        
        # Print success summary
        print("\n" + "="*60)
        if result.success:
            print("🎉 DOWNLOAD COMPLETED SUCCESSFULLY")
        else:
            print("⚠️  DOWNLOAD COMPLETED WITH ISSUES")
        print("="*60)
        print(f"📁 Files downloaded: {result.files_processed:,}")
        print(f"❌ Files failed: {result.files_failed:,}")
        print(f"📊 Files skipped: {result.metadata.get('files_skipped', 0):,}")
        print(f"⏱️  Elapsed time: {result.metadata.get('elapsed_time', 0):.2f} seconds")
        print(f"📈 Success rate: {result.metadata.get('success_rate', 0):.1f}%")
        print(f"💾 Download directory: {result.metadata.get('download_root', 'N/A')}")
        
        # Show warnings if any
        if result.warnings:
            print("\n⚠️  Warnings:")
            for warning in result.warnings:
                print(f"   - {warning}")
        
        # Show errors if any
        if result.errors:
            print("\n❌ Errors:")
            for error in result.errors[:5]:  # Show first 5 errors
                print(f"   - {error}")
            if len(result.errors) > 5:
                print(f"   ... and {len(result.errors) - 5} more errors")
        
        logger.info("Download mode completed", extra={
            "component": "DOWNLOADER",
            "structured_data": {
                "dataset": dataset_name,
                "files_processed": result.files_processed,
                "files_failed": result.files_failed,
                "success": result.success,
                "elapsed_time": result.metadata.get('elapsed_time', 0)
            }
        })
        
        return 0 if result.success else 1
        
    except ConfigurationError as e:
        logger.error(f"Configuration error in download mode: {str(e)}", extra={
            "component": "DOWNLOADER",
            "structured_data": {
                "dataset": dataset_name,
                "error_type": "ConfigurationError",
                "error": str(e)
            }
        })
        print(f"❌ Configuration error: {e}")
        return 1
        
    except Exception as e:
        logger.error(f"Download mode failed: {str(e)}", extra={
            "component": "DOWNLOADER",
            "structured_data": {
                "dataset": dataset_name,
                "error_type": type(e).__name__,
                "error": str(e)
            }
        })
        print(f"❌ Download failed: {e}")
        if logger.logger.level <= 10:  # DEBUG level
            traceback.print_exc()
        return 1

def main():
    parser = setup_argument_parser()
    args = parser.parse_args()
    
    component_name = "PIPELINE"
    logger = get_logger(component_name)
    
    if args.mode == 'download':
        return run_download_mode(args.dataset, logger, args.dry_run, args.limit)
    elif args.mode == 'process':
        # Process mode logic here
        pass
    else:
        print("Invalid mode specified")
        return 1

if __name__ == "__main__":
    exit(main())
