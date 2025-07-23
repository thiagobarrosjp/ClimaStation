#!/usr/bin/env python3
"""
ClimaStation Bulk Ingestion Runner

SCRIPT IDENTIFICATION: DWD10TAH0T (Main Entry Point)

PURPOSE:
Main entry point for the ClimaStation bulk historical data ingestion pipeline.
Provides command-line interface for running bulk ingestion across all datasets
with comprehensive configuration, monitoring, and error handling capabilities.
"""

import argparse
import signal
import sys
import time
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Dict, Any
import logging

# Add the project root to Python path so we can import from app
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Import ClimaStation components
from app.orchestrators.bulk_ingestion_controller import BulkIngestionController, ProcessingMode, ProcessingResult
from app.utils.config_manager import ConfigManager, ConfigurationError
from app.utils.progress_tracker import ProgressTracker
from app.utils.enhanced_logger import setup_logger, configure_root_logger, LogConfig


class BulkIngestionRunner:
    """
    Main runner class for bulk ingestion operations.
    
    Handles command-line interface, process coordination, and provides
    comprehensive monitoring and error handling for the ingestion pipeline.
    """
    
    def __init__(self):
        """Initialize the bulk ingestion runner."""
        self.controller: Optional[BulkIngestionController] = None
        self.logger: Optional[logging.Logger] = None
        self.interrupted = False
        self.start_time: Optional[datetime] = None
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle interrupt signals for graceful shutdown."""
        if not self.interrupted:
            self.interrupted = True
            if self.logger:
                self.logger.warning("🛑 Interrupt signal received. Initiating graceful shutdown...")
                self.logger.info("Please wait for current operations to complete safely.")
            else:
                print("\n🛑 Interrupt signal received. Shutting down gracefully...")
        else:
            if self.logger:
                self.logger.error("🚨 Force shutdown requested. Exiting immediately.")
            else:
                print("🚨 Force shutdown. Exiting now.")
            sys.exit(1)
    
    def setup_logging(self, verbose: bool = False, quiet: bool = False, log_dir: Optional[Path] = None):
        """
        Setup logging configuration for the runner.
        
        Args:
            verbose: Enable verbose (DEBUG) logging
            quiet: Suppress non-essential output
            log_dir: Directory for log files
        """
        # Determine log level
        if quiet:
            log_level = "WARNING"
        elif verbose:
            log_level = "DEBUG"
        else:
            log_level = "INFO"
        
        # Configure root logger
        log_config = LogConfig(
            level=log_level,
            console_output=not quiet,
            file_output=True,
            log_dir=log_dir or Path("logs")
        )
        configure_root_logger(log_config)
        
        # Setup component logger
        self.logger = setup_logger("DWD10TAH0T", "bulk_ingestion_runner", log_config)
        
        self.logger.info("=" * 80)
        self.logger.info("🚀 CLIMASTATION BULK INGESTION RUNNER")
        self.logger.info("=" * 80)
        self.logger.info(f"Log level: {log_level}")
        self.logger.info(f"Start time: {datetime.now()}")
    
    def _log_or_print(self, level: str, message: str):
        """Log message if logger exists, otherwise print to console."""
        if self.logger:
            getattr(self.logger, level.lower())(message)
        else:
            print(f"{level.upper()}: {message}")
    
    def validate_configuration(self, config_dir: Optional[Path] = None) -> bool:
        """
        Validate all configuration files.
        
        Args:
            config_dir: Directory containing configuration files
        
        Returns:
            True if all configurations are valid, False otherwise
        """
        # Ensure we have a logger for validation
        if self.logger is None:
            # Create a temporary logger for validation
            temp_logger = logging.getLogger("validation")
            temp_logger.setLevel(logging.INFO)
            if not temp_logger.handlers:
                handler = logging.StreamHandler()
                formatter = logging.Formatter('%(levelname)s - %(message)s')
                handler.setFormatter(formatter)
                temp_logger.addHandler(handler)
            logger = temp_logger
        else:
            logger = self.logger
        
        logger.info("🔍 Validating configuration files...")
        
        try:
            # Default to app/config directory
            if config_dir is None:
                config_dir = Path(__file__).parent.parent / "config"
            
            config_manager = ConfigManager(str(config_dir))
            
            # Validate base configuration
            base_config = config_manager.get_validated_base_config()
            logger.info("✅ Base configuration validated")
            
            # Validate dataset configurations
            available_datasets = ["10_minutes_air_temperature"]  # Add more as implemented
            
            for dataset in available_datasets:
                try:
                    dataset_config = config_manager.get_validated_dataset_config(dataset)
                    logger.info(f"✅ Dataset configuration validated: {dataset}")
                except ConfigurationError as e:
                    logger.error(f"❌ Dataset configuration invalid: {dataset} - {e}")
                    return False
            
            # Validate paths exist
            required_paths = [
                Path(base_config['paths']['progress_db']).parent,
                Path(base_config['paths']['log_dir'])
            ]
            
            for path in required_paths:
                try:
                    path.mkdir(parents=True, exist_ok=True)
                    logger.info(f"✅ Path validated: {path}")
                except Exception as e:
                    logger.error(f"❌ Cannot create path: {path} - {e}")
                    return False
            
            logger.info("✅ All configurations validated successfully")
            return True
            
        except ConfigurationError as e:
            logger.error(f"❌ Configuration validation failed: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Unexpected error during validation: {e}")
            return False
    
    def show_status(self, config_dir: Optional[Path] = None) -> Dict[str, Any]:
        """
        Show current processing status.
        
        Args:
            config_dir: Directory containing configuration files
            
        Returns:
            Dictionary with status information
        """
        # Ensure we have a logger for status
        if self.logger is None:
            # Create a temporary logger for status
            temp_logger = logging.getLogger("status")
            temp_logger.setLevel(logging.INFO)
            if not temp_logger.handlers:
                handler = logging.StreamHandler()
                formatter = logging.Formatter('%(levelname)s - %(message)s')
                handler.setFormatter(formatter)
                temp_logger.addHandler(handler)
            logger = temp_logger
        else:
            logger = self.logger
        
        logger.info("📊 Retrieving processing status...")
        
        try:
            # Default to app/config directory
            if config_dir is None:
                config_dir = Path(__file__).parent.parent / "config"
            
            config_manager = ConfigManager(str(config_dir))
            base_config = config_manager.get_validated_base_config()
            
            progress_db_path = Path(base_config['paths']['progress_db'])
            progress_tracker = ProgressTracker(progress_db_path)
            
            # Get database stats
            db_stats = progress_tracker.get_database_stats()
            
            # Get dataset summaries
            dataset_summaries = {}
            for dataset in db_stats.get('datasets', []):
                summary = progress_tracker.get_processing_summary(dataset)
                dataset_summaries[dataset] = summary
            
            status = {
                'timestamp': datetime.now().isoformat(),
                'database_stats': db_stats,
                'dataset_summaries': dataset_summaries
            }
            
            # Log status summary
            logger.info("📊 PROCESSING STATUS SUMMARY")
            logger.info("=" * 60)
            logger.info(f"Total records: {db_stats.get('total_records', 0):,}")
            logger.info(f"Database size: {db_stats.get('database_size_mb', 0):.1f} MB")
            logger.info(f"Datasets: {len(db_stats.get('datasets', []))}")
            
            for dataset, summary in dataset_summaries.items():
                status_counts = summary['status_counts']
                total = summary['total_files']
                success_rate = summary['success_rate']
                
                logger.info(f"  📁 {dataset}:")
                logger.info(f"    Total files: {total:,}")
                logger.info(f"    Success rate: {success_rate:.1f}%")
                logger.info(f"    Status: {status_counts}")
            
            return status
            
        except Exception as e:
            logger.error(f"❌ Error retrieving status: {e}")
            return {"error": str(e)}
    
    def reset_failed_files(self, datasets: List[str], config_dir: Optional[Path] = None) -> bool:
        """
        Reset failed files for reprocessing.
        
        Args:
            datasets: List of datasets to reset
            config_dir: Directory containing configuration files
            
        Returns:
            True if reset was successful, False otherwise
        """
        # Ensure we have a logger
        if self.logger is None:
            temp_logger = logging.getLogger("reset_failed")
            temp_logger.setLevel(logging.INFO)
            if not temp_logger.handlers:
                handler = logging.StreamHandler()
                formatter = logging.Formatter('%(levelname)s - %(message)s')
                handler.setFormatter(formatter)
                temp_logger.addHandler(handler)
            logger = temp_logger
        else:
            logger = self.logger
            
        logger.info(f"🔄 Resetting failed files for datasets: {datasets}")
        
        try:
            # Default to app/config directory
            if config_dir is None:
                config_dir = Path(__file__).parent.parent / "config"
            
            config_manager = ConfigManager(str(config_dir))
            base_config = config_manager.get_validated_base_config()
            
            progress_db_path = Path(base_config['paths']['progress_db'])
            progress_tracker = ProgressTracker(progress_db_path)
            
            total_reset = 0
            for dataset in datasets:
                reset_count = progress_tracker.reset_failed_files(dataset)
                total_reset += reset_count
                logger.info(f"✅ Reset {reset_count} failed files for {dataset}")
            
            logger.info(f"✅ Total files reset: {total_reset}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error resetting failed files: {e}")
            return False
    
    def reset_stuck_files(self, datasets: List[str], config_dir: Optional[Path] = None, timeout_hours: float = 2.0) -> bool:
        """
        Reset stuck files (from crashed workers).
        
        Args:
            datasets: List of datasets to reset
            config_dir: Directory containing configuration files
            timeout_hours: Hours after which files are considered stuck
            
        Returns:
            True if reset was successful, False otherwise
        """
        # Ensure we have a logger
        if self.logger is None:
            temp_logger = logging.getLogger("reset_stuck")
            temp_logger.setLevel(logging.INFO)
            if not temp_logger.handlers:
                handler = logging.StreamHandler()
                formatter = logging.Formatter('%(levelname)s - %(message)s')
                handler.setFormatter(formatter)
                temp_logger.addHandler(handler)
            logger = temp_logger
        else:
            logger = self.logger
            
        logger.info(f"🔄 Resetting stuck files for datasets: {datasets} (timeout: {timeout_hours}h)")
        
        try:
            # Default to app/config directory
            if config_dir is None:
                config_dir = Path(__file__).parent.parent / "config"
            
            config_manager = ConfigManager(str(config_dir))
            base_config = config_manager.get_validated_base_config()
            
            progress_db_path = Path(base_config['paths']['progress_db'])
            progress_tracker = ProgressTracker(progress_db_path)
            
            total_reset = 0
            for dataset in datasets:
                reset_count = progress_tracker.reset_stuck_files(dataset, timeout_hours)
                total_reset += reset_count
                logger.info(f"✅ Reset {reset_count} stuck files for {dataset}")
            
            logger.info(f"✅ Total stuck files reset: {total_reset}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error resetting stuck files: {e}")
            return False
    
    def cleanup_old_records(self, config_dir: Optional[Path] = None, days_old: int = 30) -> bool:
        """
        Clean up old processing records.
        
        Args:
            config_dir: Directory containing configuration files
            days_old: Remove records older than this many days
            
        Returns:
            True if cleanup was successful, False otherwise
        """
        # Ensure we have a logger
        if self.logger is None:
            temp_logger = logging.getLogger("cleanup")
            temp_logger.setLevel(logging.INFO)
            if not temp_logger.handlers:
                handler = logging.StreamHandler()
                formatter = logging.Formatter('%(levelname)s - %(message)s')
                handler.setFormatter(formatter)
                temp_logger.addHandler(handler)
            logger = temp_logger
        else:
            logger = self.logger
            
        logger.info(f"🧹 Cleaning up records older than {days_old} days...")
        
        try:
            # Default to app/config directory
            if config_dir is None:
                config_dir = Path(__file__).parent.parent / "config"
            
            config_manager = ConfigManager(str(config_dir))
            base_config = config_manager.get_validated_base_config()
            
            progress_db_path = Path(base_config['paths']['progress_db'])
            progress_tracker = ProgressTracker(progress_db_path)
            
            removed_count = progress_tracker.cleanup_old_records(days_old)
            logger.info(f"✅ Cleaned up {removed_count} old records")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error during cleanup: {e}")
            return False
    
    def run_ingestion(
        self,
        datasets: Optional[List[str]] = None,
        mode: ProcessingMode = ProcessingMode.BULK,
        config_dir: Optional[Path] = None,
        max_workers: Optional[int] = None
    ) -> bool:
        """
        Run the bulk ingestion process.
        
        Args:
            datasets: List of datasets to process
            mode: Processing mode
            config_dir: Directory containing configuration files
            max_workers: Maximum number of parallel workers
            
        Returns:
            True if ingestion was successful, False otherwise
        """
        self.start_time = datetime.now()
        
        # Ensure we have a logger
        if self.logger is None:
            temp_logger = logging.getLogger("run_ingestion")
            temp_logger.setLevel(logging.INFO)
            if not temp_logger.handlers:
                handler = logging.StreamHandler()
                formatter = logging.Formatter('%(levelname)s - %(message)s')
                handler.setFormatter(formatter)
                temp_logger.addHandler(handler)
            logger = temp_logger
        else:
            logger = self.logger
        
        try:
            # Default to app/config directory
            if config_dir is None:
                config_dir = Path(__file__).parent.parent / "config"
            
            # Initialize controller
            self.controller = BulkIngestionController(config_dir)

            # Check if controller was successfully initialized
            if self.controller is None:
                logger.error("❌ Failed to initialize BulkIngestionController")
                return False
            
            logger.info("🚀 Starting bulk ingestion process...")
            logger.info(f"Mode: {mode.value}")
            logger.info(f"Datasets: {datasets or 'all available'}")
            logger.info(f"Max workers: {max_workers or 'default'}")
            
            # Check for interruption before starting
            if self.interrupted:
                logger.warning("🛑 Process interrupted before starting")
                return False
            
            # Run ingestion with progress monitoring
            result = self._run_with_monitoring(datasets, mode, max_workers)
            
            # Process results
            if result.overall_result == ProcessingResult.SUCCESS:
                logger.info("🎉 Bulk ingestion completed successfully!")
                return True
            elif result.overall_result == ProcessingResult.PARTIAL_SUCCESS:
                logger.warning("⚠️ Bulk ingestion completed with some failures")
                self._log_recovery_recommendations(result)
                return False
            else:
                logger.error("❌ Bulk ingestion failed")
                self._log_recovery_recommendations(result)
                return False
                
        except KeyboardInterrupt:
            logger.warning("🛑 Process interrupted by user")
            return False
        except Exception as e:
            logger.error(f"💥 Critical error during ingestion: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False
        finally:
            if self.start_time:
                elapsed = (datetime.now() - self.start_time).total_seconds()
                logger.info(f"⏱️ Total runtime: {elapsed:.2f} seconds")
    
    def _run_with_monitoring(self, datasets, mode, max_workers):
        """Run ingestion with progress monitoring."""
        # Safety check for controller
        if self.controller is None:
            raise RuntimeError("Controller not initialized")
        
        # Start ingestion in a way that allows monitoring
        result = self.controller.run_bulk_ingestion(
            datasets=datasets,
            mode=mode,
            max_workers=max_workers
        )
        
        return result
    
    def _log_recovery_recommendations(self, result):
        """Log recovery recommendations based on failure patterns."""
        # Ensure we have a logger
        if self.logger is None:
            temp_logger = logging.getLogger("recovery")
            temp_logger.setLevel(logging.INFO)
            if not temp_logger.handlers:
                handler = logging.StreamHandler()
                formatter = logging.Formatter('%(levelname)s - %(message)s')
                handler.setFormatter(formatter)
                temp_logger.addHandler(handler)
            logger = temp_logger
        else:
            logger = self.logger
            
        logger.info("🔧 RECOVERY RECOMMENDATIONS")
        logger.info("=" * 50)
        
        failed_datasets = [r for r in result.datasets_processed if not r.success]
        
        if failed_datasets:
            logger.info("To retry failed datasets:")
            for dataset_result in failed_datasets:
                logger.info(f"  python app/main/run_bulk_ingestion.py --datasets {dataset_result.dataset_name} --reset-failed")
        
        logger.info("To check detailed status:")
        logger.info("  python app/main/run_bulk_ingestion.py --status")
        
        logger.info("To reset stuck files (if workers crashed):")
        logger.info("  python app/main/run_bulk_ingestion.py --reset-stuck --datasets <dataset_name>")


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="ClimaStation Bulk Historical Data Ingestion",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                    # Process all datasets in bulk mode
  %(prog)s --datasets air_temp --mode incremental  # Process specific dataset
  %(prog)s --validate-config                  # Validate configuration only
  %(prog)s --status                          # Show processing status
  %(prog)s --reset-failed --datasets air_temp # Reset failed files
        """
    )
    
    # Main processing options
    parser.add_argument(
        "--datasets",
        type=str,
        help="Comma-separated list of datasets to process (default: all available)"
    )
    
    parser.add_argument(
        "--mode",
        choices=["bulk", "incremental"],
        default="bulk",
        help="Processing mode (default: bulk)"
    )
    
    parser.add_argument(
        "--config-dir",
        type=Path,
        help="Directory containing configuration files (default: app/config)"
    )
    
    parser.add_argument(
        "--max-workers",
        type=int,
        help="Maximum number of parallel workers (default: from config)"
    )
    
    # Utility operations
    parser.add_argument(
        "--validate-config",
        action="store_true",
        help="Validate configuration files only"
    )
    
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show current processing status"
    )
    
    parser.add_argument(
        "--reset-failed",
        action="store_true",
        help="Reset failed files for reprocessing"
    )
    
    parser.add_argument(
        "--reset-stuck",
        action="store_true",
        help="Reset stuck files (from crashed workers)"
    )
    
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Clean up old processing records"
    )
    
    parser.add_argument(
        "--cleanup-days",
        type=int,
        default=30,
        help="Days old for cleanup (default: 30)"
    )
    
    # Logging options
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose (DEBUG) logging"
    )
    
    parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Suppress non-essential output"
    )
    
    parser.add_argument(
        "--log-dir",
        type=Path,
        help="Directory for log files (default: logs)"
    )
    
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_arguments()
    
    # Initialize runner
    runner = BulkIngestionRunner()
    
    # Setup logging
    runner.setup_logging(
        verbose=args.verbose,
        quiet=args.quiet,
        log_dir=args.log_dir
    )
    
    try:
        # Handle utility operations first
        if args.validate_config:
            success = runner.validate_configuration(args.config_dir)
            sys.exit(0 if success else 1)
        
        if args.status:
            runner.show_status(args.config_dir)
            sys.exit(0)
        
        if args.cleanup:
            success = runner.cleanup_old_records(args.config_dir, args.cleanup_days)
            sys.exit(0 if success else 1)
        
        # Parse datasets
        datasets = None
        if args.datasets:
            datasets = [d.strip() for d in args.datasets.split(",")]
        
        # Handle reset operations - Fixed: Use _log_or_print instead of direct logger access
        if args.reset_failed:
            if not datasets:
                runner._log_or_print("error", "❌ --reset-failed requires --datasets to be specified")
                sys.exit(1)
            success = runner.reset_failed_files(datasets, args.config_dir)
            sys.exit(0 if success else 1)
        
        if args.reset_stuck:
            if not datasets:
                runner._log_or_print("error", "❌ --reset-stuck requires --datasets to be specified")
                sys.exit(1)
            success = runner.reset_stuck_files(datasets, args.config_dir)
            sys.exit(0 if success else 1)
        
        # Run main ingestion process
        mode = ProcessingMode.BULK if args.mode == "bulk" else ProcessingMode.INCREMENTAL
        
        success = runner.run_ingestion(
            datasets=datasets,
            mode=mode,
            config_dir=args.config_dir,
            max_workers=args.max_workers
        )
        
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        if runner.logger:
            runner.logger.warning("🛑 Process interrupted by user")
        else:
            print("\n🛑 Process interrupted by user")
        sys.exit(130)  # Standard exit code for Ctrl+C
    
    except Exception as e:
        if runner.logger:
            runner.logger.error(f"💥 Unexpected error: {e}")
        else:
            print(f"💥 Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
