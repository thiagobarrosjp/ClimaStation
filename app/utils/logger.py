"""
Enhanced logging utility for ClimaStation weather data processing pipeline.

Purpose:
- Provides structured logging with consistent formatting
- Supports both console and file output with different log levels
- Creates timestamped log files in Germany-specific debug directory
- Handles log rotation and cleanup

Input: Script name, optional log file path
Output: Configured logger instance, log files in data/germany/0_debug/

Usage:
    from app.utils.logger import setup_logger
    logger = setup_logger(script_name="parse_air_temperature")
    logger.info("Processing started")
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional


def setup_logger(
    log_file_path: Optional[Path] = None, 
    script_name: Optional[str] = None,
    console_level: int = logging.INFO,
    file_level: int = logging.DEBUG
) -> logging.Logger:
    """
    Setup enhanced logger with console and file handlers.
    
    Args:
        log_file_path: Optional custom log file path
        script_name: Script name for automatic log file naming
        console_level: Console logging level (default: INFO)
        file_level: File logging level (default: DEBUG)
    
    Returns:
        Configured logger instance
    """
    
    # Create logger
    logger = logging.getLogger('climastation_logger')
    logger.setLevel(logging.DEBUG)
    
    # Clear existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # Determine log file path
    if log_file_path is None and script_name:
        # Use Germany-specific debug folder with static naming
        log_dir = Path("data/germany/0_debug")
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Static filename without timestamp - will overwrite previous version
        log_file_path = log_dir / f"{script_name}.debug.log"
    elif log_file_path is None:
        # Fallback to default location
        log_dir = Path("data/germany/0_debug")
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file_path = log_dir / "climastation.debug.log"
    
    # Create formatters
    console_formatter = logging.Formatter(
        '%(asctime)s — %(levelname)s — %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    file_formatter = logging.Formatter(
        '%(asctime)s — %(levelname)s — %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(console_level)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # File handler - use 'w' mode to overwrite previous log
    if log_file_path:
        file_handler = logging.FileHandler(log_file_path, mode='w', encoding='utf-8')
        file_handler.setLevel(file_level)
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
        
        # Log the initialization
        logger.debug(f"Logger 'climastation_logger' initialized, writing to: {log_file_path}")
    
    return logger


def get_logger() -> logging.Logger:
    """
    Get existing logger instance.
    
    Returns:
        Existing logger or creates new one if none exists
    """
    logger = logging.getLogger('climastation_logger')
    
    if not logger.handlers:
        # No logger configured, create default
        return setup_logger(script_name="default")
    
    return logger


def log_processing_start(logger: logging.Logger, script_name: str, input_path: str) -> None:
    """
    Log standardized processing start message.
    
    Args:
        logger: Logger instance
        script_name: Name of the processing script
        input_path: Input data path
    """
    logger.info("=" * 80)
    logger.info(f"🚀 Starting {script_name}")
    logger.info(f"📁 Input path: {input_path}")
    logger.info(f"🕐 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)


def log_processing_end(logger: logging.Logger, script_name: str, success: bool, duration: float) -> None:
    """
    Log standardized processing end message.
    
    Args:
        logger: Logger instance
        script_name: Name of the processing script
        success: Whether processing was successful
        duration: Processing duration in seconds
    """
    status = "✅ COMPLETED" if success else "❌ FAILED"
    logger.info("=" * 80)
    logger.info(f"{status}: {script_name}")
    logger.info(f"⏱️  Duration: {duration:.2f} seconds")
    logger.info(f"🕐 Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)


def log_file_processing(logger: logging.Logger, file_name: str, file_size: int, records: int) -> None:
    """
    Log standardized file processing message.
    
    Args:
        logger: Logger instance
        file_name: Name of processed file
        file_size: File size in bytes
        records: Number of records processed
    """
    size_mb = file_size / (1024 * 1024)
    logger.info(f"📄 Processed: {file_name}")
    logger.info(f"💾 Size: {size_mb:.2f} MB")
    logger.info(f"📊 Records: {records:,}")


def log_error_with_context(logger: logging.Logger, error: Exception, context: str) -> None:
    """
    Log error with additional context information.
    
    Args:
        logger: Logger instance
        error: Exception that occurred
        context: Additional context about where/when error occurred
    """
    logger.error(f"❌ Error in {context}: {str(error)}")
    logger.debug(f"❌ Full error details: {repr(error)}", exc_info=True)


def log_validation_results(logger: logging.Logger, total: int, valid: int, invalid: int) -> None:
    """
    Log standardized validation results.
    
    Args:
        logger: Logger instance
        total: Total records processed
        valid: Number of valid records
        invalid: Number of invalid records
    """
    success_rate = (valid / total * 100) if total > 0 else 0
    logger.info(f"📊 Validation Results:")
    logger.info(f"   📈 Total records: {total:,}")
    logger.info(f"   ✅ Valid records: {valid:,}")
    logger.info(f"   ❌ Invalid records: {invalid:,}")
    logger.info(f"   📊 Success rate: {success_rate:.1f}%")


# Example usage and testing
if __name__ == "__main__":
    # Test the logger setup
    test_logger = setup_logger(script_name="test_logger")
    
    test_logger.info("Testing logger functionality")
    test_logger.debug("This is a debug message")
    test_logger.warning("This is a warning message")
    test_logger.error("This is an error message")
    
    # Test utility functions
    log_processing_start(test_logger, "test_script", "/test/input/path")
    log_file_processing(test_logger, "test_file.zip", 1024000, 5000)
    log_validation_results(test_logger, 5000, 4950, 50)
    log_processing_end(test_logger, "test_script", True, 45.67)
    
    print(f"Test log written to: data/germany/0_debug/test_logger.debug.log")
