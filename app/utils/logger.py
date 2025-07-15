"""
Logging Utility with Overwrite Support for ClimaStation Pipeline

This module provides centralized logging functionality for the weather data processing pipeline.
It creates structured log files with timestamps and appropriate formatting, supporting both
append and overwrite modes for different use cases.

AUTHOR: ClimaStation Backend Pipeline
VERSION: Enhanced for production use
LAST UPDATED: 2025-01-15

KEY FEATURES:
- File-based logging with UTF-8 encoding for international characters
- Configurable log levels (DEBUG, INFO, WARNING, ERROR)
- Prevents duplicate handlers when called multiple times
- Support for both append and overwrite modes
- Automatic directory creation
- Clean log file management

EXPECTED OUTPUT:
CLIMASTATION-BACKEND/
├── data/
│   └── 0_debug/
│       ├── parse_10_minutes_air_temperature_hist.debug.log
│       ├── jsonl_to_pretty_json.debug.log
│       └── [other_script_name].debug.log

USAGE:
    from app.utils.logger import setup_logger, get_logger, clear_log_file
    
    # Set up a new logger (overwrites existing log)
    logger = setup_logger(Path("data/0_debug/my_script.log"), overwrite=True)
    
    # Get an existing logger
    logger = get_logger("my_logger")
    
    # Clear a log file
    clear_log_file(Path("data/0_debug/old_log.log"))
"""

import logging
from pathlib import Path
from typing import Optional


def setup_logger(
    log_path: Path, 
    level: int = logging.DEBUG, 
    logger_name: str = "climastation_logger", 
    overwrite: bool = False
) -> logging.Logger:
    """
    Set up a file-based logger with proper formatting and UTF-8 encoding.
    
    This function creates a logger that writes to both file and optionally console.
    It handles directory creation, prevents duplicate handlers, and supports both
    append and overwrite modes for flexible log management.
    
    Args:
        log_path: Path where log file should be created (directories created automatically)
        level: Logging level (default: DEBUG for detailed logging)
        logger_name: Name for the logger instance (default: "climastation_logger")
        overwrite: If True, overwrites existing log file; if False, appends (default: False)
        
    Returns:
        Configured logger instance ready for use
        
    Example:
        # Create a fresh log file for a new run
        logger = setup_logger(Path("data/0_debug/parser.log"), overwrite=True)
        logger.info("Processing started")
        
        # Append to existing log file
        logger = setup_logger(Path("data/0_debug/parser.log"), overwrite=False)
        logger.info("Additional processing")
    """
    # Ensure log directory exists (create parent directories if needed)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    # Get or create logger with specified name
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

    # Clear existing handlers if we want to overwrite or if this is a fresh setup
    # This prevents duplicate log entries when the same logger is set up multiple times
    if overwrite or not logger.handlers:
        # Remove and close existing handlers to prevent resource leaks
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
            handler.close()
        
        # Determine file mode based on overwrite setting
        file_mode = 'w' if overwrite else 'a'
        
        # Create file handler with UTF-8 encoding for international characters
        file_handler = logging.FileHandler(log_path, mode=file_mode, encoding='utf-8')
        
        # Create formatter with timestamp and clear message format
        formatter = logging.Formatter(
            '%(asctime)s — %(levelname)s — %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        # Optional: Add console handler for immediate feedback
        # Uncomment the following lines if you want console output too:
        # console_handler = logging.StreamHandler()
        # console_handler.setFormatter(formatter)
        # logger.addHandler(console_handler)
        
        # Log the logger setup (only when creating new handlers)
        if overwrite:
            logger.debug(f"Logger '{logger_name}' initialized with fresh log file: {log_path}")
        else:
            logger.debug(f"Logger '{logger_name}' initialized, appending to: {log_path}")

    return logger


def get_logger(logger_name: str = "climastation_logger") -> logging.Logger:
    """
    Get an existing logger instance by name.
    
    This function retrieves a previously configured logger. If the logger
    doesn't exist, it returns a basic logger instance. This is useful for
    accessing the same logger from multiple modules.
    
    Args:
        logger_name: Name of the logger to retrieve (default: "climastation_logger")
        
    Returns:
        Existing logger instance or new basic logger if not found
        
    Example:
        # In main script
        logger = setup_logger(Path("data/0_debug/main.log"))
        
        # In another module
        logger = get_logger()  # Gets the same logger instance
        logger.info("Message from another module")
    """
    logger = logging.getLogger(logger_name)
    
    # If logger has no handlers, it hasn't been set up yet
    if not logger.handlers:
        # Return a basic logger with warning
        logger.setLevel(logging.WARNING)
        logger.warning(f"Logger '{logger_name}' requested but not set up. Use setup_logger() first.")
    
    return logger


def clear_log_file(log_path: Path) -> bool:
    """
    Clear/delete an existing log file.
    
    This function safely removes a log file if it exists. Useful for
    cleaning up old logs or ensuring fresh starts for new processing runs.
    
    Args:
        log_path: Path to the log file to clear/delete
        
    Returns:
        True if file was cleared/deleted successfully, False if file didn't exist
        
    Example:
        # Clear old log before starting new processing
        if clear_log_file(Path("data/0_debug/old_process.log")):
            print("Old log file cleared")
        else:
            print("No old log file to clear")
    """
    try:
        if log_path.exists():
            log_path.unlink()
            return True
        return False
    except (OSError, PermissionError) as e:
        # Handle cases where file is locked or permission denied
        print(f"Warning: Could not clear log file {log_path}: {e}")
        return False


def setup_console_logger(logger_name: str = "console_logger", level: int = logging.INFO) -> logging.Logger:
    """
    Set up a console-only logger for immediate feedback.
    
    This function creates a logger that only outputs to console, useful for
    scripts that need immediate feedback without file logging.
    
    Args:
        logger_name: Name for the console logger (default: "console_logger")
        level: Logging level (default: INFO for general messages)
        
    Returns:
        Console-only logger instance
        
    Example:
        # For quick debugging or user feedback
        console_logger = setup_console_logger()
        console_logger.info("Quick status update")
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    
    # Clear existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()
    
    # Add only console handler
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(levelname)s — %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger


# Module-level convenience functions for common use cases
def quick_file_logger(log_file_name: str, overwrite: bool = True) -> logging.Logger:
    """
    Quickly set up a file logger in the standard debug directory.
    
    Args:
        log_file_name: Name of the log file (e.g., "my_script.log")
        overwrite: Whether to overwrite existing log (default: True)
        
    Returns:
        Configured file logger
        
    Example:
        logger = quick_file_logger("data_processing.log")
        logger.info("Processing started")
    """
    log_path = Path("data/0_debug") / log_file_name
    return setup_logger(log_path, overwrite=overwrite)


if __name__ == "__main__":
    """
    Test the logger functionality when run directly.
    
    This provides a simple test to verify that the logger is working correctly.
    Run this file directly to test: python -m app.utils.logger
    """
    print("Testing ClimaStation Logger...")
    
    # Test file logger
    test_log_path = Path("data/0_debug/logger_test.log")
    logger = setup_logger(test_log_path, overwrite=True)
    
    logger.debug("Debug message test")
    logger.info("Info message test")
    logger.warning("Warning message test")
    logger.error("Error message test")
    
    print(f"✅ Test log created at: {test_log_path}")
    
    # Test console logger
    console_logger = setup_console_logger()
    console_logger.info("Console logger test - you should see this message")
    
    print("✅ Logger testing complete")