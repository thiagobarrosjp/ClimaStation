"""
Logging Utility with Overwrite Support

Sets up file-based logging for the weather data processing pipeline.
Creates structured log files with timestamps and appropriate formatting.
Supports both append and overwrite modes.

Expected Output:
- Log files in specified directory with UTF-8 encoding
- Formatted log entries with timestamp, level, and message

Usage:
- Called by main processing scripts to set up logging
- Supports different log levels (DEBUG, INFO, WARNING, ERROR)
- Prevents duplicate handlers when called multiple times
- Can overwrite existing log files for fresh runs
"""

import logging
from pathlib import Path


def setup_logger(log_path: Path, level: int = logging.DEBUG, logger_name: str = "climastation_logger", overwrite: bool = False) -> logging.Logger:
    """
    Set up a file-based logger with proper formatting.
    
    Args:
        log_path: Path where log file should be created
        level: Logging level (default: DEBUG)
        logger_name: Name for the logger instance
        overwrite: If True, overwrites existing log file; if False, appends (default: False)
        
    Returns:
        Configured logger instance
        
    Note:
        - Creates parent directories if they don't exist
        - Prevents duplicate handlers if called multiple times
        - Uses UTF-8 encoding for international characters
        - Can overwrite or append to existing log files
    """
    # Ensure log directory exists
    log_path.parent.mkdir(parents=True, exist_ok=True)

    # Get or create logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

    # Clear existing handlers if we want to overwrite or if this is a fresh setup
    if overwrite or not logger.handlers:
        # Remove existing handlers
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
            handler.close()
        
        # Determine file mode
        file_mode = 'w' if overwrite else 'a'
        
        # Create file handler with UTF-8 encoding
        file_handler = logging.FileHandler(log_path, mode=file_mode, encoding='utf-8')
        
        # Create formatter with timestamp
        formatter = logging.Formatter(
            '%(asctime)s — %(levelname)s — %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        # Optionally add console handler for immediate feedback
        # Uncomment the following lines if you want console output too:
        # console_handler = logging.StreamHandler()
        # console_handler.setFormatter(formatter)
        # logger.addHandler(console_handler)

    return logger


def get_logger(logger_name: str = "climastation_logger") -> logging.Logger:
    """
    Get an existing logger instance.
    
    Args:
        logger_name: Name of the logger to retrieve
        
    Returns:
        Existing logger instance or new basic logger if not found
    """
    return logging.getLogger(logger_name)


def clear_log_file(log_path: Path) -> bool:
    """
    Clear/delete an existing log file.
    
    Args:
        log_path: Path to the log file to clear
        
    Returns:
        True if file was cleared/deleted, False if file didn't exist
    """
    if log_path.exists():
        log_path.unlink()
        return True
    return False
