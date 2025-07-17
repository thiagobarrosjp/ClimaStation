"""
Centralized Logging Utility for ClimaStation Weather Data Processing Pipeline

SCRIPT IDENTIFICATION: DWD10TAH3L
- DWD: Deutscher Wetterdienst data source
- 10T: 10-minute air temperature dataset
- AH: Air temperature Historical data
- 3: Station processing component
- L: Logging utility component

PURPOSE:
Provides centralized, consistent logging across all ClimaStation pipeline components
with script identification codes for traceability. Ensures ALL components automatically
join centralized logging and messages appear in true chronological order.

KEY FEATURES:
- Script identification codes for traceability (e.g., DWD10TAH3P, DWD10TAH3T, etc.)
- AUTOMATIC centralized logging detection - all components join automatically
- Thread-safe chronological message ordering with immediate writes
- Consistent log formatting across all pipeline components
- UTF-8 encoding for international characters (German weather data)
- SINGLE shared logger instance to eliminate race conditions
- Comprehensive debugging to track message flow

LOGGING ARCHITECTURE:
- Main script creates the master logger
- ALL child components automatically detect and join centralized logging
- Single shared logger instance ensures perfect chronological order
- Each message shows its script identification code for traceability
- Immediate writes with no buffering or race conditions

EXPECTED OUTPUT STRUCTURE:
data/dwd/0_debug/
└── parse_10_minutes_air_temperature_hist.debug.log (chronological messages from ALL components)

USAGE:
    from app.utils.logger import setup_logger
    
    # Main script - creates master logger
    main_logger = setup_logger("DWD10TAH3P", "parse_10_minutes_air_temperature_hist")
    
    # Child components - automatically join centralized logging
    child_logger = setup_logger("DWD10TAH3T", "raw_parser")
    station_logger = setup_logger("DWD10TAH3I", "station_info_parser")
    # All will write chronologically to the same log

AUTHOR: ClimaStation Backend Pipeline
VERSION: Automatic centralized logging with perfect chronological order
LAST UPDATED: 2025-01-17
"""

import logging
import sys
from pathlib import Path
from typing import Optional, Dict
from datetime import datetime
import re
import threading


# Valid script identification codes for the ClimaStation pipeline
VALID_SCRIPT_CODES = {
    "DWD10TAH3P": "parse_10_minutes_air_temperature_hist.py - Main parsing pipeline",
    "DWD10TAH3T": "raw_parser.py - Raw data processing component",
    "DWD10TAH3S": "sensor_metadata.py - Sensor metadata parsing component",
    "DWD10TAH3I": "station_info_parser.py - Station information parsing component",
    "DWD10TAH3J": "jsonl_to_pretty_json.py - JSON formatting utility component",
    "DWD10TAH3Z": "zip_handler.py - ZIP file handling utility component",
    "DWD10TAH3L": "logger.py - Logging utility component (this file)",
    # Add more codes as needed for new scripts
}

# Global state for centralized logging
_master_logger: Optional[logging.Logger] = None
_centralized_log_file: Optional[Path] = None
_main_script_code: Optional[str] = None
_logger_lock = threading.Lock()  # Thread safety for logger creation


def validate_script_code(script_code: str) -> bool:
    """
    Validate that the script identification code follows the expected format.
    
    Args:
        script_code: Script identification code to validate (e.g., "DWD10TAH3P")
        
    Returns:
        True if code is valid, False otherwise
    """
    if not script_code:
        return False
    
    # Check if it's in our registry of valid codes
    if script_code in VALID_SCRIPT_CODES:
        return True
    
    # Check format: DWD10TAH3 followed by a single letter
    pattern = r'^DWD10TAH3[A-Z]$'
    return bool(re.match(pattern, script_code))


def get_log_file_path(script_code: str, script_name: str) -> Path:
    """
    Generate standardized log file path based on script identification.
    
    Args:
        script_code: Script identification code (e.g., "DWD10TAH3P")
        script_name: Script name for file naming
        
    Returns:
        Path object for the log file
    """
    # Create debug directory structure
    debug_dir = Path("data/dwd/0_debug")
    debug_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate log file name
    log_filename = f"{script_name}.debug.log"
    log_file_path = debug_dir / log_filename
    
    return log_file_path


def setup_logger(script_code: str, script_name: str, parent_log_file: Optional[Path] = None) -> logging.Logger:
    """
    Set up a logger for a specific script with automatic centralized logging detection.
    
    This function creates a logger that:
    1. Automatically detects if centralized logging is active
    2. Joins the centralized logging system seamlessly
    3. Uses script identification codes in all messages
    4. Ensures perfect chronological ordering
    5. Provides both console and file output
    6. Is thread-safe for concurrent access
    
    Args:
        script_code: Script identification code (e.g., "DWD10TAH3P")
        script_name: Script name for logger identification and file naming
        parent_log_file: Optional path to parent script's log file (auto-detected)
        
    Returns:
        Configured logger instance that automatically joins centralized logging
        
    Raises:
        ValueError: If script_code is invalid
    """
    global _master_logger, _centralized_log_file, _main_script_code
    
    # Validate script code
    if not validate_script_code(script_code):
        print(f"ERROR: Invalid script identification code: {script_code}")
        print(f"Valid codes: {list(VALID_SCRIPT_CODES.keys())}")
        raise ValueError(f"Invalid script identification code: {script_code}")
    
    # Thread-safe logger creation
    with _logger_lock:
        # Check if this is the main script that should create the master logger
        if script_code == "DWD10TAH3P":
            return _create_master_logger(script_code, script_name)
        
        # For all other scripts, check if centralized logging is available
        if _master_logger is not None:
            return _create_child_logger(script_code, script_name)
        
        # Fallback: create individual logger
        return _create_individual_logger(script_code, script_name)


def _create_master_logger(script_code: str, script_name: str) -> logging.Logger:
    """Create the master logger that all other components will use."""
    global _master_logger, _centralized_log_file, _main_script_code
    
    _main_script_code = script_code
    _centralized_log_file = get_log_file_path(script_code, script_name)
    
    # Create the master logger with a unique name
    import time
    unique_name = f"climastation_master_{int(time.time() * 1000000)}"
    _master_logger = logging.getLogger(unique_name)
    _master_logger.setLevel(logging.DEBUG)
    
    # Clear any existing handlers
    for handler in _master_logger.handlers[:]:
        _master_logger.removeHandler(handler)
        handler.close()
    
    # Create custom formatter that handles script codes
    class ScriptCodeFormatter(logging.Formatter):
        def formatTime(self, record, datefmt=None):
            import time
            ct = self.converter(record.created)
            s = time.strftime('%Y-%m-%d %H:%M:%S', ct)
            s += f',{int(record.msecs):03d}'
            return s
        
        def format(self, record):
            # Extract script code from the message or use default
            script_code = getattr(record, 'script_code', 'UNKNOWN')
            
            # Create the formatted message
            formatted_time = self.formatTime(record)
            return f'{formatted_time} — [{script_code}] — {record.levelname} {record.getMessage()}'
    
    formatter = ScriptCodeFormatter()
    
    # Set up console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    _master_logger.addHandler(console_handler)
    
    # Set up file handler with immediate flushing
    try:
        file_handler = logging.FileHandler(str(_centralized_log_file), mode='w', encoding='utf-8')
        file_handler.setFormatter(formatter)
        
        # Force immediate flushing
        original_emit = file_handler.emit
        def emit_with_flush(record):
            original_emit(record)
            file_handler.flush()
        file_handler.emit = emit_with_flush
        
        _master_logger.addHandler(file_handler)
        
    except Exception as e:
        print(f"ERROR: Failed to create log file {_centralized_log_file}: {e}")
    
    # Add script code filter to master logger
    def add_script_code_filter(record):
        if not hasattr(record, 'script_code'):
            record.script_code = script_code
        return True
    
    _master_logger.addFilter(add_script_code_filter)
    
    # Log initialization
    _master_logger.debug(f"Master logger initialized with centralized logging: {_centralized_log_file}")
    
    return _master_logger


def _create_child_logger(script_code: str, script_name: str) -> logging.Logger:
    """Create a child logger that uses the master logger."""
    global _master_logger
    
    if _master_logger is None:
        raise RuntimeError("Master logger not available for child logger creation")
    
    # Create a new logger instance that shares handlers with master logger
    import time
    unique_name = f"climastation_child_{script_code}_{int(time.time() * 1000000)}"
    child_logger = logging.getLogger(unique_name)
    child_logger.setLevel(logging.DEBUG)
    
    # Clear any existing handlers
    for handler in child_logger.handlers[:]:
        child_logger.removeHandler(handler)
        handler.close()
    
    # Share all handlers from master logger
    for handler in _master_logger.handlers:
        child_logger.addHandler(handler)
    
    # Add script code filter to this child logger
    def add_script_code_filter(record):
        record.script_code = script_code
        return True
    
    child_logger.addFilter(add_script_code_filter)
    
    # Log initialization
    child_logger.debug(f"Child logger '{script_name}' joined centralized logging")
    
    return child_logger


def _create_individual_logger(script_code: str, script_name: str) -> logging.Logger:
    """Create an individual logger when centralized logging is not available."""
    log_file_path = get_log_file_path(script_code, script_name)
    
    # Create individual logger
    import time
    unique_name = f"climastation_{script_code}_{script_name}_{int(time.time() * 1000000)}"
    logger = logging.getLogger(unique_name)
    logger.setLevel(logging.DEBUG)
    
    # Clear existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()
    
    # Create formatter
    class MillisecondFormatter(logging.Formatter):
        def formatTime(self, record, datefmt=None):
            import time
            ct = self.converter(record.created)
            s = time.strftime('%Y-%m-%d %H:%M:%S', ct)
            s += f',{int(record.msecs):03d}'
            return s

    formatter = MillisecondFormatter(f'%(asctime)s — [{script_code}] — %(levelname)s %(message)s')
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler
    try:
        file_handler = logging.FileHandler(str(log_file_path), mode='w', encoding='utf-8')
        file_handler.setFormatter(formatter)
        
        # Force immediate flushing
        original_emit = file_handler.emit
        def emit_with_flush(record):
            original_emit(record)
            file_handler.flush()
        file_handler.emit = emit_with_flush
        
        logger.addHandler(file_handler)
        
    except Exception as e:
        print(f"ERROR: Failed to create log file {log_file_path}: {e}")
    
    logger.debug(f"Individual logger '{script_name}' initialized: {log_file_path}")
    
    return logger


def get_main_log_file_path() -> Optional[Path]:
    """Get the path to the main script's log file if it exists."""
    return _centralized_log_file


def is_centralized_logging_active() -> bool:
    """Check if centralized logging is currently active."""
    return _master_logger is not None


def list_active_loggers() -> Dict[str, str]:
    """List information about the centralized logging system."""
    result = {}
    if _master_logger is not None:
        result["master_logger"] = _main_script_code or "unknown"
        result["centralized_log_file"] = str(_centralized_log_file) if _centralized_log_file else "none"
        result["status"] = "active"
    else:
        result["status"] = "inactive"
    
    return result


def cleanup_all_loggers() -> int:
    """Clean up the centralized logging system."""
    global _master_logger, _centralized_log_file, _main_script_code
    
    cleanup_count = 0
    
    if _master_logger is not None:
        # Close all handlers
        for handler in _master_logger.handlers[:]:
            handler.close()
            _master_logger.removeHandler(handler)
        
        cleanup_count = 1
    
    # Reset global state
    _master_logger = None
    _centralized_log_file = None
    _main_script_code = None
    
    return cleanup_count


def get_script_code_info() -> Dict[str, str]:
    """Get information about all valid script identification codes."""
    return VALID_SCRIPT_CODES.copy()


if __name__ == "__main__":
    """Test the logging utility with focus on chronological ordering and component detection."""
    print("Testing ClimaStation Logging Utility [DWD10TAH3L]...")
    print("=" * 60)
    
    # Test chronological logging with multiple components
    print("\n🔧 Testing automatic centralized logging detection:")
    try:
        # Create main logger (this creates the master logger)
        print("   Creating main logger...")
        main_logger = setup_logger("DWD10TAH3P", "test_main_script")
        main_logger.info("Message 1: Main script started")
        
        # Create child loggers (these should automatically join centralized logging)
        print("   Creating child loggers...")
        raw_parser_logger = setup_logger("DWD10TAH3T", "test_raw_parser")
        station_logger = setup_logger("DWD10TAH3I", "test_station_parser")
        sensor_logger = setup_logger("DWD10TAH3S", "test_sensor_metadata")
        zip_logger = setup_logger("DWD10TAH3Z", "test_zip_handler")
        
        # Test chronological ordering with interleaved messages
        main_logger.info("Message 2: About to process ZIP file")
        zip_logger.info("Message 3: Extracting ZIP file contents")
        raw_parser_logger.info("Message 4: Loading raw measurement data")
        station_logger.info("Message 5: Parsing station information")
        sensor_logger.info("Message 6: Loading sensor metadata")
        main_logger.info("Message 7: Processing measurements")
        raw_parser_logger.info("Message 8: Writing output file")
        main_logger.info("Message 9: Processing complete")
        
        print("   ✅ Multi-component chronological logging test completed")
        print(f"   📄 Check log file: {get_main_log_file_path()}")
        print(f"   🔄 Centralized logging active: {is_centralized_logging_active()}")
        print("   📋 Messages should appear in perfect chronological order (1-9)")
        
    except Exception as e:
        print(f"   ❌ Multi-component logging test failed: {e}")
        import traceback
        traceback.print_exc()
    
    # Test logger registry
    print("\n📋 Testing centralized logging status:")
    status = list_active_loggers()
    for key, value in status.items():
        print(f"   {key}: {value}")
    
    # Test cleanup
    print("\n🧹 Testing cleanup:")
    cleanup_count = cleanup_all_loggers()
    print(f"   ✅ Cleaned up centralized logging system")
    print(f"   🔄 Centralized logging active after cleanup: {is_centralized_logging_active()}")
    
    print("\n✅ All tests completed!")
    print("🔍 Check the log file to verify:")
    print("   - All components automatically joined centralized logging")
    print("   - Messages appear in perfect chronological order")
    print("   - Each message shows its script identification code")
    print("   - No missing messages from any component")
