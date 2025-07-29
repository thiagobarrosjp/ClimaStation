"""
ClimaStation Enhanced Logging System

SCRIPT IDENTIFICATION: DWD10TAH3L (Enhanced Logger)

PURPOSE:
Centralized logging system for the ClimaStation platform with component-based
identification codes, structured formatting, and high-volume processing support.
Provides consistent logging across all pipeline components with proper integration
to the configuration system.

RESPONSIBILITIES:
- Create component-specific loggers with standardized formatting
- Support multiple output destinations (file, console) with different levels
- Provide structured logging for processing statistics and metadata
- Handle high-volume logging for bulk file processing (500k+ files)
- Integrate with configuration system for dynamic log settings
- Support component-based traceability across the entire pipeline

USAGE:
    from app.utils.enhanced_logger import get_logger
    logger = get_logger("PROCESSOR")
    logger.info("Processing started", extra={"component": "PROCESSOR", "dataset": "air_temp"})

COMPONENT CODES:
- CONFIG: Configuration loading and validation
- PROCESSOR: Dataset processing operations  
- ORCHESTRATOR: Pipeline orchestration and coordination
- WORKER: Parallel file processing workers
- DOWNLOAD: File download operations
- EXTRACT: ZIP extraction operations
- VALIDATE: Data validation operations

LOG FORMAT:
[TIMESTAMP] [COMPONENT] [LEVEL] MESSAGE {structured_data}

PERFORMANCE:
- Optimized for high-volume logging scenarios
- Efficient file rotation and buffering
- Minimal performance impact on processing pipeline
"""

import logging
import logging.handlers
from pathlib import Path
from typing import Dict, Any, Optional, Union
import json
import sys
import threading

# Global logger registry to avoid duplicate logger creation
_logger_registry: Dict[str, "StructuredLoggerAdapter"] = {}
_logging_configured = False
_config_lock = threading.Lock()

# Centralized log directory path (change here if needed)
DEFAULT_LOG_DIR = Path("data/dwd/0_debug")
DEFAULT_LOG_FILE = DEFAULT_LOG_DIR / "climastation.log"


class ComponentFormatter(logging.Formatter):
    """
    Custom formatter for component-based logging with structured data support.
    
    Provides consistent formatting across all ClimaStation components with
    support for structured data and component identification.
    """
    
    def __init__(self, include_structured_data: bool = True):
        """
        Initialize the component formatter.
        
        Args:
            include_structured_data: Whether to include structured data in output
        """
        self.include_structured_data = include_structured_data
        # Base format: [TIMESTAMP] [COMPONENT] [LEVEL] MESSAGE
        super().__init__(
            fmt='%(asctime)s [%(component)s] [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record with component information and structured data.
        
        Args:
            record: Log record to format
            
        Returns:
            Formatted log message string
        """
        # Ensure component is always present
        if not hasattr(record, 'component'):
            setattr(record, 'component', 'UNKNOWN')
        
        # Format the base message
        formatted_message = super().format(record)
        
        # Add structured data if present and enabled
        if self.include_structured_data:
            structured_data = getattr(record, 'structured_data', None)
            if structured_data is not None:
                try:
                    structured_str = json.dumps(structured_data, separators=(',', ':'))
                    formatted_message += f" {structured_str}"
                except (TypeError, ValueError):
                    # Fallback if structured data can't be serialized
                    formatted_message += f" {structured_data}"
        
        return formatted_message

class StructuredLoggerAdapter(logging.LoggerAdapter):
    """
    Logger adapter that automatically adds component information and supports structured data.
    
    Enhances standard logging with component identification and structured data support
    for better traceability and analysis.
    """
    
    def __init__(self, logger: logging.Logger, component_code: str):
        """
        Initialize the structured logger adapter.
        
        Args:
            logger: Base logger instance
            component_code: Component identification code (e.g., 'PROCESSOR', 'CONFIG')
        """
        super().__init__(logger, {'component': component_code})
        self.component_code = component_code
    
    def process(self, msg: str, kwargs: Dict[str, Any]) -> tuple:
        """
        Process log message and add component information.
        
        Args:
            msg: Log message
            kwargs: Keyword arguments including 'extra'
            
        Returns:
            Tuple of (message, kwargs) with component information added
        """
        # Ensure extra dict exists
        if 'extra' not in kwargs:
            kwargs['extra'] = {}
        
        # Add component information
        kwargs['extra']['component'] = self.component_code
        
        # Handle structured data
        if 'structured_data' in kwargs:
            kwargs['extra']['structured_data'] = kwargs.pop('structured_data')
        
        return msg, kwargs
    
    def log_processing_stats(self, level: int, dataset: str, files_processed: int, 
                           files_failed: int, duration_seconds: float, **kwargs):
        """
        Log processing statistics in a structured format.
        
        Args:
            level: Log level (logging.INFO, logging.ERROR, etc.)
            dataset: Dataset name being processed
            files_processed: Number of files successfully processed
            files_failed: Number of files that failed processing
            duration_seconds: Processing duration in seconds
            **kwargs: Additional structured data
        """
        structured_data = {
            'dataset': dataset,
            'files_processed': files_processed,
            'files_failed': files_failed,
            'duration_seconds': round(duration_seconds, 2),
            'success_rate': round((files_processed / (files_processed + files_failed)) * 100, 1) if (files_processed + files_failed) > 0 else 0,
            **kwargs
        }
        
        message = f"Processing completed for {dataset}: {files_processed} success, {files_failed} failed"
        self.log(level, message, structured_data=structured_data)
    
    def log_file_operation(self, level: int, operation: str, file_path: Union[str, Path], 
                          success: bool, error_msg: Optional[str] = None, **kwargs):
        """
        Log file operation with structured data.
        
        Args:
            level: Log level
            operation: Operation type (download, extract, process, validate)
            file_path: Path to file being operated on
            success: Whether operation succeeded
            error_msg: Error message if operation failed
            **kwargs: Additional structured data
        """
        structured_data = {
            'operation': operation,
            'file_path': str(file_path),
            'success': success,
            **kwargs
        }
        
        if error_msg:
            structured_data['error'] = error_msg
        
        status = "completed" if success else "failed"
        message = f"File {operation} {status}: {Path(file_path).name}"
        
        self.log(level, message, structured_data=structured_data)

def _setup_logging_configuration(config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Set up logging configuration.
    """
    logging_config = {
        'level': 'INFO',
        'log_to_file': True,
        'max_file_size_mb': 100,
        'backup_count': 5,
        'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    }
    # Use fixed debug folder
    log_dir = DEFAULT_LOG_DIR
    log_file = DEFAULT_LOG_FILE

    log_dir.mkdir(parents=True, exist_ok=True)
    logging_config['log_file'] = log_file

    return logging_config



def _create_file_handler(log_file_path: Path, logging_config: Dict[str, Any]) -> logging.Handler:
    """
    Create rotating file handler for centralized logging.
    
    Args:
        log_file_path: Path to log file
        logging_config: Logging configuration dictionary
        
    Returns:
        Configured file handler
    """
    # Use rotating file handler for high-volume logging
    max_bytes = logging_config.get('max_file_size_mb', 100) * 1024 * 1024
    backup_count = logging_config.get('backup_count', 5)
    
    file_handler = logging.handlers.RotatingFileHandler(
        log_file_path,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding='utf-8'
    )
    
    log_level = logging_config.get('level', 'INFO')
    file_handler.setLevel(getattr(logging, log_level.upper()))
    file_handler.setFormatter(ComponentFormatter(include_structured_data=True))
    
    return file_handler

def _create_console_handler(logging_config: Dict[str, Any]) -> logging.Handler:
    """
    Create console handler for development and debugging.
    
    Args:
        logging_config: Logging configuration dictionary
        
    Returns:
        Configured console handler
    """
    console_handler = logging.StreamHandler(sys.stdout)
    
    # Console typically shows INFO and above to reduce noise
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(ComponentFormatter(include_structured_data=False))
    
    return console_handler

def configure_logging(config: Optional[Dict[str, Any]] = None):
    """
    Configure the global logging system.
    
    This function should be called once at application startup to set up
    the centralized logging configuration.
    
    Args:
        config: Configuration dictionary from config manager
    """
    global _logging_configured
    
    with _config_lock:
        if _logging_configured:
            return
        
        logging_config = _setup_logging_configuration(config)
        
        # Configure root logger
        root_logger = logging.getLogger()
        log_level = logging_config.get('level', 'INFO')
        root_logger.setLevel(getattr(logging, log_level.upper()))
        
        # Clear any existing handlers
        root_logger.handlers.clear()
        
        # Add file handler if enabled
        if logging_config.get('log_to_file', True):
            log_file_path = logging_config['log_file']
            file_handler = _create_file_handler(log_file_path, logging_config)
            root_logger.addHandler(file_handler)
        
        # Add console handler
        console_handler = _create_console_handler(logging_config)
        root_logger.addHandler(console_handler)
        
        _logging_configured = True

def get_logger(component_name: str, config: Optional[Dict[str, Any]] = None) -> StructuredLoggerAdapter:
    """
    Get a component-specific logger with structured logging support.
    
    This is the main function from available_functions.py that provides
    standardized logging across all ClimaStation components.
    
    Args:
        component_name: Component code (CONFIG, PROCESSOR, ORCHESTRATOR, etc.)
        config: Optional configuration dictionary (will load from config manager if not provided)
        
    Returns:
        Structured logger adapter configured for the component
        
    Raises:
        ValueError: If component_name is invalid
    """
    # Validate component name
    valid_components = {
        'CONFIG', 'PROCESSOR', 'PIPELINE', 'ORCHESTRATOR', 'WORKER', 
        'DOWNLOAD', 'EXTRACT', 'VALIDATE', 'CRAWLER',  'UNKNOWN'
    }
    
    if component_name not in valid_components:
        raise ValueError(f"Invalid component name: {component_name}. Must be one of: {valid_components}")
    
    # Configure logging if not already done
    if not _logging_configured:
        configure_logging(config)
    
    # Check if logger already exists in our manual cache
    logger_key = f"climastation.{component_name.lower()}"
    
    if logger_key in _logger_registry:
        return _logger_registry[logger_key]
    
    # Create new logger
    base_logger = logging.getLogger(logger_key)
    structured_logger = StructuredLoggerAdapter(base_logger, component_name)
    
    # Cache the logger manually
    _logger_registry[logger_key] = structured_logger
    
    return structured_logger

def get_logger_with_config_manager(component_name: str, config_manager=None) -> StructuredLoggerAdapter:
    """
    Get logger with automatic config manager integration.
    
    Convenience function that automatically loads configuration from
    the config manager if available.
    
    Args:
        component_name: Component code
        config_manager: ConfigManager instance (optional)
        
    Returns:
        Structured logger adapter
    """
    config = None
    if config_manager:
        try:
            # Try to get logging config from config manager
            base_config = config_manager._load_base_config(logging.getLogger("temp"))
            config = base_config  # Use the full config now
        except Exception:
            # Fall back to default config if config manager fails
            pass
    
    return get_logger(component_name, config)

def clear_logger_cache():
    """
    Clear the logger cache.
    
    Useful for testing and development when logger configuration changes.
    """
    global _logger_registry, _logging_configured
    _logger_registry.clear()
    _logging_configured = False

