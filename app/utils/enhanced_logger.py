"""
ClimaStation Enhanced Logger

SCRIPT IDENTIFICATION: DWDLOG1T (Enhanced Logger)

PURPOSE:
Centralized logging system for the ClimaStation platform with component-based
identification, structured output, and performance monitoring capabilities.
Provides consistent logging across all processors, orchestrators, and utilities.

RESPONSIBILITIES:
- Create component-specific loggers with unique identification codes
- Provide structured logging with consistent formatting
- Support multiple log levels and output destinations
- Enable performance monitoring and timing
- Handle log rotation and file management
- Support both console and file output

COMPONENT CODES:
- DWD10TAH1T: Main orchestrator
- DWD10TAH2T: Configuration manager
- DWD10TAH3T: Raw parser
- DWD10TAH4T: Base processor
- DWD10TAH5T: Dataset processors
- DWDLOG1T: Enhanced logger
- DWDTRANS1T: Translation manager

USAGE:
    from utils.enhanced_logger import setup_logger, get_logger
    
    # Create component logger
    logger = setup_logger("DWD10TAH5T", "air_temperature_processor")
    
    # Use structured logging
    logger.info("Processing started", extra={"file_count": 100, "dataset": "air_temp"})
    
    # Performance timing
    with logger.timer("file_processing"):
        process_file(file_path)

FEATURES:
- Component-based identification for traceability
- Structured logging with extra context
- Performance timing utilities
- Automatic log rotation
- Console and file output
- Debug mode support
- Memory usage tracking

ERROR HANDLING:
- Graceful fallback if log directory creation fails
- Safe handling of log rotation errors
- Proper cleanup of resources
"""

import logging
import logging.handlers
import sys
from pathlib import Path
from typing import Optional, Dict, Any, Union
from datetime import datetime
from contextlib import contextmanager
import time
import traceback
import threading
from dataclasses import dataclass


@dataclass
class LogConfig:
    """Configuration for logger setup"""
    level: str = "INFO"
    console_output: bool = True
    file_output: bool = True
    log_dir: Path = Path("logs")
    max_file_size: int = 10 * 1024 * 1024  # 10MB
    backup_count: int = 5
    format_string: str = "%(asctime)s - [%(component_id)s] - %(name)s - %(levelname)s - %(message)s"


class ComponentLogger(logging.Logger):
    """
    Enhanced logger with component identification and performance monitoring.
    
    Extends the standard Python logger with ClimaStation-specific features
    like component codes, structured logging, and timing utilities.
    """
    
    def __init__(self, name: str, component_id: str, level: int = logging.NOTSET):
        """
        Initialize component logger.
        
        Args:
            name: Logger name
            component_id: Component identification code (e.g., "DWD10TAH5T")
            level: Logging level
        """
        super().__init__(name, level)
        self.component_id = component_id
        self._timers: Dict[str, float] = {}
        self._lock = threading.Lock()
    
    def _log_with_component(self, level: int, msg: Any, args, exc_info=None, extra=None, stack_info=False, **kwargs):
        """Override to add component ID to all log records"""
        if extra is None:
            extra = {}
        extra['component_id'] = self.component_id
        
        # Add structured data if provided
        if kwargs:
            extra.update(kwargs)
        
        super()._log(level, msg, args, exc_info=exc_info, extra=extra, stack_info=stack_info)
    
    def debug(self, msg, *args, **kwargs):
        """Log debug message with component ID"""
        if self.isEnabledFor(logging.DEBUG):
            self._log_with_component(logging.DEBUG, msg, args, **kwargs)
    
    def info(self, msg, *args, **kwargs):
        """Log info message with component ID"""
        if self.isEnabledFor(logging.INFO):
            self._log_with_component(logging.INFO, msg, args, **kwargs)
    
    def warning(self, msg, *args, **kwargs):
        """Log warning message with component ID"""
        if self.isEnabledFor(logging.WARNING):
            self._log_with_component(logging.WARNING, msg, args, **kwargs)
    
    def error(self, msg, *args, **kwargs):
        """Log error message with component ID"""
        if self.isEnabledFor(logging.ERROR):
            self._log_with_component(logging.ERROR, msg, args, **kwargs)
    
    def critical(self, msg, *args, **kwargs):
        """Log critical message with component ID"""
        if self.isEnabledFor(logging.CRITICAL):
            self._log_with_component(logging.CRITICAL, msg, args, **kwargs)
    
    def start_timer(self, timer_name: str):
        """Start a performance timer"""
        with self._lock:
            self._timers[timer_name] = time.time()
            self.debug(f"Timer started: {timer_name}")
    
    def stop_timer(self, timer_name: str) -> Optional[float]:
        """Stop a performance timer and return elapsed time"""
        with self._lock:
            if timer_name not in self._timers:
                self.warning(f"Timer '{timer_name}' was not started")
                return None
            
            elapsed = time.time() - self._timers[timer_name]
            del self._timers[timer_name]
            
            self.info(f"Timer completed: {timer_name} ({elapsed:.3f}s)")
            return elapsed
    
    @contextmanager
    def timer(self, timer_name: str):
        """Context manager for timing operations"""
        self.start_timer(timer_name)
        try:
            yield
        finally:
            self.stop_timer(timer_name)
    
    def log_exception(self, msg: str = "Exception occurred"):
        """Log current exception with full traceback"""
        self.error(f"{msg}: {traceback.format_exc()}")
    
    def log_performance(self, operation: str, duration: float, **metrics):
        """Log performance metrics"""
        extra_data = {"operation": operation, "duration_seconds": duration}
        extra_data.update(metrics)
        
        self.info(f"Performance: {operation} completed in {duration:.3f}s", **extra_data)


class ComponentLoggerAdapter(logging.LoggerAdapter):
    """Adapter to add component ID to existing loggers"""
    
    def __init__(self, logger: logging.Logger, component_id: str):
        super().__init__(logger, {"component_id": component_id})
        self.component_id = component_id
    
    def process(self, msg, kwargs):
        """Add component ID to log record"""
        if 'extra' not in kwargs:
            kwargs['extra'] = {}
        kwargs['extra']['component_id'] = self.component_id
        return msg, kwargs


# Global logger registry
_logger_registry: Dict[str, ComponentLogger] = {}
_registry_lock = threading.Lock()


def setup_logger(
    component_id: str,
    name: Optional[str] = None,
    config: Optional[LogConfig] = None
) -> ComponentLogger:
    """
    Set up a component logger with standardized configuration.
    
    Args:
        component_id: Component identification code (e.g., "DWD10TAH5T")
        name: Logger name (defaults to component_id)
        config: Logger configuration (uses defaults if not provided)
        
    Returns:
        Configured ComponentLogger instance
    """
    if config is None:
        config = LogConfig()
    
    if name is None:
        name = component_id.lower()
    
    # Check if logger already exists
    registry_key = f"{component_id}_{name}"
    with _registry_lock:
        if registry_key in _logger_registry:
            return _logger_registry[registry_key]
    
    # Create new logger
    logger = ComponentLogger(name, component_id)
    logger.setLevel(getattr(logging, config.level.upper()))
    
    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter(config.format_string)
    
    # Console handler
    if config.console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(getattr(logging, config.level.upper()))
        logger.addHandler(console_handler)
    
    # File handler with rotation
    if config.file_output:
        try:
            # Ensure log directory exists
            config.log_dir.mkdir(parents=True, exist_ok=True)
            
            # Create rotating file handler
            log_file = config.log_dir / f"{component_id.lower()}_{name}.log"
            file_handler = logging.handlers.RotatingFileHandler(
                log_file,
                maxBytes=config.max_file_size,
                backupCount=config.backup_count,
                encoding='utf-8'
            )
            file_handler.setFormatter(formatter)
            file_handler.setLevel(getattr(logging, config.level.upper()))
            logger.addHandler(file_handler)
            
        except Exception as e:
            # Fallback: log to console only
            logger.warning(f"Failed to create file handler: {e}")
    
    # Register logger
    with _registry_lock:
        _logger_registry[registry_key] = logger
    
    logger.info(f"Logger initialized: {component_id} ({name})")
    return logger


def get_logger(component_id: str, name: Optional[str] = None) -> Optional[ComponentLogger]:
    """
    Get existing logger from registry.
    
    Args:
        component_id: Component identification code
        name: Logger name (defaults to component_id)
        
    Returns:
        Existing ComponentLogger or None if not found
    """
    if name is None:
        name = component_id.lower()
    
    registry_key = f"{component_id}_{name}"
    with _registry_lock:
        return _logger_registry.get(registry_key)


def configure_root_logger(config: Optional[LogConfig] = None):
    """
    Configure the root logger for the entire application.
    
    Args:
        config: Logger configuration
    """
    if config is None:
        config = LogConfig()
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, config.level.upper()))
    
    # Remove existing handlers
    root_logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Console handler
    if config.console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
    
    # File handler
    if config.file_output:
        try:
            config.log_dir.mkdir(parents=True, exist_ok=True)
            log_file = config.log_dir / "climastation.log"
            
            file_handler = logging.handlers.RotatingFileHandler(
                log_file,
                maxBytes=config.max_file_size,
                backupCount=config.backup_count,
                encoding='utf-8'
            )
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)
            
        except Exception as e:
            print(f"Warning: Failed to create root file handler: {e}")


def shutdown_logging():
    """Shutdown all loggers and clean up resources"""
    with _registry_lock:
        for logger in _logger_registry.values():
            for handler in logger.handlers:
                handler.close()
        _logger_registry.clear()
    
    logging.shutdown()


# Convenience functions for common logging patterns
def log_function_entry(logger: ComponentLogger, func_name: str, **kwargs):
    """Log function entry with parameters"""
    params = ", ".join(f"{k}={v}" for k, v in kwargs.items())
    logger.debug(f"Entering {func_name}({params})")


def log_function_exit(logger: ComponentLogger, func_name: str, result=None, duration: Optional[float] = None):
    """Log function exit with result and timing"""
    msg = f"Exiting {func_name}"
    if duration is not None:
        msg += f" (took {duration:.3f}s)"
    if result is not None:
        msg += f" -> {result}"
    logger.debug(msg)


# Example usage and testing
if __name__ == "__main__":
    print("Testing ClimaStation Enhanced Logger...")
    
    # Test basic logger setup
    logger = setup_logger("DWDLOG1T", "test_logger")
    
    # Test different log levels
    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    
    # Test structured logging
    logger.info("Processing file", file_name="test.zip", file_size=1024)
    
    # Test timing
    with logger.timer("test_operation"):
        time.sleep(0.1)  # Simulate work
    
    # Test performance logging
    logger.log_performance("file_processing", 0.5, files_processed=10, records=1000)
    
    # Test exception logging
    try:
        raise ValueError("Test exception")
    except ValueError:
        logger.log_exception("Test exception handling")
    
    print("✅ Enhanced logger test completed")
    
    # Clean up
    shutdown_logging()
