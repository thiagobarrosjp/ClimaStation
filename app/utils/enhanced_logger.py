"""
ClimaStation Enhanced Logger — single-session file logging

Goal (per task):
- One log file for the entire run: data/dwd/0_debug/pipeline.log
- Overwrite the file on every run (mode='w'), UTF-8 encoding
- Millisecond timestamps with format: "YYYY-MM-DD HH:MM:SS,mmm LEVEL logger.name: message"
- Centralized, idempotent setup via `configure_session_file_logging(...)`
- Keep console logging behavior unchanged (do not add duplicates)
- No rotation, no per-component file handlers
- Components should use named loggers (e.g., logging.getLogger("pipeline.downloader"))

Backwards compatibility:
- Provide `ComponentLogger`, `ComponentLoggerAdapter`, `get_logger`, `setup_logger`,
  `configure_root_logger`, and `shutdown_logging` so existing imports continue working.
  These defer to stdlib logging and the single file handler configured on the root.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Optional
import threading

# ------------------------------
# Public types (back-compat)
# ------------------------------

class ComponentLogger(logging.Logger):
    """Thin subclass for typing/compat only."""
    pass


class ComponentLoggerAdapter(logging.LoggerAdapter):
    """Compatibility adapter that preserves extra fields if callers use it."""
    def process(self, msg, kwargs):  # type: ignore[override]
        kwargs.setdefault("extra", {})
        return msg, kwargs

# Alias kept for prior code that referenced this name
StructuredLoggerAdapter = ComponentLoggerAdapter


# ------------------------------
# Module-level state for idempotent setup
# ------------------------------

__lock = threading.Lock()
__configured: bool = False
__file_handler: Optional[logging.Handler] = None
__file_path: Optional[Path] = None

# Millisecond formatter (required)
_MS_FORMAT = "%(asctime)s,%(msecs)03d %(levelname)s %(name)s: %(message)s"
_MS_DATEFMT = "%Y-%m-%d %H:%M:%S"


# ------------------------------
# Helper: coerce log level
# ------------------------------

def _coerce_level(level: int | str | None) -> int:
    if isinstance(level, int):
        return level
    if isinstance(level, str):
        m = level.strip().upper()
        return {
            "CRITICAL": logging.CRITICAL,
            "ERROR": logging.ERROR,
            "WARNING": logging.WARNING,
            "INFO": logging.INFO,
            "DEBUG": logging.DEBUG,
            "NOTSET": logging.NOTSET,
        }.get(m, logging.INFO)
    return logging.INFO


# ------------------------------
# Public: configure single session file logging
# ------------------------------

def configure_session_file_logging(
    log_path: str = "data/dwd/0_debug/pipeline.log",
    level: int | str = "INFO",
) -> logging.Logger:
    """
    Configure the *root* logger once per process to write all logs into a single file.

    Behavior:
    - Creates the directory for `log_path` (Windows-safe) if missing.
    - Attaches exactly one FileHandler(log_path, mode='w', encoding='utf-8') to the root.
    - Uses millisecond timestamp format: "YYYY-MM-DD HH:MM:SS,mmm LEVEL logger.name: message".
    - Leaves existing console handlers in place (no duplication). If none exist on the root,
      one StreamHandler is added to preserve console output.
    - Subsequent calls are idempotent (no duplicate handlers, does not reopen the file).

    Returns:
        The root logger instance.
    """
    global __configured, __file_handler, __file_path

    with __lock:
        root = logging.getLogger()
        root.setLevel(_coerce_level(level))

        # Ensure a console handler exists *once*, but don't add duplicates or change formatting.
        if not any(isinstance(h, logging.StreamHandler) for h in root.handlers):
            ch = logging.StreamHandler()
            # Do not force a specific console formatter to keep behavior "unchanged".
            # If there isn't one yet, use the same ms format for consistency.
            ch.setFormatter(logging.Formatter(_MS_FORMAT, datefmt=_MS_DATEFMT))
            root.addHandler(ch)

        # If already configured for this session, return early
        if __configured and __file_handler is not None and __file_path == Path(log_path):
            return root

        # Remove our previously attached file handler if path changed (rare)
        if __configured and __file_handler is not None and __file_path is not None:
            try:
                root.removeHandler(__file_handler)
                __file_handler.close()
            except Exception:
                pass
            finally:
                __file_handler = None
                __file_path = None

        # Prepare directory and handler
        log_file = Path(log_path)
        try:
            os.makedirs(str(log_file.parent), exist_ok=True)
        except Exception:
            # If directory creation fails, we leave only console logging enabled.
            return root

        fh = logging.FileHandler(filename=str(log_file), mode='w', encoding='utf-8', delay=False)
        fh.setLevel(_coerce_level(level))
        fh.setFormatter(logging.Formatter(_MS_FORMAT, datefmt=_MS_DATEFMT))
        root.addHandler(fh)

        __file_handler = fh
        __file_path = log_file
        __configured = True
        return root


# ------------------------------
# Backwards-compatible helpers (no rotation, no per-component files)
# ------------------------------

def setup_logger(component_id: str, name: Optional[str] = None, config: Optional[Any] = None) -> ComponentLogger:
    """Return a named logger; formatting/handlers are controlled by the root.

    This function exists for compatibility with earlier code that called `setup_logger`.
    It does *not* attach any handlers; it simply returns `logging.getLogger(<component_id>)`.
    """
    _ = (name, config)  # intentionally unused
    return logging.getLogger(str(component_id))  # type: ignore[return-value]


def get_logger(component_id: str, name: Optional[str] = None) -> Optional[ComponentLogger]:
    """Compatibility accessor; always returns the named logger (never None)."""
    _ = name
    return logging.getLogger(str(component_id))  # type: ignore[return-value]


def configure_root_logger(config: Optional[Any] = None) -> None:
    """Deprecated shim. Prefer `configure_session_file_logging(...)`."""
    _ = config
    # No-op: root configuration is handled by `configure_session_file_logging`.
    return None


def shutdown_logging() -> None:
    """Flush and close the single session file handler without disturbing console handlers."""
    global __file_handler, __file_path, __configured
    with __lock:
        if __file_handler is not None:
            try:
                __file_handler.flush()
                __file_handler.close()
            except Exception:
                pass
            root = logging.getLogger()
            try:
                root.removeHandler(__file_handler)
            except Exception:
                pass
        __file_handler = None
        __file_path = None
        __configured = False
    # Allow the logging module to perform standard shutdown procedures
    logging.shutdown()


# ------------------------------
# Convenience utilities (kept from prior version)
# ------------------------------

def log_function_entry(logger: logging.Logger, func_name: str, **kwargs) -> None:
    params = ", ".join(f"{k}={v}" for k, v in kwargs.items())
    logger.debug(f"Entering {func_name}({params})")


def log_function_exit(
    logger: logging.Logger,
    func_name: str,
    result: Any | None = None,
    duration: float | None = None,
) -> None:
    msg = f"Exiting {func_name}"
    if duration is not None:
        msg += f" (took {duration:.3f}s)"
    if result is not None:
        msg += f" | result={result}"
    logger.debug(msg)
