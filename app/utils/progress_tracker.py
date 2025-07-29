"""
ClimaStation Progress Tracking System

SCRIPT IDENTIFICATION: DWD10TAH3P (Progress Tracker)

PURPOSE:
SQLite-based progress tracking system for large-scale DWD file processing.
Provides thread-safe coordination for parallel workers with resume capability
after crashes, interruptions, or manual stops. Optimized for 500K+ files
across multiple datasets with atomic operations and performance optimization.

RESPONSIBILITIES:
- Initialize and manage file processing queues for datasets
- Coordinate work distribution among parallel workers (up to 4)
- Track file processing status: pending → processing → completed/failed
- Provide atomic claim/release operations to prevent race conditions
- Support resume capability after system interruptions
- Generate processing statistics and progress reports
- Integrate with configuration system and enhanced logging

USAGE:
    from app.utils.progress_tracker import ProgressTracker, initialize_progress_tracking
    from app.utils.enhanced_logger import get_logger
    from app.utils.config_manager import load_config
    
    logger = get_logger("WORKER")
    config = load_config("10_minutes_air_temperature", logger)
    
    # Initialize tracking for a dataset
    files = ["file1.zip", "file2.zip", "file3.zip"]
    initialize_progress_tracking("air_temp_10min", files, config, logger)
    
    # Worker coordination
    tracker = ProgressTracker(config, logger)
    file_path = tracker.claim_next_file("air_temp_10min", "worker_001")
    if file_path:
        # Process file...
        tracker.mark_file_completed(file_path, records_processed=1500)

DATABASE SCHEMA:
- file_processing_log: Main tracking table with status, timing, worker info
- dataset_metadata: Dataset-level information and statistics
- worker_sessions: Active worker session tracking

PERFORMANCE:
- WAL mode for concurrent access without blocking
- Optimized indexes for fast queries on 500K+ records
- Connection pooling and prepared statements
- Memory usage <50MB for tracking operations
- Query operations <1 second response time

THREAD SAFETY:
- Atomic claim operations using SELECT FOR UPDATE equivalent
- Proper transaction isolation and rollback handling
- Connection-per-thread pattern for SQLite safety
- Comprehensive error recovery and cleanup
"""

import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from contextlib import contextmanager
import json

# Import ClimaStation utilities
from .enhanced_logger import StructuredLoggerAdapter
from .config_manager import ConfigurationError

@dataclass
class ProcessingStats:
    """Statistics for dataset processing progress."""
    dataset: str
    total_files: int
    pending_files: int
    processing_files: int
    completed_files: int
    failed_files: int
    success_rate: float
    avg_processing_time: float
    estimated_completion: Optional[str]
    active_workers: int

@dataclass
class FileStatus:
    """Status information for a single file."""
    file_path: str
    status: str  # pending, processing, completed, failed
    worker_id: Optional[str]
    start_time: Optional[datetime]
    end_time: Optional[datetime]
    processing_duration: Optional[float]
    records_processed: Optional[int]
    error_message: Optional[str]
    retry_count: int

class ProgressTrackingError(Exception):
    """Raised when progress tracking operations fail."""
    pass

class ProgressTracker:
    """
    Thread-safe progress tracking system for DWD file processing.
    
    Manages SQLite database operations for tracking file processing status
    across multiple datasets and parallel workers. Provides atomic operations
    for work coordination and comprehensive progress reporting.
    """
    
    # Database schema version for migrations
    SCHEMA_VERSION = 1
    
    # File status constants
    STATUS_PENDING = "pending"
    STATUS_PROCESSING = "processing"
    STATUS_COMPLETED = "completed"
    STATUS_FAILED = "failed"
    
    def __init__(self, config: Dict[str, Any], logger: StructuredLoggerAdapter):
        """
        Initialize progress tracker with configuration and logger.
        
        Args:
            config: Configuration dictionary from config manager
            logger: StructuredLoggerAdapter instance with PROG component
        """
        self.config = config
        self.logger = logger
        
        # Get database path from configuration
        try:
            if 'paths' in config:
                self.db_path = Path(config['paths'].get('progress_db', 'data/progress_tracking.db'))
            else:
                # Fallback to default path
                self.db_path = Path("data/progress_tracking.db")
        except Exception as e:
            raise ConfigurationError(f"Failed to get progress database path: {e}")
        
        # Ensure database directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Thread-local storage for database connections
        self._local = threading.local()
        
        # Configuration settings
        progress_config = config.get('progress_tracking', {})
        self.max_retry_attempts = progress_config.get('max_retry_attempts', 3)
        self.claim_timeout_minutes = progress_config.get('claim_timeout_minutes', 30)
        self.cleanup_interval_hours = progress_config.get('cleanup_interval_hours', 24)
        
        # Initialize database
        self._initialize_database()
        
        self.logger.info("Progress tracker initialized", extra={
            "component": "WORKER",
            "structured_data": {
                "db_path": str(self.db_path),
                "max_retry_attempts": self.max_retry_attempts,
                "claim_timeout_minutes": self.claim_timeout_minutes
            }
        })
    
    @contextmanager
    def _get_connection(self, timeout: float = 30.0):
        """
        Get thread-local database connection with proper configuration.
        
        Args:
            timeout: Connection timeout in seconds
            
        Yields:
            SQLite connection object
        """
        if not hasattr(self._local, 'connection') or self._local.connection is None:
            try:
                # Create connection with optimized settings
                conn = sqlite3.connect(
                    str(self.db_path),
                    timeout=timeout,
                    check_same_thread=False
                )
                
                # Enable WAL mode for concurrent access
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.execute("PRAGMA cache_size=10000")
                conn.execute("PRAGMA temp_store=MEMORY")
                
                # Set row factory for easier data access
                conn.row_factory = sqlite3.Row
                
                self._local.connection = conn
                
            except Exception as e:
                self.logger.error(f"Failed to create database connection: {e}", extra={
                    "component": "WORKER",
                    "structured_data": {"error": str(e), "db_path": str(self.db_path)}
                })
                raise ProgressTrackingError(f"Database connection failed: {e}")
        
        try:
            yield self._local.connection
        except Exception as e:
            # Rollback any pending transaction
            try:
                self._local.connection.rollback()
            except:
                pass
            raise
    
    def _initialize_database(self):
        """Initialize database schema and indexes."""
        try:
            with self._get_connection() as conn:
                # Create main file processing log table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS file_processing_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        dataset TEXT NOT NULL,
                        file_path TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'pending',
                        worker_id TEXT,
                        start_time TIMESTAMP,
                        end_time TIMESTAMP,
                        processing_duration REAL,
                        records_processed INTEGER,
                        error_message TEXT,
                        retry_count INTEGER DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(dataset, file_path)
                    )
                """)
                
                # Create dataset metadata table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS dataset_metadata (
                        dataset TEXT PRIMARY KEY,
                        total_files INTEGER NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Create worker sessions table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS worker_sessions (
                        worker_id TEXT PRIMARY KEY,
                        dataset TEXT NOT NULL,
                        last_heartbeat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        status TEXT DEFAULT 'active'
                    )
                """)
                
                # Create indexes for performance
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_file_processing_dataset_status 
                    ON file_processing_log(dataset, status)
                """)
                
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_file_processing_worker 
                    ON file_processing_log(worker_id, status)
                """)
                
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_file_processing_updated 
                    ON file_processing_log(updated_at)
                """)
                
                # Create schema version table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS schema_version (
                        version INTEGER PRIMARY KEY,
                        applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Insert current schema version
                conn.execute("""
                    INSERT OR REPLACE INTO schema_version (version) VALUES (?)
                """, (self.SCHEMA_VERSION,))
                
                conn.commit()
                
            self.logger.info("Database schema initialized successfully", extra={
                "component": "WORKER",
                "structured_data": {"schema_version": self.SCHEMA_VERSION}
            })
            
        except Exception as e:
            self.logger.error(f"Failed to initialize database schema: {e}", extra={
                "component": "WORKER",
                "structured_data": {"error": str(e)}
            })
            raise ProgressTrackingError(f"Database initialization failed: {e}")
    
    def initialize_dataset(self, dataset: str, files: List[str]) -> None:
        """
        Initialize progress tracking for a dataset.
        
        Args:
            dataset: Dataset name
            files: List of file paths to track
            
        Raises:
            ProgressTrackingError: If initialization fails
        """
        try:
            with self._get_connection() as conn:
                # Start transaction
                conn.execute("BEGIN IMMEDIATE")
                
                # Insert or update dataset metadata
                conn.execute("""
                    INSERT OR REPLACE INTO dataset_metadata (dataset, total_files, updated_at)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                """, (dataset, len(files)))
                
                # Insert files that don't already exist
                existing_files = set()
                cursor = conn.execute("""
                    SELECT file_path FROM file_processing_log WHERE dataset = ?
                """, (dataset,))
                
                for row in cursor:
                    existing_files.add(row['file_path'])
                
                new_files = []
                for file_path in files:
                    if file_path not in existing_files:
                        new_files.append((dataset, file_path, self.STATUS_PENDING))
                
                if new_files:
                    conn.executemany("""
                        INSERT INTO file_processing_log (dataset, file_path, status)
                        VALUES (?, ?, ?)
                    """, new_files)
                
                conn.commit()
                
                self.logger.info(f"Dataset initialized: {dataset}", extra={
                    "component": "WORKER",
                    "structured_data": {
                        "dataset": dataset,
                        "total_files": len(files),
                        "new_files": len(new_files),
                        "existing_files": len(existing_files)
                    }
                })
                
        except Exception as e:
            self.logger.error(f"Failed to initialize dataset {dataset}: {e}", extra={
                "component": "WORKER",
                "structured_data": {"dataset": dataset, "error": str(e)}
            })
            raise ProgressTrackingError(f"Dataset initialization failed: {e}")
    
    def claim_next_file(self, dataset: str, worker_id: str) -> Optional[str]:
        """
        Atomically claim the next available file for processing.
        
        Args:
            dataset: Dataset name
            worker_id: Unique worker identifier
            
        Returns:
            File path to process or None if no files available
            
        Raises:
            ProgressTrackingError: If claim operation fails
        """
        try:
            with self._get_connection() as conn:
                # Start immediate transaction for atomic operation
                conn.execute("BEGIN IMMEDIATE")
                
                # Clean up stale processing claims first
                timeout_threshold = datetime.now(timezone.utc).timestamp() - (self.claim_timeout_minutes * 60)
                
                conn.execute("""
                    UPDATE file_processing_log 
                    SET status = ?, worker_id = NULL, start_time = NULL
                    WHERE dataset = ? AND status = ? 
                    AND start_time < datetime(?, 'unixepoch')
                """, (self.STATUS_PENDING, dataset, self.STATUS_PROCESSING, timeout_threshold))
                
                # Find next available file
                cursor = conn.execute("""
                    SELECT file_path FROM file_processing_log
                    WHERE dataset = ? AND status = ?
                    ORDER BY created_at ASC
                    LIMIT 1
                """, (dataset, self.STATUS_PENDING))
                
                row = cursor.fetchone()
                if not row:
                    conn.rollback()
                    return None
                
                file_path = row['file_path']
                
                # Claim the file
                current_time = datetime.now(timezone.utc)
                result = conn.execute("""
                    UPDATE file_processing_log
                    SET status = ?, worker_id = ?, start_time = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE dataset = ? AND file_path = ? AND status = ?
                """, (self.STATUS_PROCESSING, worker_id, current_time, dataset, file_path, self.STATUS_PENDING))
                
                if result.rowcount == 0:
                    # File was claimed by another worker
                    conn.rollback()
                    return None
                
                # Update worker session
                conn.execute("""
                    INSERT OR REPLACE INTO worker_sessions (worker_id, dataset, last_heartbeat, status)
                    VALUES (?, ?, CURRENT_TIMESTAMP, 'active')
                """, (worker_id, dataset))
                
                conn.commit()
                
                self.logger.debug(f"File claimed by worker {worker_id}: {file_path}", extra={
                    "component": "WORKER",
                    "structured_data": {
                        "dataset": dataset,
                        "worker_id": worker_id,
                        "file_path": file_path
                    }
                })
                
                return file_path
                
        except Exception as e:
            self.logger.error(f"Failed to claim file for dataset {dataset}: {e}", extra={
                "component": "WORKER",
                "structured_data": {"dataset": dataset, "worker_id": worker_id, "error": str(e)}
            })
            raise ProgressTrackingError(f"File claim failed: {e}")
    
    def mark_file_completed(self, file_path: str, records_processed: int, dataset: Optional[str] = None) -> None:
        """
        Mark a file as successfully completed.
        
        Args:
            file_path: Path to the processed file
            records_processed: Number of records processed from the file
            dataset: Dataset name (optional, will be looked up if not provided)
            
        Raises:
            ProgressTrackingError: If update operation fails
        """
        try:
            with self._get_connection() as conn:
                current_time = datetime.now(timezone.utc)
                
                # Calculate processing duration
                cursor = conn.execute("""
                    SELECT start_time FROM file_processing_log
                    WHERE file_path = ? AND status = ?
                """, (file_path, self.STATUS_PROCESSING))
                
                row = cursor.fetchone()
                if not row:
                    raise ProgressTrackingError(f"File not found in processing state: {file_path}")
                
                start_time = datetime.fromisoformat(row['start_time'].replace('Z', '+00:00'))
                duration = (current_time - start_time).total_seconds()
                
                # Update file status
                result = conn.execute("""
                    UPDATE file_processing_log
                    SET status = ?, end_time = ?, processing_duration = ?, 
                        records_processed = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE file_path = ? AND status = ?
                """, (self.STATUS_COMPLETED, current_time, duration, records_processed, 
                      file_path, self.STATUS_PROCESSING))
                
                if result.rowcount == 0:
                    raise ProgressTrackingError(f"Failed to update file status: {file_path}")
                
                conn.commit()
                
                self.logger.info(f"File completed: {Path(file_path).name}", extra={
                    "component": "WORKER",
                    "structured_data": {
                        "file_path": file_path,
                        "records_processed": records_processed,
                        "processing_duration": round(duration, 2)
                    }
                })
                
        except Exception as e:
            self.logger.error(f"Failed to mark file completed: {file_path}: {e}", extra={
                "component": "WORKER",
                "structured_data": {"file_path": file_path, "error": str(e)}
            })
            raise ProgressTrackingError(f"File completion update failed: {e}")
    
    def mark_file_failed(self, file_path: str, error_message: str, dataset: Optional[str] = None) -> None:
        """
        Mark a file as failed with error information.
        
        Args:
            file_path: Path to the failed file
            error_message: Error message describing the failure
            dataset: Dataset name (optional, will be looked up if not provided)
            
        Raises:
            ProgressTrackingError: If update operation fails
        """
        try:
            with self._get_connection() as conn:
                current_time = datetime.now(timezone.utc)
                
                # Get current retry count and start time
                cursor = conn.execute("""
                    SELECT start_time, retry_count FROM file_processing_log
                    WHERE file_path = ? AND status = ?
                """, (file_path, self.STATUS_PROCESSING))
                
                row = cursor.fetchone()
                if not row:
                    raise ProgressTrackingError(f"File not found in processing state: {file_path}")
                
                start_time = datetime.fromisoformat(row['start_time'].replace('Z', '+00:00'))
                duration = (current_time - start_time).total_seconds()
                retry_count = row['retry_count'] + 1
                
                # Determine if we should retry or mark as failed
                if retry_count <= self.max_retry_attempts:
                    new_status = self.STATUS_PENDING
                    self.logger.warning(f"File failed, will retry ({retry_count}/{self.max_retry_attempts}): {Path(file_path).name}", extra={
                        "component": "WORKER",
                        "structured_data": {
                            "file_path": file_path,
                            "retry_count": retry_count,
                            "error": error_message
                        }
                    })
                else:
                    new_status = self.STATUS_FAILED
                    self.logger.error(f"File failed permanently after {retry_count} attempts: {Path(file_path).name}", extra={
                        "component": "WORKER",
                        "structured_data": {
                            "file_path": file_path,
                            "retry_count": retry_count,
                            "error": error_message
                        }
                    })
                
                # Update file status
                result = conn.execute("""
                    UPDATE file_processing_log
                    SET status = ?, end_time = ?, processing_duration = ?, 
                        error_message = ?, retry_count = ?, worker_id = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE file_path = ? AND status = ?
                """, (new_status, current_time, duration, error_message, retry_count,
                      file_path, self.STATUS_PROCESSING))
                
                if result.rowcount == 0:
                    raise ProgressTrackingError(f"Failed to update file status: {file_path}")
                
                conn.commit()
                
        except Exception as e:
            self.logger.error(f"Failed to mark file failed: {file_path}: {e}", extra={
                "component": "WORKER",
                "structured_data": {"file_path": file_path, "error": str(e)}
            })
            raise ProgressTrackingError(f"File failure update failed: {e}")
    
    def get_processing_stats(self, dataset: str) -> ProcessingStats:
        """
        Get comprehensive processing statistics for a dataset.
        
        Args:
            dataset: Dataset name
            
        Returns:
            ProcessingStats object with current progress information
            
        Raises:
            ProgressTrackingError: If stats retrieval fails
        """
        try:
            with self._get_connection() as conn:
                # Get file counts by status
                cursor = conn.execute("""
                    SELECT status, COUNT(*) as count
                    FROM file_processing_log
                    WHERE dataset = ?
                    GROUP BY status
                """, (dataset,))
                
                status_counts = {self.STATUS_PENDING: 0, self.STATUS_PROCESSING: 0, 
                               self.STATUS_COMPLETED: 0, self.STATUS_FAILED: 0}
                
                for row in cursor:
                    status_counts[row['status']] = row['count']
                
                total_files = sum(status_counts.values())
                
                if total_files == 0:
                    raise ProgressTrackingError(f"No files found for dataset: {dataset}")
                
                # Calculate success rate
                completed = status_counts[self.STATUS_COMPLETED]
                failed = status_counts[self.STATUS_FAILED]
                finished = completed + failed
                success_rate = (completed / finished * 100) if finished > 0 else 0
                
                # Get average processing time
                cursor = conn.execute("""
                    SELECT AVG(processing_duration) as avg_duration
                    FROM file_processing_log
                    WHERE dataset = ? AND status = ? AND processing_duration IS NOT NULL
                """, (dataset, self.STATUS_COMPLETED))
                
                row = cursor.fetchone()
                avg_processing_time = row['avg_duration'] or 0
                
                # Estimate completion time
                pending = status_counts[self.STATUS_PENDING]
                processing = status_counts[self.STATUS_PROCESSING]
                remaining = pending + processing
                
                estimated_completion = None
                if remaining > 0 and avg_processing_time > 0:
                    # Get active worker count
                    cursor = conn.execute("""
                        SELECT COUNT(DISTINCT worker_id) as active_workers
                        FROM worker_sessions
                        WHERE dataset = ? AND status = 'active'
                        AND last_heartbeat > datetime('now', '-5 minutes')
                    """, (dataset,))
                    
                    active_workers = cursor.fetchone()['active_workers'] or 1
                    
                    estimated_seconds = (remaining * avg_processing_time) / active_workers
                    estimated_completion = datetime.now() + timedelta(seconds=estimated_seconds)
                    estimated_completion = estimated_completion.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    active_workers = 0
                
                stats = ProcessingStats(
                    dataset=dataset,
                    total_files=total_files,
                    pending_files=status_counts[self.STATUS_PENDING],
                    processing_files=status_counts[self.STATUS_PROCESSING],
                    completed_files=status_counts[self.STATUS_COMPLETED],
                    failed_files=status_counts[self.STATUS_FAILED],
                    success_rate=round(success_rate, 1),
                    avg_processing_time=round(avg_processing_time, 2),
                    estimated_completion=estimated_completion,
                    active_workers=active_workers
                )
                
                self.logger.debug(f"Retrieved stats for dataset: {dataset}", extra={
                    "component": "WORKER",
                    "structured_data": {
                        "dataset": dataset,
                        "total_files": total_files,
                        "completed": completed,
                        "success_rate": success_rate
                    }
                })
                
                return stats
                
        except Exception as e:
            self.logger.error(f"Failed to get processing stats for dataset {dataset}: {e}", extra={
                "component": "WORKER",
                "structured_data": {"dataset": dataset, "error": str(e)}
            })
            raise ProgressTrackingError(f"Stats retrieval failed: {e}")
    
    def get_file_status(self, file_path: str) -> Optional[FileStatus]:
        """
        Get detailed status information for a specific file.
        
        Args:
            file_path: Path to the file
            
        Returns:
            FileStatus object or None if file not found
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.execute("""
                    SELECT * FROM file_processing_log WHERE file_path = ?
                """, (file_path,))
                
                row = cursor.fetchone()
                if not row:
                    return None
                
                # Parse timestamps
                start_time = None
                end_time = None
                
                if row['start_time']:
                    start_time = datetime.fromisoformat(row['start_time'].replace('Z', '+00:00'))
                
                if row['end_time']:
                    end_time = datetime.fromisoformat(row['end_time'].replace('Z', '+00:00'))
                
                return FileStatus(
                    file_path=row['file_path'],
                    status=row['status'],
                    worker_id=row['worker_id'],
                    start_time=start_time,
                    end_time=end_time,
                    processing_duration=row['processing_duration'],
                    records_processed=row['records_processed'],
                    error_message=row['error_message'],
                    retry_count=row['retry_count']
                )
                
        except Exception as e:
            self.logger.error(f"Failed to get file status: {file_path}: {e}", extra={
                "component": "WORKER",
                "structured_data": {"file_path": file_path, "error": str(e)}
            })
            return None
    
    def cleanup_stale_sessions(self) -> int:
        """
        Clean up stale worker sessions and reset abandoned files.
        
        Returns:
            Number of files reset to pending status
        """
        try:
            with self._get_connection() as conn:
                # Reset files from inactive workers
                timeout_threshold = datetime.now(timezone.utc).timestamp() - (self.claim_timeout_minutes * 60)
                
                result = conn.execute("""
                    UPDATE file_processing_log 
                    SET status = ?, worker_id = NULL, start_time = NULL
                    WHERE status = ? AND start_time < datetime(?, 'unixepoch')
                """, (self.STATUS_PENDING, self.STATUS_PROCESSING, timeout_threshold))
                
                reset_count = result.rowcount
                
                # Clean up old worker sessions
                conn.execute("""
                    DELETE FROM worker_sessions 
                    WHERE last_heartbeat < datetime('now', '-1 hour')
                """, )
                
                conn.commit()
                
                if reset_count > 0:
                    self.logger.info(f"Cleaned up {reset_count} stale processing claims", extra={
                        "component": "WORKER",
                        "structured_data": {"reset_files": reset_count}
                    })
                
                return reset_count
                
        except Exception as e:
            self.logger.error(f"Failed to cleanup stale sessions: {e}", extra={
                "component": "WORKER",
                "structured_data": {"error": str(e)}
            })
            return 0
    
    def reset_dataset(self, dataset: str) -> bool:
        """
        Reset all files in a dataset back to pending status.
        
        Args:
            dataset: Dataset name to reset
            
        Returns:
            True if reset was successful
        """
        try:
            with self._get_connection() as conn:
                result = conn.execute("""
                    UPDATE file_processing_log 
                    SET status = ?, worker_id = NULL, start_time = NULL, 
                        end_time = NULL, processing_duration = NULL, 
                        error_message = NULL, updated_at = CURRENT_TIMESTAMP
                    WHERE dataset = ? AND status IN (?, ?, ?)
                """, (self.STATUS_PENDING, dataset, self.STATUS_PROCESSING, 
                      self.STATUS_COMPLETED, self.STATUS_FAILED))
                
                conn.commit()
                reset_count = result.rowcount
                
                self.logger.info(f"Reset dataset {dataset}: {reset_count} files", extra={
                    "component": "WORKER",
                    "structured_data": {"dataset": dataset, "reset_count": reset_count}
                })
                
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to reset dataset {dataset}: {e}", extra={
                "component": "WORKER",
                "structured_data": {"dataset": dataset, "error": str(e)}
            })
            return False
    
    def close(self):
        """Close database connections and cleanup resources."""
        if hasattr(self._local, 'connection') and self._local.connection:
            try:
                self._local.connection.close()
                self._local.connection = None
            except Exception as e:
                self.logger.warning(f"Error closing database connection: {e}", extra={
                    "component": "WORKER"
                })

# Convenience functions for the available_functions.py interface

def initialize_progress_tracking(dataset: str, files: List[str], config: Dict[str, Any], 
                               logger: StructuredLoggerAdapter) -> None:
    """
    Initialize progress tracking for a dataset.
    
    This is one of the required functions from available_functions.py.
    Creates a ProgressTracker instance and initializes the dataset.
    
    Args:
        dataset: Dataset name
        files: List of file paths to track
        config: Configuration dictionary from config manager
        logger: StructuredLoggerAdapter instance with WORKER component
        
    Raises:
        ProgressTrackingError: If initialization fails
    """
    tracker = ProgressTracker(config, logger)
    try:
        tracker.initialize_dataset(dataset, files)
    finally:
        tracker.close()

def claim_next_file(dataset: str, worker_id: str, config: Dict[str, Any], 
                   logger: StructuredLoggerAdapter) -> Optional[str]:
    """
    Claim the next available file for processing.
    
    This is one of the required functions from available_functions.py.
    
    Args:
        dataset: Dataset name
        worker_id: Unique worker identifier
        config: Configuration dictionary from config manager
        logger: StructuredLoggerAdapter instance with WORKER component
        
    Returns:
        File path to process or None if no files available
    """
    tracker = ProgressTracker(config, logger)
    try:
        return tracker.claim_next_file(dataset, worker_id)
    finally:
        tracker.close()

def mark_file_completed(file_path: str, records: int, config: Dict[str, Any], 
                       logger: StructuredLoggerAdapter) -> None:
    """
    Mark a file as successfully completed.
    
    This is one of the required functions from available_functions.py.
    
    Args:
        file_path: Path to the processed file
        records: Number of records processed from the file
        config: Configuration dictionary from config manager
        logger: StructuredLoggerAdapter instance with WORKER component
    """
    tracker = ProgressTracker(config, logger)
    try:
        tracker.mark_file_completed(file_path, records)
    finally:
        tracker.close()

def mark_file_failed(file_path: str, error: str, config: Dict[str, Any], 
                    logger: StructuredLoggerAdapter) -> None:
    """
    Mark a file as failed with error information.
    
    This is one of the required functions from available_functions.py.
    
    Args:
        file_path: Path to the failed file
        error: Error message describing the failure
        config: Configuration dictionary from config manager
        logger: StructuredLoggerAdapter instance with WORKER component
    """
    tracker = ProgressTracker(config, logger)
    try:
        tracker.mark_file_failed(file_path, error)
    finally:
        tracker.close()

def get_processing_stats(dataset: str, config: Dict[str, Any], 
                        logger: StructuredLoggerAdapter) -> Dict[str, int]:
    """
    Get processing statistics for a dataset.
    
    This is one of the required functions from available_functions.py.
    
    Args:
        dataset: Dataset name
        config: Configuration dictionary from config manager
        logger: StructuredLoggerAdapter instance with WORKER component
        
    Returns:
        Dictionary with processing statistics
    """
    tracker = ProgressTracker(config, logger)
    try:
        stats = tracker.get_processing_stats(dataset)
        return {
            'total_files': stats.total_files,
            'pending_files': stats.pending_files,
            'processing_files': stats.processing_files,
            'completed_files': stats.completed_files,
            'failed_files': stats.failed_files,
            'active_workers': stats.active_workers
        }
    finally:
        tracker.close()

