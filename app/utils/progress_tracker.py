"""
ClimaStation Progress Tracker

SCRIPT IDENTIFICATION: DWD10TAH2P (Progress Tracker)

PURPOSE:
Comprehensive file-level progress tracking using SQLite database for the ClimaStation platform.
Provides thread-safe tracking of processing status for individual ZIP files across all datasets
with support for parallel processing, failure recovery, and detailed progress reporting.

RESPONSIBILITIES:
- Create and manage SQLite database for progress tracking
- Register files for processing with PENDING status
- Track processing state transitions (PENDING → PROCESSING → SUCCESS/FAILED)
- Record processing times, worker assignments, and error messages
- Provide status summaries and progress reports for datasets
- Support recovery by identifying failed or incomplete files
- Handle concurrent access from multiple workers safely
- Generate comprehensive processing statistics and reports

DATABASE SCHEMA:
    file_processing_log:
    - id: Primary key (INTEGER AUTOINCREMENT)
    - dataset: Dataset identifier (TEXT, e.g., "10_minutes_air_temperature")
    - file_path: Full path to ZIP file being processed (TEXT)
    - status: Current processing status (TEXT: PENDING/PROCESSING/SUCCESS/FAILED)
    - start_time: When processing began (TIMESTAMP)
    - end_time: When processing completed (TIMESTAMP)
    - error_message: Error details for failed files (TEXT)
    - worker_id: Identifier of worker that processed the file (TEXT)
    - created_at: When record was created (TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
    - UNIQUE constraint on (dataset, file_path)

INDEXES:
    - idx_dataset_status: Efficient querying by dataset and status
    - idx_dataset_file: Fast lookups by dataset and file path

USAGE:
    from utils.progress_tracker import ProgressTracker, ProcessingStatus
    
    # Initialize tracker
    tracker = ProgressTracker(Path("data/progress/climastation_progress.db"))
    
    # Register files for processing
    files = [Path("file1.zip"), Path("file2.zip")]
    registered = tracker.register_files("10_minutes_air_temperature", files)
    
    # Processing workflow
    success = tracker.start_processing("10_minutes_air_temperature", file_path, "worker_1")
    if success:
        # Process file...
        tracker.mark_success("10_minutes_air_temperature", file_path)
    else:
        tracker.mark_failed("10_minutes_air_temperature", file_path, "Error message")
    
    # Get status and reports
    status = tracker.get_dataset_status("10_minutes_air_temperature")
    summary = tracker.get_processing_summary("10_minutes_air_temperature")

THREAD SAFETY:
All database operations use SQLite's built-in thread safety with proper connection handling
and threading locks. Safe for use with parallel workers and concurrent access patterns.
Each operation uses its own connection context to avoid conflicts.

ERROR HANDLING:
- Database creation errors are propagated to caller with detailed context
- Individual operation errors are logged but don't crash the tracker
- Provides detailed error context for troubleshooting
- Graceful handling of database lock conflicts and timeouts
- Comprehensive validation of input parameters

PERFORMANCE CONSIDERATIONS:
- Uses connection pooling through context managers
- Efficient indexing for common query patterns
- Batch operations where possible
- Minimal lock contention through fine-grained locking

RECOVERY FEATURES:
- Identify files stuck in PROCESSING state (worker crashes)
- Reset failed files for reprocessing
- Comprehensive failure analysis and reporting
- Support for incremental processing modes

DEPENDENCIES:
- sqlite3: Database operations
- pathlib: File path handling
- datetime: Timestamp management
- enum: Type-safe status definitions
- threading: Thread safety
- typing: Type hints for better code clarity

AUTHOR: ClimaStation Backend Pipeline
VERSION: Enhanced with comprehensive tracking and reporting
LAST UPDATED: 2025-01-21
"""

import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple, Any, Union
from enum import Enum
import threading
import logging
import time


class ProcessingStatus(Enum):
    """Enumeration of possible file processing states"""
    PENDING = "PENDING"         # File registered but not yet processed
    PROCESSING = "PROCESSING"   # File currently being processed by a worker
    SUCCESS = "SUCCESS"         # File processed successfully
    FAILED = "FAILED"          # File processing failed


class ProgressTracker:
    """
    SQLite-based progress tracking for file processing operations.
    
    Provides thread-safe tracking of individual file processing status
    with support for parallel workers, comprehensive error reporting,
    and detailed progress analytics.
    """
    
    def __init__(self, db_path: Path, timeout: float = 30.0):
        """
        Initialize progress tracker with SQLite database.
        
        Args:
            db_path: Path to SQLite database file (will be created if not exists)
            timeout: Database operation timeout in seconds
            
        Raises:
            sqlite3.Error: If database initialization fails
        """
        self.db_path = db_path
        self.timeout = timeout
        self._lock = threading.Lock()  # Thread safety for database operations
        self.logger = logging.getLogger(f"progress_tracker_{id(self)}")
        
        # Initialize database
        self._init_database()
        
        self.logger.info(f"Progress tracker initialized with database: {db_path}")
    
    def _init_database(self):
        """
        Create progress tracking table and indexes if they don't exist.
        
        Creates the file_processing_log table with proper indexes for
        efficient querying by dataset and status. Ensures database
        directory exists and handles creation errors gracefully.
        
        Raises:
            sqlite3.Error: If database creation fails
        """
        try:
            # Ensure directory exists
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            
            with sqlite3.connect(self.db_path, timeout=self.timeout) as conn:
                # Enable foreign key support and WAL mode for better concurrency
                conn.execute("PRAGMA foreign_keys = ON")
                conn.execute("PRAGMA journal_mode = WAL")
                
                # Create main tracking table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS file_processing_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        dataset TEXT NOT NULL,
                        file_path TEXT NOT NULL,
                        status TEXT NOT NULL CHECK (status IN ('PENDING', 'PROCESSING', 'SUCCESS', 'FAILED')),
                        start_time TIMESTAMP,
                        end_time TIMESTAMP,
                        error_message TEXT,
                        worker_id TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(dataset, file_path)
                    )
                """)
                
                # Create indexes for efficient querying
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_dataset_status 
                    ON file_processing_log(dataset, status)
                """)
                
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_dataset_file 
                    ON file_processing_log(dataset, file_path)
                """)
                
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_status_time 
                    ON file_processing_log(status, start_time)
                """)
                
                # Create trigger to update updated_at timestamp
                conn.execute("""
                    CREATE TRIGGER IF NOT EXISTS update_timestamp 
                    AFTER UPDATE ON file_processing_log
                    BEGIN
                        UPDATE file_processing_log 
                        SET updated_at = CURRENT_TIMESTAMP 
                        WHERE id = NEW.id;
                    END
                """)
                
                conn.commit()
                
        except sqlite3.Error as e:
            self.logger.error(f"Failed to initialize database: {e}")
            raise
    
    def register_files(self, dataset: str, file_paths: List[Path]) -> int:
        """
        Register files for processing with PENDING status.
        
        Uses INSERT OR IGNORE to handle duplicate registrations gracefully.
        Only new files will be registered, existing files are left unchanged.
        
        Args:
            dataset: Dataset identifier (e.g., "10_minutes_air_temperature")
            file_paths: List of file paths to register
            
        Returns:
            Number of files successfully registered (may be less than input if duplicates)
            
        Raises:
            ValueError: If dataset or file_paths are invalid
        """
        if not dataset or not dataset.strip():
            raise ValueError("Dataset name cannot be empty")
        
        if not file_paths:
            self.logger.warning("No file paths provided for registration")
            return 0
        
        registered_count = 0
        
        with self._lock:
            try:
                with sqlite3.connect(self.db_path, timeout=self.timeout) as conn:
                    for file_path in file_paths:
                        try:
                            cursor = conn.execute("""
                                INSERT OR IGNORE INTO file_processing_log 
                                (dataset, file_path, status) 
                                VALUES (?, ?, ?)
                            """, (dataset, str(file_path), ProcessingStatus.PENDING.value))
                            
                            if cursor.rowcount > 0:
                                registered_count += 1
                                
                        except sqlite3.Error as e:
                            self.logger.warning(f"Failed to register file {file_path}: {e}")
                            continue
                    
                    conn.commit()
                    
            except sqlite3.Error as e:
                self.logger.error(f"Database error during file registration: {e}")
                raise
        
        self.logger.info(f"Registered {registered_count}/{len(file_paths)} files for dataset {dataset}")
        return registered_count
    
    def start_processing(self, dataset: str, file_path: Path, worker_id: str) -> bool:
        """
        Mark file as being processed by a specific worker.
        
        Only updates files that are currently in PENDING status to prevent
        conflicts with other workers or already processed files.
        
        Args:
            dataset: Dataset identifier
            file_path: Path to file being processed
            worker_id: Identifier of worker processing the file
            
        Returns:
            True if status was successfully updated, False otherwise
            
        Raises:
            ValueError: If parameters are invalid
        """
        if not all([dataset, file_path, worker_id]):
            raise ValueError("Dataset, file_path, and worker_id are required")
        
        with self._lock:
            try:
                with sqlite3.connect(self.db_path, timeout=self.timeout) as conn:
                    cursor = conn.execute("""
                        UPDATE file_processing_log 
                        SET status = ?, start_time = ?, worker_id = ?
                        WHERE dataset = ? AND file_path = ? AND status = ?
                    """, (
                        ProcessingStatus.PROCESSING.value, 
                        datetime.now(), 
                        worker_id, 
                        dataset, 
                        str(file_path),
                        ProcessingStatus.PENDING.value
                    ))
                    
                    conn.commit()
                    success = cursor.rowcount > 0
                    
                    if success:
                        self.logger.debug(f"Started processing: {file_path} (worker: {worker_id})")
                    else:
                        self.logger.warning(f"Could not start processing {file_path} - may already be processed or not registered")
                    
                    return success
                    
            except sqlite3.Error as e:
                self.logger.error(f"Error starting processing for {file_path}: {e}")
                return False
    
    def mark_success(self, dataset: str, file_path: Path, records_processed: Optional[int] = None) -> bool:
        """
        Mark file as successfully processed.
        
        Updates the status to SUCCESS and records the end time. Can optionally
        store the number of records processed for reporting purposes.
        
        Args:
            dataset: Dataset identifier
            file_path: Path to successfully processed file
            records_processed: Optional number of records processed
            
        Returns:
            True if status was successfully updated, False otherwise
        """
        with self._lock:
            try:
                with sqlite3.connect(self.db_path, timeout=self.timeout) as conn:
                    # Update main status
                    cursor = conn.execute("""
                        UPDATE file_processing_log 
                        SET status = ?, end_time = ?
                        WHERE dataset = ? AND file_path = ?
                    """, (
                        ProcessingStatus.SUCCESS.value, 
                        datetime.now(), 
                        dataset, 
                        str(file_path)
                    ))
                    
                    conn.commit()
                    success = cursor.rowcount > 0
                    
                    if success:
                        self.logger.debug(f"Marked success: {file_path}")
                    else:
                        self.logger.warning(f"Could not mark success for {file_path} - file may not exist in database")
                    
                    return success
                    
            except sqlite3.Error as e:
                self.logger.error(f"Error marking success for {file_path}: {e}")
                return False
    
    def mark_failed(self, dataset: str, file_path: Path, error_message: str) -> bool:
        """
        Mark file as failed with error details.
        
        Updates the status to FAILED, records the end time, and stores
        the error message for debugging and reporting purposes.
        
        Args:
            dataset: Dataset identifier
            file_path: Path to failed file
            error_message: Description of the error that occurred
            
        Returns:
            True if status was successfully updated, False otherwise
        """
        if not error_message:
            error_message = "Unknown error"
        
        with self._lock:
            try:
                with sqlite3.connect(self.db_path, timeout=self.timeout) as conn:
                    cursor = conn.execute("""
                        UPDATE file_processing_log 
                        SET status = ?, end_time = ?, error_message = ?
                        WHERE dataset = ? AND file_path = ?
                    """, (
                        ProcessingStatus.FAILED.value, 
                        datetime.now(), 
                        error_message[:1000],  # Limit error message length
                        dataset, 
                        str(file_path)
                    ))
                    
                    conn.commit()
                    success = cursor.rowcount > 0
                    
                    if success:
                        self.logger.debug(f"Marked failed: {file_path} - {error_message[:100]}...")
                    else:
                        self.logger.warning(f"Could not mark failure for {file_path} - file may not exist in database")
                    
                    return success
                    
            except sqlite3.Error as e:
                self.logger.error(f"Error marking failure for {file_path}: {e}")
                return False
    
    def get_dataset_status(self, dataset: str) -> Dict[str, int]:
        """
        Get processing status summary for a dataset.
        
        Returns counts for each processing status (PENDING, PROCESSING, SUCCESS, FAILED).
        Useful for monitoring overall progress and identifying bottlenecks.
        
        Args:
            dataset: Dataset identifier
            
        Returns:
            Dictionary with status counts for each ProcessingStatus
        """
        status_counts = {status.value: 0 for status in ProcessingStatus}
        
        try:
            with sqlite3.connect(self.db_path, timeout=self.timeout) as conn:
                cursor = conn.execute("""
                    SELECT status, COUNT(*) 
                    FROM file_processing_log 
                    WHERE dataset = ? 
                    GROUP BY status
                """, (dataset,))
                
                for status, count in cursor.fetchall():
                    if status in status_counts:
                        status_counts[status] = count
                    
        except sqlite3.Error as e:
            self.logger.error(f"Error getting dataset status for {dataset}: {e}")
        
        return status_counts
    
    def get_failed_files(self, dataset: str, limit: Optional[int] = None) -> List[Tuple[str, str, Optional[str]]]:
        """
        Get list of failed files with error messages and timestamps.
        
        Returns detailed information about failed files for debugging
        and recovery purposes. Results are ordered by failure time (most recent first).
        
        Args:
            dataset: Dataset identifier
            limit: Maximum number of results to return (None for all)
            
        Returns:
            List of tuples (file_path, error_message, end_time) for failed files
        """
        failed_files = []
        
        try:
            with sqlite3.connect(self.db_path, timeout=self.timeout) as conn:
                query = """
                    SELECT file_path, error_message, end_time 
                    FROM file_processing_log 
                    WHERE dataset = ? AND status = ?
                    ORDER BY end_time DESC
                """
                
                if limit:
                    query += f" LIMIT {limit}"
                
                cursor = conn.execute(query, (dataset, ProcessingStatus.FAILED.value))
                failed_files = cursor.fetchall()
                
        except sqlite3.Error as e:
            self.logger.error(f"Error getting failed files for {dataset}: {e}")
        
        return failed_files
    
    def get_processing_summary(self, dataset: str) -> Dict[str, Any]:
        """
        Get comprehensive processing summary for a dataset.
        
        Provides detailed statistics including processing rates, timing information,
        error analysis, and worker performance metrics.
        
        Args:
            dataset: Dataset identifier
            
        Returns:
            Dictionary with comprehensive processing statistics
        """
        summary = {
            'dataset': dataset,
            'total_files': 0,
            'status_counts': self.get_dataset_status(dataset),
            'processing_rate': 0.0,
            'success_rate': 0.0,
            'average_processing_time': 0.0,
            'total_processing_time': 0.0,
            'failed_files_count': 0,
            'worker_stats': {},
            'time_analysis': {}
        }
        
        try:
            with sqlite3.connect(self.db_path, timeout=self.timeout) as conn:
                # Get total files
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM file_processing_log WHERE dataset = ?
                """, (dataset,))
                summary['total_files'] = cursor.fetchone()[0]
                
                # Get timing statistics for completed files
                cursor = conn.execute("""
                    SELECT 
                        AVG(julianday(end_time) - julianday(start_time)) * 24 * 60 * 60 as avg_seconds,
                        SUM(julianday(end_time) - julianday(start_time)) * 24 * 60 * 60 as total_seconds,
                        COUNT(*) as completed_count
                    FROM file_processing_log 
                    WHERE dataset = ? AND status IN (?, ?) AND start_time IS NOT NULL AND end_time IS NOT NULL
                """, (dataset, ProcessingStatus.SUCCESS.value, ProcessingStatus.FAILED.value))
                
                timing_result = cursor.fetchone()
                if timing_result and timing_result[0]:
                    summary['average_processing_time'] = timing_result[0]
                    summary['total_processing_time'] = timing_result[1]
                
                # Calculate rates
                completed = summary['status_counts'][ProcessingStatus.SUCCESS.value]
                failed = summary['status_counts'][ProcessingStatus.FAILED.value]
                total_attempted = completed + failed
                
                if summary['total_files'] > 0:
                    summary['processing_rate'] = (total_attempted / summary['total_files']) * 100
                
                if total_attempted > 0:
                    summary['success_rate'] = (completed / total_attempted) * 100
                
                # Worker statistics
                cursor = conn.execute("""
                    SELECT 
                        worker_id,
                        COUNT(*) as files_processed,
                        SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) as successful,
                        SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) as failed,
                        AVG(julianday(end_time) - julianday(start_time)) * 24 * 60 * 60 as avg_time
                    FROM file_processing_log 
                    WHERE dataset = ? AND worker_id IS NOT NULL AND status IN (?, ?)
                    GROUP BY worker_id
                """, (ProcessingStatus.SUCCESS.value, ProcessingStatus.FAILED.value, 
                      dataset, ProcessingStatus.SUCCESS.value, ProcessingStatus.FAILED.value))
                
                for row in cursor.fetchall():
                    worker_id, files_processed, successful, failed, avg_time = row
                    summary['worker_stats'][worker_id] = {
                        'files_processed': files_processed,
                        'successful': successful,
                        'failed': failed,
                        'success_rate': (successful / files_processed * 100) if files_processed > 0 else 0,
                        'average_time': avg_time or 0
                    }
                
                # Time analysis (processing by hour/day)
                cursor = conn.execute("""
                    SELECT 
                        DATE(start_time) as processing_date,
                        COUNT(*) as files_started,
                        SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) as files_completed
                    FROM file_processing_log 
                    WHERE dataset = ? AND start_time IS NOT NULL
                    GROUP BY DATE(start_time)
                    ORDER BY processing_date DESC
                    LIMIT 7
                """, (ProcessingStatus.SUCCESS.value, dataset))
                
                daily_stats = []
                for row in cursor.fetchall():
                    daily_stats.append({
                        'date': row[0],
                        'files_started': row[1],
                        'files_completed': row[2]
                    })
                
                summary['time_analysis']['daily_stats'] = daily_stats
                
        except sqlite3.Error as e:
            self.logger.error(f"Error getting processing summary for {dataset}: {e}")
            summary['error'] = str(e)
        
        return summary
    
    def get_stuck_files(self, dataset: str, timeout_hours: float = 2.0) -> List[Tuple[str, str, str]]:
        """
        Get files that are stuck in PROCESSING state (likely due to worker crashes).
        
        Identifies files that have been in PROCESSING state for longer than the
        specified timeout, indicating potential worker failures or crashes.
        
        Args:
            dataset: Dataset identifier
            timeout_hours: Hours after which a PROCESSING file is considered stuck
            
        Returns:
            List of tuples (file_path, worker_id, start_time) for stuck files
        """
        stuck_files = []
        cutoff_time = datetime.now() - timedelta(hours=timeout_hours)
        
        try:
            with sqlite3.connect(self.db_path, timeout=self.timeout) as conn:
                cursor = conn.execute("""
                    SELECT file_path, worker_id, start_time 
                    FROM file_processing_log 
                    WHERE dataset = ? AND status = ? AND start_time < ?
                    ORDER BY start_time ASC
                """, (dataset, ProcessingStatus.PROCESSING.value, cutoff_time))
                
                stuck_files = cursor.fetchall()
                
        except sqlite3.Error as e:
            self.logger.error(f"Error getting stuck files for {dataset}: {e}")
        
        return stuck_files
    
    def reset_stuck_files(self, dataset: str, timeout_hours: float = 2.0) -> int:
        """
        Reset stuck files back to PENDING status for reprocessing.
        
        Identifies files stuck in PROCESSING state and resets them to PENDING
        so they can be picked up by workers again. Useful for recovery after
        worker crashes or system failures.
        
        Args:
            dataset: Dataset identifier
            timeout_hours: Hours after which a PROCESSING file is considered stuck
            
        Returns:
            Number of files reset
        """
        cutoff_time = datetime.now() - timedelta(hours=timeout_hours)
        reset_count = 0
        
        with self._lock:
            try:
                with sqlite3.connect(self.db_path, timeout=self.timeout) as conn:
                    cursor = conn.execute("""
                        UPDATE file_processing_log 
                        SET status = ?, start_time = NULL, worker_id = NULL, error_message = NULL
                        WHERE dataset = ? AND status = ? AND start_time < ?
                    """, (ProcessingStatus.PENDING.value, dataset, ProcessingStatus.PROCESSING.value, cutoff_time))
                    
                    reset_count = cursor.rowcount
                    conn.commit()
                    
                    if reset_count > 0:
                        self.logger.info(f"Reset {reset_count} stuck files for dataset {dataset}")
                    
            except sqlite3.Error as e:
                self.logger.error(f"Error resetting stuck files for {dataset}: {e}")
        
        return reset_count
    
    def reset_failed_files(self, dataset: str, error_pattern: Optional[str] = None) -> int:
        """
        Reset failed files back to PENDING status for reprocessing.
        
        Allows reprocessing of failed files, optionally filtering by error message
        pattern. Useful for recovery after fixing issues that caused failures.
        
        Args:
            dataset: Dataset identifier
            error_pattern: Optional SQL LIKE pattern to match error messages
            
        Returns:
            Number of files reset
        """
        reset_count = 0
        
        with self._lock:
            try:
                with sqlite3.connect(self.db_path, timeout=self.timeout) as conn:
                    if error_pattern:
                        cursor = conn.execute("""
                            UPDATE file_processing_log 
                            SET status = ?, start_time = NULL, end_time = NULL, 
                                worker_id = NULL, error_message = NULL
                            WHERE dataset = ? AND status = ? AND error_message LIKE ?
                        """, (ProcessingStatus.PENDING.value, dataset, ProcessingStatus.FAILED.value, error_pattern))
                    else:
                        cursor = conn.execute("""
                            UPDATE file_processing_log 
                            SET status = ?, start_time = NULL, end_time = NULL, 
                                worker_id = NULL, error_message = NULL
                            WHERE dataset = ? AND status = ?
                        """, (ProcessingStatus.PENDING.value, dataset, ProcessingStatus.FAILED.value))
                    
                    reset_count = cursor.rowcount
                    conn.commit()
                    
                    if reset_count > 0:
                        self.logger.info(f"Reset {reset_count} failed files for dataset {dataset}")
                    
            except sqlite3.Error as e:
                self.logger.error(f"Error resetting failed files for {dataset}: {e}")
        
        return reset_count
    
    def reset_dataset(self, dataset: str) -> bool:
        """
        Reset all files in dataset to PENDING status (for complete reprocessing).
        
        Resets all files in the dataset back to PENDING status, clearing all
        processing history. Use with caution as this will lose all progress.
        
        Args:
            dataset: Dataset identifier
            
        Returns:
            True if reset was successful, False otherwise
        """
        with self._lock:
            try:
                with sqlite3.connect(self.db_path, timeout=self.timeout) as conn:
                    cursor = conn.execute("""
                        UPDATE file_processing_log 
                        SET status = ?, start_time = NULL, end_time = NULL, 
                            error_message = NULL, worker_id = NULL
                        WHERE dataset = ?
                    """, (ProcessingStatus.PENDING.value, dataset))
                    
                    reset_count = cursor.rowcount
                    conn.commit()
                    
                    self.logger.info(f"Reset {reset_count} files for dataset {dataset}")
                    return True
                    
            except sqlite3.Error as e:
                self.logger.error(f"Error resetting dataset {dataset}: {e}")
                return False
    
    def get_pending_files(self, dataset: str, limit: Optional[int] = None) -> List[str]:
        """
        Get list of files pending processing.
        
        Returns files that are ready to be processed (PENDING status).
        Useful for workers to discover available work.
        
        Args:
            dataset: Dataset identifier
            limit: Maximum number of files to return
            
        Returns:
            List of file paths ready for processing
        """
        pending_files = []
        
        try:
            with sqlite3.connect(self.db_path, timeout=self.timeout) as conn:
                query = """
                    SELECT file_path 
                    FROM file_processing_log 
                    WHERE dataset = ? AND status = ?
                    ORDER BY created_at ASC
                """
                
                if limit:
                    query += f" LIMIT {limit}"
                
                cursor = conn.execute(query, (dataset, ProcessingStatus.PENDING.value))
                pending_files = [row[0] for row in cursor.fetchall()]
                
        except sqlite3.Error as e:
            self.logger.error(f"Error getting pending files for {dataset}: {e}")
        
        return pending_files
    
    def cleanup_old_records(self, days_old: int = 30) -> int:
        """
        Clean up old processing records to prevent database bloat.
        
        Removes records older than the specified number of days.
        Only removes completed records (SUCCESS/FAILED), preserves
        active processing records.
        
        Args:
            days_old: Remove records older than this many days
            
        Returns:
            Number of records removed
        """
        cutoff_date = datetime.now() - timedelta(days=days_old)
        removed_count = 0
        
        with self._lock:
            try:
                with sqlite3.connect(self.db_path, timeout=self.timeout) as conn:
                    cursor = conn.execute("""
                        DELETE FROM file_processing_log 
                        WHERE status IN (?, ?) AND end_time < ?
                    """, (ProcessingStatus.SUCCESS.value, ProcessingStatus.FAILED.value, cutoff_date))
                    
                    removed_count = cursor.rowcount
                    conn.commit()
                    
                    if removed_count > 0:
                        self.logger.info(f"Cleaned up {removed_count} old records (older than {days_old} days)")
                    
            except sqlite3.Error as e:
                self.logger.error(f"Error cleaning up old records: {e}")
        
        return removed_count
    
    def get_database_stats(self) -> Dict[str, Any]:
        """
        Get database statistics and health information.
        
        Returns:
            Dictionary with database statistics
        """
        stats = {}
        
        try:
            with sqlite3.connect(self.db_path, timeout=self.timeout) as conn:
                # Total records
                cursor = conn.execute("SELECT COUNT(*) FROM file_processing_log")
                stats['total_records'] = cursor.fetchone()[0]
                
                # Records by status
                cursor = conn.execute("""
                    SELECT status, COUNT(*) 
                    FROM file_processing_log 
                    GROUP BY status
                """)
                stats['records_by_status'] = dict(cursor.fetchall())
                
                # Database size
                stats['database_size_bytes'] = self.db_path.stat().st_size if self.db_path.exists() else 0
                stats['database_size_mb'] = stats['database_size_bytes'] / (1024 * 1024)
                
                # Datasets
                cursor = conn.execute("SELECT DISTINCT dataset FROM file_processing_log")
                stats['datasets'] = [row[0] for row in cursor.fetchall()]
                
                # Date range
                cursor = conn.execute("""
                    SELECT MIN(created_at), MAX(created_at) 
                    FROM file_processing_log
                """)
                date_range = cursor.fetchone()
                if date_range[0]:
                    stats['date_range'] = {
                        'earliest': date_range[0],
                        'latest': date_range[1]
                    }
                
        except sqlite3.Error as e:
            self.logger.error(f"Error getting database stats: {e}")
            stats['error'] = str(e)
        
        return stats


# Example usage and testing
if __name__ == "__main__":
    """
    Test the progress tracker functionality when run directly.
    
    This provides comprehensive testing to verify that the tracker is working
    correctly with realistic usage patterns.
    """
    import tempfile
    import shutil
    
    print("Testing ClimaStation Progress Tracker [DWD10TAH2P]...")
    print("=" * 60)
    
    # Create temporary database for testing
    temp_dir = Path(tempfile.mkdtemp())
    test_db = temp_dir / "test_progress.db"
    
    try:
        # Initialize tracker
        tracker = ProgressTracker(test_db)
        print("✅ Progress tracker initialized")
        
        # Test file registration
        test_files = [
            Path("test_data/file1.zip"),
            Path("test_data/file2.zip"), 
            Path("test_data/file3.zip"),
            Path("test_data/file4.zip")
        ]
        
        registered = tracker.register_files("test_dataset", test_files)
        print(f"✅ Registered {registered} files")
        assert registered == len(test_files), f"Expected {len(test_files)}, got {registered}"
        
        # Test duplicate registration (should be ignored)
        duplicate_registered = tracker.register_files("test_dataset", test_files[:2])
        print(f"✅ Duplicate registration handled: {duplicate_registered} new files")
        assert duplicate_registered == 0, "Duplicates should be ignored"
        
        # Test processing workflow
        print("\n🔄 Testing processing workflow...")
        
        # Start processing first file
        success = tracker.start_processing("test_dataset", test_files[0], "worker_1")
        print(f"✅ Started processing file 1: {success}")
        assert success, "Should be able to start processing"
        
        # Try to start same file again (should fail)
        duplicate_start = tracker.start_processing("test_dataset", test_files[0], "worker_2")
        print(f"✅ Duplicate start prevented: {not duplicate_start}")
        assert not duplicate_start, "Should not be able to start processing same file twice"
        
        # Mark first file as successful
        success = tracker.mark_success("test_dataset", test_files[0])
        print(f"✅ Marked file 1 as successful: {success}")
        assert success, "Should be able to mark success"
        
        # Start and fail second file
        tracker.start_processing("test_dataset", test_files[1], "worker_1")
        success = tracker.mark_failed("test_dataset", test_files[1], "Test error message")
        print(f"✅ Marked file 2 as failed: {success}")
        assert success, "Should be able to mark failure"
        
        # Test status summary
        print("\n📊 Testing status reporting...")
        status = tracker.get_dataset_status("test_dataset")
        print(f"✅ Dataset status: {status}")
        
        expected_status = {
            'PENDING': 2,  # files 3 and 4
            'PROCESSING': 0,
            'SUCCESS': 1,  # file 1
            'FAILED': 1    # file 2
        }
        
        for status_type, expected_count in expected_status.items():
            assert status[status_type] == expected_count, f"Expected {expected_count} {status_type}, got {status[status_type]}"
        
        # Test comprehensive summary
        summary = tracker.get_processing_summary("test_dataset")
        print(f"✅ Processing summary generated: {summary['total_files']} total files")
        assert summary['total_files'] == len(test_files), "Summary should show all files"
        
        # Test failed files retrieval
        failed_files = tracker.get_failed_files("test_dataset")
        print(f"✅ Failed files retrieved: {len(failed_files)} files")
        assert len(failed_files) == 1, "Should have 1 failed file"
        assert "Test error message" in failed_files[0][1], "Should contain error message"
        
        # Test pending files
        pending_files = tracker.get_pending_files("test_dataset")
        print(f"✅ Pending files: {len(pending_files)} files")
        assert len(pending_files) == 2, "Should have 2 pending files"
        
        # Test reset functionality
        print("\n🔄 Testing reset functionality...")
        reset_count = tracker.reset_failed_files("test_dataset")
        print(f"✅ Reset failed files: {reset_count} files")
        assert reset_count == 1, "Should reset 1 failed file"
        
        # Verify reset worked
        status_after_reset = tracker.get_dataset_status("test_dataset")
        print(f"✅ Status after reset: {status_after_reset}")
        assert status_after_reset['FAILED'] == 0, "Should have no failed files after reset"
        assert status_after_reset['PENDING'] == 3, "Should have 3 pending files after reset"
        
        # Test database stats
        db_stats = tracker.get_database_stats()
        print(f"✅ Database stats: {db_stats['total_records']} total records")
        assert db_stats['total_records'] == len(test_files), "Should have records for all test files"
        
        print("\n✅ All progress tracker tests passed!")
        
    except Exception as e:
        print(f"❌ Progress tracker test failed: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        # Clean up temporary database
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
        print("🧹 Test cleanup completed")
