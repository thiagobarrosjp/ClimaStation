"""
ClimaStation Progress Tracker

SCRIPT IDENTIFICATION: DWD10TAH3T (Progress Tracker)

PURPOSE:
File-level progress tracking using SQLite database for the ClimaStation platform.
Provides comprehensive tracking of processing status for individual ZIP files
across all datasets with support for parallel processing and failure recovery.

RESPONSIBILITIES:
- Create and manage SQLite database for progress tracking
- Register files for processing with PENDING status
- Track processing state transitions (PENDING → PROCESSING → SUCCESS/FAILED)
- Record processing times, worker assignments, and error messages
- Provide status summaries and progress reports for datasets
- Support recovery by identifying failed or incomplete files

USAGE:
    tracker = ProgressTracker(Path("data/dwd/0_debug/progress.db"))
    tracker.register_files("10_minutes_air_temperature", file_list)
    tracker.start_processing("10_minutes_air_temperature", file_path, "worker_1")
    tracker.mark_success("10_minutes_air_temperature", file_path)

DATABASE SCHEMA:
    file_processing_log:
    - id: Primary key
    - dataset: Dataset identifier (e.g., "10_minutes_air_temperature")
    - file_path: Full path to ZIP file being processed
    - status: PENDING/PROCESSING/SUCCESS/FAILED
    - start_time: When processing began
    - end_time: When processing completed (success or failure)
    - error_message: Error details for failed files
    - worker_id: Identifier of worker that processed the file

THREAD SAFETY:
All database operations use SQLite's built-in thread safety with proper
connection handling. Safe for use with parallel workers.

ERROR HANDLING:
- Database creation errors are propagated to caller
- Individual operation errors are logged but don't crash the tracker
- Provides detailed error context for troubleshooting

DEPENDENCIES:
- sqlite3 for database operations
- pathlib for file path handling
- datetime for timestamp management
- enum for status type safety
"""

import sqlite3
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Any
from enum import Enum
import threading

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
    with support for parallel workers and comprehensive error reporting.
    """
    
    def __init__(self, db_path: Path):
        """
        Initialize progress tracker with SQLite database.
        
        Args:
            db_path: Path to SQLite database file (will be created if not exists)
        """
        self.db_path = db_path
        self._lock = threading.Lock()  # Thread safety for database operations
        self._init_database()
    
    def _init_database(self):
        """
        Create progress tracking table if it doesn't exist.
        
        Creates the file_processing_log table with proper indexes for
        efficient querying by dataset and status.
        
        Raises:
            sqlite3.Error: If database creation fails
        """
        # Ensure directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            # Create main tracking table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS file_processing_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dataset TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    status TEXT NOT NULL,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    error_message TEXT,
                    worker_id TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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
            
            conn.commit()
    
    def register_files(self, dataset: str, file_paths: List[Path]) -> int:
        """
        Register files for processing with PENDING status.
        
        Args:
            dataset: Dataset identifier (e.g., "10_minutes_air_temperature")
            file_paths: List of file paths to register
            
        Returns:
            Number of files successfully registered (may be less than input if duplicates)
        """
        registered_count = 0
        
        with self._lock:
            with sqlite3.connect(self.db_path) as conn:
                for file_path in file_paths:
                    try:
                        conn.execute("""
                            INSERT OR IGNORE INTO file_processing_log 
                            (dataset, file_path, status) 
                            VALUES (?, ?, ?)
                        """, (dataset, str(file_path), ProcessingStatus.PENDING.value))
                        
                        if conn.total_changes > 0:
                            registered_count += 1
                            
                    except sqlite3.Error as e:
                        print(f"Warning: Failed to register file {file_path}: {e}")
                        continue
                
                conn.commit()
        
        return registered_count
    
    def start_processing(self, dataset: str, file_path: Path, worker_id: str) -> bool:
        """
        Mark file as being processed by a specific worker.
        
        Args:
            dataset: Dataset identifier
            file_path: Path to file being processed
            worker_id: Identifier of worker processing the file
            
        Returns:
            True if status was successfully updated, False otherwise
        """
        with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute("""
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
                    return conn.total_changes > 0
                    
            except sqlite3.Error as e:
                print(f"Error starting processing for {file_path}: {e}")
                return False
    
    def mark_success(self, dataset: str, file_path: Path) -> bool:
        """
        Mark file as successfully processed.
        
        Args:
            dataset: Dataset identifier
            file_path: Path to successfully processed file
            
        Returns:
            True if status was successfully updated, False otherwise
        """
        with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute("""
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
                    return conn.total_changes > 0
                    
            except sqlite3.Error as e:
                print(f"Error marking success for {file_path}: {e}")
                return False
    
    def mark_failed(self, dataset: str, file_path: Path, error_message: str) -> bool:
        """
        Mark file as failed with error details.
        
        Args:
            dataset: Dataset identifier
            file_path: Path to failed file
            error_message: Description of the error that occurred
            
        Returns:
            True if status was successfully updated, False otherwise
        """
        with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute("""
                        UPDATE file_processing_log 
                        SET status = ?, end_time = ?, error_message = ?
                        WHERE dataset = ? AND file_path = ?
                    """, (
                        ProcessingStatus.FAILED.value, 
                        datetime.now(), 
                        error_message,
                        dataset, 
                        str(file_path)
                    ))
                    
                    conn.commit()
                    return conn.total_changes > 0
                    
            except sqlite3.Error as e:
                print(f"Error marking failure for {file_path}: {e}")
                return False
    
    def get_dataset_status(self, dataset: str) -> Dict[str, int]:
        """
        Get processing status summary for a dataset.
        
        Args:
            dataset: Dataset identifier
            
        Returns:
            Dictionary with status counts (PENDING, PROCESSING, SUCCESS, FAILED)
        """
        status_counts = {status.value: 0 for status in ProcessingStatus}
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    SELECT status, COUNT(*) 
                    FROM file_processing_log 
                    WHERE dataset = ? 
                    GROUP BY status
                """, (dataset,))
                
                for status, count in cursor.fetchall():
                    status_counts[status] = count
                    
        except sqlite3.Error as e:
            print(f"Error getting dataset status for {dataset}: {e}")
        
        return status_counts
    
    def get_failed_files(self, dataset: str) -> List[Tuple[str, str]]:
        """
        Get list of failed files with error messages.
        
        Args:
            dataset: Dataset identifier
            
        Returns:
            List of tuples (file_path, error_message) for failed files
        """
        failed_files = []
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    SELECT file_path, error_message 
                    FROM file_processing_log 
                    WHERE dataset = ? AND status = ?
                    ORDER BY end_time DESC
                """, (dataset, ProcessingStatus.FAILED.value))
                
                failed_files = cursor.fetchall()
                
        except sqlite3.Error as e:
            print(f"Error getting failed files for {dataset}: {e}")
        
        return failed_files
    
    # CORRECT - 'Any' is the proper type hint
def get_processing_summary(self, dataset: str) -> Dict[str, Any]:
        """
        Get comprehensive processing summary for a dataset.
        
        Args:
            dataset: Dataset identifier
            
        Returns:
            Dictionary with detailed processing statistics
        """
        summary = {
            'total_files': 0,
            'status_counts': self.get_dataset_status(dataset),
            'processing_rate': 0.0,
            'average_processing_time': 0.0,
            'failed_files': []
        }
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Get total files
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM file_processing_log WHERE dataset = ?
                """, (dataset,))
                summary['total_files'] = cursor.fetchone()[0]
                
                # Get average processing time for successful files
                cursor = conn.execute("""
                    SELECT AVG(julianday(end_time) - julianday(start_time)) * 24 * 60
                    FROM file_processing_log 
                    WHERE dataset = ? AND status = ? AND start_time IS NOT NULL
                """, (dataset, ProcessingStatus.SUCCESS.value))
                
                avg_time = cursor.fetchone()[0]
                summary['average_processing_time'] = avg_time if avg_time else 0.0
                
                # Calculate processing rate
                completed = summary['status_counts'][ProcessingStatus.SUCCESS.value]
                if summary['total_files'] > 0:
                    summary['processing_rate'] = (completed / summary['total_files']) * 100
                
                # Get failed files
                summary['failed_files'] = self.get_failed_files(dataset)
                
        except sqlite3.Error as e:
            print(f"Error getting processing summary for {dataset}: {e}")
        
        return summary
    
    def reset_dataset(self, dataset: str) -> bool:
        """
        Reset all files in dataset to PENDING status (for reprocessing).
        
        Args:
            dataset: Dataset identifier
            
        Returns:
            True if reset was successful, False otherwise
        """
        with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute("""
                        UPDATE file_processing_log 
                        SET status = ?, start_time = NULL, end_time = NULL, 
                            error_message = NULL, worker_id = NULL
                        WHERE dataset = ?
                    """, (ProcessingStatus.PENDING.value, dataset))
                    
                    conn.commit()
                    return True
                    
            except sqlite3.Error as e:
                print(f"Error resetting dataset {dataset}: {e}")
                return False


# Example usage and testing
if __name__ == "__main__":
    # Test progress tracker functionality
    test_db = Path("test_progress.db")
    
    try:
        # Initialize tracker
        tracker = ProgressTracker(test_db)
        
        # Test file registration
        test_files = [Path("test1.zip"), Path("test2.zip"), Path("test3.zip")]
        registered = tracker.register_files("test_dataset", test_files)
        print(f"Registered {registered} files")
        
        # Test processing workflow
        tracker.start_processing("test_dataset", test_files[0], "worker_1")
        tracker.mark_success("test_dataset", test_files[0])
        
        tracker.start_processing("test_dataset", test_files[1], "worker_2")
        tracker.mark_failed("test_dataset", test_files[1], "Test error message")
        
        # Get status summary
        status = tracker.get_dataset_status("test_dataset")
        print(f"Dataset status: {status}")
        
        # Get processing summary
        summary = tracker.get_processing_summary("test_dataset")
        print(f"Processing summary: {summary}")
        
        print("✅ Progress tracker test successful")
        
    except Exception as e:
        print(f"❌ Progress tracker test failed: {e}")
    
    finally:
        # Clean up test database
        if test_db.exists():
            test_db.unlink()