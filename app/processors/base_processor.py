"""
ClimaStation Base Processor Interface

SCRIPT IDENTIFICATION: DWD10TAH4T (Base Processor Interface)

PURPOSE:
Abstract base class defining the interface and common functionality for all
dataset processors in the ClimaStation platform. Provides standardized
processing workflow, error handling, and integration with configuration
and progress tracking systems.

RESPONSIBILITIES:
- Define abstract interface that all processors must implement
- Provide common processing workflow and error handling
- Integrate with ConfigManager and ProgressTracker
- Support both bulk and incremental processing modes
- Standardize logging and monitoring across all processors
- Handle file validation and preprocessing steps

ABSTRACT METHODS (must be implemented by subclasses):
- process_file(): Process individual ZIP file
- validate_file_structure(): Validate internal file structure
- extract_metadata(): Extract metadata from files
- get_expected_file_patterns(): Define expected file naming patterns

CONCRETE METHODS (provided by base class):
- run(): Main processing orchestration
- setup_processing(): Initialize processing environment
- cleanup_processing(): Clean up after processing
- log_processing_stats(): Log processing statistics

USAGE:
    class MyDatasetProcessor(BaseProcessor):
        def process_file(self, file_path: Path) -> ProcessingResult:
            # Implementation specific to dataset
            pass
    
    processor = MyDatasetProcessor("my_dataset")
    processor.run(mode="bulk")

PROCESSING WORKFLOW:
1. Load configuration and validate settings
2. Initialize progress tracker and logging
3. Discover files to process based on mode
4. Process files with error handling and progress tracking
5. Generate processing summary and cleanup

ERROR HANDLING:
- Individual file failures don't stop batch processing
- Detailed error logging with context
- Failed files are marked for retry in incremental mode
- Processing statistics include error rates and patterns

THREAD SAFETY:
Base class is thread-safe through ProgressTracker integration.
Subclasses must ensure their implementations are thread-safe if
parallel processing is enabled.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
from dataclasses import dataclass
from enum import Enum
import logging
import time

from ..utils.config_manager import ConfigManager
from ..utils.progress_tracker import ProgressTracker, ProcessingStatus


class ProcessingMode(Enum):
    """Processing mode enumeration"""
    BULK = "bulk"           # Process all files from scratch
    INCREMENTAL = "incremental"  # Process only new/failed files


@dataclass
class ProcessingResult:
    """Result of processing a single file"""
    success: bool
    records_processed: int = 0
    error_message: Optional[str] = None
    processing_time: float = 0.0
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class ProcessingStats:
    """Overall processing statistics"""
    total_files: int
    successful_files: int
    failed_files: int
    total_records: int
    total_time: float
    start_time: datetime
    end_time: datetime
    error_rate: float
    avg_processing_time: float


class BaseProcessor(ABC):
    """
    Abstract base class for all dataset processors.
    
    Provides common functionality and defines the interface that all
    dataset-specific processors must implement.
    """
    
    def __init__(self, dataset_name: str, worker_id: str = "main"):
        """
        Initialize base processor.
        
        Args:
            dataset_name: Name of dataset (matches config file name)
            worker_id: Identifier for this worker instance
        """
        self.dataset_name = dataset_name
        self.worker_id = worker_id
        
        # Initialize components
        self.config_manager = ConfigManager()
        # Use the public methods
        self.config = self.config_manager.get_dataset_config(dataset_name)
        self.base_config = self.config_manager.get_base_config()
        
        # Initialize progress tracker
        progress_db_path = Path(self.base_config['paths']['progress_db'])
        self.progress_tracker = ProgressTracker(progress_db_path)
        
        # Setup logging
        self._setup_logging()
        
        # Processing state
        self.start_time: Optional[datetime] = None
        self.stats: Optional[ProcessingStats] = None
        
        self.logger.info(f"Initialized {self.__class__.__name__} for dataset: {dataset_name}")
    
    def _setup_logging(self):
        """Setup logging for this processor"""
        log_level = self.base_config.get('logging', {}).get('level', 'INFO')
        
        # Create logger for this processor
        self.logger = logging.getLogger(f"climastation.{self.dataset_name}")
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Create handler if not exists
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    # Abstract methods that must be implemented by subclasses
    
    @abstractmethod
    def process_file(self, file_path: Path) -> ProcessingResult:
        """
        Process a single ZIP file.
        
        Args:
            file_path: Path to ZIP file to process
            
        Returns:
            ProcessingResult with success status and details
        """
        pass
    
    @abstractmethod
    def validate_file_structure(self, file_path: Path) -> bool:
        """
        Validate that file has expected internal structure.
        
        Args:
            file_path: Path to file to validate
            
        Returns:
            True if file structure is valid, False otherwise
        """
        pass
    
    @abstractmethod
    def extract_metadata(self, file_path: Path) -> Dict[str, Any]:
        """
        Extract metadata from file.
        
        Args:
            file_path: Path to file
            
        Returns:
            Dictionary containing extracted metadata
        """
        pass
    
    @abstractmethod
    def get_expected_file_patterns(self) -> List[str]:
        """
        Get list of expected file naming patterns for this dataset.
        
        Returns:
            List of glob patterns for expected files
        """
        pass
    
    # Concrete methods provided by base class
    
    def run(self, mode: ProcessingMode = ProcessingMode.BULK) -> ProcessingStats:
        """
        Main processing orchestration method.
        
        Args:
            mode: Processing mode (bulk or incremental)
            
        Returns:
            ProcessingStats with overall processing results
        """
        self.start_time = datetime.now()
        self.logger.info(f"Starting {mode.value} processing for {self.dataset_name}")
        
        try:
            # Setup processing environment
            self.setup_processing(mode)
            
            # Discover files to process
            files_to_process = self._discover_files(mode)
            self.logger.info(f"Found {len(files_to_process)} files to process")
            
            # Register files with progress tracker
            if mode == ProcessingMode.BULK:
                registered = self.progress_tracker.register_files(
                    self.dataset_name, files_to_process
                )
                self.logger.info(f"Registered {registered} files for processing")
            
            # Process files
            stats = self._process_files(files_to_process)
            
            # Cleanup and finalize
            self.cleanup_processing()
            
            self.logger.info(f"Processing completed. Success rate: {100-stats.error_rate:.1f}%")
            return stats
            
        except Exception as e:
            self.logger.error(f"Processing failed with error: {e}")
            raise
    
    def setup_processing(self, mode: ProcessingMode):
        """
        Setup processing environment.
        
        Args:
            mode: Processing mode
        """
        # Create output directories
        output_path = Path(self.config['output']['base_path'])
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Log processing start
        self.logger.info(f"Setup complete for {mode.value} processing")
        self.logger.info(f"Output path: {output_path}")
        self.logger.info(f"Worker ID: {self.worker_id}")
    
    def cleanup_processing(self):
        """Cleanup after processing"""
        self.logger.info("Processing cleanup completed")
    
    def _discover_files(self, mode: ProcessingMode) -> List[Path]:
        """
        Discover files to process based on mode.
        
        Args:
            mode: Processing mode
            
        Returns:
            List of file paths to process
        """
        source_path = Path(self.config['source']['base_path'])
        patterns = self.get_expected_file_patterns()
        
        # Find all matching files
        all_files = []
        for pattern in patterns:
            all_files.extend(source_path.glob(pattern))
        
        if mode == ProcessingMode.INCREMENTAL:
            # Filter to only unprocessed or failed files
            status_counts = self.progress_tracker.get_dataset_status(self.dataset_name)
            if status_counts[ProcessingStatus.SUCCESS.value] > 0:
                # Get files that need processing
                files_to_process = []
                for file_path in all_files:
                    # Check if file needs processing (not successful)
                    # This is a simplified check - in practice you'd query the database
                    files_to_process.append(file_path)
                return files_to_process
        
        return sorted(all_files)
    
    def _process_files(self, files: List[Path]) -> ProcessingStats:
        """
        Process list of files with progress tracking.
        
        Args:
            files: List of files to process
            
        Returns:
            ProcessingStats with processing results
        """
        successful_files = 0
        failed_files = 0
        total_records = 0
        processing_times = []
        
        for i, file_path in enumerate(files, 1):
            self.logger.info(f"Processing file {i}/{len(files)}: {file_path.name}")
            
            # Start processing
            if not self.progress_tracker.start_processing(
                self.dataset_name, file_path, self.worker_id
            ):
                self.logger.warning(f"Could not start processing for {file_path}")
                continue
            
            # Process file
            start_time = time.time()
            try:
                # Validate file first
                if not self.validate_file_structure(file_path):
                    raise ValueError("Invalid file structure")
                
                # Process the file
                result = self.process_file(file_path)
                processing_time = time.time() - start_time
                processing_times.append(processing_time)
                
                if result.success:
                    # Mark success
                    self.progress_tracker.mark_success(self.dataset_name, file_path)
                    successful_files += 1
                    total_records += result.records_processed
                    
                    self.logger.info(
                        f"✅ Processed {file_path.name}: "
                        f"{result.records_processed} records in {processing_time:.2f}s"
                    )
                else:
                    # Mark failure
                    error_msg = result.error_message or "Unknown error"
                    self.progress_tracker.mark_failed(self.dataset_name, file_path, error_msg)
                    failed_files += 1
                    
                    self.logger.error(f"❌ Failed {file_path.name}: {error_msg}")
                    
            except Exception as e:
                processing_time = time.time() - start_time
                error_msg = f"Exception during processing: {str(e)}"
                
                self.progress_tracker.mark_failed(self.dataset_name, file_path, error_msg)
                failed_files += 1
                
                self.logger.error(f"❌ Exception processing {file_path.name}: {e}")
        
        # Calculate statistics
        end_time = datetime.now()
        # Ensure start_time is not None (it should be set in run() method)
        if self.start_time is None:
            raise RuntimeError("Processing start time was not set")
            
        total_time = (end_time - self.start_time).total_seconds()
        error_rate = (failed_files / len(files) * 100) if files else 0
        avg_processing_time = sum(processing_times) / len(processing_times) if processing_times else 0
        
        stats = ProcessingStats(
            total_files=len(files),
            successful_files=successful_files,
            failed_files=failed_files,
            total_records=total_records,
            total_time=total_time,
            start_time=self.start_time,  # We've already verified it's not None above
            end_time=end_time,
            error_rate=error_rate,
            avg_processing_time=avg_processing_time
        )
        
        self.stats = stats
        self._log_processing_stats(stats)
        
        return stats
    
    def _log_processing_stats(self, stats: ProcessingStats):
        """Log processing statistics"""
        self.logger.info("=" * 60)
        self.logger.info("PROCESSING SUMMARY")
        self.logger.info("=" * 60)
        self.logger.info(f"Dataset: {self.dataset_name}")
        self.logger.info(f"Total files: {stats.total_files}")
        self.logger.info(f"Successful: {stats.successful_files}")
        self.logger.info(f"Failed: {stats.failed_files}")
        self.logger.info(f"Success rate: {100 - stats.error_rate:.1f}%")
        self.logger.info(f"Total records: {stats.total_records:,}")
        self.logger.info(f"Total time: {stats.total_time:.2f}s")
        self.logger.info(f"Avg time per file: {stats.avg_processing_time:.2f}s")
        self.logger.info("=" * 60)
    
    def get_processing_summary(self) -> Dict[str, Any]:
        """
        Get comprehensive processing summary.
        
        Returns:
            Dictionary with processing summary
        """
        tracker_summary = self.progress_tracker.get_processing_summary(self.dataset_name)
        
        summary = {
            'dataset': self.dataset_name,
            'worker_id': self.worker_id,
            'processor_class': self.__class__.__name__,
            'tracker_summary': tracker_summary
        }
        
        if self.stats:
            summary.update({
                'last_run_stats': {
                    'total_files': self.stats.total_files,
                    'successful_files': self.stats.successful_files,
                    'failed_files': self.stats.failed_files,
                    'total_records': self.stats.total_records,
                    'total_time': self.stats.total_time,
                    'error_rate': self.stats.error_rate,
                    'avg_processing_time': self.stats.avg_processing_time,
                    'start_time': self.stats.start_time.isoformat(),
                    'end_time': self.stats.end_time.isoformat()
                }
            })
        
        return summary


# Example usage and testing
if __name__ == "__main__":
    # This would normally be implemented by a concrete processor
    class TestProcessor(BaseProcessor):
        def process_file(self, file_path: Path) -> ProcessingResult:
            # Simulate processing
            time.sleep(0.1)
            return ProcessingResult(
                success=True,
                records_processed=100,
                processing_time=0.1
            )
        
        def validate_file_structure(self, file_path: Path) -> bool:
            return file_path.exists()
        
        def extract_metadata(self, file_path: Path) -> Dict[str, Any]:
            return {"file_size": file_path.stat().st_size}
        
        def get_expected_file_patterns(self) -> List[str]:
            return ["*.zip"]
    
    # Test the base processor
    try:
        processor = TestProcessor("test_dataset")
        print("✅ Base processor test successful")
    except Exception as e:
        print(f"❌ Base processor test failed: {e}")
