"""
ClimaStation Dataset Orchestrator

SCRIPT IDENTIFICATION: DWD10TAH5O (Dataset Processing Orchestrator)

PURPOSE:
Orchestrates the processing of individual datasets using parallel workers.
Manages worker lifecycle, progress tracking, error handling, and resource
allocation for efficient bulk data processing operations.

RESPONSIBILITIES:
- Initialize and manage parallel processing workers
- Coordinate dataset processing workflow
- Track processing progress and handle failures
- Manage resource allocation and cleanup
- Generate comprehensive processing reports
- Handle graceful shutdown and error recovery

PROCESSING WORKFLOW:
1. Load dataset configuration and validate settings
2. Initialize progress tracking and worker management
3. Discover files to process based on processing mode
4. Distribute work across parallel workers
5. Monitor progress and handle worker failures
6. Generate processing summary and cleanup resources

PARALLEL PROCESSING:
- Configurable number of worker processes
- Work distribution via multiprocessing queues
- Progress aggregation from multiple workers
- Graceful handling of worker failures
- Resource cleanup on completion or interruption

ERROR HANDLING:
- Individual file failures don't stop processing
- Worker failures are logged and recovered
- Processing continues with remaining workers
- Comprehensive error reporting and statistics

USAGE:
    orchestrator = DatasetOrchestrator("10_minutes_air_temperature")
    result = orchestrator.process_dataset(mode="bulk")
    print(f"Processed {result.successful_files} files successfully")

INTEGRATION:
- Uses ConfigManager for configuration loading
- Integrates with ProgressTracker for state management
- Works with dataset-specific processors
- Supports BulkIngestionController coordination
"""

import multiprocessing as mp
from pathlib import Path
from typing import Dict, Any, List, Optional, Type, Tuple
from datetime import datetime
from dataclasses import dataclass
from enum import Enum
import logging
import time
import signal
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed, Future

from ..utils.config_manager import ConfigManager, ConfigurationError
from ..utils.progress_tracker import ProgressTracker, ProcessingStatus
from ..processors.base_processor import BaseProcessor, ProcessingMode, ProcessingResult


class OrchestrationStatus(Enum):
    """Status of orchestration process"""
    INITIALIZING = "initializing"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    INTERRUPTED = "interrupted"


@dataclass
class OrchestrationResult:
    """Result of dataset orchestration"""
    dataset_name: str
    status: OrchestrationStatus
    total_files: int
    successful_files: int
    failed_files: int
    total_records: int
    processing_time: float
    worker_count: int
    error_message: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None


class DatasetOrchestrator:
    """
    Orchestrates parallel processing of a single dataset.
    
    Manages worker processes, progress tracking, and resource allocation
    for efficient bulk processing of meteorological data files.
    """
    
    def __init__(self, dataset_name: str):
        """
        Initialize dataset orchestrator.
        
        Args:
            dataset_name: Name of dataset to process
        """
        self.dataset_name = dataset_name
        self.status = OrchestrationStatus.INITIALIZING
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        
        # Initialize configuration
        try:
            self.config_manager = ConfigManager()
            self.base_config = self.config_manager.get_base_config()
            self.dataset_config = self.config_manager.get_dataset_config(dataset_name)
        except ConfigurationError as e:
            raise ConfigurationError(f"Failed to load configuration for {dataset_name}: {e}")
        
        # Initialize progress tracker
        progress_db_path = Path(self.base_config['paths']['progress_db'])
        self.progress_tracker = ProgressTracker(progress_db_path)
        
        # Setup logging
        self._setup_logging()
        
        # Processing state
        self.worker_futures: List[Future] = []
        self.executor: Optional[ProcessPoolExecutor] = None
        self.interrupted = False
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self.logger.info(f"Initialized orchestrator for dataset: {dataset_name}")
    
    def _setup_logging(self):
        """Setup logging for orchestrator"""
        log_level = self.base_config.get('logging', {}).get('level', 'INFO')
        
        self.logger = logging.getLogger(f"climastation.orchestrator.{self.dataset_name}")
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self.logger.warning(f"Received signal {signum}, initiating graceful shutdown...")
        self.interrupted = True
        self.status = OrchestrationStatus.INTERRUPTED
        
        if self.executor:
            self.logger.info("Shutting down worker processes...")
            self.executor.shutdown(wait=False)
    
    def process_dataset(self, mode: ProcessingMode = ProcessingMode.BULK) -> OrchestrationResult:
        """
        Process dataset using parallel workers.
        
        Args:
            mode: Processing mode (bulk or incremental)
            
        Returns:
            OrchestrationResult with processing summary
        """
        self.start_time = datetime.now()
        self.status = OrchestrationStatus.PROCESSING
        
        self.logger.info(f"Starting {mode.value} processing for {self.dataset_name}")
        
        try:
            # Get processing configuration
            max_workers = self.base_config['processing']['max_workers']
            # Ensure max_workers is never None and is always a valid integer
            max_workers = max_workers or 4
            
            # Discover files to process
            files_to_process = self._discover_files(mode)
            
            if not files_to_process:
                self.logger.warning("No files found to process")
                return self._create_empty_result()
            
            self.logger.info(f"Found {len(files_to_process)} files to process")
            self.logger.info(f"Using {min(max_workers, len(files_to_process))} workers")
            
            # Register files for processing if in bulk mode
            if mode == ProcessingMode.BULK:
                registered = self.progress_tracker.register_files(
                    self.dataset_name, files_to_process
                )
                self.logger.info(f"Registered {registered} files for processing")
            
            # Process files with workers
            result = self._process_with_workers(files_to_process, max_workers)
            
            self.status = OrchestrationStatus.COMPLETED
            self.end_time = datetime.now()
            
            self.logger.info(f"Processing completed successfully")
            self._log_orchestration_summary(result)
            
            return result
            
        except Exception as e:
            self.status = OrchestrationStatus.FAILED
            self.end_time = datetime.now()
            
            # Calculate processing time safely
            if self.start_time is not None:
                processing_time = (datetime.now() - self.start_time).total_seconds()
            else:
                processing_time = 0.0
            
            error_msg = f"Dataset processing failed: {str(e)}"
            self.logger.error(error_msg)
            
            return OrchestrationResult(
                dataset_name=self.dataset_name,
                status=OrchestrationStatus.FAILED,
                total_files=0,
                successful_files=0,
                failed_files=0,
                total_records=0,
                processing_time=processing_time,
                worker_count=0,
                error_message=error_msg,
                start_time=self.start_time,
                end_time=self.end_time
            )
    
    def _discover_files(self, mode: ProcessingMode) -> List[Path]:
        """
        Discover files to process based on mode.
        
        Args:
            mode: Processing mode
            
        Returns:
            List of file paths to process
        """
        source_path = Path(self.dataset_config['source']['base_path'])
        file_patterns = self.dataset_config['source'].get('file_patterns', ['*.zip'])
        
        # Find all matching files
        all_files = []
        for pattern in file_patterns:
            matching_files = list(source_path.glob(pattern))
            all_files.extend(matching_files)
            self.logger.debug(f"Pattern '{pattern}' matched {len(matching_files)} files")
        
        if mode == ProcessingMode.INCREMENTAL:
            # Filter to only unprocessed or failed files
            files_to_process = []
            for file_path in all_files:
                # Check processing status
                status = self.progress_tracker.get_file_status(self.dataset_name, file_path)
                if status != ProcessingStatus.SUCCESS:
                    files_to_process.append(file_path)
            
            self.logger.info(f"Incremental mode: {len(files_to_process)} files need processing")
            return sorted(files_to_process)
        
        return sorted(all_files)
    
    def _process_with_workers(self, files: List[Path], max_workers: int) -> OrchestrationResult:
        """
        Process files using parallel workers.
        
        Args:
            files: List of files to process
            max_workers: Maximum number of worker processes
            
        Returns:
            OrchestrationResult with processing summary
        """
        # Determine optimal worker count
        worker_count = min(max_workers, len(files), mp.cpu_count())
        
        # Split files among workers
        file_chunks = self._split_files_for_workers(files, worker_count)
        
        successful_files = 0
        failed_files = 0
        total_records = 0
        
        try:
            with ProcessPoolExecutor(max_workers=worker_count) as executor:
                self.executor = executor
                
                # Submit worker tasks
                future_to_chunk = {}
                for i, chunk in enumerate(file_chunks):
                    if chunk:  # Only submit non-empty chunks
                        future = executor.submit(
                            self._worker_process_files,
                            chunk,
                            f"worker_{i}",
                            self.dataset_name
                        )
                        future_to_chunk[future] = chunk
                        self.worker_futures.append(future)
                
                self.logger.info(f"Submitted {len(future_to_chunk)} worker tasks")
                
                # Collect results
                for future in as_completed(future_to_chunk):
                    if self.interrupted:
                        self.logger.warning("Processing interrupted, cancelling remaining tasks")
                        break
                    
                    chunk = future_to_chunk[future]
                    try:
                        worker_result = future.result()
                        successful_files += worker_result['successful_files']
                        failed_files += worker_result['failed_files']
                        total_records += worker_result['total_records']
                        
                        self.logger.info(
                            f"Worker completed: {worker_result['successful_files']} success, "
                            f"{worker_result['failed_files']} failed"
                        )
                        
                    except Exception as e:
                        self.logger.error(f"Worker failed with error: {e}")
                        failed_files += len(chunk)
                
        except Exception as e:
            self.logger.error(f"Error in worker management: {e}")
            raise
        
        finally:
            self.executor = None
        
        # Calculate processing time safely
        if self.start_time is not None:
            processing_time = (datetime.now() - self.start_time).total_seconds()
        else:
            processing_time = 0.0
        
        return OrchestrationResult(
            dataset_name=self.dataset_name,
            status=OrchestrationStatus.COMPLETED if not self.interrupted else OrchestrationStatus.INTERRUPTED,
            total_files=len(files),
            successful_files=successful_files,
            failed_files=failed_files,
            total_records=total_records,
            processing_time=processing_time,
            worker_count=worker_count,
            start_time=self.start_time,
            end_time=datetime.now()
        )
    
    def _split_files_for_workers(self, files: List[Path], worker_count: int) -> List[List[Path]]:
        """
        Split files into chunks for parallel processing.
        
        Args:
            files: List of files to split
            worker_count: Number of workers
            
        Returns:
            List of file chunks for each worker
        """
        if not files:
            return []
        
        chunk_size = max(1, len(files) // worker_count)
        chunks = []
        
        for i in range(0, len(files), chunk_size):
            chunk = files[i:i + chunk_size]
            chunks.append(chunk)
        
        # Ensure we don't have more chunks than workers
        while len(chunks) > worker_count:
            # Merge the last two chunks
            last_chunk = chunks.pop()
            chunks[-1].extend(last_chunk)
        
        return chunks
    
    @staticmethod
    def _worker_process_files(files: List[Path], worker_id: str, dataset_name: str) -> Dict[str, Any]:
        """
        Worker function to process a chunk of files.
        
        Args:
            files: List of files to process
            worker_id: Identifier for this worker
            dataset_name: Name of dataset being processed
            
        Returns:
            Dictionary with processing results
        """
        # Import here to avoid issues with multiprocessing
        from ..processors.ten_minutes_air_temperature_processor import TenMinutesAirTemperatureProcessor
        
        # Create processor instance for this worker
        processor = TenMinutesAirTemperatureProcessor(dataset_name, worker_id)
        
        successful_files = 0
        failed_files = 0
        total_records = 0
        
        for file_path in files:
            try:
                # Start processing
                if not processor.progress_tracker.start_processing(
                    dataset_name, file_path, worker_id
                ):
                    processor.logger.warning(f"Could not start processing for {file_path}")
                    failed_files += 1
                    continue
                
                # Validate and process file
                if not processor.validate_file_structure(file_path):
                    raise ValueError("Invalid file structure")
                
                result = processor.process_file(file_path)
                
                if result.success:
                    processor.progress_tracker.mark_success(dataset_name, file_path)
                    successful_files += 1
                    total_records += result.records_processed
                    
                    processor.logger.info(
                        f"✅ {worker_id} processed {file_path.name}: "
                        f"{result.records_processed} records"
                    )
                else:
                    error_msg = result.error_message or "Unknown error"
                    processor.progress_tracker.mark_failed(dataset_name, file_path, error_msg)
                    failed_files += 1
                    
                    processor.logger.error(f"❌ {worker_id} failed {file_path.name}: {error_msg}")
                
            except Exception as e:
                error_msg = f"Exception during processing: {str(e)}"
                processor.progress_tracker.mark_failed(dataset_name, file_path, error_msg)
                failed_files += 1
                
                processor.logger.error(f"❌ {worker_id} exception {file_path.name}: {e}")
        
        return {
            'worker_id': worker_id,
            'successful_files': successful_files,
            'failed_files': failed_files,
            'total_records': total_records,
            'processed_files': len(files)
        }
    
    def _create_empty_result(self) -> OrchestrationResult:
        """Create result for when no files are found"""
        # Calculate processing time safely
        if self.start_time is not None:
            processing_time = (datetime.now() - self.start_time).total_seconds()
        else:
            processing_time = 0.0
            
        return OrchestrationResult(
            dataset_name=self.dataset_name,
            status=OrchestrationStatus.COMPLETED,
            total_files=0,
            successful_files=0,
            failed_files=0,
            total_records=0,
            processing_time=processing_time,
            worker_count=0,
            start_time=self.start_time,
            end_time=datetime.now()
        )
    
    def _log_orchestration_summary(self, result: OrchestrationResult):
        """Log orchestration summary"""
        self.logger.info("=" * 60)
        self.logger.info("ORCHESTRATION SUMMARY")
        self.logger.info("=" * 60)
        self.logger.info(f"Dataset: {result.dataset_name}")
        self.logger.info(f"Status: {result.status.value}")
        self.logger.info(f"Total files: {result.total_files}")
        self.logger.info(f"Successful: {result.successful_files}")
        self.logger.info(f"Failed: {result.failed_files}")
        if result.total_files > 0:
            success_rate = (result.successful_files / result.total_files) * 100
            self.logger.info(f"Success rate: {success_rate:.1f}%")
        self.logger.info(f"Total records: {result.total_records:,}")
        self.logger.info(f"Processing time: {result.processing_time:.2f}s")
        self.logger.info(f"Workers used: {result.worker_count}")
        self.logger.info("=" * 60)
    
    def get_processing_status(self) -> Dict[str, Any]:
        """
        Get current processing status.
        
        Returns:
            Dictionary with current status information
        """
        status_info = {
            'dataset_name': self.dataset_name,
            'status': self.status.value,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
        }
        
        if self.start_time:
            if self.end_time:
                elapsed_time = (self.end_time - self.start_time).total_seconds()
            else:
                elapsed_time = (datetime.now() - self.start_time).total_seconds()
            status_info['elapsed_time'] = elapsed_time
        
        # Add progress information from tracker
        try:
            progress_summary = self.progress_tracker.get_processing_summary(self.dataset_name)
            status_info['progress'] = progress_summary
        except Exception as e:
            self.logger.warning(f"Could not get progress summary: {e}")
        
        return status_info
    
    def _generate_processing_summary(self) -> Dict[str, Any]:
        """Generate comprehensive processing summary"""
        summary = {
            'dataset_name': self.dataset_name,
            'orchestrator_status': self.status.value,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
        }
        
        # Calculate orchestration time safely
        if self.start_time is not None:
            if self.end_time:
                orchestration_time = (self.end_time - self.start_time).total_seconds()
            else:
                orchestration_time = (datetime.now() - self.start_time).total_seconds()
            summary['orchestration_time'] = orchestration_time
        
        # Add progress tracker summary
        try:
            progress_summary = self.progress_tracker.get_processing_summary(self.dataset_name)
            summary['progress_summary'] = progress_summary
        except Exception as e:
            self.logger.warning(f"Could not generate progress summary: {e}")
            summary['progress_summary'] = {'error': str(e)}
        
        return summary


# Example usage and testing
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 2:
        print("Usage: python dataset_orchestrator.py <dataset_name>")
        sys.exit(1)
    
    dataset_name = sys.argv[1]
    
    try:
        orchestrator = DatasetOrchestrator(dataset_name)
        result = orchestrator.process_dataset(ProcessingMode.BULK)
        
        print(f"\nProcessing completed:")
        print(f"Status: {result.status.value}")
        print(f"Files processed: {result.successful_files}/{result.total_files}")
        print(f"Records processed: {result.total_records:,}")
        print(f"Processing time: {result.processing_time:.2f}s")
        
        if result.status == OrchestrationStatus.FAILED:
            print(f"Error: {result.error_message}")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Orchestration failed: {e}")
        sys.exit(1)
