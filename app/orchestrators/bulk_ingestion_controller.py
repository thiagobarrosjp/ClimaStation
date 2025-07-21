"""
ClimaStation Bulk Ingest Controller

SCRIPT IDENTIFICATION: DWD10TAH1T (Bulk Ingest Controller)

PURPOSE:
Main orchestrator for bulk historical data ingestion across all datasets.
Coordinates dataset processors, manages resource allocation, and provides
comprehensive progress tracking and error handling for large-scale processing.

RESPONSIBILITIES:
- Orchestrate processing across multiple datasets sequentially
- Manage resource allocation and worker coordination
- Provide comprehensive progress tracking and reporting
- Handle failures with detailed error reporting and recovery options
- Coordinate with configuration management and logging systems
- Generate processing summaries and performance metrics

ARCHITECTURE:
Sequential Datasets + Parallel Workers approach:
- Process one dataset at a time (fail-fast behavior)
- Each dataset uses parallel workers for file processing
- All workers must succeed for dataset to be marked complete
- Comprehensive progress tracking at file level

USAGE:
    from orchestrators.bulk_ingest_controller import BulkIngestController
    
    controller = BulkIngestController()
    results = controller.run_bulk_ingestion(datasets=["10_minutes_air_temperature"])

PROCESSING WORKFLOW:
1. Load configuration and validate settings
2. Initialize progress tracking and logging
3. For each dataset:
   a. Create dataset processor
   b. Run processing with parallel workers
   c. Validate results and handle failures
   d. Generate dataset summary
4. Generate overall processing summary
5. Cleanup and resource management

ERROR HANDLING:
- Dataset-level failures stop processing (fail-fast)
- Individual file failures are tracked but don't stop dataset
- Comprehensive error reporting with context
- Recovery recommendations for failed processing
- Resource cleanup on failures

DEPENDENCIES:
- ConfigManager for configuration loading
- ProgressTracker for file-level tracking
- Enhanced Logger for structured logging
- Dataset processors for actual processing
"""

from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import time
import traceback
from dataclasses import dataclass
from enum import Enum

from ..utils.config_manager import ConfigManager
from ..utils.progress_tracker import ProgressTracker, ProcessingStatus
from ..utils.enhanced_logger import setup_logger, ComponentLogger
from ..processors.base_processor import BaseProcessor, ProcessingMode
from ..processors.ten_minutes_air_temperature_processor import TenMinutesAirTemperatureProcessor


class ProcessingResult(Enum):
    """Overall processing result status"""
    SUCCESS = "SUCCESS"
    PARTIAL_SUCCESS = "PARTIAL_SUCCESS"
    FAILURE = "FAILURE"
    CANCELLED = "CANCELLED"


@dataclass
class DatasetResult:
    """Result of processing a single dataset"""
    dataset_name: str
    success: bool
    files_processed: int
    files_failed: int
    processing_time: float
    error_message: Optional[str] = None
    summary: Optional[Dict[str, Any]] = None


@dataclass
class BulkIngestResult:
    """Overall result of bulk ingestion process"""
    overall_result: ProcessingResult
    datasets_processed: List[DatasetResult]
    total_processing_time: float
    start_time: datetime
    end_time: datetime
    summary: Dict[str, Any]


class BulkIngestController:
    """
    Main controller for bulk historical data ingestion.
    
    Orchestrates the processing of multiple datasets with parallel workers,
    comprehensive progress tracking, and detailed error handling.
    """
    
    def __init__(self, config_dir: Optional[Path] = None):
        """
        Initialize the bulk ingest controller.
        
        Args:
            config_dir: Directory containing configuration files
        """
        # Initialize configuration
        config_dir_str = str(config_dir) if config_dir else "configs"
        self.config_manager = ConfigManager(config_dir_str)
        
        # Initialize progress tracker
        self.base_config = self.config_manager.get_validated_base_config()
        progress_db_path = Path(self.base_config['paths']['progress_db'])
        self.progress_tracker = ProgressTracker(progress_db_path)
        
        # Initialize logger
        self.logger = setup_logger("DWD10TAH1T", "bulk_ingest_controller")
        
        # Processing state
        self.start_time: Optional[datetime] = None
        self.dataset_results: List[DatasetResult] = []
        
        # Available processors
        self.processor_registry = {
            "10_minutes_air_temperature": TenMinutesAirTemperatureProcessor
        }
        
        self.logger.info("Bulk Ingest Controller initialized")
        self.logger.info(f"Available processors: {list(self.processor_registry.keys())}")
    
    def run_bulk_ingestion(
        self, 
        datasets: Optional[List[str]] = None,
        mode: ProcessingMode = ProcessingMode.BULK,
        max_workers: Optional[int] = None
    ) -> BulkIngestResult:
        """
        Run bulk ingestion for specified datasets.
        
        Args:
            datasets: List of dataset names to process (None = all available)
            mode: Processing mode (BULK or INCREMENTAL)
            max_workers: Maximum number of parallel workers per dataset
            
        Returns:
            BulkIngestResult with comprehensive processing results
        """
        self.start_time = datetime.now()
        self.dataset_results = []
        
        self.logger.info("=" * 80)
        self.logger.info("🚀 BULK INGEST CONTROLLER STARTING")
        self.logger.info("=" * 80)
        self.logger.info(f"Mode: {mode.value}")
        self.logger.info(f"Start time: {self.start_time}")
        
        try:
            # Determine datasets to process
            if datasets is None:
                datasets = list(self.processor_registry.keys())
            
            self.logger.info(f"Datasets to process: {datasets}")
            
            # Validate datasets
            invalid_datasets = [d for d in datasets if d not in self.processor_registry]
            if invalid_datasets:
                raise ValueError(f"Unknown datasets: {invalid_datasets}")
            
            # Process each dataset sequentially
            overall_success = True
            
            for i, dataset_name in enumerate(datasets, 1):
                self.logger.info(f"📊 Processing dataset {i}/{len(datasets)}: {dataset_name}")
                
                try:
                    result = self._process_dataset(dataset_name, mode, max_workers)
                    self.dataset_results.append(result)
                    
                    if not result.success:
                        self.logger.error(f"❌ Dataset {dataset_name} failed: {result.error_message}")
                        overall_success = False
                        break  # Fail-fast behavior
                    else:
                        self.logger.info(f"✅ Dataset {dataset_name} completed successfully")
                        
                except Exception as e:
                    error_msg = f"Critical error processing dataset {dataset_name}: {e}"
                    self.logger.error(error_msg)
                    self.logger.error(f"Traceback: {traceback.format_exc()}")
                    
                    # Create failure result
                    result = DatasetResult(
                        dataset_name=dataset_name,
                        success=False,
                        files_processed=0,
                        files_failed=0,
                        processing_time=0.0,
                        error_message=error_msg
                    )
                    self.dataset_results.append(result)
                    overall_success = False
                    break  # Fail-fast behavior
            
            # Generate final result
            end_time = datetime.now()
            total_time = (end_time - self.start_time).total_seconds()
            
            # Determine overall result
            if overall_success and all(r.success for r in self.dataset_results):
                overall_result = ProcessingResult.SUCCESS
            elif any(r.success for r in self.dataset_results):
                overall_result = ProcessingResult.PARTIAL_SUCCESS
            else:
                overall_result = ProcessingResult.FAILURE
            
            # Generate summary
            summary = self._generate_summary()
            
            result = BulkIngestResult(
                overall_result=overall_result,
                datasets_processed=self.dataset_results,
                total_processing_time=total_time,
                start_time=self.start_time,
                end_time=end_time,
                summary=summary
            )
            
            # Log final results
            self._log_final_results(result)
            
            return result
            
        except Exception as e:
            end_time = datetime.now()
            total_time = (end_time - self.start_time).total_seconds()
            
            self.logger.error(f"💥 Bulk ingestion failed with critical error: {e}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            
            return BulkIngestResult(
                overall_result=ProcessingResult.FAILURE,
                datasets_processed=self.dataset_results,
                total_processing_time=total_time,
                start_time=self.start_time,
                end_time=end_time,
                summary={"error": str(e), "traceback": traceback.format_exc()}
            )
    
    def _process_dataset(
        self, 
        dataset_name: str, 
        mode: ProcessingMode,
        max_workers: Optional[int]
    ) -> DatasetResult:
        """
        Process a single dataset with comprehensive error handling.
        
        Args:
            dataset_name: Name of dataset to process
            mode: Processing mode
            max_workers: Maximum number of workers
            
        Returns:
            DatasetResult with processing details
        """
        start_time = time.time()
        
        self.logger.info(f"🔄 Starting dataset processing: {dataset_name}")
        
        try:
            # Get processor class
            processor_class = self.processor_registry[dataset_name]
            
            # Create processor instance
            processor = processor_class("main_worker")
            
            # Run processing
            with self.logger.timer(f"dataset_{dataset_name}"):
                stats = processor.run(mode)
            
            processing_time = time.time() - start_time
            
            # Determine success
            success = stats.failed_files == 0
            
            # Get processing summary
            summary = processor.get_processing_summary()
            
            result = DatasetResult(
                dataset_name=dataset_name,
                success=success,
                files_processed=stats.successful_files,
                files_failed=stats.failed_files,
                processing_time=processing_time,
                summary=summary
            )
            
            if success:
                self.logger.info(f"✅ Dataset {dataset_name} processing completed successfully")
                self.logger.info(f"   📊 Files processed: {stats.successful_files}")
                self.logger.info(f"   📊 Records processed: {stats.total_records:,}")
                self.logger.info(f"   ⏱️  Processing time: {processing_time:.2f}s")
            else:
                error_msg = f"Dataset processing completed with {stats.failed_files} failures"
                result.error_message = error_msg
                self.logger.warning(f"⚠️ {error_msg}")
            
            return result
            
        except Exception as e:
            processing_time = time.time() - start_time
            error_msg = f"Dataset processing failed: {e}"
            
            self.logger.error(f"❌ {error_msg}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            
            return DatasetResult(
                dataset_name=dataset_name,
                success=False,
                files_processed=0,
                files_failed=0,
                processing_time=processing_time,
                error_message=error_msg
            )
    
    def _generate_summary(self) -> Dict[str, Any]:
        """Generate comprehensive processing summary"""
        total_files_processed = sum(r.files_processed for r in self.dataset_results)
        total_files_failed = sum(r.files_failed for r in self.dataset_results)
        successful_datasets = sum(1 for r in self.dataset_results if r.success)
        
        return {
            "datasets": {
                "total": len(self.dataset_results),
                "successful": successful_datasets,
                "failed": len(self.dataset_results) - successful_datasets
            },
            "files": {
                "total_processed": total_files_processed,
                "total_failed": total_files_failed,
                "success_rate": (total_files_processed / (total_files_processed + total_files_failed) * 100) if (total_files_processed + total_files_failed) > 0 else 0
            },
            "timing": {
                "total_processing_time": sum(r.processing_time for r in self.dataset_results),
                "average_time_per_dataset": sum(r.processing_time for r in self.dataset_results) / len(self.dataset_results) if self.dataset_results else 0
            },
            "dataset_details": [
                {
                    "name": r.dataset_name,
                    "success": r.success,
                    "files_processed": r.files_processed,
                    "files_failed": r.files_failed,
                    "processing_time": r.processing_time,
                    "error": r.error_message
                }
                for r in self.dataset_results
            ]
        }
    
    def _log_final_results(self, result: BulkIngestResult):
        """Log comprehensive final results"""
        self.logger.info("=" * 80)
        self.logger.info("📊 BULK INGEST CONTROLLER RESULTS")
        self.logger.info("=" * 80)
        
        # Overall status
        status_emoji = {
            ProcessingResult.SUCCESS: "✅",
            ProcessingResult.PARTIAL_SUCCESS: "⚠️",
            ProcessingResult.FAILURE: "❌",
            ProcessingResult.CANCELLED: "🚫"
        }
        
        self.logger.info(f"{status_emoji[result.overall_result]} Overall Result: {result.overall_result.value}")
        self.logger.info(f"⏱️  Total Time: {result.total_processing_time:.2f}s")
        self.logger.info(f"📅 Start: {result.start_time}")
        self.logger.info(f"📅 End: {result.end_time}")
        
        # Dataset summary
        self.logger.info(f"📊 Datasets: {result.summary['datasets']['successful']}/{result.summary['datasets']['total']} successful")
        self.logger.info(f"📄 Files: {result.summary['files']['total_processed']:,} processed, {result.summary['files']['total_failed']} failed")
        self.logger.info(f"📈 Success Rate: {result.summary['files']['success_rate']:.1f}%")
        
        # Individual dataset results
        self.logger.info("📋 Dataset Details:")
        for dataset_result in result.datasets_processed:
            status = "✅" if dataset_result.success else "❌"
            self.logger.info(f"   {status} {dataset_result.dataset_name}: {dataset_result.files_processed} files ({dataset_result.processing_time:.1f}s)")
            if dataset_result.error_message:
                self.logger.info(f"      Error: {dataset_result.error_message}")
        
        self.logger.info("=" * 80)
    
    def get_processing_status(self) -> Dict[str, Any]:
        """Get current processing status"""
        if not self.start_time:
            return {"status": "not_started"}
        
        current_time = datetime.now()
        elapsed_time = (current_time - self.start_time).total_seconds()
        
        return {
            "status": "running" if not self.dataset_results or len(self.dataset_results) < len(self.processor_registry) else "completed",
            "start_time": self.start_time.isoformat(),
            "elapsed_time": elapsed_time,
            "datasets_completed": len(self.dataset_results),
            "current_dataset": self.dataset_results[-1].dataset_name if self.dataset_results else None
        }


# Example usage and testing
if __name__ == "__main__":
    try:
        # Test bulk ingest controller
        controller = BulkIngestController()
        
        # Run test with air temperature dataset
        result = controller.run_bulk_ingestion(
            datasets=["10_minutes_air_temperature"],
            mode=ProcessingMode.BULK
        )
        
        print(f"✅ Bulk ingestion test completed: {result.overall_result.value}")
        print(f"Datasets processed: {len(result.datasets_processed)}")
        print(f"Total time: {result.total_processing_time:.2f}s")
        
    except Exception as e:
        print(f"❌ Bulk ingest controller test failed: {e}")
