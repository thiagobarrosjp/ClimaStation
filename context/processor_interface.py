"""
Standard processor interface for ClimaStation dataset processors.
All dataset processors must implement this interface.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from pathlib import Path

@dataclass
class ProcessingResult:
    """Standard result object returned by all processors"""
    success: bool
    files_processed: int
    files_failed: int
    output_files: List[Path]
    errors: List[str]
    metadata: Dict[str, Any]

class IDatasetProcessor(ABC):
    """Interface that all dataset processors must implement"""
    
    @abstractmethod
    def process_files(self, file_paths: List[Path], output_dir: Path) -> ProcessingResult:
        """Process a batch of files and return standardized results"""
        pass
    
    @abstractmethod
    def validate_file(self, file_path: Path) -> bool:
        """Validate if file can be processed by this processor"""
        pass