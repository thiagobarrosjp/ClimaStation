"""
ClimaStation Dataset Processor Interface (Reference Only)

SCRIPT IDENTIFICATION: (interface-only)

PURPOSE:
Defines the `IDatasetProcessor` interface and `ProcessingResult` dataclass
used by all dataset processing modules in the ClimaStation pipeline.

This file serves as a REFERENCE for implementation. It MUST NOT be imported
or executed at runtime.

RESPONSIBILITIES:
- Provide a standard interface contract for all processing components
- Define the expected output structure via `ProcessingResult`
- Support typing and IDE auto-completion for implementation modules

USAGE:
    # Do NOT import or call this module directly!
    # Refer to it when implementing a processor, e.g.:
    class MyProcessor(IDatasetProcessor):
        def process_files(...): ...
        def validate_file(...): ...

PROTECTION:
This file will raise an ImportError if imported during runtime to prevent misuse.
"""

raise ImportError("Do not import 'processor_interface.py' directly — it is for reference only.")

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
    warnings: Optional[List[str]] = None

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
