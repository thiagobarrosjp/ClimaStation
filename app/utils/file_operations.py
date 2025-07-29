"""
ClimaStation File Operations Utilities

SCRIPT IDENTIFICATION: DWD10TAH3F (File Operations)

PURPOSE:
Robust file handling utilities for downloading, extracting, and validating DWD
climate data files. Provides standardized file operations with proper error
handling, retry logic, and comprehensive logging integration.

RESPONSIBILITIES:
- Download files from DWD servers with retry logic and validation
- Extract ZIP archives and return lists of extracted files
- Validate CSV/TXT file structure against expected schemas
- Handle DWD-specific file formats (semicolon-delimited, German encoding)
- Integrate with configuration system for timeouts and retry settings
- Provide detailed logging for all file operations

USAGE:
    from app.utils.file_operations import download_file, extract_zip, validate_file_structure
    from app.utils.enhanced_logger import get_logger
    from app.utils.config_manager import load_config
    
    logger = get_logger("DOWNLOAD")
    config = load_config("10_minutes_air_temperature", logger)
    
    # Download file
    success = download_file("https://example.com/file.zip", Path("data/file.zip"), config, logger)
    
    # Extract ZIP
    extracted_files = extract_zip(Path("data/file.zip"), Path("data/extracted/"), config, logger)
    
    # Validate file structure
    is_valid = validate_file_structure(Path("data/file.txt"), ["STATIONS_ID", "MESS_DATUM"], config, logger)

FILE FORMAT SUPPORT:
- DWD ZIP archives containing multiple data files
- Semicolon-delimited CSV/TXT files with German encoding (ISO-8859-1/UTF-8)
- Station metadata files and measurement data files
- Handles both historical and recent data file formats

ERROR HANDLING:
- Network timeouts and connection errors for downloads
- Corrupted or incomplete ZIP files
- Invalid file structures and encoding issues
- Partial extraction cleanup on failures
- Detailed error logging with component-specific codes
"""

import requests
import zipfile
from pathlib import Path
from typing import List, Dict, Any
import time
import shutil
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os

# Import the StructuredLoggerAdapter type for proper type hints
from .enhanced_logger import StructuredLoggerAdapter

def download_file(url: str, destination: Path, config: Dict[str, Any], logger: StructuredLoggerAdapter) -> bool:
    """
    Download file with retry logic and validation.
    
    Downloads files from DWD servers with configurable retry logic, timeout handling,
    and comprehensive validation. Supports resuming partial downloads and handles
    network errors gracefully.
    
    Args:
        url: URL to download from
        destination: Path where file should be saved
        config: Configuration dictionary containing retry and timeout settings
        logger: StructuredLoggerAdapter instance for progress and error reporting
        
    Returns:
        True if download successful, False otherwise
        
    Example:
        config = load_config("10_minutes_air_temperature", logger)
        success = download_file(
            "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/historical/stundenwerte_TU_00001_19370101_19860630_hist.zip",
            Path("data/downloaded/temp_data.zip"),
            config,
            logger
        )
    """
    try:
        # Get configuration settings with defaults
        failure_config = config.get('failure_handling', {})
        max_retries = failure_config.get('max_retries', 3)
        retry_delay = failure_config.get('retry_delay_seconds', 300)
        timeout_seconds = config.get('processing', {}).get('worker_timeout_minutes', 30) * 60
        
        # Create destination directory if it doesn't exist
        destination.parent.mkdir(parents=True, exist_ok=True)
        
        # Configure session with retry strategy
        session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set headers to identify as a research/academic user
        headers = {
            'User-Agent': 'ClimaStation/1.0 (Climate Data Research Tool)',
            'Accept': 'application/octet-stream, */*',
            'Accept-Encoding': 'gzip, deflate'
        }
        
        logger.info(f"Starting download: {url}", extra={
            "component": "DOWNLOAD",
            "structured_data": {
                "url": url,
                "destination": str(destination),
                "max_retries": max_retries,
                "timeout_seconds": timeout_seconds
            }
        })
        
        # Attempt download with retries
        for attempt in range(max_retries + 1):
            try:
                # Check if file already exists and get its size
                resume_header = {}
                if destination.exists():
                    existing_size = destination.stat().st_size
                    resume_header['Range'] = f'bytes={existing_size}-'
                    logger.info(f"Resuming download from byte {existing_size}", extra={
                        "component": "DOWNLOAD",
                        "structured_data": {"existing_size": existing_size, "attempt": attempt + 1}
                    })
                
                # Make the request
                response = session.get(
                    url, 
                    headers={**headers, **resume_header},
                    timeout=timeout_seconds,
                    stream=True
                )
                response.raise_for_status()
                
                # Get content length for progress tracking
                content_length = response.headers.get('content-length')
                if content_length:
                    total_size = int(content_length)
                    logger.info(f"Download size: {total_size:,} bytes", extra={
                        "component": "DOWNLOAD",
                        "structured_data": {"total_size": total_size}
                    })
                
                # Download with progress tracking
                mode = 'ab' if 'Range' in resume_header else 'wb'
                downloaded_size = destination.stat().st_size if destination.exists() and mode == 'ab' else 0
                
                with open(destination, mode) as f:
                    start_time = time.time()
                    chunk_size = 8192  # 8KB chunks
                    
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:  # Filter out keep-alive chunks
                            f.write(chunk)
                            downloaded_size += len(chunk)
                            
                            # Log progress every 10MB
                            if downloaded_size % (10 * 1024 * 1024) == 0:
                                elapsed = time.time() - start_time
                                speed = downloaded_size / elapsed if elapsed > 0 else 0
                                logger.debug(f"Downloaded {downloaded_size:,} bytes ({speed/1024:.1f} KB/s)", extra={
                                    "component": "DOWNLOAD",
                                    "structured_data": {
                                        "downloaded_size": downloaded_size,
                                        "speed_kbps": round(speed/1024, 1)
                                    }
                                })
                
                # Validate download
                if not destination.exists():
                    raise Exception("Downloaded file does not exist")
                
                final_size = destination.stat().st_size
                if final_size == 0:
                    raise Exception("Downloaded file is empty")
                
                # Verify file is not corrupted (basic check for ZIP files)
                if destination.suffix.lower() == '.zip':
                    try:
                        with zipfile.ZipFile(destination, 'r') as zip_file:
                            zip_file.testzip()  # Test ZIP integrity
                    except zipfile.BadZipFile:
                        raise Exception("Downloaded ZIP file is corrupted")
                
                elapsed_time = time.time() - start_time
                logger.info(f"Download completed successfully", extra={
                    "component": "DOWNLOAD",
                    "structured_data": {
                        "final_size": final_size,
                        "duration_seconds": round(elapsed_time, 2),
                        "average_speed_kbps": round((final_size / elapsed_time) / 1024, 1) if elapsed_time > 0 else 0,
                        "attempts_used": attempt + 1
                    }
                })
                
                return True
                
            except Exception as e:
                error_msg = f"Download attempt {attempt + 1} failed: {str(e)}"
                
                if attempt < max_retries:
                    logger.warning(f"{error_msg}. Retrying in {retry_delay} seconds...", extra={
                        "component": "DOWNLOAD",
                        "structured_data": {
                            "attempt": attempt + 1,
                            "max_retries": max_retries,
                            "retry_delay": retry_delay,
                            "error": str(e)
                        }
                    })
                    time.sleep(retry_delay)
                else:
                    logger.error(f"{error_msg}. All retry attempts exhausted.", extra={
                        "component": "DOWNLOAD",
                        "structured_data": {
                            "final_attempt": attempt + 1,
                            "total_attempts": max_retries + 1,
                            "error": str(e)
                        }
                    })
                    
                    # Clean up partial download
                    if destination.exists():
                        try:
                            destination.unlink()
                            logger.info("Cleaned up partial download", extra={"component": "DOWNLOAD"})
                        except Exception as cleanup_error:
                            logger.warning(f"Failed to clean up partial download: {cleanup_error}", extra={"component": "DOWNLOAD"})
                    
                    return False
        
        return False
        
    except Exception as e:
        logger.error(f"Download failed with unexpected error: {str(e)}", extra={
            "component": "DOWNLOAD",
            "structured_data": {"error": str(e), "url": url}
        })
        return False

def extract_zip(zip_path: Path, extract_to: Path, config: Dict[str, Any], logger: StructuredLoggerAdapter) -> List[Path]:
    """
    Extract ZIP file and return list of extracted files.
    
    Extracts ZIP archives with proper error handling, progress logging, and cleanup
    of partial extractions on failure. Handles DWD ZIP file structures and encoding issues.
    
    Args:
        zip_path: Path to ZIP file to extract
        extract_to: Directory to extract files to
        config: Configuration dictionary (currently unused but kept for consistency)
        logger: StructuredLoggerAdapter instance for progress and error reporting
        
    Returns:
        List of Path objects for successfully extracted files
        
    Raises:
        Exception: If ZIP file is corrupted or extraction fails
        
    Example:
        extracted_files = extract_zip(
            Path("data/downloaded/temp_data.zip"),
            Path("data/extracted/temp_data/"),
            config,
            logger
        )
        for file_path in extracted_files:
            print(f"Extracted: {file_path}")
    """
    extracted_files = []
    
    try:
        # Validate input
        if not zip_path.exists():
            raise FileNotFoundError(f"ZIP file not found: {zip_path}")
        
        if not zip_path.suffix.lower() == '.zip':
            raise ValueError(f"File is not a ZIP archive: {zip_path}")
        
        # Create extraction directory
        extract_to.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Starting ZIP extraction: {zip_path.name}", extra={
            "component": "EXTRACT",
            "structured_data": {
                "zip_path": str(zip_path),
                "extract_to": str(extract_to),
                "zip_size": zip_path.stat().st_size
            }
        })
        
        # Extract ZIP file
        with zipfile.ZipFile(zip_path, 'r') as zip_file:
            # Get list of files in ZIP
            file_list = zip_file.namelist()
            total_files = len(file_list)
            
            logger.info(f"ZIP contains {total_files} files", extra={
                "component": "EXTRACT",
                "structured_data": {
                    "total_files": total_files,
                    "file_list": file_list[:10]  # Log first 10 files to avoid spam
                }
            })
            
            # Test ZIP integrity first
            bad_file = zip_file.testzip()
            if bad_file:
                raise zipfile.BadZipFile(f"ZIP file is corrupted. Bad file: {bad_file}")
            
            # Extract all files
            start_time = time.time()
            for i, file_info in enumerate(zip_file.infolist()):
                try:
                    # Skip directories
                    if file_info.is_dir():
                        continue
                    
                    # Extract file
                    extracted_path = extract_to / file_info.filename
                    
                    # Create subdirectories if needed
                    extracted_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    # Extract the file
                    with zip_file.open(file_info) as source, open(extracted_path, 'wb') as target:
                        shutil.copyfileobj(source, target)
                    
                    # Preserve file timestamps if available
                    if hasattr(file_info, 'date_time'):
                        timestamp = time.mktime(file_info.date_time + (0, 0, -1))
                        os.utime(extracted_path, (timestamp, timestamp))
                    
                    extracted_files.append(extracted_path)
                    
                    # Log progress for large extractions
                    if (i + 1) % 100 == 0 or (i + 1) == total_files:
                        logger.debug(f"Extracted {i + 1}/{total_files} files", extra={
                            "component": "EXTRACT",
                            "structured_data": {
                                "progress": f"{i + 1}/{total_files}",
                                "current_file": file_info.filename
                            }
                        })
                
                except Exception as file_error:
                    logger.warning(f"Failed to extract file {file_info.filename}: {file_error}", extra={
                        "component": "EXTRACT",
                        "structured_data": {
                            "failed_file": file_info.filename,
                            "error": str(file_error)
                        }
                    })
                    continue
        
        extraction_time = time.time() - start_time
        logger.info(f"ZIP extraction completed", extra={
            "component": "EXTRACT",
            "structured_data": {
                "extracted_files_count": len(extracted_files),
                "total_files_in_zip": total_files,
                "duration_seconds": round(extraction_time, 2),
                "success_rate": round((len(extracted_files) / total_files) * 100, 1) if total_files > 0 else 0
            }
        })
        
        return extracted_files
        
    except Exception as e:
        logger.error(f"ZIP extraction failed: {str(e)}", extra={
            "component": "EXTRACT",
            "structured_data": {
                "zip_path": str(zip_path),
                "error": str(e),
                "extracted_files_count": len(extracted_files)
            }
        })
        
        # Clean up partial extraction
        if extracted_files:
            logger.info("Cleaning up partial extraction", extra={"component": "EXTRACT"})
            for file_path in extracted_files:
                try:
                    if file_path.exists():
                        file_path.unlink()
                except Exception as cleanup_error:
                    logger.warning(f"Failed to clean up {file_path}: {cleanup_error}", extra={"component": "EXTRACT"})
        
        raise

def validate_file_structure(file_path: Path, expected_columns: List[str], config: Dict[str, Any], logger: StructuredLoggerAdapter) -> bool:
    """
    Validate CSV/TXT file has expected structure.
    
    Validates DWD data files against expected column schemas, handling German
    encoding and semicolon delimiters. Checks for minimum data requirements
    and provides detailed validation feedback.
    
    Args:
        file_path: Path to file to validate
        expected_columns: List of expected column names
        config: Configuration dictionary containing validation settings
        logger: StructuredLoggerAdapter instance for validation results
        
    Returns:
        True if file structure is valid, False otherwise
        
    Example:
        is_valid = validate_file_structure(
            Path("data/extracted/produkt_tu_stunde_19370101_19861231_00001.txt"),
            ["STATIONS_ID", "MESS_DATUM", "TT_TU", "RF_TU"],
            config,
            logger
        )
    """
    try:
        # Validate input
        if not file_path.exists():
            logger.error(f"File not found: {file_path}", extra={
                "component": "VALIDATE",
                "structured_data": {"file_path": str(file_path), "error": "file_not_found"}
            })
            return False
        
        if file_path.stat().st_size == 0:
            logger.error(f"File is empty: {file_path}", extra={
                "component": "VALIDATE",
                "structured_data": {"file_path": str(file_path), "error": "empty_file"}
            })
            return False
        
        # Get validation configuration
        validation_config = config.get('validation', {})
        min_measurements = validation_config.get('min_measurements_per_file', 1)
        
        logger.info(f"Validating file structure: {file_path.name}", extra={
            "component": "VALIDATE",
            "structured_data": {
                "file_path": str(file_path),
                "expected_columns": expected_columns,
                "min_measurements": min_measurements,
                "file_size": file_path.stat().st_size
            }
        })
        
        # Try different encodings common in DWD files
        encodings = ['utf-8', 'iso-8859-1', 'cp1252']
        file_content = None
        used_encoding = None
        
        for encoding in encodings:
            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    file_content = f.read()
                    used_encoding = encoding
                    break
            except UnicodeDecodeError:
                continue
        
        if file_content is None:
            logger.error(f"Could not decode file with any supported encoding", extra={
                "component": "VALIDATE",
                "structured_data": {
                    "file_path": str(file_path),
                    "tried_encodings": encodings,
                    "error": "encoding_error"
                }
            })
            return False
        
        logger.debug(f"Successfully decoded file using {used_encoding} encoding", extra={
            "component": "VALIDATE",
            "structured_data": {"encoding": used_encoding}
        })
        
        # Parse CSV content (DWD uses semicolon delimiter)
        lines = file_content.strip().split('\n')
        if len(lines) < 2:  # Need at least header + 1 data row
            logger.error(f"File has insufficient data (only {len(lines)} lines)", extra={
                "component": "VALIDATE",
                "structured_data": {
                    "file_path": str(file_path),
                    "line_count": len(lines),
                    "error": "insufficient_data"
                }
            })
            return False
        
        # Parse header row
        header_line = lines[0].strip()
        
        # Try different delimiters
        delimiters = [';', ',', '\t']
        columns = None
        used_delimiter = None
        
        for delimiter in delimiters:
            test_columns = [col.strip() for col in header_line.split(delimiter)]
            if len(test_columns) > 1:  # Must have multiple columns
                columns = test_columns
                used_delimiter = delimiter
                break
        
        if columns is None:
            logger.error(f"Could not parse header with any supported delimiter", extra={
                "component": "VALIDATE",
                "structured_data": {
                    "file_path": str(file_path),
                    "header_line": header_line[:100],  # First 100 chars
                    "tried_delimiters": delimiters,
                    "error": "delimiter_error"
                }
            })
            return False
        
        logger.debug(f"Parsed header using '{used_delimiter}' delimiter", extra={
            "component": "VALIDATE",
            "structured_data": {
                "delimiter": used_delimiter,
                "column_count": len(columns),
                "columns": columns
            }
        })
        
        # Check for expected columns
        missing_columns = []
        for expected_col in expected_columns:
            if expected_col not in columns:
                missing_columns.append(expected_col)
        
        if missing_columns:
            logger.error(f"Missing expected columns: {missing_columns}", extra={
                "component": "VALIDATE",
                "structured_data": {
                    "file_path": str(file_path),
                    "missing_columns": missing_columns,
                    "found_columns": columns,
                    "error": "missing_columns"
                }
            })
            return False
        
        # Count data rows (excluding header)
        data_rows = len(lines) - 1
        if data_rows < min_measurements:
            logger.error(f"Insufficient data rows: {data_rows} (minimum: {min_measurements})", extra={
                "component": "VALIDATE",
                "structured_data": {
                    "file_path": str(file_path),
                    "data_rows": data_rows,
                    "min_required": min_measurements,
                    "error": "insufficient_measurements"
                }
            })
            return False
        
        # Validate a sample of data rows
        sample_size = min(10, data_rows)  # Check first 10 rows or all if fewer
        valid_rows = 0
        
        for i in range(1, sample_size + 1):
            try:
                data_line = lines[i].strip()
                data_values = [val.strip() for val in data_line.split(used_delimiter)]
                
                if len(data_values) == len(columns):
                    valid_rows += 1
                else:
                    logger.warning(f"Row {i} has {len(data_values)} values, expected {len(columns)}", extra={
                        "component": "VALIDATE",
                        "structured_data": {
                            "row_number": i,
                            "expected_columns": len(columns),
                            "actual_values": len(data_values)
                        }
                    })
            except Exception as row_error:
                logger.warning(f"Error parsing row {i}: {row_error}", extra={
                    "component": "VALIDATE",
                    "structured_data": {"row_number": i, "error": str(row_error)}
                })
        
        # Calculate validation success rate
        success_rate = (valid_rows / sample_size) * 100 if sample_size > 0 else 0
        
        if success_rate < 80:  # Require at least 80% of sample rows to be valid
            logger.error(f"Low data quality: only {success_rate:.1f}% of sample rows are valid", extra={
                "component": "VALIDATE",
                "structured_data": {
                    "file_path": str(file_path),
                    "success_rate": success_rate,
                    "valid_rows": valid_rows,
                    "sample_size": sample_size,
                    "error": "low_data_quality"
                }
            })
            return False
        
        # Validation successful
        logger.info(f"File validation successful", extra={
            "component": "VALIDATE",
            "structured_data": {
                "file_path": str(file_path),
                "encoding": used_encoding,
                "delimiter": used_delimiter,
                "column_count": len(columns),
                "data_rows": data_rows,
                "sample_success_rate": round(success_rate, 1),
                "all_expected_columns_found": True
            }
        })
        
        return True
        
    except Exception as e:
        logger.error(f"File validation failed with unexpected error: {str(e)}", extra={
            "component": "VALIDATE",
            "structured_data": {
                "file_path": str(file_path),
                "error": str(e)
            }
        })
        return False

