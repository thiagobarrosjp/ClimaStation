"""
ZIP File Handler Utility for ClimaStation Weather Data Processing

SCRIPT IDENTIFICATION: DWD10TAH3Z
- DWD: Deutscher Wetterdienst data source
- 10T: 10-minute air temperature dataset
- AH: Air temperature Historical data
- 3: Station processing component
- Z: ZIP handling utility component

PURPOSE:
Handles extraction of text files from ZIP archives containing German weather station 
data and metadata. Provides robust extraction with proper error handling, validation, 
and comprehensive logging for the weather data processing pipeline.

KEY FEATURES:
- Extracts .txt files from weather data ZIP archives with comprehensive logging
- Handles both raw measurement data and metadata ZIP files from DWD
- Robust error handling for corrupted files, permission errors, and missing archives
- Automatic directory creation and cleanup with detailed progress tracking
- File validation and filtering with size and integrity checks
- Support for different encodings (Latin-1 for German data)
- ZIP file information gathering and validation utilities
- Comprehensive cleanup functionality for temporary files

EXPECTED INPUT:
- ZIP files containing German weather station data (.txt files)
- Both raw data ZIPs and metadata ZIPs from DWD (German Weather Service)
- Paths: data/dwd/1_raw/historical/ and data/dwd/1_raw/meta_data/

EXPECTED OUTPUT:
- Extracted .txt files in specified destination folders
- List of Path objects pointing to successfully extracted files
- Detailed logging of extraction process and results

USAGE:
    from app.io_helpers.zip_handler import extract_txt_files_from_zip
    from app.utils.logger import setup_logger
    
    logger = setup_logger("DWD10TAH3Z", script_name="zip_handler")
    
    # Extract files from a weather data ZIP
    extracted_files = extract_txt_files_from_zip(
        zip_path=Path("data/dwd/1_raw/historical/weather_station_12345.zip"),
        extract_to=Path("data/temp/extracted"),
        logger=logger
    )
    
    for file_path in extracted_files:
        logger.info(f"Extracted: {file_path}")

AUTHOR: ClimaStation Backend Pipeline
VERSION: Enhanced with centralized logging support
LAST UPDATED: 2025-01-17
"""

import zipfile
from pathlib import Path
from typing import List, Optional, Set, Dict, Any, Tuple
import logging
import os
import shutil

# Import logger utility
try:
    from app.utils.logger import setup_logger
    HAS_LOGGER = True
except ImportError:
    HAS_LOGGER = False


def extract_txt_files_from_zip(zip_path: Path, extract_to: Path, logger: Optional[logging.Logger] = None) -> List[Path]:
    """
    Extract all .txt files from a ZIP archive to specified folder.
    
    This function safely extracts text files from ZIP archives, with robust
    error handling for common issues like corrupted files, permission errors,
    and missing archives. It only extracts .txt files to avoid extracting
    unwanted binary files or directories.
    
    Args:
        zip_path: Path to the ZIP file to extract from
        extract_to: Destination folder for extracted files
        logger: Logger instance (will automatically use centralized logging if available)
        
    Returns:
        List of Path objects pointing to successfully extracted .txt files
    """
    # Always create our own logger that automatically joins centralized logging
    if logger is None and HAS_LOGGER:
        logger = setup_logger("DWD10TAH3Z", script_name="zip_handler")
    elif logger is None:
        # Fallback logger if utils.logger not available
        logger = logging.getLogger("zip_handler_fallback")
        logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s — [DWD10TAH3Z] — %(levelname)s %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    def log_message(level: str, message: str):
        """Helper function to log messages with fallback to print."""
        if logger:
            getattr(logger, level.lower())(message)
        else:
            print(f"{level.upper()}: [DWD10TAH3Z] {message}")
    
    # Validate input ZIP file exists
    if not zip_path.exists():
        error_msg = f"ZIP file not found: {zip_path}"
        log_message("ERROR", error_msg)
        raise FileNotFoundError(error_msg)
    
    # Validate ZIP file is actually a file (not a directory)
    if not zip_path.is_file():
        error_msg = f"Path is not a file: {zip_path}"
        log_message("ERROR", error_msg)
        raise FileNotFoundError(error_msg)
    
    log_message("INFO", f"📦 Extracting text files from: {zip_path.name}")
    log_message("DEBUG", f"   📍 Full ZIP path: {zip_path.absolute()}")
    log_message("DEBUG", f"   📁 Extract to: {extract_to.absolute()}")
    
    # Log ZIP file size
    try:
        zip_size = zip_path.stat().st_size / 1024 / 1024  # MB
        log_message("DEBUG", f"   📊 ZIP file size: {zip_size:.2f} MB")
    except Exception as e:
        log_message("DEBUG", f"   ⚠️  Could not get ZIP file size: {e}")
    
    # Ensure extraction directory exists
    try:
        extract_to.mkdir(parents=True, exist_ok=True)
        log_message("DEBUG", f"   📁 Extraction directory ready: {extract_to}")
    except PermissionError as e:
        error_msg = f"Permission denied creating extraction directory: {extract_to}"
        log_message("ERROR", error_msg)
        raise PermissionError(error_msg) from e
    
    extracted_files = []
    
    try:
        # Open and validate ZIP file
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            log_message("DEBUG", f"   📋 ZIP file opened successfully")
            
            # Get list of all files in ZIP
            all_members = zip_ref.namelist()
            log_message("DEBUG", f"   📊 Total files in ZIP: {len(all_members)}")
            
            # Filter for .txt files only (case-insensitive)
            txt_members = [f for f in all_members if f.lower().endswith(".txt")]
            
            if not txt_members:
                log_message("WARNING", f"   ⚠️  No .txt files found in {zip_path.name}")
                return []
            
            log_message("INFO", f"   📄 Found {len(txt_members)} text files to extract")
            
            # Log the files that will be extracted (limit to first 10 for readability)
            if len(txt_members) <= 10:
                for i, member in enumerate(txt_members, 1):
                    log_message("DEBUG", f"      {i}. {member}")
            else:
                for i, member in enumerate(txt_members[:5], 1):
                    log_message("DEBUG", f"      {i}. {member}")
                log_message("DEBUG", f"      ... and {len(txt_members) - 5} more files")
            
            # Extract only .txt files with validation
            successful_extractions = 0
            failed_extractions = 0
            total_extracted_size = 0
            
            for i, member in enumerate(txt_members, 1):
                try:
                    # Validate member is actually a file (not directory)
                    member_info = zip_ref.getinfo(member)
                    if member_info.is_dir():
                        log_message("DEBUG", f"   ⏭️  Skipping directory: {member}")
                        continue
                    
                    # Log progress for large extractions
                    if len(txt_members) > 5:
                        log_message("DEBUG", f"   🔄 Extracting {i}/{len(txt_members)}: {member}")
                    
                    # Extract the file
                    zip_ref.extract(member, extract_to)
                    
                    # Get the extracted file path
                    extracted_file_path = extract_to / member
                    
                    # Validate extraction was successful
                    if extracted_file_path.exists():
                        file_size = extracted_file_path.stat().st_size
                        total_extracted_size += file_size
                        extracted_files.append(extracted_file_path)
                        successful_extractions += 1
                        
                        log_message("DEBUG", f"   ✅ Extracted: {member} ({file_size:,} bytes)")
                    else:
                        log_message("ERROR", f"   ❌ Extraction failed: {member} (file not found after extraction)")
                        failed_extractions += 1
                        
                except zipfile.BadZipFile as e:
                    log_message("ERROR", f"   ❌ Bad ZIP file error extracting {member}: {e}")
                    failed_extractions += 1
                    continue
                    
                except PermissionError as e:
                    log_message("ERROR", f"   ❌ Permission error extracting {member}: {e}")
                    failed_extractions += 1
                    continue
                    
                except Exception as e:
                    log_message("ERROR", f"   ❌ Unexpected error extracting {member}: {e}")
                    failed_extractions += 1
                    continue
            
            # Log extraction summary
            log_message("INFO", f"   📊 Extraction summary:")
            log_message("INFO", f"      ✅ Successful: {successful_extractions}")
            log_message("INFO", f"      ❌ Failed: {failed_extractions}")
            log_message("INFO", f"      📊 Total size: {total_extracted_size:,} bytes ({total_extracted_size / 1024 / 1024:.2f} MB)")
            
            if successful_extractions > 0:
                log_message("INFO", f"   🎉 Successfully extracted {successful_extractions} files")
            
            if failed_extractions > 0:
                log_message("WARNING", f"   ⚠️  {failed_extractions} files failed to extract")
            
            # Debug: Log total extracted size
            log_message("DEBUG", f"   📊 Total extracted size: {total_extracted_size:,} bytes")
            
    except zipfile.BadZipFile as e:
        error_msg = f"Invalid or corrupted ZIP file: {zip_path} - {e}"
        log_message("ERROR", error_msg)
        raise zipfile.BadZipFile(error_msg) from e
        
    except PermissionError as e:
        error_msg = f"Permission denied accessing ZIP file: {zip_path} - {e}"
        log_message("ERROR", error_msg)
        raise PermissionError(error_msg) from e
        
    except FileNotFoundError as e:
        error_msg = f"ZIP file not found during extraction: {zip_path} - {e}"
        log_message("ERROR", error_msg)
        raise FileNotFoundError(error_msg) from e
        
    except Exception as e:
        error_msg = f"Unexpected error extracting from {zip_path}: {e}"
        log_message("ERROR", error_msg)
        raise RuntimeError(error_msg) from e
    
    return extracted_files


def get_zip_info(zip_path: Path, logger: Optional[logging.Logger] = None) -> Dict[str, Any]:
    """
    Get information about ZIP file contents without extracting.
    
    Args:
        zip_path: Path to the ZIP file to analyze
        logger: Logger instance (will automatically use centralized logging if available)
        
    Returns:
        Dictionary with ZIP file information:
        {
            'total_files': int,
            'txt_files': int,
            'file_list': List[str],
            'txt_file_list': List[str],
            'total_size': int,
            'compressed_size': int
        }
    """
    # If no logger provided, create one with automatic centralized logging
    if logger is None and HAS_LOGGER:
        logger = setup_logger("DWD10TAH3Z", script_name="zip_handler")
    
    def log_message(level: str, message: str):
        """Helper function to log messages with fallback to print."""
        if logger:
            getattr(logger, level.lower())(message)
        else:
            print(f"{level.upper()}: [DWD10TAH3Z] {message}")
    
    if not zip_path.exists():
        error_msg = f"ZIP file not found: {zip_path}"
        log_message("ERROR", error_msg)
        raise FileNotFoundError(error_msg)
    
    log_message("DEBUG", f"📋 Analyzing ZIP file: {zip_path.name}")
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            all_members = zip_ref.namelist()
            txt_members = [f for f in all_members if f.lower().endswith(".txt")]
            
            total_size = 0
            compressed_size = 0
            
            for member in all_members:
                info = zip_ref.getinfo(member)
                total_size += info.file_size
                compressed_size += info.compress_size
            
            zip_info = {
                'total_files': len(all_members),
                'txt_files': len(txt_members),
                'file_list': all_members,
                'txt_file_list': txt_members,
                'total_size': total_size,
                'compressed_size': compressed_size,
                'compression_ratio': (compressed_size / total_size * 100) if total_size > 0 else 0
            }
            
            log_message("DEBUG", f"   📊 ZIP info: {len(all_members)} files, {len(txt_members)} .txt files")
            log_message("DEBUG", f"   📊 Size: {total_size:,} bytes uncompressed, {compressed_size:,} bytes compressed")
            log_message("DEBUG", f"   📊 Compression ratio: {zip_info['compression_ratio']:.1f}%")
            
            return zip_info
            
    except Exception as e:
        error_msg = f"Failed to analyze ZIP file {zip_path}: {e}"
        log_message("ERROR", error_msg)
        raise RuntimeError(error_msg) from e


def validate_zip_file(zip_path: Path, logger: Optional[logging.Logger] = None) -> Tuple[bool, List[str]]:
    """
    Validate ZIP file integrity and content.
    
    Args:
        zip_path: Path to the ZIP file to validate
        logger: Logger instance (will automatically use centralized logging if available)
        
    Returns:
        Tuple of (is_valid, list_of_issues)
    """
    # If no logger provided, create one with automatic centralized logging
    if logger is None and HAS_LOGGER:
        logger = setup_logger("DWD10TAH3Z", script_name="zip_handler")
    
    def log_message(level: str, message: str):
        """Helper function to log messages with fallback to print."""
        if logger:
            getattr(logger, level.lower())(message)
        else:
            print(f"{level.upper()}: [DWD10TAH3Z] {message}")
    
    issues = []
    
    if not zip_path.exists():
        issues.append(f"ZIP file not found: {zip_path}")
        return False, issues
    
    if not zip_path.is_file():
        issues.append(f"Path is not a file: {zip_path}")
        return False, issues
    
    log_message("DEBUG", f"🔍 Validating ZIP file: {zip_path.name}")
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Test ZIP file integrity
            bad_files = zip_ref.testzip()
            if bad_files:
                issues.append(f"Corrupted files in ZIP: {bad_files}")
            
            # Check if ZIP is empty
            if not zip_ref.namelist():
                issues.append("ZIP file is empty")
            
            # Check for .txt files
            txt_files = [f for f in zip_ref.namelist() if f.lower().endswith(".txt")]
            if not txt_files:
                issues.append("No .txt files found in ZIP")
            
            # Check file sizes
            for member in zip_ref.namelist():
                info = zip_ref.getinfo(member)
                if info.file_size == 0 and not info.is_dir():
                    issues.append(f"Empty file found: {member}")
            
            is_valid = len(issues) == 0
            
            if is_valid:
                log_message("DEBUG", f"   ✅ ZIP file validation passed")
            else:
                log_message("WARNING", f"   ⚠️  ZIP file validation issues: {len(issues)}")
                for issue in issues:
                    log_message("WARNING", f"      - {issue}")
            
            return is_valid, issues
            
    except zipfile.BadZipFile as e:
        issues.append(f"Invalid ZIP file format: {e}")
        log_message("ERROR", f"   ❌ ZIP validation failed: Invalid format")
        return False, issues
        
    except Exception as e:
        issues.append(f"Validation error: {e}")
        log_message("ERROR", f"   ❌ ZIP validation failed: {e}")
        return False, issues


def cleanup_extracted_files(extract_dir: Path, logger: Optional[logging.Logger] = None) -> bool:
    """
    Clean up extracted files and directories.
    
    Args:
        extract_dir: Directory containing extracted files to clean up
        logger: Logger instance (will automatically use centralized logging if available)
        
    Returns:
        True if cleanup successful, False otherwise
    """
    # If no logger provided, create one with automatic centralized logging
    if logger is None and HAS_LOGGER:
        logger = setup_logger("DWD10TAH3Z", script_name="zip_handler")
    
    def log_message(level: str, message: str):
        """Helper function to log messages with fallback to print."""
        if logger:
            getattr(logger, level.lower())(message)
        else:
            print(f"{level.upper()}: [DWD10TAH3Z] {message}")
    
    if not extract_dir.exists():
        log_message("DEBUG", f"🧹 Cleanup: Directory does not exist: {extract_dir}")
        return True
    
    log_message("DEBUG", f"🧹 Cleaning up extracted files: {extract_dir}")
    
    try:
        # Count files before cleanup
        file_count = 0
        for item in extract_dir.rglob("*"):
            if item.is_file():
                file_count += 1
        
        # Remove the entire directory tree
        shutil.rmtree(extract_dir)
        
        log_message("DEBUG", f"   ✅ Cleaned up {file_count} files and directory: {extract_dir}")
        return True
        
    except PermissionError as e:
        log_message("WARNING", f"   ⚠️  Permission error during cleanup: {e}")
        return False
        
    except Exception as e:
        log_message("WARNING", f"   ⚠️  Cleanup error (non-critical): {e}")
        return False


if __name__ == "__main__":
    """
    Test the ZIP handler functionality when run directly.
    """
    print("Testing ClimaStation ZIP Handler [DWD10TAH3Z]...")
    print("=" * 60)
    
    # Create test logger
    if HAS_LOGGER:
        test_logger = setup_logger("DWD10TAH3Z", script_name="zip_handler_test")
    else:
        test_logger = logging.getLogger("test_logger")
        test_logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s — [DWD10TAH3Z] — %(levelname)s %(message)s')
        handler.setFormatter(formatter)
        test_logger.addHandler(handler)
    
    # Test with a sample ZIP file (if available)
    test_zip_path = Path("data/dwd/2_downloaded_files/10_minutes/air_temperature/historical/10minutenwerte_TU_00003_19930428_19991231_hist.zip")
    
    if test_zip_path.exists():
        test_logger.info(f"🧪 Testing with ZIP file: {test_zip_path.name}")
        
        try:
            # Test ZIP info
            test_logger.info("📋 Testing get_zip_info...")
            zip_info = get_zip_info(test_zip_path, test_logger)
            test_logger.info(f"   ✅ ZIP contains {zip_info['total_files']} files, {zip_info['txt_files']} .txt files")
            
            # Test ZIP validation
            test_logger.info("🔍 Testing validate_zip_file...")
            is_valid, issues = validate_zip_file(test_zip_path, test_logger)
            test_logger.info(f"   ✅ ZIP validation: {'PASSED' if is_valid else 'FAILED'}")
            if issues:
                for issue in issues:
                    test_logger.info(f"      - {issue}")
            
            # Test extraction (to temporary directory)
            test_extract_dir = Path("temp_test_extraction")
            test_logger.info(f"📦 Testing extract_txt_files_from_zip to {test_extract_dir}...")
            
            extracted_files = extract_txt_files_from_zip(test_zip_path, test_extract_dir, test_logger)
            test_logger.info(f"   ✅ Extracted {len(extracted_files)} files")
            
            # Test cleanup
            test_logger.info("🧹 Testing cleanup_extracted_files...")
            cleanup_success = cleanup_extracted_files(test_extract_dir, test_logger)
            test_logger.info(f"   ✅ Cleanup: {'SUCCESS' if cleanup_success else 'FAILED'}")
            
        except Exception as e:
            test_logger.error(f"❌ Test failed: {e}")
    else:
        test_logger.warning(f"⚠️  Test ZIP file not found: {test_zip_path}")
        test_logger.info("ℹ️  Place a test ZIP file at the expected location to run full tests")
    
    # Test utility functions
    test_logger.info("✅ ZIP handler testing complete")
    test_logger.info("ℹ️  For full testing, ensure test ZIP files are available")
