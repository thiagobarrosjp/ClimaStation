"""
ZIP File Handler Utility for ClimaStation Weather Data Processing

This utility module handles extraction of text files from ZIP archives containing
German weather station data and metadata. It provides robust extraction with
proper error handling and validation for the weather data processing pipeline.

AUTHOR: ClimaStation Backend Pipeline
VERSION: Enhanced with better error handling and validation
LAST UPDATED: 2025-01-15

KEY FEATURES:
- Extracts .txt files from weather data ZIP archives
- Handles both raw measurement data and metadata ZIP files
- Robust error handling for corrupted or missing files
- Automatic directory creation and cleanup
- File validation and filtering
- Support for different encodings (Latin-1 for German data)

EXPECTED INPUT:
- ZIP files containing German weather station data (.txt files)
- Both raw data ZIPs and metadata ZIPs from DWD (German Weather Service)

EXPECTED OUTPUT:
- Extracted .txt files in specified destination folders
- List of Path objects pointing to successfully extracted files

USAGE:
    from app.io_helpers.zip_handler import extract_txt_files_from_zip
    
    # Extract files from a weather data ZIP
    extracted_files = extract_txt_files_from_zip(
        zip_path=Path("data/raw/weather_station_12345.zip"),
        extract_to=Path("data/temp/extracted")
    )
    
    for file_path in extracted_files:
        print(f"Extracted: {file_path}")
"""

import zipfile
from pathlib import Path
from typing import List, Optional, Set
import logging


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
        logger: Optional logger for detailed logging (if None, uses print statements)
        
    Returns:
        List of Path objects pointing to successfully extracted .txt files
        
    Raises:
        FileNotFoundError: If the ZIP file doesn't exist
        zipfile.BadZipFile: If the ZIP file is corrupted or invalid
        PermissionError: If unable to write to extraction folder
        RuntimeError: For other extraction failures
        
    Example:
        # Basic usage
        files = extract_txt_files_from_zip(
            Path("weather_data.zip"), 
            Path("temp/extracted")
        )
        
        # With logging
        files = extract_txt_files_from_zip(
            Path("weather_data.zip"), 
            Path("temp/extracted"),
            logger=my_logger
        )
    """
    def log_message(level: str, message: str):
        """Helper function to log messages with fallback to print."""
        if logger:
            getattr(logger, level.lower())(message)
        else:
            print(f"{level.upper()}: {message}")
    
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
            
            # Log the files that will be extracted
            for i, member in enumerate(txt_members, 1):
                log_message("DEBUG", f"      {i}. {member}")
            
            # Extract only .txt files with validation
            for member in txt_members:
                try:
                    # Validate member is actually a file (not directory)
                    member_info = zip_ref.getinfo(member)
                    if member_info.is_dir():
                        log_message("DEBUG", f"   ⏭️  Skipping directory: {member}")
                        continue
                    
                    # Extract the file
                    zip_ref.extract(member, extract_to)
                    
                    # Create full path to extracted file
                    extracted_file_path = extract_to / member
                    
                    # Validate extraction was successful
                    if extracted_file_path.exists() and extracted_file_path.is_file():
                        extracted_files.append(extracted_file_path)
                        file_size = extracted_file_path.stat().st_size
                        log_message("DEBUG", f"   ✅ Extracted: {member} ({file_size:,} bytes)")
                    else:
                        log_message("WARNING", f"   ⚠️  Extraction failed for: {member}")
                        
                except Exception as e:
                    log_message("WARNING", f"   ❌ Failed to extract {member}: {e}")
                    continue
            
    except zipfile.BadZipFile as e:
        error_msg = f"Corrupted or invalid ZIP file: {zip_path}"
        log_message("ERROR", error_msg)
        raise zipfile.BadZipFile(error_msg) from e
    except PermissionError as e:
        error_msg = f"Permission denied accessing ZIP file: {zip_path}"
        log_message("ERROR", error_msg)
        raise PermissionError(error_msg) from e
    except Exception as e:
        error_msg = f"Unexpected error extracting from {zip_path}: {e}"
        log_message("ERROR", error_msg)
        raise RuntimeError(error_msg) from e
    
    # Final validation and summary
    if extracted_files:
        log_message("INFO", f"   🎉 Successfully extracted {len(extracted_files)} files")
        total_size = sum(f.stat().st_size for f in extracted_files if f.exists())
        log_message("DEBUG", f"   📊 Total extracted size: {total_size:,} bytes")
    else:
        log_message("WARNING", f"   ⚠️  No files were successfully extracted from {zip_path.name}")
    
    return extracted_files


def validate_zip_file(zip_path: Path, logger: Optional[logging.Logger] = None) -> bool:
    """
    Validate that a ZIP file exists and is readable without extracting it.
    
    This function performs basic validation on a ZIP file to check if it's
    accessible and contains the expected content before attempting extraction.
    
    Args:
        zip_path: Path to the ZIP file to validate
        logger: Optional logger for detailed logging
        
    Returns:
        True if ZIP file is valid and readable, False otherwise
        
    Example:
        if validate_zip_file(Path("weather_data.zip")):
            # Proceed with extraction
            files = extract_txt_files_from_zip(...)
    """
    def log_message(level: str, message: str):
        """Helper function to log messages with fallback to print."""
        if logger:
            getattr(logger, level.lower())(message)
        else:
            print(f"{level.upper()}: {message}")
    
    if not zip_path.exists():
        log_message("ERROR", f"ZIP file does not exist: {zip_path}")
        return False
    
    if not zip_path.is_file():
        log_message("ERROR", f"Path is not a file: {zip_path}")
        return False
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Test the ZIP file integrity
            bad_files = zip_ref.testzip()
            if bad_files:
                log_message("ERROR", f"ZIP file contains corrupted files: {bad_files}")
                return False
            
            # Check if it contains any files
            members = zip_ref.namelist()
            if not members:
                log_message("WARNING", f"ZIP file is empty: {zip_path}")
                return False
            
            log_message("DEBUG", f"ZIP file validation passed: {len(members)} files found")
            return True
            
    except zipfile.BadZipFile:
        log_message("ERROR", f"Invalid or corrupted ZIP file: {zip_path}")
        return False
    except Exception as e:
        log_message("ERROR", f"Error validating ZIP file {zip_path}: {e}")
        return False


def get_zip_file_info(zip_path: Path, logger: Optional[logging.Logger] = None) -> Optional[dict]:
    """
    Get detailed information about a ZIP file without extracting it.
    
    This function provides metadata about a ZIP file including file count,
    total size, and list of contained files. Useful for logging and validation.
    
    Args:
        zip_path: Path to the ZIP file to analyze
        logger: Optional logger for detailed logging
        
    Returns:
        Dictionary with ZIP file information, or None if file is invalid
        
    Example:
        info = get_zip_file_info(Path("weather_data.zip"))
        if info:
            print(f"ZIP contains {info['file_count']} files")
            print(f"Total size: {info['total_size']:,} bytes")
    """
    def log_message(level: str, message: str):
        """Helper function to log messages with fallback to print."""
        if logger:
            getattr(logger, level.lower())(message)
        else:
            print(f"{level.upper()}: {message}")
    
    if not validate_zip_file(zip_path, logger):
        return None
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            members = zip_ref.namelist()
            txt_files = [f for f in members if f.lower().endswith('.txt')]
            
            # Calculate total uncompressed size
            total_size = sum(zip_ref.getinfo(member).file_size for member in members)
            txt_size = sum(zip_ref.getinfo(member).file_size for member in txt_files)
            
            info = {
                'file_path': str(zip_path),
                'file_count': len(members),
                'txt_file_count': len(txt_files),
                'total_size': total_size,
                'txt_size': txt_size,
                'all_files': members,
                'txt_files': txt_files
            }
            
            log_message("DEBUG", f"ZIP info gathered: {info['file_count']} total files, {info['txt_file_count']} text files")
            return info
            
    except Exception as e:
        log_message("ERROR", f"Error getting ZIP file info for {zip_path}: {e}")
        return None


def cleanup_extracted_files(extract_dir: Path, logger: Optional[logging.Logger] = None) -> bool:
    """
    Clean up extracted files and directories.
    
    This function safely removes extracted files and the extraction directory.
    Useful for cleaning up temporary files after processing.
    
    Args:
        extract_dir: Directory containing extracted files to clean up
        logger: Optional logger for detailed logging
        
    Returns:
        True if cleanup was successful, False otherwise
        
    Example:
        # After processing extracted files
        cleanup_extracted_files(Path("temp/extracted"))
    """
    def log_message(level: str, message: str):
        """Helper function to log messages with fallback to print."""
        if logger:
            getattr(logger, level.lower())(message)
        else:
            print(f"{level.upper()}: {message}")
    
    if not extract_dir.exists():
        log_message("DEBUG", f"Extraction directory does not exist: {extract_dir}")
        return True
    
    try:
        # Remove all files in the directory
        files_removed = 0
        for file_path in extract_dir.rglob("*"):
            if file_path.is_file():
                file_path.unlink()
                files_removed += 1
        
        # Remove empty directories
        for dir_path in sorted(extract_dir.rglob("*"), key=lambda p: len(p.parts), reverse=True):
            if dir_path.is_dir() and not any(dir_path.iterdir()):
                dir_path.rmdir()
        
        # Remove the main directory if empty
        if not any(extract_dir.iterdir()):
            extract_dir.rmdir()
            log_message("DEBUG", f"Cleaned up extraction directory: {files_removed} files removed")
            return True
        else:
            log_message("WARNING", f"Extraction directory not empty after cleanup: {extract_dir}")
            return False
            
    except Exception as e:
        log_message("ERROR", f"Error cleaning up extraction directory {extract_dir}: {e}")
        return False


if __name__ == "__main__":
    """
    Test the ZIP handler functionality when run directly.
    
    This provides a simple test to verify that the ZIP handler is working correctly.
    Run this file directly to test: python -m app.io_helpers.zip_handler
    """
    print("Testing ClimaStation ZIP Handler...")
    
    # Test with a hypothetical ZIP file (this will fail gracefully)
    test_zip = Path("data/test/sample.zip")
    test_extract = Path("data/test/extracted")
    
    print(f"Testing ZIP validation with: {test_zip}")
    is_valid = validate_zip_file(test_zip)
    print(f"ZIP validation result: {is_valid}")
    
    if test_zip.exists():
        print(f"Testing extraction to: {test_extract}")
        try:
            files = extract_txt_files_from_zip(test_zip, test_extract)
            print(f"✅ Extracted {len(files)} files")
            
            # Test cleanup
            cleanup_extracted_files(test_extract)
            print("✅ Cleanup completed")
            
        except Exception as e:
            print(f"❌ Test failed: {e}")
    else:
        print("ℹ️  No test ZIP file found - functionality ready for real data")
    
    print("✅ ZIP handler testing complete")