#!/usr/bin/env python3
"""
ClimaStation Downloader Module

SCRIPT IDENTIFICATION:
    Name: downloader.py
    Purpose: Handles the downloading of DWD ZIP files from the list provided by the crawler.
    Author: ClimaStation Pipeline System
    Version: 1.0.0
    Created: 2025-01-29

PURPOSE:
    Handles the downloading of DWD ZIP files from the list provided by the crawler.
    Respects dataset structure, provides retry logic, and logs progress or failures.
    Designed to support partial test downloads or full dataset ingestion via config.

RESPONSIBILITIES:
    - Read list of files to download from dwd_urls.jsonl
    - Save ZIP files into correct subdirectories under data/dwd/2_downloaded_files/
    - Retry failed downloads with exponential backoff
    - Log progress and errors using enhanced logging system
    - Integrate with run_pipeline orchestration script

USAGE:
    from app.pipeline.downloader import run_downloader
    result = run_downloader(config, logger)

COMPONENT CODE: DOWNLOAD
    File download operations

LOG FORMAT:
    [TIMESTAMP] [COMPONENT] [LEVEL] MESSAGE {structured_data}

PERFORMANCE:
    - Retry logic for robustness
    - Efficient for batch or filtered downloads
    - Scales to support high-volume ingestion
"""

import json
import time
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass
from urllib.parse import urlparse
import traceback

# Import ClimaStation utilities
from app.utils.enhanced_logger import StructuredLoggerAdapter
from app.utils.file_operations import download_file

# ============================================================================
# DATA STRUCTURES
# ============================================================================

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

# ============================================================================
# PATH CONSTANTS
# ============================================================================

# Input paths
URLS_FILE_PATH = Path("data/dwd/1_crawl_dwd/dwd_urls.jsonl")

# Output paths  
DOWNLOAD_ROOT = Path("data/dwd/2_downloaded_files")

# Configuration paths
CONFIG_ROOT = Path("app/config")

# ============================================================================
# CORE DOWNLOADER FUNCTIONS
# ============================================================================

def is_valid_download_url(url: str) -> bool:
    """
    Check if URL points to a downloadable file (not a directory).
    
    Args:
        url: URL to validate
        
    Returns:
        True if URL appears to be a downloadable file, False otherwise
    """
    # Skip directory URLs (end with /)
    if url.endswith('/'):
        return False
    
    # Check for common file extensions
    valid_extensions = ['.zip', '.tar.gz', '.txt', '.csv', '.dat']
    return any(url.lower().endswith(ext) for ext in valid_extensions)

def parse_download_subfolder(url: str, config: Dict[str, Any]) -> str:
    """
    Parse the appropriate subfolder for a download URL based on dataset configuration.
    
    Args:
        url: The download URL to parse
        config: Dataset configuration dictionary
        
    Returns:
        Subfolder path as string (e.g., "recent/10_minutes/air_temperature")
    """
    try:
        # Parse URL to extract path components
        parsed_url = urlparse(url)
        path_parts = [part for part in parsed_url.path.split('/') if part]
        
        # Use dataset-specific path mapping if available
        path_mapping = config.get('downloader', {}).get('path_mapping', {})
        
        # Default: use the directory structure from the URL
        if len(path_parts) >= 3:
            # Extract meaningful path components (skip domain-specific parts)
            relevant_parts = []
            for part in path_parts:
                if part not in ['pub', 'CDC', 'observations_germany', 'climate']:
                    relevant_parts.append(part)
            
            # Join the relevant parts to create subfolder
            if relevant_parts:
                return '/'.join(relevant_parts[:-1])  # Exclude filename
        
        # Fallback to generic subfolder
        return config.get('downloader', {}).get('default_subfolder', 'misc')
        
    except Exception:
        # Fallback for any parsing errors
        return 'misc'

def check_file_already_downloaded(url: str, subfolder: str, config: Dict[str, Any]) -> bool:
    """
    Check if a file has already been downloaded.
    
    Args:
        url: The download URL
        subfolder: Target subfolder for the download
        config: Dataset configuration dictionary
        
    Returns:
        True if file already exists, False otherwise
    """
    try:
        # Extract filename from URL
        filename = Path(urlparse(url).path).name
        
        # Construct expected file path
        file_path = DOWNLOAD_ROOT / subfolder / filename
        
        # Check if file exists and has reasonable size
        if file_path.exists():
            file_size = file_path.stat().st_size
            min_size = config.get('downloader', {}).get('min_file_size_bytes', 100)
            return file_size >= min_size
            
        return False
        
    except Exception:
        return False

def download_with_retry(url: str, destination: Path, config: Dict[str, Any], 
                       logger: StructuredLoggerAdapter) -> bool:
    """
    Download a file with exponential backoff retry logic.
    
    Args:
        url: URL to download
        destination: Local file path to save to
        config: Dataset configuration dictionary
        logger: Structured logger instance
        
    Returns:
        True if download succeeded, False otherwise
    """
    max_retries = config.get('downloader', {}).get('max_retries', 3)
    base_delay = config.get('downloader', {}).get('retry_delay_seconds', 1)
    
    for attempt in range(max_retries + 1):
        try:
            logger.info(f"Downloading file (attempt {attempt + 1}/{max_retries + 1})", extra={
                "component": "DOWNLOAD",
                "structured_data": {
                    "url": url,
                    "destination": str(destination),
                    "attempt": attempt + 1,
                    "max_attempts": max_retries + 1
                }
            })
            
            # Ensure destination directory exists
            destination.parent.mkdir(parents=True, exist_ok=True)
            
            # Use the utility download function
            success = download_file(url, destination, config, logger)
            
            if success:
                logger.info("Download completed successfully", extra={
                    "component": "DOWNLOAD",
                    "structured_data": {
                        "url": url,
                        "destination": str(destination),
                        "attempt": attempt + 1,
                        "file_size": destination.stat().st_size if destination.exists() else 0
                    }
                })
                return True
            else:
                logger.warning(f"Download failed on attempt {attempt + 1}", extra={
                    "component": "DOWNLOAD",
                    "structured_data": {
                        "url": url,
                        "destination": str(destination),
                        "attempt": attempt + 1
                    }
                })
                
        except Exception as e:
            logger.warning(f"Download error on attempt {attempt + 1}: {str(e)}", extra={
                "component": "DOWNLOAD",
                "structured_data": {
                    "url": url,
                    "destination": str(destination),
                    "attempt": attempt + 1,
                    "error": str(e),
                    "error_type": type(e).__name__
                }
            })
        
        # Wait before retry (exponential backoff)
        if attempt < max_retries:
            delay = base_delay * (2 ** attempt)
            logger.info(f"Waiting {delay} seconds before retry", extra={
                "component": "DOWNLOAD",
                "structured_data": {
                    "url": url,
                    "delay_seconds": delay,
                    "next_attempt": attempt + 2
                }
            })
            time.sleep(delay)
    
    logger.error("Download failed after all retry attempts", extra={
        "component": "DOWNLOAD",
        "structured_data": {
            "url": url,
            "destination": str(destination),
            "total_attempts": max_retries + 1
        }
    })
    return False

def load_urls_from_jsonl(urls_file: Path, logger: StructuredLoggerAdapter, 
                        limit: Optional[int] = None) -> List[str]:
    """
    Load URLs from the JSONL file created by the crawler.
    
    Args:
        urls_file: Path to the dwd_urls.jsonl file
        logger: Structured logger instance
        limit: Optional limit on number of URLs to return (for testing)
        
    Returns:
        List of URLs to download
    """
    urls = []
    
    try:
        if not urls_file.exists():
            logger.error("URLs file not found", extra={
                "component": "DOWNLOAD",
                "structured_data": {
                    "urls_file": str(urls_file),
                    "file_exists": False
                }
            })
            return urls
        
        logger.info("Loading URLs from file", extra={
            "component": "DOWNLOAD",
            "structured_data": {
                "urls_file": str(urls_file),
                "limit": limit
            }
        })
        
        with open(urls_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    line = line.strip()
                    if not line:
                        continue
                        
                    # Parse JSON line
                    data = json.loads(line)
                    
                    # Extract URL (handle different possible formats)
                    url = None
                    if isinstance(data, dict):
                        url = data.get('url') or data.get('download_url')
                    elif isinstance(data, str):
                        url = data
                    
                    if url:
                        # Skip directory URLs - only process file URLs
                        if not is_valid_download_url(url):
                            logger.debug(f"Skipping directory URL: {url}", extra={
                                "component": "DOWNLOAD",
                                "structured_data": {
                                    "url": url,
                                    "reason": "directory_url_skipped"
                                }
                            })
                            continue
                        
                        urls.append(url)
                        
                        # Apply limit if specified
                        if limit and len(urls) >= limit:
                            logger.info(f"Reached URL limit of {limit}", extra={
                                "component": "DOWNLOAD",
                                "structured_data": {
                                    "limit": limit,
                                    "urls_loaded": len(urls)
                                }
                            })
                            break
                            
                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON on line {line_num}: {line}", extra={
                        "component": "DOWNLOAD",
                        "structured_data": {
                            "line_number": line_num,
                            "line_content": line[:100],
                            "json_error": str(e)
                        }
                    })
                    continue
                    
        logger.info(f"Loaded {len(urls)} URLs from file", extra={
            "component": "DOWNLOAD",
            "structured_data": {
                "urls_file": str(urls_file),
                "total_urls": len(urls),
                "limit_applied": limit is not None
            }
        })
        
    except Exception as e:
        logger.error(f"Failed to load URLs from file: {str(e)}", extra={
            "component": "DOWNLOAD",
            "structured_data": {
                "urls_file": str(urls_file),
                "error": str(e),
                "error_type": type(e).__name__
            }
        })
    
    return urls

def run_downloader(config: Dict[str, Any], logger: StructuredLoggerAdapter, 
                  limit: Optional[int] = None) -> ProcessingResult:
    """
    Main downloader function that orchestrates the download process.
    
    Args:
        config: Dataset configuration dictionary
        logger: Structured logger instance
        limit: Optional limit on number of files to download (for testing)
        
    Returns:
        ProcessingResult with download statistics and outcomes
    """
    start_time = time.time()
    
    logger.info("Starting DWD file downloader", extra={
        "component": "DOWNLOAD",
        "structured_data": {
            "limit": limit,
            "urls_file": str(URLS_FILE_PATH),
            "download_root": str(DOWNLOAD_ROOT)
        }
    })
    
    # Initialize result tracking
    files_processed = 0
    files_failed = 0
    files_skipped = 0
    output_files = []
    errors = []
    warnings = []
    
    try:
        # Load URLs from the crawler output
        urls = load_urls_from_jsonl(URLS_FILE_PATH, logger, limit)
        
        if not urls:
            error_msg = "No URLs found to download"
            logger.error(error_msg, extra={
                "component": "DOWNLOAD",
                "structured_data": {
                    "urls_file": str(URLS_FILE_PATH),
                    "urls_found": 0
                }
            })
            errors.append(error_msg)
            return ProcessingResult(
                success=False,
                files_processed=0,
                files_failed=0,
                output_files=[],
                errors=errors,
                metadata={"elapsed_time": time.time() - start_time},
                warnings=warnings
            )
        
        logger.info(f"Processing {len(urls)} URLs for download", extra={
            "component": "DOWNLOAD",
            "structured_data": {
                "total_urls": len(urls),
                "limit_applied": limit is not None
            }
        })
        
        # Process each URL
        for i, url in enumerate(urls, 1):
            try:
                logger.info(f"Processing URL {i}/{len(urls)}", extra={
                    "component": "DOWNLOAD",
                    "structured_data": {
                        "url": url,
                        "progress": f"{i}/{len(urls)}",
                        "percent_complete": round((i / len(urls)) * 100, 1)
                    }
                })
                
                # Determine subfolder for this download
                subfolder = parse_download_subfolder(url, config)
                
                # Check if file already exists
                if check_file_already_downloaded(url, subfolder, config):
                    filename = Path(urlparse(url).path).name
                    logger.info("File already downloaded, skipping", extra={
                        "component": "DOWNLOAD",
                        "structured_data": {
                            "url": url,
                            "filename": filename,
                            "subfolder": subfolder
                        }
                    })
                    files_skipped += 1
                    continue
                
                # Construct destination path
                filename = Path(urlparse(url).path).name
                destination = DOWNLOAD_ROOT / subfolder / filename
                
                # Download the file
                success = download_with_retry(url, destination, config, logger)
                
                if success:
                    files_processed += 1
                    output_files.append(destination)
                    logger.info("File downloaded successfully", extra={
                        "component": "DOWNLOAD",
                        "structured_data": {
                            "url": url,
                            "destination": str(destination),
                            "file_size": destination.stat().st_size if destination.exists() else 0
                        }
                    })
                else:
                    files_failed += 1
                    error_msg = f"Failed to download: {url}"
                    errors.append(error_msg)
                    logger.error(error_msg, extra={
                        "component": "DOWNLOAD",
                        "structured_data": {
                            "url": url,
                            "destination": str(destination)
                        }
                    })
                
            except Exception as e:
                files_failed += 1
                error_msg = f"Error processing URL {url}: {str(e)}"
                errors.append(error_msg)
                logger.error(error_msg, extra={
                    "component": "DOWNLOAD",
                    "structured_data": {
                        "url": url,
                        "error": str(e),
                        "error_type": type(e).__name__
                    }
                })
        
        # Calculate final statistics
        elapsed_time = time.time() - start_time
        total_processed = files_processed + files_failed + files_skipped
        
        logger.info("Download process completed", extra={
            "component": "DOWNLOAD",
            "structured_data": {
                "total_urls": len(urls),
                "files_processed": files_processed,
                "files_failed": files_failed,
                "files_skipped": files_skipped,
                "elapsed_time": elapsed_time,
                "success_rate": round((files_processed / max(total_processed - files_skipped, 1)) * 100, 1)
            }
        })
        
        # Add warnings if appropriate
        if files_skipped > 0:
            warnings.append(f"{files_skipped} files were skipped (already downloaded)")
        
        if files_failed > 0:
            warnings.append(f"{files_failed} files failed to download")
        
        # Determine overall success
        success = files_failed == 0 and files_processed > 0
        
        return ProcessingResult(
            success=success,
            files_processed=files_processed,
            files_failed=files_failed,
            output_files=output_files,
            errors=errors,
            metadata={
                "elapsed_time": elapsed_time,
                "files_skipped": files_skipped,
                "total_urls": len(urls),
                "download_root": str(DOWNLOAD_ROOT),
                "success_rate": round((files_processed / max(total_processed - files_skipped, 1)) * 100, 1) if total_processed > files_skipped else 0
            },
            warnings=warnings
        )
        
    except Exception as e:
        elapsed_time = time.time() - start_time
        error_msg = f"Downloader failed with unexpected error: {str(e)}"
        logger.error(error_msg, extra={
            "component": "DOWNLOAD",
            "structured_data": {
                "error": str(e),
                "error_type": type(e).__name__,
                "elapsed_time": elapsed_time
            }
        })
        errors.append(error_msg)
        
        return ProcessingResult(
            success=False,
            files_processed=files_processed,
            files_failed=files_failed,
            output_files=output_files,
            errors=errors,
            metadata={
                "elapsed_time": elapsed_time,
                "fatal_error": str(e)
            },
            warnings=warnings
        )
