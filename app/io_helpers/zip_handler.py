"""
ZIP File Handler Utility

This utility extracts text files from ZIP archives for weather data processing.
Used to extract both raw measurement data and metadata files from German weather station archives.

Expected Input:
- ZIP files containing .txt files with weather data or metadata
- Extraction destination folder path

Expected Output:
- Extracted .txt files in specified destination folder
- List of Path objects pointing to extracted files

Usage:
- Called by raw_parser.py to extract data and metadata files
- Handles both raw data ZIPs and metadata ZIPs
"""

import zipfile
from pathlib import Path
from typing import List


def extract_txt_files_from_zip(zip_path: Path, extract_to: Path) -> List[Path]:
    """
    Extract all .txt files from a ZIP archive to specified folder.
    
    Args:
        zip_path: Path to the ZIP file to extract from
        extract_to: Destination folder for extracted files
        
    Returns:
        List of Path objects pointing to extracted .txt files
        
    Raises:
        zipfile.BadZipFile: If the ZIP file is corrupted
        FileNotFoundError: If the ZIP file doesn't exist
        PermissionError: If unable to write to extraction folder
    """
    if not zip_path.exists():
        raise FileNotFoundError(f"ZIP file not found: {zip_path}")
    
    # Ensure extraction directory exists
    extract_to.mkdir(parents=True, exist_ok=True)
    
    extracted_files = []
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Filter for .txt files only
            txt_members = [f for f in zip_ref.namelist() if f.endswith(".txt")]
            
            if not txt_members:
                return []
            
            # Extract only .txt files
            zip_ref.extractall(extract_to, txt_members)
            
            # Return list of extracted file paths
            extracted_files = [extract_to / member for member in txt_members]
            
    except zipfile.BadZipFile as e:
        raise zipfile.BadZipFile(f"Corrupted ZIP file: {zip_path}") from e
    except Exception as e:
        raise RuntimeError(f"Failed to extract files from {zip_path}: {e}") from e
    
    return extracted_files
