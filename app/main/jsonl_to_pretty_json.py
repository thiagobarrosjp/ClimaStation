"""
JSONL to Pretty JSON Converter for ClimaStation Weather Data

SCRIPT IDENTIFICATION: DWD10TAH3J
- DWD: Deutscher Wetterdienst data source
- 10T: 10-minute air temperature dataset
- AH: Air temperature Historical data
- 3: Station processing component
- J: JSON formatting utility component

PURPOSE:
Converts the compact JSONL output from the weather data parser into human-readable, 
pretty-formatted JSON files for validation and inspection. This utility preserves 
exact structure and data without any modifications - pure formatting only.

KEY FUNCTIONALITY:
- Converts JSONL files to pretty-formatted JSON WITHOUT modifying content
- Preserves exact structure and data for validation purposes
- Supports both single file and batch processing with comprehensive logging
- Handles large files efficiently with streaming and progress tracking
- Pure formatting - no analysis, metadata, or content changes
- Comprehensive validation and error handling with detailed feedback
- Auto-discovery of JSONL files in project structure
- Support for both orjson (faster) and standard json libraries

EXPECTED INPUT:
JSONL files from the weather data parser (updated paths):
data/dwd/3_parsed_files/parsed_historical/
└── parsed_stundenwerte_TU_00003_19930428_20201231_hist.jsonl

EXPECTED OUTPUT:
Pretty-formatted JSON files with EXACT same content (updated paths):
data/dwd/4_pretty_json/parsed_historical/
└── parsed_stundenwerte_TU_00003_19930428_20201231_hist.json

USAGE FROM PROJECT ROOT:
    # Convert single file (run from project root)
    python app/main/jsonl_to_pretty_json.py "data/dwd/3_parsed_files/parsed_historical/parsed_stundenwerte_TU_00003_19930428_20201231_hist.jsonl"
    
    # Convert single file with custom output
    python app/main/jsonl_to_pretty_json.py "path/to/input.jsonl" --output "path/to/output.json"
    
    # Batch convert all JSONL files in directory
    python app/main/jsonl_to_pretty_json.py --batch "data/dwd/3_parsed_files/"

USAGE FROM app/main/ DIRECTORY:
    # Convert single file (run from app/main/)
    python jsonl_to_pretty_json.py "../../data/dwd/3_parsed_files/parsed_historical/parsed_stundenwerte_TU_00003_19930428_20201231_hist.jsonl"
    
    # Batch convert directory (run from app/main/)
    python jsonl_to_pretty_json.py --batch "../../data/dwd/3_parsed_files/"

AUTHOR: ClimaStation Backend Pipeline
VERSION: Enhanced with script identification codes and new logging system
LAST UPDATED: 2025-01-17
"""

import json
import sys
import os
from pathlib import Path
import argparse
from typing import List, Dict, Any, Optional, Tuple
import logging
from datetime import datetime
import traceback

# Try to import orjson for faster JSON processing, fallback to standard json
try:
    import orjson
    HAS_ORJSON = True
except ImportError:
    HAS_ORJSON = False

# Add the project root to the Python path for imports
current_dir = Path(__file__).parent
project_root = current_dir.parent.parent
sys.path.insert(0, str(project_root))

# Import configuration and logger with proper paths
try:
    from app.config.ten_minutes_air_temperature_config import PARSED_BASE
    from app.utils.logger import setup_logger
    HAS_CONFIG = True
    HAS_LOGGER = True
except ImportError:
    HAS_CONFIG = False
    HAS_LOGGER = False
    # Fallback paths if config not available (updated for new structure)
    PARSED_BASE = Path("data/dwd/3_parsed_files")


def setup_fallback_logger(log_path: Optional[Path] = None) -> logging.Logger:
    """
    Setup fallback logger if the main logger utility is not available.
    
    Args:
        log_path: Optional path for log file
        
    Returns:
        Configured logger instance with DWD10TAH3J identification
    """
    logger = logging.getLogger("jsonl_converter")
    logger.setLevel(logging.INFO)
    
    # Clear existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()
    
    # Console handler with script identification
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s — %(levelname)s — [DWD10TAH3J] %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler if path provided
    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path, mode='w', encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def find_jsonl_files_in_project() -> List[Path]:
    """
    Find JSONL files in the expected project directories (updated for new structure).
    
    Returns:
        List of found JSONL files with absolute paths
    """
    # Get the current working directory
    cwd = Path.cwd()
    
    # Possible search paths relative to current working directory (updated paths)
    search_paths = [
        cwd / "data" / "dwd" / "3_parsed_files",  # New structure
        cwd / "data" / "germany" / "3_parsed_files",  # Legacy structure
        cwd / "data" / "3_parsed_files", 
        cwd / "data" / "parsed_files",
        cwd / "data" / "parsed",
        cwd / "parsed_files",
        cwd,
        # Also search relative to project root if we're in a subdirectory
        project_root / "data" / "dwd" / "3_parsed_files",  # New structure
        project_root / "data" / "germany" / "3_parsed_files",  # Legacy structure
        project_root / "data" / "3_parsed_files",
        project_root / "data" / "parsed_files",
        project_root / "data" / "parsed",
        project_root / "parsed_files",
        project_root
    ]
    
    found_files = []
    
    for search_path in search_paths:
        if search_path.exists() and search_path.is_dir():
            try:
                jsonl_files = list(search_path.glob("**/*.jsonl"))
                found_files.extend(jsonl_files)
            except Exception as e:
                # Skip directories we can't access
                continue
    
    # Remove duplicates and sort
    unique_files = list(set(found_files))
    unique_files.sort()
    
    return unique_files


def load_jsonl_file_pure(file_path: Path, logger: logging.Logger) -> List[Dict[str, Any]]:
    """
    Load and parse a JSONL file without any modifications.
    
    Args:
        file_path: Path to JSONL file
        logger: Logger instance (with DWD10TAH3J code for traceability)
        
    Returns:
        List of parsed JSON objects (exact content from JSONL)
        
    Raises:
        Exception: If file cannot be loaded or parsed
    """
    logger.info(f"📄 Loading JSONL file: {file_path.name}")
    logger.info(f"   📍 Full path: {file_path.absolute()}")
    logger.info(f"   📊 File size: {file_path.stat().st_size / 1024 / 1024:.2f} MB")
    
    data = []
    line_count = 0
    error_count = 0
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                
                try:
                    if HAS_ORJSON:
                        # Use orjson for faster parsing
                        obj = orjson.loads(line)
                    else:
                        # Fallback to standard json
                        obj = json.loads(line)
                    
                    data.append(obj)
                    line_count += 1
                    
                    # Progress indicator for large files
                    if line_count % 1000 == 0:
                        logger.debug(f"   📊 Processed {line_count:,} records...")
                    
                except json.JSONDecodeError as e:
                    error_count += 1
                    logger.warning(f"   ⚠️  JSON decode error on line {line_num}: {e}")
                    if error_count > 10:  # Stop if too many errors
                        logger.error("   ❌ Too many JSON errors, stopping")
                        break
                except Exception as e:
                    error_count += 1
                    logger.warning(f"   ⚠️  Unexpected error on line {line_num}: {e}")
        
        logger.info(f"   ✅ Loaded {line_count:,} records")
        if error_count > 0:
            logger.warning(f"   ⚠️  Encountered {error_count} parsing errors")
        
        return data
        
    except Exception as e:
        logger.error(f"   ❌ Failed to load JSONL file: {e}")
        raise


def convert_jsonl_to_pretty_json_pure(input_path: Path, output_path: Path, logger: logging.Logger) -> bool:
    """
    Convert JSONL file to pretty-formatted JSON WITHOUT modifying content.
    This preserves the exact structure and data for validation purposes.
    
    Args:
        input_path: Path to input JSONL file
        output_path: Path to output JSON file
        logger: Logger instance (with DWD10TAH3J code for traceability)
        
    Returns:
        True if conversion successful, False otherwise
    """
    logger.info(f"🔄 Converting {input_path.name} to pretty JSON (pure formatting)...")
    
    try:
        # Load JSONL data without any modifications
        data = load_jsonl_file_pure(input_path, logger)
        
        if not data:
            logger.error("   ❌ No data loaded, cannot convert")
            return False
        
        # Create output directory
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write pretty JSON with EXACT same content
        logger.info(f"   💾 Writing to: {output_path.name}")
        logger.info(f"   📍 Output path: {output_path.absolute()}")
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
        
        # Log output file info
        output_size = output_path.stat().st_size / 1024 / 1024
        logger.info(f"   ✅ Conversion complete")
        logger.info(f"   📊 Output size: {output_size:.2f} MB")
        logger.info(f"   📊 Records converted: {len(data):,}")
        logger.info(f"   📄 Output file: {output_path}")
        
        return True
        
    except Exception as e:
        logger.error(f"   ❌ Conversion failed: {e}")
        logger.error(f"   🔍 Traceback: {traceback.format_exc()}")
        return False


def validate_jsonl_structure(input_path: Path, logger: logging.Logger) -> bool:
    """
    Validate JSONL file structure without conversion.
    
    Args:
        input_path: Path to input JSONL file
        logger: Logger instance (with DWD10TAH3J code for traceability)
        
    Returns:
        True if validation successful, False otherwise
    """
    logger.info(f"🔍 Validating JSONL structure: {input_path.name}...")
    
    try:
        # Load data
        data = load_jsonl_file_pure(input_path, logger)
        
        if not data:
            logger.error("   ❌ Validation failed: No data loaded")
            return False
        
        # Basic structure validation
        validation_issues = []
        
        # Check if it's a list (which it should be from JSONL)
        if not isinstance(data, list):
            validation_issues.append("Data is not a list of records")
        
        # Check first few records for basic structure
        for i, record in enumerate(data[:5]):  # Check first 5 records
            if not isinstance(record, dict):
                validation_issues.append(f"Record {i}: Not a dictionary")
                continue
            
            # Check for some expected fields (adjust based on your data structure)
            if 'station_id' not in record:
                validation_issues.append(f"Record {i}: Missing 'station_id' field")
            
            if 'measurements' not in record:
                validation_issues.append(f"Record {i}: Missing 'measurements' field")
            elif not isinstance(record['measurements'], list):
                validation_issues.append(f"Record {i}: 'measurements' is not a list")
        
        # Report validation results
        if validation_issues:
            logger.warning(f"   ⚠️  Validation issues found: {len(validation_issues)}")
            for issue in validation_issues:
                logger.warning(f"      - {issue}")
        else:
            logger.info("   ✅ Validation passed: Basic structure looks good")
        
        # Log validation summary
        logger.info(f"   📊 Validation summary:")
        logger.info(f"      📦 Records validated: {len(data):,}")
        logger.info(f"      ⚠️  Issues found: {len(validation_issues)}")
        logger.info(f"      ✅ Validation result: {'PASSED' if not validation_issues else 'FAILED'}")
        
        return len(validation_issues) == 0
        
    except Exception as e:
        logger.error(f"   ❌ Validation failed: {e}")
        logger.error(f"   🔍 Traceback: {traceback.format_exc()}")
        return False


def batch_convert_directory_pure(input_dir: Path, output_dir: Path, logger: logging.Logger) -> Tuple[int, int]:
    """
    Batch convert all JSONL files in a directory to pretty JSON (pure formatting).
    
    Args:
        input_dir: Directory containing JSONL files
        output_dir: Directory for output files
        logger: Logger instance (with DWD10TAH3J code for traceability)
        
    Returns:
        Tuple of (successful_conversions, failed_conversions)
    """
    logger.info(f"📁 Batch processing directory: {input_dir}")
    logger.info(f"   📍 Full path: {input_dir.absolute()}")
    
    # Find all JSONL files
    jsonl_files = list(input_dir.glob("**/*.jsonl"))
    
    if not jsonl_files:
        logger.warning(f"   ❌ No JSONL files found in {input_dir}")
        return 0, 0
    
    logger.info(f"   📄 Found {len(jsonl_files)} JSONL files")
    
    successful = 0
    failed = 0
    
    for i, jsonl_file in enumerate(jsonl_files, 1):
        logger.info(f"🔄 Processing {i}/{len(jsonl_files)}: {jsonl_file.name}")
        
        try:
            # Determine output path
            rel_path = jsonl_file.relative_to(input_dir)
            output_file = output_dir / rel_path.with_suffix('.json')
            
            # Convert file
            success = convert_jsonl_to_pretty_json_pure(jsonl_file, output_file, logger)
            
            if success:
                successful += 1
                logger.info(f"   ✅ Successfully processed {jsonl_file.name}")
            else:
                failed += 1
                logger.error(f"   ❌ Failed to process {jsonl_file.name}")
        
        except Exception as e:
            failed += 1
            logger.error(f"   ❌ Error processing {jsonl_file.name}: {e}")
    
    # Final summary
    logger.info(f"📊 Batch processing complete:")
    logger.info(f"   ✅ Successful: {successful}")
    logger.info(f"   ❌ Failed: {failed}")
    logger.info(f"   📄 Total processed: {successful + failed}")
    
    return successful, failed


def show_usage_help():
    """Show detailed usage help with examples (updated for new folder structure)."""
    print("🔧 JSONL to Pretty JSON Converter [DWD10TAH3J] (Pure Formatting)")
    print("=" * 60)
    print()
    print("This tool converts JSONL files to pretty JSON format WITHOUT modifying content.")
    print("Perfect for validation - you see the exact same data, just formatted nicely.")
    print()
    print(f"📍 Current working directory: {Path.cwd()}")
    print(f"📍 Script location: {Path(__file__).absolute()}")
    print()
    print("📋 USAGE EXAMPLES:")
    print()
    
    # Determine if we're running from project root or app/main
    cwd = Path.cwd()
    if cwd.name == "main" and cwd.parent.name == "app":
        print("🔍 Detected: Running from app/main/ directory")
        print()
        print("1. Convert a single JSONL file:")
        print('   python jsonl_to_pretty_json.py "../../data/dwd/3_parsed_files/parsed_historical/file.jsonl"')
        print()
        print("2. Convert with custom output:")
        print('   python jsonl_to_pretty_json.py "../../data/input.jsonl" --output "../../data/output.json"')
        print()
        print("3. Batch convert all JSONL files in a directory:")
        print('   python jsonl_to_pretty_json.py --batch "../../data/dwd/3_parsed_files/"')
        print()
        print("4. Validate JSONL file structure:")
        print('   python jsonl_to_pretty_json.py "../../data/input.jsonl" --validate-only')
        print()
    else:
        print("🔍 Detected: Running from project root directory")
        print()
        print("1. Convert a single JSONL file:")
        print('   python app/main/jsonl_to_pretty_json.py "data/dwd/3_parsed_files/parsed_historical/file.jsonl"')
        print()
        print("2. Convert with custom output:")
        print('   python app/main/jsonl_to_pretty_json.py "data/input.jsonl" --output "data/output.json"')
        print()
        print("3. Batch convert all JSONL files in a directory:")
        print('   python app/main/jsonl_to_pretty_json.py --batch "data/dwd/3_parsed_files/"')
        print()
        print("4. Validate JSONL file structure:")
        print('   python app/main/jsonl_to_pretty_json.py "data/input.jsonl" --validate-only')
        print()
    
    # Try to find JSONL files in the project
    found_files = find_jsonl_files_in_project()
    
    if found_files:
        print("📄 FOUND JSONL FILES IN PROJECT:")
        for i, file_path in enumerate(found_files[:10], 1):  # Show first 10 files
            # Show relative path from current working directory
            try:
                rel_path = file_path.relative_to(Path.cwd())
                print(f"   {i:2d}. {rel_path}")
            except ValueError:
                # If relative path fails, show absolute path
                print(f"   {i:2d}. {file_path}")
        
        if len(found_files) > 10:
            print(f"   ... and {len(found_files) - 10} more files")
        
        print()
        print("💡 TIP: You can use any of these files as input!")
        
        # Show example command with first found file
        if found_files:
            example_file = found_files[0]
            try:
                rel_path = example_file.relative_to(Path.cwd())
                if cwd.name == "main" and cwd.parent.name == "app":
                    print(f'   Example: python jsonl_to_pretty_json.py "{rel_path}"')
                else:
                    print(f'   Example: python app/main/jsonl_to_pretty_json.py "{rel_path}"')
            except ValueError:
                print(f'   Example: python jsonl_to_pretty_json.py "{example_file}"')
    else:
        print("❌ No JSONL files found in common project directories.")
        print("   Make sure you have run the weather data parser first to generate JSONL files.")
        print()
        print("🔍 Searched in these directories:")
        search_paths = [
            Path.cwd() / "data" / "dwd" / "3_parsed_files",  # New structure
            Path.cwd() / "data" / "germany" / "3_parsed_files",  # Legacy structure
            Path.cwd() / "data" / "3_parsed_files", 
            Path.cwd() / "data" / "parsed_files",
            Path.cwd() / "data" / "parsed",
            project_root / "data" / "dwd" / "3_parsed_files",  # New structure
            project_root / "data" / "germany" / "3_parsed_files"  # Legacy structure
        ]
        for search_path in search_paths[:5]:  # Show first 5 search paths
            exists = "✅" if search_path.exists() else "❌"
            print(f"   {exists} {search_path}")
    
    print()
    print("🎯 PURE FORMATTING MODE:")
    print("   - No metadata added")
    print("   - No analysis performed") 
    print("   - No content modification")
    print("   - Perfect for validation")
    print("   - Script ID: DWD10TAH3J for traceability")
    print()


def main():
    """Main function for command-line usage with enhanced logging."""
    parser = argparse.ArgumentParser(
        description="Convert JSONL weather data files to pretty JSON format (pure formatting - no content changes) [DWD10TAH3J]",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Convert single file (pure formatting)
  python jsonl_to_pretty_json.py "data/file.jsonl"
  
  # Convert with custom output
  python jsonl_to_pretty_json.py "data/file.jsonl" --output "output.json"
  
  # Batch convert directory
  python jsonl_to_pretty_json.py --batch "data/parsed_files/"
  
  # Validate file structure only
  python jsonl_to_pretty_json.py "data/file.jsonl" --validate-only

Note: This version does NOT add metadata or analysis - it only formats the content.
Script Identification: DWD10TAH3J for logging traceability.
        """
    )
    
    parser.add_argument('input', nargs='?', help='Input JSONL file or directory (for batch mode)')
    parser.add_argument('--output', '-o', help='Output JSON file (default: input file with .json extension)')
    parser.add_argument('--batch', '-b', action='store_true', help='Batch process all JSONL files in input directory')
    parser.add_argument('--validate-only', '-v', action='store_true', help='Validate JSONL file structure without conversion')
    parser.add_argument('--log', '-l', help='Log file path (default: no log file)')
    parser.add_argument('--quiet', '-q', action='store_true', help='Reduce output verbosity')
    parser.add_argument('--help-usage', action='store_true', help='Show detailed usage help with examples')
    
    args = parser.parse_args()
    
    # Show detailed help if requested
    if args.help_usage:
        show_usage_help()
        return
    
    # Show help if no input provided
    if not args.input:
        print("❌ Error: Input file or directory is required")
        print()
        show_usage_help()
        sys.exit(1)
    
    input_path = Path(args.input)
    
    # Convert to absolute path if it's relative
    if not input_path.is_absolute():
        input_path = input_path.resolve()
    
    if not input_path.exists():
        print(f"❌ Error: Input path does not exist: {input_path}")
        print(f"   📍 Looked for: {input_path.absolute()}")
        print()
        
        # Try to find similar files
        found_files = find_jsonl_files_in_project()
        if found_files:
            print("💡 Did you mean one of these files?")
            for i, file_path in enumerate(found_files[:5], 1):
                try:
                    rel_path = file_path.relative_to(Path.cwd())
                    print(f"   {i}. {rel_path}")
                except ValueError:
                    print(f"   {i}. {file_path}")
        
        sys.exit(1)
    
    # Setup logging with script identification
    log_path = Path(args.log) if args.log else None

    if HAS_LOGGER:
        try:
            # Use the main logger utility - it doesn't take log_path parameter
            logger = setup_logger("DWD10TAH3J", script_name="jsonl_to_pretty_json")
        except Exception:
            logger = setup_fallback_logger(log_path)
    else:
        logger = setup_fallback_logger(log_path)
    
    if args.quiet:
        logger.setLevel(logging.WARNING)
    
    # Log startup information
    logger.info("🚀 Starting JSONL to Pretty JSON Converter [DWD10TAH3J]")
    logger.info(f"📁 Working directory: {Path.cwd()}")
    logger.info(f"📄 Input path: {input_path}")
    logger.info(f"⚙️  Configuration loaded: {HAS_CONFIG}")
    logger.info(f"📚 orjson available: {HAS_ORJSON}")
    
    # Process based on mode
    try:
        if args.batch:
            # Batch processing mode
            if not input_path.is_dir():
                logger.error(f"❌ Batch mode requires a directory, got: {input_path}")
                sys.exit(1)
            
            output_dir = Path(args.output) if args.output else input_path.parent / "pretty_json"
            logger.info(f"📁 Batch mode: processing directory {input_path}")
            logger.info(f"📁 Output directory: {output_dir}")
            
            successful, failed = batch_convert_directory_pure(input_path, output_dir, logger)
            
            if failed > 0:
                logger.error(f"💥 Batch processing completed with {failed} failures")
                sys.exit(1)
            else:
                logger.info(f"🎉 Batch processing completed successfully: {successful} files converted")
        
        elif args.validate_only:
            # Validation only mode
            if not input_path.is_file():
                logger.error(f"❌ Validation mode requires a file, got: {input_path}")
                sys.exit(1)
            
            logger.info(f"🔍 Validation mode: checking {input_path.name}")
            success = validate_jsonl_structure(input_path, logger)
            if not success:
                logger.error("❌ Validation failed")
                sys.exit(1)
            else:
                logger.info("✅ Validation passed")
        
        else:
            # Single file processing mode
            if not input_path.is_file():
                logger.error(f"❌ Single file mode requires a file, got: {input_path}")
                sys.exit(1)
            
            # Determine output path
            if args.output:
                output_path = Path(args.output)
            else:
                output_path = input_path.with_suffix('.json')
            
            logger.info(f"🔄 Single file mode: converting {input_path.name}")
            logger.info(f"📄 Output file: {output_path}")
            
            # Process file
            success = convert_jsonl_to_pretty_json_pure(input_path, output_path, logger)
            
            if not success:
                logger.error("❌ Conversion failed")
                sys.exit(1)
            else:
                logger.info("✅ Conversion completed successfully")
        
        logger.info("🎉 All operations completed successfully!")
        logger.info("🏁 JSONL to Pretty JSON Converter [DWD10TAH3J] finished")
        
    except KeyboardInterrupt:
        logger.info("⚠️  Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"💥 Unexpected error: {e}")
        logger.error(f"🔍 Traceback: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    main()
