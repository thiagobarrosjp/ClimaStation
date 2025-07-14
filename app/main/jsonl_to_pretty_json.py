"""
JSONL to Pretty JSON Converter - Complete Version with Fixed Types

This script converts JSONL (JSON Lines) output files from the weather data parser
into pretty-formatted JSON files for easier human reading and debugging.
This version preserves ALL data fields from the original JSONL.

Expected Input:
- JSONL files from the weather data processing pipeline
- Each line contains a complete JSON object with weather station data

Expected Output:
- Pretty-formatted JSON files with proper indentation
- Same filename with '_pretty.json' suffix
- Preserves ALL data structure and content (not just measurements)

Usage:
    python jsonl_to_pretty_json.py [input_file.jsonl]
"""

import json
import sys
from pathlib import Path
import argparse
from typing import List, Dict, Any, Optional

# Try to import orjson, fallback to json if not available
try:
    import orjson
    HAS_ORJSON = True
except ImportError:
    HAS_ORJSON = False

# Try to import config, provide fallback if not available
try:
    from config.ten_minutes_air_temperature_config import PARSED_BASE
    from utils.logger import setup_logger
    HAS_CONFIG = True
except ImportError:
    HAS_CONFIG = False
    # Fallback paths
    PARSED_BASE = Path("data/germany/3_parsed_files/parsed_10_minutes/parsed_air_temperature")


def setup_fallback_logger(log_path: Path):
    """Fallback logger setup if utils.logger is not available."""
    import logging
    
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    logger = logging.getLogger("jsonl_converter")
    logger.setLevel(logging.DEBUG)
    
    if not logger.handlers:
        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s — %(levelname)s — %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        # Also add console handler for immediate feedback
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    return logger


def load_jsonl_file(jsonl_path: Path, logger) -> List[Dict[Any, Any]]:
    """
    Load all JSON objects from a JSONL file.
    
    Args:
        jsonl_path: Path to the JSONL file
        logger: Logger instance for debugging
        
    Returns:
        List of dictionaries, one per line in the JSONL file
    """
    if not jsonl_path.exists():
        raise FileNotFoundError(f"JSONL file not found: {jsonl_path}")
    
    data_objects = []
    
    logger.info(f"📖 Reading JSONL file: {jsonl_path.name}")
    
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:  # Skip empty lines
                continue
                
            try:
                # Parse JSON line
                if HAS_ORJSON:
                    try:
                        obj = orjson.loads(line)
                    except:
                        obj = json.loads(line)
                else:
                    obj = json.loads(line)
                
                # Debug: Log what we're loading
                if line_num <= 3:  # Log first 3 objects for debugging
                    logger.info(f"📋 Line {line_num} keys: {list(obj.keys()) if isinstance(obj, dict) else 'Not a dict'}")
                    if isinstance(obj, dict):
                        logger.info(f"   Station ID: {obj.get('station_id', 'Missing')}")
                        logger.info(f"   Measurements count: {len(obj.get('measurements', []))}")
                        logger.info(f"   Sensors count: {len(obj.get('sensors', []))}")
                
                data_objects.append(obj)
                
            except json.JSONDecodeError as e:
                logger.error(f"❌ JSON decode error on line {line_num}: {e}")
                logger.error(f"   Line content: {line[:100]}...")
                continue
            except Exception as e:
                logger.error(f"❌ Unexpected error on line {line_num}: {e}")
                continue
    
    logger.info(f"✅ Loaded {len(data_objects)} JSON objects from {jsonl_path.name}")
    return data_objects


def save_pretty_json(data: List[Dict[Any, Any]], output_path: Path, logger) -> None:
    """
    Save data as pretty-formatted JSON file.
    
    Args:
        data: List of dictionaries to save
        output_path: Path where to save the pretty JSON file
        logger: Logger instance
    """
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"💾 Saving pretty JSON to: {output_path.name}")
    
    # Debug: Check what we're about to save
    if data:
        sample_obj = data[0]
        logger.info(f"📋 Sample object keys: {list(sample_obj.keys()) if isinstance(sample_obj, dict) else 'Not a dict'}")
        
        # Check for specific fields
        if isinstance(sample_obj, dict):
            logger.info(f"   Has station_metadata: {'station_metadata' in sample_obj}")
            logger.info(f"   Has sensors: {'sensors' in sample_obj}")
            logger.info(f"   Has measurements: {'measurements' in sample_obj}")
            logger.info(f"   Has time_range: {'time_range' in sample_obj}")
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False, sort_keys=False)  # Don't sort keys to preserve order
    
    logger.info(f"✅ Pretty JSON saved successfully")


def convert_jsonl_to_pretty(jsonl_path: Path, logger, output_dir: Optional[Path] = None) -> Optional[Path]:
    """
    Convert a single JSONL file to pretty JSON format.
    
    Args:
        jsonl_path: Path to input JSONL file
        logger: Logger instance
        output_dir: Optional custom output directory
        
    Returns:
        Path to the created pretty JSON file, or None if conversion failed
    """
    logger.info(f"🔄 Converting {jsonl_path.name}")
    
    try:
        # Load JSONL data
        data = load_jsonl_file(jsonl_path, logger)
        
        if not data:
            logger.warning(f"⚠️  No data loaded from {jsonl_path.name}")
            return None
        
        logger.info(f"📊 Loaded {len(data)} JSON objects")
        
        # Create output path
        if output_dir:
            output_path = output_dir / f"{jsonl_path.stem}_pretty.json"
        else:
            output_path = jsonl_path.parent / f"{jsonl_path.stem}_pretty.json"
        
        # Save as pretty JSON
        save_pretty_json(data, output_path, logger)
        
        # Calculate file sizes for comparison
        input_size = jsonl_path.stat().st_size
        output_size = output_path.stat().st_size
        
        logger.info(f"✅ Created {output_path.name}")
        logger.info(f"📏 Size: {input_size:,} bytes → {output_size:,} bytes")
        
        return output_path
        
    except Exception as e:
        logger.error(f"❌ Failed to convert {jsonl_path.name}: {e}")
        import traceback
        logger.error(f"   Traceback: {traceback.format_exc()}")
        raise


def find_jsonl_files(base_path: Path) -> List[Path]:
    """
    Find all JSONL files in the parsed output directory structure.
    
    Args:
        base_path: Base directory to search in
        
    Returns:
        List of JSONL file paths
    """
    jsonl_files = []
    
    if base_path.exists():
        # Search recursively for .jsonl files
        jsonl_files = list(base_path.rglob("*.jsonl"))
    
    return sorted(jsonl_files)


def main():
    """
    Main function to convert JSONL files to pretty JSON format.
    """
    parser = argparse.ArgumentParser(
        description="Convert JSONL files to pretty-formatted JSON (preserves ALL fields)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Convert specific file
    python jsonl_to_pretty_json.py data/output/parsed_station_12345.jsonl
    
    # Convert all JSONL files in parsed directory
    python jsonl_to_pretty_json.py
    
    # Convert all files in specific directory
    python jsonl_to_pretty_json.py --directory data/custom/path/
        """
    )
    
    parser.add_argument(
        'input_file', 
        nargs='?', 
        help='Specific JSONL file to convert (optional)'
    )
    
    parser.add_argument(
        '--directory', '-d',
        type=Path,
        help='Directory to search for JSONL files (default: parsed output directory)'
    )
    
    parser.add_argument(
        '--output-dir', '-o',
        type=Path,
        help='Output directory for pretty JSON files (default: same as input)'
    )
    
    args = parser.parse_args()
    
    # Set up logging (with fallback)
    log_path = Path("data/germany/0_debug/jsonl_to_pretty_json.debug.log")
    if HAS_CONFIG:
        logger = setup_logger(log_path)
    else:
        logger = setup_fallback_logger(log_path)
        logger.warning("⚠️  Using fallback logger - config module not found")
    
    logger.info("🚀 Starting JSONL to Pretty JSON conversion")
    
    try:
        if args.input_file:
            # Convert specific file
            input_path = Path(args.input_file)
            if not input_path.exists():
                logger.error(f"❌ Input file not found: {input_path}")
                print(f"❌ Input file not found: {input_path}")
                sys.exit(1)
            
            if not input_path.suffix == '.jsonl':
                logger.error(f"❌ Input file must have .jsonl extension: {input_path}")
                print(f"❌ Input file must have .jsonl extension: {input_path}")
                sys.exit(1)
            
            jsonl_files = [input_path]
            
        else:
            # Find all JSONL files
            search_dir = args.directory or PARSED_BASE
            logger.info(f"🔍 Searching for JSONL files in: {search_dir}")
            print(f"🔍 Searching for JSONL files in: {search_dir}")
            
            jsonl_files = find_jsonl_files(search_dir)
            
            if not jsonl_files:
                logger.warning(f"⚠️  No JSONL files found in {search_dir}")
                print(f"⚠️  No JSONL files found in {search_dir}")
                print("💡 Try specifying a specific file or directory with --directory")
                return
        
        logger.info(f"📦 Found {len(jsonl_files)} JSONL file(s) to convert")
        print(f"📦 Found {len(jsonl_files)} JSONL file(s) to convert")
        
        # Convert each file
        converted_count = 0
        error_count = 0
        
        for i, jsonl_path in enumerate(jsonl_files, 1):
            print(f"🔄 Processing {i}/{len(jsonl_files)}: {jsonl_path.name}")
            logger.info(f"🔄 Processing {i}/{len(jsonl_files)}: {jsonl_path.name}")
            
            try:
                result = convert_jsonl_to_pretty(jsonl_path, logger, args.output_dir)
                if result:
                    converted_count += 1
                    print(f"✅ Created: {result.name}")
                else:
                    error_count += 1
                    print(f"⚠️  No data to convert in: {jsonl_path.name}")
                
            except Exception as e:
                error_count += 1
                logger.error(f"❌ Failed to convert {jsonl_path.name}: {e}")
                print(f"❌ Failed to convert {jsonl_path.name}: {e}")
                continue
        
        # Final summary
        print("=" * 60)
        print("📊 CONVERSION SUMMARY")
        print(f"📦 Total files found: {len(jsonl_files)}")
        print(f"✅ Successfully converted: {converted_count}")
        print(f"❌ Failed to convert: {error_count}")
        
        logger.info("=" * 60)
        logger.info("📊 CONVERSION SUMMARY")
        logger.info(f"📦 Total files found: {len(jsonl_files)}")
        logger.info(f"✅ Successfully converted: {converted_count}")
        logger.info(f"❌ Failed to convert: {error_count}")
        
        if error_count == 0:
            print("🎉 All files converted successfully!")
            logger.info("🎉 All files converted successfully!")
        elif converted_count > 0:
            print(f"⚠️  Conversion completed with {error_count} errors")
            logger.warning(f"⚠️  Conversion completed with {error_count} errors")
        else:
            print("💥 No files were converted successfully")
            logger.error("💥 No files were converted successfully")
        
        print("🏁 Conversion complete")
        logger.info("🏁 Conversion complete")
        
    except KeyboardInterrupt:
        print("⏹️  Conversion interrupted by user")
        logger.info("⏹️  Conversion interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"💥 Unexpected error: {e}")
        logger.error(f"💥 Unexpected error: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    main()
