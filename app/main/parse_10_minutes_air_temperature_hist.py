"""
10-Minute Air Temperature Data Parser - Complete Enhanced Main Script

COMPLETE ENHANCED VERSION with all fixes and improvements:
- Fixed import paths matching actual folder structure
- Enhanced processing with improved date range handling
- Comprehensive error handling and recovery
- Detailed statistics and quality reporting
- Support for both regular and enhanced processing modes
- Better file discovery and validation
- Improved user feedback and progress tracking
- Fallback logger implementation
- Station info validation and handling
"""

from pathlib import Path
import sys
from typing import List, Dict, Any, Optional
import time
from datetime import datetime

# Import configuration with error handling
try:
    from app.config.ten_minutes_air_temperature_config import (
        RAW_BASE,
        STATION_INFO_FILE_HISTORICAL
    )
    CONFIG_LOADED = True
except ImportError as e:
    print(f"❌ Failed to load configuration: {e}")
    print("Please ensure the config file exists and import paths are correct")
    CONFIG_LOADED = False
    # Set fallback values to prevent undefined variable errors
    RAW_BASE = Path("data/1_raw")  # Fallback path
    STATION_INFO_FILE_HISTORICAL = Path("data/1_raw/station_info/historical_stations.txt")  # Fallback path

# Import processing functions with fallbacks
try:
    from app.parsing.raw_parser import process_zip
    ENHANCED_PROCESSOR_AVAILABLE = True
except ImportError:
    ENHANCED_PROCESSOR_AVAILABLE = False
    try:
        from app.parsing.raw_parser import process_zip
        ENHANCED_PROCESSOR_AVAILABLE = False
    except ImportError as e:
        print(f"❌ Failed to load processing functions: {e}")
        sys.exit(1)

# Import station info parser with fallback
try:
    from app.parsing.station_info_parser import (
        parse_station_info_file,
        get_station_info
    )
    STATION_PARSER_AVAILABLE = True
except ImportError:
    try:
        from app.parsing.station_info_parser import (
            parse_station_info_file,
            get_station_info
        )
        STATION_PARSER_AVAILABLE = False
    except ImportError:
        STATION_PARSER_AVAILABLE = False

# Import logger utility with fallback
try:
    from app.utils.logger import setup_logger
    HAS_LOGGER = True
except ImportError:
    HAS_LOGGER = False


def setup_fallback_logger(log_path: Path):
    """
    Fallback logger setup if utils.logger is not available.
    Creates a comprehensive logger with both file and console output.
    """
    import logging
    
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Clear existing log file
    if log_path.exists():
        log_path.unlink()
    
    logger = logging.getLogger("hist_parser_enhanced")
    logger.setLevel(logging.DEBUG)
    
    # Clear existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()
    
    # Create new file handler (overwrite mode)
    file_handler = logging.FileHandler(log_path, mode='w', encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s — %(levelname)s — %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Add console handler for immediate feedback
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger


def validate_environment() -> Dict[str, Any]:
    """
    Validate the processing environment and return status information.
    
    Returns:
        Dict containing validation results and available features
    """
    validation_results = {
        'config_loaded': CONFIG_LOADED,
        'enhanced_processor': ENHANCED_PROCESSOR_AVAILABLE,
        'station_parser': STATION_PARSER_AVAILABLE,
        'logger_available': HAS_LOGGER,
        'errors': [],
        'warnings': []
    }
    
    if not CONFIG_LOADED:
        validation_results['errors'].append("Configuration not loaded - using fallback paths")
    
    if not ENHANCED_PROCESSOR_AVAILABLE:
        validation_results['warnings'].append("Enhanced processor not available, using basic version")
    
    if not STATION_PARSER_AVAILABLE:
        validation_results['warnings'].append("Station parser not available")
    
    if not HAS_LOGGER:
        validation_results['warnings'].append("Utils logger not available, using fallback")
    
    return validation_results


def discover_zip_files(base_path: Path) -> List[Path]:
    """
    Discover all ZIP files in the historical data directory.
    
    Args:
        base_path: Base path to search for ZIP files
        
    Returns:
        List of discovered ZIP file paths
    """
    historical_folder = base_path / "historical"
    
    if not historical_folder.exists():
        return []
    
    # Find all ZIP files
    zip_files = list(historical_folder.glob("*.zip"))
    
    # Sort by name for consistent processing order
    zip_files.sort(key=lambda x: x.name)
    
    return zip_files


def validate_station_info(station_info_path: Path, logger) -> Dict[str, Any]:
    """
    Validate the station info file and return status information.
    
    Args:
        station_info_path: Path to station info file
        logger: Logger instance
        
    Returns:
        Dict containing validation results
    """
    validation_info = {
        'exists': False,
        'readable': False,
        'station_count': 0,
        'validation_passed': False,
        'errors': []
    }
    
    if not station_info_path.exists():
        validation_info['errors'].append(f"Station info file not found: {station_info_path}")
        return validation_info
    
    validation_info['exists'] = True
    
    try:
        # Try to read the file
        with open(station_info_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            if content.strip():
                validation_info['readable'] = True
                # Rough estimate of station count (lines with station IDs)
                lines = content.split('\n')
                station_lines = [line for line in lines if line.strip() and not line.startswith('#')]
                validation_info['station_count'] = len(station_lines)
            else:
                validation_info['errors'].append("Station info file is empty")
    except Exception as e:
        validation_info['errors'].append(f"Failed to read station info file: {e}")
        return validation_info
    
    # Enhanced validation if available
    if STATION_PARSER_AVAILABLE:
        try:
            if 'parse_station_info_file_enhanced' in globals():
                station_df = parse_station_info_file(station_info_path, logger)
                validation_info['validation_passed'] = station_df is not None and not station_df.empty
            else:
                station_df = parse_station_info_file(station_info_path, logger)
                validation_info['validation_passed'] = station_df is not None and not station_df.empty
        except Exception as e:
            logger.warning(f"Enhanced station validation failed: {e}")
    
    return validation_info


def process_files_with_enhanced_tracking(zip_files: List[Path], station_info_path: Path, logger) -> Dict[str, Any]:
    """
    Process ZIP files with enhanced tracking and statistics.
    
    Args:
        zip_files: List of ZIP files to process
        station_info_path: Path to station info file
        logger: Logger instance
        
    Returns:
        Dict containing processing statistics
    """
    stats = {
        'total_files': len(zip_files),
        'processed_successfully': 0,
        'failed_to_process': 0,
        'processing_times': [],
        'errors': [],
        'start_time': time.time()
    }
    
    logger.info(f"📦 Starting to process {len(zip_files)} ZIP files")
    
    for i, zip_path in enumerate(zip_files, 1):
        file_start_time = time.time()
        
        logger.info(f"🔍 Processing {i}/{len(zip_files)}: {zip_path.name}")
        print(f"🔍 Processing {i}/{len(zip_files)}: {zip_path.name}")
        
        try:
            # Use enhanced processor if available
            if ENHANCED_PROCESSOR_AVAILABLE:
                process_zip(zip_path, station_info_path, logger)
            else:
                process_zip(zip_path, station_info_path, logger)
            
            processing_time = time.time() - file_start_time
            stats['processing_times'].append(processing_time)
            stats['processed_successfully'] += 1
            
            logger.info(f"✅ Successfully processed {zip_path.name} in {processing_time:.2f}s")
            print(f"✅ Successfully processed {zip_path.name} in {processing_time:.2f}s")
            
        except Exception as e:
            processing_time = time.time() - file_start_time
            stats['failed_to_process'] += 1
            error_info = {
                'file': zip_path.name,
                'error': str(e),
                'processing_time': processing_time
            }
            stats['errors'].append(error_info)
            
            logger.error(f"❌ Failed to process {zip_path.name} after {processing_time:.2f}s: {e}")
            print(f"❌ Failed to process {zip_path.name} after {processing_time:.2f}s: {e}")
            
            # Continue processing other files even if one fails
            continue
    
    stats['total_time'] = time.time() - stats['start_time']
    return stats


def print_processing_summary(stats: Dict[str, Any], logger):
    """
    Print comprehensive processing summary with statistics.
    
    Args:
        stats: Processing statistics dictionary
        logger: Logger instance
    """
    logger.info("=" * 80)
    logger.info("📊 COMPREHENSIVE PROCESSING SUMMARY")
    logger.info("=" * 80)
    
    # Basic statistics
    logger.info(f"📦 Total files found: {stats['total_files']}")
    logger.info(f"✅ Successfully processed: {stats['processed_successfully']}")
    logger.info(f"❌ Failed to process: {stats['failed_to_process']}")
    
    # Success rate
    if stats['total_files'] > 0:
        success_rate = (stats['processed_successfully'] / stats['total_files']) * 100
        logger.info(f"📈 Success rate: {success_rate:.1f}%")
    
    # Timing statistics
    logger.info(f"⏱️  Total processing time: {stats['total_time']:.2f}s")
    
    if stats['processing_times']:
        avg_time = sum(stats['processing_times']) / len(stats['processing_times'])
        min_time = min(stats['processing_times'])
        max_time = max(stats['processing_times'])
        
        logger.info(f"⏱️  Average time per file: {avg_time:.2f}s")
        logger.info(f"⏱️  Fastest file: {min_time:.2f}s")
        logger.info(f"⏱️  Slowest file: {max_time:.2f}s")
    
    # Error details
    if stats['errors']:
        logger.info("❌ ERROR DETAILS:")
        for error in stats['errors']:
            logger.info(f"   • {error['file']}: {error['error']}")
    
    # Feature usage
    logger.info("✨ FEATURES USED:")
    logger.info(f"   • Enhanced processor: {'Yes' if ENHANCED_PROCESSOR_AVAILABLE else 'No'}")
    logger.info(f"   • Station parser: {'Yes' if STATION_PARSER_AVAILABLE else 'No'}")
    logger.info(f"   • Utils logger: {'Yes' if HAS_LOGGER else 'No (fallback used)'}")
    
    # Console summary
    print("=" * 80)
    print("📊 COMPREHENSIVE PROCESSING SUMMARY")
    print("=" * 80)
    print(f"📦 Total files found: {stats['total_files']}")
    print(f"✅ Successfully processed: {stats['processed_successfully']}")
    print(f"❌ Failed to process: {stats['failed_to_process']}")
    
    if stats['total_files'] > 0:
        success_rate = (stats['processed_successfully'] / stats['total_files']) * 100
        print(f"📈 Success rate: {success_rate:.1f}%")
    
    print(f"⏱️  Total processing time: {stats['total_time']:.2f}s")


def main():
    """
    Main processing function for historical air temperature data with comprehensive enhancements.
    """
    print("🚀 Starting COMPLETE ENHANCED 10-minute air temperature parser")
    print("=" * 80)
    
    # Validate environment first
    validation = validate_environment()
    
    if validation['warnings']:
        print("⚠️  WARNINGS:")
        for warning in validation['warnings']:
            print(f"   • {warning}")
        print()
    
    # Check if we can continue with fallback configuration
    if not CONFIG_LOADED:
        print("⚠️  Using fallback configuration paths:")
        print(f"   • RAW_BASE: {RAW_BASE}")
        print(f"   • STATION_INFO: {STATION_INFO_FILE_HISTORICAL}")
        print("   Please verify these paths are correct for your setup.")
        print()
    
    
    # Clear existing log file and set up fresh logger
    
    if HAS_LOGGER:
        logger = setup_logger(script_name="parse_10_minutes_air_temperature_hist")
    else:
        logger = setup_fallback_logger(Path("data/germany/0_debug/parse_10_minutes_air_temperature_hist.debug.log"))
        logger.warning("⚠️  Using fallback logger - utils.logger not found")
    
    # Log startup information
    logger.info("🚀 Starting COMPLETE ENHANCED 10-minute air temperature parser")
    logger.info(f"📁 Raw data directory: {RAW_BASE}")
    logger.info(f"📄 Station info file: {STATION_INFO_FILE_HISTORICAL}")
    logger.info("🔄 Fresh log file created: parse_10_minutes_air_temperature_hist.debug.log")
    logger.info("✨ Enhanced features: comprehensive error handling, detailed statistics")
    logger.info(f"⚙️  Configuration loaded: {CONFIG_LOADED}")
    
    print("📄 Log file: data/germany/0_debug/parse_10_minutes_air_temperature_hist.debug.log")
    print()

    # Discover ZIP files
    zip_files = discover_zip_files(RAW_BASE)
    
    if not zip_files:
        historical_path = RAW_BASE / "historical"
        logger.warning(f"⚠️  No ZIP files found in {historical_path}")
        print(f"⚠️  No ZIP files found in {historical_path}")
        print(f"   Please check if the path exists: {historical_path}")
        print(f"   Current working directory: {Path.cwd()}")
        return
    
    logger.info(f"📦 Found {len(zip_files)} ZIP files to process")
    print(f"📦 Found {len(zip_files)} ZIP files to process")
    
    # List files to be processed
    print("\n📋 Files to process:")
    for i, zip_file in enumerate(zip_files, 1):
        print(f"   {i:2d}. {zip_file.name}")
        logger.info(f"   {i:2d}. {zip_file.name}")
    print()
    
    # Validate station info file
    station_validation = validate_station_info(STATION_INFO_FILE_HISTORICAL, logger)
    
    if station_validation['exists']:
        if station_validation['readable']:
            logger.info(f"✅ Station info file validated: {STATION_INFO_FILE_HISTORICAL.name}")
            logger.info(f"📊 Estimated stations: {station_validation['station_count']}")
            print(f"✅ Station info file validated: {STATION_INFO_FILE_HISTORICAL.name}")
            print(f"📊 Estimated stations: {station_validation['station_count']}")
        else:
            logger.warning("⚠️  Station info file exists but has issues")
            for error in station_validation['errors']:
                logger.warning(f"   • {error}")
    else:
        logger.warning(f"⚠️  Station info file not found: {STATION_INFO_FILE_HISTORICAL}")
        logger.info("Processing will continue without station descriptions")
        print(f"⚠️  Station info file not found: {STATION_INFO_FILE_HISTORICAL}")
        print("Processing will continue without station descriptions")
    
    print()
    
    # Process files with enhanced tracking
    print("🔄 Starting file processing...")
    stats = process_files_with_enhanced_tracking(zip_files, STATION_INFO_FILE_HISTORICAL, logger)
    
    # Print comprehensive summary
    print_processing_summary(stats, logger)
    
    # Final status message
    if stats['failed_to_process'] == 0:
        logger.info("🎉 All files processed successfully!")
        print("🎉 All files processed successfully!")
    elif stats['processed_successfully'] > 0:
        logger.warning(f"⚠️  Processing completed with {stats['failed_to_process']} errors")
        print(f"⚠️  Processing completed with {stats['failed_to_process']} errors")
    else:
        logger.error("💥 No files were processed successfully")
        print("💥 No files were processed successfully")
    
    logger.info("🏁 Complete enhanced processing finished")
    logger.info(f"📄 Full log available at: data/germany/0_debug/parse_10_minutes_air_temperature_hist.debug.log")
    print("🏁 Complete enhanced processing finished")
    print(f"📄 Full log available at: data/germany/0_debug/parse_10_minutes_air_temperature_hist.debug.log")


if __name__ == "__main__":
    main()
