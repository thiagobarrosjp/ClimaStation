"""
10-Minute Air Temperature Data Parser - Main Script with Clean Log

This is the main entry point for processing historical German weather station data.
It orchestrates the parsing of ZIP files containing 10-minute air temperature measurements
and associated metadata.

KEY FEATURE: Debug log file is overwritten on each run for cleaner debugging.

Expected Input Structure:
├── data/germany/2_downloaded_files/10_minutes/air_temperature/
│   ├── historical/
│   │   ├── zehn_min_tu_[STATION_ID]_[DATE_RANGE].zip
│   │   └── zehn_min_tu_Beschreibung_Stationen.txt
│   └── meta_data/
│       └── Meta_Daten_zehn_min_tu_[STATION_ID].zip

Expected Output:
├── data/germany/3_parsed_files/parsed_10_minutes/parsed_air_temperature/
│   └── parsed_historical/
│       └── parsed_zehn_min_tu_[STATION_ID]_[DATE_RANGE].jsonl

Process Flow:
1. Clear previous log file for fresh debugging
2. Scan for ZIP files in historical folder
3. For each ZIP file, extract station ID and process data
4. Load raw measurement data and metadata
5. Parse and structure data with bilingual metadata
6. Output structured JSONL files with measurements and sensor info

Usage:
    python parse_10_minutes_air_temperature_hist.py
"""

from pathlib import Path
from config.ten_minutes_air_temperature_config import (
    RAW_BASE,
    STATION_INFO_FILE_HISTORICAL
)
from parsing.raw_parser import process_zip

# Import logger utility with fallback
try:
    from utils.logger import setup_logger
    HAS_LOGGER = True
except ImportError:
    HAS_LOGGER = False


def setup_fallback_logger(log_path: Path):
    """Fallback logger setup if utils.logger is not available."""
    import logging
    
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Clear existing log file
    if log_path.exists():
        log_path.unlink()
    
    logger = logging.getLogger("hist_parser")
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


def main():
    """
    Main processing function for historical air temperature data.
    
    Scans the historical folder for ZIP files and processes each one
    using the raw_parser module. Logs progress and errors to debug file.
    
    The debug log file is overwritten on each run for cleaner debugging.
    """
    # Set up logging with overwrite mode
    debug_log_path = Path("data/germany/0_debug/parse_10_minutes_air_temperature_hist.debug.log")
    
    # Clear existing log file and set up fresh logger
    if debug_log_path.exists():
        debug_log_path.unlink()
        print(f"🗑️  Cleared previous log file: {debug_log_path.name}")
    
    if HAS_LOGGER:
        logger = setup_logger(debug_log_path)
    else:
        logger = setup_fallback_logger(debug_log_path)
        logger.warning("⚠️  Using fallback logger - utils.logger not found")
    
    logger.info("🚀 Starting 10-minute air temperature parser for historical data")
    logger.info(f"📁 Raw data directory: {RAW_BASE}")
    logger.info(f"📄 Station info file: {STATION_INFO_FILE_HISTORICAL}")
    logger.info("🔄 Fresh log file created for this run")
    
    print("🚀 Starting 10-minute air temperature parser for historical data")
    print(f"📄 Log file: {debug_log_path}")

    # Locate historical data folder
    historical_folder = RAW_BASE / "historical"
    
    if not historical_folder.exists():
        logger.error(f"❌ Historical data folder not found: {historical_folder}")
        print(f"❌ Historical data folder not found: {historical_folder}")
        return
    
    # Find all ZIP files to process
    zip_files = list(historical_folder.glob("*.zip"))
    
    if not zip_files:
        logger.warning(f"⚠️  No ZIP files found in {historical_folder}")
        print(f"⚠️  No ZIP files found in {historical_folder}")
        return
    
    logger.info(f"📦 Found {len(zip_files)} ZIP files to process")
    print(f"📦 Found {len(zip_files)} ZIP files to process")
    
    # Verify station info file exists
    if not STATION_INFO_FILE_HISTORICAL.exists():
        logger.warning(f"⚠️  Station info file not found: {STATION_INFO_FILE_HISTORICAL}")
        logger.info("Processing will continue without station descriptions")
        print(f"⚠️  Station info file not found: {STATION_INFO_FILE_HISTORICAL}")
    
    # Process each ZIP file
    processed_count = 0
    error_count = 0
    
    for i, zip_path in enumerate(zip_files, 1):
        logger.info(f"🔍 Processing {i}/{len(zip_files)}: {zip_path.name}")
        print(f"🔍 Processing {i}/{len(zip_files)}: {zip_path.name}")
        
        try:
            process_zip(zip_path, STATION_INFO_FILE_HISTORICAL, logger)
            processed_count += 1
            logger.info(f"✅ Successfully processed {zip_path.name}")
            print(f"✅ Successfully processed {zip_path.name}")
            
        except Exception as e:
            error_count += 1
            logger.error(f"❌ Failed to process {zip_path.name}: {e}")
            print(f"❌ Failed to process {zip_path.name}: {e}")
            # Continue processing other files even if one fails
            continue
    
    # Final summary
    logger.info("=" * 60)
    logger.info("📊 PROCESSING SUMMARY")
    logger.info(f"📦 Total files found: {len(zip_files)}")
    logger.info(f"✅ Successfully processed: {processed_count}")
    logger.info(f"❌ Failed to process: {error_count}")
    
    print("=" * 60)
    print("📊 PROCESSING SUMMARY")
    print(f"📦 Total files found: {len(zip_files)}")
    print(f"✅ Successfully processed: {processed_count}")
    print(f"❌ Failed to process: {error_count}")
    
    if error_count == 0:
        logger.info("🎉 All files processed successfully!")
        print("🎉 All files processed successfully!")
    elif processed_count > 0:
        logger.warning(f"⚠️  Processing completed with {error_count} errors")
        print(f"⚠️  Processing completed with {error_count} errors")
    else:
        logger.error("💥 No files were processed successfully")
        print("💥 No files were processed successfully")
    
    logger.info("🏁 Processing complete")
    logger.info(f"📄 Full log available at: {debug_log_path}")
    print("🏁 Processing complete")
    print(f"📄 Full log available at: {debug_log_path}")


if __name__ == "__main__":
    main()
