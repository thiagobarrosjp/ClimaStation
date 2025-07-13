# main/parse_10_minutes_air_temperature_hist.py

from pathlib import Path
from config.ten_minutes_air_temperature_config import (
    RAW_BASE,
    STATION_INFO_FILE_HISTORICAL
)
from utils.logger import setup_logger
from parsing.raw_parser import process_zip


station_info_file = STATION_INFO_FILE_HISTORICAL

DEBUG_LOG = Path("data/germany/0_debug/parse_10_minutes_air_temperature_hist.debug.log")

def main():
    logger = setup_logger(DEBUG_LOG)
    logger.info("🚀 Starting 10-minute air temperature parser")

    historical_folder = RAW_BASE / "historical"
    zip_files = list(historical_folder.glob("*.zip"))

    logger.info(f"📦 Found {len(zip_files)} ZIP files to process.")

    for zip_path in zip_files:
        logger.info(f"🔍 Processing {zip_path.name}")
        process_zip(zip_path, station_info_file, logger)


    logger.info("✅ All files processed.")

if __name__ == "__main__":
    main()
