"""
Script: universal_parser.py
Module: dwd_pipeline

Purpose:
    Prepare the ClimaStation universal parser for DWD raw climate data.
    This script sets up the parsing environment, verifies input file availability,
    and extracts one raw data file from a ZIP archive to validate access.

Inputs:
    - data/5_matching/station_profile_merged.pretty.json
        → Merged profile of raw files and associated metadata
    - data/2_samples/raw/*.zip
        → Raw DWD ZIP archives containing semicolon-delimited text files

Output:
    - No parsed records yet
    - data/0_debug/universal_parser_debug.log
        → Logs file openings, validation checks, and first-level ZIP extraction

Features:
    - Validates input paths and structure
    - Logs each step in a readable debug file
    - Extracts first raw `.txt` file from archive for inspection (no parsing)
"""

import json
import zipfile
import logging
from pathlib import Path

# Paths (adjust as needed for your project structure)
BASE_DIR = Path(__file__).resolve().parent.parent.parent  # → /app/features/tools
DATA_DIR = BASE_DIR.parent.parent / "data"
DEBUG_LOG_PATH = DATA_DIR / "0_debug" / "universal_parser_debug.log"
MERGED_PROFILE_PATH = DATA_DIR / "5_matching" / "station_profile_merged.pretty.json"
RAW_DATA_DIR = DATA_DIR / "2_samples" / "raw"

# Logging setup
DEBUG_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=DEBUG_LOG_PATH,
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.info("=== Starting universal_parser.py ===")

# Load merged profile
if not MERGED_PROFILE_PATH.exists():
    logging.error("❌ station_profile_merged.pretty.json not found.")
    exit()

with open(MERGED_PROFILE_PATH, "r", encoding="utf-8") as f:
    merged_profile = json.load(f)
    logging.info(f"Loaded merged profile with {len(merged_profile)} datasets")

# List available ZIP files
zip_files = sorted(RAW_DATA_DIR.glob("*.zip"))
logging.info(f"Found {len(zip_files)} ZIP files in {RAW_DATA_DIR}")
for zip_file in zip_files:
    logging.info(f"Available ZIP: {zip_file.name}")

# Try to extract sample content from one ZIP + .txt file
for dataset, dataset_info in merged_profile.items():
    raw_files = dataset_info.get("raw_files", {})
    for txt_filename, file_info in raw_files.items():
        zip_filename = f"{txt_filename}.zip"
        zip_path = RAW_DATA_DIR / zip_filename

        if not zip_path.exists():
            logging.warning(f"ZIP file not found: {zip_path}")
            continue

        logging.info(f"Opening ZIP: {zip_path.name}")
        try:
            with zipfile.ZipFile(zip_path, "r") as zipf:
                zip_names = zipf.namelist()
                if txt_filename not in zip_names:
                    logging.warning(f"Expected file '{txt_filename}' not found in ZIP.")
                    continue

                with zipf.open(txt_filename) as f:
                    sample = f.read(500).decode("utf-8", errors="replace")
                    logging.info(f"Extracted sample from {txt_filename}:\n{sample[:200]}...")

        except Exception as e:
            logging.error(f"Error reading ZIP {zip_path.name}: {e}")
            continue

        break  # Process only the first file
    break  # Only one dataset

print("✅ Script completed. See debug log for details.")
