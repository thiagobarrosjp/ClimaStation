"""
parser.py

Parses a sample `.zip` file from the DWD 10-minute air temperature dataset.
Extracts the `.txt` file, parses rows into AirTemperatureRecord objects,
and writes them to a JSON file in `data/air_temperature_10min/parsed/`.

The temp extraction folder is automatically removed after parsing.
Uses Pydantic's model_dump_json() to handle datetime serialization.
"""

import os
import zipfile
import shutil
from pathlib import Path
from typing import List
from app.features.dwd.schemas import AirTemperatureRecord
import pandas as pd

# Constants
ENCODING = "ISO-8859-1"
DELIMITER = ";"
MISSING_VALUE = "-999"
TEMP_FOLDER = "extracted_temp"
OUTPUT_FOLDER = "data/air_temperature_10min/parsed"
OUTPUT_PREFIX = "10min_air_temperature_historical"


def extract_txt_from_zip(zip_file_path: str, extraction_dir: str = TEMP_FOLDER) -> str:
    """Unzips a DWD .zip archive and returns the path to the first .txt file inside."""
    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        zip_ref.extractall(extraction_dir)
        txt_files = [f for f in zip_ref.namelist() if f.endswith(".txt")]
        if not txt_files:
            raise Exception("No .txt file found inside the zip archive.")
        return os.path.join(extraction_dir, txt_files[0])


def load_and_parse_txt(txt_file_path: str, max_rows: int = 10) -> List[AirTemperatureRecord]:
    """Reads a .txt file into a DataFrame and parses it into records."""
    with open(txt_file_path, "r", encoding=ENCODING) as f:
        raw_data = pd.read_csv(
            f,
            delimiter=DELIMITER,
            na_values=[MISSING_VALUE],
            nrows=max_rows
        )

    # Make sure the DataFrame is fully in memory
    raw_data = raw_data.copy()

    records = []
    for _, row in raw_data.iterrows():
        record = AirTemperatureRecord.from_raw_row(row)
        records.append(record)

    return records


def save_records_as_json(records: List[AirTemperatureRecord], output_folder: str, prefix: str):
    """Saves parsed records to a uniquely numbered JSON file using model_dump_json()."""
    os.makedirs(output_folder, exist_ok=True)
    existing_files = list(Path(output_folder).glob(f"{prefix}_*.json"))
    file_number = len(existing_files) + 1
    filename = f"{prefix}_{file_number:04}.json"
    output_path = os.path.join(output_folder, filename)

    # Serialize each record individually to ensure proper datetime handling
    json_data = "[" + ",\n".join(r.model_dump_json() for r in records) + "]"

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(json_data)

    print(f"✅ Saved {len(records)} records to: {output_path}")


import time  # make sure this is at the top of the file

def parse_zip_file(zip_file_path: str, extraction_dir: str = TEMP_FOLDER) -> List[AirTemperatureRecord]:
    """
    Extracts, parses, and returns records from a single zip file.
    Automatically deletes the extracted directory after processing.
    """
    print(f"📦 Parsing: {zip_file_path}")

    # Create temp directory
    os.makedirs(extraction_dir, exist_ok=True)

    # Extract the .txt file from the zip
    txt_file_path = extract_txt_from_zip(zip_file_path, extraction_dir)

    # Load and parse the .txt file
    records = load_and_parse_txt(txt_file_path)

    # Give Windows a moment to release any open file handles
    time.sleep(0.2)

    # Clean up the extracted files
    shutil.rmtree(extraction_dir)
    print(f"🧹 Cleaned up: {extraction_dir}/")

    return records


if __name__ == "__main__":
    sample_zip_path = next(Path("data/air_temperature_10min/historical").glob("*.zip"), None)
    if sample_zip_path:
        records = parse_zip_file(str(sample_zip_path))
        save_records_as_json(records, output_folder=OUTPUT_FOLDER, prefix=OUTPUT_PREFIX)
    else:
        print("⚠️ No .zip file found in historical folder.")
