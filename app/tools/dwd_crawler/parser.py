"""
parser.py — Step 3: Parse Air Temperature Records from DWD 10-Minute Subset

This script unzips and parses a sample raw dataset from:
    data/raw/air_temperature/historical/

It extracts the .txt file, reads the data, and maps each row to the evolving 
universal record format (defined in record_v*.py).

Parsed output is saved to:
    data/processed/air_temperature/records.json

Current schema includes:
    - station_id
    - timestamp (ISO 8601)
    - TT_10 (temperature)
    - RF_10 (relative humidity)
    - quality_flag
    - dummy location and sensor metadata

NOTE: This version uses hardcoded input/output paths and mock metadata. 
      It is meant for testing the pipeline with real data.
"""

import os
import zipfile
import json
import pandas as pd
from pathlib import Path


def find_first_zip_file(folder: Path) -> Path:
    """Returns the first .zip file found in the given folder."""
    for file in folder.glob("*.zip"):
        return file
    raise FileNotFoundError(f"No ZIP file found in: {folder}")


def extract_zip_file(zip_path: Path, destination: Path) -> Path:
    """Extracts a zip file and returns the first extracted .txt file."""
    destination.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(destination)
    for txt_file in destination.glob("*.txt"):
        return txt_file
    raise FileNotFoundError("No .txt file found after extracting ZIP.")


def parse_air_temperature_file(txt_path: Path) -> "list[dict]":
    """Parses the raw air temperature file and maps to record format."""
    df = pd.read_csv(txt_path, sep=";", na_values=["-999", "-999.0"], dtype=str)

    column_map = {
        "STATIONS_ID": "station_id",
        "MESS_DATUM": "timestamp",
        "QN_10": "quality_flag",
        "TT_10": "TT_10",
        "RF_10": "RF_10"
    }

    df = df.rename(columns=column_map)
    df = df[list(column_map.values())]

    # Convert types
    df["station_id"] = df["station_id"].astype(int)
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y%m%d%H%M")
    df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df["TT_10"] = pd.to_numeric(df["TT_10"], errors="coerce")
    df["RF_10"] = pd.to_numeric(df["RF_10"], errors="coerce")
    df["quality_flag"] = pd.to_numeric(df["quality_flag"], errors="coerce")

    # Dummy metadata
    records = []
    for _, row in df.iterrows():
        record = {
            "station_id": row["station_id"],
            "station_name": None,
            "timestamp": row["timestamp"],
            "TT_10": row["TT_10"],
            "RF_10": row["RF_10"],
            "quality_flag": row["quality_flag"],
            "location": {
                "latitude": 0.0,
                "longitude": 0.0,
                "elevation_m": None
            },
            "sensor": {
                "TT_10": "unknown",
                "RF_10": "unknown"
            }
        }
        records.append(record)

    return records


def save_records(records: "list[dict]", output_path: Path):
    """Saves parsed records to a JSON file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)
    print(f"✅ Saved {len(records)} records to {output_path}")


def main():
    zip_dir = Path("data/raw/air_temperature/historical")
    zip_path = find_first_zip_file(zip_dir)

    temp_extract_path = Path("data/tmp/air_temperature")
    txt_file_path = extract_zip_file(zip_path, temp_extract_path)

    parsed_records = parse_air_temperature_file(txt_file_path)

    output_file = Path("data/processed/air_temperature/records.json")
    save_records(parsed_records, output_file)


if __name__ == "__main__":
    main()
