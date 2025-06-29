"""
parse_station_metadata.py

This script parses metadata for a single weather station (station ID: 3)
from the downloaded zip archive in the meta_data folder of the
10-minute air temperature dataset.

It extracts location information (latitude, longitude, elevation)
and instrument details (sensor types per parameter) and saves this
as a structured JSON file named `station_air_temperature_10min.json`.

The extracted temp folder is automatically deleted after processing.
"""

import os
import zipfile
import pandas as pd
import json
import time
import shutil
from pathlib import Path

# Config paths
METADATA_FOLDER = "data/air_temperature_10min/meta_data"
TEMP_FOLDER = "extracted_meta"
OUTPUT_FILE = "data/air_temperature_10min/station_air_temperature_10min.json"

def extract_metadata_zip(zip_path: str, extract_to: str):
    """Unzips the metadata archive to a temporary directory."""
    os.makedirs(extract_to, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_to)

from datetime import datetime

def parse_geography_file(filepath: str) -> dict:
    """Parses the station's location, elevation, name, and active period from the geography file."""
    geography_table = pd.read_csv(filepath, sep=";", encoding="ISO-8859-1")

    latest_row = geography_table.iloc[-1]  # Most recent metadata row

    def parse_date(date_str):
        return datetime.strptime(str(date_str), "%Y%m%d").date().isoformat()

    location_data = {
        "name": latest_row["Stationsname"].strip(),
        "latitude": float(str(latest_row["Geogr.Breite"]).replace(",", ".")),
        "longitude": float(str(latest_row["Geogr.Laenge"]).replace(",", ".")),
        "elevation_m": float(str(latest_row["Stationshoehe"]).replace(",", ".")),
        "active_from": parse_date(latest_row["von_datum"]),
        "active_to": parse_date(latest_row["bis_datum"])
    }

    del geography_table
    return location_data


def parse_sensor_file(filepath: str) -> dict:
    """Parses sensor types for each parameter from the instrument metadata file."""
    sensor_table = pd.read_csv(filepath, sep=";", encoding="ISO-8859-1")
    sensors = {}

    for _, row in sensor_table.iterrows():
        parameter = row.get("Element")
        instrument = row.get("Messgerät")
        if pd.notna(parameter) and pd.notna(instrument):
            sensors[parameter.strip()] = instrument.strip()

    del sensor_table  # Free the file handle
    return sensors

def parse_station_3_metadata():
    """Main logic to parse metadata for station 3 and write it to JSON."""
    station_id = 3
    zip_filename = f"Meta_Daten_zehn_min_tu_00003.zip"
    zip_path = os.path.join(METADATA_FOLDER, zip_filename)

    if not os.path.exists(zip_path):
        print(f"❌ Zip file not found: {zip_path}")
        return

    extract_metadata_zip(zip_path, TEMP_FOLDER)

    geography_file = next(Path(TEMP_FOLDER).glob("Metadaten_Geographie_*.txt"), None)
    sensor_file = next(Path(TEMP_FOLDER).glob("Metadaten_Geraete_*.txt"), None)

    if not geography_file or not sensor_file:
        print("❌ Required metadata files not found in zip.")
        return

    location_data = parse_geography_file(str(geography_file))
    sensor_data = parse_sensor_file(str(sensor_file))

    station_metadata = {
        "station_id": station_id,
        **location_data,
        "sensors": sensor_data
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump([station_metadata], f, indent=2, ensure_ascii=False)

    print(f"✅ Saved metadata for station {station_id} to {OUTPUT_FILE}")

    # Wait and clean up temp files (Windows safety)
    time.sleep(0.2)
    shutil.rmtree(TEMP_FOLDER)

if __name__ == "__main__":
    parse_station_3_metadata()
