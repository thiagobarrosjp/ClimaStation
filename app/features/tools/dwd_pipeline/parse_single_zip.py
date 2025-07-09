"""
ClimaStation Utility Script: Extract and Parse DWD Raw Data ZIP File with Schema Validation + Debug Log

This script:
1. Extracts a .txt file from a ZIP archive in 2_downloaded_files/
2. Parses each data line into a JSON object
3. Validates the object against v1_universal_schema.json
4. Writes valid records to a .jsonl file in 3_parsed_files/
5. Logs debug messages to data/germany/0_debug/parse_single_zip_debug.log
"""

import os
import zipfile
import json
from pathlib import Path
from typing import Optional
from jsonschema import validate, ValidationError
import logging

# === CONFIG ===

ZIP_FILENAME = "10minutenwerte_TU_00003_19930428_19991231_hist.zip"

INPUT_PATH = Path("data/germany/2_downloaded_files/10_minutes/air_temperature/historical") / ZIP_FILENAME
OUTPUT_DIR = Path("data/germany/3_parsed_files/parsed_10_minutes/parsed_air_temperature/parsed_historical")
OUTPUT_FILENAME = "parsed_10minutenwerte_TU_00003_19930428_19991231_hist.jsonl"
OUTPUT_PATH = OUTPUT_DIR / OUTPUT_FILENAME

SCHEMA_PATH = Path("app/features/dwd/record_schemas/v1_universal_schema.json")

DEBUG_LOG_PATH = Path("data/germany/0_debug/parse_single_zip_debug.log")
DEBUG_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# === LOGGING ===

if DEBUG_LOG_PATH.exists():
    DEBUG_LOG_PATH.unlink()  # clear old log

logging.basicConfig(
    filename=DEBUG_LOG_PATH,
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.info("=== Started parse_single_zip.py ===")

# === Load schema ===

try:
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        SCHEMA = json.load(f)
    logging.info("Schema loaded successfully.")
except Exception as e:
    logging.error(f"Failed to load schema: {e}")
    raise

# === Metadata ===

PARAM_MAP = {
    "TT_10": {
        "label": "air_temperature",
        "unit": "C",
        "desc": {"de": "Lufttemperatur", "en": "Air Temperature"}
    },
    "TM5_10": {
        "label": "ground_temperature",
        "unit": "C",
        "desc": {"de": "Temperatur 5 cm ueber Boden", "en": "Air Temp. 5 cm above ground"}
    },
    "PP_10": {
        "label": "air_pressure",
        "unit": "hPa",
        "desc": {"de": "Luftdruck", "en": "Air Pressure"}
    },
    "RF_10": {
        "label": "humidity",
        "unit": "%",
        "desc": {"de": "Relative Luftfeuchtigkeit", "en": "Relative Humidity"}
    },
    "TD_10": {
        "label": "dew_point",
        "unit": "C",
        "desc": {"de": "Taupunkt", "en": "Dew Point"}
    }
}

# === Parser Function ===

def parse_row_to_json(row_dict: dict) -> Optional[dict]:
    try:
        station_id = row_dict.get("STATIONS_ID") or row_dict.get("STATION_ID") or "00000"
        timestamp_str = row_dict.get("MESS_DATUM")

        if not timestamp_str or len(timestamp_str) != 12:
            raise ValueError("Invalid timestamp format")

        timestamp_iso = f"{timestamp_str[:4]}-{timestamp_str[4:6]}-{timestamp_str[6:8]}T{timestamp_str[8:10]}:{timestamp_str[10:]}:00"

        parameters = {}
        for key, meta in PARAM_MAP.items():
            raw_val = row_dict.get(key)
            if raw_val is None:
                continue
            try:
                parsed_val = float(raw_val) if raw_val.strip() != "-999" else None
            except Exception:
                parsed_val = None
            parameters[meta["label"]] = {
                "value": parsed_val,
                "unit": meta["unit"],
                "parameter_description": meta["desc"],
                "data_source": {"de": "DWD", "en": "DWD"}
            }

        record = {
            "timestamp": {
                "value": timestamp_iso,
                "time_reference": "UTC"
            },
            "countries": {
                "DE": {
                    "stations": {
                        station_id: {
                            "station": {
                                "station_id": station_id,
                                "station_name": "Unknown"
                            },
                            "location": {
                                "latitude": 0.0,
                                "longitude": 0.0,
                                "station_altitude_m": 0
                            },
                            "measurements": {
                                "10_minutes_air_temperature": {
                                    "source_reference": {
                                        "data_zip": ZIP_FILENAME,
                                        "metadata_zip": "",
                                        "description_pdf": ""
                                    },
                                    "parameters": parameters
                                }
                            }
                        }
                    }
                }
            }
        }

        validate(instance=record, schema=SCHEMA)
        return record

    except (ValueError, ValidationError) as e:
        logging.warning(f"Invalid row skipped: {e}")
        return None

# === Main ===

def extract_and_parse_zip():
    try:
        logging.info(f"Opening ZIP: {INPUT_PATH}")
        with zipfile.ZipFile(INPUT_PATH, "r") as zip_ref:
            txt_filenames = [f for f in zip_ref.namelist() if f.endswith(".txt")]
            if not txt_filenames:
                logging.error("No .txt file found in ZIP archive.")
                print("\u274c No .txt file found in archive.")
                return
            txt_file = txt_filenames[0]

            with zip_ref.open(txt_file) as file:
                lines = file.read().decode("utf-8").splitlines()

        logging.info(f"Read {len(lines)} lines from {txt_file}")

        headers = lines[0].strip().split(";")
        valid_count = 0
        skipped_count = 0

        with open(OUTPUT_PATH, "w", encoding="utf-8") as out_f:
            MAX_RECORDS = 500  # Limit for testing
            for i, line in enumerate(lines[1:], start=1):
                if not line.strip():
                    continue
                if valid_count >= MAX_RECORDS:
                    break
                row_values = line.strip().split(";")
                row_dict = dict(zip(headers, row_values))
                record = parse_row_to_json(row_dict)
                if record:
                    out_f.write(json.dumps(record) + "\n")
                    valid_count += 1
                else:
                    skipped_count += 1

        logging.info(f"Parsed {valid_count} records, skipped {skipped_count}.")
        print(f"\u2705 Parsed {valid_count} valid records to: {OUTPUT_PATH}")

    except Exception as e:
        logging.exception(f"Fatal error while parsing: {e}")
        raise

if __name__ == "__main__":
    extract_and_parse_zip()
    logging.info("=== Finished parse_single_zip.py ===")