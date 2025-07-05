"""
Script: generate_record_schema.py
Module: dwd_pipeline

Purpose:
    Generate the official ClimaStation universal record format schema (Version 1) in JSON Schema format.
    This schema will define the structure of all parsed records, combining raw measurements and enriched metadata.

Inputs:
    - data/6_fields/dataset_fields.json
        → Aggregated field inventory grouped by dataset
    - app/features/dwd/record_schemas/field_map.json
        → Canonical mapping of field names from raw metadata

Output:
    - app/features/dwd/record_schemas/v1_universal_schema.json
        → JSON Schema (Draft-07) defining the structure of a single timestamped climate record
    - data/0_debug/generate_record_schema_debug.log
        → Debug log of schema generation process

Features:
    - Establishes required top-level fields (`station_id`, `timestamp`, `parameters`)
    - Supports flexible nested structures for metadata, sensor, and location
    - Inferred types for all fields (string, number, integer)
    - Designed to be forward-compatible and reusable for schema validation

Notes:
    - This script does not yet dynamically include all parameter-specific metadata fields.
    - Schema evolution is tracked manually via versioned files in the `record_schemas/` directory.
"""

import json
import logging
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent.parent  # → /app/features/tools
RECORD_SCHEMA_DIR = BASE_DIR / "dwd" / "record_schemas"
DATA_DIR = BASE_DIR.parent.parent / "data"
DEBUG_LOG_PATH = DATA_DIR / "0_debug" / "generate_record_schema_debug.log"

FIELD_MAP_PATH = RECORD_SCHEMA_DIR / "field_map.json"
FIELD_LIST_PATH = DATA_DIR / "6_fields" / "dataset_fields.json"
OUTPUT_PATH = RECORD_SCHEMA_DIR / "v1_universal_schema.json"

# Logging setup
DEBUG_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=DEBUG_LOG_PATH,
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.info("=== Starting generate_record_schema.py ===")

# Load field map and dataset field list
with open(FIELD_MAP_PATH, "r", encoding="utf-8") as f:
    field_map = json.load(f)
    logging.info(f"Loaded field map with {len(field_map)} entries")

with open(FIELD_LIST_PATH, "r", encoding="utf-8") as f:
    dataset_fields = json.load(f)
    logging.info(f"Loaded dataset fields for {len(dataset_fields)} datasets")

# Helper to guess field types (default = number)
def infer_type(field_name):
    lower = field_name.lower()
    if "datum" in lower or "timestamp" in lower:
        return "string"  # ISO date string
    elif "id" in lower:
        return "string"
    elif "name" in lower:
        return "string"
    elif "latitude" in lower or "longitude" in lower or "elevation" in lower:
        return "number"
    elif any(k in lower for k in ["beschreibung", "einheit", "datenquelle"]):
        return "string"
    return "number"

# Required top-level fields
required_fields = ["station_id", "timestamp", "parameters"]

# Build schema skeleton
schema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "ClimaStation Universal Record Format V1",
    "type": "object",
    "required": required_fields,
    "properties": {
        "station_id": {"type": "string"},
        "station_name": {"type": "string"},
        "timestamp": {"type": "string", "format": "date-time"},
        "dataset": {"type": "string"},
        "variant": {"type": "string"},
        "parameters": {
            "type": "object",
            "additionalProperties": {"type": "number"}
        },
        "quality_flag": {
            "type": "object",
            "additionalProperties": {"type": "integer"}
        },
        "location": {
            "type": "object",
            "properties": {
                "latitude": {"type": "number"},
                "longitude": {"type": "number"},
                "elevation_m": {"type": "number"}
            },
            "required": ["latitude", "longitude", "elevation_m"]
        },
        "sensor": {
            "type": "object",
            "additionalProperties": {"type": "string"}
        },
        "metadata": {
            "type": "object",
            "additionalProperties": {
                "type": "object",
                "properties": {
                    "unit": {"type": "string"},
                    "beschreibung": {"type": "string"},
                    "datenquelle": {"type": "string"}
                },
                "required": ["unit"]
            }
        }
    }
}

# Save schema
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    json.dump(schema, f, indent=2, ensure_ascii=False)

logging.info(f"✔ Schema written to {OUTPUT_PATH.relative_to(BASE_DIR.parent)}")
print(f"✅ Schema written to {OUTPUT_PATH.relative_to(BASE_DIR.parent)}")
