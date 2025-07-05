"""
Script: field_map.py
Module: dwd.record_schemas

Purpose:
    Provides access to the canonical field name mapping used across the ClimaStation pipeline
    to standardize raw metadata keys into a unified schema.

Functions:
    - load_field_map():
        → Loads `field_map.json`, skipping any internal keys like "_comment"

    - normalize_metadata_row(metadata_row, field_map):
        → Applies canonical field mapping to a single parsed metadata row
        → Strips whitespace, renames keys, preserves 'from' and 'to' for interval logic

Constants:
    - CANONICAL_FIELD_MAP:
        → The loaded mapping from raw field names to canonical equivalents

Notes:
    - Used by `build_station_summary.py` to normalize metadata rows
    - Ensures consistent field names before schema generation and matching
"""


import json
from pathlib import Path

# Path to the field map JSON file
FIELD_MAP_PATH = Path(__file__).parent / "field_map.json"

def load_field_map():
    """
    Loads the canonical field map from a JSON file,
    excluding any reserved keys like "_comment".
    """
    with open(FIELD_MAP_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    return {k: v for k, v in data.items() if k != "_comment"}

def normalize_metadata_row(metadata_row: dict, field_map: dict) -> dict:
    """
    Normalizes a single metadata row using a field mapping dictionary.
    
    - Renames fields based on `field_map`
    - Discards any fields mapped to `null` or missing from the map
    - Explicitly preserves special fields like 'from' and 'to' even if not mapped
    
    Args:
        metadata_row (dict): A single row of parsed metadata with raw field names.
        field_map (dict): A dictionary mapping raw field names to canonical field names.
    
    Returns:
        dict: A new row with canonical field names and preserved special keys.
    """
    normalized_row = {}

    for raw_key, value in metadata_row.items():
        clean_key = raw_key.strip().lower()
        
        # Preserve 'from' and 'to' manually (for interval matching)
        if clean_key in ("from", "to"):
            normalized_row[clean_key] = value.strip()
            continue

        # Use field map to rename fields (if present)
        canonical_key = field_map.get(clean_key)

        if canonical_key is not None:
            normalized_row[canonical_key] = value.strip()
        # If key is not in map, drop it silently

    return normalized_row

# Load the field map when the module is imported
CANONICAL_FIELD_MAP = load_field_map()
