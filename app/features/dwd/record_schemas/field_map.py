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
