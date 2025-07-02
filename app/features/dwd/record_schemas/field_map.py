import json
from pathlib import Path

FIELD_MAP_PATH = Path(__file__).parent / "field_map.json"

def load_field_map():
    with open(FIELD_MAP_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    return {k: v for k, v in data.items() if k != "_comment"}

def normalize_metadata_row(row, field_map):
    return {
        field_map.get(k, k): v
        for k, v in row.items()
        if field_map.get(k, k) is not None
    }

CANONICAL_FIELD_MAP = load_field_map()
