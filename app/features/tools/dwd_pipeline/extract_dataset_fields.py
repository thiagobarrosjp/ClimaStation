"""
Script: extract_dataset_fields.py
Module: dwd_pipeline

Purpose:
    Extract all unique canonical field names per dataset from the merged station metadata profile.
    Aggregates raw data fields and matched metadata fields to build a dataset-wise field inventory.

Input:
    - data/5_matching/station_profile_merged.pretty.json
        → Output from `build_station_summary.py` containing raw and matched metadata info

Output:
    - data/6_fields/dataset_fields.json
        → Dictionary of { dataset_name: [field_1, field_2, ...] } sorted alphabetically
    - data/0_debug/extract_dataset_fields_debug.log
        → Logs of fields discovered and grouped per dataset

Notes:
    - Handles both structure-sampled metadata (partial parse) and full metadata (parameter files)
    - Excludes helper keys like `metadata_fields_original` and `metadata_fields_canonical`
    - Final output is used to define the universal record schema (V1)
"""


import json
import logging
from pathlib import Path
from collections import defaultdict

# === Paths ===
INPUT_PATH = Path("data/5_matching/station_profile_merged.pretty.json")
OUTPUT_PATH = Path("data/6_fields/dataset_fields.json")
DEBUG_LOG_PATH = Path("data/0_debug/extract_dataset_fields_debug.log")

# === Logging setup ===
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
DEBUG_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
if DEBUG_LOG_PATH.exists():
    DEBUG_LOG_PATH.unlink()

logging.basicConfig(
    filename=DEBUG_LOG_PATH,
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.info("=== Starting extract_dataset_fields.py ===")

# === Ignore these helper keys (not real schema fields) ===
IGNORED_KEYS = {"metadata_fields_canonical", "metadata_fields_original"}

def extract_dataset_fields(profile_path: Path, output_path: Path):
    with open(profile_path, "r", encoding="utf-8") as f:
        profile_data = json.load(f)

    dataset_fields = defaultdict(set)

    for station_id, datasets in profile_data.items():
        for dataset_name, dataset in datasets.items():
            for raw_file, file_info in dataset.get("raw_files", {}).items():
                raw_fields = file_info.get("raw_fields_canonical", [])
                dataset_fields[dataset_name].update(raw_fields)
                logging.debug(f"{dataset_name} ← raw fields: {raw_fields}")

                for meta_source, meta_rows in file_info.get("matched_metadata", {}).items():
                    if isinstance(meta_rows, list):
                        for row in meta_rows:
                            metadata_fields = [
                                k for k in row.keys()
                                if isinstance(k, str) and k not in IGNORED_KEYS
                            ]
                            dataset_fields[dataset_name].update(metadata_fields)
                            logging.debug(f"{dataset_name} ← metadata fields: {metadata_fields}")

                            canonical_list = row.get("metadata_fields_canonical")
                            if isinstance(canonical_list, list):
                                dataset_fields[dataset_name].update([
                                    f for f in canonical_list
                                    if isinstance(f, str) and f not in IGNORED_KEYS
                                ])
                                logging.debug(f"{dataset_name} ← canonical from structure sample: {canonical_list}")

                    elif isinstance(meta_rows, dict) and "parameter_metadata" in meta_rows:
                        for param_code, param_entries in meta_rows["parameter_metadata"].items():
                            for entry in param_entries:
                                metadata_fields = [
                                    k for k in entry.keys()
                                    if isinstance(k, str) and k not in IGNORED_KEYS
                                ]
                                dataset_fields[dataset_name].update(metadata_fields)
                                logging.debug(f"{dataset_name} ← param {param_code} metadata fields: {metadata_fields}")

    cleaned_dataset_fields = {
        dataset: sorted(f for f in fields if isinstance(f, str) and f not in IGNORED_KEYS)
        for dataset, fields in dataset_fields.items()
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(cleaned_dataset_fields, f, indent=2, ensure_ascii=False)

    logging.info(f"✔ Written dataset field inventory to: {output_path}")
    print(f"✅ Field inventory written to: {output_path}")

if __name__ == "__main__":
    extract_dataset_fields(INPUT_PATH, OUTPUT_PATH)
