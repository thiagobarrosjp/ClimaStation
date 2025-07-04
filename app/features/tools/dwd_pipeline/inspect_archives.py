"""
inspect_archives.py — DWD Archive Inspector for ClimaStation

This script analyzes all `.zip` files downloaded from the DWD climate data repository,
inspects their `.txt` contents, and produces structured reports for schema evolution.

Inputs:
- data/2_samples/raw/ → .zip files downloaded by download_samples.py
- data/2_samples/downloaded_files.txt → maps filenames to original DWD URLs

Outputs:
- data/3_inspection/[timestamp]_archive_inspection.jsonl
- data/3_inspection/[timestamp]_archive_inspection.pretty.json
- data/4_summaries/[timestamp]_station_and_dataset_summary.pretty.json

Debug:
- data/0_debug/inspect_archives_debug.log

Author: ClimaStation Team
"""

import os
import zipfile
import re
import json
import logging
from datetime import datetime
from collections import defaultdict
from app.features.tools.dwd_pipeline.utils import classify_content, extract_dataset_and_variant

# === Paths ===
RAW_DATA_DIR = "data/2_samples/raw"
DOWNLOAD_LOG = "data/2_samples/downloaded_files.txt"
INSPECTION_DIR = "data/3_inspection"
SUMMARY_DIR = "data/4_summaries"
DEBUG_LOG_PATH = "data/0_debug/inspect_archives_debug.log"

os.makedirs(INSPECTION_DIR, exist_ok=True)
os.makedirs(SUMMARY_DIR, exist_ok=True)
os.makedirs(os.path.dirname(DEBUG_LOG_PATH), exist_ok=True)

if os.path.exists(DEBUG_LOG_PATH):
    os.remove(DEBUG_LOG_PATH)

logging.basicConfig(
    filename=DEBUG_LOG_PATH,
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.info("=== Starting inspect_archives.py ===")

# === File naming ===
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
jsonl_path = os.path.join(INSPECTION_DIR, f"{timestamp}_archive_inspection.jsonl")
pretty_path = os.path.join(INSPECTION_DIR, f"{timestamp}_archive_inspection.pretty.json")
merged_summary_path = os.path.join(SUMMARY_DIR, f"{timestamp}_station_and_dataset_summary.pretty.json")

BASE_URL = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/"


def load_download_log():
    mapping = {}
    if not os.path.exists(DOWNLOAD_LOG):
        print("❌ downloaded_files.txt not found.")
        return mapping

    with open(DOWNLOAD_LOG, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split(" -> ")
            if len(parts) == 3:
                _, filename, full_url = parts
                if full_url.startswith(BASE_URL):
                    rel_path = full_url[len(BASE_URL):]
                    path_only = "/".join(rel_path.split("/")[:-1])
                    mapping[filename] = path_only
    return mapping


def is_valid_station_id(station_id):
    return isinstance(station_id, str) and station_id.isdigit() and len(station_id) == 5


def extract_station_id_from_lines(lines):
    try:
        header = lines[0].split(";")
        values = lines[1].split(";")
        if "stations_id" in [h.strip().lower() for h in header]:
            index = [h.strip().lower() for h in header].index("stations_id")
            return values[index].strip()
    except Exception:
        pass
    return "unknown"


def inspect_zip(zip_path, source_mapping):
    entries = []
    zip_name = os.path.basename(zip_path)
    source_url_path = source_mapping.get(zip_name, "unknown")
    dataset, variant = extract_dataset_and_variant(source_url_path)

    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            for name in z.namelist():
                if name.endswith(".txt"):
                    try:
                        with z.open(name) as f:
                            lines = [f.readline().decode("utf-8", errors="ignore").strip() for _ in range(3)]

                        classification = classify_content(lines, filename=name)
                        station_id = extract_station_id_from_lines(lines)

                        # 🛡️ Validate station ID
                        if not is_valid_station_id(station_id):
                            logging.warning(f"⚠ Invalid station ID in file {name}: {station_id}")
                            station_id = "invalid"

                        # 🛡️ Validate header/sample row consistency
                        if len(lines) >= 2:
                            header_cols = len(lines[0].split(";"))
                            row_cols = len(lines[1].split(";"))
                            if header_cols != row_cols:
                                logging.warning(f"⚠ Header/row mismatch in {name} (header: {header_cols}, row: {row_cols})")

                        record = {
                            "filename": name,
                            "lines": len(lines),
                            "header": lines[0] if lines else "",
                            "sample_row": lines[1] if len(lines) > 1 else "",
                            "dataset": dataset,
                            "classification": classification,
                            "dataset_key": f"{dataset}_{classification}",
                            "station_id": station_id
                        }

                        if classification == "raw":
                            match = re.search(r"_(\d{8})_(\d{8})_", name)
                            if match:
                                from_str, to_str = match.groups()
                                try:
                                    record["from"] = datetime.strptime(from_str, "%Y%m%d").strftime("%Y-%m-%d")
                                    record["to"] = datetime.strptime(to_str, "%Y%m%d").strftime("%Y-%m-%d")
                                except ValueError:
                                    pass

                        entries.append(record)
                        logging.debug(f"✓ Parsed {name} in {zip_name} as {classification}")

                    except Exception as e:
                        logging.warning(f"✗ Failed to parse file in zip: {name} → {e}")
                        entries.append({
                            "filename": name,
                            "error": f"Failed to read file: {e}",
                            "dataset": dataset,
                            "dataset_key": f"{dataset}_unknown",
                            "station_id": "unknown"
                        })

    except Exception as e:
        logging.error(f"✗ Failed to open archive: {zip_path} → {e}")
        return {
            "zip_file": zip_name,
            "source_url_path": source_url_path,
            "dataset": dataset,
            "variant": variant,
            "error": f"Failed to open zip: {e}",
            "entries": []
        }

    return {
        "zip_file": zip_name,
        "source_url_path": source_url_path,
        "dataset": dataset,
        "variant": variant,
        "entries": entries
    }


def generate_dataset_summary(results):
    summary = defaultdict(lambda: defaultdict(list))
    for zip_entry in results:
        for entry in zip_entry.get("entries", []):
            dataset = entry.get("dataset", "unknown")
            dataset_key = entry.get("dataset_key", "unknown")
            filename = entry.get("filename", "[no name]")
            summary[dataset][dataset_key].append(filename)
    return summary


def generate_station_summary(results):
    summary = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for zip_entry in results:
        for entry in zip_entry.get("entries", []):
            station = entry.get("station_id", "unknown")
            dataset = entry.get("dataset", "unknown")
            dataset_key = entry.get("dataset_key", "unknown")
            summary[station][dataset][dataset_key].append({
                "filename": entry.get("filename", "[no name]"),
                "header": entry.get("header", ""),
                "sample_row": entry.get("sample_row", ""),
                "from": entry.get("from", None),
                "to": entry.get("to", None)
            })
    return summary


def merge_by_dataset(dataset_summary, station_summary):
    merged = {}
    dataset_keys = set(dataset_summary) | set(
        ds for station in station_summary.values() for ds in station
    )
    for ds in sorted(dataset_keys):
        merged[ds] = {
            "dataset_summary": dataset_summary.get(ds, {}),
            "station_summary": {}
        }
        for station_id, station_datasets in station_summary.items():
            if ds in station_datasets:
                merged[ds]["station_summary"][station_id] = station_datasets[ds]
    return merged


def run():
    source_mapping = load_download_log()
    all_results = []

    with open(jsonl_path, "w", encoding="utf-8") as out:
        for file in os.listdir(RAW_DATA_DIR):
            if file.endswith(".zip"):
                zip_path = os.path.join(RAW_DATA_DIR, file)
                result = inspect_zip(zip_path, source_mapping)
                out.write(json.dumps(result, ensure_ascii=False) + "\n")
                all_results.append(result)
                print(f"🔍 Inspected {file}")

    with open(pretty_path, "w", encoding="utf-8") as pretty_out:
        json.dump(all_results, pretty_out, indent=2, ensure_ascii=False)

    dataset_summary = generate_dataset_summary(all_results)
    station_summary = generate_station_summary(all_results)
    merged_summary = merge_by_dataset(dataset_summary, station_summary)

    with open(merged_summary_path, "w", encoding="utf-8") as f:
        json.dump(merged_summary, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Archive inspection report saved to:\n{jsonl_path}")
    print(f"📄 Pretty-printed version saved to:\n{pretty_path}")
    print(f"📊 Merged summary saved to:\n{merged_summary_path}")
    logging.info("✔ Inspection completed and all outputs written")


if __name__ == "__main__":
    run()
