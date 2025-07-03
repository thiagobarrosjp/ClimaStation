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
- data/4_summaries/[timestamp]_dataset_summary.pretty.json
- data/4_summaries/[timestamp]_station_summary.pretty.json

Debug:
- data/0_debug/inspect_archives_debug.log

Main tasks:
- Classify `.txt` files as raw or metadata
- Extract headers, sample rows, and station IDs
- Infer date ranges from filenames
- Group results by dataset and station

Author: ClimaStation Team
"""

import os
import zipfile
import re
import json
import logging
from datetime import datetime
from collections import defaultdict

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
summary_path = os.path.join(SUMMARY_DIR, f"{timestamp}_dataset_summary.pretty.json")
station_summary_path = os.path.join(SUMMARY_DIR, f"{timestamp}_station_summary.pretty.json")

BASE_URL = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/"
KNOWN_VARIANTS = {"historical", "recent", "now", "meta_data", "metadata"}


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


def extract_dataset_and_variant(source_path):
    if source_path == "unknown":
        return "unknown", "unknown"

    parts = source_path.strip("/").split("/")
    for i in range(len(parts) - 1, -1, -1):
        if parts[i].lower() in KNOWN_VARIANTS:
            dataset = "_".join(parts[:i])
            variant = parts[i].lower()
            return dataset, variant
    dataset = "_".join(parts)
    return dataset, None


def classify_content(lines, filename=""):
    fname = filename.lower()
    if fname.startswith(("metadaten_", "beschreibung", "stationen")):
        return "metadata"
    if "produkt_" in fname or "stundenwerte_" in fname or "zehn_min_" in fname:
        return "raw"
    joined = " ".join(lines).lower()
    if "mess_datum" in joined and any(p in joined for p in ["tt_", "rf_", "pp_", "fx", "fm", "n", "sd", "sh", "qn", "rs"]):
        return "raw"
    return "unknown"


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

    summary = generate_dataset_summary(all_results)
    with open(summary_path, "w", encoding="utf-8") as summary_out:
        json.dump(summary, summary_out, indent=2, ensure_ascii=False)

    station_summary = generate_station_summary(all_results)
    with open(station_summary_path, "w", encoding="utf-8") as station_out:
        json.dump(station_summary, station_out, indent=2, ensure_ascii=False)

    print(f"\n✅ Archive inspection report saved to:\n{jsonl_path}")
    print(f"📄 Pretty-printed version saved to:\n{pretty_path}")
    print(f"📊 Dataset summary saved to:\n{summary_path}")
    print(f"📊 Station summary saved to:\n{station_summary_path}")
    logging.info("✔ Inspection completed and all outputs written")


if __name__ == "__main__":
    run()
