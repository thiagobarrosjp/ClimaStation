"""
inspect_archives.py

This script inspects each .zip file in the `data/raw/` directory, listing its internal .txt files
and reading their first few lines to help classify them as raw data or metadata.

It does NOT parse or extract the files — only reads headers and metadata in memory.

URL source paths are inferred from `data/raw/downloaded_files.txt`, created by download_samples.py.

Output:
- JSONL file: `data/dwd_validation_logs/[timestamp]_archive_inspection.jsonl`
- Pretty JSON file: `data/dwd_validation_logs/[timestamp]_archive_inspection.pretty.json`

Author: ClimaStation Team
Date: 2025-07-01
"""

import os
import zipfile
from datetime import datetime
import json

RAW_DATA_DIR = "data/raw"
DOWNLOAD_LOG = os.path.join(RAW_DATA_DIR, "downloaded_files.txt")
OUTPUT_DIR = "data/dwd_validation_logs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

BASE_URL = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/"
KNOWN_VARIANTS = {"historical", "recent", "now", "meta_data", "metadata"}

timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
jsonl_path = os.path.join(OUTPUT_DIR, f"{timestamp}_archive_inspection.jsonl")
pretty_path = os.path.join(OUTPUT_DIR, f"{timestamp}_archive_inspection.pretty.json")

def load_download_log():
    """Parse downloaded_files.txt and map filename → source_url_path"""
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
    """From a DWD URL path, extract dataset and variant (if any)"""
    if source_path == "unknown":
        return "unknown", "unknown"

    parts = source_path.strip("/").split("/")

    # Traverse from the end to find known variant
    for i in range(len(parts) - 1, -1, -1):
        if parts[i].lower() in KNOWN_VARIANTS:
            dataset = "_".join(parts[:i])
            variant = parts[i].lower()
            return dataset, variant

    # No known variant → treat entire path as dataset
    dataset = "_".join(parts)
    return dataset, None  # No variant

def classify_content(lines, filename=""):
    fname = filename.lower()
    if (
        fname.startswith("metadaten_") or
        fname.startswith("beschreibung") or
        fname.startswith("stationen")
    ):
        return "metadata"

    if "produkt_" in fname or "stundenwerte_" in fname or "zehn_min_" in fname:
        return "raw"

    joined = " ".join(lines).lower()
    if "mess_datum" in joined and any(p in joined for p in [
        "tt_", "rf_", "pp_", "fx", "fm", "n", "sd", "sh", "qn", "rs"
    ]):
        return "raw"

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
                            lines = []
                            for _ in range(3):
                                line = f.readline().decode("utf-8", errors="ignore").strip()
                                lines.append(line)
                            classification = classify_content(lines, filename=name)
                            entries.append({
                                "filename": name,
                                "lines": len(lines),
                                "header": lines[0] if len(lines) > 0 else "",
                                "sample_row": lines[1] if len(lines) > 1 else "",
                                "classification": classification
                            })
                    except Exception as e:
                        entries.append({
                            "filename": name,
                            "error": f"Failed to read file: {e}"
                        })
    except Exception as e:
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

    print(f"\n✅ Archive inspection report saved to:\n{jsonl_path}")
    print(f"📄 Pretty-printed version saved to:\n{pretty_path}")

if __name__ == "__main__":
    run()
