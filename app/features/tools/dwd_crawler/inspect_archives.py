"""
inspect_archives.py

This script inspects each .zip file in the `data/raw/` directory, listing its internal .txt files
and reading their first few lines to help classify them as raw data or metadata.

It does NOT parse or extract the files — only reads headers and metadata in memory.

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
OUTPUT_DIR = "data/dwd_validation_logs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
jsonl_path = os.path.join(OUTPUT_DIR, f"{timestamp}_archive_inspection.jsonl")
pretty_path = os.path.join(OUTPUT_DIR, f"{timestamp}_archive_inspection.pretty.json")

def classify_content(lines, filename=""):
    fname = filename.lower()

    # Strong filename-based rules
    if (
        fname.startswith("metadaten_") or
        fname.startswith("beschreibung") or
        fname.startswith("stationen")
    ):
        return "metadata"

    if "produkt_" in fname or "stundenwerte_" in fname or "zehn_min_" in fname:
        return "raw"

    # Optional fallback based on content
    joined = " ".join(lines).lower()
    if "mess_datum" in joined and any(p in joined for p in [
        "tt_", "rf_", "pp_", "fx", "fm", "n", "sd", "sh", "qn", "rs"
    ]):
        return "raw"

    return "unknown"

def inspect_zip(zip_path):
    entries = []
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
            "zip_file": os.path.basename(zip_path),
            "error": f"Failed to open zip: {e}",
            "entries": []
        }

    return {
        "zip_file": os.path.basename(zip_path),
        "entries": entries
    }

def run():
    all_results = []

    with open(jsonl_path, "w", encoding="utf-8") as out:
        for file in os.listdir(RAW_DATA_DIR):
            if file.endswith(".zip"):
                zip_path = os.path.join(RAW_DATA_DIR, file)
                result = inspect_zip(zip_path)
                out.write(json.dumps(result, ensure_ascii=False) + "\n")
                all_results.append(result)
                print(f"🔍 Inspected {file}")

    # Also write a pretty-printed version
    with open(pretty_path, "w", encoding="utf-8") as pretty_out:
        json.dump(all_results, pretty_out, indent=2, ensure_ascii=False)

    print(f"\n✅ Archive inspection report saved to:\n{jsonl_path}")
    print(f"📄 Pretty-printed version saved to:\n{pretty_path}")

if __name__ == "__main__":
    run()
