
# build_station_summary.py

import os
import json
import zipfile
from pathlib import Path
from datetime import datetime

SUMMARY_DIR = "data/dwd_validation_logs"
RAW_DIR = "data/raw"
DOWNLOAD_LOG = os.path.join(RAW_DIR, "downloaded_files.txt")
OUTPUT_FILE = os.path.join(SUMMARY_DIR, "station_profile.pretty.json")


def load_latest_station_summary():
    files = sorted(Path(SUMMARY_DIR).glob("*station_summary.pretty.json"))
    if not files:
        raise FileNotFoundError("No station_summary.pretty.json found.")
    with open(files[-1], "r", encoding="utf-8") as f:
        return json.load(f)


def build_zip_index():
    index = {}
    with open(DOWNLOAD_LOG, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split(" -> ")
            if len(parts) == 3:
                _, zip_filename, _ = parts
                if zip_filename.endswith(".zip"):
                    zip_path = os.path.join(RAW_DIR, zip_filename)
                    try:
                        with zipfile.ZipFile(zip_path, "r") as zf:
                            for name in zf.namelist():
                                if name.endswith(".txt") and name not in index:
                                    index[name] = zip_filename
                    except Exception:
                        continue
    return index


def parse_metadata_lines(lines, station_id=None):
    header = None
    parsed = []
    for line in lines:
        if not line.strip() or line.startswith("#"):
            continue
        if header is None:
            header = [h.strip().lower() for h in line.split(";")]
            continue
        values = [v.strip() for v in line.split(";")]
        row = dict(zip(header, values))
        if station_id and row.get("stations_id") != station_id:
            continue
        try:
            from_raw = row.get("von_datum") or row.get("von") or ""
            to_raw = row.get("bis_datum") or row.get("bis") or ""
            row["from"] = datetime.strptime(from_raw[:8], "%Y%m%d").strftime("%Y-%m-%d")
            row["to"] = datetime.strptime(to_raw[:8], "%Y%m%d").strftime("%Y-%m-%d")
        except Exception:
            row["from"] = row["to"] = None
        parsed.append(row)
    return parsed


def match_by_interval(rfrom, rto, rows):
    results = []
    try:
        rf = datetime.strptime(rfrom, "%Y-%m-%d")
        rt = datetime.strptime(rto, "%Y-%m-%d")
        for row in rows:
            if "from" in row and "to" in row and row["from"] and row["to"]:
                try:
                    mf = datetime.strptime(row["from"], "%Y-%m-%d")
                    mt = datetime.strptime(row["to"], "%Y-%m-%d")
                    if mf <= rt and mt >= rf:
                        results.append(row)
                except Exception:
                    continue
    except Exception:
        pass
    return results


def run():
    summary = load_latest_station_summary()
    zip_index = build_zip_index()
    output = {}

    for station_id, datasets in summary.items():
        output[station_id] = {}
        for dataset, content in datasets.items():
            raw_files = content.get(f"{dataset}_raw", [])
            meta_files = content.get(f"{dataset}_metadata", [])
            output[station_id][dataset] = {"raw_files": {}}

            for raw in raw_files:
                rname = raw["filename"]
                rfrom = raw.get("from")
                rto = raw.get("to")
                result = {
                    "from": rfrom,
                    "to": rto,
                    "matched_metadata": {}
                }

                for meta in meta_files:
                    mname = meta["filename"]
                    zip_name = zip_index.get(mname)
                    if not zip_name:
                        continue
                    zip_path = os.path.join(RAW_DIR, zip_name)
                    try:
                        with zipfile.ZipFile(zip_path, "r") as zf:
                            with zf.open(mname) as f:
                                lines = f.read().decode("utf-8", errors="ignore").splitlines()
                                parsed = parse_metadata_lines(lines, station_id=station_id)
                                matches = match_by_interval(rfrom, rto, parsed)
                                if matches:
                                    result["matched_metadata"][mname] = matches
                    except Exception:
                        continue

                output[station_id][dataset]["raw_files"][rname] = result

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f"✅ Output written to {OUTPUT_FILE}")


if __name__ == "__main__":
    run()
