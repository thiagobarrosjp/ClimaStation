import json
import logging
from pathlib import Path
from datetime import datetime
from zipfile import ZipFile
from tqdm import tqdm
from app.features.dwd.record_schemas.field_map import CANONICAL_FIELD_MAP, normalize_metadata_row
from app.features.tools.dwd_pipeline.utils import match_by_interval

# === Config ===
SUMMARY_DIR = Path("data/4_summaries")
ARCHIVE_INSPECTION_DIR = Path("data/3_inspection")
RAW_DATA_DIR = Path("data/2_samples/raw")
OUT_DIR = Path("data/5_matching")
DEBUG_LOG_PATH = Path("data/0_debug/station_summary_debug.log")

# === Setup ===
OUT_DIR.mkdir(parents=True, exist_ok=True)
DEBUG_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
if DEBUG_LOG_PATH.exists():
    DEBUG_LOG_PATH.unlink()

logging.basicConfig(
    filename=DEBUG_LOG_PATH,
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.info("=== Starting build_station_summary.py ===")

def sample_metadata_structure(lines):
    for line in lines:
        if not line.strip():
            continue
        parts = line.strip().split(";")
        if all(p.strip() == "" for p in parts):
            continue
        header = [h.strip() for h in parts]
        break
    else:
        return None, None

    for line in lines:
        parts = line.strip().split(";")
        if len(parts) == len(header) and any(p.strip() for p in parts):
            sample = dict(zip(header, [p.strip() for p in parts]))
            return header, sample

    return header, None

def load_latest_station_summary():
    merged_files = sorted(SUMMARY_DIR.glob("*_station_and_dataset_summary.pretty.json"))
    if not merged_files:
        raise FileNotFoundError("No *_station_and_dataset_summary.pretty.json file found.")
    latest = merged_files[-1]
    logging.info(f"Using merged station+dataset summary: {latest}")
    merged_data = json.loads(latest.read_text(encoding="utf-8"))

    station_summary_flat = {}
    for dataset, group in merged_data.items():
        station_block = group.get("station_summary", {})
        for station_id, datasets in station_block.items():
            if station_id not in station_summary_flat:
                station_summary_flat[station_id] = {}
            station_summary_flat[station_id][dataset] = datasets
    return station_summary_flat

def build_zip_index():
    inspection_files = sorted(ARCHIVE_INSPECTION_DIR.glob("*_archive_inspection.pretty.json"))
    if not inspection_files:
        raise FileNotFoundError("No *_archive_inspection.pretty.json file found.")

    latest = inspection_files[-1]
    logging.info(f"Using archive inspection: {latest}")
    data = json.loads(latest.read_text(encoding="utf-8"))

    result = {}
    headers_by_file = {}
    for entry in data:
        zip_path = RAW_DATA_DIR / entry["zip_file"]
        for meta in entry["entries"]:
            fname = meta["filename"]
            result[fname] = zip_path
            if "header" in meta:
                raw_fields = [h.strip() for h in meta["header"].split(";")]
                canonical_fields = [CANONICAL_FIELD_MAP.get(h.lower(), h) for h in raw_fields]
                headers_by_file[fname] = {
                    "original": raw_fields,
                    "canonical": canonical_fields
                }
            logging.debug(f"Mapped {fname} -> {zip_path}")
    logging.info(f"Indexed {len(result)} metadata files.")
    return result, headers_by_file

def parse_metadata_lines(lines, station_id=None):
    header = []
    rows = []
    for line in lines:
        if not line.strip():
            continue
        parts = line.strip().split(";")
        if not header:
            header = [h.strip() for h in parts]
            continue
        if len(parts) != len(header):
            text = ";".join(parts).lower()
            if text.startswith("generiert") or "legende" in text:
                logging.info(f"[COMMENTARY] Ignored non-data line: {parts}")
            else:
                logging.warning(f"⚠ Skipping malformed metadata row: {parts}")
            continue

        row = dict(zip(header, parts))
        norm = {k.strip().lower(): v.strip() for k, v in row.items()}

        sid = norm.get("stations_id") or norm.get("station_id")
        if station_id and sid and sid.lstrip("0") != str(station_id).lstrip("0"):
            continue
        if not sid or not sid.isdigit():
            logging.warning(f"⚠ Invalid or missing stations_id: {norm}")
            continue

        def extract(fieldnames):
            date_formats = ("%Y%m%d", "%d.%m.%Y")
            for key in fieldnames:
                if key in norm:
                    value = norm[key].strip()
                    if not value:
                        continue
                    for fmt in date_formats:
                        try:
                            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
                        except ValueError:
                            continue
            return None

        norm["from"] = extract(["von_datum", "metadata_valid_from", "valid_from"])
        norm["to"] = extract(["bis_datum", "metadata_valid_to", "valid_to"])

        if not norm.get("from") or not norm.get("to"):
            logging.warning(f"⚠ Missing or invalid from/to in row: {norm}")
            continue

        rows.append(norm)
    return rows

def run():
    try:
        summary_data = load_latest_station_summary()
        zip_index, headers_by_file = build_zip_index()
        full_summary = {}

        for station_id, datasets in tqdm(summary_data.items(), desc="Stations"):
            full_summary[station_id] = {}
            for dataset_name, dataset in datasets.items():
                raw_files = dataset.get(f"{dataset_name}_raw", [])
                metadata_files = dataset.get(f"{dataset_name}_metadata", [])
                result = {"raw_files": {}}

                for raw_file in raw_files:
                    filename = raw_file["filename"]
                    rfrom, rto = raw_file["from"], raw_file["to"]
                    matched = {}

                    logging.debug(f"📦 Raw file: {filename} ({rfrom} → {rto})")

                    for meta in metadata_files:
                        meta_file = meta["filename"]
                        zip_path = zip_index.get(meta_file)
                        if not zip_path or not zip_path.exists():
                            continue
                        try:
                            with ZipFile(zip_path, "r") as archive:
                                with archive.open(meta_file) as f:
                                    lines = f.read().decode("utf-8", errors="ignore").splitlines()

                                    if "parameter" not in meta_file.lower():
                                        header, sample = sample_metadata_structure(lines)
                                        logging.info(f"[STRUCTURE_SAMPLE] {meta_file} fields: {header}")
                                        if header and sample:
                                            logging.info(f"[STRUCTURE_SAMPLE] Example row: {sample}")
                                            raw_fields = [h.strip().lower() for h in header]
                                            canonical_fields = [CANONICAL_FIELD_MAP.get(f, f) for f in raw_fields if CANONICAL_FIELD_MAP.get(f, f)]
                                            matched[meta_file] = [{
                                                "metadata_fields_original": raw_fields,
                                                "metadata_fields_canonical": canonical_fields
                                            }]
                                        continue

                                    raw_rows = parse_metadata_lines(lines, station_id=station_id)
                                    canonical_rows = [normalize_metadata_row(r, CANONICAL_FIELD_MAP) for r in raw_rows]

                                    # Group by parameter_code
                                    grouped = {}
                                    for row in canonical_rows:
                                        code = row.get("parameter_code")
                                        if not code:
                                            continue
                                        grouped.setdefault(code, []).append(row)

                                    # Match each parameter's timeline separately
                                    param_matches = {}
                                    for code, rows in grouped.items():
                                        param_matches[code] = match_by_interval(rfrom, rto, rows)

                                    matched[meta_file] = {"parameter_metadata": param_matches}

                        except Exception as e:
                            logging.warning(f"Failed reading {meta_file}: {e}")
                            continue

                    entry = {
                        "from": rfrom,
                        "to": rto,
                        "matched_metadata": matched
                    }

                    if filename in headers_by_file:
                        entry["raw_fields_original"] = headers_by_file[filename]["original"]
                        entry["raw_fields_canonical"] = headers_by_file[filename]["canonical"]
                    else:
                        logging.warning(f"No header info for {filename}")

                    logging.debug(f"ENTRY {filename}: {json.dumps(entry, ensure_ascii=False)}")
                    result["raw_files"][filename] = entry

                full_summary[station_id][dataset_name] = result

        merged_path = OUT_DIR / "station_profile_merged.pretty.json"
        with open(merged_path, "w", encoding="utf-8") as f:
            json.dump(full_summary, f, indent=2, ensure_ascii=False)
        logging.info(f"Saved merged station profile: {merged_path}")
        print(f"\n😊 Merged station profile written to:\n- {merged_path}")

    except Exception as e:
        logging.exception("🚥 Fatal error in build_station_summary.py")

if __name__ == "__main__":
    run()
