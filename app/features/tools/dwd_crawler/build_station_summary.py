import json
import logging
from pathlib import Path
from datetime import datetime
from zipfile import ZipFile
from tqdm import tqdm
from app.features.dwd.record_schemas.field_map import CANONICAL_FIELD_MAP, normalize_metadata_row

# Setup logging
log_path = Path("data/dwd_validation_logs/station_summary_debug.log")
if log_path.exists():
    log_path.unlink()  # Delete existing log file before writing new one

logging.basicConfig(
    filename=log_path,
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.info("=== Started build_station_summary.py ===")


def load_latest_station_summary():
    summary_dir = Path("data/dwd_validation_logs/")
    summary_files = sorted(summary_dir.glob("*_station_summary.pretty.json"))
    if not summary_files:
        logging.error("No *_station_summary.pretty.json file found.")
        raise FileNotFoundError("No *_station_summary.pretty.json file found.")
    latest_file = summary_files[-1]
    logging.info(f"Using station summary: {latest_file}")
    with open(latest_file, "r", encoding="utf-8") as f:
        return json.load(f)


def build_zip_index_from_inspection():
    inspection_files = sorted(Path("data/dwd_validation_logs").glob("*_archive_inspection.pretty.json"))
    if not inspection_files:
        logging.error("No *_archive_inspection.pretty.json file found.")
        raise FileNotFoundError("No *_archive_inspection.pretty.json file found.")

    latest = inspection_files[-1]
    logging.info(f"Using archive inspection: {latest}")
    with open(latest, "r", encoding="utf-8") as f:
        data = json.load(f)

    result = {}
    headers_by_file = {}
    for entry in data:
        zip_path = Path("data/raw") / entry["zip_file"]
        for meta_entry in entry["entries"]:
            filename = meta_entry["filename"]
            result[filename] = zip_path
            if "header" in meta_entry:
                raw_fields = [h.strip() for h in meta_entry["header"].split(";")]
                canonical_fields = [
                    CANONICAL_FIELD_MAP.get(h.strip().lower(), h.strip()) for h in raw_fields
                ]
                headers_by_file[filename] = {
                    "original": raw_fields,
                    "canonical": canonical_fields
                }
            logging.debug(f"Mapped {filename} -> {zip_path}")
    logging.info(f"Total metadata files indexed: {len(result)}")
    return result, headers_by_file


def parse_metadata_lines(lines, station_id=None):
    header = []
    rows = []
    matched_count = 0
    skipped_count = 0

    for line in lines:
        if not line.strip():
            continue
        parts = line.strip().split(";")
        if not header:
            header = [h.strip() for h in parts]
            continue
        if len(parts) != len(header):
            skipped_count += 1
            continue

        row = dict(zip(header, parts))
        normalized_row = {k.strip().lower(): v.strip() for k, v in row.items()}

        sid = normalized_row.get("stations_id") or normalized_row.get("station_id")
        if station_id and sid and sid.lstrip("0") != str(station_id).lstrip("0"):
            skipped_count += 1
            continue

        def extract_date(field_names):
            for key in field_names:
                if key in normalized_row:
                    try:
                        return datetime.strptime(normalized_row[key], "%Y%m%d").strftime("%Y-%m-%d")
                    except:
                        return None
            return None

        normalized_row["from"] = extract_date(["von_datum", "metadata_valid_from", "valid_from"])
        normalized_row["to"] = extract_date(["bis_datum", "metadata_valid_to", "valid_to"])

        rows.append(normalized_row)
        matched_count += 1

    logging.debug(f"parse_metadata_lines(): header: {header}")
    logging.debug(f"parse_metadata_lines(): total lines: {len(lines)}, matched rows: {matched_count}, skipped rows: {skipped_count}")

    if rows:
        logging.debug(f"parse_metadata_lines(): first parsed row: {rows[0]}")
    else:
        logging.warning(f"parse_metadata_lines(): no valid row parsed. Station filter: {station_id}")

    return rows


def match_by_interval(rfrom, rto, metadata_rows):
    results = []
    for row in metadata_rows:
        from_str = row.get("from")
        to_str = row.get("to")
        if not from_str or not to_str:
            continue
        try:
            mfrom = datetime.strptime(from_str, "%Y-%m-%d")
            mto = datetime.strptime(to_str, "%Y-%m-%d")
            r_start = datetime.strptime(rfrom, "%Y-%m-%d")
            r_end = datetime.strptime(rto, "%Y-%m-%d")
            logging.debug(f"Matching row: from={from_str}, to={to_str}")
            if mfrom <= r_end and mto >= r_start:
                results.append(row)
        except:
            continue
    return results


def run():
    try:
        station_summary = load_latest_station_summary()
        zip_index, headers_by_file = build_zip_index_from_inspection()
        summary = {}

        for station_id, datasets in tqdm(station_summary.items(), desc="Processing stations"):
            summary[station_id] = {}
            for dataset_name, dataset in datasets.items():
                raw_files = dataset.get(f"{dataset_name}_raw", [])
                metadata_files = dataset.get(f"{dataset_name}_metadata", [])
                result = {"raw_files": {}}

                for raw_file in raw_files:
                    filename = raw_file["filename"]
                    rfrom, rto = raw_file["from"], raw_file["to"]
                    matched = {}

                    logging.debug(f"Processing raw file: {filename} ({rfrom} → {rto})")

                    for meta in metadata_files:
                        meta_file = meta["filename"]
                        zip_path = zip_index.get(meta_file)
                        if not zip_path or not Path(zip_path).exists():
                            logging.warning(f"Zip not found for metadata file: {meta_file}")
                            continue

                        try:
                            logging.debug(f"Trying to match with metadata file: {meta_file}")
                            with ZipFile(zip_path, "r") as archive:
                                with archive.open(meta_file) as f:
                                    lines = f.read().decode("utf-8", errors="ignore").splitlines()
                                    parsed_raw = parse_metadata_lines(lines, station_id=station_id)
                                    parsed = [normalize_metadata_row(row, CANONICAL_FIELD_MAP) for row in parsed_raw]
                                    matches = match_by_interval(rfrom, rto, parsed)
                                    matched[meta_file] = matches
                                    logging.debug(f"{len(matches)} metadata rows matched from {meta_file}")
                        except Exception as e:
                            logging.exception(f"Error matching metadata file: {meta_file}")
                            continue

                    entry = {
                        "from": rfrom,
                        "to": rto
                    }

                    header_info = headers_by_file.get(filename)
                    if header_info:
                        logging.debug(f"Injecting header fields for: {filename}")
                        entry["raw_fields_original"] = header_info.get("original", [])
                        entry["raw_fields_canonical"] = header_info.get("canonical", [])
                    else:
                        logging.warning(f"No header fields found for: {filename}")

                    entry["matched_metadata"] = matched


                    logging.debug(f"ENTRY CONTENT ({filename}): " + json.dumps(entry, ensure_ascii=False))
                    result["raw_files"][filename] = entry

                summary[station_id][dataset_name] = result

        out_path = Path("data/dwd_validation_logs/station_profile.pretty.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        logging.info(f"Written station summary to: {out_path}")

        canonical_out_path = out_path.with_name("station_profile_canonical.pretty.json")
        with open(canonical_out_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        logging.info(f"Written canonical station summary to: {canonical_out_path}")

    except Exception as e:
        logging.exception("Fatal error during run.")


if __name__ == "__main__":
    run()
