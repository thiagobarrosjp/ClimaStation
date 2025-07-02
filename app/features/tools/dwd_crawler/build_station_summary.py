import json
from pathlib import Path
from datetime import datetime
from zipfile import ZipFile
from tqdm import tqdm
from app.features.dwd.record_schemas.field_map import CANONICAL_FIELD_MAP, normalize_metadata_row


def load_latest_station_summary():
    summary_dir = Path("data/dwd_validation_logs/")
    summary_files = sorted(summary_dir.glob("*_station_summary.pretty.json"))
    if not summary_files:
        raise FileNotFoundError("No *_station_summary.pretty.json file found.")
    latest_file = summary_files[-1]
    with open(latest_file, "r", encoding="utf-8") as f:
        return json.load(f)


def build_zip_index_from_inspection():
    inspection_files = sorted(Path("data/dwd_validation_logs").glob("*_archive_inspection.pretty.json"))
    if not inspection_files:
        raise FileNotFoundError("No *_archive_inspection.pretty.json file found.")
    
    latest = inspection_files[-1]
    with open(latest, "r", encoding="utf-8") as f:
        data = json.load(f)

    result = {}
    for entry in data:
        zip_path = Path("data/raw") / entry["zip_file"]
        for meta_entry in entry["entries"]:
            filename = meta_entry["filename"]
            result[filename] = zip_path
    return result


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
            continue

        row = dict(zip(header, parts))
        normalized_row = {k.strip().lower(): v.strip() for k, v in row.items()}

        # Match station ID (many formats: "stations_id", "station_id")
        sid = normalized_row.get("stations_id") or normalized_row.get("station_id")
        if station_id and sid and sid.lstrip("0") != str(station_id).lstrip("0"):
            continue

        # Flexible date field resolution
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

            if mfrom <= r_end and mto >= r_start:
                results.append(row)
        except:
            continue
    return results


def run():
    station_summary = load_latest_station_summary()
    zip_index = build_zip_index_from_inspection()
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

                for meta in metadata_files:
                    meta_file = meta["filename"]
                    zip_path = zip_index.get(meta_file)
                    if not zip_path or not Path(zip_path).exists():
                        continue

                    try:
                        with ZipFile(zip_path, "r") as archive:
                            with archive.open(meta_file) as f:
                                lines = f.read().decode("utf-8", errors="ignore").splitlines()
                                parsed_raw = parse_metadata_lines(lines, station_id=station_id)
                                parsed = [normalize_metadata_row(row, CANONICAL_FIELD_MAP) for row in parsed_raw]
                                matches = match_by_interval(rfrom, rto, parsed)
                                matched[meta_file] = matches
                    except:
                        continue

                result["raw_files"][filename] = {
                    "from": rfrom,
                    "to": rto,
                    "matched_metadata": matched
                }

            summary[station_id][dataset_name] = result

    # Write standard output
    out_path = Path("data/dwd_validation_logs/station_profile.pretty.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    # Write canonical version
    canonical_out_path = out_path.with_name(out_path.stem + "_canonical.pretty.json")
    with open(canonical_out_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    run()
