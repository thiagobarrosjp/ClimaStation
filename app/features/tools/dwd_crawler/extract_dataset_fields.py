import json
from pathlib import Path
from collections import defaultdict

def extract_dataset_fields(profile_path: Path, output_path: Path):
    with open(profile_path, "r", encoding="utf-8") as f:
        profile_data = json.load(f)

    dataset_fields = defaultdict(set)

    for station_id, datasets in profile_data.items():
        for dataset_name, dataset in datasets.items():
            for raw_file, file_info in dataset.get("raw_files", {}).items():
                dataset_fields[dataset_name].update(file_info.get("raw_fields_canonical", []))
                for meta_rows in file_info.get("matched_metadata", {}).values():
                    for row in meta_rows:
                        dataset_fields[dataset_name].update(
                            k for k in row.keys() if isinstance(k, str)
                        )

    cleaned_dataset_fields = {
        dataset: sorted(f for f in fields if isinstance(f, str))
        for dataset, fields in dataset_fields.items()
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(cleaned_dataset_fields, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    profile_file = Path("data/dwd_validation_logs/station_profile_canonical.pretty.json")
    output_file = Path("data/dwd_validation_logs/dataset_fields.json")
    extract_dataset_fields(profile_file, output_file)
