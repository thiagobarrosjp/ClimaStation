import orjson
import json
from pathlib import Path


def pretty_print_jsonl(input_file: Path, output_dir: Path, max_measurements_per_file: int = 20000):
    output_dir.mkdir(parents=True, exist_ok=True)

    current_batch = []
    file_counter = 1
    measurement_counter = 0

    with open(input_file, "r", encoding="utf-8") as f:
        for line in f:
            data = orjson.loads(line)

            # Count how many measurements this block contains
            num_measurements = len(data.get("measurements", []))
            measurement_counter += num_measurements
            current_batch.append(data)

            if measurement_counter >= max_measurements_per_file:
                output_file = output_dir / f"pretty_part_{str(file_counter).zfill(3)}.json"
                with open(output_file, "w", encoding="utf-8") as out:
                    json.dump(current_batch, out, indent=2, ensure_ascii=False)
                file_counter += 1
                current_batch = []
                measurement_counter = 0

    # Write remaining data if any
    if current_batch:
        output_file = output_dir / f"pretty_part_{str(file_counter).zfill(3)}.json"
        with open(output_file, "w", encoding="utf-8") as out:
            json.dump(current_batch, out, indent=2, ensure_ascii=False)

    print(f"✅ Written pretty JSON files to {output_dir}")


# Usage example
if __name__ == "__main__":
    INPUT_JSONL = Path(r"C:\Users\thiag\Dropbox\ClimaStation\climastation-backend\data\germany\3_parsed_files\parsed_10_minutes\parsed_air_temperature\parsed_historical\parsed_10minutenwerte_TU_00003_19930428_19991231_hist.jsonl")
    OUTPUT_DIR = Path(r"C:\Users\thiag\Dropbox\ClimaStation\climastation-backend\data\germany\3_parsed_files\parsed_10_minutes\parsed_air_temperature\parsed_historical\pretty_output")

    pretty_print_jsonl(INPUT_JSONL, OUTPUT_DIR)
