import os
import zipfile
import json
import io
from datetime import datetime
from pathlib import Path
import pandas as pd

RAW_DATA_FOLDER = Path("data/germany/2_downloaded_files/10_minutes/air_temperature/historical")
METADATA_FOLDER = Path("data/germany/2_downloaded_files/10_minutes/air_temperature/meta_data")
OUTPUT_FOLDER = Path("data/germany/3_parsed_files/parsed_10_minutes/parsed_air_temperature/parsed_historical")

# --- Helper functions ---
def extract_zip(zip_path: Path, extract_to: Path) -> list:
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            members = zip_ref.namelist()
            zip_ref.extractall(extract_to)
        return [extract_to / member for member in members if member.endswith(".txt")]
    except FileNotFoundError:
        raise FileNotFoundError(f"ZIP file not found: {zip_path}")
    except zipfile.BadZipFile:
        raise ValueError(f"Invalid ZIP file: {zip_path}")

def cleanup_files(file_paths):
    for file_path in file_paths:
        try:
            os.remove(file_path)
        except Exception as e:
            print(f"⚠️ Failed to remove {file_path}: {e}")

def parse_zip_with_metadata(raw_zip: str, station_id: int):
    raw_zip_path = RAW_DATA_FOLDER / raw_zip
    raw_extract_path = RAW_DATA_FOLDER / "_temp"
    raw_extract_path.mkdir(parents=True, exist_ok=True)

    raw_txt_files = extract_zip(raw_zip_path, raw_extract_path)
    if not raw_txt_files:
        raise RuntimeError("No .txt file found in raw ZIP")
    raw_txt_file = raw_txt_files[0]

    raw_df = pd.read_csv(raw_txt_file, sep=';', skipinitialspace=True, encoding='utf-8')
    raw_df.columns = raw_df.columns.str.strip()
    raw_df['MESS_DATUM'] = raw_df['MESS_DATUM'].astype(str).str.zfill(12)

    cleanup_files(raw_txt_files)

    meta_zip_filename = f"Meta_Daten_zehn_min_tu_{str(station_id).zfill(5)}.zip"
    meta_zip_path = METADATA_FOLDER / meta_zip_filename
    meta_extract_path = METADATA_FOLDER / "_temp"
    meta_extract_path.mkdir(parents=True, exist_ok=True)

    meta_txt_files = extract_zip(meta_zip_path, meta_extract_path)
    parameter_file = next((f for f in meta_txt_files if f.name.startswith("Metadaten_Parameter") and f.name.endswith(".txt")), None)
    if not parameter_file:
        raise RuntimeError("No Metadaten_Parameter_*.txt file found")

    with open(parameter_file, encoding='iso-8859-1') as f:
        lines = [line for line in f if not line.lstrip().startswith("generiert")]
    import io
    param_meta = pd.read_csv(io.StringIO("".join(lines)), sep=';', engine='python')
    param_meta.columns = param_meta.columns.str.strip()
    if param_meta.columns[-1].lower().strip() == 'eor':
        param_meta = param_meta[param_meta.columns[:-1]]
    param_meta['Parameter'] = param_meta['Parameter'].astype(str).str.strip()
    param_meta['Stations_ID'] = param_meta['Stations_ID'].astype(str).str.strip()

    cleanup_files(meta_txt_files)

    param_map = {
        "TT_10": "air_temperature",
        "TM5_10": "ground_temperature",
        "PP_10": "air_pressure",
        "RF_10": "humidity",
        "TD_10": "dew_point"
    }

    metadata = {
        "data_zip": raw_zip,
        "metadata_zip": meta_zip_filename,
        "station_name": "Aachen",
        "station_operator": "DWD",
        "location": {
            "latitude": 50.7827,
            "longitude": 6.0941,
            "station_altitude_m": 202,
            "city": "Aachen",
            "state": "",
            "region": None
        },
        "sensors": []
    }

    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_FOLDER / f"parsed_{raw_zip.replace('.zip', '.jsonl')}"

    with open(output_path, "w", encoding="utf-8") as out_file:
        for i, row in raw_df.head(500).iterrows():
            try:
                timestamp = datetime.strptime(row['MESS_DATUM'], "%Y%m%d%H%M")
                timestamp_str = timestamp.strftime("%Y-%m-%dT%H:%M:%S")
                date_int = int(timestamp.strftime("%Y%m%d"))
                parameters = {}
                for raw_key, target_key in param_map.items():
                    try:
                        value = float(row[raw_key])
                    except:
                        value = None
                    meta_rows = param_meta[
                        (param_meta['Parameter'] == raw_key.strip()) &
                        (param_meta['Stations_ID'] == str(station_id).strip())
                    ]
                    match = None
                    for _, meta_row in meta_rows.iterrows():
                        try:
                            start = int(str(meta_row['Von_Datum']))
                            end = int(str(meta_row['Bis_Datum']))
                            if start <= date_int <= end:
                                match = meta_row
                                break
                        except:
                            continue
                    if match is not None:
                        unit = str(match['Einheit']).strip()
                        desc_de = str(match['Parameterbeschreibung']).strip()
                        source_de = str(match['Datenquelle (Strukturversion=SV)']).strip()
                    else:
                        unit = ""
                        desc_de = ""
                        source_de = ""
                    parameters[target_key] = {
                        "value": value,
                        "unit": unit,
                        "parameter_description": {"de": desc_de, "en": ""},
                        "data_source": {"de": source_de, "en": ""}
                    }

                enriched = {
                    "timestamp": {
                        "value": timestamp_str,
                        "time_reference": "UTC"
                    },
                    "countries": {
                        "DE": {
                            "stations": {
                                str(station_id): {
                                    "station": {
                                        "station_id": str(station_id),
                                        "station_name": metadata.get("station_name"),
                                        "station_operator": metadata.get("station_operator")
                                    },
                                    "location": metadata.get("location"),
                                    "measurements": {
                                        "10_minutes_air_temperature": {
                                            "source_reference": {
                                                "data_zip": metadata.get("data_zip"),
                                                "metadata_zip": metadata.get("metadata_zip"),
                                                "description_pdf": ""
                                            },
                                            "sensors": metadata.get("sensors"),
                                            "parameters": parameters
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                json.dump(enriched, out_file, ensure_ascii=False)
                out_file.write("\n")
            except Exception as e:
                print(f"⚠️ Error in row {i}: {e}")

# --- MAIN ---
if __name__ == "__main__":
    try:
        parse_zip_with_metadata("10minutenwerte_TU_00003_19930428_19991231_hist.zip", station_id=3)
    except Exception as e:
        print(f"❌ Fatal error during parsing: {e}")