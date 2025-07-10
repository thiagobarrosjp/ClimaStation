"""
ClimaStation DWD Parser - 10-Minute Air Temperature Data

This script processes historical DWD ZIP files containing 10-minute air temperature data
from automatic weather stations in Germany. It extracts and parses both raw measurement
data and corresponding metadata (sensor descriptions, station info, etc.), then writes
enriched `.jsonl` output in a standardized universal record format.

Expected Input Files and Folder Structure:
- Raw ZIP files: stored under
    data/germany/2_downloaded_files/10_minutes/air_temperature/historical/
    (e.g., 10minutenwerte_TU_00003_19930428_19991231_hist.zip)
- Metadata ZIP files: stored under
    data/germany/2_downloaded_files/10_minutes/air_temperature/meta_data/
    (e.g., Meta_Daten_zehn_min_tu_00003.zip)
- Station info TXT file: stored at
    data/germany/2_downloaded_files/10_minutes/air_temperature/historical/zehn_min_tu_Beschreibung_Stationen.txt

Output Files and Folder Structure:
- Enriched parsed records as JSONL files, stored under:
    data/germany/3_parsed_files/parsed_10_minutes/parsed_air_temperature/parsed_historical/
    (e.g., parsed_10minutenwerte_TU_00003_19930428_19991231_hist.jsonl)
- Debug log output:
    data/germany/0_debug/parse_germany_10_minutes_air_temperature.debug.log

Key Features:
- Metadata matching by station ID and date range
- Parameter description and source translations (DE ➝ EN)
- Failsafe debug logging for data exploration and QA
- Fully self-contained JSONL output with timestamp, location, metadata, and measurements

Run this script as a standalone program to process all `.zip` files in the `historical/` folder.

"""


import os
import zipfile
import json
import io
from datetime import datetime
from pathlib import Path
import pandas as pd

RAW_BASE = Path("data/germany/2_downloaded_files/10_minutes/air_temperature")
PARSED_BASE = Path("data/germany/3_parsed_files/parsed_10_minutes/parsed_air_temperature")
DEBUG_LOG = Path("data/germany/0_debug/parse_germany_10_minutes_air_temperature.debug.log")
DEBUG_LOG.parent.mkdir(parents=True, exist_ok=True)

PARAM_NAME_MAP = {
    "Lufttemperatur": {
        "en": "air temperature 2 m above ground",
        "de": "Lufttemperatur"
    },
    "Momentane_Temperatur_In_5cm": {
        "en": "air temperature 5 cm above ground",
        "de": "Momentane Temperatur in 5 cm"
    },
    "Rel_Feuchte": {
        "en": "relative humidity",
        "de": "relative Feuchte"
    }
}

MEASUREMENT_METHOD_TRANSLATIONS  = {
    "Temperaturmessung, elektr.": {
        "en": "temperature measurement, electric",
        "de": "Temperaturmessung, elektr."
    },
    "Feuchtemessung, elektr.": {
        "en": "humidity measurement, electric",
        "de": "Feuchtemessung, elektr."
    }
}

SENSOR_TYPE_TRANSLATIONS  = {
    "PT 100 (Luft)": {
        "en": "PT 100 (air)",
        "de": "PT 100 (Luft)"
    },
    "HYGROMER MP100": {
        "en": "HYGROMER MP100",
        "de": "HYGROMER MP100"
    }
}

PARAM_MAP = {
    "TT_10": "air temperature 2 m above ground",
    "TM5_10": "air temperature 5 cm above ground",
    "PP_10": "air pressure at station altitude",
    "RF_10": "relative humidity",
    "TD_10": "dew point (calculated from air temp. and humidity)"
}

QUALITY_LEVEL_MAP = {
    "1": "only formal control",
    "2": "controlled with individually defined criteria",
    "3": "automatic control and correction"
}


DESCRIPTION_TRANSLATIONS = {
    "momentane Lufttemperatur in 2m Hoehe": "instantaneous air temperature at 2m height",
    "Momentane Temperatur in 5 cm Hoehe 10min": "instantaneous temperature at 5cm height",
    "Luftdruck in Stationshoehe der voran. 10 min": "air pressure at station altitude (preceding 10 min)",
    "relative Feucht in 2m Hoehe": "relative humidity at 2m height",
    "Taupunkttemperatur in 2m Hoehe": "dew point temperature at 2m height",
}

SOURCE_TRANSLATIONS = {
    "10-Minutenwerte von automatischen Stationen der 1. Generation (MIRIAM/AFMS2, ESAU-Daten bis 31.12.1999 (Zeitbezug ist MEZ)":
        "10-minute values from automatic stations (1st gen, MIRIAM/AFMS2, ESAU data until 31 Dec 1999, time reference is CET)",
    "10-Minutenwerte von automatischen Stationen der 1. Generation (MIRIAM/AFMS2, ESAU-Daten ab 01.01.2000 (Zeitbezug ist UTC)":
        "10-minute values from automatic stations (1st gen, MIRIAM/AFMS2, ESAU data from 1 Jan 2000, time reference is UTC)",
    "aus Messnetz2000":
        "from Messnetz2000",
}


def log_error(message):
    with open(DEBUG_LOG, "a", encoding="utf-8") as log:
        log.write("[ERROR] " + message + "\n")

def log_debug(message):
    with open(DEBUG_LOG, "a", encoding="utf-8") as log:
        log.write("[DEBUG] " + message + "\n")

# Load station info once
STATION_INFO_FILE = RAW_BASE / "historical/zehn_min_tu_Beschreibung_Stationen.txt"
station_info_df = None
if STATION_INFO_FILE.exists():
    station_info_df = pd.read_fwf(
    STATION_INFO_FILE,
    skiprows=2,  # skip header and separator
    header=None,  # no header line now
    names=["station_id", "von_datum", "bis_datum", "station_altitude_m", "latitude", "longitude", "station_name", "state", "delivery"],
    dtype=str
)
    station_info_df = station_info_df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)



def extract_zip(zip_path: Path, extract_to: Path) -> list:
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        members = [f for f in zip_ref.namelist() if f.endswith(".txt")]
        zip_ref.extractall(extract_to, members)
    return [extract_to / m for m in members]

def load_raw_data(filepath: Path) -> pd.DataFrame:
    return pd.read_csv(filepath, sep=';', skipinitialspace=True, encoding='latin-1')

def load_metadata_df(path: Path) -> pd.DataFrame:
    with open(path, encoding='latin-1') as f:
        lines = []
        for line in f:
            if line.startswith("Legende"):
                break
            if not line.startswith("generiert"):
                lines.append(line)
    df = pd.read_csv(io.StringIO("".join(lines)), sep=';', skipinitialspace=True, dtype={"Von_Datum": str, "Bis_Datum": str})
    log_debug(f"Parsed metadata from {path.name} with {len(df)} rows")
    return df

def parse_sensor_metadata(meta_files: list, station_id: int, date_int: int) -> list:
    relevant_files = [
        f for f in meta_files
        if "Metadaten_Geraete" in f.name and f.name.endswith(f"_{str(station_id).zfill(5)}.txt")
    ]

    sensors = []

    for path in relevant_files:
        try:
            with open(path, encoding='latin-1') as f:
                lines = []
                for line in f:
                    if line.startswith("generiert"):
                        break
                    lines.append(line)

            df = pd.read_csv(
                io.StringIO("".join(lines)),
                sep=';',
                skipinitialspace=True,
                dtype=str
            )

            df = df[df['Stations_ID'].astype(str).str.strip() == str(station_id)]

            param_raw = path.name.replace("Metadaten_Geraete_", "").replace(f"_{str(station_id).zfill(5)}.txt", "")
            param_entry = PARAM_NAME_MAP.get(param_raw, {"en": param_raw, "de": param_raw})


            for _, row in df.iterrows():
                try:
                    von = int(row['Von_Datum'].strip())
                    bis = int(row['Bis_Datum'].strip())
                    if von <= date_int <= bis:
                        sensor_type_de = row.get("Geraetetyp Name", "").strip()
                        sensor_type_entry = SENSOR_TYPE_TRANSLATIONS.get(sensor_type_de, {"en": sensor_type_de, "de": sensor_type_de})

                        method_de = row.get("Messverfahren", "").strip()
                        method_entry = MEASUREMENT_METHOD_TRANSLATIONS.get(method_de, {"en": method_de, "de": method_de})

                        sensors.append({
                            "measured_variable": {
                                "de": param_entry["de"],
                                "en": param_entry["en"]
                            },
                            "sensor_type": {
                                "de": sensor_type_entry["de"],
                                "en": sensor_type_entry["en"]
                            },
                            "measurement_method": {
                                "de": method_entry["de"],
                                "en": method_entry["en"]
                            },
                            "sensor_height_m": float(row.get("Geberhoehe ueber Grund [m]", "0").replace(",", "."))
                        })

                except Exception as e:
                    log_debug(f"[parse_sensor_metadata] Failed row in {path.name}: {e}")

        except Exception as e:
            log_error(f"❌ Failed to parse sensor metadata file {path.name}: {e}")

    return sensors


def find_matching_row(df: pd.DataFrame, date: int) -> dict:
    for _, row in df.iterrows():
        try:
            start = int(row['Von_Datum'].strip())
            end = int(row['Bis_Datum'].strip())
            if start <= date <= end:
                return row.to_dict()
        except Exception as e:
            log_debug(f"[find_matching_row] Failed to parse row: {e}")
            continue
    return {}

def get_station_info(station_id: int, date_int: int) -> dict:
    if station_info_df is None:
        return {}
    df = station_info_df[station_info_df['station_id'].astype(str) == str(station_id).zfill(5)]
    log_debug(f"🔎 Looking for station ID: {str(station_id).zfill(5)} in station_info_df")
    log_debug(f"    └─ Available IDs: {station_info_df['station_id'].unique()[:5]}")
    for _, row in df.iterrows():
        try:
            start = int(row['von_datum'])
            end = int(row['bis_datum'])
            if start <= date_int <= end:
                return row.to_dict()
        except:
            continue
    return {}

def process_zip(raw_zip_path: Path):
    station_id = int(raw_zip_path.stem.split("_")[2])
    base_folder = raw_zip_path.parent
    meta_folder = base_folder.parent / "meta_data"
    meta_zip = meta_folder / f"Meta_Daten_zehn_min_tu_{str(station_id).zfill(5)}.zip"
    temp_raw = base_folder / "_temp_raw"
    temp_meta = meta_folder / "_temp_meta"
    temp_raw.mkdir(parents=True, exist_ok=True)
    temp_meta.mkdir(parents=True, exist_ok=True)

    try:
        raw_txt_files = extract_zip(raw_zip_path, temp_raw)
        if not raw_txt_files:
            raise FileNotFoundError(f"No .txt files found in {raw_zip_path.name}")
        raw_txt = raw_txt_files[0]
        log_debug(f"📄 Extracted raw data file: {raw_txt.name}")

        raw_df = load_raw_data(raw_txt)
        log_debug(f"📊 Raw data shape: {raw_df.shape}")
        log_debug(f"🧾 Raw data columns: {list(raw_df.columns)[:10]}")

        log_debug(f"📦 Found metadata zip: {meta_zip.name}")
        meta_files = extract_zip(meta_zip, temp_meta)
        for f in meta_files:
            log_debug(f"    └─ Extracted metadata file: {f.name}")

        param_file = [f for f in meta_files if "Parameter" in f.name][0]
        param_meta = load_metadata_df(param_file)

        rel_path = raw_zip_path.relative_to(RAW_BASE)
        out_path = PARSED_BASE / rel_path.parent.name.replace("historical", "parsed_historical") / f"parsed_{raw_zip_path.stem}.jsonl"
        out_path.parent.mkdir(parents=True, exist_ok=True)

        with open(out_path, "w", encoding="utf-8") as f:
            for i, row in raw_df.iloc[1000:1005].iterrows():
                try:
                    timestamp_str_raw = str(row['MESS_DATUM']).split(".")[0]
                    timestamp = datetime.strptime(timestamp_str_raw, "%Y%m%d%H%M")
                    timestamp_str = timestamp.strftime("%Y-%m-%dT%H:%M:%S")
                    date_key = timestamp_str[:10].replace('-', '')
                    date_int = int(date_key)
                    
                    # Extract QN (quality level)
                    qn_raw_val = row.get("QN", None)
                    try:
                        qn_value = int(float(qn_raw_val)) if pd.notna(qn_raw_val) else None
                    except:
                        qn_value = None

                    quality_level = {
                        "value": qn_value,
                        "description": QUALITY_LEVEL_MAP.get(str(qn_value), "missing" if qn_value is None else "unknown")
                    }

                    


                    parameters = {}

                    log_debug(f"⏱️ Row {i}: timestamp = {timestamp_str}, date_int = {date_int}")
                    log_debug(f"🧪 Quality Level: raw='{qn_raw_val}' → value={qn_value}, desc='{quality_level['description']}'")

                    for raw_key, target_key in PARAM_MAP.items():
                        raw_val = row.get(raw_key, '')
                        value = float(raw_val) if pd.notna(raw_val) and raw_val != '' else None
                        log_debug(f"🔍 Param {raw_key} ({target_key}): raw_val={raw_val}, parsed={value}")

                        meta_rows = param_meta[
                            (param_meta['Parameter'].astype(str).str.strip() == raw_key) &
                            (param_meta['Stations_ID'].astype(str).str.strip() == str(station_id))
                        ]

                        if meta_rows.empty:
                            log_debug(f"⚠️ No metadata rows found for param {raw_key} and station {station_id}")
                            meta = {}
                        else:
                            meta = find_matching_row(meta_rows, date_int)
                            if not meta:
                                log_debug(f"⚠️ No matching metadata row for date {date_int} in param {raw_key}")

                        desc_de = meta.get("Parameterbeschreibung", "")
                        source_de = meta.get("Datenquelle (Strukturversion=SV)", "")
                        desc_en = DESCRIPTION_TRANSLATIONS.get(desc_de.strip(), "NO TRANSLATION FOUND")
                        source_en = SOURCE_TRANSLATIONS.get(source_de.strip(), "NO TRANSLATION FOUND")

                        log_debug(f"    └─ Unit: {meta.get('Einheit', '')}, Desc(DE): {desc_de}, Desc(EN): {desc_en}, Source(DE): {source_de}, Source(EN): {source_en}")

                        parameters[target_key] = {
                            "value": value,
                            "unit": meta.get("Einheit", ""),
                            "parameter_description": {
                                "de": desc_de,
                                "en": desc_en
                            },
                            "data_source": {
                                "de": source_de,
                                "en": source_en
                            }
                        }

                    station_meta = get_station_info(station_id, date_int)
                    if not station_meta:
                        log_debug(f"⚠️ No station metadata found for station {station_id} on date {date_int}")
                    else:
                        log_debug(f"🏫 Station metadata: {station_meta.get('station_name', 'Unknown')} at ({station_meta.get('latitude')}, {station_meta.get('longitude')})")

                    sensor_info = parse_sensor_metadata(meta_files, station_id, date_int)

                    enriched = {
                        "timestamp": {"value": timestamp_str, "time_reference": "UTC"},
                        "quality_level": quality_level, 
                        "countries": {
                            "DE": {
                                "stations": {
                                    str(station_id): {
                                        "station": {
                                            "station_id": str(station_id),
                                            "station_name": station_meta.get("station_name", "Unknown"),
                                            "station_operator": "DWD"
                                        },
                                        "location": {
                                            "latitude": float(station_meta.get("latitude", 0.0)),
                                            "longitude": float(station_meta.get("longitude", 0.0)),
                                            "station_altitude_m": int(station_meta.get("station_altitude_m", 0)),
                                            "state": station_meta.get("state", ""),
                                            "region": None
                                        },
                                        "measurements": {
                                            "10_minutes_air_temperature": {
                                                "source_reference": {
                                                    "data_zip": raw_zip_path.name,
                                                    "metadata_zip": meta_zip.name,
                                                    "description_pdf": "DESCRIPTION_obsgermany_climate_10min_air_temperature_en.pdf"
                                                },
                                                "sensors": sensor_info,
                                                "parameters": parameters
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }



                    f.write(json.dumps(enriched, ensure_ascii=False) + "\n")

                    log_debug(f"✅ Row {i}: Done\n")

                except Exception as e:
                    msg = f"⚠️ Skipped row {i} in {raw_zip_path.name}: {e}"
                    print(msg)
                    log_error(msg)


        print(f"✅ Parsed: {raw_zip_path.name}")

    except Exception as e:
        msg = f"❌ Failed to process {raw_zip_path.name}: {e}"
        print(msg)
        log_error(msg)

    finally:
        for p in temp_raw.glob("*"): p.unlink()
        for p in temp_meta.glob("*"): p.unlink()
        if temp_raw.exists():
            try: temp_raw.rmdir()
            except OSError as e: log_error(f"Could not remove {temp_raw}: {e}")
        if temp_meta.exists():
            try: temp_meta.rmdir()
            except OSError as e: log_error(f"Could not remove {temp_meta}: {e}")


if __name__ == "__main__":
    with open(DEBUG_LOG, "w", encoding="utf-8") as log:
        log.write("[INFO] Debug log started.\n")

    historical_folder = RAW_BASE / "historical"
    zip_files = list(historical_folder.glob("*.zip"))
    print(f"Found {len(zip_files)} zip files.")
    for zf in zip_files:
        process_zip(zf)