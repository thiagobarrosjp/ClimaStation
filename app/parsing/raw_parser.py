import json
import orjson
import io
from datetime import datetime
from pathlib import Path
import pandas as pd
from typing import Optional

from config.ten_minutes_air_temperature_config import (
    RAW_BASE, PARSED_BASE, 
    PARAM_MAP, PARAM_NAME_MAP, QUALITY_LEVEL_MAP,
    DESCRIPTION_TRANSLATIONS, SOURCE_TRANSLATIONS, 
    SENSOR_TYPE_TRANSLATIONS, MEASUREMENT_METHOD_TRANSLATIONS
)

from io_helpers.zip_handler import extract_txt_files_from_zip
from parsing.sensor_metadata import parse_sensor_metadata, load_sensor_metadata


def load_station_info(station_info_file: Path) -> Optional[pd.DataFrame]:
    if not station_info_file.exists():
        return None
    df = pd.read_fwf(
        station_info_file,
        skiprows=2,
        header=None,
        names=["station_id", "von_datum", "bis_datum", "station_altitude_m", "latitude", "longitude", "station_name", "state", "delivery"],
        dtype=str
    )
    df["station_id"] = df["station_id"].astype(str).str.strip().str.lstrip("0")

    return df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)


def get_station_info(station_info_df, station_id: int, date_int: int, logger) -> dict:
    if station_info_df is None:
        return {}
    df = station_info_df[station_info_df['station_id'] == str(station_id)]

    for _, row in df.iterrows():
        try:
            start = int(row['von_datum'])
            end = int(row['bis_datum'])
            if start <= date_int <= end:
                return row.to_dict()
        except:
            continue
    return {}

def find_matching_row(df: pd.DataFrame, date: int, logger) -> dict:
    for _, row in df.iterrows():
        try:
            start = int(row['Von_Datum'].strip())
            end = int(row['Bis_Datum'].strip())
            if start <= date <= end:
                return row.to_dict()
        except Exception as e:
            logger.debug(f"[find_matching_row] Failed row: {e}")
    return {}

def load_metadata_df(path: Path, logger) -> pd.DataFrame:
    with open(path, encoding='latin-1') as f:
        lines = []
        for line in f:
            if line.startswith("Legende"):
                break
            if not line.startswith("generiert"):
                lines.append(line)
    df = pd.read_csv(io.StringIO("".join(lines)), sep=';', skipinitialspace=True, dtype=str)
    df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
    logger.debug(f"Parsed metadata from {path.name} with {len(df)} rows")
    return df

from config.ten_minutes_air_temperature_config import (
    RAW_BASE, PARSED_BASE, 
    PARAM_NAME_MAP, QUALITY_LEVEL_MAP,
    DESCRIPTION_TRANSLATIONS, SOURCE_TRANSLATIONS, 
    SENSOR_TYPE_TRANSLATIONS, MEASUREMENT_METHOD_TRANSLATIONS,
    COLUMN_NAME_MAP
)

def normalize_columns(df: pd.DataFrame, column_map: dict) -> pd.DataFrame:
    df.columns = df.columns.str.strip()
    df.rename(columns=column_map, inplace=True)
    return df


def process_zip(raw_zip_path: Path, station_info_file: Path, logger):
    station_id = int(raw_zip_path.stem.split("_")[2])
    base_folder = raw_zip_path.parent
    meta_folder = base_folder.parent / "meta_data"
    meta_zip = meta_folder / f"Meta_Daten_zehn_min_tu_{str(station_id).zfill(5)}.zip"

    temp_raw = base_folder / "_temp_raw"
    temp_meta = meta_folder / "_temp_meta"
    temp_raw.mkdir(parents=True, exist_ok=True)
    temp_meta.mkdir(parents=True, exist_ok=True)

    try:
        # === Load raw data ===
        raw_txt_files = extract_txt_files_from_zip(raw_zip_path, temp_raw)
        raw_df = pd.read_csv(raw_txt_files[0], sep=';', skipinitialspace=True, encoding='latin-1')
        raw_df["timestamp"] = pd.to_datetime(raw_df["MESS_DATUM"], format="%Y%m%d%H%M")

        first_date = raw_df["timestamp"].min().normalize().date()
        last_date = raw_df["timestamp"].max().normalize().date()

        # === Load metadata ===
        meta_files = extract_txt_files_from_zip(meta_zip, temp_meta)
        param_file = next(f for f in meta_files if "Parameter" in f.name)
        param_meta = pd.read_csv(param_file, sep=';', skipinitialspace=True, encoding='latin-1', dtype=str)
        param_meta = param_meta.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
        param_meta = normalize_columns(param_meta, COLUMN_NAME_MAP)
        logger.debug(f"Parameter metadata columns: {list(param_meta.columns)}")
        logger.debug(f"First few rows of param_meta:\n{param_meta.head()}")

        sensor_meta_df = load_sensor_metadata(meta_files, logger)

        # Build parameter intervals
        param_intervals = []
        for _, row in param_meta.iterrows():
            try:
                start = datetime.strptime(row["from_date"], "%Y%m%d").date()
                end = datetime.strptime(row["to_date"], "%Y%m%d").date()
                param_intervals.append((start, end))
            except:
                continue

        # Sort & fill gaps
        all_dates = {first_date, last_date}
        for start, end in param_intervals:
            all_dates.update([start, end])
        sorted_dates = sorted(all_dates)

        ranges = []
        for i in range(len(sorted_dates) - 1):
            start = sorted_dates[i]
            end = sorted_dates[i + 1]
            if (start, end) not in ranges:
                ranges.append((start, end))

        # Geography metadata
        geo_file = next((f for f in meta_files if "Geographie" in f.name), None)
        station_meta_row = {}
        if geo_file:
            geo_df = pd.read_csv(geo_file, sep=';', skipinitialspace=True, encoding='latin-1', dtype=str)
            geo_df = geo_df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
            geo_df = normalize_columns(geo_df, COLUMN_NAME_MAP)
            geo_df = geo_df[geo_df["station_id"] == str(station_id)]
            for _, row in geo_df.iterrows():
                start = int(row["from_date"])
                end = int(row["to_date"])
                if start <= int(first_date.strftime("%Y%m%d")) <= end:
                    station_meta_row = {
                        "station_id": row["station_id"],
                        "station_name": row["station_name"],
                        "latitude": float(row["latitude"].replace(",", ".")),
                        "longitude": float(row["longitude"].replace(",", ".")),
                        "station_altitude_m": float(row["station_altitude_m"].replace(",", ".")),
                        "state": "",
                        "region": None
                    }
                    break

        # === Output ===
        rel_path = raw_zip_path.relative_to(RAW_BASE)
        out_path_base = PARSED_BASE / rel_path.parent.name.replace("historical", "parsed_historical") / f"parsed_{raw_zip_path.stem}.jsonl"
        out_path_base.parent.mkdir(parents=True, exist_ok=True)

        quality_level = 1
        source_reference = {
            "data_zip": raw_zip_path.name,
            "metadata_zip": meta_zip.name,
            "description_pdf": "DESCRIPTION_obsgermany_climate_10min_air_temperature_en.pdf"
        }

        with open(out_path_base, "w", encoding="utf-8") as f_out:
            for start, end in ranges:
                mask = (raw_df["timestamp"].dt.date >= start) & (raw_df["timestamp"].dt.date <= end)
                df_range = raw_df.loc[mask]
                if df_range.empty:
                    continue

                # Sensors metadata
                sensors = []
                valid_sensor_meta = sensor_meta_df[
                    (sensor_meta_df["from_date"].astype(str) <= end.strftime("%Y%m%d")) &
                    (sensor_meta_df["to_date"].astype(str) >= start.strftime("%Y%m%d"))
                ]

                for _, meta_row in valid_sensor_meta.iterrows():
                    param_raw = meta_row["parameter"]
                    param_entry = PARAM_NAME_MAP.get(param_raw, {"en": param_raw, "de": param_raw})
                    sensor_type = meta_row.get("sensor_type", "").strip()
                    method = meta_row.get("measurement_method", "").strip()
                    sensors.append({
                        "measured_variable": {
                            "de": param_entry["de"],
                            "en": param_entry["en"]
                        },
                        "sensor_type": {
                            "de": SENSOR_TYPE_TRANSLATIONS.get(sensor_type, {"de": sensor_type, "en": sensor_type})["de"],
                            "en": SENSOR_TYPE_TRANSLATIONS.get(sensor_type, {"de": sensor_type, "en": sensor_type})["en"]
                        },
                        "measurement_method": {
                            "de": MEASUREMENT_METHOD_TRANSLATIONS.get(method, {"de": method, "en": method})["de"],
                            "en": MEASUREMENT_METHOD_TRANSLATIONS.get(method, {"de": method, "en": method})["en"]
                        },
                        "sensor_height_m": float(meta_row.get("sensor_height_m", "0").replace(",", "."))
                    })

                measurements = []
                for _, row in df_range.iterrows():
                    params = {
                        target_key: float(row[raw_key]) if pd.notna(row[raw_key]) else None
                        for raw_key, target_key in PARAM_NAME_MAP.items()
                        if raw_key in row
                    }
                    measurements.append({
                        "timestamp": row["timestamp"].strftime("%Y-%m-%dT%H:%M:%S"),
                        "parameters": params
                    })

                output_obj = {
                    "station_id": station_id,
                    "quality_level": quality_level,
                    "station_metadata": station_meta_row,
                    "time_range": {
                        "from": start.isoformat(),
                        "to": end.isoformat()
                    },
                    "source_reference": source_reference,
                    "sensors": sensors,
                    "measurements": measurements
                }
                f_out.write(orjson.dumps(output_obj).decode() + "\n")

        logger.info(f"✅ Parsed and wrote {raw_zip_path.name}")

    except Exception as e:
        logger.error(f"❌ Failed to process {raw_zip_path.name}: {e}")

    finally:
        for p in temp_raw.glob("*"): p.unlink()
        for p in temp_meta.glob("*"): p.unlink()
        try: temp_raw.rmdir()
        except OSError: pass
        try: temp_meta.rmdir()
        except OSError: pass
