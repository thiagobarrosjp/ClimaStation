import json
import orjson
import io
from datetime import datetime
from pathlib import Path
import pandas as pd
from typing import Optional
import traceback
import logging

from config.ten_minutes_air_temperature_config import (
    RAW_BASE, PARSED_BASE, 
    PARAM_MAP, PARAM_NAME_MAP, QUALITY_LEVEL_MAP,
    DESCRIPTION_TRANSLATIONS, SOURCE_TRANSLATIONS, 
    SENSOR_TYPE_TRANSLATIONS, MEASUREMENT_METHOD_TRANSLATIONS,
    COLUMN_NAME_MAP
)
from io_helpers.zip_handler import extract_txt_files_from_zip
from parsing.sensor_metadata import parse_sensor_metadata, load_sensor_metadata

def safe_int_conversion(value: str, default: int = 0) -> int:
    """Safely convert string to int with fallback."""
    try:
        return int(str(value).strip())
    except (ValueError, TypeError):
        return default

def safe_float_conversion(value: str, default: float = 0.0) -> float:
    """Safely convert string to float with fallback."""
    try:
        return float(str(value).replace(",", ".").strip())
    except (ValueError, TypeError):
        return default

def load_station_info(station_info_file: Path) -> Optional[pd.DataFrame]:
    """Load station information from file with error handling."""
    if not station_info_file.exists():
        return None
    
    try:
        df = pd.read_fwf(
            station_info_file,
            skiprows=2,
            header=None,
            names=["station_id", "von_datum", "bis_datum", "station_altitude_m", 
                   "latitude", "longitude", "station_name", "state", "delivery"],
            dtype=str
        )
        df["station_id"] = df["station_id"].astype(str).str.strip().str.lstrip("0")
        return df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
    except Exception as e:
        logging.error(f"Failed to load station info from {station_info_file}: {e}")
        return None

def get_station_info(station_info_df: Optional[pd.DataFrame], station_id: int, date_int: int, logger) -> dict:
    """Get station information for specific station and date."""
    if station_info_df is None:
        return {}
    
    try:
        df = station_info_df[station_info_df['station_id'] == str(station_id)]
        for _, row in df.iterrows():
            start = safe_int_conversion(row['von_datum'])
            end = safe_int_conversion(row['bis_datum'])
            
            if start <= date_int <= end:
                return row.to_dict()
    except Exception as e:
        logger.warning(f"Error getting station info for {station_id}: {e}")
    
    return {}

def find_matching_row(df: pd.DataFrame, date: int, logger) -> dict:
    """Find matching row in DataFrame for given date."""
    for _, row in df.iterrows():
        try:
            start = safe_int_conversion(row['Von_Datum'])
            end = safe_int_conversion(row['Bis_Datum'])
            
            if start <= date <= end:
                return row.to_dict()
        except Exception as e:
            logger.debug(f"[find_matching_row] Failed to process row: {e}")
            continue
    
    return {}

def load_metadata_df(path: Path, logger) -> pd.DataFrame:
    """Load metadata DataFrame from file."""
    try:
        with open(path, encoding='latin-1') as f:
            lines = []
            for line in f:
                if line.startswith("Legende"):
                    break
                if not line.startswith("generiert"):
                    lines.append(line)
        
        df = pd.read_csv(
            io.StringIO("".join(lines)), 
            sep=';', 
            skipinitialspace=True, 
            dtype=str
        )
        df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
        logger.debug(f"Parsed metadata from {path.name} with {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"Failed to load metadata from {path}: {e}")
        return pd.DataFrame()

def normalize_columns(df: pd.DataFrame, column_map: dict) -> pd.DataFrame:
    """Normalize DataFrame columns using provided mapping."""
    df.columns = df.columns.str.strip()
    df.rename(columns=column_map, inplace=True)
    return df

def process_zip(raw_zip_path: Path, station_info_file: Path, logger):
    """Process weather data ZIP file."""
    try:
        station_id = int(raw_zip_path.stem.split("_")[2])
    except (IndexError, ValueError) as e:
        logger.error(f"Failed to extract station_id from {raw_zip_path.stem}: {e}")
        return
    
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
        if not raw_txt_files:
            logger.error(f"No text files found in {raw_zip_path}")
            return
            
        raw_df = pd.read_csv(
            raw_txt_files[0], 
            sep=';', 
            skipinitialspace=True, 
            encoding='latin-1'
        )
        
        # Validate required columns
        if "MESS_DATUM" not in raw_df.columns:
            logger.error(f"MESS_DATUM column not found in {raw_txt_files[0]}")
            return
            
        raw_df["timestamp"] = pd.to_datetime(raw_df["MESS_DATUM"], format="%Y%m%d%H%M", errors='coerce')
        
        # Remove rows with invalid timestamps
        raw_df = raw_df.dropna(subset=['timestamp'])
        if raw_df.empty:
            logger.error(f"No valid timestamps found in {raw_txt_files[0]}")
            return
            
        first_date = raw_df["timestamp"].min().normalize().date()
        last_date = raw_df["timestamp"].max().normalize().date()
        
        # === Load metadata ===
        if not meta_zip.exists():
            logger.warning(f"Metadata file not found: {meta_zip}")
            return
            
        meta_files = extract_txt_files_from_zip(meta_zip, temp_meta)
        param_file = next((f for f in meta_files if "Parameter" in f.name), None)
        
        if not param_file:
            logger.error(f"No Parameter file found in {meta_zip}")
            return
        
        # Clean parameter file
        clean_lines = []
        try:
            with open(param_file, "r", encoding="latin-1") as f:
                for line in f:
                    stripped = line.strip()
                    if stripped and (stripped[0].isdigit() and stripped.startswith(str(station_id)) or
                                   stripped.startswith("Stations_ID") or stripped.startswith("StationsId")):
                        clean_lines.append(stripped)
        except Exception as e:
            logger.error(f"Failed to read parameter file {param_file}: {e}")
            return
        
        if not clean_lines:
            logger.warning(f"No valid parameter data found for station {station_id}")
            return
            
        cleaned_param_file = temp_meta / f"{param_file.stem}_cleaned.csv"
        with open(cleaned_param_file, "w", encoding="latin-1") as f:
            f.write("\n".join(clean_lines))
        
        param_meta = pd.read_csv(
            cleaned_param_file, 
            sep=';', 
            skipinitialspace=True, 
            encoding='latin-1', 
            dtype=str
        )
        param_meta = param_meta.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
        param_meta = normalize_columns(param_meta, COLUMN_NAME_MAP)
        
        if param_meta.empty:
            logger.warning(f"No metadata entries found for station_id {station_id}")
            return
        
        sensor_meta_df = load_sensor_metadata(meta_files, logger)
        
        # Build parameter intervals with better error handling
        param_intervals = []
        for _, row in param_meta.iterrows():
            try:
                if 'from_date' not in row or 'to_date' not in row:
                    logger.warning(f"Missing date columns in parameter metadata")
                    continue
                    
                start = datetime.strptime(str(row["from_date"]).strip(), "%Y%m%d").date()
                end = datetime.strptime(str(row["to_date"]).strip(), "%Y%m%d").date()
                param_intervals.append((start, end))
            except ValueError as e:
                logger.warning(f"Invalid date format in parameter metadata: {e}")
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
        
        # Geography metadata with better error handling
        geo_file = next((f for f in meta_files if "Geographie" in f.name), None)
        station_meta_row = {}
        
        if geo_file:
            try:
                geo_df = pd.read_csv(
                    geo_file, 
                    sep=';', 
                    skipinitialspace=True, 
                    encoding='latin-1', 
                    dtype=str
                )
                geo_df = geo_df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
                geo_df = normalize_columns(geo_df, COLUMN_NAME_MAP)
                geo_df = geo_df[geo_df["station_id"] == str(station_id)]
                
                for _, row in geo_df.iterrows():
                    start = safe_int_conversion(row["from_date"])
                    end = safe_int_conversion(row["to_date"])
                    first_date_int = int(first_date.strftime("%Y%m%d"))
                    
                    if start <= first_date_int <= end:
                        station_meta_row = {
                            "station_id": str(row["station_id"]),
                            "station_name": str(row.get("station_name", "")),
                            "latitude": safe_float_conversion(row.get("latitude", "0")),
                            "longitude": safe_float_conversion(row.get("longitude", "0")),
                            "station_altitude_m": safe_float_conversion(row.get("station_altitude_m", "0")),
                            "state": str(row.get("state", "")),
                            "region": row.get("region")
                        }
                        break
            except Exception as e:
                logger.warning(f"Failed to process geography metadata: {e}")
        
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
                
                # Sensors metadata with better error handling
                sensors = []
                try:
                    valid_sensor_meta = sensor_meta_df[
                        (sensor_meta_df["from_date"].astype(str) <= end.strftime("%Y%m%d")) &
                        (sensor_meta_df["to_date"].astype(str) >= start.strftime("%Y%m%d"))
                    ]
                    
                    for _, meta_row in valid_sensor_meta.iterrows():
                        param_raw = str(meta_row.get("parameter", ""))
                        param_entry = PARAM_NAME_MAP.get(param_raw, {"en": param_raw, "de": param_raw})
                        
                        sensor_type = str(meta_row.get("sensor_type", "")).strip()
                        method = str(meta_row.get("measurement_method", "")).strip()
                        
                        sensors.append({
                            "measured_variable": {
                                "de": param_entry.get("de", param_raw),
                                "en": param_entry.get("en", param_raw)
                            },
                            "sensor_type": {
                                "de": SENSOR_TYPE_TRANSLATIONS.get(sensor_type, {"de": sensor_type, "en": sensor_type}).get("de", sensor_type),
                                "en": SENSOR_TYPE_TRANSLATIONS.get(sensor_type, {"de": sensor_type, "en": sensor_type}).get("en", sensor_type)
                            },
                            "measurement_method": {
                                "de": MEASUREMENT_METHOD_TRANSLATIONS.get(method, {"de": method, "en": method}).get("de", method),
                                "en": MEASUREMENT_METHOD_TRANSLATIONS.get(method, {"de": method, "en": method}).get("en", method)
                            },
                            "sensor_height_m": safe_float_conversion(meta_row.get("sensor_height_m", "0"))
                        })
                except Exception as e:
                    logger.warning(f"Failed to process sensor metadata: {e}")
                
                # Measurements with better error handling
                measurements = []
                for _, row in df_range.iterrows():
                    try:
                        params = {}
                        for raw_key, param_info in PARAM_NAME_MAP.items():
                            if raw_key in row and pd.notna(row[raw_key]):
                                # Handle both dict and string formats
                                if isinstance(param_info, dict):
                                    target_key = param_info.get("en", raw_key)
                                else:
                                    target_key = param_info
                                
                                try:
                                    params[target_key] = float(row[raw_key])
                                except (ValueError, TypeError):
                                    params[target_key] = None
                        
                        measurements.append({
                            "timestamp": row["timestamp"].strftime("%Y-%m-%dT%H:%M:%S"),
                            "parameters": params
                        })
                    except Exception as e:
                        logger.debug(f"Failed to process measurement row: {e}")
                        continue
                
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
        logger.error(traceback.format_exc())
    finally:
        # Cleanup with better error handling
        try:
            for p in temp_raw.glob("*"): 
                p.unlink(missing_ok=True)
            for p in temp_meta.glob("*"): 
                p.unlink(missing_ok=True)
            temp_raw.rmdir()
            temp_meta.rmdir()
        except OSError as e:
            logger.debug(f"Cleanup warning: {e}")