import json
import orjson
import io
from datetime import datetime, date
from pathlib import Path
import pandas as pd
from typing import Optional, List, Dict, Any, Tuple
import traceback
import logging
import shutil

from app.config.ten_minutes_air_temperature_config import (
    RAW_BASE, PARSED_BASE, 
    PARAM_MAP, PARAM_NAME_MAP, QUALITY_LEVEL_MAP,
    DESCRIPTION_TRANSLATIONS, SOURCE_TRANSLATIONS, 
    SENSOR_TYPE_TRANSLATIONS, MEASUREMENT_METHOD_TRANSLATIONS,
    COLUMN_NAME_MAP
)
from app.io_helpers.zip_handler import extract_txt_files_from_zip
from app.parsing.sensor_metadata import load_sensor_metadata, parse_sensor_metadata
from app.parsing.station_info_parser import parse_station_info_file, get_station_info

try:
    from app.utils.logger import setup_logger
    HAS_LOGGER = True
except ImportError:
    HAS_LOGGER = False


def safe_int_conversion(value: str, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except (ValueError, TypeError):
        return default


def safe_float_conversion(value: str, default: float = 0.0) -> float:
    try:
        cleaned = str(value).replace(",", ".").strip()
        if not cleaned or cleaned.lower() in ['nan', 'null', 'none', '']:
            return default
        return float(cleaned)
    except (ValueError, TypeError):
        return default


def normalize_columns(df: pd.DataFrame, col_map: dict) -> pd.DataFrame:
    df.columns = df.columns.str.strip()
    df.rename(columns=col_map, inplace=True)
    return df


def extract_station_id_from_filename(filename: str, logger: logging.Logger) -> Optional[int]:
    try:
        logger.debug(f"🔍 Extracting station ID from: {filename}")
        parts = filename.split("_")
        
        for i, part in enumerate(parts):
            if part == "TU" and i + 1 < len(parts):
                sid = int(parts[i + 1])
                logger.debug(f"   ✅ Extracted station ID {sid}")
                return sid
        
        # Fallback
        try:
            sid = int(parts[2])
            logger.debug(f"   ✅ Extracted station ID {sid} (fallback)")
            return sid
        except (ValueError, IndexError):
            pass
        
        logger.error(f"   ❌ Could not extract station ID from: {filename}")
        return None
        
    except Exception as e:
        logger.error(f"   ❌ Failed to extract station_id: {e}")
        return None


def load_raw_measurement_data(raw_files: List[Path], logger: logging.Logger) -> Optional[pd.DataFrame]:
    if not raw_files:
        logger.error("❌ No text files found")
        return None
    
    raw_file = raw_files[0]
    logger.info(f"📄 Loading: {raw_file.name}")
    
    try:
        df = pd.read_csv(raw_file, sep=';', skipinitialspace=True, encoding='latin-1', dtype=str)
        
        logger.info(f"📊 Shape: {df.shape}")
        
        if "MESS_DATUM" not in df.columns:
            logger.error(f"❌ MESS_DATUM column missing")
            return None
        
        avail_cols = [col for col in df.columns if col in PARAM_MAP]
        logger.info(f"📈 Available measurements: {avail_cols}")
        
        logger.info("🕐 Parsing timestamps...")
        df["timestamp"] = pd.to_datetime(df["MESS_DATUM"], format="%Y%m%d%H%M", errors='coerce')
        
        invalid_ts = df["timestamp"].isna().sum()
        if invalid_ts > 0:
            logger.warning(f"⚠️  Removing {invalid_ts} invalid timestamps")
            df = df.dropna(subset=['timestamp'])
        
        if df.empty:
            logger.error(f"❌ No valid timestamps")
            return None
        
        first_date = df["timestamp"].min().normalize().date()
        last_date = df["timestamp"].max().normalize().date()
        logger.info(f"📅 Range: {first_date} to {last_date}")
        logger.info(f"📊 Total: {len(df):,}")
        
        return df
        
    except Exception as e:
        logger.error(f"❌ Failed to load: {e}")
        return None


def load_and_process_metadata(meta_zip: Path, sid: int, temp_meta: Path, logger: logging.Logger) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    logger.info(f"📋 Loading metadata: {meta_zip.name}")
    
    param_meta = pd.DataFrame()
    sensor_meta = pd.DataFrame()
    station_meta = {}
    
    if not meta_zip.exists():
        logger.warning(f"⚠️  Metadata not found: {meta_zip}")
        return param_meta, sensor_meta, station_meta
    
    try:
        meta_files = extract_txt_files_from_zip(meta_zip, temp_meta, logger)
        if not meta_files:
            logger.error(f"❌ No files in {meta_zip}")
            return param_meta, sensor_meta, station_meta
        
        logger.info(f"📄 Extracted {len(meta_files)} files")
        
        # Load parameter metadata
        param_file = next((f for f in meta_files if "Parameter" in f.name), None)
        
        if param_file:
            logger.info(f"📋 Processing: {param_file.name}")
            
            clean_lines = []
            try:
                with open(param_file, "r", encoding="latin-1") as f:
                    for line in f:
                        stripped = line.strip()
                        if stripped and (
                            stripped.startswith("Stations_ID") or stripped.startswith("StationsId") or
                            (stripped[0].isdigit() and stripped.startswith(str(sid).zfill(5)))
                        ):
                            clean_lines.append(stripped)
            except Exception as e:
                logger.error(f"❌ Failed to read: {e}")
            
            if clean_lines:
                cleaned_file = temp_meta / f"{param_file.stem}_cleaned.csv"
                with open(cleaned_file, "w", encoding="latin-1") as f:
                    f.write("\n".join(clean_lines))
                
                param_meta = pd.read_csv(cleaned_file, sep=';', skipinitialspace=True, encoding='latin-1', dtype=str)
                param_meta = param_meta.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
                param_meta = normalize_columns(param_meta, COLUMN_NAME_MAP)
                
                logger.info(f"   ✅ Loaded {len(param_meta)} entries")
            else:
                logger.warning(f"   ⚠️  No data for station {sid}")
        
        # Load sensor metadata
        logger.info("🔧 Loading sensor metadata...")
        sensor_meta = load_sensor_metadata(meta_files, logger)
        
        # Load geographic metadata
        geo_file = next((f for f in meta_files if "Geographie" in f.name), None)
        
        if geo_file:
            logger.info(f"🌍 Processing: {geo_file.name}")
            
            try:
                geo_df = pd.read_csv(geo_file, sep=';', skipinitialspace=True, encoding='latin-1', dtype=str)
                geo_df = geo_df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
                geo_df = normalize_columns(geo_df, COLUMN_NAME_MAP)
                
                station_geo = geo_df[geo_df["station_id"] == str(sid).zfill(5)]
                
                if not station_geo.empty:
                    row = station_geo.iloc[0]
                    station_meta = {
                        "station_id": str(row["station_id"]),
                        "station_name": str(row.get("station_name", "")),
                        "latitude": safe_float_conversion(row.get("latitude", "0")),
                        "longitude": safe_float_conversion(row.get("longitude", "0")),
                        "station_altitude_m": safe_float_conversion(row.get("station_altitude_m", "0")),
                        "state": str(row.get("state", "")),
                        "region": str(row.get("region", ""))
                    }
                    logger.info(f"   ✅ Found: {station_meta['station_name']}")
                else:
                    logger.warning(f"   ⚠️  No data for station {sid}")
                    
            except Exception as e:
                logger.warning(f"⚠️  Failed to process geo metadata: {e}")
        
        return param_meta, sensor_meta, station_meta
        
    except Exception as e:
        logger.error(f"❌ Failed to process metadata: {e}")
        return param_meta, sensor_meta, station_meta


def build_date_ranges(param_meta: pd.DataFrame, raw_df: pd.DataFrame, logger: logging.Logger) -> List[Tuple[date, date]]:
    logger.info("📅 Building date ranges...")
    
    first_date = raw_df["timestamp"].min().normalize().date()
    last_date = raw_df["timestamp"].max().normalize().date()
    logger.info(f"   📊 Data spans: {first_date} to {last_date}")
    
    param_intervals = []
    
    if not param_meta.empty and 'from_date' in param_meta.columns and 'to_date' in param_meta.columns:
        for _, row in param_meta.iterrows():
            try:
                start_str = str(row["from_date"]).strip()
                end_str = str(row["to_date"]).strip()
                
                if start_str and end_str and start_str != 'nan' and end_str != 'nan':
                    start = datetime.strptime(start_str, "%Y%m%d").date()
                    end = datetime.strptime(end_str, "%Y%m%d").date()
                    param_intervals.append((start, end))
            except ValueError:
                continue
    
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
    
    logger.info(f"   ✅ Created {len(ranges)} ranges")
    return ranges


def process_measurements_for_range(raw_df: pd.DataFrame, start_date: date, end_date: date, logger: logging.Logger) -> List[Dict[str, Any]]:
    logger.debug(f"📊 Processing {start_date} to {end_date}")
    
    mask = (raw_df["timestamp"].dt.date >= start_date) & (raw_df["timestamp"].dt.date <= end_date)
    df_range = raw_df.loc[mask]
    
    if df_range.empty:
        logger.warning(f"   ⚠️  No measurements for {start_date} to {end_date}")
        return []
    
    logger.debug(f"   📊 Found {len(df_range):,} measurements")
    
    measurements = []
    
    for _, row in df_range.iterrows():
        try:
            params = {}
            
            for csv_col, eng_name in PARAM_MAP.items():
                if csv_col in row and pd.notna(row[csv_col]):
                    try:
                        raw_val = str(row[csv_col]).strip()
                        
                        if not raw_val or raw_val.lower() in ['nan', 'null', 'none']:
                            continue
                        
                        val = safe_float_conversion(raw_val, float('inf'))
                        
                        if val != float('inf'):
                            params[eng_name] = val
                            
                    except Exception:
                        continue
            
            if params:
                measurements.append({
                    "timestamp": row["timestamp"].strftime("%Y-%m-%dT%H:%M:%S"),
                    "parameters": params
                })
        
        except Exception:
            continue
    
    return measurements


def process_zip(raw_zip_path: Path, station_info_file: Path, calling_logger: Optional[logging.Logger] = None) -> None:
    """Main function to process weather data ZIP file."""
    
    if HAS_LOGGER:
        logger = setup_logger("DWD10TAH3T", script_name="raw_parser")
    else:
        logger = logging.getLogger("raw_parser_DWD10TAH3T")
        logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s — [DWD10TAH3T] — %(levelname)s %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    if calling_logger:
        calling_logger.info(f"🔄 Raw parser starting: {raw_zip_path.name}")
    
    logger.info("=" * 80)
    logger.info(f"🚀 RAW PARSER [DWD10TAH3T]: {raw_zip_path.name}")
    logger.info("=" * 80)
    
    # Extract station ID
    sid = extract_station_id_from_filename(raw_zip_path.stem, logger)
    if sid is None:
        logger.error(f"❌ Cannot extract station ID: {raw_zip_path.name}")
        if calling_logger:
            calling_logger.error(f"❌ Raw parser failed: Cannot extract station ID")
        return
    
    logger.info(f"🏢 Station ID: {sid}")
    
    # Set up paths
    base_folder = raw_zip_path.parent
    meta_folder = base_folder.parent / "meta_data"
    meta_zip = meta_folder / f"Meta_Daten_zehn_min_tu_{str(sid).zfill(5)}.zip"
    
    temp_raw = base_folder / "_temp_raw"
    temp_meta = meta_folder / "_temp_meta"
    temp_raw.mkdir(parents=True, exist_ok=True)
    temp_meta.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"📁 Raw: {raw_zip_path.name}")
    logger.info(f"📁 Meta: {meta_zip.name}")
    
    try:
        # Load raw data
        logger.info("📊 STEP 1: Loading raw data...")
        raw_files = extract_txt_files_from_zip(raw_zip_path, temp_raw, logger)
        raw_df = load_raw_measurement_data(raw_files, logger)
        
        if raw_df is None:
            logger.error("❌ Failed to load raw data")
            if calling_logger:
                calling_logger.error(f"❌ Raw parser failed: Could not load data")
            return
        
        logger.info(f"   ✅ Loaded {len(raw_df):,} measurements")
        
        # Load metadata
        logger.info("📋 STEP 2: Loading metadata...")
        param_meta, sensor_meta, station_meta_fallback = load_and_process_metadata(
            meta_zip, sid, temp_meta, logger
        )
        
        # Load station info
        logger.info("🏢 STEP 3: Loading station info...")
        station_meta = {}
        
        if station_info_file.exists():
            logger.info(f"   📋 Loading from: {station_info_file.name}")
            station_df = parse_station_info_file(station_info_file, logger)
            
            if station_df is not None:
                station_info = get_station_info(station_df, sid, logger)
                if station_info:
                    station_meta = station_info
                    logger.info(f"   ✅ Station: {station_info['station_name']}")
                    logger.info(f"   📍 Location: {station_info['latitude']:.4f}°N, {station_info['longitude']:.4f}°E")
                else:
                    logger.warning(f"   ⚠️  Station {sid} not found")
            else:
                logger.warning("   ❌ Failed to parse station file")
        else:
            logger.warning(f"   ⚠️  Station file not found")
        
        # Fallback to geo metadata
        if not station_meta and station_meta_fallback:
            logger.info("   📍 Using geo metadata fallback")
            station_meta = station_meta_fallback
        
        # Build date ranges
        logger.info("📅 STEP 4: Building date ranges...")
        date_ranges = build_date_ranges(param_meta, raw_df, logger)
        
        if not date_ranges:
            logger.error("❌ No valid date ranges")
            if calling_logger:
                calling_logger.error(f"❌ Raw parser failed: No date ranges")
            return
        
        logger.info(f"   ✅ Created {len(date_ranges)} ranges")
        
        # Prepare output
        logger.info("📝 STEP 5: Preparing output...")
        
        rel_path = raw_zip_path.relative_to(RAW_BASE)
        output_folder = rel_path.parent.name.replace("historical", "parsed_historical")
        out_path = PARSED_BASE / output_folder / f"parsed_{raw_zip_path.stem}.jsonl"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"   📄 Output: {out_path}")
        
        quality_level = 1
        source_ref = {
            "data_zip": raw_zip_path.name,
            "metadata_zip": meta_zip.name,
            "description_pdf": "DESCRIPTION_obsgermany_climate_10min_air_temperature_en.pdf"
        }
        
        # Process measurements
        logger.info("🔄 STEP 6: Processing measurements...")
        
        total_written = 0
        
        with open(out_path, "w", encoding="utf-8") as f_out:
            for i, (start_date, end_date) in enumerate(date_ranges, 1):
                logger.info(f"📅 Range {i}/{len(date_ranges)}: {start_date} to {end_date}")
                
                measurements = process_measurements_for_range(raw_df, start_date, end_date, logger)
                
                if not measurements:
                    logger.warning(f"   ⚠️  No measurements for range")
                    continue
                
                logger.info("   🔧 Loading sensor metadata...")
                sensors = []
                try:
                    date_int = int(start_date.strftime("%Y%m%d"))
                    sensors = parse_sensor_metadata(sensor_meta, sid, date_int, logger)
                    logger.info(f"   ✅ Found {len(sensors)} sensors")
                    
                except Exception as e:
                    logger.warning(f"   ⚠️  Failed sensor metadata: {e}")
                
                output_obj = {
                    "station_id": sid,
                    "quality_level": quality_level,
                    "station_metadata": station_meta,
                    "time_range": {
                        "from": start_date.isoformat(),
                        "to": end_date.isoformat()
                    },
                    "source_reference": source_ref,
                    "sensors": sensors,
                    "measurements": measurements
                }
                
                f_out.write(orjson.dumps(output_obj).decode() + "\n")
                total_written += len(measurements)
                
                logger.info(f"   ✅ Wrote {len(measurements):,} measurements")
        
        # Summary
        logger.info("=" * 80)
        logger.info("✅ RAW PARSER [DWD10TAH3T] COMPLETE")
        logger.info(f"📊 Total written: {total_written:,}")
        logger.info(f"📅 Ranges processed: {len(date_ranges)}")
        logger.info(f"📄 Output: {out_path.name}")
        logger.info(f"💾 Size: {out_path.stat().st_size / 1024 / 1024:.2f} MB")
        logger.info("=" * 80)
        
        if calling_logger:
            calling_logger.info(f"✅ Raw parser completed: {total_written:,} measurements")
        
    except Exception as e:
        logger.error(f"💥 CRITICAL ERROR: {e}")
        logger.error(f"🔍 Traceback: {traceback.format_exc()}")
        if calling_logger:
            calling_logger.error(f"💥 Raw parser failed: {e}")
        raise
        
    finally:
        # Cleanup
        logger.info("🧹 Cleaning up...")
        
        try:
            for temp_dir in [temp_raw, temp_meta]:
                if temp_dir.exists():
                    for file_path in temp_dir.glob("*"):
                        file_path.unlink(missing_ok=True)
                    temp_dir.rmdir()
            
            logger.debug("   ✅ Cleanup complete")
            
        except OSError as e:
            logger.debug(f"   ⚠️  Cleanup warning: {e}")


if __name__ == "__main__":
    """Test the raw parser functionality."""
    print("Testing ClimaStation Raw Data Parser [DWD10TAH3T]...")
    
    if HAS_LOGGER:
        test_logger = setup_logger("DWD10TAH3T", script_name="raw_parser_test")
    else:
        test_logger = logging.getLogger("test_logger")
        test_logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s — [DWD10TAH3T] — %(levelname)s %(message)s')
        handler.setFormatter(formatter)
        test_logger.addHandler(handler)
    
    test_logger.info("✅ Testing utility functions...")
    
    # Test conversions
    assert safe_int_conversion("123", 0) == 123
    assert safe_int_conversion("invalid", 999) == 999
    assert safe_float_conversion("23.5", 0.0) == 23.5
    assert safe_float_conversion("23,5", 0.0) == 23.5
    assert safe_float_conversion("-999", 0.0) == -999.0
    test_logger.info("   ✅ Safe conversions working")
    
    # Test station ID extraction
    test_filename = "10minutenwerte_TU_00003_19930428_19991231_hist.zip"
    sid = extract_station_id_from_filename(test_filename, test_logger)
    assert sid == 3
    test_logger.info("   ✅ Station ID extraction working")
    
    test_logger.info(f"✅ Parameter mappings: {len(PARAM_MAP)} parameters")
    test_logger.info("✅ Raw parser testing complete")