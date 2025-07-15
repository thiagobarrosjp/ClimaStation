"""
Raw Weather Data Parser for German Weather Station Data - CLEAN VERSION

This is the main parsing module that processes German weather station ZIP files
and converts them into structured JSONL format with complete metadata integration.

AUTHOR: ClimaStation Backend Pipeline
VERSION: Clean single-function version
LAST UPDATED: 2025-01-15

KEY FUNCTIONALITY:
- Processes weather data ZIP files containing 10-minute air temperature measurements
- Extracts and parses metadata from companion metadata ZIP files
- Combines raw measurements with sensor specifications and station information
- Handles fixed-width station description files using dedicated parser
- Preserves all measurement values including -999 (missing/invalid data markers)
- Outputs structured JSONL with bilingual metadata and complete sensor descriptions

EXPECTED INPUT STRUCTURE:
data/germany/2_downloaded_files/10_minutes/air_temperature/historical/
├── stundenwerte_TU_00003_19930428_20201231_hist.zip    # Raw measurement data
└── meta_data/
    └── Meta_Daten_zehn_min_tu_00003.zip                # Metadata (sensors, parameters, geography)

EXPECTED OUTPUT STRUCTURE:
data/germany/3_parsed_files/parsed_10_minutes/parsed_air_temperature/parsed_historical/
└── parsed_stundenwerte_TU_00003_19930428_20201231_hist.jsonl

STATION INFO FILE:
data/germany/2_downloaded_files/10_minutes/air_temperature/historical/
└── zehn_min_tu_Beschreibung_Stationen.txt             # Fixed-width station descriptions

USAGE:
    from app.parsing.raw_parser import process_zip
    
    # Process a single ZIP file
    process_zip(zip_path, station_info_file, logger)
"""

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

# Import configuration and utilities with proper paths
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


def safe_int_conversion(value: str, default: int = 0) -> int:
    """
    Safely convert string to int with fallback.
    
    Args:
        value: String value to convert
        default: Default value if conversion fails
        
    Returns:
        Integer value or default
    """
    try:
        return int(str(value).strip())
    except (ValueError, TypeError):
        return default


def safe_float_conversion(value: str, default: float = 0.0) -> float:
    """
    Safely convert string to float with fallback.
    
    Handles German decimal format (comma as decimal separator) and various edge cases.
    
    Args:
        value: String value to convert
        default: Default value if conversion fails
        
    Returns:
        Float value or default
    """
    try:
        # Handle German decimal format and clean whitespace
        cleaned_value = str(value).replace(",", ".").strip()
        if not cleaned_value or cleaned_value.lower() in ['nan', 'null', 'none', '']:
            return default
        return float(cleaned_value)
    except (ValueError, TypeError):
        return default


def normalize_columns(df: pd.DataFrame, column_map: dict) -> pd.DataFrame:
    """
    Normalize DataFrame columns using provided mapping.
    
    This handles the inconsistent column naming in German weather data files
    by applying a standardized mapping and cleaning whitespace.
    
    Args:
        df: DataFrame to normalize
        column_map: Dictionary mapping original names to standardized names
        
    Returns:
        DataFrame with normalized column names
    """
    # Clean column names (remove extra whitespace)
    df.columns = df.columns.str.strip()
    
    # Apply column name mapping
    df.rename(columns=column_map, inplace=True)
    
    return df


def extract_station_id_from_filename(filename: str, logger: logging.Logger) -> Optional[int]:
    """
    Extract station ID from ZIP filename.
    
    Handles both filename formats:
    - stundenwerte_TU_SSSSS_YYYYMMDD_YYYYMMDD_hist.zip
    - 10minutenwerte_TU_SSSSS_YYYYMMDD_YYYYMMDD_hist.zip
    where SSSSS is the 5-digit station ID.
    
    Args:
        filename: ZIP filename to parse
        logger: Logger instance
        
    Returns:
        Station ID as integer or None if extraction fails
    """
    try:
        logger.debug(f"🔍 Extracting station ID from filename: {filename}")
        
        # Split filename and look for station ID
        parts = filename.split("_")
        logger.debug(f"   📋 Filename parts: {parts}")
        
        # Handle different filename formats
        if len(parts) >= 3:
            # Look for the part that comes after "TU"
            for i, part in enumerate(parts):
                if part == "TU" and i + 1 < len(parts):
                    station_id_str = parts[i + 1]
                    station_id = int(station_id_str)
                    logger.debug(f"   ✅ Extracted station ID {station_id} from {filename}")
                    return station_id
            
            # Fallback: try the 3rd component (old logic)
            try:
                station_id = int(parts[2])
                logger.debug(f"   ✅ Extracted station ID {station_id} from {filename} (fallback method)")
                return station_id
            except (ValueError, IndexError):
                pass
        
        logger.error(f"   ❌ Could not extract station ID from filename: {filename}")
        logger.error(f"   📋 Expected format: [prefix]_TU_SSSSS_[dates]_hist.zip")
        return None
        
    except Exception as e:
        logger.error(f"   ❌ Failed to extract station_id from {filename}: {e}")
        return None


def load_raw_measurement_data(raw_txt_files: List[Path], logger: logging.Logger) -> Optional[pd.DataFrame]:
    """
    Load and validate raw measurement data from extracted text files.
    
    Args:
        raw_txt_files: List of extracted text files from raw data ZIP
        logger: Logger instance
        
    Returns:
        DataFrame with raw measurement data or None if loading fails
    """
    if not raw_txt_files:
        logger.error("❌ No text files found in raw data ZIP")
        return None
    
    # Use the first (and typically only) text file
    raw_file = raw_txt_files[0]
    logger.info(f"📄 Loading raw data from: {raw_file.name}")
    
    try:
        # Read CSV with German encoding
        raw_df = pd.read_csv(
            raw_file, 
            sep=';', 
            skipinitialspace=True, 
            encoding='latin-1',
            dtype=str  # Keep everything as strings initially for better control
        )
        
        logger.info(f"📊 Raw data shape: {raw_df.shape}")
        logger.debug(f"📋 Raw CSV columns: {list(raw_df.columns)}")
        
        # Validate essential columns
        if "MESS_DATUM" not in raw_df.columns:
            logger.error(f"❌ Required MESS_DATUM column not found in {raw_file.name}")
            logger.error(f"   Available columns: {list(raw_df.columns)}")
            return None
        
        # Check which measurement columns are available
        available_measurement_cols = [col for col in raw_df.columns if col in PARAM_MAP]
        logger.info(f"📈 Available measurement columns: {available_measurement_cols}")
        
        if not available_measurement_cols:
            logger.warning("⚠️  No recognized measurement columns found")
            logger.debug(f"   Expected columns: {list(PARAM_MAP.keys())}")
            logger.debug(f"   Found columns: {list(raw_df.columns)}")
        
        # Parse timestamps
        logger.info("🕐 Parsing timestamps...")
        raw_df["timestamp"] = pd.to_datetime(raw_df["MESS_DATUM"], format="%Y%m%d%H%M", errors='coerce')
        
        # Count and remove invalid timestamps
        invalid_timestamps = raw_df["timestamp"].isna().sum()
        if invalid_timestamps > 0:
            logger.warning(f"⚠️  Found {invalid_timestamps} invalid timestamps, removing them")
            raw_df = raw_df.dropna(subset=['timestamp'])
        
        if raw_df.empty:
            logger.error(f"❌ No valid timestamps found in {raw_file.name}")
            return None
        
        # Log date range
        first_date = raw_df["timestamp"].min().normalize().date()
        last_date = raw_df["timestamp"].max().normalize().date()
        total_measurements = len(raw_df)
        logger.info(f"📅 Date range: {first_date} to {last_date}")
        logger.info(f"📊 Total measurements: {total_measurements:,}")
        
        # Log sample of measurement values for debugging
        for col in available_measurement_cols:
            if col in raw_df.columns:
                non_null_count = raw_df[col].notna().sum()
                sample_values = raw_df[col].dropna().head(3).tolist()
                logger.debug(f"   {col}: {non_null_count:,} non-null values, sample: {sample_values}")
        
        return raw_df
        
    except Exception as e:
        logger.error(f"❌ Failed to load raw data from {raw_file.name}: {e}")
        logger.error(f"🔍 Traceback: {traceback.format_exc()}")
        return None


def load_and_process_metadata(meta_zip: Path, station_id: int, temp_meta: Path, logger: logging.Logger) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    """
    Load and process all metadata from the metadata ZIP file.
    
    Args:
        meta_zip: Path to metadata ZIP file
        station_id: Station ID for filtering
        temp_meta: Temporary directory for extraction
        logger: Logger instance
        
    Returns:
        Tuple of (parameter_metadata, sensor_metadata, station_geography)
    """
    logger.info(f"📋 Loading metadata from: {meta_zip.name}")
    
    # Initialize return values
    param_meta = pd.DataFrame()
    sensor_meta_df = pd.DataFrame()
    station_meta_row = {}
    
    if not meta_zip.exists():
        logger.warning(f"⚠️  Metadata file not found: {meta_zip}")
        return param_meta, sensor_meta_df, station_meta_row
    
    try:
        # Extract metadata files
        meta_files = extract_txt_files_from_zip(meta_zip, temp_meta)
        if not meta_files:
            logger.error(f"❌ No text files found in {meta_zip}")
            return param_meta, sensor_meta_df, station_meta_row
        
        logger.info(f"📄 Extracted {len(meta_files)} metadata files:")
        for i, file_path in enumerate(meta_files, 1):
            logger.info(f"   {i}. {file_path.name}")
        
        # === LOAD PARAMETER METADATA ===
        param_file = next((f for f in meta_files if "Parameter" in f.name), None)
        
        if param_file:
            logger.info(f"📋 Processing parameter metadata: {param_file.name}")
            
            # Clean parameter file (remove footer and filter for station)
            clean_lines = []
            try:
                with open(param_file, "r", encoding="latin-1") as f:
                    for line in f:
                        stripped = line.strip()
                        if stripped and (
                            # Include header line
                            stripped.startswith("Stations_ID") or stripped.startswith("StationsId") or
                            # Include data lines for this station
                            (stripped[0].isdigit() and stripped.startswith(str(station_id).zfill(5)))
                        ):
                            clean_lines.append(stripped)
            except Exception as e:
                logger.error(f"❌ Failed to read parameter file {param_file}: {e}")
            
            if clean_lines:
                # Write cleaned data to temporary file
                cleaned_param_file = temp_meta / f"{param_file.stem}_cleaned.csv"
                with open(cleaned_param_file, "w", encoding="latin-1") as f:
                    f.write("\n".join(clean_lines))
                
                # Parse as CSV
                param_meta = pd.read_csv(
                    cleaned_param_file, 
                    sep=';', 
                    skipinitialspace=True, 
                    encoding='latin-1', 
                    dtype=str
                )
                
                # Clean and normalize
                param_meta = param_meta.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
                param_meta = normalize_columns(param_meta, COLUMN_NAME_MAP)
                
                logger.info(f"   ✅ Loaded {len(param_meta)} parameter entries")
                
                # Log parameters found
                if 'parameter' in param_meta.columns:
                    unique_params = param_meta['parameter'].unique()
                    logger.info(f"   📈 Parameters: {list(unique_params)}")
            else:
                logger.warning(f"   ⚠️  No valid parameter data found for station {station_id}")
        else:
            logger.error(f"❌ No Parameter file found in {meta_zip}")
        
        # === LOAD SENSOR METADATA ===
        logger.info("🔧 Loading sensor metadata...")
        sensor_meta_df = load_sensor_metadata(meta_files, logger)
        
        # === LOAD GEOGRAPHIC METADATA (FALLBACK) ===
        geo_file = next((f for f in meta_files if "Geographie" in f.name), None)
        
        if geo_file:
            logger.info(f"🌍 Processing geographic metadata: {geo_file.name}")
            
            try:
                geo_df = pd.read_csv(
                    geo_file, 
                    sep=';', 
                    skipinitialspace=True, 
                    encoding='latin-1', 
                    dtype=str
                )
                
                # Clean and normalize
                geo_df = geo_df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
                geo_df = normalize_columns(geo_df, COLUMN_NAME_MAP)
                
                # Filter for this station
                station_geo = geo_df[geo_df["station_id"] == str(station_id).zfill(5)]
                
                if not station_geo.empty:
                    # Use the first matching row
                    row = station_geo.iloc[0]
                    station_meta_row = {
                        "station_id": str(row["station_id"]),
                        "station_name": str(row.get("station_name", "")),
                        "latitude": safe_float_conversion(row.get("latitude", "0")),
                        "longitude": safe_float_conversion(row.get("longitude", "0")),
                        "station_altitude_m": safe_float_conversion(row.get("station_altitude_m", "0")),
                        "state": str(row.get("state", "")),
                        "region": str(row.get("region", ""))
                    }
                    logger.info(f"   ✅ Found geographic info: {station_meta_row['station_name']}")
                else:
                    logger.warning(f"   ⚠️  No geographic data found for station {station_id}")
                    
            except Exception as e:
                logger.warning(f"⚠️  Failed to process geographic metadata: {e}")
        else:
            logger.info("ℹ️  No geographic metadata file found (will use station info file)")
        
        return param_meta, sensor_meta_df, station_meta_row
        
    except Exception as e:
        logger.error(f"❌ Failed to process metadata from {meta_zip}: {e}")
        logger.error(f"🔍 Traceback: {traceback.format_exc()}")
        return param_meta, sensor_meta_df, station_meta_row


def build_date_ranges(param_meta: pd.DataFrame, raw_df: pd.DataFrame, logger: logging.Logger) -> List[Tuple[date, date]]:
    """
    Build date ranges for processing based on parameter metadata and raw data.
    
    This function creates time intervals where sensor configurations were stable,
    allowing us to group measurements with consistent metadata.
    
    Args:
        param_meta: Parameter metadata DataFrame
        raw_df: Raw measurement data DataFrame
        logger: Logger instance
        
    Returns:
        List of (start_date, end_date) tuples for processing
    """
    logger.info("📅 Building date ranges for processing...")
    
    # Get overall data range from raw measurements
    first_date = raw_df["timestamp"].min().normalize().date()
    last_date = raw_df["timestamp"].max().normalize().date()
    logger.info(f"   📊 Raw data spans: {first_date} to {last_date}")
    
    # Build parameter intervals from metadata
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
                    logger.debug(f"   📋 Parameter interval: {start} to {end}")
            except ValueError as e:
                logger.debug(f"   ⚠️  Invalid date format in parameter metadata: {e}")
                continue
    
    # Create comprehensive date range
    all_dates = {first_date, last_date}
    
    # Add parameter interval boundaries
    for start, end in param_intervals:
        all_dates.update([start, end])
    
    # Sort dates and create ranges
    sorted_dates = sorted(all_dates)
    ranges = []
    
    for i in range(len(sorted_dates) - 1):
        start = sorted_dates[i]
        end = sorted_dates[i + 1]
        
        # Avoid duplicate ranges
        if (start, end) not in ranges:
            ranges.append((start, end))
    
    logger.info(f"   ✅ Created {len(ranges)} date ranges for processing")
    for i, (start, end) in enumerate(ranges, 1):
        logger.debug(f"   {i}. {start} to {end}")
    
    return ranges


def process_measurements_for_range(raw_df: pd.DataFrame, start_date: date, end_date: date, logger: logging.Logger) -> List[Dict[str, Any]]:
    """
    Process measurements for a specific date range.
    
    This function extracts and converts measurement data for a given time period,
    preserving all values including -999 (missing/invalid data markers).
    
    Args:
        raw_df: Raw measurement DataFrame
        start_date: Start date for processing
        end_date: End date for processing
        logger: Logger instance
        
    Returns:
        List of measurement dictionaries with timestamp and parameters
    """
    logger.debug(f"📊 Processing measurements for {start_date} to {end_date}")
    
    # Filter data for the date range
    mask = (raw_df["timestamp"].dt.date >= start_date) & (raw_df["timestamp"].dt.date <= end_date)
    df_range = raw_df.loc[mask]
    
    if df_range.empty:
        logger.warning(f"   ⚠️  No measurements found for date range {start_date} to {end_date}")
        return []
    
    logger.debug(f"   📊 Found {len(df_range):,} measurements in date range")
    
    # Track parameter extraction statistics
    param_extraction_counts = {param: 0 for param in PARAM_MAP.values()}
    measurements = []
    
    # Process each measurement row
    for _, row in df_range.iterrows():
        try:
            params = {}
            
            # Extract parameters using PARAM_MAP (CSV column -> English name)
            # IMPORTANT: Keep ALL values including -999 (missing data markers)
            for csv_column, english_name in PARAM_MAP.items():
                if csv_column in row and pd.notna(row[csv_column]):
                    try:
                        raw_value = str(row[csv_column]).strip()
                        
                        # Skip empty or clearly invalid values
                        if not raw_value or raw_value.lower() in ['nan', 'null', 'none']:
                            continue
                        
                        # Convert to float, preserving all numeric values including -999
                        value = safe_float_conversion(raw_value, float('inf'))  # Use infinity as sentinel
                        
                        if value != float('inf'):  # Conversion succeeded
                            params[english_name] = value
                            param_extraction_counts[english_name] += 1
                        else:
                            logger.debug(f"Could not convert {csv_column} value '{raw_value}' to float")
                            
                    except Exception as e:
                        logger.debug(f"Could not process {csv_column} value '{row[csv_column]}': {e}")
                        continue
            
            # Create measurement record (even if some parameters are missing)
            if params:  # Only add if we have at least one parameter
                measurements.append({
                    "timestamp": row["timestamp"].strftime("%Y-%m-%dT%H:%M:%S"),
                    "parameters": params
                })
        
        except Exception as e:
            logger.debug(f"Failed to process measurement row: {e}")
            continue
    
    # Log extraction statistics (only if debug level)
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"   📈 Parameter extraction summary:")
        total_extracted = 0
        for param, count in param_extraction_counts.items():
            if count > 0:
                logger.debug(f"      {param}: {count:,} values")
                total_extracted += count
        
        logger.debug(f"   ✅ Processed {len(measurements):,} measurements with {total_extracted:,} total parameter values")
    
    return measurements


def process_zip(raw_zip_path: Path, station_info_file: Path, logger: logging.Logger) -> None:
    """
    Main function to process a weather data ZIP file.
    
    This is the primary entry point that orchestrates the entire parsing process:
    1. Extract station ID from filename
    2. Set up temporary directories
    3. Load raw measurement data
    4. Load and process metadata
    5. Load station information from description file
    6. Process measurements in date ranges
    7. Generate structured JSONL output
    
    Args:
        raw_zip_path: Path to raw weather data ZIP file
        station_info_file: Path to station description file (fixed-width format)
        logger: Logger instance for detailed logging
        
    Returns:
        None (writes output to JSONL file)
    """
    logger.info("=" * 80)
    logger.info(f"🚀 PROCESSING: {raw_zip_path.name}")
    logger.info("=" * 80)
    
    # === EXTRACT STATION ID ===
    station_id = extract_station_id_from_filename(raw_zip_path.stem, logger)
    if station_id is None:
        logger.error(f"❌ Cannot process file without valid station ID: {raw_zip_path.name}")
        return
    
    logger.info(f"🏢 Station ID: {station_id}")
    
    # === SET UP PATHS ===
    base_folder = raw_zip_path.parent
    meta_folder = base_folder.parent / "meta_data"
    meta_zip = meta_folder / f"Meta_Daten_zehn_min_tu_{str(station_id).zfill(5)}.zip"
    
    # Create temporary directories
    temp_raw = base_folder / "_temp_raw"
    temp_meta = meta_folder / "_temp_meta"
    temp_raw.mkdir(parents=True, exist_ok=True)
    temp_meta.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"📁 Raw ZIP: {raw_zip_path.name}")
    logger.info(f"📁 Meta ZIP: {meta_zip.name}")
    logger.info(f"📁 Station info: {station_info_file.name}")
    
    try:
        # === LOAD RAW MEASUREMENT DATA ===
        logger.info("📊 STEP 1: Loading raw measurement data...")
        raw_txt_files = extract_txt_files_from_zip(raw_zip_path, temp_raw)
        raw_df = load_raw_measurement_data(raw_txt_files, logger)
        
        if raw_df is None:
            logger.error("❌ Failed to load raw measurement data")
            return
        
        # === LOAD METADATA ===
        logger.info("📋 STEP 2: Loading metadata...")
        param_meta, sensor_meta_df, station_meta_fallback = load_and_process_metadata(
            meta_zip, station_id, temp_meta, logger
        )
        
        # === LOAD STATION INFORMATION ===
        logger.info("🏢 STEP 3: Loading station information...")
        station_meta_row = {}
        
        # Try to load from station description file first (preferred)
        if station_info_file.exists():
            logger.info(f"   📋 Loading from station description file: {station_info_file.name}")
            station_df = parse_station_info_file(station_info_file, logger)
            
            if station_df is not None:
                station_info = get_station_info(station_df, station_id, logger)
                if station_info:
                    station_meta_row = station_info
                    logger.info(f"   ✅ Station info: {station_info['station_name']}")
                    logger.info(f"   📍 Location: {station_info['latitude']:.4f}°N, {station_info['longitude']:.4f}°E")
                    logger.info(f"   🏔️  Altitude: {station_info['station_altitude_m']}m")
                else:
                    logger.warning(f"   ⚠️  Station {station_id} not found in station description file")
            else:
                logger.warning("   ❌ Failed to parse station description file")
        else:
            logger.warning(f"   ⚠️  Station description file not found: {station_info_file}")
        
        # Fallback to geographic metadata if station info not found
        if not station_meta_row and station_meta_fallback:
            logger.info("   📍 Using geographic metadata as fallback")
            station_meta_row = station_meta_fallback
        
        # === BUILD DATE RANGES ===
        logger.info("📅 STEP 4: Building processing date ranges...")
        date_ranges = build_date_ranges(param_meta, raw_df, logger)
        
        if not date_ranges:
            logger.error("❌ No valid date ranges found for processing")
            return
        
        # === PREPARE OUTPUT ===
        logger.info("📝 STEP 5: Preparing output...")
        
        # Determine output path
        rel_path = raw_zip_path.relative_to(RAW_BASE)
        output_folder_name = rel_path.parent.name.replace("historical", "parsed_historical")
        out_path = PARSED_BASE / output_folder_name / f"parsed_{raw_zip_path.stem}.jsonl"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"   📄 Output file: {out_path}")
        
        # Prepare common metadata
        quality_level = 1  # Default quality level
        source_reference = {
            "data_zip": raw_zip_path.name,
            "metadata_zip": meta_zip.name,
            "description_pdf": "DESCRIPTION_obsgermany_climate_10min_air_temperature_en.pdf"
        }
        
        # === PROCESS EACH DATE RANGE ===
        logger.info("🔄 STEP 6: Processing measurements by date range...")
        
        total_measurements_written = 0
        
        with open(out_path, "w", encoding="utf-8") as f_out:
            for i, (start_date, end_date) in enumerate(date_ranges, 1):
                logger.info(f"📅 Processing range {i}/{len(date_ranges)}: {start_date} to {end_date}")
                
                # Process measurements for this date range
                measurements = process_measurements_for_range(raw_df, start_date, end_date, logger)
                
                if not measurements:
                    logger.warning(f"   ⚠️  No measurements found for range {start_date} to {end_date}")
                    continue
                
                # Get sensor metadata for this date range
                logger.info("   🔧 Loading sensor metadata for date range...")
                sensors = []
                try:
                    date_int = int(start_date.strftime("%Y%m%d"))
                    sensors = parse_sensor_metadata(sensor_meta_df, station_id, date_int, logger)
                    logger.info(f"   ✅ Found {len(sensors)} sensors for date range")
                    
                    # Log sensor summary
                    if sensors:
                        sensor_types = [s['measured_variable']['en'] for s in sensors]
                        logger.debug(f"      📋 Sensor types: {sensor_types}")
                    
                except Exception as e:
                    logger.warning(f"   ⚠️  Failed to process sensor metadata: {e}")
                
                # Create output object for this date range
                output_obj = {
                    "station_id": station_id,
                    "quality_level": quality_level,
                    "station_metadata": station_meta_row,
                    "time_range": {
                        "from": start_date.isoformat(),
                        "to": end_date.isoformat()
                    },
                    "source_reference": source_reference,
                    "sensors": sensors,
                    "measurements": measurements
                }
                
                # Write to JSONL file
                f_out.write(orjson.dumps(output_obj).decode() + "\n")
                total_measurements_written += len(measurements)
                
                logger.info(f"   ✅ Wrote {len(measurements):,} measurements for date range")
        
        # === FINAL SUMMARY ===
        logger.info("=" * 80)
        logger.info("✅ PROCESSING COMPLETE")
        logger.info(f"📊 Total measurements written: {total_measurements_written:,}")
        logger.info(f"📅 Date ranges processed: {len(date_ranges)}")
        logger.info(f"📄 Output file: {out_path.name}")
        logger.info(f"💾 File size: {out_path.stat().st_size / 1024 / 1024:.2f} MB")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"💥 CRITICAL ERROR processing {raw_zip_path.name}: {e}")
        logger.error(f"🔍 Traceback: {traceback.format_exc()}")
        raise  # Re-raise to allow calling code to handle
        
    finally:
        # === CLEANUP TEMPORARY DIRECTORIES ===
        logger.info("🧹 Cleaning up temporary files...")
        
        try:
            # Remove temporary files
            for temp_dir in [temp_raw, temp_meta]:
                if temp_dir.exists():
                    for file_path in temp_dir.glob("*"):
                        file_path.unlink(missing_ok=True)
                    temp_dir.rmdir()
            
            logger.debug("   ✅ Temporary files cleaned up")
            
        except OSError as e:
            logger.debug(f"   ⚠️  Cleanup warning (non-critical): {e}")


if __name__ == "__main__":
    """
    Test the raw parser functionality when run directly.
    """
    print("Testing ClimaStation Raw Data Parser...")
    
    # Test utility functions
    print("✅ Testing utility functions...")
    
    # Test safe conversions
    assert safe_int_conversion("123", 0) == 123
    assert safe_int_conversion("invalid", 999) == 999
    assert safe_float_conversion("23.5", 0.0) == 23.5
    assert safe_float_conversion("23,5", 0.0) == 23.5  # German format
    assert safe_float_conversion("-999", 0.0) == -999.0  # Preserve -999
    print("   ✅ Safe conversion functions working")
    
    # Test station ID extraction
    test_filename = "10minutenwerte_TU_00003_19930428_19991231_hist.zip"
    
    # Create a proper test logger
    test_logger = logging.getLogger("test_logger")
    test_logger.setLevel(logging.DEBUG)
    
    # Add a null handler to prevent logging output during testing
    null_handler = logging.NullHandler()
    test_logger.addHandler(null_handler)
    
    station_id = extract_station_id_from_filename(test_filename, test_logger)
    assert station_id == 3
    print("   ✅ Station ID extraction working")
    
    # Test parameter mappings
    print(f"✅ Parameter mappings: {len(PARAM_MAP)} parameters")
    for csv_col, english_name in PARAM_MAP.items():
        print(f"   {csv_col} -> {english_name}")
    
    print("✅ Raw parser testing complete")
    print("ℹ️  For full testing, run with actual ZIP files from the main pipeline")
