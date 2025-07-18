"""
Sensor Metadata Parser for German Weather Station Data

SCRIPT IDENTIFICATION: DWD10TAH3S
- DWD: Deutscher Wetterdienst data source
- 10T: 10-minute air temperature dataset
- AH: Air temperature Historical data
- 3: Station processing component
- S: Sensor metadata parsing pipeline component

PURPOSE:
Handles the parsing and processing of sensor metadata from German weather station 
metadata files. Combines parameter metadata with device specifications to create 
comprehensive sensor descriptions with bilingual translations and quality validation.

KEY FUNCTIONALITY:
- Loads parameter metadata (what was measured when) with proper header detection
- Loads device metadata (sensor specifications and calibration info)
- Combines metadata with flexible date matching
- Provides comprehensive sensor descriptions with bilingual translations
- Validates metadata quality and completeness
- Uses centralized configuration from ten_minutes_air_temperature_config.py
- Handles both 'eor' (end of record) and trailing semicolon CSV formats

FIXES APPLIED:
- Added proper header row detection similar to station_info_parser.py
- Fixed CSV parsing to skip header and separator lines
- Enhanced error handling for malformed metadata files
- Added hybrid approach: uses 'eor' when available, falls back to semicolon handling
- Centralized all column mappings using COLUMN_NAME_MAP from config
- Fixed type checking issues with pandas iterrows()
- Improved debugging and logging throughout

USAGE:
    from app.parsing.sensor_metadata import load_sensor_metadata, parse_sensor_metadata
    from app.utils.logger import setup_logger
    
    logger = setup_logger("DWD10TAH3S", script_name="sensor_metadata")
    
    # Load all sensor metadata from files
    sensor_df = load_sensor_metadata(meta_files, logger)
    
    # Parse sensors for specific station and date
    sensors = parse_sensor_metadata(sensor_df, station_id, date_int, logger)

DATA FLOW:
1. load_parameter_metadata() - Parses parameter metadata files with eor/semicolon handling
2. load_device_metadata() - Parses device metadata files with same hybrid approach
3. combine_metadata() - Combines parameter and device data with flexible date matching
4. parse_sensor_metadata() - Extracts sensors for specific station/date with translations
5. validate_sensor_metadata() - Quality checks and validation metrics

SUPPORTED FILE FORMATS:
- Files with 'eor' (end of record) markers - uses eor as definitive boundary
- Files with trailing semicolons - removes empty columns automatically
- Mixed formats - detects and applies appropriate parsing strategy
- Various DWD metadata file structures (Parameter, Geraete, etc.)

AUTHOR: ClimaStation Backend Pipeline
VERSION: Enhanced with script identification codes and new logging system
LAST UPDATED: 2025-01-17
"""

import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import logging
from datetime import datetime, date
import re
import traceback

from app.config.ten_minutes_air_temperature_config import (
    PARAM_NAME_MAP, SENSOR_TYPE_TRANSLATIONS, 
    MEASUREMENT_METHOD_TRANSLATIONS, COLUMN_NAME_MAP
)

# Import logger utility
try:
    from app.utils.logger import setup_logger
    HAS_LOGGER = True
except ImportError:
    HAS_LOGGER = False


def safe_int_conversion(value: str, default: int = 0) -> int:
    """Safely convert string to int with fallback."""
    try:
        return int(str(value).strip())
    except (ValueError, TypeError):
        return default


def safe_float_conversion(value: str, default: float = 0.0) -> float:
    """Safely convert string to float with fallback."""
    try:
        cleaned_value = str(value).replace(",", ".").strip()
        if not cleaned_value or cleaned_value.lower() in ['nan', 'null', 'none', '']:
            return default
        return float(cleaned_value)
    except (ValueError, TypeError):
        return default


def normalize_columns(df: pd.DataFrame, column_map: dict) -> pd.DataFrame:
    """Normalize DataFrame columns using provided mapping."""
    df.columns = df.columns.str.strip()
    df.rename(columns=column_map, inplace=True)
    return df


def detect_data_start_line(file_path: Path, encoding: str = 'latin-1') -> int:
    """
    Detect where actual data starts by finding header and separator lines.
    Similar logic to station_info_parser.py
    
    Args:
        file_path: Path to the metadata file
        encoding: File encoding to use
        
    Returns:
        Line number where data starts (0-based), or 0 if no header detected
    """
    try:
        with open(file_path, 'r', encoding=encoding) as f:
            lines = f.readlines()
        
        header_line = None
        separator_line = None
        
        for i, line in enumerate(lines):
            line_stripped = line.strip()
            
            # Look for header line (contains column names like "Stations_ID", "Parameter", etc.)
            if ('Stations_ID' in line or 'Parameter' in line or 'Stationsname' in line) and ';' in line:
                header_line = i
                continue
            
            # Look for separator line (contains "---" or similar)
            if header_line is not None and ('---' in line_stripped or line_stripped.startswith('-') or 'eor' in line_stripped):
                separator_line = i
                break
        
        if header_line is not None and separator_line is not None:
            return separator_line + 1  # Data starts after separator
        elif header_line is not None:
            return header_line + 1  # Data starts after header if no separator
        else:
            return 0  # No header detected, start from beginning
            
    except Exception as e:
        return 0  # Fallback to start from beginning


def load_parameter_metadata(meta_files: List[Path], logger: logging.Logger) -> pd.DataFrame:
    """
    Load parameter metadata using centralized configuration and hybrid eor/semicolon approach.
    
    This function handles both DWD file formats:
    - Files with 'eor' (end of record) markers - uses eor as definitive boundary
    - Files with trailing semicolons - removes empty columns automatically
    
    Args:
        meta_files: List of metadata file paths
        logger: Logger instance (with DWD10TAH3S code)
        
    Returns:
        DataFrame with parameter metadata or empty DataFrame if not found
    """
    param_file = next((f for f in meta_files if "Parameter" in f.name), None)
    
    if not param_file:
        logger.warning("📋 No Parameter file found in metadata")
        return pd.DataFrame()
    
    try:
        logger.info(f"📋 Loading parameter metadata: {param_file.name}")
        
        # Detect where data starts (skip header and separator lines)
        skip_rows = detect_data_start_line(param_file)
        
        if skip_rows > 0:
            logger.debug(f"   📏 Skipping {skip_rows} header/separator lines")
        
        # Read CSV normally first
        param_df = pd.read_csv(
            param_file, 
            sep=';', 
            skipinitialspace=True, 
            encoding='latin-1', 
            dtype=str,
            skiprows=skip_rows,
            header=None
        )
        
        # Remove completely empty rows
        param_df = param_df.dropna(how='all')
        
        if param_df.empty:
            logger.warning("   ⚠️  No data found in parameter file")
            return pd.DataFrame()
        
        logger.debug(f"   📊 Raw data shape: {param_df.shape}")
        
        # STEP 1: Check if 'eor' is present in the data
        has_eor = False
        eor_column = None
        
        for col_idx in range(len(param_df.columns)):
            if param_df.iloc[:, col_idx].astype(str).str.strip().eq('eor').any():
                has_eor = True
                eor_column = col_idx
                logger.debug(f"   📍 Found 'eor' column at index {eor_column}")
                break
        
        # STEP 2: Apply appropriate parsing strategy
        if has_eor and eor_column is not None:
            logger.debug("   🎯 Using 'eor' boundary approach")
            param_df = param_df.iloc[:, :eor_column + 1]
            logger.debug(f"   ✂️  Trimmed to {param_df.shape[1]} columns using 'eor' boundary")
        else:
            logger.debug("   🔧 Using trailing semicolon approach (no 'eor' found)")
            # Handle trailing semicolons by removing empty columns
            param_df = param_df.dropna(axis=1, how='all')
            
            # Remove columns that only contain empty strings
            for col in param_df.columns:
                if param_df[col].astype(str).str.strip().eq('').all():
                    param_df = param_df.drop(columns=[col])
            
            logger.debug(f"   ✂️  Cleaned to {param_df.shape[1]} columns after removing empty columns")
        
        # STEP 3: Remove footer rows (common to both approaches)
        footer_indicators = ['generiert:', 'Legende:', 'Deutscher Wetterdienst', '--']
        mask = ~param_df.iloc[:, 0].astype(str).str.contains('|'.join(footer_indicators), na=False, case=False)
        param_df = param_df[mask]
        
        # Remove rows where the first column is 'eor' (pure footer markers)
        param_df = param_df[param_df.iloc[:, 0].astype(str).str.strip() != 'eor']
        
        if param_df.empty:
            logger.warning("   ⚠️  No data rows found after filtering")
            return pd.DataFrame()
        
        # STEP 4: Assign raw column names based on expected DWD parameter metadata structure
        actual_columns = len(param_df.columns)
        logger.debug(f"   📊 Final data shape: {param_df.shape}")
        
        # Use the standard DWD parameter metadata column order
        # Based on typical DWD format: Stations_ID;Von_Datum;Bis_Datum;Stationsname;Parameter;Parameterbeschreibung;Einheit;Datenquelle;Zusatz-Info;Besonderheiten;Literaturhinweis;eor;
        raw_column_names = [
            'Stations_ID',                              # Column 0
            'Von_Datum',                               # Column 1
            'Bis_Datum',                               # Column 2
            'Stationsname',                            # Column 3
            'Parameter',                               # Column 4
            'Parameterbeschreibung',                   # Column 5
            'Einheit',                                 # Column 6
            'Datenquelle (Strukturversion=SV)',        # Column 7
            'Zusatz-Info',                             # Column 8
            'Besonderheiten',                          # Column 9
            'Literaturhinweis',                        # Column 10
            'eor'                                      # Column 11 (if present)
        ]
        
        # Use only as many names as we have columns
        column_names = raw_column_names[:actual_columns]
        param_df.columns = column_names
        
        logger.debug(f"   📋 Assigned raw columns: {list(param_df.columns)}")
        
        # STEP 5: Clean the data
        param_df = param_df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
        
        # Remove rows where eor column actually contains 'eor' (these are footers)
        if 'eor' in param_df.columns:
            param_df = param_df[param_df['eor'].astype(str).str.strip() != 'eor']
        
        # STEP 6: Apply the centralized column name mapping from config
        param_df = normalize_columns(param_df, COLUMN_NAME_MAP)
        logger.debug(f"   🔄 Normalized columns: {list(param_df.columns)}")
        
        logger.info(f"   ✅ Loaded {len(param_df)} parameter entries")
        logger.debug(f"   📊 Parsing strategy: {'eor-based' if has_eor else 'semicolon-based'}")
        
        # Debug: Show sample data (fixed type checking issue)
        if not param_df.empty and logger.level <= logging.DEBUG:
            logger.debug("   📋 Sample parameter entries:")
            for idx, (i, row) in enumerate(param_df.head(3).iterrows(), 1):
                station_id = row.get('station_id', 'N/A')
                parameter = row.get('parameter', 'N/A')
                logger.debug(f"      {idx}. Station {station_id}: {parameter}")
        
        return param_df
        
    except Exception as e:
        logger.error(f"❌ Failed to load parameter metadata: {e}")
        logger.debug(f"   📋 Full error: {traceback.format_exc()}")
        return pd.DataFrame()


def load_device_metadata(meta_files: List[Path], logger: logging.Logger) -> pd.DataFrame:
    """
    Load device metadata using centralized configuration and hybrid approach.
    
    Args:
        meta_files: List of metadata file paths
        logger: Logger instance (with DWD10TAH3S code)
        
    Returns:
        DataFrame with device metadata or empty DataFrame if not found
    """
    device_file = next((f for f in meta_files if "Geraete" in f.name), None)
    
    if not device_file:
        logger.warning("🔧 No Geraete (device) file found in metadata")
        return pd.DataFrame()
    
    try:
        logger.info(f"🔧 Loading device metadata: {device_file.name}")
        
        skip_rows = detect_data_start_line(device_file)
        
        if skip_rows > 0:
            logger.debug(f"   📏 Skipping {skip_rows} header/separator lines")
        
        # Read CSV normally
        device_df = pd.read_csv(
            device_file, 
            sep=';', 
            skipinitialspace=True, 
            encoding='latin-1', 
            dtype=str,
            skiprows=skip_rows,
            header=None
        )
        
        # Remove empty rows
        device_df = device_df.dropna(how='all')
        
        if device_df.empty:
            logger.warning("   ⚠️  No data found in device file")
            return pd.DataFrame()
        
        # Check for 'eor' presence
        has_eor = False
        eor_column = None
        
        for col_idx in range(len(device_df.columns)):
            if device_df.iloc[:, col_idx].astype(str).str.strip().eq('eor').any():
                has_eor = True
                eor_column = col_idx
                logger.debug(f"   📍 Found 'eor' column at index {eor_column}")
                break
        
        # Apply appropriate strategy
        if has_eor and eor_column is not None:
            logger.debug("   🎯 Using 'eor' boundary approach")
            device_df = device_df.iloc[:, :eor_column + 1]
        else:
            logger.debug("   🔧 Using trailing semicolon approach")
            device_df = device_df.dropna(axis=1, how='all')
            for col in device_df.columns:
                if device_df[col].astype(str).str.strip().eq('').all():
                    device_df = device_df.drop(columns=[col])
        
        # Remove footer rows
        footer_indicators = ['generiert:', 'Deutscher Wetterdienst', '--']
        mask = ~device_df.iloc[:, 0].astype(str).str.contains('|'.join(footer_indicators), na=False, case=False)
        device_df = device_df[mask]
        
        device_df = device_df[device_df.iloc[:, 0].astype(str).str.strip() != 'eor']
        
        if device_df.empty:
            logger.warning("   ⚠️  No data rows found after filtering")
            return pd.DataFrame()
        
        # Assign raw column names based on typical DWD device metadata structure
        actual_columns = len(device_df.columns)
        logger.debug(f"   📊 Final device data shape: {device_df.shape}")
        
        # Standard DWD device metadata columns (based on typical structure)
        raw_column_names = [
            'Stations_ID',                    # Column 0
            'Stationsname',                   # Column 1
            'Geo. Laenge [Grad]',            # Column 2
            'Geo. Breite [Grad]',            # Column 3
            'Stationshoehe [m]',             # Column 4
            'Geberhoehe ueber Grund [m]',    # Column 5
            'Von_Datum',                     # Column 6
            'Bis_Datum',                     # Column 7
            'Geraetetyp Name',               # Column 8
            'Messverfahren',                 # Column 9
            'eor'                            # Column 10 (if present)
        ]
        
        column_names = raw_column_names[:actual_columns]
        device_df.columns = column_names
        
        logger.debug(f"   📋 Assigned raw columns: {list(device_df.columns)}")
        
        # Clean data
        device_df = device_df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
        
        if 'eor' in device_df.columns:
            device_df = device_df[device_df['eor'].astype(str).str.strip() != 'eor']
        
        # Apply centralized column name mapping
        device_df = normalize_columns(device_df, COLUMN_NAME_MAP)
        logger.debug(f"   🔄 Normalized columns: {list(device_df.columns)}")
        
        logger.info(f"   ✅ Loaded {len(device_df)} device entries")
        logger.debug(f"   📊 Parsing strategy: {'eor-based' if has_eor else 'semicolon-based'}")
        
        return device_df
        
    except Exception as e:
        logger.error(f"❌ Failed to load device metadata: {e}")
        logger.debug(f"   📋 Full error: {traceback.format_exc()}")
        return pd.DataFrame()


def combine_metadata(param_df: pd.DataFrame, device_df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Combine parameter and device metadata with flexible date matching.
    
    Args:
        param_df: Parameter metadata DataFrame
        device_df: Device metadata DataFrame
        logger: Logger instance (with DWD10TAH3S code)
        
    Returns:
        Combined DataFrame with sensor metadata
    """
    if param_df.empty:
        logger.warning("⚠️  No parameter metadata to combine")
        return pd.DataFrame()
    
    if device_df.empty:
        logger.warning("⚠️  No device metadata to combine - using parameter data only")
        return param_df
    
    logger.info("🔗 Combining parameter and device metadata...")
    
    combined_rows = []
    successful_matches = 0
    
    for _, param_row in param_df.iterrows():
        try:
            station_id = str(param_row.get('station_id', '')).strip()
            parameter = str(param_row.get('parameter', '')).strip()
            param_from_date = str(param_row.get('from_date', '')).strip()
            param_to_date = str(param_row.get('to_date', '')).strip()
            
            # Skip rows with missing essential data
            if not all([station_id, parameter, param_from_date, param_to_date]):
                logger.debug(f"Skipping row with missing data: station_id={station_id}, parameter={parameter}")
                continue
            
            # Skip obvious header remnants
            if (station_id.lower() in ['stations_id', 'station_id'] or 
                parameter.lower() in ['parameter', 'parameterbeschreibung']):
                logger.debug(f"Skipping header row: station_id={station_id}, parameter={parameter}")
                continue
            
            # Convert parameter dates to integers for comparison
            try:
                param_from_int = int(param_from_date)
                param_to_int = int(param_to_date)
            except ValueError:
                logger.debug(f"Invalid date format: from={param_from_date}, to={param_to_date}")
                continue
            
            # Find matching device entries with flexible date matching
            device_matches = device_df[
                (device_df['station_id'].astype(str).str.strip() == station_id) &
                (device_df['parameter'].astype(str).str.strip() == parameter)
            ]
            
            best_match = None
            best_overlap = 0
            
            for _, device_row in device_matches.iterrows():
                try:
                    device_from_str = str(device_row.get('from_date', '')).strip()
                    device_to_str = str(device_row.get('to_date', '')).strip()
                    
                    if not device_from_str or not device_to_str:
                        continue
                    
                    device_from_int = int(device_from_str)
                    device_to_int = int(device_to_str)
                    
                    # Calculate overlap between parameter and device date ranges
                    overlap_start = max(param_from_int, device_from_int)
                    overlap_end = min(param_to_int, device_to_int)
                    overlap = max(0, overlap_end - overlap_start)
                    
                    if overlap > best_overlap:
                        best_overlap = overlap
                        best_match = device_row
                        
                except ValueError:
                    continue
            
            # Create combined row
            combined_row = param_row.copy()
            
            if best_match is not None:
                # Add device information
                combined_row['sensor_type'] = str(best_match.get('sensor_type', '')).strip()
                combined_row['measurement_method'] = str(best_match.get('measurement_method', '')).strip()
                combined_row['sensor_height_m'] = str(best_match.get('sensor_height_m', '0')).strip()
                successful_matches += 1
            else:
                # No device match found - use defaults
                combined_row['sensor_type'] = ''
                combined_row['measurement_method'] = ''
                combined_row['sensor_height_m'] = '0'
            
            combined_rows.append(combined_row)
            
        except Exception as e:
            # More informative error logging
            param_info = str(param_row.get('parameter', 'unknown'))[:50]
            station_info = str(param_row.get('station_id', 'unknown'))
            logger.debug(f"Failed to process parameter row: Station {station_info}, Parameter {param_info} - Error: {e}")
            continue
    
    if combined_rows:
        combined_df = pd.DataFrame(combined_rows)
        logger.info(f"   ✅ Combined {len(combined_df)} sensor entries")
        logger.info(f"   📊 Device matches: {successful_matches}/{len(combined_df)} ({successful_matches/len(combined_df)*100:.1f}%)")
        return combined_df
    else:
        logger.warning("⚠️  No successful metadata combinations")
        return pd.DataFrame()


def load_sensor_metadata(meta_files: List[Path], logger: logging.Logger, parent_log_file: Optional[Path] = None) -> pd.DataFrame:
    """
    Load and combine all sensor metadata from metadata files.
    
    This is the main function that orchestrates loading parameter metadata,
    device metadata, and combining them into a comprehensive sensor dataset.
    
    Args:
        meta_files: List of extracted metadata file paths
        logger: Logger instance (with DWD10TAH3S code for traceability)
        parent_log_file: Optional path to parent script's log file for centralized logging
        
    Returns:
        DataFrame with combined sensor metadata
    """
    # If we have a parent log file, create a new logger that writes to it
    if parent_log_file and HAS_LOGGER:
        logger = setup_logger("DWD10TAH3S", script_name="sensor_metadata", parent_log_file=parent_log_file)
    
    logger.info("🔧 Loading sensor metadata...")
    
    if not meta_files:
        logger.warning("⚠️  No metadata files provided")
        return pd.DataFrame()
    
    # Load parameter metadata
    param_df = load_parameter_metadata(meta_files, logger)
    
    # Load device metadata
    device_df = load_device_metadata(meta_files, logger)
    
    # Combine metadata
    sensor_df = combine_metadata(param_df, device_df, logger)
    
    if not sensor_df.empty:
        logger.info(f"✅ Sensor metadata loading complete: {len(sensor_df)} entries")
        
        # Log summary statistics
        unique_stations = sensor_df['station_id'].nunique() if 'station_id' in sensor_df.columns else 0
        unique_parameters = sensor_df['parameter'].nunique() if 'parameter' in sensor_df.columns else 0
        logger.info(f"   📊 Unique stations: {unique_stations}")
        logger.info(f"   📊 Unique parameters: {unique_parameters}")
    else:
        logger.warning("⚠️  No sensor metadata loaded")
    
    return sensor_df


def parse_sensor_metadata(sensor_df: pd.DataFrame, station_id: int, date_int: int, logger: logging.Logger) -> List[Dict[str, Any]]:
    """
    Parse sensor metadata for a specific station and date.
    
    Args:
        sensor_df: Sensor metadata DataFrame from load_sensor_metadata()
        station_id: Station ID to filter for
        date_int: Date as integer (YYYYMMDD format)
        logger: Logger instance (with DWD10TAH3S code)
        
    Returns:
        List of sensor dictionaries with bilingual descriptions
        
    Example:
        sensors = parse_sensor_metadata(sensor_df, 3, 19930428, logger)
        # Returns: [{"measured_variable": {"en": "air temperature 2 m above ground", "de": "..."}, ...}]
    """
    logger.debug(f"🔍 Parsing sensors for station {station_id} on date {date_int}")
    
    if sensor_df.empty:
        logger.debug("   ❌ No sensor metadata available")
        return []
    
    # Filter for the specific station
    station_df = sensor_df[sensor_df['station_id'].astype(str).str.strip() == str(station_id).zfill(5)]
    
    if station_df.empty:
        logger.debug(f"   ❌ No sensor metadata found for station {station_id}")
        return []
    
    sensors = []
    
    # Process each sensor entry
    for _, row in station_df.iterrows():
        try:
            # Parse date range
            von_str = str(row['from_date']).strip()
            bis_str = str(row['to_date']).strip()
            
            if not von_str or not bis_str or von_str == 'nan' or bis_str == 'nan':
                continue
            
            von = int(von_str)
            bis = int(bis_str)
            
            # Check if date falls within range (with 1-day tolerance for flexibility)
            if (von - 1) <= date_int <= (bis + 1):
                param_raw = str(row.get('parameter', '')).strip()
                
                # Get parameter translation
                param_entry = PARAM_NAME_MAP.get(param_raw, {"en": param_raw, "de": param_raw})

                # Get sensor type with translation
                sensor_type_de = str(row.get("sensor_type", "")).strip()
                sensor_type_entry = SENSOR_TYPE_TRANSLATIONS.get(
                    sensor_type_de, 
                    {"en": sensor_type_de, "de": sensor_type_de}
                )

                # Get measurement method with translation
                method_de = str(row.get("measurement_method", "")).strip()
                method_entry = MEASUREMENT_METHOD_TRANSLATIONS.get(
                    method_de, 
                    {"en": method_de, "de": method_de}
                )

                # Parse sensor height
                height_str = str(row.get("sensor_height_m", "0")).replace(",", ".")
                try:
                    sensor_height = float(height_str) if height_str and height_str != 'nan' else 0.0
                except (ValueError, TypeError):
                    sensor_height = 0.0

                sensor_dict = {
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
                    "sensor_height_m": sensor_height
                }
                
                sensors.append(sensor_dict)

        except (ValueError, TypeError) as e:
            logger.debug(f"   ❌ Failed to process sensor row: {e}")
            continue

    logger.debug(f"   📊 Found {len(sensors)} sensors for station {station_id} on date {date_int}")
    return sensors


def validate_sensor_metadata(sensor_df: pd.DataFrame, logger: logging.Logger) -> Dict[str, Any]:
    """
    Validate sensor metadata quality and completeness.
    
    Args:
        sensor_df: Sensor metadata DataFrame
        logger: Logger instance (with DWD10TAH3S code)
        
    Returns:
        Dictionary with validation results and quality metrics
    """
    logger.info("🔍 Validating sensor metadata quality...")
    
    if sensor_df.empty:
        return {
            "valid": False,
            "reason": "No sensor metadata available",
            "quality_score": 0.0,
            "metrics": {}
        }
    
    metrics = {}
    issues = []
    
    # Check required columns
    required_columns = ['station_id', 'parameter', 'from_date', 'to_date']
    missing_columns = [col for col in required_columns if col not in sensor_df.columns]
    
    if missing_columns:
        issues.append(f"Missing required columns: {missing_columns}")
        metrics['missing_columns'] = missing_columns
    
    # Check data completeness
    total_rows = len(sensor_df)
    
    for col in required_columns:
        if col in sensor_df.columns:
            null_count = sensor_df[col].isna().sum()
            null_percentage = (null_count / total_rows) * 100
            metrics[f'{col}_null_percentage'] = null_percentage
            
            if null_percentage > 10:  # More than 10% null values
                issues.append(f"High null percentage in {col}: {null_percentage:.1f}%")
    
    # Check date format validity
    if 'from_date' in sensor_df.columns and 'to_date' in sensor_df.columns:
        invalid_dates = 0
        for _, row in sensor_df.iterrows():
            try:
                from_date = str(row['from_date']).strip()
                to_date = str(row['to_date']).strip()
                
                if from_date and to_date and from_date != 'nan' and to_date != 'nan':
                    int(from_date)  # Test conversion
                    int(to_date)
                else:
                    invalid_dates += 1
            except (ValueError, TypeError):
                invalid_dates += 1
        
        invalid_date_percentage = (invalid_dates / total_rows) * 100
        metrics['invalid_date_percentage'] = invalid_date_percentage
        
        if invalid_date_percentage > 5:  # More than 5% invalid dates
            issues.append(f"High invalid date percentage: {invalid_date_percentage:.1f}%")
    
    # Calculate quality score
    quality_score = 100.0
    
    # Deduct points for issues
    if missing_columns:
        quality_score -= 30.0
    
    for col in required_columns:
        if col in metrics:
            null_pct = metrics.get(f'{col}_null_percentage', 0)
            quality_score -= min(null_pct, 20.0)  # Max 20 points deduction per column
    
    invalid_date_pct = metrics.get('invalid_date_percentage', 0)
    quality_score -= min(invalid_date_pct * 2, 30.0)  # Max 30 points for date issues
    
    quality_score = max(0.0, quality_score)  # Ensure non-negative
    
    # Determine overall validity
    is_valid = quality_score >= 70.0 and not missing_columns
    
    result = {
        "valid": is_valid,
        "quality_score": quality_score,
        "total_entries": total_rows,
        "issues": issues,
        "metrics": metrics
    }
    
    if is_valid:
        logger.info(f"   ✅ Sensor metadata validation passed (score: {quality_score:.1f}/100)")
    else:
        logger.warning(f"   ⚠️  Sensor metadata validation issues (score: {quality_score:.1f}/100)")
        for issue in issues:
            logger.warning(f"      - {issue}")
    
    return result


if __name__ == "__main__":
    """
    Test the sensor metadata functionality when run directly.
    """
    print("Testing ClimaStation Sensor Metadata Parser [DWD10TAH3S]...")
    
    # Set up test logger with script identification
    if HAS_LOGGER:
        test_logger = setup_logger("DWD10TAH3S", script_name="sensor_metadata_test")
    else:
        # Fallback logger for testing
        test_logger = logging.getLogger("test_logger")
        test_logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s — %(levelname)s — [DWD10TAH3S] %(message)s')
        handler.setFormatter(formatter)
        test_logger.addHandler(handler)
    
    # Test utility functions
    test_logger.info("✅ Testing utility functions...")
    
    assert safe_int_conversion("123", 0) == 123
    assert safe_int_conversion("invalid", 999) == 999
    assert safe_float_conversion("23.5", 0.0) == 23.5
    assert safe_float_conversion("23,5", 0.0) == 23.5  # German format
    test_logger.info("   ✅ Utility functions working")
    
    # Test parameter mappings
    test_logger.info(f"✅ Parameter mappings available: {len(PARAM_NAME_MAP)} parameters")
    
    test_logger.info("✅ Sensor metadata testing complete")
    test_logger.info("ℹ️  For full testing, run with actual metadata files from the pipeline")
