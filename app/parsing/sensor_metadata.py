"""
Sensor Metadata Parser - Complete Fixed Version

This script processes sensor metadata from German weather station ZIP files.
It combines parameter metadata with sensor device metadata to create complete sensor information.

KEY FIX: 
- Parameter information comes from Metadaten_Parameter_*.txt files
- Sensor device information comes from Metadaten_Geraete_*.txt files  
- We need to combine them by matching parameter names in filenames

Expected Input:
- Metadata files from ZIP archives
- Parameter files: contain parameter names, descriptions, date ranges
- Device files: contain sensor types, measurement methods, heights

Expected Output:
- List of sensor dictionaries with complete bilingual descriptions
"""

import io
from pathlib import Path
from typing import List, Dict, Tuple
import pandas as pd

from config.ten_minutes_air_temperature_config import (
    PARAM_NAME_MAP, COLUMN_NAME_MAP,
    SENSOR_TYPE_TRANSLATIONS,
    MEASUREMENT_METHOD_TRANSLATIONS
)


def load_parameter_metadata(meta_files: List[Path], logger) -> pd.DataFrame:
    """
    Load parameter metadata from Metadaten_Parameter files.
    
    Args:
        meta_files: List of metadata file paths
        logger: Logger instance for debugging
        
    Returns:
        DataFrame with parameter information
    """
    param_files = [f for f in meta_files if "Parameter" in f.name]
    
    if not param_files:
        logger.warning("No parameter metadata files found")
        return pd.DataFrame()
    
    all_frames = []
    
    for path in param_files:
        try:
            logger.info(f"📋 Loading parameter metadata from {path.name}")
            
            # Read and clean the file
            with open(path, encoding='latin-1') as f:
                lines = []
                for line in f:
                    if line.startswith("generiert") or line.startswith("Legende"):
                        break
                    lines.append(line)
            
            df = pd.read_csv(
                io.StringIO("".join(lines)),
                sep=';',
                skipinitialspace=True,
                dtype=str
            )
            
            # Normalize columns
            df.columns = [col.strip().lower() for col in df.columns]
            df.rename(columns=COLUMN_NAME_MAP, inplace=True)
            df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
            
            all_frames.append(df)
            logger.info(f"   ✅ Loaded {len(df)} parameter entries")
            
        except Exception as e:
            logger.error(f"❌ Failed to load parameter metadata from {path.name}: {e}")
    
    if all_frames:
        combined = pd.concat(all_frames, ignore_index=True)
        logger.info(f"📊 Combined parameter metadata: {len(combined)} total entries")
        return combined
    else:
        return pd.DataFrame()


def load_device_metadata(meta_files: List[Path], logger) -> Dict[str, pd.DataFrame]:
    """
    Load device metadata from Metadaten_Geraete files.
    
    Args:
        meta_files: List of metadata file paths
        logger: Logger instance for debugging
        
    Returns:
        Dictionary mapping parameter names to device DataFrames
    """
    device_files = [f for f in meta_files if "Metadaten_Geraete" in f.name]
    
    if not device_files:
        logger.warning("No device metadata files found")
        return {}
    
    device_data = {}
    
    for path in device_files:
        try:
            logger.info(f"🔧 Loading device metadata from {path.name}")
            
            # Extract parameter name from filename
            # e.g., "Metadaten_Geraete_Lufttemperatur_00003.txt" -> "Lufttemperatur"
            filename_parts = path.stem.split('_')
            if len(filename_parts) >= 3:
                param_name = filename_parts[2]  # "Lufttemperatur", "Rel_Feuchte", etc.
                logger.info(f"   📋 Parameter from filename: '{param_name}'")
            else:
                logger.warning(f"   ⚠️  Could not extract parameter from filename: {path.name}")
                continue
            
            # Read and clean the file
            with open(path, encoding='latin-1') as f:
                lines = []
                for line in f:
                    if line.startswith("generiert") or line.startswith("Legende"):
                        break
                    lines.append(line)
            
            df = pd.read_csv(
                io.StringIO("".join(lines)),
                sep=';',
                skipinitialspace=True,
                dtype=str
            )
            
            # Normalize columns
            df.columns = [col.strip().lower() for col in df.columns]
            
            # Create device-specific column mapping
            device_column_map = {
                **COLUMN_NAME_MAP,
                'stations_id': 'station_id',
                'von_datum': 'from_date',
                'bis_datum': 'to_date',
                'geberhoehe ueber grund [m]': 'sensor_height_m',
                'geraetetyp name': 'sensor_type',
                'messverfahren': 'measurement_method'
            }
            
            df.rename(columns=device_column_map, inplace=True)
            df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
            
            # Add the parameter name to each row
            df['parameter'] = param_name
            
            device_data[param_name] = df
            logger.info(f"   ✅ Loaded {len(df)} device entries for {param_name}")
            
        except Exception as e:
            logger.error(f"❌ Failed to load device metadata from {path.name}: {e}")
    
    logger.info(f"📊 Loaded device metadata for {len(device_data)} parameters: {list(device_data.keys())}")
    return device_data


def combine_metadata(param_df: pd.DataFrame, device_data: Dict[str, pd.DataFrame], logger) -> pd.DataFrame:
    """
    Combine parameter and device metadata.
    
    Args:
        param_df: Parameter metadata DataFrame
        device_data: Dictionary of device metadata DataFrames
        logger: Logger instance
        
    Returns:
        Combined DataFrame with complete sensor information
    """
    if param_df.empty:
        logger.warning("No parameter metadata to combine")
        return pd.DataFrame()
    
    combined_rows = []
    
    for _, param_row in param_df.iterrows():
        param_name = param_row.get('parameter', '')
        station_id = param_row.get('station_id', '')
        param_from = param_row.get('from_date', '')
        param_to = param_row.get('to_date', '')
        
        logger.debug(f"🔍 Processing parameter: {param_name} for station {station_id} ({param_from}-{param_to})")
        
        # Find matching device data
        device_df = None
        
        # Try exact match first
        if param_name in device_data:
            device_df = device_data[param_name]
        else:
            # Try to find by mapping parameter names
            # Map CSV parameter names to device file names
            param_to_device_map = {
                'TT_10': 'Lufttemperatur',
                'TM5_10': 'Momentane_Temperatur_In_5cm', 
                'RF_10': 'Rel_Feuchte',
                'PP_10': 'Luftdruck',  # Might not exist
                'TD_10': 'Taupunkt'    # Might not exist
            }
            
            device_name = param_to_device_map.get(param_name)
            if device_name and device_name in device_data:
                device_df = device_data[device_name]
                logger.debug(f"   📋 Mapped {param_name} → {device_name}")
        
        if device_df is not None:
            # Find device entries for this station and overlapping time period
            station_devices = device_df[device_df['station_id'].astype(str).str.strip() == str(station_id)]
            
            for _, device_row in station_devices.iterrows():
                device_from = device_row.get('from_date', '')
                device_to = device_row.get('to_date', '')
                
                # Check for date overlap (simplified - could be more sophisticated)
                try:
                    param_from_int = int(param_from) if param_from else 0
                    param_to_int = int(param_to) if param_to else 99999999
                    device_from_int = int(device_from) if device_from else 0
                    device_to_int = int(device_to) if device_to else 99999999
                    
                    # Check for overlap
                    if param_from_int <= device_to_int and param_to_int >= device_from_int:
                        # Combine the data
                        combined_row = {
                            'station_id': station_id,
                            'parameter': param_name,
                            'from_date': max(param_from, device_from) if param_from and device_from else (param_from or device_from),
                            'to_date': min(param_to, device_to) if param_to and device_to else (param_to or device_to),
                            'sensor_type': device_row.get('sensor_type', ''),
                            'measurement_method': device_row.get('measurement_method', ''),
                            'sensor_height_m': device_row.get('sensor_height_m', '0')
                        }
                        combined_rows.append(combined_row)
                        logger.debug(f"   ✅ Combined {param_name} with device data ({device_from}-{device_to})")
                
                except (ValueError, TypeError):
                    logger.debug(f"   ⚠️  Date parsing error for {param_name}")
                    continue
        else:
            logger.debug(f"   ❌ No device data found for parameter {param_name}")
            # Create entry without device info
            combined_row = {
                'station_id': station_id,
                'parameter': param_name,
                'from_date': param_from,
                'to_date': param_to,
                'sensor_type': '',
                'measurement_method': '',
                'sensor_height_m': '0'
            }
            combined_rows.append(combined_row)
    
    if combined_rows:
        combined_df = pd.DataFrame(combined_rows)
        logger.info(f"📊 Combined metadata: {len(combined_df)} sensor entries")
        return combined_df
    else:
        logger.warning("No combined metadata created")
        return pd.DataFrame()


def load_sensor_metadata(meta_files: List[Path], logger) -> pd.DataFrame:
    """
    Load and combine sensor metadata from multiple files.
    
    Args:
        meta_files: List of metadata file paths
        logger: Logger instance for debugging
        
    Returns:
        Combined DataFrame with complete sensor metadata
    """
    logger.info(f"🔍 Loading sensor metadata from {len(meta_files)} files")
    
    # Load parameter metadata
    param_df = load_parameter_metadata(meta_files, logger)
    
    # Load device metadata
    device_data = load_device_metadata(meta_files, logger)
    
    # Combine them
    combined_df = combine_metadata(param_df, device_data, logger)
    
    logger.info(f"✅ Final sensor metadata: {len(combined_df)} entries")
    if not combined_df.empty:
        logger.info(f"   📋 Columns: {list(combined_df.columns)}")
        logger.info(f"   📊 Parameters: {combined_df['parameter'].unique().tolist()}")
    
    return combined_df


def parse_sensor_metadata(meta_df: pd.DataFrame, station_id: int, date_int: int, logger) -> List[Dict]:
    """
    Parse sensor metadata for a specific station and date.
    
    Args:
        meta_df: DataFrame with sensor metadata (with normalized column names)
        station_id: Station ID to filter for
        date_int: Date as integer (YYYYMMDD format)
        logger: Logger instance
        
    Returns:
        List of sensor dictionaries with bilingual descriptions
    """
    sensors = []

    logger.info(f"🔍 Parsing sensors for station {station_id} on date {date_int}")
    logger.info(f"   📊 Available metadata entries: {len(meta_df)}")

    if meta_df.empty:
        logger.warning("   ❌ No sensor metadata available")
        return sensors

    # Filter for the specific station
    station_df = meta_df[meta_df['station_id'].astype(str).str.strip() == str(station_id)]
    
    if station_df.empty:
        logger.warning(f"   ❌ No sensor metadata found for station {station_id}")
        available_stations = meta_df['station_id'].unique()
        logger.info(f"   📋 Available station IDs: {list(available_stations)}")
        return sensors

    logger.info(f"   ✅ Found {len(station_df)} sensor entries for station {station_id}")

    # Process each sensor entry
    for _, row in station_df.iterrows():
        try:
            # Parse date range
            von_str = str(row['from_date']).strip()
            bis_str = str(row['to_date']).strip()
            
            if not von_str or not bis_str or von_str == 'nan' or bis_str == 'nan':
                logger.debug(f"   ⚠️  Invalid date range: {von_str} - {bis_str}")
                continue
            
            von = int(von_str)
            bis = int(bis_str)
            
            # Check if date falls within range
            if von <= date_int <= bis:
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
                logger.debug(f"   ✅ Added sensor: {param_raw} ({sensor_type_de})")

        except (ValueError, TypeError) as e:
            logger.debug(f"   ❌ Failed to process sensor row: {e}")
            continue
        except Exception as e:
            logger.warning(f"   💥 Unexpected error processing sensor row: {e}")
            continue

    logger.info(f"   📊 Final result: {len(sensors)} sensors for station {station_id} on date {date_int}")
    return sensors
