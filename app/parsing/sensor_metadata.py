"""
Sensor Metadata Parser

This script processes sensor metadata from German weather station ZIP files.
It extracts and parses sensor specifications, measurement methods, and equipment details.

Expected Input:
- Metadata files from ZIP archives containing "Metadaten_Geraete" in filename
- Files are semicolon-separated CSV with latin-1 encoding
- Contains sensor specifications for different time periods

Expected Output:
- Pandas DataFrame with normalized sensor metadata
- List of sensor dictionaries with bilingual descriptions (German/English)

Key Functions:
- load_sensor_metadata(): Loads and combines metadata files into DataFrame
- parse_sensor_metadata(): Extracts sensors for specific station/date combinations
"""

import io
from pathlib import Path
from typing import List, Dict
import pandas as pd

from config.ten_minutes_air_temperature_config import (
    PARAM_NAME_MAP, COLUMN_NAME_MAP,
    SENSOR_TYPE_TRANSLATIONS,
    MEASUREMENT_METHOD_TRANSLATIONS
)


def load_sensor_metadata(meta_files: List[Path], logger) -> pd.DataFrame:
    """
    Load and combine sensor metadata from multiple files.
    
    Args:
        meta_files: List of metadata file paths
        logger: Logger instance for debugging
        
    Returns:
        Combined DataFrame with normalized column names
    """
    relevant_files = [
        f for f in meta_files
        if "Metadaten_Geraete" in f.name
    ]

    all_frames = []

    for path in relevant_files:
        try:
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

            # Normalize column names using the mapping
            df.columns = [col.strip().lower() for col in df.columns]
            df.rename(columns=COLUMN_NAME_MAP, inplace=True)

            # Clean string columns
            df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
            all_frames.append(df)
            logger.debug(f"Loaded metadata from {path.name} with {len(df)} rows")

        except Exception as e:
            logger.error(f"❌ Failed to parse sensor metadata file {path.name}: {e}")

    if all_frames:
        combined = pd.concat(all_frames, ignore_index=True)
        logger.debug(f"Combined sensor metadata: {len(combined)} total rows")
        return combined
    else:
        # Return empty DataFrame with correct normalized column names
        logger.warning("No sensor metadata files found, returning empty DataFrame")
        return pd.DataFrame(columns=[
            'parameter', 'station_id', 'from_date', 'to_date',
            'sensor_type', 'measurement_method', 'sensor_height_m'
        ])


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

    # Validate required columns
    required_columns = ['station_id', 'parameter', 'from_date', 'to_date']
    missing_columns = [col for col in required_columns if col not in meta_df.columns]
    if missing_columns:
        logger.warning(f"Missing required columns in sensor metadata: {missing_columns}")
        return sensors

    # Filter for the specific station
    station_df = meta_df[meta_df['station_id'].astype(str).str.strip() == str(station_id)]
    
    if station_df.empty:
        logger.debug(f"No sensor metadata found for station {station_id}")
        return sensors

    # Process each unique parameter
    for param_raw in station_df['parameter'].unique():
        if pd.isna(param_raw):
            continue
            
        param_entry = PARAM_NAME_MAP.get(param_raw, {"en": param_raw, "de": param_raw})
        param_df = station_df[station_df['parameter'] == param_raw]

        for _, row in param_df.iterrows():
            try:
                # Parse date range
                von_str = str(row['from_date']).strip()
                bis_str = str(row['to_date']).strip()
                
                if not von_str or not bis_str or von_str == 'nan' or bis_str == 'nan':
                    logger.debug(f"Invalid date range for parameter {param_raw}: {von_str} - {bis_str}")
                    continue
                
                von = int(von_str)
                bis = int(bis_str)
                
                # Check if date falls within range
                if von <= date_int <= bis:
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
                    logger.debug(f"Added sensor for parameter {param_raw} at station {station_id}")

            except (ValueError, TypeError) as e:
                logger.debug(f"Failed to process sensor metadata row for parameter {param_raw}: {e}")
                continue
            except Exception as e:
                logger.warning(f"Unexpected error processing sensor metadata for parameter {param_raw}: {e}")
                continue

    logger.debug(f"Found {len(sensors)} sensors for station {station_id} on date {date_int}")
    return sensors
