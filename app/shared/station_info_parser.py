"""
Station Information Parser for German Weather Station Description Files

SCRIPT IDENTIFICATION: DWD10TAH3I
- DWD: Deutscher Wetterdienst data source
- 10T: 10-minute air temperature dataset
- AH: Air temperature Historical data
- 3: Station processing component
- I: Station Info parsing pipeline component

PURPOSE:
Parses German weather station description files which contain station metadata 
in a space-separated format with fixed column headers. Provides comprehensive 
station information lookup with German-specific validation and error handling.

KEY FUNCTIONALITY:
- Handles space-separated format (not fixed-width) with robust parsing
- Robust German state name detection and coordinate validation
- Proper date parsing (YYYYMMDD format) with comprehensive error handling
- Coordinate validation for German geography (47-55°N, 5-15°E)
- Fills missing values with descriptive placeholders
- German-specific region handling and altitude validation
- Dynamic validation (no hardcoded station data)
- Comprehensive logging and debugging support

EXPECTED INPUT FILES:
- Station description file: zehn_min_tu_Beschreibung_Stationen.txt
  Format: Space-separated values with header and separator lines
  Location: data/dwd/1_raw/historical/
  
  Expected structure:
  Stations_id von_datum bis_datum Stationshoehe geoBreite geoLaenge Stationsname Bundesland Abgabe
  ----------- --------- --------- ------------- --------- --------- ----------------------------------------- ---------- ------
  00003 19930429 20110331    202    50.7827    6.0941    Aachen    Nordrhein-Westfalen    Frei
  00044 20070209 20250710    44    52.9336    8.2370 Großenkneten    Niedersachsen    Frei

EXPECTED OUTPUT:
- pandas DataFrame with standardized columns:
  - station_id: 5-digit station ID (e.g., "00003")
  - station_name: Station name (e.g., "Aachen")
  - latitude: Latitude in decimal degrees (e.g., 50.7827)
  - longitude: Longitude in decimal degrees (e.g., 6.0941)
  - station_height: Altitude in meters (e.g., 202.0)
  - state: German state name (e.g., "Nordrhein-Westfalen")
  - from_date: Start date as datetime
  - to_date: End date as datetime
  - availability: Data availability status (e.g., "Frei")

USAGE:
    from app.parsing.station_info_parser import parse_station_info_file, get_station_info
    from app.utils.logger import setup_logger
    
    logger = setup_logger("DWD10TAH3I", script_name="station_info_parser")
    
    # Parse station description file
    station_df = parse_station_info_file(file_path, logger)
    
    # Look up specific station
    station_info = get_station_info(station_df, station_id=3, logger)

AUTHOR: ClimaStation Backend Pipeline
VERSION: Enhanced with script identification codes and new logging system
LAST UPDATED: 2025-01-17
"""

import pandas as pd
from pandas import Series
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
import re
import logging
from datetime import datetime, date
import traceback

# Import logger utility
try:
    from app.utils.logger import setup_logger
    HAS_LOGGER = True
except ImportError:
    HAS_LOGGER = False


def parse_station_info_file(file_path: Path, logger: logging.Logger, parent_log_file: Optional[Path] = None) -> Optional[pd.DataFrame]:
    """
    Parse the German weather station description file.
    
    This function handles the space-separated format used by German weather service
    station description files. The file contains a header line, separator line,
    and data lines with station metadata.
    
    Args:
        file_path: Path to the station description file (zehn_min_tu_Beschreibung_Stationen.txt)
        logger: Logger instance (with DWD10TAH3I code for traceability)
        parent_log_file: Optional path to parent script's log file for centralized logging
        
    Returns:
        DataFrame with station information or None if parsing fails
        
    File Format Expected:
        Line 1: Header with column names
        Line 2: Separator line with dashes
        Line 3+: Data lines with space-separated values
        
    Example:
        Stations_id von_datum bis_datum Stationshoehe geoBreite geoLaenge Stationsname Bundesland Abgabe
        ----------- --------- --------- ------------- --------- --------- ----------------------------------------- ---------- ------
        00003 19930429 20110331    202    50.7827    6.0941    Aachen    Nordrhein-Westfalen    Frei
    """
    # If we have a parent log file, create a new logger that writes to it
    if logger is None and HAS_LOGGER:
        logger = setup_logger("DWD10TAH3I", script_name="station_info_parser")
    elif logger is None:
        # Fallback logger if utils.logger not available
        logger = logging.getLogger("station_info_parser_fallback")
        logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s — [DWD10TAH3I] — %(levelname)s %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    if not file_path.exists():
        logger.warning(f"❌ Station info file not found: {file_path}")
        return None
    
    logger.info(f"📋 Parsing station info file: {file_path.name}")
    logger.info(f"   📄 File size: {file_path.stat().st_size / 1024:.1f} KB")
    
    try:
        # Read the entire file with German encoding
        with open(file_path, 'r', encoding='latin-1') as f:
            lines = f.readlines()
        
        logger.info(f"   📊 Read {len(lines)} lines from file")
        
        # Identify header, separator, and data lines
        header_line = None
        separator_line = None
        data_start_idx = None
        
        for i, line in enumerate(lines):
            line_stripped = line.strip()
            
            # Look for header line (contains column names)
            if ('Stations_id' in line or 'stations_id' in line) and ('von_datum' in line or 'from_date' in line):
                header_line = line_stripped
                logger.info(f"   📋 Found header at line {i+1}: {header_line[:60]}...")
                
            # Look for separator line (dashes indicating column boundaries)
            elif line_stripped.startswith('---') and len(line_stripped) > 20:
                separator_line = line_stripped
                data_start_idx = i + 1
                logger.info(f"   📏 Found separator at line {i+1}, data starts at line {data_start_idx+1}")
                break
        
        if not header_line:
            logger.error("❌ Could not find header line in station info file")
            logger.debug("   🔍 Looking for lines containing 'Stations_id' and 'von_datum'")
            return None
            
        if data_start_idx is None:
            logger.error("❌ Could not find separator line in station info file")
            logger.debug("   🔍 Looking for lines starting with '---'")
            return None
        
        # Parse data lines using space-separated approach
        logger.info(f"📊 Parsing data lines starting from line {data_start_idx+1}")
        stations = []
        lines_processed = 0
        lines_skipped = 0
        parsing_errors = []
        
        # Process each data line
        for i, line in enumerate(lines[data_start_idx:], data_start_idx + 1):
            line = line.strip()
            
            # Skip empty lines
            if not line:
                lines_skipped += 1
                continue
            
            try:
                # Split line by whitespace
                parts = line.split()
                
                # Need at least 6 parts: station_id, von_datum, bis_datum, hoehe, breite, laenge
                if len(parts) < 6:
                    logger.debug(f"   ⚠️  Skipping line {i} - insufficient parts: {len(parts)}")
                    lines_skipped += 1
                    continue
                
                # Extract basic fields
                station_id = parts[0]
                von_datum = parts[1]
                bis_datum = parts[2]
                hoehe = parts[3]
                breite = parts[4]
                laenge = parts[5]
                
                # Everything after position 5 contains station name, state, and availability
                remaining_parts = parts[6:] if len(parts) > 6 else []
                
                # Parse station name, state, and availability from remaining parts
                availability = "no availability info"  # Default value
                state = "no state info available"      # Default value
                station_name = "no station name available"  # Default value
                
                if remaining_parts:
                    # Check if last part is "Frei" (availability status)
                    if remaining_parts[-1] == "Frei":
                        availability = "Frei"
                        name_state_parts = remaining_parts[:-1]
                    else:
                        name_state_parts = remaining_parts
                    
                    # German states (Bundesländer) - comprehensive list
                    german_states = [
                        'Nordrhein-Westfalen', 'Baden-Württemberg', 'Bayern', 'Niedersachsen',
                        'Hessen', 'Rheinland-Pfalz', 'Sachsen', 'Thüringen', 'Brandenburg',
                        'Sachsen-Anhalt', 'Schleswig-Holstein', 'Mecklenburg-Vorpommern',
                        'Saarland', 'Berlin', 'Bremen', 'Hamburg'
                    ]
                    
                    # Try to identify the German state in the remaining text
                    remaining_text = ' '.join(name_state_parts)
                    
                    state_found = False
                    for german_state in german_states:
                        if german_state in remaining_text:
                            state = german_state
                            # Remove state from remaining text to get station name
                            station_name = remaining_text.replace(german_state, '').strip()
                            if not station_name:  # If station name becomes empty after removing state
                                station_name = "no station name available"
                            state_found = True
                            break
                    
                    # Fallback: if no known state found, assume last word is state
                    if not state_found and len(name_state_parts) > 1:
                        state = name_state_parts[-1]
                        station_name = ' '.join(name_state_parts[:-1])
                        if not station_name:
                            station_name = "no station name available"
                    elif not state_found and len(name_state_parts) == 1:
                        station_name = name_state_parts[0]
                        state = "no state info available"
                
                # Validate essential fields
                if station_id and len(station_id) >= 3:
                    # Clean and standardize station_id (ensure 5 digits with leading zeros)
                    clean_station_id = station_id.zfill(5)
                    
                    # Validate station_id is numeric
                    try:
                        int(clean_station_id)
                    except ValueError:
                        logger.debug(f"   ⚠️  Line {i} has non-numeric station_id: '{station_id}'")
                        lines_skipped += 1
                        continue
                    
                    # Create station data record with default values for missing data
                    station_data = {
                        'station_id': clean_station_id,
                        'from_date': von_datum if von_datum else "no start date available",
                        'to_date': bis_datum if bis_datum else "no end date available",
                        'station_height': hoehe if hoehe else "no altitude available",
                        'latitude': breite if breite else "no latitude available",
                        'longitude': laenge if laenge else "no longitude available",
                        'station_name': station_name,
                        'state': state,
                        'availability': availability
                    }
                    
                    stations.append(station_data)
                    lines_processed += 1
                    
                    # Log first few stations for debugging
                    if lines_processed <= 3:
                        logger.debug(f"   📊 Station {lines_processed}: {clean_station_id} - {station_name} ({state})")
                else:
                    error_msg = f"Line {i} missing essential fields: station_id='{station_id}'"
                    logger.debug(f"   ⚠️  {error_msg}")
                    parsing_errors.append(error_msg)
                    lines_skipped += 1
                    
            except Exception as e:
                error_msg = f"Failed to parse line {i}: {e}"
                logger.debug(f"   ❌ {error_msg}")
                parsing_errors.append(error_msg)
                lines_skipped += 1
                continue
        
        logger.info(f"   ✅ Processed {lines_processed} stations, skipped {lines_skipped} lines")
        
        # Log parsing errors if any (but limit output)
        if parsing_errors and len(parsing_errors) <= 10:
            logger.debug("   📋 Parsing errors encountered:")
            for error in parsing_errors[:10]:
                logger.debug(f"      - {error}")
        elif len(parsing_errors) > 10:
            logger.debug(f"   📋 {len(parsing_errors)} parsing errors encountered (showing first 10)")
            for error in parsing_errors[:10]:
                logger.debug(f"      - {error}")
        
        if not stations:
            logger.error("❌ No valid station data found")
            return None
        
        # Create DataFrame and perform data cleaning
        logger.info("🔄 Creating DataFrame and cleaning data...")
        df = pd.DataFrame(stations)
        
        # Clean station IDs (ensure consistent 5-digit format)
        logger.info("   🧹 Cleaning station IDs...")
        df['station_id'] = df['station_id'].astype(str).str.strip().str.zfill(5)
        
        # Convert numeric fields with proper error handling
        logger.info("   🔢 Converting numeric fields...")
        numeric_fields = ['station_height', 'latitude', 'longitude']
        
        for field in numeric_fields:
            if field in df.columns:
                # Create a mask for rows that don't have placeholder text
                valid_mask = ~df[field].astype(str).str.contains('no .* available', regex=True)
                
                # Only convert numeric values, leave placeholder text as is
                if valid_mask.any():
                    # Handle German decimal format (comma as decimal separator)
                    df.loc[valid_mask, field] = df.loc[valid_mask, field].astype(str).str.replace(',', '.').str.strip()
                    
                    # Remove any non-numeric characters except decimal point and minus sign
                    df.loc[valid_mask, field] = df.loc[valid_mask, field].str.replace(r'[^\d.-]', '', regex=True)
                    
                    # Convert to numeric with error handling
                    original_count = valid_mask.sum()
                    df.loc[valid_mask, field] = pd.to_numeric(df.loc[valid_mask, field], errors='coerce')
                    
                    # Replace NaN values with placeholder text
                    nan_mask = df[field].isna() & valid_mask
                    df.loc[nan_mask, field] = f"no {field} value available"
                    
                    # Log conversion results
                    valid_count = df.loc[valid_mask, field].notna().sum()
                    invalid_count = original_count - valid_count
                    logger.debug(f"      {field}: {valid_count}/{original_count} valid values")
                    if invalid_count > 0:
                        logger.debug(f"         {invalid_count} invalid values replaced with placeholder")
        
        # Convert date fields with proper format handling
        logger.info("   📅 Converting date fields...")
        date_fields = ['from_date', 'to_date']
        
        for field in date_fields:
            if field in df.columns:
                # Create a mask for rows that don't have placeholder text
                valid_mask = ~df[field].astype(str).str.contains('no .* available', regex=True)
                
                if valid_mask.any():
                    # Clean date strings
                    df.loc[valid_mask, field] = df.loc[valid_mask, field].astype(str).str.strip()
                    
                    # Convert YYYYMMDD format to datetime
                    original_count = valid_mask.sum()
                    df.loc[valid_mask, field] = pd.to_datetime(df.loc[valid_mask, field], format='%Y%m%d', errors='coerce')
                    
                    # Replace NaT values with placeholder text
                    nat_mask = df[field].isna() & valid_mask
                    df.loc[nat_mask, field] = f"no {field} value available"
                    
                    # Log conversion results
                    valid_count = df.loc[valid_mask, field].notna().sum()
                    invalid_count = original_count - valid_count
                    logger.debug(f"      {field}: {valid_count}/{original_count} valid dates")
                    if invalid_count > 0:
                        logger.debug(f"         {invalid_count} invalid dates replaced with placeholder")
        
        # Clean text fields
        logger.info("   📝 Cleaning text fields...")
        text_fields = ['station_name', 'state', 'availability']
        
        for field in text_fields:
            if field in df.columns:
                # Remove extra whitespace and handle null values
                df[field] = df[field].astype(str).str.strip()
                
                # Replace empty or null values with appropriate placeholder
                empty_mask = df[field].isin(['nan', 'None', 'NULL', '', 'null'])
                df.loc[empty_mask, field] = f"no {field} available"
                
                # Clean up multiple spaces
                df[field] = df[field].str.replace(r'\s+', ' ', regex=True)
        
        # Validation and quality checks
        logger.info("📊 Final dataset summary:")
        logger.info(f"   📈 Total stations: {len(df)}")
        
        if 'station_id' in df.columns:
            unique_stations = df['station_id'].nunique()
            id_range = f"{df['station_id'].min()} to {df['station_id'].max()}"
            logger.info(f"   🆔 Unique stations: {unique_stations}")
            logger.info(f"   🆔 Station ID range: {id_range}")
        
        # Validate coordinates (should be reasonable for Germany)
        if 'latitude' in df.columns and 'longitude' in df.columns:
            # Count only numeric coordinates (not placeholder text)
            numeric_lat_mask = pd.to_numeric(df['latitude'], errors='coerce').notna()
            numeric_lon_mask = pd.to_numeric(df['longitude'], errors='coerce').notna()
            valid_coords = (numeric_lat_mask & numeric_lon_mask).sum()
            
            if valid_coords > 0:
                numeric_lats = pd.to_numeric(df['latitude'], errors='coerce').dropna()
                numeric_lons = pd.to_numeric(df['longitude'], errors='coerce').dropna()
                
                lat_min, lat_max = numeric_lats.min(), numeric_lats.max()
                lon_min, lon_max = numeric_lons.min(), numeric_lons.max()
                
                # Check if coordinates are reasonable for Germany (47-55°N, 5-15°E)
                reasonable_lat = (47 <= lat_min <= 55) and (47 <= lat_max <= 55)
                reasonable_lon = (5 <= lon_min <= 15) and (5 <= lon_max <= 15)
                
                logger.info(f"   📍 Valid coordinates: {valid_coords}/{len(df)}")
                logger.info(f"   📍 Coordinate ranges: {lat_min:.4f}°N to {lat_max:.4f}°N, {lon_min:.4f}°E to {lon_max:.4f}°E")
                
                if not (reasonable_lat and reasonable_lon):
                    logger.warning("   ⚠️  Coordinate ranges seem unreasonable for Germany")
                    logger.warning("   ⚠️  Expected: Lat 47-55°N, Lon 5-15°E")
            else:
                logger.warning("   ⚠️  No valid coordinates found")
        
        # Validate altitudes (should be reasonable for Germany: -10m to 3000m)
        if 'station_height' in df.columns:
            numeric_alt_mask = pd.to_numeric(df['station_height'], errors='coerce').notna()
            valid_heights = numeric_alt_mask.sum()
            
            if valid_heights > 0:
                numeric_alts = pd.to_numeric(df['station_height'], errors='coerce').dropna()
                alt_min, alt_max = numeric_alts.min(), numeric_alts.max()
                reasonable_alt = (-10 <= alt_min <= 3000) and (-10 <= alt_max <= 3000)
                
                logger.info(f"   🏔️  Valid altitudes: {valid_heights}/{len(df)}")
                logger.info(f"   🏔️  Altitude range: {alt_min:.0f}m to {alt_max:.0f}m")
                
                if not reasonable_alt:
                    logger.warning("   ⚠️  Altitude ranges seem unreasonable for Germany")
                    logger.warning("   ⚠️  Expected: -10m to 3000m")
            else:
                logger.warning("   ⚠️  No valid altitudes found")
        
        # Validate dates
        if 'from_date' in df.columns and 'to_date' in df.columns:
            from_date_mask = pd.to_datetime(df['from_date'], errors='coerce').notna()
            to_date_mask = pd.to_datetime(df['to_date'], errors='coerce').notna()
            valid_dates = (from_date_mask & to_date_mask).sum()
            
            logger.info(f"   📅 Valid date ranges: {valid_dates}/{len(df)}")
            
            if valid_dates > 0:
                valid_from_dates = pd.to_datetime(df['from_date'], errors='coerce').dropna()
                valid_to_dates = pd.to_datetime(df['to_date'], errors='coerce').dropna()
                
                earliest_date = valid_from_dates.min()
                latest_date = valid_to_dates.max()
                logger.info(f"   📅 Overall date range: {earliest_date.date()} to {latest_date.date()}")
        
        # Log sample stations for verification
        logger.debug("   📋 Sample stations:")
        for i, (_, row) in enumerate(df.head(3).iterrows()):
            state_info = row.get('state', 'no state info available')
            logger.debug(f"      {i+1}. {row['station_id']}: {row['station_name']} ({state_info})")
        
        logger.info(f"✅ Successfully parsed station info file: {len(df)} stations")
        return df
        
    except Exception as e:
        logger.error(f"💥 Failed to parse station info file: {e}")
        logger.error(f"🔍 Traceback: {traceback.format_exc()}")
        return None


def get_station_info(station_df: pd.DataFrame, station_id: int, logger: logging.Logger) -> Optional[Dict[str, Any]]:
    """
    Get station information for a specific station ID.
    
    This function looks up a specific station in the parsed station DataFrame
    and returns a standardized dictionary with station metadata. For German stations,
    the region field is set to indicate that regions are not applicable in Germany.
    
    Args:
        station_df: DataFrame with station information from parse_station_info_file
        station_id: Station ID to look up (e.g., 3 for station 00003)
        logger: Logger instance (with DWD10TAH3I code for traceability)
        
    Returns:
        Dictionary with station information or None if not found:
        {
            'station_id': '00003',
            'station_name': 'Aachen',
            'latitude': 50.7827,
            'longitude': 6.0941,
            'station_altitude_m': 202.0,
            'state': 'Nordrhein-Westfalen',
            'region': 'regions not applicable for Germany'
        }
        
    Example:
        station_info = get_station_info(station_df, station_id=3, logger)
        if station_info:
            print(f"Station: {station_info['station_name']}")
            print(f"Location: {station_info['latitude']}, {station_info['longitude']}")
    """
    if station_df is None or station_df.empty:
        logger.warning("❌ No station data available for lookup")
        return None
    
    # Try multiple station ID formats for robust lookup
    station_id_formats = [
        str(station_id).zfill(5),  # 5-digit with leading zeros (preferred): "00003"
        str(station_id),           # As-is: "3"
        f"{station_id:05d}",       # Alternative 5-digit formatting: "00003"
    ]
    
    logger.debug(f"🔍 Looking up station {station_id}")
    logger.debug(f"   📋 Trying formats: {station_id_formats}")
    
    # Try each format until we find a match
    station_rows = pd.DataFrame()
    matched_format = None
    
    for format_attempt in station_id_formats:
        station_rows = station_df[station_df['station_id'] == format_attempt]
        if not station_rows.empty:
            matched_format = format_attempt
            logger.debug(f"   ✅ Found match with format: {format_attempt}")
            break
    
    if station_rows.empty:
        logger.warning(f"❌ Station {station_id} not found in station info")
        
        # Provide debugging information
        available_stations = station_df['station_id'].unique()
        logger.debug(f"   📋 Available station IDs (first 10): {list(available_stations[:10])}")
        
        # Check for similar station IDs
        similar_stations = [sid for sid in available_stations if str(station_id) in str(sid)]
        if similar_stations:
            logger.debug(f"   🔍 Similar station IDs found: {similar_stations}")
        
        # Check for partial matches (last 3 digits)
        partial_matches = [sid for sid in available_stations if sid.endswith(str(station_id).zfill(3))]
        if partial_matches:
            logger.debug(f"   🔍 Partial matches (last 3 digits): {partial_matches}")
        
        return None
    
    # Take the first matching row (there should typically only be one)
    station_row = station_rows.iloc[0]
    
    logger.debug(f"   ✅ Found station: {station_row['station_name']}")
    
    # Create standardized station info dictionary with comprehensive error handling
    try:
        # Helper function to safely extract scalar values from pandas objects
        def to_scalar(value):
            """Convert pandas Series/objects to scalar values safely."""
            # Handle pandas Series
            if hasattr(value, 'iloc'):
                try:
                    if len(value) > 0:
                        return value.iloc[0]
                    else:
                        return None
                except (TypeError, AttributeError):
                    # If len() fails, try to get the value directly
                    try:
                        return value.iloc[0]
                    except (IndexError, AttributeError):
                        return None
            
            # Handle pandas scalar values that might still have pandas types
            if hasattr(value, 'item'):
                try:
                    return value.item()
                except (ValueError, AttributeError):
                    pass
            
            # Return as-is if it's already a scalar
            return value
        
        # Helper function to safely check if a scalar value is null/empty
        def is_null_or_empty(scalar_value):
            """Check if a scalar value is null or empty."""
            if scalar_value is None:
                return True
            try:
                if pd.isna(scalar_value):
                    return True
            except (TypeError, ValueError):
                # pd.isna might fail on some types, that's okay
                pass
            return str(scalar_value).strip() == ''
        
        # Helper function to safely extract values with fallbacks
        def safe_extract(field_name, default_value):
            value = station_row[field_name]
            scalar_value = to_scalar(value)
            
            if is_null_or_empty(scalar_value):
                return default_value
            return scalar_value
        
        # Helper function to safely convert to float
        def safe_float(field_name, default_value):
            value = station_row[field_name]
            scalar_value = to_scalar(value)

            # If it's a Series, extract its first element (or bail out)
            if isinstance(scalar_value, Series):
                if scalar_value.empty:
                    return default_value
                try:
                    scalar_value = scalar_value.iloc[0]
                except (IndexError, AttributeError):
                    return default_value  # couldn't pull a valid element

            # Now scalar_value is either None, a primitive, or NaN
            if scalar_value is None:
                return default_value

            # placeholder string?
            if isinstance(scalar_value, str) and 'no ' in scalar_value and 'available' in scalar_value:
                return default_value

            # pandas NA?
            try:
                if pd.isna(scalar_value):
                    return default_value
            except (TypeError, ValueError):
                pass

            # finally, try float conversion
            if not isinstance(scalar_value, (int, float, str)):
                return default_value

            try:
                return float(scalar_value)
            except (ValueError, TypeError):
                return default_value
        
        station_info = {
            'station_id': str(safe_extract('station_id', 'no station ID available')),
            'station_name': str(safe_extract('station_name', 'no station name available')),
            'latitude': safe_float('latitude', 'no latitude value available'),
            'longitude': safe_float('longitude', 'no longitude value available'),
            'station_altitude_m': safe_float('station_height', 'no altitude value available'),
            'state': str(safe_extract('state', 'no state info available')),
            'region': 'regions not applicable for Germany'  # German-specific region handling
        }
        
        # Data quality assessment
        data_quality_issues = []
        
        # Check for missing or invalid coordinates
        if isinstance(station_info['latitude'], str) or isinstance(station_info['longitude'], str):
            data_quality_issues.append("Missing coordinates")
        elif not (47 <= station_info['latitude'] <= 55) or not (5 <= station_info['longitude'] <= 15):
            data_quality_issues.append("Coordinates outside expected German range")
        
        # Check for missing station name
        if 'no station name' in str(station_info['station_name']):
            data_quality_issues.append("Missing station name")
        
        # Check for unreasonable altitude
        if isinstance(station_info['station_altitude_m'], str):
            data_quality_issues.append("Missing altitude")
        elif not (-10 <= station_info['station_altitude_m'] <= 3000):
            data_quality_issues.append("Altitude outside expected German range")
        
        # Check for missing state information
        if 'no state info' in str(station_info['state']):
            data_quality_issues.append("Missing state information")
        
        # Log the retrieved information with quality assessment
        logger.debug(f"   📊 Station info retrieved:")
        logger.debug(f"      🏢 Name: {station_info['station_name']}")
        
        if isinstance(station_info['latitude'], (int, float)) and isinstance(station_info['longitude'], (int, float)):
            logger.debug(f"      📍 Coordinates: {station_info['latitude']:.4f}°N, {station_info['longitude']:.4f}°E")
        else:
            logger.debug(f"      📍 Coordinates: {station_info['latitude']}, {station_info['longitude']}")
        
        if isinstance(station_info['station_altitude_m'], (int, float)):
            logger.debug(f"      🏔️  Altitude: {station_info['station_altitude_m']:.0f}m")
        else:
            logger.debug(f"      🏔️  Altitude: {station_info['station_altitude_m']}")
        
        logger.debug(f"      🏛️  State: {station_info['state']}")
        logger.debug(f"      🌍 Region: {station_info['region']}")
        
        if data_quality_issues:
            logger.debug(f"      ⚠️  Data quality issues: {', '.join(data_quality_issues)}")
        else:
            logger.debug(f"      ✅ Data quality: Complete")
        
        return station_info
        
    except Exception as e:
        logger.error(f"   ❌ Error creating station info dictionary: {e}")
        return None


if __name__ == "__main__":
    """
    Test the station info parser functionality when run directly.
    
    This provides comprehensive testing to verify that the parser is working correctly
    with real German weather station description files.
    
    Run this file directly to test: python -m app.parsing.station_info_parser
    """
    import sys
    
    print("🧪 Testing ClimaStation Station Info Parser [DWD10TAH3I]...")
    print("=" * 60)
    
    # Set up test logger with script identification
    if HAS_LOGGER:
        test_logger = setup_logger("DWD10TAH3I", script_name="station_info_parser_test")
    else:
        # Fallback logger for testing
        test_logger = logging.getLogger("test_station_parser")
        test_logger.setLevel(logging.DEBUG)
        
        # Add console handler for test output
        console_handler = logging.StreamHandler()
        formatter = logging.Formatter('%(levelname)s — [DWD10TAH3I] %(message)s')
        console_handler.setFormatter(formatter)
        test_logger.addHandler(console_handler)
    
    # Test file path (updated for new folder structure)
    test_file_path = Path("data/dwd/1_raw/historical/zehn_min_tu_Beschreibung_Stationen.txt")
    
    # Alternative paths to try
    alternative_paths = [
        Path("data/germany/2_downloaded_files/10_minutes/air_temperature/historical/zehn_min_tu_Beschreibung_Stationen.txt"),
        Path("zehn_min_tu_Beschreibung_Stationen.txt"),
        Path("data/station_info/zehn_min_tu_Beschreibung_Stationen.txt")
    ]
    
    # Find the test file
    actual_test_file = None
    for path in [test_file_path] + alternative_paths:
        if path.exists():
            actual_test_file = path
            break
    
    if not actual_test_file:
        test_logger.error("❌ Test file not found. Tried these paths:")
        for path in [test_file_path] + alternative_paths:
            test_logger.error(f"   - {path}")
        test_logger.error("Please ensure the station description file is available.")
        sys.exit(1)
    
    try:
        test_logger.info(f"🧪 Testing station info parsing with file: {actual_test_file}")
        
        # Test parsing
        station_df = parse_station_info_file(actual_test_file, test_logger)
        
        if station_df is not None and not station_df.empty:
            test_logger.info(f"✅ Successfully parsed {len(station_df)} stations")
            
            # Show sample of parsed data
            test_logger.info("📊 Sample of parsed stations:")
            sample_df = station_df.head(5)[['station_id', 'station_name', 'latitude', 'longitude', 'state']]
            print(sample_df.to_string(index=False))
            
            # Test station lookup - dynamically test the first available station
            available_stations = station_df['station_id'].unique()
            if len(available_stations) > 0:
                # Get the first station ID and convert to integer for testing
                first_station_id_str = available_stations[0]
                try:
                    first_station_id = int(first_station_id_str)
                except ValueError:
                    first_station_id = int(first_station_id_str.lstrip('0')) if first_station_id_str.lstrip('0') else 0
                
                test_logger.info(f"🔍 Testing station lookup for station {first_station_id} (ID: {first_station_id_str})...")
                station_info = get_station_info(station_df, first_station_id, test_logger)
                
                if station_info:
                    test_logger.info("✅ Station lookup test successful:")
                    test_logger.info(f"   🏢 Station: {station_info['station_name']}")
                    
                    if isinstance(station_info['latitude'], (int, float)) and isinstance(station_info['longitude'], (int, float)):
                        test_logger.info(f"   📍 Location: {station_info['latitude']:.4f}°N, {station_info['longitude']:.4f}°E")
                        
                        # Dynamic coordinate validation - check if within German bounds
                        if 47 <= station_info['latitude'] <= 55 and 5 <= station_info['longitude'] <= 15:
                            test_logger.info("   ✅ Coordinates are within expected German bounds")
                        else:
                            test_logger.warning("   ⚠️  Coordinates are outside expected German bounds")
                            test_logger.warning("      Expected: Lat 47-55°N, Lon 5-15°E")
                    else:
                        test_logger.info(f"   📍 Location: {station_info['latitude']}, {station_info['longitude']}")
                    
                    if isinstance(station_info['station_altitude_m'], (int, float)):
                        test_logger.info(f"   🏔️  Altitude: {station_info['station_altitude_m']:.0f}m")
                        
                        # Dynamic altitude validation
                        if -10 <= station_info['station_altitude_m'] <= 3000:
                            test_logger.info("   ✅ Altitude is within expected German range")
                        else:
                            test_logger.warning("   ⚠️  Altitude is outside expected German range (-10m to 3000m)")
                    else:
                        test_logger.info(f"   🏔️  Altitude: {station_info['station_altitude_m']}")
                    
                    test_logger.info(f"   🏛️  State: {station_info['state']}")
                    test_logger.info(f"   🌍 Region: {station_info['region']}")
                    
                else:
                    test_logger.error("❌ Station lookup test failed")
            
            # Test a few more stations dynamically
            test_logger.info("🔍 Testing additional station lookups...")
            test_station_ids = available_stations[:3]  # Test first 3 available stations
            
            for station_id_str in test_station_ids:
                try:
                    test_id = int(station_id_str.lstrip('0')) if station_id_str.lstrip('0') else 0
                    station_info = get_station_info(station_df, test_id, test_logger)
                    if station_info:
                        test_logger.info(f"   ✅ Station {station_id_str}: {station_info['station_name']} ({station_info['state']})")
                    else:
                        test_logger.warning(f"   ❌ Station {station_id_str}: Not found")
                except ValueError:
                    test_logger.warning(f"   ⚠️  Station {station_id_str}: Invalid ID format")
            
            test_logger.info("✅ All tests completed successfully!")
            
        else:
            test_logger.error("❌ Failed to parse station info file")
            sys.exit(1)
            
    except Exception as e:
        test_logger.error(f"💥 Test failed with error: {e}")
        test_logger.error(f"🔍 Traceback: {traceback.format_exc()}")
        sys.exit(1)
