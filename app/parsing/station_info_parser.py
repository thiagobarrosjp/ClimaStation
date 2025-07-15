"""
Station Information Parser for Fixed-Width Format Files

This module parses the German weather station description files which are in
fixed-width format rather than CSV format.
"""

import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
import re
import logging
from datetime import datetime, date
import traceback


def detect_column_positions_enhanced(header_line: str, separator_line: str, logger: logging.Logger) -> List[Tuple[str, int, int]]:
    """
    ENHANCED: Detect column positions from header and separator lines with fixed positions.
    
    The German station files have a specific fixed-width format. Instead of trying to parse
    the separator line dynamically, we use the known column positions for reliability.
    
    Args:
        header_line: Header line with column names
        separator_line: Separator line with dashes indicating column boundaries
        logger: Logger instance
        
    Returns:
        List of tuples (column_name, start_pos, end_pos)
        
    Example:
        positions = detect_column_positions_enhanced(header_line, separator_line, logger)
        # Returns: [("station_id", 0, 11), ("from_date", 12, 21), ...]
    """
    logger.debug("🔍 Detecting column positions from header and separator")
    logger.debug(f"   📋 Header: {header_line}")
    logger.debug(f"   📏 Separator: {separator_line}")
    
    # ENHANCED: Use fixed column positions based on the known German station file format
    # This is more reliable than trying to parse the separator line dynamically
    column_specs = [
        ('station_id', 0, 11),          # Stations_id: positions 0-10
        ('from_date', 12, 21),          # von_datum: positions 12-20  
        ('to_date', 22, 31),            # bis_datum: positions 22-30
        ('station_height', 32, 45),     # Stationshoehe: positions 32-44
        ('latitude', 46, 57),           # geoBreite: positions 46-56
        ('longitude', 58, 69),          # geoLaenge: positions 58-68
        ('station_name', 70, 110),      # Stationsname: positions 70-109
        ('state', 111, 150),            # Bundesland: positions 111-149
        ('availability', 151, 160)      # Abgabe: positions 151-159 (optional)
    ]
    
    logger.info(f"   ✅ Using enhanced fixed column positions for German station file format")
    for i, (name, start, end) in enumerate(column_specs, 1):
        logger.debug(f"   📊 Column {i}: '{name}' at positions {start}-{end}")
    
    return column_specs


def clean_station_data_enhanced(raw_data: str, logger: logging.Logger) -> str:
    """
    ENHANCED: Clean raw station data string for better parsing.
    
    Args:
        raw_data: Raw data string from station file
        logger: Logger instance
        
    Returns:
        Cleaned data string
    """
    # Remove extra whitespace and normalize line endings
    cleaned = raw_data.strip()
    
    # Handle encoding issues
    try:
        # Try to decode and re-encode to handle any encoding issues
        if isinstance(cleaned, bytes):
            cleaned = cleaned.decode('latin-1', errors='replace')
        
        # Normalize whitespace but preserve structure
        lines = cleaned.split('\n')
        cleaned_lines = []
        
        for line in lines:
            # Remove trailing whitespace but preserve leading structure
            cleaned_line = line.rstrip()
            if cleaned_line:  # Skip empty lines
                cleaned_lines.append(cleaned_line)
        
        cleaned = '\n'.join(cleaned_lines)
        
    except Exception as e:
        logger.debug(f"   ⚠️  Data cleaning warning: {e}")
    
    return cleaned


def parse_station_info_file_enhanced(file_path: Path, logger: logging.Logger) -> Optional[pd.DataFrame]:
    """
    ENHANCED: Parse the fixed-width station description file with improved error handling.
    
    This is the main parsing function that handles the complex fixed-width format
    used by German weather service station description files.
    
    Args:
        file_path: Path to the station description file
        logger: Logger instance for detailed logging
        
    Returns:
        DataFrame with station information or None if parsing fails
        
    The function handles:
    - Fixed-width format parsing with enhanced column detection
    - German text encoding (latin-1) with error recovery
    - Header detection and column position calculation
    - Data validation and cleaning with quality checks
    - Date parsing and conversion with error handling
    - Coordinate and altitude parsing with validation
    - Comprehensive error reporting and debugging
    
    Example:
        station_df = parse_station_info_file_enhanced(
            Path("zehn_min_tu_Beschreibung_Stationen.txt"), 
            logger
        )
    """
    if not file_path.exists():
        logger.warning(f"❌ Station info file not found: {file_path}")
        return None
    
    logger.info(f"📋 Parsing station info file with enhanced features: {file_path.name}")
    logger.info(f"   📄 File size: {file_path.stat().st_size / 1024:.1f} KB")
    
    try:
        # Read the entire file with enhanced encoding handling
        try:
            with open(file_path, 'r', encoding='latin-1') as f:
                content = f.read()
        except UnicodeDecodeError as e:
            logger.warning(f"   ⚠️  Encoding issue, trying with error handling: {e}")
            with open(file_path, 'r', encoding='latin-1', errors='replace') as f:
                content = f.read()
        
        # Clean the content
        content = clean_station_data_enhanced(content, logger)
        lines = content.split('\n')
        
        logger.info(f"   📊 Read {len(lines)} lines from file")
        
        # Find the header line and separator line with enhanced detection
        header_line = None
        separator_line = None
        data_start_idx = None
        
        for i, line in enumerate(lines):
            line_stripped = line.strip()
            
            # Look for header line (contains column names) - enhanced detection
            if ('Stations_id' in line or 'stations_id' in line) and ('von_datum' in line or 'from_date' in line):
                header_line = line_stripped
                logger.info(f"   📋 Found header at line {i+1}: {header_line[:60]}...")
                
            # Look for separator line (dashes indicating column boundaries) - enhanced detection
            elif (line_stripped.startswith('---') or line_stripped.startswith('===')) and len(line_stripped) > 20:
                separator_line = line_stripped
                data_start_idx = i + 1
                logger.info(f"   📏 Found separator at line {i+1}, data starts at line {data_start_idx+1}")
                break
        
        if not header_line:
            logger.error("❌ Could not find header line in station info file")
            logger.debug("   🔍 Looking for lines containing 'Stations_id' and 'von_datum'")
            # Show first few lines for debugging
            for i, line in enumerate(lines[:10]):
                logger.debug(f"   Line {i+1}: {line[:80]}...")
            return None
            
        if data_start_idx is None:
            logger.error("❌ Could not find separator line in station info file")
            logger.debug("   🔍 Looking for lines starting with '---' or '==='")
            # Show lines around header for debugging
            header_idx = next((i for i, line in enumerate(lines) if 'Stations_id' in line), -1)
            if header_idx >= 0:
                for i in range(max(0, header_idx-2), min(len(lines), header_idx+5)):
                    logger.debug(f"   Line {i+1}: {lines[i][:80]}...")
            return None
        
        # Detect column positions with enhanced method
        column_specs = detect_column_positions_enhanced(header_line, separator_line or "", logger)
        
        if not column_specs:
            logger.error("❌ Could not detect column positions")
            return None
        
        # Parse data lines with enhanced error handling
        logger.info(f"📊 Parsing data lines starting from line {data_start_idx+1}")
        station_data = []
        lines_processed = 0
        lines_skipped = 0
        parsing_errors = []
        
        for i, line in enumerate(lines[data_start_idx:], data_start_idx + 1):
            line = line.rstrip('\n\r')
            
            # Skip empty lines
            if not line.strip():
                lines_skipped += 1
                continue
                
            # Skip lines that are too short to contain meaningful data
            if len(line) < 50:
                logger.debug(f"   ⚠️  Skipping short line {i}: '{line.strip()}'")
                lines_skipped += 1
                continue
            
            try:
                row_data = {}
                
                # Extract data for each column using enhanced fixed positions
                for col_name, start, end in column_specs:
                    if start < len(line):
                        if end <= len(line):
                            value = line[start:end].strip()
                        else:
                            # Handle lines shorter than expected
                            value = line[start:].strip()
                        
                        # Enhanced value cleaning
                        if value and value not in ['', '-', 'N/A', 'NULL']:
                            row_data[col_name] = value
                        else:
                            row_data[col_name] = None
                    else:
                        row_data[col_name] = None
                
                # Enhanced validation for essential fields
                station_id = row_data.get('station_id')
                station_name = row_data.get('station_name')
                
                if station_id and station_name and len(station_id) >= 5:
                    # Clean station_id - take only the first 5 digits and ensure it's numeric
                    clean_station_id = station_id[:5] if len(station_id) >= 5 else station_id
                    
                    # Validate station_id is numeric
                    try:
                        int(clean_station_id)
                        row_data['station_id'] = clean_station_id.zfill(5)  # Ensure 5 digits with leading zeros
                        
                        station_data.append(row_data)
                        lines_processed += 1
                        
                        # Log first few stations for debugging
                        if lines_processed <= 3:
                            logger.debug(f"   📊 Station {lines_processed}: {clean_station_id} - {station_name[:30]}...")
                    except ValueError:
                        logger.debug(f"   ⚠️  Line {i} has non-numeric station_id: '{station_id}'")
                        lines_skipped += 1
                else:
                    error_msg = f"Line {i} missing essential fields: station_id='{station_id}', station_name='{station_name}'"
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
        
        if parsing_errors and len(parsing_errors) <= 10:  # Show first 10 errors
            logger.debug("   📋 Parsing errors encountered:")
            for error in parsing_errors[:10]:
                logger.debug(f"      - {error}")
        elif len(parsing_errors) > 10:
            logger.debug(f"   📋 {len(parsing_errors)} parsing errors encountered (showing first 10)")
            for error in parsing_errors[:10]:
                logger.debug(f"      - {error}")
        
        if not station_data:
            logger.error("❌ No valid station data found")
            return None
        
        # Create DataFrame with enhanced processing
        logger.info("🔄 Creating DataFrame and cleaning data with enhanced features...")
        df = pd.DataFrame(station_data)
        
        # Enhanced data cleaning and conversion
        logger.info("   🧹 Cleaning station IDs...")
        df['station_id'] = df['station_id'].astype(str).str.strip().str.zfill(5)
        
        # Convert numeric fields with enhanced error handling
        logger.info("   🔢 Converting numeric fields...")
        numeric_fields = ['station_height', 'latitude', 'longitude']
        
        for field in numeric_fields:
            if field in df.columns:
                # Handle German decimal format (comma as decimal separator) and various formats
                df[field] = df[field].astype(str).str.replace(',', '.').str.strip()
                
                # Remove any non-numeric characters except decimal point and minus sign
                df[field] = df[field].str.replace(r'[^\d.-]', '', regex=True)
                
                # Convert to numeric with enhanced error handling
                original_count = len(df)
                df[field] = pd.to_numeric(df[field], errors='coerce')
                
                # Log conversion results
                valid_count = df[field].notna().sum()
                invalid_count = original_count - valid_count
                logger.debug(f"      {field}: {valid_count}/{original_count} valid values")
                if invalid_count > 0:
                    logger.debug(f"         {invalid_count} invalid values converted to NaN")
        
        # Convert date fields with enhanced error handling
        logger.info("   📅 Converting date fields...")
        date_fields = ['from_date', 'to_date']
        
        for field in date_fields:
            if field in df.columns:
                # Clean date strings
                df[field] = df[field].astype(str).str.strip()
                
                # Convert YYYYMMDD format to datetime with enhanced error handling
                original_count = len(df)
                df[field] = pd.to_datetime(df[field], format='%Y%m%d', errors='coerce')
                
                # Log conversion results
                valid_count = df[field].notna().sum()
                invalid_count = original_count - valid_count
                logger.debug(f"      {field}: {valid_count}/{original_count} valid dates")
                if invalid_count > 0:
                    logger.debug(f"         {invalid_count} invalid dates converted to NaT")
        
        # Clean text fields with enhanced processing
        logger.info("   📝 Cleaning text fields...")
        text_fields = ['station_name', 'state']
        
        for field in text_fields:
            if field in df.columns:
                # Remove extra whitespace and handle encoding issues
                df[field] = df[field].astype(str).str.strip()
                df[field] = df[field].replace(['nan', 'None', 'NULL', ''], None)
                
                # Clean up German characters and encoding issues
                df[field] = df[field].str.replace(r'\s+', ' ', regex=True)  # Multiple spaces to single
        
        # Enhanced validation and summary
        logger.info("📊 Final dataset summary:")
        logger.info(f"   📈 Total stations: {len(df)}")
        
        if 'station_id' in df.columns:
            unique_stations = df['station_id'].nunique()
            id_range = f"{df['station_id'].min()} to {df['station_id'].max()}"
            logger.info(f"   🆔 Unique stations: {unique_stations}")
            logger.info(f"   🆔 Station ID range: {id_range}")
        
        if 'latitude' in df.columns and 'longitude' in df.columns:
            valid_coords = df[['latitude', 'longitude']].notna().all(axis=1).sum()
            if valid_coords > 0:
                lat_range = f"{df['latitude'].min():.2f}° to {df['latitude'].max():.2f}°"
                lon_range = f"{df['longitude'].min():.2f}° to {df['longitude'].max():.2f}°"
                logger.info(f"   🌍 Valid coordinates: {valid_coords}/{len(df)}")
                logger.info(f"   🌍 Latitude range: {lat_range}")
                logger.info(f"   🌍 Longitude range: {lon_range}")
            else:
                logger.warning("   ⚠️  No valid coordinates found")
        
        if 'station_height' in df.columns:
            valid_heights = df['station_height'].notna().sum()
            if valid_heights > 0:
                alt_range = f"{df['station_height'].min():.0f}m to {df['station_height'].max():.0f}m"
                logger.info(f"   🏔️  Valid altitudes: {valid_heights}/{len(df)}")
                logger.info(f"   🏔️  Altitude range: {alt_range}")
            else:
                logger.warning("   ⚠️  No valid altitudes found")
        
        # Log sample stations
        logger.debug("   📋 Sample stations:")
        for i, (_, row) in enumerate(df.head(3).iterrows()):
            state_info = row.get('state', 'Unknown state')
            logger.debug(f"      {i+1}. {row['station_id']}: {row['station_name']} ({state_info})")
        
        logger.info(f"✅ Successfully parsed station info file: {len(df)} stations")
        return df
        
    except Exception as e:
        logger.error(f"💥 Failed to parse station info file: {e}")
        logger.error(f"🔍 Traceback: {traceback.format_exc()}")
        return None


def get_station_info_enhanced(station_df: pd.DataFrame, station_id: int, logger: logging.Logger) -> Optional[Dict[str, Any]]:
    """
    ENHANCED: Get station information for a specific station ID with improved lookup.
    
    This function looks up a specific station in the parsed station DataFrame
    and returns a standardized dictionary with station metadata.
    
    Args:
        station_df: DataFrame with station information from parse_station_info_file_enhanced
        station_id: Station ID to look up (e.g., 3 for station 00003)
        logger: Logger instance
        
    Returns:
        Dictionary with station information or None if not found:
        {
            'station_id': '00003',
            'station_name': 'AACHEN-ORSBACH',
            'latitude': 50.7827,
            'longitude': 6.0941,
            'station_altitude_m': 202.0,
            'state': 'Nordrhein-Westfalen',
            'region': 'Nordrhein-Westfalen'
        }
        
    Example:
        station_info = get_station_info_enhanced(station_df, station_id=3, logger)
        if station_info:
            print(f"Station: {station_info['station_name']}")
            print(f"Location: {station_info['latitude']}, {station_info['longitude']}")
    """
    if station_df is None or station_df.empty:
        logger.warning("❌ No station data available for lookup")
        return None
    
    # Enhanced station ID formatting - try multiple formats
    station_id_formats = [
        str(station_id).zfill(5),  # 5-digit with leading zeros (preferred)
        str(station_id),           # As-is
        f"{station_id:05d}",       # Alternative 5-digit formatting
    ]
    
    logger.debug(f"🔍 Looking up station {station_id} with enhanced search")
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
        
        # Enhanced debugging information
        available_stations = station_df['station_id'].unique()
        logger.debug(f"   📋 Available station IDs (first 10): {list(available_stations[:10])}")
        
        # Check if there are similar station IDs
        similar_stations = [sid for sid in available_stations if str(station_id) in str(sid)]
        if similar_stations:
            logger.debug(f"   🔍 Similar station IDs found: {similar_stations}")
        
        # Check for partial matches
        partial_matches = [sid for sid in available_stations if sid.endswith(str(station_id).zfill(3))]
        if partial_matches:
            logger.debug(f"   🔍 Partial matches (last 3 digits): {partial_matches}")
        
        return None
    
    # Take the first matching row (there should typically only be one)
    station_row = station_rows.iloc[0]
    
    logger.debug(f"   ✅ Found station: {station_row['station_name']}")
    
    # Create standardized station info dictionary with enhanced error handling
    try:
        station_info = {
            'station_id': str(station_row['station_id']) if pd.notna(station_row['station_id']) else '',
            'station_name': str(station_row['station_name']) if pd.notna(station_row['station_name']) else '',
            'latitude': float(station_row['latitude']) if pd.notna(station_row['latitude']) else 0.0,
            'longitude': float(station_row['longitude']) if pd.notna(station_row['longitude']) else 0.0,
            'station_altitude_m': float(station_row['station_height']) if pd.notna(station_row['station_height']) else 0.0,
            'state': str(station_row['state']) if pd.notna(station_row['state']) else '',
            'region': str(station_row['state']) if pd.notna(station_row['state']) else ''  # Use state as region
        }
        
        # Enhanced validation of retrieved data
        data_quality_issues = []
        
        if station_info['latitude'] == 0.0 and station_info['longitude'] == 0.0:
            data_quality_issues.append("Missing coordinates")
        
        if not station_info['station_name']:
            data_quality_issues.append("Missing station name")
        
        if station_info['station_altitude_m'] == 0.0:
            data_quality_issues.append("Missing altitude")
        
        if not station_info['state']:
            data_quality_issues.append("Missing state information")
        
        # Log the retrieved information with quality assessment
        logger.debug(f"   📊 Station info retrieved:")
        logger.debug(f"      🏢 Name: {station_info['station_name']}")
        logger.debug(f"      📍 Coordinates: {station_info['latitude']:.4f}°N, {station_info['longitude']:.4f}°E")
        logger.debug(f"      🏔️  Altitude: {station_info['station_altitude_m']:.0f}m")
        logger.debug(f"      🏛️  State: {station_info['state']}")
        
        if data_quality_issues:
            logger.debug(f"      ⚠️  Data quality issues: {', '.join(data_quality_issues)}")
        else:
            logger.debug(f"      ✅ Data quality: Complete")
        
        return station_info
        
    except Exception as e:
        logger.error(f"   ❌ Error creating station info dictionary: {e}")
        return None


def validate_station_data_enhanced(station_df: pd.DataFrame, logger: logging.Logger) -> Dict[str, Any]:
    """
    ENHANCED: Validate parsed station data and provide comprehensive quality metrics.
    
    This function performs comprehensive validation of the parsed station data
    to identify potential issues and provide detailed quality metrics.
    
    Args:
        station_df: DataFrame with parsed station information
        logger: Logger instance
        
    Returns:
        Dictionary with validation results and quality metrics
        
    Example:
        validation_results = validate_station_data_enhanced(station_df, logger)
        print(f"Data quality score: {validation_results['quality_score']:.2f}")
        print(f"Issues found: {len(validation_results['issues'])}")
    """
    if station_df is None or station_df.empty:
        logger.warning("❌ No station data to validate")
        return {
            'quality_score': 0.0, 
            'issues': ['No data available'],
            'metrics': {},
            'total_stations': 0,
            'recommendations': ['Ensure station data file exists and is readable']
        }
    
    logger.info(f"🔍 Validating station data with enhanced checks ({len(station_df)} stations)")
    
    issues = []
    quality_metrics = {}
    recommendations = []
    
    # Check for required columns
    required_columns = ['station_id', 'station_name', 'latitude', 'longitude']
    missing_columns = [col for col in required_columns if col not in station_df.columns]
    
    if missing_columns:
        issues.append(f"Missing required columns: {missing_columns}")
        logger.warning(f"   ❌ Missing columns: {missing_columns}")
        recommendations.append("Verify station file format and column headers")
    
    # Enhanced station ID validation
    if 'station_id' in station_df.columns:
        # Check for duplicates
        duplicate_ids = station_df['station_id'].duplicated().sum()
        if duplicate_ids > 0:
            issues.append(f"Found {duplicate_ids} duplicate station IDs")
            logger.warning(f"   ⚠️  {duplicate_ids} duplicate station IDs found")
            recommendations.append("Review data source for duplicate entries")
        
        # Check ID format consistency
        invalid_format_ids = station_df[~station_df['station_id'].str.match(r'^\d{5}$')]['station_id'].count()
        if invalid_format_ids > 0:
            issues.append(f"Found {invalid_format_ids} station IDs with invalid format")
            logger.warning(f"   ⚠️  {invalid_format_ids} station IDs don't match 5-digit format")
            recommendations.append("Standardize station ID format to 5 digits with leading zeros")
        
        quality_metrics['unique_stations'] = station_df['station_id'].nunique()
        quality_metrics['total_stations'] = len(station_df)
        quality_metrics['duplicate_rate'] = duplicate_ids / len(station_df) if len(station_df) > 0 else 0
    
    # Enhanced coordinate validation
    if 'latitude' in station_df.columns and 'longitude' in station_df.columns:
        # Check for reasonable coordinate ranges (Germany is roughly 47-55°N, 6-15°E)
        # Extended ranges to account for neighboring countries and territories
        invalid_lat = ((station_df['latitude'] < 45) | (station_df['latitude'] > 57)).sum()
        invalid_lon = ((station_df['longitude'] < 4) | (station_df['longitude'] > 17)).sum()
        
        if invalid_lat > 0:
            issues.append(f"Found {invalid_lat} stations with latitude outside expected range (45-57°N)")
            logger.warning(f"   ⚠️  {invalid_lat} stations with invalid latitude")
            recommendations.append("Review stations with coordinates outside Germany/Central Europe")
        
        if invalid_lon > 0:
            issues.append(f"Found {invalid_lon} stations with longitude outside expected range (4-17°E)")
            logger.warning(f"   ⚠️  {invalid_lon} stations with invalid longitude")
            recommendations.append("Review stations with coordinates outside Germany/Central Europe")
        
        # Check for missing coordinates
        missing_coords = (station_df['latitude'].isna() | station_df['longitude'].isna()).sum()
        if missing_coords > 0:
            issues.append(f"Found {missing_coords} stations with missing coordinates")
            logger.warning(f"   ⚠️  {missing_coords} stations with missing coordinates")
            recommendations.append("Obtain coordinate data for stations with missing location info")
        
        # Check for suspicious coordinates (0,0 or very similar values)
        zero_coords = ((station_df['latitude'] == 0) & (station_df['longitude'] == 0)).sum()
        if zero_coords > 0:
            issues.append(f"Found {zero_coords} stations with (0,0) coordinates")
            logger.warning(f"   ⚠️  {zero_coords} stations with (0,0) coordinates")
            recommendations.append("Verify coordinate data for stations at (0,0)")
        
        quality_metrics['valid_coordinates'] = len(station_df) - missing_coords - invalid_lat - invalid_lon - zero_coords
        quality_metrics['coordinate_completeness'] = (len(station_df) - missing_coords) / len(station_df) if len(station_df) > 0 else 0
    
    # Enhanced station name validation
    if 'station_name' in station_df.columns:
        empty_names = station_df['station_name'].isna().sum()
        if empty_names > 0:
            issues.append(f"Found {empty_names} stations with missing names")
            logger.warning(f"   ⚠️  {empty_names} stations with missing names")
            recommendations.append("Obtain station names for unnamed stations")
        
        # Check for very short names (likely incomplete)
        short_names = (station_df['station_name'].str.len() < 3).sum()
        if short_names > 0:
            issues.append(f"Found {short_names} stations with very short names (< 3 characters)")
            logger.warning(f"   ⚠️  {short_names} stations with very short names")
            recommendations.append("Review stations with unusually short names")
        
        quality_metrics['named_stations'] = len(station_df) - empty_names
        quality_metrics['name_completeness'] = (len(station_df) - empty_names) / len(station_df) if len(station_df) > 0 else 0
    
    # Enhanced altitude validation
    if 'station_height' in station_df.columns:
        missing_altitude = station_df['station_height'].isna().sum()
        
        # Check for reasonable altitude ranges (Germany: -4m to 2962m, extended for safety)
        invalid_altitude = ((station_df['station_height'] < -10) | (station_df['station_height'] > 3000)).sum()
        
        if missing_altitude > 0:
            issues.append(f"Found {missing_altitude} stations with missing altitude")
            logger.warning(f"   ⚠️  {missing_altitude} stations with missing altitude")
        
        if invalid_altitude > 0:
            issues.append(f"Found {invalid_altitude} stations with altitude outside expected range (-10m to 3000m)")
            logger.warning(f"   ⚠️  {invalid_altitude} stations with invalid altitude")
            recommendations.append("Review stations with extreme altitude values")
        
        quality_metrics['valid_altitudes'] = len(station_df) - missing_altitude - invalid_altitude
        quality_metrics['altitude_completeness'] = (len(station_df) - missing_altitude) / len(station_df) if len(station_df) > 0 else 0
    
    # Enhanced date validation
    date_fields = ['from_date', 'to_date']
    for field in date_fields:
        if field in station_df.columns:
            missing_dates = station_df[field].isna().sum()
            if missing_dates > 0:
                issues.append(f"Found {missing_dates} stations with missing {field}")
                logger.warning(f"   ⚠️  {missing_dates} stations with missing {field}")
            
            quality_metrics[f'{field}_completeness'] = (len(station_df) - missing_dates) / len(station_df) if len(station_df) > 0 else 0
    
    # Calculate overall quality score with enhanced weighting
    total_stations = len(station_df)
    if total_stations > 0:
        valid_coords = quality_metrics.get('valid_coordinates', 0)
        named_stations = quality_metrics.get('named_stations', 0)
        unique_stations = quality_metrics.get('unique_stations', 0)
        valid_altitudes = quality_metrics.get('valid_altitudes', 0)
        
        # Enhanced quality score calculation
        quality_score = (
            (valid_coords / total_stations * 0.3) +           # Coordinates: 30%
            (named_stations / total_stations * 0.25) +        # Names: 25%
            (unique_stations / total_stations * 0.25) +       # Uniqueness: 25%
            (valid_altitudes / total_stations * 0.2)          # Altitudes: 20%
        )
    else:
        quality_score = 0.0
    
    # Enhanced logging of validation results
    logger.info(f"   📊 Enhanced validation results:")
    logger.info(f"      ✅ Valid coordinates: {quality_metrics.get('valid_coordinates', 0)}/{total_stations}")
    logger.info(f"      ✅ Named stations: {quality_metrics.get('named_stations', 0)}/{total_stations}")
    logger.info(f"      ✅ Unique stations: {quality_metrics.get('unique_stations', 0)}/{total_stations}")
    logger.info(f"      ✅ Valid altitudes: {quality_metrics.get('valid_altitudes', 0)}/{total_stations}")
    logger.info(f"      📈 Overall quality score: {quality_score:.2f}/1.00")
    
    # Quality assessment
    if quality_score >= 0.9:
        quality_assessment = "Excellent"
    elif quality_score >= 0.8:
        quality_assessment = "Good"
    elif quality_score >= 0.7:
        quality_assessment = "Fair"
    elif quality_score >= 0.5:
        quality_assessment = "Poor"
    else:
        quality_assessment = "Very Poor"
    
    logger.info(f"      🎯 Data quality assessment: {quality_assessment}")
    
    if issues:
        logger.warning(f"   ⚠️  Issues found: {len(issues)}")
        for i, issue in enumerate(issues[:10], 1):  # Show first 10 issues
            logger.warning(f"      {i}. {issue}")
        if len(issues) > 10:
            logger.warning(f"      ... and {len(issues) - 10} more issues")
    else:
        logger.info("   ✅ No validation issues found")
    
    if recommendations:
        logger.info(f"   💡 Recommendations: {len(recommendations)}")
        for i, rec in enumerate(recommendations[:5], 1):  # Show first 5 recommendations
            logger.info(f"      {i}. {rec}")
    
    return {
        'quality_score': quality_score,
        'quality_assessment': quality_assessment,
        'issues': issues,
        'recommendations': recommendations,
        'metrics': quality_metrics,
        'total_stations': total_stations
    }


def find_stations_by_name_enhanced(station_df: pd.DataFrame, name_pattern: str, logger: logging.Logger) -> List[Dict[str, Any]]:
    """
    ENHANCED: Find stations by name pattern with fuzzy matching.
    
    Args:
        station_df: DataFrame with station information
        name_pattern: Pattern to search for (supports partial matches)
        logger: Logger instance
        
    Returns:
        List of matching station dictionaries
        
    Example:
        matches = find_stations_by_name_enhanced(station_df, "AACHEN", logger)
        # Returns stations with "AACHEN" in the name
    """
    if station_df is None or station_df.empty:
        logger.warning("❌ No station data available for search")
        return []
    
    if 'station_name' not in station_df.columns:
        logger.warning("❌ No station_name column available for search")
        return []
    
    logger.info(f"🔍 Searching for stations matching pattern: '{name_pattern}'")
    
    # Case-insensitive search
    pattern_lower = name_pattern.lower()
    matches = station_df[station_df['station_name'].str.lower().str.contains(pattern_lower, na=False)]
    
    logger.info(f"   ✅ Found {len(matches)} matching stations")
    
    # Convert to list of dictionaries
    results = []
    for _, row in matches.iterrows():
        station_info = get_station_info_enhanced(pd.DataFrame([row]), row['station_id'], logger)
        if station_info:
            results.append(station_info)
    
    return results


def get_stations_in_region_enhanced(station_df: pd.DataFrame, lat_min: float, lat_max: float, 
                                  lon_min: float, lon_max: float, logger: logging.Logger) -> List[Dict[str, Any]]:
    """
    ENHANCED: Get stations within a geographic bounding box.
    
    Args:
        station_df: DataFrame with station information
        lat_min: Minimum latitude
        lat_max: Maximum latitude
        lon_min: Minimum longitude
        lon_max: Maximum longitude
        logger: Logger instance
        
    Returns:
        List of stations within the specified region
        
    Example:
        # Get stations in North Rhine-Westphalia region
        nrw_stations = get_stations_in_region_enhanced(
            station_df, 50.3, 52.5, 5.9, 9.5, logger
        )
    """
    if station_df is None or station_df.empty:
        logger.warning("❌ No station data available for region search")
        return []
    
    required_cols = ['latitude', 'longitude']
    if not all(col in station_df.columns for col in required_cols):
        logger.warning(f"❌ Missing required columns for region search: {required_cols}")
        return []
    
    logger.info(f"🌍 Searching for stations in region: {lat_min}°-{lat_max}°N, {lon_min}°-{lon_max}°E")
    
    # Filter stations within bounding box
    mask = (
        (station_df['latitude'] >= lat_min) & (station_df['latitude'] <= lat_max) &
        (station_df['longitude'] >= lon_min) & (station_df['longitude'] <= lon_max) &
        station_df['latitude'].notna() & station_df['longitude'].notna()
    )
    
    matches = station_df[mask]
    logger.info(f"   ✅ Found {len(matches)} stations in specified region")
    
    # Convert to list of dictionaries
    results = []
    for _, row in matches.iterrows():
        station_info = get_station_info_enhanced(pd.DataFrame([row]), row['station_id'], logger)
        if station_info:
            results.append(station_info)
    
    return results


# Legacy function names for backward compatibility
def parse_station_info_file(file_path: Path, logger: logging.Logger) -> Optional[pd.DataFrame]:
    """Legacy function name - redirects to enhanced version."""
    logger.debug("🔄 Using enhanced station info parsing (legacy function name)")
    return parse_station_info_file_enhanced(file_path, logger)


def get_station_info(station_df: pd.DataFrame, station_id: int, logger: logging.Logger) -> Optional[Dict[str, Any]]:
    """Legacy function name - redirects to enhanced version."""
    logger.debug("🔄 Using enhanced station info lookup (legacy function name)")
    return get_station_info_enhanced(station_df, station_id, logger)


def validate_station_data(station_df: pd.DataFrame, logger: logging.Logger) -> Dict[str, Any]:
    """Legacy function name - redirects to enhanced version."""
    logger.debug("🔄 Using enhanced station data validation (legacy function name)")
    return validate_station_data_enhanced(station_df, logger)


if __name__ == "__main__":
    """
    Test the enhanced station info parser functionality when run directly.
    
    This provides comprehensive testing to verify that the parser is working correctly.
    Run this file directly to test: python -m app.parsing.station_info_parser
    """
    import traceback
    
    print("Testing ClimaStation Enhanced Station Info Parser...")
    
    # Create a proper test logger
    test_logger = logging.getLogger("test_station_parser_enhanced")
    test_logger.setLevel(logging.DEBUG)
    
    # Add console handler for test output
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    test_logger.addHandler(console_handler)
    
    # Test file path (adjust as needed for your setup)
    test_file_path = Path("data/1_raw/station_info/historical_stations.txt")
    
    try:
        print(f"\n🧪 Testing enhanced station info parsing with file: {test_file_path}")
        
        # Test parsing
        station_df = parse_station_info_file_enhanced(test_file_path, test_logger)
        
        if station_df is not None:
            print(f"✅ Successfully parsed {len(station_df)} stations")
            
            # Test validation
            validation_results = validate_station_data_enhanced(station_df, test_logger)
            print(f"📊 Data quality score: {validation_results['quality_score']:.2f}")
            
            # Test station lookup
            if len(station_df) > 0:
                test_station_id = int(station_df.iloc[0]['station_id'])
                station_info = get_station_info_enhanced(station_df, test_station_id, test_logger)
                
                if station_info:
                    print(f"✅ Station lookup test successful:")
                    print(f"   Station: {station_info['station_name']}")
                    print(f"   Location: {station_info['latitude']:.4f}°N, {station_info['longitude']:.4f}°E")
                else:
                    print("❌ Station lookup test failed")
            
            # Test search functionality
            if len(station_df) > 0:
                search_results = find_stations_by_name_enhanced(station_df, "BERLIN", test_logger)
                print(f"🔍 Found {len(search_results)} stations matching 'BERLIN'")
            
            print("✅ All enhanced tests completed successfully!")
            
        else:
            print("❌ Failed to parse station info file")
            
    except Exception as e:
        print(f"💥 Test failed with error: {e}")
        print(f"🔍 Traceback: {traceback.format_exc()}")
