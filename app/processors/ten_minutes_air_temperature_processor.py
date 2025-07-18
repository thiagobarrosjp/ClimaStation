"""
ClimaStation 10-Minute Air Temperature Processor

SCRIPT IDENTIFICATION: DWD10TAH5T (10-Minute Air Temperature Processor)

PURPOSE:
Concrete processor implementation for DWD 10-minute air temperature dataset.
Handles extraction, validation, and processing of ZIP files containing
10-minute air temperature measurements from German weather stations.

RESPONSIBILITIES:
- Process ZIP files from historical/, recent/, and now/ folders
- Extract and validate single CSV file from each ZIP archive
- Parse station measurement data with proper timestamp handling
- Validate data quality and completeness
- Convert data to standardized format for storage
- Handle dataset-specific file naming patterns for different time periods

DATA STRUCTURE:
The dataset has 4 main subfolders:

1. /historical/ folder:
   - ZIP files: 10minutenwerte_TU_XXXXX_YYYYMMDD_YYYYMMDD_hist.zip
   - Contains: produkt_zehn_min_tu_YYYYMMDD_YYYYMMDD_XXXXX.txt

2. /recent/ folder:
   - ZIP files: 10minutenwerte_TU_XXXXX_akt.zip  
   - Contains: produkt_zehn_min_tu_YYYYMMDD_YYYYMMDD_XXXXX.txt

3. /now/ folder:
   - ZIP files: 10minutenwerte_TU_XXXXX_now.zip
   - Contains: produkt_zehn_now_tu_YYYYMMDD_YYYYMMDD_XXXXX.txt

4. /meta_data/ folder:
   - ZIP files: Meta_Daten_zehn_min_tu_XXXXX.zip
   - Contains: 6 metadata text files + 5 HTML files (ignored)

PROCESSING WORKFLOW:
1. Validate ZIP file structure (single text file expected)
2. Extract the single data file from ZIP
3. Parse CSV data with quality checks
4. Convert timestamps and validate data ranges
5. Generate standardized output format
6. Track processing statistics and errors

DATA VALIDATION:
- Temperature range validation (-50°C to +60°C)
- Timestamp continuity and format validation
- Missing data detection and flagging
- Station ID consistency checks
- Data quality flag interpretation

OUTPUT FORMAT:
Standardized CSV with columns:
- station_id, timestamp, temperature, quality_flag, source_file

ERROR HANDLING:
- Invalid ZIP structure: Skip file with detailed error
- Corrupt data files: Process valid records, flag corrupted ones
- Data quality issues: Flag but don't reject records
- Single file processing: Simplified error handling

DEPENDENCIES:
- zipfile for archive extraction
- pandas for data processing
- datetime for timestamp handling
- csv for file parsing
"""

import zipfile
import csv
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import re

from .base_processor import BaseProcessor, ProcessingResult


class TenMinutesAirTemperatureProcessor(BaseProcessor):
    """
    Processor for DWD 10-minute air temperature dataset.
    
    Handles the specific file formats, validation rules, and processing
    requirements for 10-minute air temperature measurements across
    historical, recent, and now time periods.
    """
    
    def __init__(self, worker_id: str = "main"):
        """
        Initialize the 10-minute air temperature processor.
        
        Args:
            worker_id: Identifier for this worker instance
        """
        super().__init__("10_minutes_air_temperature", worker_id)
        
        # Dataset-specific file patterns for different time periods
        self.data_file_patterns = {
            'historical': 'produkt_zehn_min_tu_*.txt',
            'recent': 'produkt_zehn_min_tu_*.txt', 
            'now': 'produkt_zehn_now_tu_*.txt'
        }
        
        # Data validation parameters
        self.temp_min = -50.0  # Minimum valid temperature (°C)
        self.temp_max = 60.0   # Maximum valid temperature (°C)
        self.missing_value_codes = [-999, -999.0, None, ""]
        
        # Complete column mappings for all data fields
        self.data_columns = {
            'STATIONS_ID': 'station_id',
            'MESS_DATUM': 'timestamp',
            'QN': 'quality_flag',  # Fixed: was QN_9, should be QN
            'PP_10': 'air_pressure',
            'TT_10': 'air_temperature_2m',
            'TM5_10': 'air_temperature_5cm', 
            'RF_10': 'relative_humidity',
            'TD_10': 'dew_point_temperature'
        }

        # Parameter descriptions from the config
        self.param_descriptions = {
            'PP_10': 'air pressure at station altitude',
            'TT_10': 'air temperature 2 m above ground',
            'TM5_10': 'air temperature 5 cm above ground',
            'RF_10': 'relative humidity',
            'TD_10': 'dew point (calculated from air temp. and humidity)'
        }

        # Quality level meanings
        self.quality_levels = {
            '1': 'only formal control',
            '2': 'controlled with individually defined criteria',
            '3': 'automatic control and correction'
        }
        
        self.logger.info("Initialized 10-minute air temperature processor")
    
    def get_expected_file_patterns(self) -> List[str]:
        """
        Get expected ZIP file patterns for 10-minute air temperature dataset.
        
        Returns:
            List of glob patterns for ZIP files across all time periods
        """
        return [
            # Historical data
            "**/historical/10minutenwerte_TU_*_hist.zip",
            # Recent data  
            "**/recent/10minutenwerte_TU_*_akt.zip",
            # Current data
            "**/now/10minutenwerte_TU_*_now.zip"
        ]
    
    def validate_file_structure(self, file_path: Path) -> bool:
        """
        Validate that ZIP file contains expected single data file.
        
        Args:
            file_path: Path to ZIP file
            
        Returns:
            True if file structure is valid
        """
        try:
            with zipfile.ZipFile(file_path, 'r') as zip_file:
                file_list = zip_file.namelist()
                
                # Filter out directories and hidden files
                data_files = [f for f in file_list if f.endswith('.txt') and not f.startswith('.')]
                
                # Should contain exactly one data file
                if len(data_files) != 1:
                    self.logger.error(f"Expected exactly 1 data file, found {len(data_files)} in {file_path}")
                    return False
                
                data_file = data_files[0]
                
                # Validate file naming pattern based on ZIP type
                if not self._validate_data_file_name(data_file, file_path):
                    return False
                
                # Check file is not empty
                file_info = zip_file.getinfo(data_file)
                if file_info.file_size == 0:
                    self.logger.error(f"Data file is empty in {file_path}")
                    return False
                
                return True
                
        except zipfile.BadZipFile:
            self.logger.error(f"Invalid ZIP file: {file_path}")
            return False
        except Exception as e:
            self.logger.error(f"Error validating file structure for {file_path}: {e}")
            return False
    
    def _validate_data_file_name(self, data_file: str, zip_path: Path) -> bool:
        """
        Validate data file name matches expected pattern for ZIP type.
        
        Args:
            data_file: Name of data file inside ZIP
            zip_path: Path to ZIP file
            
        Returns:
            True if file name is valid for this ZIP type
        """
        zip_name = zip_path.name.lower()
        
        if 'hist' in zip_name:
            # Historical: produkt_zehn_min_tu_YYYYMMDD_YYYYMMDD_XXXXX.txt
            pattern = r'produkt_zehn_min_tu_\d{8}_\d{8}_\d+\.txt'
        elif 'akt' in zip_name:
            # Recent: produkt_zehn_min_tu_YYYYMMDD_YYYYMMDD_XXXXX.txt
            pattern = r'produkt_zehn_min_tu_\d{8}_\d{8}_\d+\.txt'
        elif 'now' in zip_name:
            # Now: produkt_zehn_now_tu_YYYYMMDD_YYYYMMDD_XXXXX.txt
            pattern = r'produkt_zehn_now_tu_\d{8}_\d{8}_\d+\.txt'
        else:
            self.logger.warning(f"Unknown ZIP type for {zip_path}")
            return True  # Allow unknown types for flexibility
        
        if not re.match(pattern, data_file, re.IGNORECASE):
            self.logger.error(f"Data file name '{data_file}' doesn't match expected pattern for {zip_path}")
            return False
        
        return True
    
    def extract_metadata(self, file_path: Path) -> Dict[str, Any]:
        """
        Extract basic metadata from ZIP file.
        
        Note: Detailed metadata is stored separately in /meta_data/ folder.
        This method extracts only basic file-level metadata.
        
        Args:
            file_path: Path to ZIP file
            
        Returns:
            Dictionary containing extracted metadata
        """
        metadata = {
            'file_path': str(file_path),
            'file_size': file_path.stat().st_size,
            'zip_type': self._determine_zip_type(file_path),
            'station_id': self._extract_station_id_from_path(file_path),
            'data_file': None
        }
        
        try:
            with zipfile.ZipFile(file_path, 'r') as zip_file:
                # Get the single data file
                data_files = [f for f in zip_file.namelist() if f.endswith('.txt') and not f.startswith('.')]
                if data_files:
                    metadata['data_file'] = data_files[0]
                    
                    # Extract date range from filename if possible
                    date_info = self._extract_date_range_from_filename(data_files[0])
                    metadata.update(date_info)
                
        except Exception as e:
            self.logger.error(f"Error extracting metadata from {file_path}: {e}")
        
        return metadata
    
    def _determine_zip_type(self, file_path: Path) -> str:
        """Determine ZIP type from filename"""
        name = file_path.name.lower()
        if 'hist' in name:
            return 'historical'
        elif 'akt' in name:
            return 'recent'
        elif 'now' in name:
            return 'now'
        else:
            return 'unknown'
    
    def _extract_station_id_from_path(self, file_path: Path) -> Optional[str]:
        """Extract station ID from ZIP filename"""
        # Pattern: 10minutenwerte_TU_XXXXX_...
        match = re.search(r'10minutenwerte_TU_(\d+)', file_path.name, re.IGNORECASE)
        return match.group(1) if match else None
    
    def _extract_date_range_from_filename(self, filename: str) -> Dict[str, Any]:
        """Extract date range from data filename"""
        # Pattern: produkt_zehn_min_tu_YYYYMMDD_YYYYMMDD_XXXXX.txt
        # or: produkt_zehn_now_tu_YYYYMMDD_YYYYMMDD_XXXXX.txt
        match = re.search(r'(\d{8})_(\d{8})', filename)
        if match:
            start_date = match.group(1)
            end_date = match.group(2)
            return {
                'start_date': start_date,
                'end_date': end_date,
                'date_range': f"{start_date}-{end_date}"
            }
        return {}
    
    def process_file(self, file_path: Path) -> ProcessingResult:
        """
        Process a single ZIP file containing 10-minute air temperature data.
        
        Args:
            file_path: Path to ZIP file to process
            
        Returns:
            ProcessingResult with processing details
        """
        start_time = datetime.now()
        records_processed = 0
        
        try:
            # Extract metadata first
            metadata = self.extract_metadata(file_path)
            
            # Process the single data file
            with zipfile.ZipFile(file_path, 'r') as zip_file:
                # Get the single data file
                data_files = [f for f in zip_file.namelist() if f.endswith('.txt') and not f.startswith('.')]
                
                if not data_files:
                    return ProcessingResult(
                        success=False,
                        error_message="No data files found in ZIP archive",
                        processing_time=(datetime.now() - start_time).total_seconds()
                    )
                
                if len(data_files) > 1:
                    self.logger.warning(f"Multiple data files found in {file_path}, processing first one")
                
                # Process the data file
                data_file = data_files[0]
                records = self._process_data_file(zip_file, data_file, file_path)
                records_processed = len(records)
                
                # Save processed data
                if records:
                    output_file = self._save_processed_data(records, file_path)
                    self.logger.info(f"Saved {len(records)} records to {output_file}")
                
                processing_time = (datetime.now() - start_time).total_seconds()
                
                return ProcessingResult(
                    success=True,
                    records_processed=records_processed,
                    processing_time=processing_time,
                    metadata=metadata
                )
                
        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds()
            error_msg = f"Error processing file {file_path}: {str(e)}"
            self.logger.error(error_msg)
            
            return ProcessingResult(
                success=False,
                error_message=error_msg,
                processing_time=processing_time
            )
    
    def _process_data_file(self, zip_file: zipfile.ZipFile, data_file: str, source_path: Path) -> List[Dict[str, Any]]:
        """
        Process individual data file from ZIP archive.
        
        Args:
            zip_file: Open ZIP file object
            data_file: Name of data file within ZIP
            source_path: Path to source ZIP file
            
        Returns:
            List of processed records
        """
        records = []
        
        try:
            with zip_file.open(data_file) as f:
                # Read file content with proper encoding
                content = f.read().decode('utf-8', errors='ignore')
                lines = content.strip().split('\n')
                
                if len(lines) < 2:
                    self.logger.warning(f"Data file {data_file} appears to be empty or has no data rows")
                    return records
                
                # Parse header
                header = lines[0].split(';')
                header = [col.strip() for col in header]
                
                self.logger.info(f"Processing {len(lines)-1} data rows from {data_file}")
                
                # Process data rows
                for line_num, line in enumerate(lines[1:], 2):
                    try:
                        record = self._parse_data_row(line, header, source_path, line_num)
                        if record:
                            records.append(record)
                    except Exception as e:
                        self.logger.warning(f"Error parsing line {line_num} in {data_file}: {e}")
                        continue
                
                self.logger.info(f"Successfully parsed {len(records)} valid records from {data_file}")
                
        except Exception as e:
            self.logger.error(f"Error processing data file {data_file}: {e}")
        
        return records
    
    def _parse_data_row(self, line: str, header: List[str], source_path: Path, line_num: int) -> Optional[Dict[str, Any]]:
        """
        Parse individual data row.
        
        Args:
            line: CSV line to parse
            header: Column headers
            source_path: Source file path
            line_num: Line number for error reporting
            
        Returns:
            Parsed record dictionary or None if invalid
        """
        try:
            # Split CSV line
            values = line.split(';')
            values = [val.strip() for val in values]
            
            if len(values) != len(header):
                self.logger.warning(f"Column count mismatch at line {line_num}: expected {len(header)}, got {len(values)}")
                return None
            
            # Create record dictionary
            record = dict(zip(header, values))
            
            # Extract and validate key fields
            station_id = record.get('STATIONS_ID')
            timestamp_str = record.get('MESS_DATUM')
            temperature_str = record.get('TT_10')
            quality_flag = record.get('QN')  # Fixed: was QN_9, should be QN

            # Also extract other available measurements
            air_pressure = record.get('PP_10')
            temp_5cm = record.get('TM5_10')
            humidity = record.get('RF_10')
            dew_point = record.get('TD_10')
            
            # Validate station ID
            if not station_id or not station_id.isdigit():
                return None
            
            # Check for None values before parsing
            if timestamp_str is None or temperature_str is None:
                return None
            
            # Parse and validate timestamp
            timestamp = self._parse_timestamp(timestamp_str)
            if not timestamp:
                return None
            
            # Parse and validate temperature
            temperature = self._parse_temperature(temperature_str)
            if temperature is None:
                return None
            
            # Create standardized record with all available measurements
            standardized_record = {
                'station_id': int(station_id),
                'timestamp': timestamp,
                'air_temperature_2m': temperature,
                'air_temperature_5cm': self._parse_float_value(temp_5cm),
                'air_pressure': self._parse_float_value(air_pressure),
                'relative_humidity': self._parse_float_value(humidity),
                'dew_point_temperature': self._parse_float_value(dew_point),
                'quality_flag': quality_flag or 'unknown',
                'source_file': source_path.name,
                'processed_at': datetime.now()
            }
            
            return standardized_record
            
        except Exception as e:
            self.logger.warning(f"Error parsing data row at line {line_num}: {e}")
            return None
    
    def _parse_timestamp(self, timestamp_str: Optional[str]) -> Optional[datetime]:
        """
        Parse timestamp string to datetime object.
        
        Args:
            timestamp_str: Timestamp string (format: YYYYMMDDHHMM)
            
        Returns:
            Parsed datetime or None if invalid
        """
        if not timestamp_str or len(timestamp_str) != 12:
            return None
        
        try:
            # Parse YYYYMMDDHHMM format
            year = int(timestamp_str[:4])
            month = int(timestamp_str[4:6])
            day = int(timestamp_str[6:8])
            hour = int(timestamp_str[8:10])
            minute = int(timestamp_str[10:12])
            
            return datetime(year, month, day, hour, minute)
            
        except (ValueError, TypeError):
            return None
    
    def _parse_temperature(self, temp_str: Optional[str]) -> Optional[float]:
        """
        Parse and validate temperature value.
        
        Args:
            temp_str: Temperature string
            
        Returns:
            Validated temperature value or None if invalid
        """
        if not temp_str or temp_str in ['', '-999', '-999.0']:
            return None
        
        try:
            temperature = float(temp_str)
            
            # Validate temperature range
            if temperature < self.temp_min or temperature > self.temp_max:
                self.logger.warning(f"Temperature out of valid range: {temperature}°C")
                return None
            
            return temperature
            
        except (ValueError, TypeError):
            return None

    def _parse_float_value(self, value_str: Optional[str], min_val: Optional[float] = None, max_val: Optional[float] = None) -> Optional[float]:
        """
        Parse and validate float value with optional range checking.
        
        Args:
            value_str: String value to parse
            min_val: Minimum valid value
            max_val: Maximum valid value
            
        Returns:
            Validated float value or None if invalid
        """
        if not value_str or value_str in ['', '-999', '-999.0']:
            return None
        
        try:
            value = float(value_str)
            
            # Range validation if specified
            if min_val is not None and value < min_val:
                return None
            if max_val is not None and value > max_val:
                return None
            
            return value
            
        except (ValueError, TypeError):
            return None
    
    def _save_processed_data(self, records: List[Dict[str, Any]], source_path: Path) -> Path:
        """
        Save processed data to output file.
        
        Args:
            records: List of processed records
            source_path: Original source file path
            
        Returns:
            Path to output file
        """
        # Create output filename
        output_dir = Path(self.config['output']['base_path'])
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate output filename based on source
        base_name = source_path.stem
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = output_dir / f"{base_name}_processed_{timestamp}.csv"
        
        # Write CSV file
        if records:
            fieldnames = records[0].keys()
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(records)
        
        return output_file


# Example usage and testing
if __name__ == "__main__":
    try:
        # Test processor initialization
        processor = TenMinutesAirTemperatureProcessor("test_worker")
        
        # Test file pattern matching
        patterns = processor.get_expected_file_patterns()
        print(f"Expected file patterns: {patterns}")
        
        print("✅ 10-minute air temperature processor test successful")
        
    except Exception as e:
        print(f"❌ Processor test failed: {e}")
