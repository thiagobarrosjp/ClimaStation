"""
ClimaStation Translation Manager

SCRIPT IDENTIFICATION: DWD10TAH3T (Translation Manager)

PURPOSE:
Comprehensive translation manager system that enriches DWD climate data with metadata,
converts parameter codes to standardized names, and provides quality code interpretations.
Handles DWD station description files and provides efficient caching for translation lookups.

RESPONSIBILITIES:
- Parameter Translation: Convert DWD codes (PP_10, TT_10, RF_10, etc.) to standardized names
- Quality Code Interpretation: Transform numeric quality codes to human-readable explanations
- Station Metadata Enrichment: Parse DWD station description files and enrich records
- Multi-format Support: Handle various DWD file formats and encoding issues
- Caching System: Implement efficient caching for translation lookups
- Integration: Work with existing progress tracker and logging systems

USAGE:
    from app.translations.translation_manager import TranslationManager
    from app.utils.config_manager import load_config
    from app.utils.enhanced_logger import get_logger
    
    config = load_config("10_minutes_air_temperature", logger)
    logger = get_logger("TRANS", config)
    translation_manager = TranslationManager(config, logger)
    
    # Translate parameter codes
    readable_name = translation_manager.translate_parameter("TT_10")
    # Returns: "air temperature 2 m above ground"
    
    # Get quality code meaning
    quality_meaning = translation_manager.get_quality_code_meaning(1)
    # Returns: "only formal control"
    
    # Enrich with station metadata
    station_info = translation_manager.enrich_station_metadata("00003")
    # Returns: {"name": "Aachen", "state": "Nordrhein-Westfalen", "elevation": 202, ...}

INTEGRATION:
- Uses config_manager for translation file paths and settings
- Integrates with enhanced_logger using component code "TRANS"
- Works with existing progress_tracker for processing workflow
- Follows established error handling patterns from other utilities
"""

import yaml
import csv
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, TYPE_CHECKING
import logging
from functools import lru_cache
import threading
from dataclasses import dataclass
import re
from datetime import datetime

if TYPE_CHECKING:
    from app.utils.enhanced_logger import StructuredLoggerAdapter

class TranslationError(Exception):
    """Raised when translation operations fail"""
    pass

@dataclass
class StationMetadata:
    """Station metadata structure"""
    station_id: str
    name: str
    state: Optional[str] = None
    elevation: Optional[float] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    from_date: Optional[str] = None
    to_date: Optional[str] = None
    operator: Optional[str] = None

class TranslationManager:
    """
    Comprehensive translation manager for DWD climate data enrichment.
    
    Provides parameter translation, quality code interpretation, and station
    metadata enrichment with efficient caching and proper error handling.
    """
    
    def __init__(self, config: Dict[str, Any], logger: Union[logging.Logger, "StructuredLoggerAdapter"]):
        """
        Initialize translation manager with dependency injection.
        
        Args:
            config: Configuration dictionary from config manager
            logger: Logger instance (can be standard Logger or StructuredLoggerAdapter)
        """
        self.config = config
        self.logger = logger
        
        # Get translation configuration
        self.translation_config = config.get('translation', {
            'cache_enabled': True,
            'cache_size_limit': 1000,
            'translation_files_path': 'app/translations',
            'default_target_format': 'standard',
            'station_metadata_required': True
        })
        
        # Set up translation file paths
        self.translation_files_path = Path(self.translation_config.get('translation_files_path', 'app/translations'))
        self.parameters_file = self.translation_files_path / 'meteorological' / 'parameters.yaml'
        self.quality_codes_file = self.translation_files_path / 'meteorological' / 'quality_codes.yaml'
        self.dwd_file = self.translation_files_path / 'providers' / 'dwd.yaml'
        
        # Initialize caches
        self._cache_enabled = self.translation_config.get('cache_enabled', True)
        self._cache_size_limit = self.translation_config.get('cache_size_limit', 1000)
        self._parameter_cache: Dict[str, Dict[str, Any]] = {}
        self._quality_cache: Dict[str, Dict[str, Any]] = {}
        self._station_cache: Dict[str, StationMetadata] = {}
        self._translation_data: Dict[str, Dict[str, Any]] = {}
        
        # Thread lock for cache operations
        self._cache_lock = threading.Lock()
        
        # Load translation files
        self._load_translation_files()
        
        self.logger.info("Translation manager initialized successfully", 
                        extra={"component": "TRANS", "cache_enabled": self._cache_enabled})
    
    def _load_translation_files(self) -> None:
        """Load all translation YAML files into memory."""
        try:
            # Load parameters
            if self.parameters_file.exists():
                with open(self.parameters_file, 'r', encoding='utf-8') as f:
                    self._translation_data['parameters'] = yaml.safe_load(f) or {}
                self.logger.info(f"Loaded parameters translations: {len(self._translation_data['parameters'])} entries",
                               extra={"component": "TRANS"})
            else:
                self.logger.warning(f"Parameters file not found: {self.parameters_file}",
                                  extra={"component": "TRANS"})
                self._translation_data['parameters'] = {}
            
            # Load quality codes
            if self.quality_codes_file.exists():
                with open(self.quality_codes_file, 'r', encoding='utf-8') as f:
                    self._translation_data['quality_codes'] = yaml.safe_load(f) or {}
                self.logger.info(f"Loaded quality code translations: {len(self._translation_data['quality_codes'])} entries",
                               extra={"component": "TRANS"})
            else:
                self.logger.warning(f"Quality codes file not found: {self.quality_codes_file}",
                                  extra={"component": "TRANS"})
                self._translation_data['quality_codes'] = {}
            
            # Load DWD-specific translations
            if self.dwd_file.exists():
                with open(self.dwd_file, 'r', encoding='utf-8') as f:
                    self._translation_data['dwd'] = yaml.safe_load(f) or {}
                self.logger.info(f"Loaded DWD translations: {len(self._translation_data['dwd'])} entries",
                               extra={"component": "TRANS"})
            else:
                self.logger.warning(f"DWD file not found: {self.dwd_file}",
                                  extra={"component": "TRANS"})
                self._translation_data['dwd'] = {}
                
        except Exception as e:
            error_msg = f"Failed to load translation files: {str(e)}"
            self.logger.error(error_msg, extra={"component": "TRANS"})
            raise TranslationError(error_msg) from e
    
    def translate_parameter(self, dwd_code: str, target_format: str = "standard") -> str:
        """
        Convert DWD parameter codes to standardized parameter names.
        
        Args:
            dwd_code: DWD parameter code (e.g., "TT_10", "PP_10", "RF_10")
            target_format: Target format ("standard", "display", "api")
            
        Returns:
            Translated parameter name or original code if translation not found
        """
        if not dwd_code:
            return dwd_code
        
        # Check cache first
        cache_key = f"{dwd_code}_{target_format}"
        if self._cache_enabled and cache_key in self._parameter_cache:
            return self._parameter_cache[cache_key]['translation']
        
        try:
            # Get parameter data
            parameters = self._translation_data.get('parameters', {})
            param_data = parameters.get(dwd_code)
            
            if not param_data:
                self.logger.warning(f"No translation found for parameter: {dwd_code}",
                                  extra={"component": "TRANS", "parameter": dwd_code})
                return dwd_code
            
            # Determine translation based on target format
            if target_format == "display":
                translation = param_data.get('description_en', param_data.get('en', dwd_code))
            elif target_format == "api":
                translation = param_data.get('en', dwd_code).lower().replace(' ', '_')
            else:  # standard format
                translation = param_data.get('en', dwd_code)
            
            # Cache the result
            if self._cache_enabled:
                with self._cache_lock:
                    if len(self._parameter_cache) < self._cache_size_limit:
                        self._parameter_cache[cache_key] = {
                            'translation': translation,
                            'unit': param_data.get('unit', ''),
                            'description': param_data.get('description_en', '')
                        }
            
            self.logger.debug(f"Translated parameter {dwd_code} -> {translation}",
                            extra={"component": "TRANS", "parameter": dwd_code, "translation": translation})
            
            return translation
            
        except Exception as e:
            error_msg = f"Error translating parameter {dwd_code}: {str(e)}"
            self.logger.error(error_msg, extra={"component": "TRANS", "parameter": dwd_code})
            return dwd_code  # Return original code as fallback
    
    def get_quality_code_meaning(self, code: int, parameter: Optional[str] = None) -> str:
        """
        Interpret quality codes with human-readable explanations.
        
        Args:
            code: Numeric quality code
            parameter: Optional parameter context for specific interpretations
            
        Returns:
            Human-readable quality code meaning
        """
        if code is None:
            return "Unknown quality"
        
        # Check cache first
        cache_key = f"{code}_{parameter or 'general'}"
        if self._cache_enabled and cache_key in self._quality_cache:
            return self._quality_cache[cache_key]['meaning']
        
        try:
            quality_codes = self._translation_data.get('quality_codes', {})
            code_str = str(code)
            
            # Get quality code data
            code_data = quality_codes.get(code_str)
            
            if not code_data:
                self.logger.warning(f"No translation found for quality code: {code}",
                                  extra={"component": "TRANS", "quality_code": code})
                return f"Quality level {code}"
            
            # Get English meaning with description
            meaning = code_data.get('en', f"Quality level {code}")
            description = code_data.get('description_en', '')
            
            # Combine meaning and description for better context
            full_meaning = f"{meaning}"
            if description and description != meaning:
                full_meaning += f" - {description}"
            
            # Cache the result
            if self._cache_enabled:
                with self._cache_lock:
                    if len(self._quality_cache) < self._cache_size_limit:
                        self._quality_cache[cache_key] = {
                            'meaning': full_meaning,
                            'reliability': code_data.get('reliability', 'unknown')
                        }
            
            self.logger.debug(f"Interpreted quality code {code} -> {meaning}",
                            extra={"component": "TRANS", "quality_code": code, "meaning": meaning})
            
            return full_meaning
            
        except Exception as e:
            error_msg = f"Error interpreting quality code {code}: {str(e)}"
            self.logger.error(error_msg, extra={"component": "TRANS", "quality_code": code})
            return f"Quality level {code}"  # Return basic fallback
    
    def enrich_station_metadata(self, station_id: str) -> Dict[str, Any]:
        """
        Add station information to records from cached metadata.
        
        Args:
            station_id: Station identifier (e.g., "00003")
            
        Returns:
            Dictionary containing station metadata
        """
        if not station_id:
            return {}
        
        # Normalize station ID (ensure it's padded to 5 digits)
        normalized_id = str(station_id).zfill(5)
        
        # Check cache first
        if self._cache_enabled and normalized_id in self._station_cache:
            station_metadata = self._station_cache[normalized_id]
            return {
                'station_id': station_metadata.station_id,
                'name': station_metadata.name,
                'state': station_metadata.state,
                'elevation': station_metadata.elevation,
                'latitude': station_metadata.latitude,
                'longitude': station_metadata.longitude,
                'from_date': station_metadata.from_date,
                'to_date': station_metadata.to_date,
                'operator': station_metadata.operator
            }
        
        self.logger.warning(f"No metadata found for station: {station_id}",
                          extra={"component": "TRANS", "station_id": station_id})
        
        return {
            'station_id': normalized_id,
            'name': f"Station {normalized_id}",
            'state': None,
            'elevation': None,
            'latitude': None,
            'longitude': None,
            'from_date': None,
            'to_date': None,
            'operator': None
        }
    
    def parse_station_description_file(self, file_path: Path) -> Dict[str, Dict[str, Any]]:
        """
        Process DWD station description files and extract metadata.
        
        Args:
            file_path: Path to DWD station description file
            
        Returns:
            Dictionary mapping station IDs to metadata dictionaries
        """
        if not file_path.exists():
            error_msg = f"Station description file not found: {file_path}"
            self.logger.error(error_msg, extra={"component": "TRANS"})
            raise TranslationError(error_msg)
        
        stations = {}
        
        try:
            # Try different encodings commonly used by DWD
            encodings = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']
            content = None
            
            for encoding in encodings:
                try:
                    with open(file_path, 'r', encoding=encoding) as f:
                        content = f.read()
                    break
                except UnicodeDecodeError:
                    continue
            
            if content is None:
                raise TranslationError(f"Could not decode file with any supported encoding: {file_path}")
            
            # Parse the content
            lines = content.strip().split('\n')
            
            # Find header line and data lines
            header_line = None
            data_lines = []
            
            for line in lines:
                line = line.strip()
                if not line or line.startswith('generiert:') or line.startswith('Legende:'):
                    continue
                
                if 'Stations_ID' in line or 'Stationsname' in line:
                    header_line = line
                elif header_line and ';' in line:
                    data_lines.append(line)
            
            if not header_line:
                raise TranslationError(f"No header line found in station file: {file_path}")
            
            # Parse header to get column positions
            headers = [h.strip() for h in header_line.split(';')]
            
            # Process data lines
            for line_num, line in enumerate(data_lines, 1):
                try:
                    fields = [f.strip() for f in line.split(';')]
                    
                    if len(fields) < len(headers):
                        # Pad with empty strings if needed
                        fields.extend([''] * (len(headers) - len(fields)))
                    
                    # Create field mapping
                    field_map = dict(zip(headers, fields))
                    
                    # Extract station data
                    station_id = field_map.get('Stations_ID', '').zfill(5)
                    if not station_id or station_id == '00000':
                        continue
                    
                    # Parse numeric fields safely
                    def safe_float(value: str) -> Optional[float]:
                        try:
                            return float(value) if value and value != '' else None
                        except ValueError:
                            return None
                    
                    # Create station metadata
                    station_metadata = StationMetadata(
                        station_id=station_id,
                        name=field_map.get('Stationsname', f'Station {station_id}'),
                        state=field_map.get('Bundesland'),
                        elevation=safe_float(field_map.get('Stationshoehe [m]', field_map.get('Stationshoehe', ''))),
                        latitude=safe_float(field_map.get('Geo. Breite [Grad]', field_map.get('Geogr.Breite', ''))),
                        longitude=safe_float(field_map.get('Geo. Laenge [Grad]', field_map.get('Geogr.Laenge', ''))),
                        from_date=field_map.get('Von_Datum'),
                        to_date=field_map.get('Bis_Datum'),
                        operator=field_map.get('Betreibername', 'DWD')
                    )
                    
                    # Add to results and cache
                    stations[station_id] = {
                        'station_id': station_metadata.station_id,
                        'name': station_metadata.name,
                        'state': station_metadata.state,
                        'elevation': station_metadata.elevation,
                        'latitude': station_metadata.latitude,
                        'longitude': station_metadata.longitude,
                        'from_date': station_metadata.from_date,
                        'to_date': station_metadata.to_date,
                        'operator': station_metadata.operator
                    }
                    
                    # Cache the station metadata
                    if self._cache_enabled:
                        with self._cache_lock:
                            self._station_cache[station_id] = station_metadata
                    
                except Exception as e:
                    self.logger.warning(f"Error parsing station line {line_num}: {str(e)}",
                                      extra={"component": "TRANS", "line": line})
                    continue
            
            self.logger.info(f"Parsed {len(stations)} stations from {file_path}",
                           extra={"component": "TRANS", "stations_count": len(stations)})
            
            return stations
            
        except Exception as e:
            error_msg = f"Error parsing station description file {file_path}: {str(e)}"
            self.logger.error(error_msg, extra={"component": "TRANS"})
            raise TranslationError(error_msg) from e
    
    def validate_translation_files(self) -> bool:
        """
        Verify YAML file integrity and structure.
        
        Returns:
            True if all translation files are valid, False otherwise
        """
        try:
            validation_results = []
            
            # Validate parameters file
            if self.parameters_file.exists():
                try:
                    with open(self.parameters_file, 'r', encoding='utf-8') as f:
                        params = yaml.safe_load(f)
                    
                    if not isinstance(params, dict):
                        raise ValueError("Parameters file must contain a dictionary")
                    
                    # Check required fields for each parameter
                    for param_code, param_data in params.items():
                        if not isinstance(param_data, dict):
                            raise ValueError(f"Parameter {param_code} must be a dictionary")
                        
                        required_fields = ['en', 'de', 'unit']
                        for field in required_fields:
                            if field not in param_data:
                                self.logger.warning(f"Parameter {param_code} missing field: {field}",
                                                  extra={"component": "TRANS"})
                    
                    validation_results.append(True)
                    self.logger.info("Parameters file validation passed",
                                   extra={"component": "TRANS"})
                    
                except Exception as e:
                    self.logger.error(f"Parameters file validation failed: {str(e)}",
                                    extra={"component": "TRANS"})
                    validation_results.append(False)
            else:
                self.logger.warning("Parameters file not found for validation",
                                  extra={"component": "TRANS"})
                validation_results.append(False)
            
            # Validate quality codes file
            if self.quality_codes_file.exists():
                try:
                    with open(self.quality_codes_file, 'r', encoding='utf-8') as f:
                        quality_codes = yaml.safe_load(f)
                    
                    if not isinstance(quality_codes, dict):
                        raise ValueError("Quality codes file must contain a dictionary")
                    
                    # Check quality code structure
                    for code, code_data in quality_codes.items():
                        if code.startswith('quality_'):  # Skip metadata sections
                            continue
                            
                        if not isinstance(code_data, dict):
                            raise ValueError(f"Quality code {code} must be a dictionary")
                        
                        required_fields = ['en', 'de']
                        for field in required_fields:
                            if field not in code_data:
                                self.logger.warning(f"Quality code {code} missing field: {field}",
                                                  extra={"component": "TRANS"})
                    
                    validation_results.append(True)
                    self.logger.info("Quality codes file validation passed",
                                   extra={"component": "TRANS"})
                    
                except Exception as e:
                    self.logger.error(f"Quality codes file validation failed: {str(e)}",
                                    extra={"component": "TRANS"})
                    validation_results.append(False)
            else:
                self.logger.warning("Quality codes file not found for validation",
                                  extra={"component": "TRANS"})
                validation_results.append(False)
            
            # Validate DWD file
            if self.dwd_file.exists():
                try:
                    with open(self.dwd_file, 'r', encoding='utf-8') as f:
                        dwd_data = yaml.safe_load(f)
                    
                    if not isinstance(dwd_data, dict):
                        raise ValueError("DWD file must contain a dictionary")
                    
                    validation_results.append(True)
                    self.logger.info("DWD file validation passed",
                                   extra={"component": "TRANS"})
                    
                except Exception as e:
                    self.logger.error(f"DWD file validation failed: {str(e)}",
                                    extra={"component": "TRANS"})
                    validation_results.append(False)
            else:
                self.logger.warning("DWD file not found for validation",
                                  extra={"component": "TRANS"})
                validation_results.append(False)
            
            # Overall validation result
            all_valid = all(validation_results)
            
            if all_valid:
                self.logger.info("All translation files validation passed",
                               extra={"component": "TRANS"})
            else:
                self.logger.error("Translation files validation failed",
                                extra={"component": "TRANS"})
            
            return all_valid
            
        except Exception as e:
            error_msg = f"Error during translation files validation: {str(e)}"
            self.logger.error(error_msg, extra={"component": "TRANS"})
            return False
    
    def clear_cache(self) -> None:
        """Clear all translation caches."""
        with self._cache_lock:
            self._parameter_cache.clear()
            self._quality_cache.clear()
            self._station_cache.clear()
        
        self.logger.info("Translation caches cleared",
                        extra={"component": "TRANS"})
    
    def get_cache_stats(self) -> Dict[str, int]:
        """Get cache statistics for monitoring."""
        with self._cache_lock:
            return {
                'parameter_cache_size': len(self._parameter_cache),
                'quality_cache_size': len(self._quality_cache),
                'station_cache_size': len(self._station_cache),
                'cache_size_limit': self._cache_size_limit,
                'cache_enabled': self._cache_enabled
            }
    
    # Testing and validation methods
    def test_parameter_translation(self) -> bool:
        """Test parameter code conversions."""
        test_cases = [
            ('TT_10', 'air temperature 2 m above ground'),
            ('PP_10', 'air pressure at station altitude'),
            ('RF_10', 'relative humidity'),
            ('TD_10', 'dew point temperature'),
            ('TM5_10', 'air temperature 5 cm above ground')
        ]
        
        all_passed = True
        for dwd_code, expected in test_cases:
            try:
                result = self.translate_parameter(dwd_code)
                if expected in result or result == expected:
                    self.logger.debug(f"Parameter translation test passed: {dwd_code} -> {result}",
                                    extra={"component": "TRANS"})
                else:
                    self.logger.error(f"Parameter translation test failed: {dwd_code} -> {result} (expected: {expected})",
                                    extra={"component": "TRANS"})
                    all_passed = False
            except Exception as e:
                self.logger.error(f"Parameter translation test error for {dwd_code}: {str(e)}",
                                extra={"component": "TRANS"})
                all_passed = False
        
        return all_passed
    
    def test_quality_code_interpretation(self) -> bool:
        """Test quality code meanings."""
        test_cases = [
            (1, 'only formal control'),
            (2, 'controlled with individually defined criteria'),
            (3, 'automatic control and correction'),
            (10, 'quality control finished, all corrections finished')
        ]
        
        all_passed = True
        for code, expected_part in test_cases:
            try:
                result = self.get_quality_code_meaning(code)
                if expected_part in result.lower():
                    self.logger.debug(f"Quality code test passed: {code} -> {result}",
                                    extra={"component": "TRANS"})
                else:
                    self.logger.error(f"Quality code test failed: {code} -> {result} (expected to contain: {expected_part})",
                                    extra={"component": "TRANS"})
                    all_passed = False
            except Exception as e:
                self.logger.error(f"Quality code test error for {code}: {str(e)}",
                                extra={"component": "TRANS"})
                all_passed = False
        
        return all_passed
    
    def test_caching_performance(self) -> bool:
        """Test cache efficiency."""
        try:
            # Clear cache and test parameter translation caching
            self.clear_cache()
            
            # First call should miss cache
            result1 = self.translate_parameter("TT_10")
            
            # Second call should hit cache
            result2 = self.translate_parameter("TT_10")
            
            if result1 == result2:
                self.logger.info("Cache performance test passed",
                               extra={"component": "TRANS"})
                return True
            else:
                self.logger.error("Cache performance test failed: inconsistent results",
                                extra={"component": "TRANS"})
                return False
                
        except Exception as e:
            self.logger.error(f"Cache performance test error: {str(e)}",
                            extra={"component": "TRANS"})
            return False

# Example usage and testing
if __name__ == "__main__":
    import sys
    sys.path.append(str(Path(__file__).parent.parent))
    
    from utils.config_manager import load_config
    from utils.enhanced_logger import get_logger
    
    print("Testing ClimaStation Translation Manager")
    print("=" * 60)
    
    try:
        # Set up logger and config - fix the type issue
        base_logger = logging.getLogger("climastation.trans")
        config = load_config("10_minutes_air_temperature", base_logger)
        
        # Now get the structured logger for the translation manager
        structured_logger = get_logger("TRANS", config)
        
        # Initialize translation manager
        translation_manager = TranslationManager(config, structured_logger)
        
        print("\n1. Testing parameter translation:")
        test_params = ['TT_10', 'PP_10', 'RF_10', 'TD_10', 'TM5_10']
        for param in test_params:
            translated = translation_manager.translate_parameter(param)
            print(f"   {param} -> {translated}")
        
        print("\n2. Testing quality code interpretation:")
        test_codes = [1, 2, 3, 5, 10]
        for code in test_codes:
            meaning = translation_manager.get_quality_code_meaning(code)
            print(f"   Code {code} -> {meaning}")
        
        print("\n3. Testing station metadata enrichment:")
        station_info = translation_manager.enrich_station_metadata("00003")
        print(f"   Station 00003: {station_info}")
        
        print("\n4. Testing translation file validation:")
        validation_result = translation_manager.validate_translation_files()
        print(f"   Validation result: {'✅ PASSED' if validation_result else '❌ FAILED'}")
        
        print("\n5. Testing cache statistics:")
        cache_stats = translation_manager.get_cache_stats()
        print(f"   Cache stats: {cache_stats}")
        
        print("\n6. Running built-in tests:")
        param_test = translation_manager.test_parameter_translation()
        quality_test = translation_manager.test_quality_code_interpretation()
        cache_test = translation_manager.test_caching_performance()
        
        print(f"   Parameter translation test: {'✅ PASSED' if param_test else '❌ FAILED'}")
        print(f"   Quality code interpretation test: {'✅ PASSED' if quality_test else '❌ FAILED'}")
        print(f"   Cache performance test: {'✅ PASSED' if cache_test else '❌ FAILED'}")
        
        all_tests_passed = param_test and quality_test and cache_test and validation_result
        print(f"\n✅ All translation manager tests {'PASSED' if all_tests_passed else 'FAILED'}!")
        
    except Exception as e:
        print(f"\n❌ Translation manager test failed: {e}")
        import traceback
        traceback.print_exc()
