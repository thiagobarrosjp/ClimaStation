"""
Translation Manager for ClimaStation Pipeline

Provides metadata and translation lookups for meteorological data processing.
All translations are loaded during initialization to avoid runtime I/O.
"""

from pathlib import Path
from typing import Dict
import logging
import yaml
import csv
import pandas as pd

class TranslationManager:
    """
    Manages translations and metadata lookups for ClimaStation pipeline.
    
    Loads all translation data during initialization and provides fast lookups
    for station metadata, parameter names, and quality codes.
    """
    
    def __init__(self, dataset_config: dict):
        """
        Initialize translation manager with dataset configuration.
        
        Args:
            dataset_config: Dataset configuration dictionary containing paths
        """
        self.dataset_config = dataset_config
        self.logger = logging.getLogger(f"climastation.translation_manager")
        
        # Storage for loaded data
        self._station_metadata: Dict[str, dict] = {}
        self._parameter_translations: Dict[str, dict] = {}
        self._quality_translations: Dict[str, dict] = {}
        self._dwd_translations: Dict[str, dict] = {}
        
        # Load all translation data during initialization
        self._load_all_translations()
    
    def _load_all_translations(self) -> None:
        """Load all translation files and station metadata during initialization."""
        try:
            # Load parameter translations
            self._load_parameter_translations()
            
            # Load quality code translations
            self._load_quality_translations()
            
            # Load DWD-specific translations
            self._load_dwd_translations()
            
            # Load station metadata
            self._load_station_metadata()
            
            self.logger.info(
                "Translation manager initialized successfully",
                extra={
                    "stations_loaded": len(self._station_metadata),
                    "parameters_loaded": len(self._parameter_translations),
                    "quality_codes_loaded": len(self._quality_translations)
                }
            )
            
        except Exception as e:
            self.logger.error(
                f"Failed to initialize translation manager: {str(e)}",
                extra={"component": "TRANSLATION_MANAGER"}
            )
            raise
    
    def _load_parameter_translations(self) -> None:
        """Load meteorological parameter translations from YAML."""
        try:
            params_path = Path("app/translations/meteorological/parameters.yaml")
            if params_path.exists():
                with open(params_path, 'r', encoding='utf-8') as f:
                    self._parameter_translations = yaml.safe_load(f) or {}
            else:
                self.logger.warning(f"Parameter translations file not found: {params_path}")
                self._parameter_translations = {}
        except Exception as e:
            self.logger.error(f"Failed to load parameter translations: {str(e)}")
            self._parameter_translations = {}
    
    def _load_quality_translations(self) -> None:
        """Load quality code translations from YAML."""
        try:
            quality_path = Path("app/translations/meteorological/quality_codes.yaml")
            if quality_path.exists():
                with open(quality_path, 'r', encoding='utf-8') as f:
                    self._quality_translations = yaml.safe_load(f) or {}
            else:
                self.logger.warning(f"Quality translations file not found: {quality_path}")
                self._quality_translations = {}
        except Exception as e:
            self.logger.error(f"Failed to load quality translations: {str(e)}")
            self._quality_translations = {}
    
    def _load_dwd_translations(self) -> None:
        """Load DWD-specific translations from YAML."""
        try:
            dwd_path = Path("app/translations/providers/dwd.yaml")
            if dwd_path.exists():
                with open(dwd_path, 'r', encoding='utf-8') as f:
                    self._dwd_translations = yaml.safe_load(f) or {}
            else:
                self.logger.warning(f"DWD translations file not found: {dwd_path}")
                self._dwd_translations = {}
        except Exception as e:
            self.logger.error(f"Failed to load DWD translations: {str(e)}")
            self._dwd_translations = {}
    
    def _load_station_metadata(self) -> None:
        """Load station metadata from the configured station list file."""
        try:
            # Get station list path from dataset config
            station_list_path = self.dataset_config.get("station_list_path")
            if not station_list_path:
                self.logger.warning("No station_list_path configured in dataset config")
                return
            
            station_path = Path(station_list_path)
            if not station_path.exists():
                self.logger.warning(f"Station list file not found: {station_path}")
                return
            
            # Try to load with pandas first (more robust), fall back to csv
            try:
                df = pd.read_csv(station_path, sep=';', encoding='utf-8')
                for _, row in df.iterrows():
                    station_id = str(row.get('Stations_ID', row.get('STATIONS_ID', '')))
                    if station_id:
                        self._station_metadata[station_id] = {
                            'station_id': station_id,
                            'name': row.get('Stationsname', 'Unknown'),
                            'latitude': row.get('Geo. Breite [Grad]', row.get('geoBreite', None)),
                            'longitude': row.get('Geo. Laenge [Grad]', row.get('geoLaenge', None)),
                            'altitude': row.get('Stationshoehe [m]', row.get('Stationshoehe', None)),
                            'from_date': row.get('Von_Datum', row.get('von_datum', None)),
                            'to_date': row.get('Bis_Datum', row.get('bis_datum', None)),
                            'state': row.get('Bundesland', None)
                        }
            except Exception:
                # Fallback to csv.DictReader
                with open(station_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f, delimiter=';')
                    for row in reader:
                        station_id = str(row.get('Stations_ID', row.get('STATIONS_ID', '')))
                        if station_id:
                            self._station_metadata[station_id] = {
                                'station_id': station_id,
                                'name': row.get('Stationsname', 'Unknown'),
                                'latitude': row.get('Geo. Breite [Grad]', row.get('geoBreite', None)),
                                'longitude': row.get('Geo. Laenge [Grad]', row.get('geoLaenge', None)),
                                'altitude': row.get('Stationshoehe [m]', row.get('Stationshoehe', None)),
                                'from_date': row.get('Von_Datum', row.get('von_datum', None)),
                                'to_date': row.get('Bis_Datum', row.get('bis_datum', None)),
                                'state': row.get('Bundesland', None)
                            }
                            
        except Exception as e:
            self.logger.error(f"Failed to load station metadata: {str(e)}")
            self._station_metadata = {}
    
    def get_station_metadata(self, station_id: str) -> dict:
        """
        Returns metadata for the given station_id.
        Falls back to {"station_id": station_id, "name": "Unknown"} if not found.
        
        Args:
            station_id: Station identifier to look up
            
        Returns:
            Dictionary containing station metadata
        """
        station_id_str = str(station_id)
        
        if station_id_str in self._station_metadata:
            return self._station_metadata[station_id_str].copy()
        
        # Fallback for unknown stations
        return {
            "station_id": station_id_str,
            "name": "Unknown",
            "latitude": None,
            "longitude": None,
            "altitude": None,
            "from_date": None,
            "to_date": None,
            "state": None
        }
    
    def translate_parameter(self, code: str) -> str:
        """
        Returns the human-readable parameter name from parameters.yaml.
        Falls back to "Unknown parameter: {code}".
        
        Args:
            code: Parameter code to translate
            
        Returns:
            Human-readable parameter name
        """
        if code in self._parameter_translations:
            param_data = self._parameter_translations[code]
            # Prefer English translation, fall back to German, then raw code
            if isinstance(param_data, dict):
                return param_data.get('en', param_data.get('de', f"Unknown parameter: {code}"))
            else:
                return str(param_data)
        
        return f"Unknown parameter: {code}"
    
    def translate_quality_code(self, parameter: str, code: int) -> str:
        """
        Returns explanation for a quality code, based on parameter type.
        Falls back to "Unknown code: {code}".
        
        Args:
            parameter: Parameter name (currently not used for differentiation)
            code: Quality code to translate
            
        Returns:
            Human-readable quality code explanation
        """
        code_str = str(code)
        
        if code_str in self._quality_translations:
            quality_data = self._quality_translations[code_str]
            if isinstance(quality_data, dict):
                # Prefer English translation with description
                en_desc = quality_data.get('description_en')
                if en_desc:
                    return f"{quality_data.get('en', code_str)}: {en_desc}"
                else:
                    return quality_data.get('en', quality_data.get('de', f"Unknown code: {code}"))
            else:
                return str(quality_data)
        
        return f"Unknown code: {code}"
    
    def get_parameter_unit(self, code: str) -> str:
        """
        Returns the unit for a given parameter code.
        
        Args:
            code: Parameter code to get unit for
            
        Returns:
            Unit string or empty string if not found
        """
        if code in self._parameter_translations:
            param_data = self._parameter_translations[code]
            if isinstance(param_data, dict):
                return param_data.get('unit', '')
        return ''
    
    def translate_dwd_term(self, term: str) -> str:
        """
        Translate DWD-specific terms using the DWD translation file.
        
        Args:
            term: DWD term to translate
            
        Returns:
            Translated term or original term if not found
        """
        if term in self._dwd_translations:
            dwd_data = self._dwd_translations[term]
            if isinstance(dwd_data, dict):
                return dwd_data.get('en', dwd_data.get('de', term))
            else:
                return str(dwd_data)
        return term
    
    @staticmethod
    def from_config(dataset_config: dict) -> 'TranslationManager':
        """
        Factory method to load all translations and return a ready instance.
        Reads the appropriate station file using dataset_config["station_list_path"].
        
        Args:
            dataset_config: Dataset configuration dictionary
            
        Returns:
            Initialized TranslationManager instance
        """
        return TranslationManager(dataset_config)


# Example usage and inline tests
if __name__ == "__main__":
    # Example configuration
    example_config = {
        "station_list_path": "data/dwd/station_list.csv",
        "name": "10_minutes_air_temperature"
    }
    
    # Initialize translation manager
    tm = TranslationManager.from_config(example_config)
    
    # Test parameter translation
    print("Parameter TT_10:", tm.translate_parameter("TT_10"))
    print("Parameter UNKNOWN:", tm.translate_parameter("UNKNOWN"))
    
    # Test quality code translation
    print("Quality code 10:", tm.translate_quality_code("TT_10", 10))
    print("Quality code 999:", tm.translate_quality_code("TT_10", 999))
    
    # Test station metadata
    print("Station 3:", tm.get_station_metadata("3"))
    print("Station 99999:", tm.get_station_metadata("99999"))
    
    # Test parameter unit
    print("Unit for TT_10:", tm.get_parameter_unit("TT_10"))
    
    # Test DWD term translation
    print("DWD term 'Stationsname':", tm.translate_dwd_term("Stationsname"))
