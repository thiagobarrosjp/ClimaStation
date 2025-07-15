"""
Configuration for 10-Minute Air Temperature Data Processing

This module contains all configuration constants, mappings, and translations
for processing German weather station 10-minute air temperature data from
the German Weather Service (DWD).

AUTHOR: ClimaStation Backend Pipeline
VERSION: Fixed for real-world DWD data compatibility
LAST UPDATED: 2025-01-15

KEY COMPONENTS:
- File paths and directory structure definitions
- Parameter name mappings (German ↔ English) for data translation
- Sensor type and measurement method translations
- Column name standardization mappings for inconsistent headers
- Quality level definitions and data validation rules

DATA SOURCES:
- German Weather Service (DWD) 10-minute air temperature data
- Historical, recent, and current data streams
- Metadata files with sensor specifications and station information


USAGE:
    from app.config.ten_minutes_air_temperature_config import RAW_BASE, PARAM_MAP
    
    # Access file paths
    historical_folder = RAW_BASE / "historical"
    
    # Use parameter mappings
    for csv_col, english_name in PARAM_MAP.items():
        print(f"{csv_col} -> {english_name}")
"""

from pathlib import Path

# =============================================================================
# DIRECTORY STRUCTURE AND FILE PATHS (UPDATED)
# =============================================================================

# FIXED: Added "germany" folder to match actual structure
RAW_BASE = Path("data/germany/2_downloaded_files/10_minutes/air_temperature")
PARSED_BASE = Path("data/germany/3_parsed_files/parsed_10_minutes/parsed_air_temperature")

STATION_INFO_FILE_NAME = "zehn_min_tu_Beschreibung_Stationen.txt"
STATION_INFO_FILE_HISTORICAL = RAW_BASE / "historical" / STATION_INFO_FILE_NAME
STATION_INFO_FILE_RECENT = RAW_BASE / "recent" / STATION_INFO_FILE_NAME
STATION_INFO_FILE_NOW = RAW_BASE / "now" / STATION_INFO_FILE_NAME

# =============================================================================
# PARAMETER MAPPINGS AND TRANSLATIONS
# =============================================================================

PARAM_NAME_MAP = {
    "Lufttemperatur": {
        "en": "air temperature 2 m above ground",
        "de": "Lufttemperatur"
    },
    "Momentane_Temperatur_In_5cm": {
        "en": "air temperature 5 cm above ground", 
        "de": "Momentane Temperatur in 5 cm"
    },
    "Rel_Feuchte": {
        "en": "relative humidity",
        "de": "relative Feuchte"
    },
    "Luftdruck": {
        "en": "air pressure at station altitude",
        "de": "Luftdruck"
    },
    # Add parameter names as they appear in the parameter file
    "PP_10": {
        "en": "air pressure at station altitude",
        "de": "Luftdruck in Stationshoehe"
    },
    "RF_10": {
        "en": "relative humidity",
        "de": "relative Feuchte"
    },
    "TD_10": {
        "en": "dew point temperature",
        "de": "Taupunkttemperatur"
    },
    "TM5_10": {
        "en": "air temperature 5 cm above ground",
        "de": "Momentane Temperatur in 5 cm"
    },
    "TT_10": {
        "en": "air temperature 2 m above ground",
        "de": "momentane Lufttemperatur"
    }
}

PARAM_MAP = {
    "TT_10": "air temperature 2 m above ground",
    "TM5_10": "air temperature 5 cm above ground", 
    "PP_10": "air pressure at station altitude",
    "RF_10": "relative humidity",
    "TD_10": "dew point (calculated from air temp. and humidity)"
}

# =============================================================================
# SENSOR AND MEASUREMENT TRANSLATIONS (FIXED)
# =============================================================================

MEASUREMENT_METHOD_TRANSLATIONS = {
    "Temperaturmessung, elektr.": {
        "en": "temperature measurement, electric",
        "de": "Temperaturmessung, elektr."
    },
    "Feuchtemessung, elektr.": {
        "en": "humidity measurement, electric", 
        "de": "Feuchtemessung, elektr."
    }
}

# FIXED: Added missing sensor type
SENSOR_TYPE_TRANSLATIONS = {
    "PT 100 (Luft)": {
        "en": "PT 100 (air)",
        "de": "PT 100 (Luft)"
    },
    "HYGROMER MP100": {
        "en": "HYGROMER MP100",
        "de": "HYGROMER MP100"
    },
    # ADDED: Missing sensor type from humidity device file
    "Feuchtesonde HMP45D": {
        "en": "Humidity probe HMP45D",
        "de": "Feuchtesonde HMP45D"
    }
}

# =============================================================================
# DATA QUALITY AND DESCRIPTIONS
# =============================================================================

QUALITY_LEVEL_MAP = {
    "1": "only formal control",
    "2": "controlled with individually defined criteria", 
    "3": "automatic control and correction"
}

DESCRIPTION_TRANSLATIONS = {
    "momentane Lufttemperatur in 2m Hoehe": "instantaneous air temperature at 2m height",
    "Momentane Temperatur in 5 cm Hoehe 10min": "instantaneous temperature at 5cm height",
    "Luftdruck in Stationshoehe der voran. 10 min": "air pressure at station altitude (preceding 10 min)",
    "relative Feucht in 2m Hoehe": "relative humidity at 2m height",
    "Taupunkttemperatur in 2m Hoehe": "dew point temperature at 2m height",
}

SOURCE_TRANSLATIONS = {
    "10-Minutenwerte von automatischen Stationen der 1. Generation (MIRIAM/AFMS2, ESAU-Daten bis 31.12.1999 (Zeitbezug ist MEZ)":
        "10-minute values from automatic stations (1st gen, MIRIAM/AFMS2, ESAU data until 31 Dec 1999, time reference is CET)",
    "10-Minutenwerte von automatischen Stationen der 1. Generation (MIRIAM/AFMS2, ESAU-Daten ab 01.01.2000 (Zeitbezug ist UTC)":
        "10-minute values from automatic stations (1st gen, MIRIAM/AFMS2, ESAU data from 1 Jan 2000, time reference is UTC)",
    "aus Messnetz2000":
        "from Messnetz2000",
}

# =============================================================================
# COLUMN NAME STANDARDIZATION (FIXED)
# =============================================================================

COLUMN_NAME_MAP = {
    # FIXED: Added lowercase variation found in actual data
    "Stations_ID": "station_id",
    "Station_ID": "station_id", 
    "stations_ID": "station_id",
    "STATIONS_ID": "station_id",
    "Stations_id": "station_id",  # This was already here
    "stations_id": "station_id",  # ADDED: Found in geography file
    
    # Date columns
    "Von_Datum": "from_date",
    "von_datum": "from_date",
    "Bis_Datum": "to_date", 
    "bis_datum": "to_date",
    
    # Station information
    "Stationsname": "station_name",
    "stationsname": "station_name",
    "Geogr.Breite": "latitude",
    "geogr.breite": "latitude", 
    "Geogr.Laenge": "longitude",
    "geogr.laenge": "longitude",
    "Stationshoehe": "station_altitude_m",
    "stationshoehe": "station_altitude_m",
    
    # ADDED: Geography file specific columns
    "Geo. Laenge [Grad]": "longitude",
    "Geo. Breite [Grad]": "latitude",
    "Stationshoehe [m]": "station_altitude_m",
    
    # Sensor specifications
    "Geberhoehe ueber Grund [m]": "sensor_height_m",
    "geberhoehe ueber grund [m]": "sensor_height_m",
    "Geraetetyp Name": "sensor_type",
    "geraetetyp name": "sensor_type",
    "Messverfahren": "measurement_method",
    "messverfahren": "measurement_method",
    
    # Parameter information
    "Parameter": "parameter",
    "parameter": "parameter",
    "Parameterbeschreibung": "parameter_description",
    "parameterbeschreibung": "parameter_description",
    "Einheit": "unit",
    "einheit": "unit",
    
    # Metadata fields
    "Datenquelle (Strukturversion=SV)": "data_source",
    "datenquelle (strukturversion=sv)": "data_source",
    "Zusatz-Info": "extra_info",
    "zusatz-info": "extra_info", 
    "Besonderheiten": "special_notes",
    "besonderheiten": "special_notes",
    "Literaturhinweis": "reference",
    "literaturhinweis": "reference",
    "eor": "eor",
}
