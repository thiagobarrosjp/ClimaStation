"""
Configuration for 10-Minute Air Temperature Data Processing

This module contains all configuration constants, mappings, and translations
for processing German weather station 10-minute air temperature data.

Key Components:
- File paths and directory structure
- Parameter name mappings (German ↔ English)
- Sensor type and measurement method translations
- Column name standardization mappings
- Quality level definitions

Data Sources:
- German Weather Service (DWD) 10-minute air temperature data
- Historical, recent, and current data streams
- Metadata files with sensor specifications and station information
"""

from pathlib import Path

# =============================================================================
# DIRECTORY STRUCTURE AND FILE PATHS
# =============================================================================

# Base directories for raw and processed data
RAW_BASE = Path("data/germany/2_downloaded_files/10_minutes/air_temperature")
PARSED_BASE = Path("data/germany/3_parsed_files/parsed_10_minutes/parsed_air_temperature")

# Station information files for different data streams
STATION_INFO_FILE_NAME = "zehn_min_tu_Beschreibung_Stationen.txt"
STATION_INFO_FILE_HISTORICAL = RAW_BASE / "historical" / STATION_INFO_FILE_NAME
STATION_INFO_FILE_RECENT = RAW_BASE / "recent" / STATION_INFO_FILE_NAME
STATION_INFO_FILE_NOW = RAW_BASE / "now" / STATION_INFO_FILE_NAME

# =============================================================================
# PARAMETER MAPPINGS AND TRANSLATIONS
# =============================================================================

# Parameter name mapping from German to English (used in filenames and metadata)
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
    }
}

# Parameter column mapping for CSV data files
PARAM_MAP = {
    "TT_10": "air temperature 2 m above ground",
    "TM5_10": "air temperature 5 cm above ground", 
    "PP_10": "air pressure at station altitude",
    "RF_10": "relative humidity",
    "TD_10": "dew point (calculated from air temp. and humidity)"
}

# =============================================================================
# SENSOR AND MEASUREMENT TRANSLATIONS
# =============================================================================

# Measurement method translations (German → English)
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

# Sensor type translations (German → English)
SENSOR_TYPE_TRANSLATIONS = {
    "PT 100 (Luft)": {
        "en": "PT 100 (air)",
        "de": "PT 100 (Luft)"
    },
    "HYGROMER MP100": {
        "en": "HYGROMER MP100",
        "de": "HYGROMER MP100"
    }
}

# =============================================================================
# DATA QUALITY AND DESCRIPTIONS
# =============================================================================

# Quality level definitions for data validation
QUALITY_LEVEL_MAP = {
    "1": "only formal control",
    "2": "controlled with individually defined criteria", 
    "3": "automatic control and correction"
}

# Parameter description translations
DESCRIPTION_TRANSLATIONS = {
    "momentane Lufttemperatur in 2m Hoehe": "instantaneous air temperature at 2m height",
    "Momentane Temperatur in 5 cm Hoehe 10min": "instantaneous temperature at 5cm height",
    "Luftdruck in Stationshoehe der voran. 10 min": "air pressure at station altitude (preceding 10 min)",
    "relative Feucht in 2m Hoehe": "relative humidity at 2m height",
    "Taupunkttemperatur in 2m Hoehe": "dew point temperature at 2m height",
}

# Data source descriptions with time reference information
SOURCE_TRANSLATIONS = {
    "10-Minutenwerte von automatischen Stationen der 1. Generation (MIRIAM/AFMS2, ESAU-Daten bis 31.12.1999 (Zeitbezug ist MEZ)":
        "10-minute values from automatic stations (1st gen, MIRIAM/AFMS2, ESAU data until 31 Dec 1999, time reference is CET)",
    "10-Minutenwerte von automatischen Stationen der 1. Generation (MIRIAM/AFMS2, ESAU-Daten ab 01.01.2000 (Zeitbezug ist UTC)":
        "10-minute values from automatic stations (1st gen, MIRIAM/AFMS2, ESAU data from 1 Jan 2000, time reference is UTC)",
    "aus Messnetz2000":
        "from Messnetz2000",
}

# =============================================================================
# COLUMN NAME STANDARDIZATION
# =============================================================================

# Column name mapping for inconsistent metadata headers
# Maps various German column names to standardized English names
COLUMN_NAME_MAP = {
    # Station ID variations
    "Stations_ID": "station_id",
    "Station_ID": "station_id", 
    "stations_ID": "station_id",
    "STATIONS_ID": "station_id",
    "Stations_id": "station_id",
    "stations_id": "station_id",
    
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
