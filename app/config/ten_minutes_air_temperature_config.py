# config/10_minutes_air_temperature_config.py

from pathlib import Path

# Base directories
RAW_BASE = Path("data/germany/2_downloaded_files/10_minutes/air_temperature")
PARSED_BASE = Path("data/germany/3_parsed_files/parsed_10_minutes/parsed_air_temperature")
STATION_INFO_FILE_NAME = "zehn_min_tu_Beschreibung_Stationen.txt"
STATION_INFO_FILE_HISTORICAL = RAW_BASE / "historical" / STATION_INFO_FILE_NAME
STATION_INFO_FILE_RECENT = RAW_BASE / "recent" / STATION_INFO_FILE_NAME
STATION_INFO_FILE_NOW = RAW_BASE / "now" / STATION_INFO_FILE_NAME


# Parameter name mapping (from filenames)
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

# Measurement method translations
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

# Sensor type translations
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

# Parameter column mapping
PARAM_MAP = {
    "TT_10": "air temperature 2 m above ground",
    "TM5_10": "air temperature 5 cm above ground",
    "PP_10": "air pressure at station altitude",
    "RF_10": "relative humidity",
    "TD_10": "dew point (calculated from air temp. and humidity)"
}

# Quality level map
QUALITY_LEVEL_MAP = {
    "1": "only formal control",
    "2": "controlled with individually defined criteria",
    "3": "automatic control and correction"
}

# Description translation map
DESCRIPTION_TRANSLATIONS = {
    "momentane Lufttemperatur in 2m Hoehe": "instantaneous air temperature at 2m height",
    "Momentane Temperatur in 5 cm Hoehe 10min": "instantaneous temperature at 5cm height",
    "Luftdruck in Stationshoehe der voran. 10 min": "air pressure at station altitude (preceding 10 min)",
    "relative Feucht in 2m Hoehe": "relative humidity at 2m height",
    "Taupunkttemperatur in 2m Hoehe": "dew point temperature at 2m height",
}

# Source translation map
SOURCE_TRANSLATIONS = {
    "10-Minutenwerte von automatischen Stationen der 1. Generation (MIRIAM/AFMS2, ESAU-Daten bis 31.12.1999 (Zeitbezug ist MEZ)":
        "10-minute values from automatic stations (1st gen, MIRIAM/AFMS2, ESAU data until 31 Dec 1999, time reference is CET)",
    "10-Minutenwerte von automatischen Stationen der 1. Generation (MIRIAM/AFMS2, ESAU-Daten ab 01.01.2000 (Zeitbezug ist UTC)":
        "10-minute values from automatic stations (1st gen, MIRIAM/AFMS2, ESAU data from 1 Jan 2000, time reference is UTC)",
    "aus Messnetz2000":
        "from Messnetz2000",
}

# Column name mapping for inconsistent metadata headers
COLUMN_NAME_MAP = {
    "Stations_ID": "station_id",
    "Station_ID": "station_id",
    "stations_ID": "station_id",
    "STATIONS_ID": "station_id",
    "Stations_id": "station_id",
    "Von_Datum": "from_date",
    "Bis_Datum": "to_date",
    "Stationsname": "station_name",
    "Geogr.Breite": "latitude",
    "Geogr.Laenge": "longitude",
    "Stationshoehe": "station_altitude_m",
    "Geberhoehe ueber Grund [m]": "sensor_height_m",
    "Geraetetyp Name": "sensor_type",
    "Messverfahren": "measurement_method",
    "Parameter": "parameter"
}

