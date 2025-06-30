"""
Record Schema — auto-generated on 20250630_125226

Initial version based on air_temperature data from DWD 10-minute observations.
"""

from pydantic import BaseModel
from typing import Optional


class Location(BaseModel):
    latitude: float
    longitude: float
    elevation_m: Optional[float] = None


class SensorInfo(BaseModel):
    TT_10: Optional[str]
    RF_10: Optional[str]


class ClimateRecord(BaseModel):
    station_id: int
    station_name: Optional[str]
    timestamp: str  # ISO 8601 format
    TT_10: Optional[float]
    RF_10: Optional[float]
    quality_flag: Optional[int]
    location: Location
    sensor: Optional[SensorInfo]
