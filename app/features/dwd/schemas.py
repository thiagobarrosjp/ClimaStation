"""
schemas.py

Defines the internal record format used by the ClimaStation parser
for the 10-minute air temperature dataset from the DWD.

This schema is timestamp-centric, flat, and nullable, and is designed
to normalize raw CSV input from DWD into structured JSON-ready records.

Record parsing is handled by the AirTemperatureRecord.from_raw_row() class method.
"""

from pydantic import BaseModel
from datetime import datetime
from typing import Optional
import pandas as pd


class AirTemperatureRecord(BaseModel):
    station_id: int
    timestamp: datetime
    TT_10: Optional[float] = None  # Temperature at 2m above ground
    TM5_10: Optional[float] = None  # Temperature at 5cm above ground
    TD_10: Optional[float] = None  # Dew point temperature
    RF_10: Optional[float] = None  # Relative humidity
    PP_10: Optional[float] = None  # Air pressure at station altitude
    quality_flag: Optional[int] = None  # Derived from QN_10 or QN_9

    @classmethod
    def from_raw_row(cls, row: pd.Series) -> "AirTemperatureRecord":
        """
        Converts a single row from the raw DWD dataset (as a pandas Series)
        into a validated AirTemperatureRecord instance.
        """
        return cls(
            station_id=int(row["STATIONS_ID"]),
            timestamp=pd.to_datetime(row["MESS_DATUM"], format="%Y%m%d%H%M").to_pydatetime(),
            TT_10=row.get("TT_10"),
            TM5_10=row.get("TM5_10"),
            TD_10=row.get("TD_10"),
            RF_10=row.get("RF_10"),
            PP_10=row.get("PP_10"),
            quality_flag=row.get("QN_10") or row.get("QN_9"),
        )
