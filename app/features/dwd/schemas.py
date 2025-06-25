from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class AirTemperatureRecord(BaseModel):
    station_id: int
    timestamp: datetime
    PP_10: Optional[float]
    TT_10: Optional[float]
    TM5_10: Optional[float]
    RF_10: Optional[float]
    TD_10: Optional[float]
    quality_flag: int
