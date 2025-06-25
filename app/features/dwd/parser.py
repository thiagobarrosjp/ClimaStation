import pandas as pd
from typing import List
from app.features.dwd.schemas import AirTemperatureRecord

def parse_temperature_file(filepath: str) -> List[AirTemperatureRecord]:
    raw_table = pd.read_csv(filepath, sep=";", skipinitialspace=True)
    raw_table["MESS_DATUM"] = pd.to_datetime(raw_table["MESS_DATUM"], format="%Y%m%d%H%M")
    raw_table = raw_table[raw_table["TT_10"] != -999]

    if "eor" in raw_table.columns:
        raw_table = raw_table.drop(columns=["eor"])

    # Rename and map fields into AirTemperatureRecord
    records = []
    for _, row in raw_table.iterrows():
        record = AirTemperatureRecord(
            station_id=row["STATIONS_ID"],
            timestamp=row["MESS_DATUM"],
            PP_10=None if row["PP_10"] == -999 else row["PP_10"],
            TT_10=None if row["TT_10"] == -999 else row["TT_10"],
            TM5_10=None if row["TM5_10"] == -999 else row["TM5_10"],
            RF_10=None if row["RF_10"] == -999 else row["RF_10"],
            TD_10=None if row["TD_10"] == -999 else row["TD_10"],
            quality_flag=row["QN"]
        )
        records.append(record)

    return records
