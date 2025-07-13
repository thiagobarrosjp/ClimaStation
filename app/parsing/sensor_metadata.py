import io
from pathlib import Path
from typing import List, Dict
import pandas as pd

from config.ten_minutes_air_temperature_config import (
    PARAM_NAME_MAP,
    SENSOR_TYPE_TRANSLATIONS,
    MEASUREMENT_METHOD_TRANSLATIONS
)


def load_sensor_metadata(meta_files: List[Path], logger) -> pd.DataFrame:
    relevant_files = [
        f for f in meta_files
        if "Metadaten_Geraete" in f.name
    ]

    all_frames = []

    for path in relevant_files:
        try:
            with open(path, encoding='latin-1') as f:
                lines = []
                for line in f:
                    if line.startswith("generiert"):
                        break
                    lines.append(line)

            df = pd.read_csv(
                io.StringIO("".join(lines)),
                sep=';',
                skipinitialspace=True,
                dtype=str
            )

            # Normalize column names
            df.columns = [col.strip().lower() for col in df.columns]

            # Ensure all expected columns exist after normalization
            expected_columns = [
                'parameter', 'stations_id', 'von_datum', 'bis_datum',
                'geraetetyp name', 'messverfahren', 'geberhoehe ueber grund [m]'
            ]
            for col in expected_columns:
                if col not in df.columns:
                    df[col] = ""

            df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
            all_frames.append(df)
            logger.debug(f"Loaded metadata from {path.name} with {len(df)} rows")

        except Exception as e:
            logger.error(f"❌ Failed to parse sensor metadata file {path.name}: {e}")

    if all_frames:
        combined = pd.concat(all_frames, ignore_index=True)
        return combined
    else:
        # Return empty DataFrame with normalized lowercase columns
        return pd.DataFrame(columns=[
            'parameter', 'stations_id', 'von_datum', 'bis_datum',
            'geraetetyp name', 'messverfahren', 'geberhoehe ueber grund [m]'
        ])


def parse_sensor_metadata(meta_df: pd.DataFrame, station_id: int, date_int: int, logger) -> List[Dict]:
    sensors = []

    df = meta_df[meta_df['stations_id'].astype(str).str.strip() == str(station_id)]

    for param_raw in df['parameter'].unique():
        param_entry = PARAM_NAME_MAP.get(param_raw, {"en": param_raw, "de": param_raw})
        df_param = df[df['parameter'] == param_raw]

        for _, row in df_param.iterrows():
            try:
                von = int(row['von_datum'].strip())
                bis = int(row['bis_datum'].strip())
                if von <= date_int <= bis:
                    sensor_type_de = row.get("geraetetyp name", "").strip()
                    sensor_type_entry = SENSOR_TYPE_TRANSLATIONS.get(sensor_type_de, {"en": sensor_type_de, "de": sensor_type_de})

                    method_de = row.get("messverfahren", "").strip()
                    method_entry = MEASUREMENT_METHOD_TRANSLATIONS.get(method_de, {"en": method_de, "de": method_de})

                    sensors.append({
                        "measured_variable": {
                            "de": param_entry["de"],
                            "en": param_entry["en"]
                        },
                        "sensor_type": {
                            "de": sensor_type_entry["de"],
                            "en": sensor_type_entry["en"]
                        },
                        "measurement_method": {
                            "de": method_entry["de"],
                            "en": method_entry["en"]
                        },
                        "sensor_height_m": float(row.get("geberhoehe ueber grund [m]", "0").replace(",", "."))
                    })

            except Exception as e:
                logger.debug(f"[parse_sensor_metadata] Failed row in parameter {param_raw}: {e}")

    return sensors
