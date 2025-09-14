# schemas/dwd/record_air_temperature_10min.py
import json, hashlib
import pyarrow as pa

DATASET_KEY = "10_minutes_air_temperature"

def arrow_schema() -> pa.Schema:
    return pa.schema([
        ("station_id", pa.string()),
        ("timestamp_utc", pa.timestamp("s", tz="UTC")),
        ("timestamp_local", pa.string()),
        ("time_ref", pa.dictionary(pa.int8(), pa.string())),
        ("temperature_2m_c", pa.float32()),
        ("temperature_0p05m_c", pa.float32()),
        ("humidity_rel_pct", pa.float32()),
        ("dewpoint_2m_c", pa.float32()),
        ("pressure_station_hpa", pa.float32()),
        ("quality_level", pa.int8()),
        ("parameter_window_found", pa.bool_()),
        # provenance
        ("source_filename", pa.string()),
        ("source_url", pa.string()),
        ("source_row", pa.int32()),
        ("file_sha256", pa.string()),
        ("ingested_at", pa.timestamp("s", tz="UTC")),
    ])

def schema_fingerprint() -> str:
    s = arrow_schema().to_json()
    return "sha256:" + hashlib.sha256(s.encode("utf-8")).hexdigest()