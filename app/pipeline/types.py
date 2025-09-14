from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, TypedDict, Literal

@dataclass(frozen=True, slots=True)
class SourceMeta:
    source_filename: str            # e.g. 10minutenwerte_TU_00003_19930428_19991231_hist.zip
    source_url: Optional[str]       # when known from urls.jsonl
    file_sha256: str                # checksum of the ZIP
    station_id: str                 # zero-padded, e.g. "00003"

class ParsedRow(TypedDict, total=False):
    # identity & time
    station_id: str
    timestamp_utc: Optional[str]    # ISO8601 "...Z" when resolvable
    timestamp_local: str            # raw "YYYYMMDDHHMM"
    time_ref: Literal["UTC", "MEZ", "unknown"]

    # measurements (v0; extend later)
    temperature_2m_c: Optional[float]
    temperature_0p05m_c: Optional[float]
    humidity_rel_pct: Optional[float]
    dewpoint_2m_c: Optional[float]
    pressure_station_hpa: Optional[float]

    # quality/window
    quality_level: Optional[int]
    parameter_window_found: bool

    # provenance
    source_filename: str
    source_url: Optional[str]
    source_row: int
    file_sha256: str
    ingested_at: str                # ISO UTC
