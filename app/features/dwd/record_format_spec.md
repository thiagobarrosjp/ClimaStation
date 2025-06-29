# ClimaStation Record Format Specification

**Version:** 1.1  
**Last Updated:** 2025-06-29  
**Scope:** Applies to all raw datasets parsed from the DWD repository under  
`/observations_germany/climate/10_minutes/air_temperature/`

---

## 📌 Definition

The ClimaStation record format is a **timestamp-centric, station-based, and measurement-flexible schema** designed to represent climate observation data from the Deutscher Wetterdienst (DWD).  
Each record corresponds to a unique measurement from a known station at a specific point in time.

This specification ensures consistent representation of raw sensor data and prepares for future expansion across additional DWD datasets.

---

## ✅ Required Fields

| Field         | Type       | Description |
|---------------|------------|-------------|
| `station_id`  | `int`      | Unique identifier for the weather station. Extracted from filename or `STATIONS_ID` column. |
| `timestamp`   | `datetime` | ISO 8601 timestamp of the observation in UTC. Derived from `MESS_DATUM`. |

---

## 🧩 Optional Fields (Measurement Columns)

These fields **may or may not** appear in each dataset. If missing, they are set to `null`.

| Field      | Type     | Description |
|------------|----------|-------------|
| `TT_10`    | `float`  | Air temperature 2m above ground (°C) |
| `TM5_10`   | `float`  | Air temperature 5cm above ground (°C) |
| `TD_10`    | `float`  | Dew point temperature (°C) |
| `RF_10`    | `float`  | Relative humidity (%) |
| `PP_10`    | `float`  | Air pressure at station level (hPa) |
| `quality_flag` | `int` or `None` | Quality level for the record (typically from `QN_10`, `QN_9`, or similar) |

Additional fields (e.g., radiation, wind speed, cloud cover) may be added in future datasets. They will follow the same naming conventions and typing logic.

---

## 📏 Structural Requirements

- Records are **flat**: no nested sub-objects.
- All timestamps must be valid ISO 8601 strings in UTC.
- All records must include a valid `station_id` and `timestamp`.
- Fields not found in the raw file are stored as `null`.
- Column names match DWD naming conventions directly.

---

## 📦 Metadata Handling

Station metadata (such as latitude, longitude, elevation, and instrumentation) is stored **outside** the main record format and linked via `station_id`.

This information is sourced from:
- The `meta_data/` folder (per-station zip files containing geographic and sensor info)
- Supplemental DWD documentation PDFs (`DESCRIPTION_*.pdf`)

---

## 🧪 Quality Flags

DWD datasets include one or more `QN_*` (Qualitätsniveau) columns, such as:
- `QN_10` → quality level for `TT_10`, `RF_10`, etc.
- `QN_9`  → used in some datasets instead of QN_10

The pipeline will:
- Prefer `QN_10` if available.
- Fall back to another `QN_*` column if no standard flag exists.
- Optionally expose all QN columns for advanced quality control.

The simplified `quality_flag` field in the record represents the general quality level and may be derived from the most relevant available QN field.

---

## 🔍 Validation Rules (Enforced Programmatically)

1. The input file must include a recognizable timestamp column (`MESS_DATUM`)
2. The station ID must be extractable and castable to `int`
3. At least one known measurement field must be present
4. Parsed rows must conform to the defined Pydantic schema in `schemas.py`
5. Unknown or extra columns are ignored (but optionally logged for schema extension)

---

## 📁 Related Code

- Schema definition: `schemas.py`
- Download logic: `downloader.py`
- Parsing logic: `parser.py` (in progress)

---

## 📌 Notes

- Timestamps before the year 2000 may use MEZ; timestamps after 2000 are standardized in UTC.
- Missing values are encoded as `-999` in the raw files and must be converted to `null` during parsing.
- Record versioning is handled internally; all data parsed is considered version `1.0+` unless explicitly stated otherwise.
