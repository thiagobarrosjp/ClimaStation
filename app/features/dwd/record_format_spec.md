# ClimaStation Record Format Specification

**Version:** 1.0  
**Last Updated:** 2025-06-27  
**Scope:** Applies to all raw datasets parsed from the DWD repository under `/observations_germany/climate/`

---

## 📌 Definition

The ClimaStation record format is a **timestamp-centric, station-based, and measurement-flexible schema** designed to represent climate observation data from the Deutscher Wetterdienst (DWD). It ensures that every row in the database corresponds to a unique measurement from a known station at a specific point in time.

---

## ✅ Required Fields

| Field         | Type       | Description |
|---------------|------------|-------------|
| `station_id`  | `int`      | Unique identifier for the weather station |
| `timestamp`   | `datetime` | ISO-formatted timestamp of observation (UTC) |

---

## 🧩 Optional Fields (Parameter Columns)

The following fields **may or may not** be present, depending on the dataset type:

| Field     | Type     | Description |
|-----------|----------|-------------|
| `TT_10`   | `float`  | Air temperature |
| `PP_10`   | `float`  | Atmospheric pressure |
| `RF_10`   | `float`  | Relative humidity |
| `TM5_10`  | `float`  | 5cm soil temperature |
| `TD_10`   | `float`  | Dew point |
| `quality_flag` | `int` or `None` | Source quality level (e.g., `QN_10`) |

Additional fields may be added in future iterations (e.g., wind speed, radiation, cloud cover) and should follow the same optional typing rules.

---

## 📏 Structural Requirements

- Every record **must contain**:
  - A valid timestamp
  - A valid station ID
- Records are **flat**: no nested sub-objects (except for internal metadata use)
- Fields not present in the raw data must be `null` in the output
- Field names should follow DWD conventions where possible

---

## 🔍 Validation Rules (Enforced Programmatically)

1. The input file must include a recognizable timestamp column (`MESS_DATUM`, etc.)
2. The station ID column must be mappable to an `int`
3. At least one known measurement field must be present
4. Rows must parse cleanly into the current Pydantic schema
5. Unknown or extra columns should be ignored, but optionally logged for review

---

## 📁 Related Code

- Schema definition: `schemas.py`
- Record validation logic: `record_validator.py` (planned)
- Sample analysis tool: `analyze_samples.py` (planned)
