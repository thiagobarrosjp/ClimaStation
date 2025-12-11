# ClimaStation: DWD Climate Data Processing Platform

**Last Updated:** 2025-12-10  
**Version:** 4.11


---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Processing Workflow](#3-processing-workflow)
4. [Data Quality Standards](#4-data-quality-standards)
5. [Reference Data](#5-reference-data)
6. [Decisions Log](#6-decisions-log)
7. [Open Questions](#7-open-questions)
8. [Version History](#8-version-history)

---

## 1. Project Overview

### Purpose

ClimaStation makes climate and weather **station** data — starting with the DWD (Deutscher Wetterdienst) repository — clean, usable, and easy to download. It processes raw files, normalizes them, and provides structured outputs that are reliable and ready for analysis.

### Target Audience

- Researchers and environmental analysts working with time-series station data
- Policy makers, planners, and NGOs interested in local observations and long-term trends
- Software engineers and climate-tech teams integrating climate data into apps
- Educators and journalists who need trustworthy, explainable datasets

### Motivation

DWD's open data is extensive but **fragmented**: many folders, many ZIPs, multiple formats, and varying metadata quality. ClimaStation reduces this friction by:

- Mirroring raw files (independence from upstream availability)
- Normalizing timestamps and preserving provenance
- Outputting compact, typed, columnar files suitable for fast queries
- Enriching data with metadata automatically

### Scale

```
https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/
├── 10_minutes/
│   ├── air_temperature/
│   ├── precipitation/
│   ├── wind/
├── hourly/ ...
├── daily/ ...
└── monthly/, annual/, 1_minute/, 5_minutes/ ...
```

- Hundreds of thousands of ZIP files across products
- Hundreds of millions of rows for long time spans and many stations
- ~400 active climate stations

### Business Model (Preliminary)

**Free Tier:** Manual data downloads via web interface, query interface for exploration

**Paid Tier (Future):** API access, programmatic access, higher rate limits, bulk exports

*Details to be defined after validating with users.*

---

## 2. Architecture

### 2.1 Core Principle: Parquet as Central Pivot

Parquet files are the single source of truth for the entire platform.

```
                    ┌─────────────────────┐
                    │   DWD Raw Data      │
                    │  (ZIP files with    │
                    │   text files)       │
                    └──────────┬──────────┘
                               │
                               │ Parse & Process
                               │ (One-time, complex)
                               ▼
                    ┌─────────────────────┐
                    │  PARQUET STORAGE    │◄─── THE PIVOT
                    │  (Source of Truth)  │
                    │                     │
                    │  • Raw data         │
                    │  • Aggregates       │
                    │  • Metadata         │
                    └──────────┬──────────┘
                               │
          ┌────────────────────┴────────────────────┐
          │                                         │
          ▼                                         ▼
┌─────────────────────┐               ┌─────────────────────┐
│  AGGREGATES         │               │  RAW + METADATA     │
│  (Internal only)    │               │  (User downloads)   │
│                     │               │                     │
│  Powers web UI      │               │  Query & Convert    │
│  visualization      │               │  on-demand          │
└──────────┬──────────┘               └──────────┬──────────┘
           │                                      │
           ▼                                      ▼
┌─────────────────────┐          ┌───────────────────────────────┐
│     Web UI          │          │     User Downloads            │
│  • Charts           │          │  • CSV, JSON, Excel, Parquet  │
│  • Dashboards       │          │  • Raw data + metadata joined │
└─────────────────────┘          └───────────────────────────────┘
```

**Why Parquet:**
- Columnar format (fast filtering and aggregation)
- Compressed (efficient storage)
- Typed columns (data validation built-in)
- Industry standard (works with pandas, DuckDB, Spark, etc.)
- Can be queried directly (no database server needed)
- Single source of truth — all outputs generated from it

### 2.2 Data Flow

**Phase 1: Ingestion (One-time per DWD update)**
```
DWD Raw ZIP → Parse → JSONL (temp) → Parquet Storage
```
- Complex: Handle DWD formats, metadata intervals, quality checks
- Infrequent: Weekly/monthly updates
- Result: Clean, queryable Parquet files

**Phase 2a: Web Visualization (Internal)**
```
Web UI Request → Read Aggregates Parquet → Return JSON → Render Chart
```
- Aggregates are internal only (not downloadable by users)
- Powers interactive charts and dashboards
- Pre-computed statistics at multiple resolutions for performance

**Phase 2b: User Downloads (External)**
```
User Query → Read Raw Parquet → Join Metadata → Convert Format → Download
```
- Users only download raw data enriched with metadata
- Available formats: CSV, JSON, Excel, Parquet
- Query defines: stations, date range, parameters
- Users receive ready-to-use files (no joining required)

### 2.3 File Organization

```
reference/
├── station_bundesland.csv              # Master lookup: stations_id → station_name, Bundesland
└── translations.json                   # German → English translations for metadata values

DWD/
└── 10-minutes-air-temperature/
    ├── historical/                      # Fully quality-controlled data
    │   ├── station_00003/
    │   │   └── raw.parquet
    │   ├── station_00164/
    │   │   └── raw.parquet
    │   └── ... (one directory per station)
    │
    ├── recent/                          # Partially quality-controlled (~500 days)
    │   └── station_XXXXX/
    │       └── raw.parquet
    │
    ├── now/                             # Newest raw data (updated frequently)
    │   └── station_XXXXX/
    │       └── raw.parquet
    │
    ├── metadata/
    │   └── stations.parquet             # All station metadata, all intervals
    │
    └── aggregates/                      # Internal only (powers web UI)
        ├── hourly.parquet
        ├── daily.parquet
        ├── weekly.parquet
        ├── monthly.parquet
        ├── quarterly.parquet
        └── yearly.parquet
```

**Directory purposes:**
- **reference/** — Shared lookup tables used across all datasets
- **historical/** — Long-term records, fully quality-controlled by DWD
- **recent/** — Last ~500 days, partial quality control
- **now/** — Newest data, minimal quality control
- **metadata/** — Station information with validity intervals
- **aggregates/** — Pre-computed statistics for web visualization (internal only)

**Note:** Timestamps across historical/recent/now do not overlap. Each data point exists in exactly one directory based on its quality control status.

**Storage estimates:**
- Reference data: ~50 KB (station_bundesland.csv)
- Raw data: ~4 GB (400 stations × ~10 MB average)
- Aggregates: ~117 MB (hourly ~80MB, daily ~20MB, weekly ~3MB, monthly ~10MB, quarterly ~3MB, yearly ~1MB)
- Metadata: ~100 KB
- **Total: ~4.1 GB**

### 2.4 Data Schemas

All schemas are formally defined in YAML files that serve as contracts for Parquet file structure. See `schemas/` directory.

#### Raw Data (Stored Per Station)

**Schema file:** `schemas/dwd_10min_air_temperature_schema.yaml`

```python
# File: DWD/10-minutes-air-temperature/historical/station_00003/raw.parquet
# Total columns: 16
{
    # Keys
    "stations_id": 3,                              # int32
    "timestamp_mez": "1993-04-28 12:30:00",        # timestamp[s], always MEZ (UTC+1)
    "timestamp_utc": "1993-04-28 11:30:00",        # timestamp[s], always UTC
    
    # Quality
    "quality_level": 1,                            # int8 (1-10)
    
    # Measurements (null for missing, DWD uses -999)
    "pp_10": 987.3,                                # float32, pressure in hPa
    "tt_10": 24.9,                                 # float32, temp 2m in °C
    "tm5_10": 28.4,                                # float32, temp 5cm in °C
    "rf_10": 23.0,                                 # float32, humidity in %
    "td_10": 2.4,                                  # float32, dew point in °C
    
    # Data quality flags
    "metadata_matched": true,                      # bool, false if orphan row
    
    # Processing metadata
    "source_zip": "10minutenwerte_TU_00003_...",   # string
    "source_modified_at": "2025-01-15 08:00:00",  # timestamp[s]
    "ingested_at": "2025-01-20 14:30:00",         # timestamp[s]
    "schema_version": "1.0",                       # string
    "parser_version": "1.0.0",                     # string
    "row_hash": "a1b2c3d4..."                      # string
}
```

**Timestamp handling:** DWD uses MEZ (UTC+1) before 2000-01-01 and UTC after. We store both `timestamp_mez` and `timestamp_utc` for every row, converting as needed. MEZ is a fixed offset — no daylight saving correction required.

**Orphan data handling:** When raw data exists outside DWD metadata intervals (e.g., raw data starts April 28 but metadata starts April 29), `metadata_matched = false`. These rows are preserved but excluded from aggregates.

#### Metadata (All Stations, All Intervals)

**Schema file:** `schemas/dwd_10min_air_temperature_metadata_schema.yaml`

Metadata uses **temporal normalization**: each row represents the complete state of a station during a non-overlapping time interval. Intervals are generated from **both** DWD metadata files **and** raw data date ranges, ensuring all raw data is covered.

```python
# File: DWD/10-minutes-air-temperature/metadata/stations.parquet
# Total columns: 81
{
    # Keys
    "stations_id": 3,                              # int32
    "valid_from": "1993-04-28 00:00:00",          # timestamp[s] — may precede DWD metadata
    "valid_to": "1993-04-28 23:59:59",            # timestamp[s]
    
    # Station identity (null if orphan interval)
    "station_name": null,                          # string
    "operator": null,                              # string
    
    # Geography (null if orphan interval)
    "latitude": null,                              # float32
    "longitude": null,                             # float32
    "elevation_m": null,                           # float32
    
    # ... other fields null for orphan intervals
}
```

**Orphan intervals:** When raw data exists before the earliest DWD metadata date, a metadata row is created with all fields set to null. This ensures:
- All raw data is queryable (no hidden data)
- Users can see gaps in metadata coverage
- Joins always succeed (though they may return nulls)

#### Aggregates (Internal Only)

**Schema file:** `schemas/dwd_10min_air_temperature_aggregates_schema.yaml`

Pre-computed statistics for web UI visualization. UTC timestamps only. Partial periods included with count showing completeness.

```python
# File: DWD/10-minutes-air-temperature/aggregates/monthly.parquet
# Total columns: 23
{
    # Keys
    "stations_id": 3,                              # int32
    "period_start": "2024-01-01 00:00:00",        # timestamp[s], UTC
    "period_end": "2024-01-31 23:59:59",          # timestamp[s], UTC
    
    # Statistics per measurement (5 parameters × 4 stats = 20 columns)
    "pp_10_mean": 1013.2,                          # float32
    "pp_10_min": 995.1,                            # float32
    "pp_10_max": 1028.4,                           # float32
    "pp_10_count": 4464,                           # int32
    # ... same pattern for tt_10, tm5_10, rf_10, td_10
}
```

**Key characteristics:**
- UTC only (no MEZ) — simplified for visualization
- All quality levels included (no filtering)
- Orphan rows excluded (metadata_matched = true only)
- Partial periods valid — count shows completeness
- No processing metadata — not needed for preview purpose

#### User Downloads (What Users Actually Receive)

Users receive raw data with metadata pre-joined. Example row:

```python
{
    # Measurement data (from raw.parquet)
    "stations_id": 3,
    "timestamp_mez": "2005-06-15 15:30:00",
    "timestamp_utc": "2005-06-15 14:30:00",
    "quality_level": 1,
    "pp_10": 1013.2,
    "tt_10": 22.4,
    "tm5_10": 25.1,
    "rf_10": 65.0,
    "td_10": 15.2,
    "metadata_matched": true,
    
    # Metadata (joined automatically from stations.parquet)
    "station_name": "Aachen",
    "latitude": 50.7827,
    "longitude": 6.0941,
    "elevation_m": 202,
    "time_reference_tt_10": "UTC",
    "device_tt_10_en": "PT 100 (Air)",
    # ... other metadata fields as needed
}
```

**Format restrictions based on query size:**
- JSON: < 10 MB (blocked for larger)
- Excel: < 50 MB (blocked for larger)
- CSV: < 250 MB (warning for larger)
- Parquet: No limits (recommended for large queries)

---

## 3. Processing Workflow

### Overview

```
Phase 1: Process Each Station (one at a time)
  ├─ Parse metadata files → Append to stations.jsonl (keep intervals in memory)
  ├─ Parse raw data ZIPs → Check against intervals → Set metadata_matched → Write to station_XXXXX_raw.jsonl
  ├─ Track orphan date ranges (if raw data falls outside metadata intervals)
  ├─ Convert station_XXXXX_raw.jsonl → raw.parquet
  ├─ Compute aggregates (only metadata_matched=true) → Append to aggregates_*.jsonl
  └─ Delete station_XXXXX_raw.jsonl

Phase 2: Finalize (after all stations)
  ├─ Create null-filled intervals for orphan data → Append to stations.jsonl
  ├─ Convert stations.jsonl → stations.parquet
  ├─ Convert aggregates_*.jsonl → *.parquet
  └─ Delete all JSONL files
```

### Why JSONL as Intermediate Format

- **Append-friendly:** Can add data from multiple source files without editing
- **Human-readable:** Can inspect data during processing
- **Resumable:** If processing crashes, don't lose progress
- **Memory efficient:** Stream data without loading everything at once
- **Debuggable:** Easy to find and fix issues

Parquet files are immutable, so we can't efficiently append to them. JSONL solves this.

### Development Stages

1. **Parse Metadata to JSONL** — Read metadata files, temporal normalization, write to stations.jsonl
2. **Parse Raw Data to JSONL** — Open ZIP, read TXT, check against metadata intervals, set metadata_matched, track orphans
3. **JSONL to Parquet** — Convert with pandas, verify result
4. **Handle Multiple ZIPs per Station** — Append to same JSONL
5. **Compute Aggregates** — Calculate statistics (excluding orphan rows)
6. **Process Multiple Stations** — Loop through all stations
7. **Finalize** — Create orphan intervals, convert all JSONL to Parquet, cleanup

*For detailed code examples and step-by-step walkthrough, see `docs/processing-details.md`*

---

## 4. Data Quality Standards

### Core Principles

1. **Understandable transformations** — Every transformation must be explainable
2. **Explicit about missing data** — Convert `-999` to `null`, never use `0` for missing
3. **Preserve original values** — Keep enough information to trace back to source
4. **Fail visibly** — Stop on errors, don't silently skip bad data
5. **No hidden data** — All raw data accessible, even orphans without metadata

### Validation (MVP Level)

**File-level checks:**
- ZIP file can be opened
- Contains expected TXT file
- TXT has expected header row

**Record-level checks:**
- Expected number of columns
- Station ID looks valid (numeric)
- Timestamp looks valid (12 digits YYYYMMDDHHMM)

### Error Handling

**Fail-fast strategy:** Parser stops on first error with detailed message (file, line number, error type).

### Duplicate Handling

Same `(stations_id, timestamp_utc)` appearing twice with different values is treated as a parsing error. Stop immediately for manual investigation.

### Orphan Data Handling

Raw data rows outside DWD metadata intervals:
- **Preserved:** All raw data is kept (no data loss)
- **Flagged:** `metadata_matched = false` in raw data
- **Covered:** Metadata file includes null-filled interval with station_name and Bundesland from lookup table
- **Logged:** Warning during processing
- **Excluded from aggregates:** Only rows with metadata contribute to statistics

---

## 5. Reference Data

### DWD Data Structure

```
https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/
├── 10_minutes/
│   ├── air_temperature/
│   │   ├── historical/         # Long-term records
│   │   ├── recent/             # Last ~500 days
│   │   ├── now/                # Newest data
│   │   └── meta_data/          # Station lists and descriptions
│   ├── precipitation/
│   ├── extreme_temperature/
│   ├── extreme_wind/
│   ├── solar/
│   └── wind/
├── hourly/
├── daily/
└── monthly/
```

**Naming pattern:**
- Directory: `10_minutes` (temporal resolution)
- Subdirectory: `air_temperature` (parameter)
- ZIP filename: `10minutenwerte_TU_<station_id>_<start_date>_<end_date>_hist.zip`

### CSV Schema (10-minute Air Temperature)

**Dialect:** Delimiter `;`, line terminator `\r\n`, header `true`, quote char `"`

| DWD Column | Our Column | Description | Unit | Type | Missing |
|------------|------------|-------------|------|------|---------|
| STATIONS_ID | stations_id | Station identifier | - | int32 | - |
| MESS_DATUM | timestamp_mez, timestamp_utc | Reference date | - | timestamp[s] | - |
| QN | quality_level | Quality level | code | int8 | - |
| PP_10 | pp_10 | Air pressure at station altitude | hPa | float32 | -999 → null |
| TT_10 | tt_10 | Air temperature 2m above ground | °C | float32 | -999 → null |
| TM5_10 | tm5_10 | Air temperature 5cm above ground | °C | float32 | -999 → null |
| RF_10 | rf_10 | Relative humidity | % | float32 | -999 → null |
| TD_10 | td_10 | Dew point (calculated) | °C | float32 | -999 → null |

**Timestamp conversion:**
- Before 2000-01-01: MESS_DATUM is MEZ → `timestamp_mez = original`, `timestamp_utc = original - 1h`
- 2000-01-01 and after: MESS_DATUM is UTC → `timestamp_utc = original`, `timestamp_mez = original + 1h`

### Sample Raw Data

**File:** `produkt_zehn_min_tu_19930428_19991231_00003.txt`

```csv
STATIONS_ID;MESS_DATUM;QN;PP_10;TT_10;TM5_10;RF_10;TD_10
      3;199304281230;    1;  987.3;  24.9;  28.4;  23.0;   2.4
      3;199304281240;    1;  987.2;  24.9;  28.6;  21.0;   1.2
      3;199304281250;    1;  987.2;  25.5;  28.7;  20.0;   0.7
```

### Metadata Files (Per Station ZIP)

Each station ZIP contains multiple metadata files:

| File | Content |
|------|---------|
| `Metadaten_Geographie_XXXXX.txt` | Location history (lat, lon, elevation) |
| `Metadaten_Stationsname_Betreibername_XXXXX.txt` | Station name, operator |
| `Metadaten_Parameter_XXXXX.txt` | Parameter descriptions, time reference |
| `Metadaten_Geraete_Lufttemperatur_XXXXX.txt` | 2m temperature instrument |
| `Metadaten_Geraete_Momentane_Temperatur_In_5cm_XXXXX.txt` | 5cm temperature instrument |
| `Metadaten_Geraete_Rel_Feuchte_XXXXX.txt` | Humidity instrument |

**Known DWD issue:** Raw data may start before metadata intervals (e.g., raw data April 28, metadata starts April 29). This is handled by creating null-filled metadata intervals.

---

## 6. Decisions Log

### File Organization

| Decision | Rationale |
|----------|-----------|
| Per-station raw files + combined aggregates | Balances simplicity with efficiency |
| Separate directories for historical/recent/now | Mirrors DWD organization, reflects QC status |
| DWD/ as top-level directory | Extensible for future data sources |
| One metadata file for all stations | Metadata is small (~100 KB); simpler than per-station files |

### Data Formats

| Decision | Rationale |
|----------|-----------|
| JSONL as intermediate format | Append-friendly, human-readable, resumable, debuggable |
| Parquet as final storage | Columnar, compressed, typed, industry standard |
| On-demand format conversion for downloads | Avoids pre-generating millions of combinations |
| YAML schema files as contracts | Documents structure, enables validation, serves as specification |

### Column Naming

| Decision | Rationale |
|----------|-----------|
| Use DWD codes in lowercase (pp_10, tt_10, etc.) | Minimal transformation, traceable to DWD docs |
| Rename MESS_DATUM → timestamp_mez + timestamp_utc | Provide both timezones explicitly |
| Rename QN → quality_level | German abbreviation unclear in English |
| snake_case for all columns | Universal compatibility (Python, SQL, R, Spark) |

### Timestamps

| Decision | Rationale |
|----------|-----------|
| Store both timestamp_mez and timestamp_utc | Users can query in either timezone without manual conversion |
| MEZ = UTC+1 fixed (no DST) | DWD used MEZ, not German local time; simple conversion |
| Convert during ingestion | One-time cost, not on every query |
| Use timestamp_utc for joins with metadata | Consistent reference point |

### Data Handling

| Decision | Rationale |
|----------|-----------|
| Convert `-999` to `null` | Standard representation, clearer semantics, correct aggregation |
| Parse timestamps to proper datetime | More useful for queries and time-series operations |
| stations_id as int32 | Leading zeros have no meaning; numeric is more efficient |

### Metadata

| Decision | Rationale |
|----------|-----------|
| Keep metadata separate in storage | 75% storage savings (4 GB vs 16 GB) |
| Pre-join metadata for all downloads | User convenience; ready-to-use files |
| Temporal normalization (non-overlapping intervals) | Simple joins; complete snapshot per interval |
| Preserve German text + add English translations | Traceability + international accessibility |
| Add bundesland field | Useful for regional analysis |
| Extend intervals to cover orphan raw data | No hidden data; all raw data queryable |

### Orphan Data

| Decision | Rationale |
|----------|-----------|
| Preserve all raw data | No data loss, even for DWD inconsistencies |
| Add metadata_matched flag | Explicit visibility of orphan rows |
| Create null-filled metadata intervals | All raw data has corresponding metadata (even if null) |
| Fill station_name and Bundesland from lookup table | Ensures orphan data is queryable by Bundesland; stations_id is always available |
| Exclude orphans from aggregates | Statistics based only on validated data |
| Log warnings during processing | Awareness of data quality issues |

### Processing Metadata

| Decision | Rationale |
|----------|-----------|
| Add source_zip to every row | Trace data back to source file |
| Add source_modified_at | Know if source data has been updated |
| Add ingested_at | Track when data was processed |
| Add schema_version and parser_version | Debug issues, handle schema evolution |
| Add row_hash | Detect if DWD changes source data |

### Aggregates

| Decision | Rationale |
|----------|-----------|
| Internal only (not downloadable) | Aggregates serve visualization; users want raw data |
| Six resolutions (hourly to yearly) | Covers all common visualization zoom levels |
| Mean, min, max, count per measurement | Essential statistics for preview |
| No standard deviation | Not needed for preview; adds complexity |
| No processing metadata | Aggregates are derived data; provenance in raw data |
| UTC only (no MEZ) | Simplified for visualization; one consistent timezone |
| All quality levels included | No filtering; show all data |
| Exclude orphan rows | Only include data with validated metadata |
| Include partial periods | Count field shows completeness; no hidden data |
| 23 columns total | 3 keys + 20 statistics (5 params × 4 stats) |

### Error Handling

| Decision | Rationale |
|----------|-----------|
| Fail-fast on parsing errors | Identify and correct problems early |
| Treat duplicates as errors | Duplicates need investigation; don't silently choose one value |
| Atomic file writes (write to .tmp, then rename) | Ensures output files are complete; safe to resume by checking file existence |
| Resume by checking output file existence | Simple; no status file needed; atomic writes guarantee completeness |

### Reference Data

| Decision | Rationale |
|----------|-----------|
| Single master lookup table for station identity | Shared across all datasets; updated when new datasets added |
| Key by stations_id | Always available (in raw data and ZIP filenames); stable identifier |
| Include station_name in lookup | Enables filling station_name for orphan intervals |
| Include Bundesland in lookup | Per-station metadata files don't include Bundesland |
| Use current Bundesland borders | Researchers care about current geography, not historical borders |
| CSV format for lookup table | Human-readable, version-controllable, easy to debug |
| Source from historical station description file | Most comprehensive station list |

---

## 7. Open Questions

*No open questions at this time. All resolved questions have been incorporated into the Decisions Log.*

### 🔮 Future (Defer Until After MVP)

- **Automated download:** How to automate ZIP downloads from DWD?
- **Multiple datasets:** How to generalize parser for precipitation, wind, etc.?
- **Web API:** FastAPI? Flask? GraphQL?
- **User access:** DuckDB CLI? Web interface?
- **Business model:** What justifies paid tier?

---

## 8. Version History

| Version | Date | Summary |
|---------|------|---------|
| 4.11 | 2025-12-10 | Moved progress tracking to STATUS.md, removed section 8 |
| 4.10 | 2025-12-09 | Added DEVELOPMENT.md with coding standards, testing, git practices, Claude Code instructions |
| 4.9 | 2025-12-09 | Temporal normalization concrete example, edge cases, eor column handling, overlap error handling |
| 4.8 | 2025-12-09 | Added translations.json, metadata file transformation tables in processing-details.md |
| 4.7 | 2025-12-09 | Simplified workflow (2 phases), metadata first, renamed metadata.jsonl → stations.jsonl |
| 4.6 | 2025-12-09 | JSONL format specifications added to processing-details.md (type mapping, examples for all file types) |
| 4.5 | 2025-12-09 | Lookup table keyed by stations_id (includes station_name, Bundesland) |
| 4.4 | 2025-12-09 | Cleaned up resolved questions, added atomic writes and progress tracking to Decisions Log |
| 4.3 | 2025-12-09 | Bundesland lookup table, reference/ directory, Q6 resolved |
| 4.2 | 2025-12-09 | Aggregates schema complete (23 columns), UTC only, no stddev, no processing metadata |
| 4.1 | 2025-12-07 | Timestamp handling (MEZ+UTC), orphan data handling, metadata_matched flag, three-phase workflow |
| 4.0 | 2025-12-07 | Schema definitions complete: raw data, metadata, YAML contracts, processing metadata |
| 3.0 | 2025-11-26 | Major restructure: consolidated redundancies, fixed contradictions, separated workflow docs |
| 2.2 | 2025-11-26 | File organization, download clarification, resolved unit/error/duplicate questions |
| 2.1 | 2025-11-25 | Learning-first approach, JSONL workflow, core architecture |
| 2.0 | 2025-11-20 | Claude Code era, removed ChatGPT references |
| 1.0 | - | Initial architecture (ChatGPT era) |

---

## Related Documents

- `docs/processing-details.md` — Detailed code examples, step-by-step processing walkthrough
- `schemas/dwd_10min_air_temperature_schema.yaml` — Raw data Parquet schema (16 columns)
- `schemas/dwd_10min_air_temperature_metadata_schema.yaml` — Metadata Parquet schema (81 columns)
- `schemas/dwd_10min_air_temperature_aggregates_schema.yaml` — Aggregates Parquet schema (23 columns)
- `reference/station_bundesland.csv` — Master lookup table: stations_id → station_name, Bundesland
- `reference/translations.json` — German → English translations for metadata values
- `DEVELOPMENT.md` — Development standards for Claude Code and contributors
- `STATUS.md` — Current progress, next tasks, blockers

---

**END OF CONTEXT DOCUMENT**
