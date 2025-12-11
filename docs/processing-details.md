# ClimaStation: Processing Details

**Last Updated:** 2025-12-09  
**Version:** 1.8
**Related to:** `ClimaStation_Context.md`

This document contains detailed code examples and step-by-step processing walkthrough.

---

## Processing Workflow Overview

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

---

## Phase 1: Processing One Station

### Example: Station 00003

Station 00003 has raw data starting April 28, 1993, but DWD metadata starts April 29, 1993.

### Step 1.1: Parse Metadata → stations.jsonl

Each station ZIP contains 6 metadata files. All use `;` as delimiter and `YYYYMMDD` for dates.

**Reference files used:**
- `reference/station_bundesland.csv` — Station identity lookup (stations_id → station_name, Bundesland)
- `reference/translations.json` — German to English translations for metadata values

**Metadata files overview:**

| File | Purpose | Target fields |
|------|---------|---------------|
| Metadaten_Geographie_*.txt | Location history | latitude, longitude, elevation_m |
| Metadaten_Stationsname_Betreibername_*.txt | Station identity | station_name, operator |
| Metadaten_Parameter_*.txt | Parameter descriptions | description_*_de/en, unit_*, data_source_*_de/en, time_reference_*, notes_*_de/en, literature_*_de/en |
| Metadaten_Geraete_Lufttemperatur_*.txt | Air temp instrument | device_tt_10_de/en, sensor_height_tt_10, method_tt_10_de/en |
| Metadaten_Geraete_Momentane_Temperatur_In_5cm_*.txt | Ground temp instrument | device_tm5_10_de/en, sensor_height_tm5_10, method_tm5_10_de/en |
| Metadaten_Geraete_Rel_Feuchte_*.txt | Humidity instrument | device_rf_10_de/en, sensor_height_rf_10, method_rf_10_de/en |

---

#### Metadaten_Geographie_*.txt

**Format:**
```
Stations_id;Stationshoehe;Geogr.Breite;Geogr.Laenge;von_datum;bis_datum;Stationsname
     3;  202.00; 50.7827;  6.0941;19500401;20081007;Aachen
```

**Transformation:**

| DWD Column | JSONL Field | Transformation |
|------------|-------------|----------------|
| Stations_id | stations_id | Parse as int |
| von_datum | valid_from | Parse YYYYMMDD → timestamp (00:00:00) |
| bis_datum | valid_to | Parse YYYYMMDD → timestamp (23:59:59) |
| Stationshoehe | elevation_m | Parse as float |
| Geogr.Breite | latitude | Parse as float |
| Geogr.Laenge | longitude | Parse as float |
| Stationsname | station_name | Strip whitespace |
| *(lookup)* | bundesland | Look up from station_bundesland.csv |
| *(lookup)* | bundesland_en | Look up from station_bundesland.csv |

**Notes:** Multiple rows = station relocated. No footer line.

---

#### Metadaten_Stationsname_Betreibername_*.txt

**Format (two sections):**
```
Stations_ID;Stationsname;Von_Datum;Bis_Datum
     3;Aachen;18910101;20120406

Stations_ID;Betreibername;Von_Datum;Bis_Datum
     3;DWD;18910101;20120406
generiert: 12.02.2025 --  Deutscher Wetterdienst  --
```

**Transformation:**

| DWD Column | JSONL Field | Transformation |
|------------|-------------|----------------|
| Stationsname | station_name | Strip whitespace |
| Betreibername | operator | Strip whitespace |

**Notes:** Skip lines starting with "generiert:". Two separate sections with headers.

---

#### Metadaten_Parameter_*.txt

**Format:**
```
Stations_ID;Von_Datum;Bis_Datum;Stationsname;Parameter;Parameterbeschreibung;Einheit;Datenquelle (Strukturversion=SV);Zusatz-Info;Besonderheiten;Literaturhinweis;eor;
3;19930429;19991231;Aachen;PP_10;Luftdruck in Stationshoehe...;hpa;10-Minutenwerte...;HHMM MEZ;;;eor;
```

**Transformation (per parameter PP_10, TT_10, TM5_10, RF_10, TD_10):**

| DWD Column | JSONL Field | Transformation |
|------------|-------------|----------------|
| Parameter | *(determines target)* | Maps to pp_10, tt_10, tm5_10, rf_10, td_10 |
| Parameterbeschreibung | description_{param}_de | As-is |
| Parameterbeschreibung | description_{param}_en | Translate via translations.json["parameter_descriptions"] |
| Einheit | unit_{param} | Normalize via translations.json["units"] |
| Datenquelle | data_source_{param}_de | As-is |
| Datenquelle | data_source_{param}_en | Translate via translations.json["data_sources"] |
| Zusatz-Info | time_reference_{param} | Extract via translations.json["time_references"] |
| Besonderheiten | notes_{param}_de | As-is (often empty) |
| Besonderheiten | notes_{param}_en | Translate if not empty |
| Literaturhinweis | literature_{param}_de | As-is (often empty) |
| Literaturhinweis | literature_{param}_en | Translate if not empty |

**Notes:** Skip lines starting with "Legende:" or "generiert:". One row per parameter per time period.

---

#### Metadaten_Geraete_*.txt (3 files, same format)

**Format:**
```
Stations_ID;Stationsname;Geo. Laenge [Grad];Geo. Breite [Grad];Stationshoehe [m];Geberhoehe ueber Grund [m];Von_Datum;Bis_Datum;Geraetetyp Name;Messverfahren;eor;
3;Aachen;6.09;50.78;202;2;19930429;20120406;PT 100 (Luft);Temperaturmessung, elektr.;eor;
```

**File to parameter mapping:**

| File | Target param |
|------|--------------|
| Metadaten_Geraete_Lufttemperatur_*.txt | tt_10 |
| Metadaten_Geraete_Momentane_Temperatur_In_5cm_*.txt | tm5_10 |
| Metadaten_Geraete_Rel_Feuchte_*.txt | rf_10 |

**Transformation:**

| DWD Column | JSONL Field | Transformation |
|------------|-------------|----------------|
| Von_Datum | *(interval boundary)* | Parse YYYYMMDD |
| Bis_Datum | *(interval boundary)* | Parse YYYYMMDD |
| Geraetetyp Name | device_{param}_de | As-is |
| Geraetetyp Name | device_{param}_en | Translate via translations.json["devices"] |
| Geberhoehe ueber Grund [m] | sensor_height_{param} | Parse as float |
| Messverfahren | method_{param}_de | As-is |
| Messverfahren | method_{param}_en | Translate via translations.json["methods"] |

**Notes:** Skip lines starting with "generiert:". Multiple rows = device changed.

---

#### Translation Lookup

```python
import json

translations = json.load(open('reference/translations.json'))

def translate(value, category):
    """
    Translate German value to English using translations.json.
    Falls back to original value if not found.
    """
    if value is None or value == '':
        return None
    return translations.get(category, {}).get(value, value)

# Example usage
device_de = "PT 100 (Luft)"
device_en = translate(device_de, "devices")  # Returns "PT 100 (Air)"

method_de = "Temperaturmessung, elektr."
method_en = translate(method_de, "methods")  # Returns "Electrical temperature measurement"
```

---

#### Temporal Normalization

Each metadata file has different date ranges. To create unified metadata intervals:

1. Collect all unique date boundaries (both `von_datum` and `bis_datum`) from all 6 files
2. Sort boundaries chronologically
3. Create non-overlapping intervals between consecutive dates
4. For each interval, look up the applicable value from each source file
5. Combine into single metadata row with all fields

**Concrete example — Station 3:**

**Step 1: Collect all date boundaries from each file:**

```
Metadaten_Geographie_00003.txt:
  18910101-19000610, 19000611-19440831, 19440901-19450331, 
  19450401-19500331, 19500401-20081007, 20081008-20120406

Metadaten_Stationsname_Betreibername_00003.txt:
  18910101-20120406 (both station name and operator)

Metadaten_Geraete_Lufttemperatur_00003.txt:
  19930429-20120406

Metadaten_Geraete_Momentane_Temperatur_In_5cm_00003.txt:
  19930429-20120406

Metadaten_Geraete_Rel_Feuchte_00003.txt:
  19930701-20040105, 20040106-20120406

Metadaten_Parameter_00003.txt (varies by parameter):
  PP_10: 19930429-19991231, 20000101-20081007, 20081009-20110331
  TT_10: 19930429-19991231, 20000101-20081007, 20081002-20110331 (overlap error!)
  TM5_10: 19930429-19991231, 20000101-20081007, 20081009-20110331
  RF_10: 19930429-19991231, 20000101-20081007, 20081009-20110331
  TD_10: 19930429-19991231, 20000101-20050125, 20090709-20110331 (gap!)
```

**Step 2: Merge into unique, non-overlapping intervals:**

```
1.  18910101-19000610   (geography only)
2.  19000611-19440831   (geography change)
3.  19440901-19450331   (geography change)
4.  19450401-19500331   (geography change)
5.  19500401-19930428   (geography change, no instruments yet)
6.  19930429-19930630   (instruments start, but no humidity yet)
7.  19930701-19991231   (humidity instrument starts, MEZ era)
8.  20000101-20040105   (UTC era starts)
9.  20040106-20050125   (humidity device change)
10. 20050126-20081001   (TD_10 gap starts)
11. 20081002-20081007   (TT_10 overlap error zone)
12. 20081008-20090708   (geography change)
13. 20090709-20110331   (TD_10 resumes)
14. 20110401-20120406   (parameters end, only geography/devices)
```

**Step 3: For each interval, look up values from each source:**

| Interval | station_name | latitude | device_tt_10 | device_rf_10 | time_ref_tt_10 | description_td_10 |
|----------|--------------|----------|--------------|--------------|----------------|-------------------|
| 1891-1900 | Aachen | 50.7833 | null | null | null | null |
| ... | ... | ... | ... | ... | ... | ... |
| 1993-04-29 to 1993-06-30 | Aachen | 50.7827 | PT 100 (Luft) | null | MEZ | Taupunkttemperatur... |
| 1993-07-01 to 1999-12-31 | Aachen | 50.7827 | PT 100 (Luft) | HYGROMER MP100 | MEZ | Taupunkttemperatur... |
| 2000-01-01 to 2004-01-05 | Aachen | 50.7827 | PT 100 (Luft) | HYGROMER MP100 | UTC | Taupunkttemperatur... |
| 2005-01-26 to 2008-10-01 | Aachen | 50.7827 | PT 100 (Luft) | Feuchtesonde HMP45D | UTC | **null** (TD_10 gap) |
| 2009-07-09 to 2011-03-31 | Aachen | 50.7827 | PT 100 (Luft) | Feuchtesonde HMP45D | UTC | Taupunkttemperatur... |

---

#### Edge Cases

| Edge case | Example | Handling |
|-----------|---------|----------|
| No data from source for interval | 1891-1993 has no instrument data | → null values for those fields |
| Source starts after interval begins | Humidity starts 1993-07-01, interval starts 1993-04-29 | → null until source date |
| Source ends before interval ends | TD_10 ends 2005-01-25, next starts 2009-07-09 | → null in the gap (this reflects real measurement gap) |
| Overlapping intervals in same source | TT_10: one ends 2008-10-07, next starts 2008-10-02 | → **fail fast**, log error, investigate |

**Note on TD_10 gap:** The gap from 2005-01-26 to 2009-07-08 is real — raw data confirms -999 values during this period. The metadata correctly documents when measurements were not available.

---

#### Implementation

```python
# Parse metadata files from ZIP and append to shared stations.jsonl
metadata_intervals = parse_metadata_files(zip_file)  # Returns list of intervals

# Keep intervals in memory for checking raw data
station_intervals[station_id] = metadata_intervals

# Append to shared JSONL file
append_to_jsonl(metadata_intervals, 'work/temp/stations.jsonl')
```

#### Error Handling for Metadata

```python
def check_for_overlaps(intervals, parameter_name):
    """
    Check that intervals for a single parameter don't overlap.
    Fail fast if overlap detected.
    """
    sorted_intervals = sorted(intervals, key=lambda x: x['von_datum'])
    
    for i in range(len(sorted_intervals) - 1):
        current_end = sorted_intervals[i]['bis_datum']
        next_start = sorted_intervals[i + 1]['von_datum']
        
        if next_start <= current_end:
            raise ValueError(
                f"Overlapping intervals for {parameter_name}: "
                f"{sorted_intervals[i]} overlaps with {sorted_intervals[i + 1]}"
            )
```

**Known DWD data quality issues:**
- Station 3, TT_10: Overlap between 2008-10-02 and 2008-10-07 (interval ends Oct 7, next starts Oct 2)
- When encountered, parser stops and logs error for manual investigation

### Step 1.2: Parse Raw Data ZIPs → station_XXXXX_raw.jsonl

**DWD raw data TXT format:**

Two format variations exist. Some files include an `eor` (end of record) column:

```
# Format A (earlier files, no eor):
STATIONS_ID;MESS_DATUM;QN;PP_10;TT_10;TM5_10;RF_10;TD_10
      3;199304281230;    1;  987.3;  24.9;  28.4;  23.0;   2.4

# Format B (later files, with eor):
STATIONS_ID;MESS_DATUM;QN;PP_10;TT_10;TM5_10;RF_10;TD_10;eor
      3;199912312300;    1;  997.3;   4.1;   3.6;  87.0;   2.1;eor
```

**Transformation table:**

| DWD Column | JSONL Field | Transformation |
|------------|-------------|----------------|
| STATIONS_ID | stations_id | Parse as int |
| MESS_DATUM | timestamp_mez | Parse YYYYMMDDhhmm, apply era logic |
| MESS_DATUM | timestamp_utc | Convert from MEZ (subtract 1 hour) or keep as-is |
| QN | quality_level | Parse as int |
| PP_10 | pp_10 | Parse as float, -999 → null |
| TT_10 | tt_10 | Parse as float, -999 → null |
| TM5_10 | tm5_10 | Parse as float, -999 → null |
| RF_10 | rf_10 | Parse as float, -999 → null |
| TD_10 | td_10 | Parse as float, -999 → null |
| eor | *(ignored)* | End-of-record marker, skip if present |
| *(computed)* | metadata_matched | Check timestamp against metadata intervals |
| *(added)* | source_zip | ZIP filename |
| *(added)* | source_modified_at | ZIP file modification timestamp |
| *(added)* | ingested_at | Current processing timestamp |
| *(added)* | schema_version | "1.0" |
| *(added)* | parser_version | "1.0.0" |
| *(added)* | row_hash | Hash of row for deduplication |

**Implementation:**

```python
jsonl_path = 'work/temp/station_00003_raw.jsonl'

# Track orphan date ranges for this station
orphan_min_date = None
orphan_max_date = None

for zip_file in find_zips_for_station(3):
    for raw_row in parse_zip_file(zip_file):
        # Convert timestamp to both MEZ and UTC
        raw_row['timestamp_mez'], raw_row['timestamp_utc'] = convert_timestamp(
            raw_row['mess_datum'],
            era='mez' if raw_row['mess_datum'] < 200001010000 else 'utc'
        )
        
        # Check if metadata exists for this timestamp
        raw_row['metadata_matched'] = is_covered_by_metadata(
            raw_row['timestamp_utc'],
            station_intervals[3]  # Intervals loaded in Step 1.1
        )
        
        # Track orphan date ranges
        if not raw_row['metadata_matched']:
            if orphan_min_date is None or raw_row['timestamp_utc'] < orphan_min_date:
                orphan_min_date = raw_row['timestamp_utc']
            if orphan_max_date is None or raw_row['timestamp_utc'] > orphan_max_date:
                orphan_max_date = raw_row['timestamp_utc']
            log_warning(f"Orphan row: station 3, timestamp {raw_row['timestamp_utc']}")
        
        append_to_jsonl(raw_row, jsonl_path)

# Store orphan ranges for Phase 2 (creating null-filled intervals)
if orphan_min_date:
    orphan_ranges[3] = (orphan_min_date, orphan_max_date)
```

**Timestamp conversion:**
```python
def convert_timestamp(mess_datum, era):
    """
    Convert DWD MESS_DATUM to both MEZ and UTC.
    MEZ = UTC+1 (fixed, no DST)
    """
    # Parse: 199304281230 → datetime(1993, 4, 28, 12, 30)
    dt = parse_mess_datum(mess_datum)
    
    if era == 'mez':
        timestamp_mez = dt
        timestamp_utc = dt - timedelta(hours=1)
    else:  # era == 'utc'
        timestamp_utc = dt
        timestamp_mez = dt + timedelta(hours=1)
    
    return timestamp_mez, timestamp_utc
```

### Step 1.3: Convert JSONL → Raw Parquet

```python
import pandas as pd

df = pd.read_json(jsonl_path, lines=True)
df.to_parquet('DWD/10-minutes-air-temperature/historical/station_00003/raw.parquet', 
              compression='snappy')
```

### Step 1.4: Compute Aggregates (Excluding Orphans)

**Schema file:** `schemas/dwd_10min_air_temperature_aggregates_schema.yaml`

Aggregates use UTC timestamps only. Partial periods are included — count shows completeness.

```python
# Only include rows with valid metadata
df_valid = df[df['metadata_matched'] == True]

def compute_hourly_aggregates(df):
    """
    Group by station and hour (truncated), compute statistics.
    Partial hours are valid — count shows how many readings contributed.
    """
    df['hour'] = df['timestamp_utc'].dt.floor('h')
    
    result = df.groupby(['stations_id', 'hour']).agg({
        'pp_10': ['mean', 'min', 'max', 'count'],
        'tt_10': ['mean', 'min', 'max', 'count'],
        'tm5_10': ['mean', 'min', 'max', 'count'],
        'rf_10': ['mean', 'min', 'max', 'count'],
        'td_10': ['mean', 'min', 'max', 'count'],
    }).reset_index()
    
    # Flatten column names
    result.columns = ['stations_id', 'period_start', 
                      'pp_10_mean', 'pp_10_min', 'pp_10_max', 'pp_10_count',
                      'tt_10_mean', 'tt_10_min', 'tt_10_max', 'tt_10_count',
                      # ... etc
                      ]
    
    # Add period_end (hour boundary - 1 second)
    result['period_end'] = result['period_start'] + pd.Timedelta(minutes=59, seconds=59)
    
    return result

hourly = compute_hourly_aggregates(df_valid)
daily = compute_daily_aggregates(df_valid)
weekly = compute_weekly_aggregates(df_valid)
monthly = compute_monthly_aggregates(df_valid)
quarterly = compute_quarterly_aggregates(df_valid)
yearly = compute_yearly_aggregates(df_valid)

# Append to shared JSONL files
append_to_jsonl(hourly, 'work/temp/aggregates_hourly.jsonl')
append_to_jsonl(daily, 'work/temp/aggregates_daily.jsonl')
# ... etc
```

**Concrete example — Station 3, first hours (UTC):**

Raw data (after MEZ→UTC conversion):
```
timestamp_utc        | tt_10
---------------------|------
1993-04-28 11:30:00  | 24.9
1993-04-28 11:40:00  | 24.9
1993-04-28 11:50:00  | 25.5
1993-04-28 12:00:00  | 25.8
1993-04-28 12:10:00  | 25.8
1993-04-28 12:20:00  | 25.7
1993-04-28 12:30:00  | 26.0
1993-04-28 12:40:00  | 26.1
1993-04-28 12:50:00  | 27.0
```

Hourly aggregates:
```
stations_id | period_start        | period_end          | tt_10_mean | tt_10_count
------------|---------------------|---------------------|------------|------------
3           | 1993-04-28 11:00:00 | 1993-04-28 11:59:59 | 25.1       | 3  (partial)
3           | 1993-04-28 12:00:00 | 1993-04-28 12:59:59 | 26.1       | 6  (full)
```

### Step 1.5: Cleanup

```python
os.remove('work/temp/station_00003_raw.jsonl')
```

**Repeat for all ~400 stations.**

---

## Phase 2: Finalize

### Step 2.1: Create Null-Filled Intervals for Orphan Data

```python
# Load lookup table for station identity
lookup_table = pd.read_csv('reference/station_bundesland.csv')

def create_null_interval(station_id, valid_from, valid_to, lookup_table):
    """
    Create a metadata row with null fields except keys and station identity.
    Station name and Bundesland come from lookup table.
    """
    station_info = lookup_table[lookup_table['stations_id'] == station_id].iloc[0]
    
    return {
        'stations_id': station_id,
        'valid_from': valid_from,
        'valid_to': valid_to,
        'station_name': station_info['station_name'],
        'operator': None,
        'latitude': None,
        'longitude': None,
        'elevation_m': None,
        'bundesland': station_info['bundesland'],
        'bundesland_en': station_info['bundesland_en'],
        # ... all other fields None
        'source_zip': 'ORPHAN_INTERVAL',
        'source_modified_at': None,
        'ingested_at': datetime.utcnow(),
        'schema_version': '1.0',
        'parser_version': '1.0.0',
        'row_hash': None,
    }

# Create null-filled intervals for all orphan ranges tracked in Phase 1
for station_id, (orphan_min, orphan_max) in orphan_ranges.items():
    orphan_interval = create_null_interval(station_id, orphan_min, orphan_max, lookup_table)
    append_to_jsonl(orphan_interval, 'work/temp/stations.jsonl')
    log_warning(f"Station {station_id}: created orphan interval {orphan_min} to {orphan_max}")
```

**Example result for station 3:**

| stations_id | valid_from | valid_to | station_name | bundesland | time_reference_tt_10 |
|-------------|------------|----------|--------------|------------|---------------------|
| 3 | 1993-04-28 00:00:00 | 1993-04-28 23:59:59 | Aachen | Nordrhein-Westfalen | null |
| 3 | 1993-04-29 00:00:00 | 1999-12-31 23:59:59 | Aachen | Nordrhein-Westfalen | MEZ |
| 3 | 2000-01-01 00:00:00 | 2008-10-07 23:59:59 | Aachen | Nordrhein-Westfalen | UTC |

### Step 2.2: Write Final Metadata Parquet

```python
df_stations = pd.read_json('work/temp/stations.jsonl', lines=True)

# Convert timestamps
for col in ['valid_from', 'valid_to', 'source_modified_at', 'ingested_at']:
    df_stations[col] = pd.to_datetime(df_stations[col])

# Sort for query efficiency
df_stations = df_stations.sort_values(['stations_id', 'valid_from'])

# Write final Parquet
df_stations.to_parquet('DWD/10-minutes-air-temperature/metadata/stations.parquet',
                       compression='snappy')

# Cleanup
os.remove('work/temp/stations.jsonl')
```

### Step 2.3: Finalize Aggregates

```python
resolutions = ['hourly', 'daily', 'weekly', 'monthly', 'quarterly', 'yearly']

for resolution in resolutions:
    df = pd.read_json(f'work/temp/aggregates_{resolution}.jsonl', lines=True)
    df = df.sort_values(['stations_id', 'period_start'])
    df.to_parquet(f'DWD/10-minutes-air-temperature/aggregates/{resolution}.parquet', 
                  compression='snappy')
    os.remove(f'work/temp/aggregates_{resolution}.jsonl')
```

---

## File System During Processing

### While Processing Station 00003 (Phase 1)

```
# In memory:
station_intervals = {
    3: [(datetime(1993, 4, 29), datetime(1999, 12, 31)), ...]  # From DWD metadata
}
orphan_ranges = {}  # Will be populated if raw data falls outside intervals

work/
├── temp/
│   ├── station_00003_raw.jsonl        # Current station's raw data
│   ├── stations.jsonl                 # Growing (all stations' metadata)
│   ├── aggregates_hourly.jsonl        # Growing
│   └── ...
│
DWD/
└── 10-minutes-air-temperature/
    └── historical/
        └── station_00003/
            └── raw.parquet            # Just created
```

### After Phase 1 (All Stations Processed)

```
# In memory:
orphan_ranges = {
    3: (datetime(1993, 4, 28, 11, 30), datetime(1993, 4, 28, 23, 50)),  # One day of orphan data
    # ... any other stations with orphan data
}

work/
├── temp/
│   ├── stations.jsonl                 # Complete (all stations' metadata)
│   ├── aggregates_hourly.jsonl        # Complete
│   ├── aggregates_daily.jsonl         # Complete
│   └── ...
│
DWD/
└── 10-minutes-air-temperature/
    └── historical/
        ├── station_00003/
        │   └── raw.parquet
        ├── station_00044/
        │   └── raw.parquet
        └── ... (all stations)
```

### After Phase 2 (Complete)

```
DWD/
└── 10-minutes-air-temperature/
    ├── historical/
    │   ├── station_00003/
    │   │   └── raw.parquet            # ✅ Final (includes metadata_matched flag)
    │   └── ... (~400 stations)
    ├── metadata/
    │   └── stations.parquet           # ✅ Final (includes null intervals for orphans)
    └── aggregates/
        ├── hourly.parquet             # ✅ Final (orphans excluded)
        ├── daily.parquet
        ├── weekly.parquet
        ├── monthly.parquet
        ├── quarterly.parquet
        └── yearly.parquet

# work/temp/ directory deleted
```

---

## Query Pattern for Downloads

```python
def generate_download(station_ids, start_date, end_date, format='parquet'):
    import duckdb
    
    result = duckdb.sql("""
        SELECT 
            r.stations_id,
            r.timestamp_mez,
            r.timestamp_utc,
            r.quality_level,
            r.pp_10,
            r.tt_10,
            r.tm5_10,
            r.rf_10,
            r.td_10,
            r.metadata_matched,
            m.station_name,
            m.latitude,
            m.longitude,
            m.elevation_m,
            m.time_reference_tt_10,
            m.device_tt_10_en
        FROM 'DWD/10-minutes-air-temperature/historical/station_*/raw.parquet' r
        JOIN 'DWD/10-minutes-air-temperature/metadata/stations.parquet' m
            ON r.stations_id = m.stations_id
            AND r.timestamp_utc BETWEEN m.valid_from AND m.valid_to
        WHERE r.stations_id IN ({station_ids})
          AND r.timestamp_utc BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY r.stations_id, r.timestamp_utc
    """)
    
    return convert_to_format(result, format)
```

**Notes:**
- Join always succeeds (metadata intervals cover all raw data, including orphans)
- Orphan rows will have null values for metadata columns
- Users can filter with `WHERE metadata_matched = true` if they want only validated data

---

## Why This Workflow Works

### 1. No Hidden Data

All raw data is accessible, even when DWD metadata is incomplete. Users can see and query everything.

### 2. Explicit Data Quality

The `metadata_matched` flag makes it clear which rows have validated metadata. No silent assumptions.

### 3. Clean Aggregates

Statistics only include validated data. Orphan rows don't pollute averages and counts.

### 4. Simple Joins

Every raw data row has a corresponding metadata interval (even if null-filled). No failed joins.

### 5. Traceable

Users can see exactly where data quality issues exist by looking for null metadata or `metadata_matched = false`.

---

## JSONL Format Specifications

JSONL files are temporary intermediate files used during processing. Each line is a valid JSON object. The structure follows the corresponding YAML schema, with types mapped as shown below.

### Type Mapping (YAML → JSON)

| YAML type | JSON representation | Example |
|-----------|---------------------|---------|
| int32, int8 | number | `3`, `1` |
| float32 | number or null | `50.7827`, `null` |
| string | string or null | `"Aachen"`, `null` |
| bool | boolean | `true`, `false` |
| timestamp[s] | ISO 8601 string | `"1993-04-29T00:00:00"` |

### stations.jsonl

One line per metadata interval. Fields match `dwd_10min_air_temperature_metadata_schema.yaml` (81 columns).

**Normal row example (formatted for readability):**

```json
{
  "stations_id": 3,
  "valid_from": "1993-04-29T00:00:00",
  "valid_to": "1999-12-31T23:59:59",

  "station_name": "Aachen",
  "operator": "DWD",

  "latitude": 50.7827,
  "longitude": 6.0941,
  "elevation_m": 202.0,

  "bundesland": "Nordrhein-Westfalen",
  "bundesland_en": "North Rhine-Westphalia",

  "device_tt_10_de": "PT 100 (Luft)",
  "device_tt_10_en": "PT 100 (Air)",
  "sensor_height_tt_10": 2.0,
  "method_tt_10_de": "Elektrisches Widerstandsthermometer",
  "method_tt_10_en": "Electrical resistance thermometer",

  "device_tm5_10_de": "PT 100 (Erdboden)",
  "device_tm5_10_en": "PT 100 (Ground)",
  "sensor_height_tm5_10": 0.05,
  "method_tm5_10_de": "Elektrisches Widerstandsthermometer",
  "method_tm5_10_en": "Electrical resistance thermometer",

  "device_rf_10_de": "Haar-Hygrometer",
  "device_rf_10_en": "Hair hygrometer",
  "sensor_height_rf_10": 2.0,
  "method_rf_10_de": "Absorptionshygrometer",
  "method_rf_10_en": "Absorption hygrometer",

  "description_pp_10_de": "Luftdruck auf Stationshöhe",
  "description_pp_10_en": "Air pressure at station altitude",
  "unit_pp_10": "hPa",
  "data_source_pp_10_de": "Messung",
  "data_source_pp_10_en": "Measurement",
  "time_reference_pp_10": "MEZ",
  "notes_pp_10_de": null,
  "notes_pp_10_en": null,
  "literature_pp_10_de": null,
  "literature_pp_10_en": null,

  "description_tt_10_de": "Lufttemperatur in 2m Höhe",
  "description_tt_10_en": "Air temperature at 2m height",
  "unit_tt_10": "°C",
  "data_source_tt_10_de": "Messung",
  "data_source_tt_10_en": "Measurement",
  "time_reference_tt_10": "MEZ",
  "notes_tt_10_de": null,
  "notes_tt_10_en": null,
  "literature_tt_10_de": null,
  "literature_tt_10_en": null,

  "description_tm5_10_de": "Erdbodentemperatur in 5cm Höhe",
  "description_tm5_10_en": "Ground temperature at 5cm height",
  "unit_tm5_10": "°C",
  "data_source_tm5_10_de": "Messung",
  "data_source_tm5_10_en": "Measurement",
  "time_reference_tm5_10": "MEZ",
  "notes_tm5_10_de": null,
  "notes_tm5_10_en": null,
  "literature_tm5_10_de": null,
  "literature_tm5_10_en": null,

  "description_rf_10_de": "Relative Feuchte",
  "description_rf_10_en": "Relative humidity",
  "unit_rf_10": "%",
  "data_source_rf_10_de": "Messung",
  "data_source_rf_10_en": "Measurement",
  "time_reference_rf_10": "MEZ",
  "notes_rf_10_de": null,
  "notes_rf_10_en": null,
  "literature_rf_10_de": null,
  "literature_rf_10_en": null,

  "description_td_10_de": "Taupunkttemperatur",
  "description_td_10_en": "Dew point temperature",
  "unit_td_10": "°C",
  "data_source_td_10_de": "Berechnung",
  "data_source_td_10_en": "Calculation",
  "time_reference_td_10": "MEZ",
  "notes_td_10_de": null,
  "notes_td_10_en": null,
  "literature_td_10_de": null,
  "literature_td_10_en": null,

  "source_zip": "10minutenwerte_TU_00003_19930428_19991231_hist.zip",
  "source_modified_at": "2025-01-15T08:00:00",
  "ingested_at": "2025-01-20T14:30:00",
  "schema_version": "1.0",
  "parser_version": "1.0.0",
  "row_hash": "abc123def456"
}
```

**Orphan row example (raw data exists but no DWD metadata):**

For orphan intervals, `station_name`, `bundesland`, and `bundesland_en` are filled from the lookup table (`reference/station_bundesland.csv`). All other fields are null.

```json
{
  "stations_id": 3,
  "valid_from": "1993-04-28T00:00:00",
  "valid_to": "1993-04-28T23:59:59",

  "station_name": "Aachen",
  "operator": null,

  "latitude": null,
  "longitude": null,
  "elevation_m": null,

  "bundesland": "Nordrhein-Westfalen",
  "bundesland_en": "North Rhine-Westphalia",

  "device_tt_10_de": null,
  "device_tt_10_en": null,
  "sensor_height_tt_10": null,
  "method_tt_10_de": null,
  "method_tt_10_en": null,

  "device_tm5_10_de": null,
  "device_tm5_10_en": null,
  "sensor_height_tm5_10": null,
  "method_tm5_10_de": null,
  "method_tm5_10_en": null,

  "device_rf_10_de": null,
  "device_rf_10_en": null,
  "sensor_height_rf_10": null,
  "method_rf_10_de": null,
  "method_rf_10_en": null,

  "description_pp_10_de": null,
  "description_pp_10_en": null,
  "unit_pp_10": null,
  "data_source_pp_10_de": null,
  "data_source_pp_10_en": null,
  "time_reference_pp_10": null,
  "notes_pp_10_de": null,
  "notes_pp_10_en": null,
  "literature_pp_10_de": null,
  "literature_pp_10_en": null,

  "description_tt_10_de": null,
  "description_tt_10_en": null,
  "unit_tt_10": null,
  "data_source_tt_10_de": null,
  "data_source_tt_10_en": null,
  "time_reference_tt_10": null,
  "notes_tt_10_de": null,
  "notes_tt_10_en": null,
  "literature_tt_10_de": null,
  "literature_tt_10_en": null,

  "description_tm5_10_de": null,
  "description_tm5_10_en": null,
  "unit_tm5_10": null,
  "data_source_tm5_10_de": null,
  "data_source_tm5_10_en": null,
  "time_reference_tm5_10": null,
  "notes_tm5_10_de": null,
  "notes_tm5_10_en": null,
  "literature_tm5_10_de": null,
  "literature_tm5_10_en": null,

  "description_rf_10_de": null,
  "description_rf_10_en": null,
  "unit_rf_10": null,
  "data_source_rf_10_de": null,
  "data_source_rf_10_en": null,
  "time_reference_rf_10": null,
  "notes_rf_10_de": null,
  "notes_rf_10_en": null,
  "literature_rf_10_de": null,
  "literature_rf_10_en": null,

  "description_td_10_de": null,
  "description_td_10_en": null,
  "unit_td_10": null,
  "data_source_td_10_de": null,
  "data_source_td_10_en": null,
  "time_reference_td_10": null,
  "notes_td_10_de": null,
  "notes_td_10_en": null,
  "literature_td_10_de": null,
  "literature_td_10_en": null,

  "source_zip": "ORPHAN_INTERVAL",
  "source_modified_at": null,
  "ingested_at": "2025-01-20T14:30:00",
  "schema_version": "1.0",
  "parser_version": "1.0.0",
  "row_hash": null
}
```

### station_XXXXX_raw.jsonl

One line per measurement. Fields match `dwd_10min_air_temperature_schema.yaml` (16 columns).

**Example (formatted for readability):**

```json
{
  "stations_id": 3,
  "timestamp_mez": "1993-04-28T12:30:00",
  "timestamp_utc": "1993-04-28T11:30:00",

  "quality_level": 1,

  "pp_10": 987.3,
  "tt_10": 24.9,
  "tm5_10": 28.4,
  "rf_10": 23.0,
  "td_10": 2.4,

  "metadata_matched": true,

  "source_zip": "10minutenwerte_TU_00003_19930428_19991231_hist.zip",
  "source_modified_at": "2025-01-15T08:00:00",
  "ingested_at": "2025-01-20T14:30:00",
  "schema_version": "1.0",
  "parser_version": "1.0.0",
  "row_hash": "a1b2c3d4"
}
```

**With null values (DWD's -999 converted to null):**

```json
{
  "stations_id": 3,
  "timestamp_mez": "1993-04-28T13:00:00",
  "timestamp_utc": "1993-04-28T12:00:00",

  "quality_level": 1,

  "pp_10": null,
  "tt_10": 25.8,
  "tm5_10": null,
  "rf_10": 19.0,
  "td_10": null,

  "metadata_matched": true,

  "source_zip": "10minutenwerte_TU_00003_19930428_19991231_hist.zip",
  "source_modified_at": "2025-01-15T08:00:00",
  "ingested_at": "2025-01-20T14:30:00",
  "schema_version": "1.0",
  "parser_version": "1.0.0",
  "row_hash": "b2c3d4e5"
}
```

**Orphan row (metadata_matched = false):**

```json
{
  "stations_id": 3,
  "timestamp_mez": "1993-04-28T12:30:00",
  "timestamp_utc": "1993-04-28T11:30:00",

  "quality_level": 1,

  "pp_10": 987.3,
  "tt_10": 24.9,
  "tm5_10": 28.4,
  "rf_10": 23.0,
  "td_10": 2.4,

  "metadata_matched": false,

  "source_zip": "10minutenwerte_TU_00003_19930428_19991231_hist.zip",
  "source_modified_at": "2025-01-15T08:00:00",
  "ingested_at": "2025-01-20T14:30:00",
  "schema_version": "1.0",
  "parser_version": "1.0.0",
  "row_hash": "c3d4e5f6"
}
```

### aggregates_*.jsonl

One line per aggregation period. Fields match `dwd_10min_air_temperature_aggregates_schema.yaml` (23 columns). Six files: `aggregates_hourly.jsonl`, `aggregates_daily.jsonl`, `aggregates_weekly.jsonl`, `aggregates_monthly.jsonl`, `aggregates_quarterly.jsonl`, `aggregates_yearly.jsonl`.

**Example — hourly aggregate (formatted for readability):**

```json
{
  "stations_id": 3,
  "period_start": "1993-04-28T11:00:00",
  "period_end": "1993-04-28T11:59:59",

  "pp_10_mean": 987.25,
  "pp_10_min": 987.2,
  "pp_10_max": 987.3,
  "pp_10_count": 3,

  "tt_10_mean": 25.1,
  "tt_10_min": 24.9,
  "tt_10_max": 25.5,
  "tt_10_count": 3,

  "tm5_10_mean": 28.57,
  "tm5_10_min": 28.4,
  "tm5_10_max": 28.7,
  "tm5_10_count": 3,

  "rf_10_mean": 21.33,
  "rf_10_min": 20.0,
  "rf_10_max": 23.0,
  "rf_10_count": 3,

  "td_10_mean": 1.43,
  "td_10_min": 0.7,
  "td_10_max": 2.4,
  "td_10_count": 3
}
```

**With null statistics (all readings were null for a parameter):**

```json
{
  "stations_id": 3,
  "period_start": "1993-04-28T15:00:00",
  "period_end": "1993-04-28T15:59:59",

  "pp_10_mean": null,
  "pp_10_min": null,
  "pp_10_max": null,
  "pp_10_count": 0,

  "tt_10_mean": 26.5,
  "tt_10_min": 26.1,
  "tt_10_max": 27.0,
  "tt_10_count": 6,

  "tm5_10_mean": null,
  "tm5_10_min": null,
  "tm5_10_max": null,
  "tm5_10_count": 0,

  "rf_10_mean": 18.5,
  "rf_10_min": 17.0,
  "rf_10_max": 20.0,
  "rf_10_count": 6,

  "td_10_mean": null,
  "td_10_min": null,
  "td_10_max": null,
  "td_10_count": 0
}
```

---

## JSONL to Parquet Conversion

When converting JSONL to Parquet, use pandas with explicit type casting:

```python
import pandas as pd

# Read JSONL
df = pd.read_json('stations.jsonl', lines=True)

# Convert timestamps from ISO strings to datetime
timestamp_cols = ['valid_from', 'valid_to', 'source_modified_at', 'ingested_at']
for col in timestamp_cols:
    df[col] = pd.to_datetime(df[col])

# Write Parquet with compression
df.to_parquet('stations.parquet', compression='snappy')
```

The YAML schema defines the expected Parquet types. Pandas will infer most types correctly from JSON, but timestamps need explicit conversion.
