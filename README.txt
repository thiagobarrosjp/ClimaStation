# ClimaStation: DWD Climate Data Processing Platform

--------------------------------------------------------------------------------------------------------------
---------------------------------------- PART 1: STABLE DOCUMENTATION ----------------------------------------
--------------------------------------------------------------------------------------------------------------

## Purpose

ClimaStation makes climate and weather **station** data — starting with the DWD (Deutscher Wetterdienst) repository — clean, usable, and easy to download. It mirrors raw files, normalizes them, and provides structured outputs that are reliable, provenance-rich, and ready for analysis.

This project focuses on **serving external users** who need high-quality, structured access to climate data (not on bespoke one-off analyses).

---

## Intended Audience

- Researchers and environmental analysts working with time-series station data  
- Policy makers, planners, and NGOs interested in local observations and long-term trends  
- Software engineers and climate-tech teams integrating climate data into apps  
- Educators and journalists who need trustworthy, explainable datasets

The architecture prioritizes data reliability, reproducibility, and ease of use for these groups.

---

## Motivation

DWD’s open data is extensive but **fragmented**: many folders, many ZIPs, multiple formats, and varying metadata quality. ClimaStation reduces this friction with an end-to-end pipeline that:

- Crawls and mirrors raw files (independence from upstream availability)
- Normalizes units/timestamps and preserves **provenance per row**
- Outputs compact, typed, columnar files suitable for fast filters/joins

---

## The Challenge in Numbers

```text
https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/
├── 10_minutes/
│   ├── air_temperature/
│   ├── precipitation/
│   ├── wind/
├── hourly/ ...
├── daily/ ...
└── monthly/, annual/, 1_minute/, 5_minutes/ ...
```

Rough scale (order of magnitude):

- Hundreds of thousands of ZIP files across products  
- Hundreds of millions of rows for long time spans and many stations

---

## Architecture at a Glance

ClimaStation separates **data semantics** from **on-disk storage**:

- **Semantic record model (for queries & APIs):**  
  **timestamp-centric per station** → each record = one `(station_id, timestamp)` with all variables available at that moment.

- **On-disk storage (for performance & ops):**  
  **Partitioned Parquet** files, one row per `(station_id, timestamp_utc)`, partitioned by `station_id` and `year`.  
  Query engine: **DuckDB** reads Parquet directly (no DB server needed).

Current focus: the **10_minutes/air_temperature** dataset (subset: **historical**, plus **meta_data**). Other datasets are added using the same pattern.

---

### Ingestion & Cache Structure

Two cooperating layers:

1) **Preprocessing (batch)**  
   - Discover URLs and **mirror raw ZIPs** locally for independence and reproducibility.

2) **Parsed Cache (on-demand + background warm-up)**  
   - On first request for a `(station_id, year)`, parse the raw TXT into a **Parquet partition** and cache it.  
   - A low-priority background job gradually pre-parses recent/hot partitions.

This hybrid approach keeps costs low while delivering fast responses.

---

### 1. Preprocessing Stages

- **Crawler (`crawler.py`)**  
  **Input:** Dataset root (e.g., `10_minutes/air_temperature`)  
  **Output:** `urls.jsonl` with one entry per downloadable ZIP (with canonical URLs)

- **Downloader (`downloader.py`)**  
  **Input:** `urls.jsonl` (or filtered subset)  
  **Output:** Mirrors ZIPs under `raw/dwd/10_minutes/air_temperature/{historical,meta_data}/`

---

### 2. File Processing Flow (Parsed Cache)

Orchestrated by the backend when needed (no permanent intermediate TXT on disk):

1. **Extractor (in-memory)**  
   - Open ZIP, stream the single TXT member; skip comment/header lines.

2. **Parser (dataset-specific, v0: air temperature)**  
   - Normalize timestamp to **UTC** (see Data Quality notes).  
   - Convert units (e.g., 0.1 °C → °C).  
   - Map DWD quality level (QN) to `quality_level`.

3. **Provenance & Lineage**  
   - Annotate every row with:  
     `source_url`, `source_filename`, **`source_row`** (line number), `file_sha256`, `ingested_at`.

4. **Writer (Parquet)**  
   - Enforce uniqueness on `(station_id, timestamp_utc)`, sort by time.  
   - Write to `parsed/dwd/10_minutes_air_temperature/station_id=<id>/year=<YYYY>/<id>_<YYYY>.parquet`.

5. **Background Warmer (optional service)**  
   - Parses recent years for all stations during idle time.

> We no longer write bulk JSONL outputs; Parquet is the ground truth for normalized values. JSON/CSV are **export formats** produced on demand.

---

### Supporting Modules

- **`app/utils/`** — logging, config, hashing, file ops, progress  
- **`app/tools/`** — validators, golden refreshers, small CLIs  
- **`app/translations/`** — quality code explanations, parameter mappings  
- **`app/pipeline/`** — crawler, downloader, parser(s), and helpers

Modules expose functions used by orchestration; they are not meant to be executed directly.

---

### Central Orchestration

Primary entry point: `run_pipeline.py`.

Examples (from repo root):

```bash
# Crawl online (with schema validation)
python -m app.main.run_pipeline --mode crawl --dataset 10_minutes_air_temperature --validate

# Crawl from golden HTML (offline, for tests/CI)
python -m app.main.run_pipeline --mode crawl --dataset 10_minutes_air_temperature --source offline --outdir .tmp/golden_out --validate
```

The **parsed cache** is invoked by the export/query service: when an export needs a `(station_id, year)` not yet cached, it parses it once and reuses it thereafter.

---

## Data Architecture Decisions

### Semantic Record Model (used by queries & API)

- **One record per station + instant.**  
  Primary time is `timestamp_utc` **when a parameter window defines the time reference**; otherwise we keep the raw local time as `timestamp_local` and set `time_ref="unknown"` (no guessing).  
- **Everything normalized at the top level** (friendly to read & filter). Field names are stable across stations.

**Normalized fields (v0, air temperature 10-min):**
- `station_id` (5-digit string)
- `timestamp_utc` (nullable ISO-8601, UTC)  
- `timestamp_local` (raw `YYYYMMDDHHMM` string from DWD)
- `time_ref` (`"UTC" | "MEZ" | "unknown"`)
- `quality_level` (int)
- `pressure_station_hpa` (float)
- `temperature_2m_c` (float)
- `temperature_0p05m_c` (float)
- `humidity_rel_pct` (float)
- `dewpoint_2m_c` (float)
- `parameter_window_found` (bool)

**Precision rule:** values are parsed as numbers but **exported/rendered with one decimal place** (as published by DWD). We never invent extra significant digits.

**Example (inside a parameter window → UTC known):**
```json
{
  "station_id": "00003",
  "timestamp_local": "199304291230",
  "time_ref": "MEZ",
  "timestamp_utc": "1993-04-29T11:30:00Z",
  "quality_level": 1,
  "pressure_station_hpa": 987.3,
  "temperature_2m_c": 24.9,
  "temperature_0p05m_c": 28.4,
  "humidity_rel_pct": 23.0,
  "dewpoint_2m_c": 2.4,
  "parameter_window_found": true
}
```

**Example (metadata gap → UTC unknown, keep raw time):**
```json
{
  "station_id": "00003",
  "timestamp_local": "199304281230",
  "time_ref": "unknown",
  "timestamp_utc": null,
  "quality_level": 1,
  "pressure_station_hpa": 987.3,
  "temperature_2m_c": 24.9,
  "temperature_0p05m_c": 28.4,
  "humidity_rel_pct": 23.0,
  "dewpoint_2m_c": 2.4,
  "parameter_window_found": false
}
```

---

### On-Disk Storage (used by the engine)

- **Parquet** files, partitioned by `station_id` and `year`.  
  - `year` = year of `timestamp_utc` **if present**, otherwise the year derived from `timestamp_local` (ensures gap rows are still partitioned deterministically).
- **v0 Parquet schema (columns):**
  - `station_id` (string, zero-padded 5)
  - `timestamp_utc` (timestamp, nullable)
  - `timestamp_local` (string, `YYYYMMDDHHMM`)
  - `time_ref` (string enum: `"UTC" | "MEZ" | "unknown"`)
  - `quality_level` (int32)
  - `pressure_station_hpa` (float64, NaN for missing)
  - `temperature_2m_c` (float64)
  - `temperature_0p05m_c` (float64)
  - `humidity_rel_pct` (float64)
  - `dewpoint_2m_c` (float64)
  - `parameter_window_found` (bool)
  - **Provenance:** `source_filename` (string), `source_url` (string), `source_row` (int32), `file_sha256` (string), `ingested_at` (timestamp, UTC)

- **No duplication of station metadata per row.**  
  Instead we maintain small, joinable registries:
  - **Station Registry** (time-segmented geography & naming):  
    `station_id, name, operator, latitude, longitude, elevation_m, valid_from, valid_to`
  - **Station-Parameter Registry** (drives parsing rules & i18n):  
    `station_id, parameter_code, unit, time_ref_enum, valid_from, valid_to, parameter_description_{de,en}, data_source_note_{de,en}, time_ref_note_{de,en}`

> Files may span site moves. If a TXT overlaps multiple geo segments, we **split once at segment boundaries** (not per row) when writing Parquet.

---

### Query Engine

- **DuckDB** reads the Parquet partitions directly (no DB server).  
  - Column pruning and predicate pushdown on `station_id`/`timestamp_*` provide fast filters.
- **Exports:** **Parquet** (default) or **CSV (zip)** plus a `manifest.json` containing:
  - dataset + query parameters,
  - list of source ZIP/TXT files with `sha256`,
  - row counts and any **metadata gaps** encountered,
  - optional parameter catalog (`{code, unit, description {en,de}}`).
- **Precision on export:** numeric fields are rendered with **exactly one decimal** (CSV) or as plain JSON numbers that reflect one decimal (no padded zeros).

---

## Parsing Strategy

- **Input:** mirrored DWD ZIPs (`historical` for values; `meta_data` for stations)  
- **Processing:**
  - Stream TXT, skip comments; robust parsing (`utf-8` with `latin-1` fallback)
  - Normalize **timezone** to UTC (see below)
  - Convert units (e.g., tenths of °C → °C)
  - Map DWD quality level to `quality_level`
  - Remove sentinels (`-999`, `-9999`, empty) → null/NaN
  - Enforce uniqueness `(station_id, timestamp_utc)` and sort
- **Output:** Parquet partition(s) per `(station_id, year)` with provenance columns populated
- **Access:** On-demand parsing with cached results; background warmer for recent years

---

## Data Quality Handling

- Preserve and expose original **quality levels** (QN) as `quality_level`
- Explicitly encode missing values as **null/NaN** (never as 0)
- Normalize timestamps to **UTC**; for legacy files that use local time references, convert during parsing and document rules in code/comments
- Maintain **provenance per row**: `source_url`, `source_filename`, `source_row`, `file_sha256`, `ingested_at`
- Ensure **uniqueness** of `(station_id, timestamp_utc)`; drop exact duplicates deterministically and log counts


---

## Raw Data Sample (Reference for AI)

**File:** 10minutenwerte_TU_00003_19930428_19991231_hist.zip
**Contents:** produkt_zehn_min_tu_19930428_19991231_00003.txt

STATIONS_ID;MESS_DATUM;QN;PP_10;TT_10;TM5_10;RF_10;TD_10
      3;199304281230;    1;  987.3;  24.9;  28.4;  23.0;   2.4
      3;199304281240;    1;  987.2;  24.9;  28.6;  21.0;   1.2
      3;199304281250;    1;  987.2;  25.5;  28.7;  20.0;   0.7
      3;199304281300;    1;  987.0;  25.8;  28.8;  20.0;   1.0
      3;199304281310;    1;  986.9;  25.8;  29.6;  20.0;   0.9
      3;199304281320;    1;  986.7;  25.7;  29.7;  19.0;   0.2
      3;199304281330;    1;  986.8;  26.0;  29.8;  20.0;   1.5
      3;199304281340;    1;  986.8;  26.1;  29.7;  18.0;   0.2
      3;199304281350;    1;  986.7;  27.0;  29.7;  19.0;   1.4

---

## Station List Sample (Reference for AI)

**File:** zehn_min_tu_Beschreibung_Stationen.txt

Stations_id von_datum bis_datum Stationshoehe geoBreite geoLaenge Stationsname Bundesland Abgabe
----------- --------- --------- ------------- --------- --------- ----------------------------------------- ---------- ------
00003   19930429    20110331    202   50.7827   6.0941    Aachen                  Nordrhein-Westfalen   Frei
00044   20070209    20250710    44    52.9336   8.2370    Großenkneten            Niedersachsen         Frei
00071   20091201    20191231    759   48.2156   8.9784    Albstadt-Badkap         Baden-Württemberg     Frei
00073   20070215    20250710    374   48.6183   13.0620   Aldersbach-Kramersepp   Bayern                Frei
00078   20041012    20250709    64    52.4853   7.9125    Alfhausen               Niedersachsen         Frei
00091   20020821    20250710    304   50.7446   9.3450    Alsfeld-Eifa            Hessen                Frei
00096   20190410    20250710    50    52.9437   12.8518   Neuruppin-Alt Ruppin    Brandenburg           Frei
00102   20250410    20250710    0   53.8633     8.1275    Leuchtturm Alte Weser   Niedersachsen         Frei

---

## AI Development Workflow

This project uses a two-chat development strategy to split architectural planning and implementation:

- **GPT-5 Thinking (Project Manager)**:
  - Plans architecture and file structure
  - Creates clear implementation prompts
  - Ensures modularity and consistency

- **GPT-5 (Implementation Assistant)**:
  - Writes code based on context files and task prompts
  - Follows strict constraints with no architectural decisions
  - Deliverables must include updated contract docs, schema changes, and passing property + golden tests

This workflow allows clean separation of concerns and reproducibility between sessions. Context files used by GPT-5 are version-controlled and updated by the developer as needed.

### Project Manager Instructions

When acting as Project Manager, you should:

1. **Understand the full project context** from this README
2. **Help prioritize next steps** based on current status and roadmap
3. **Create implementation prompts** that include:
   - Specific task description
   - Required context files to attach
   - Expected output format
   - Architectural constraints to follow
4. **Ensure consistency** with established patterns and interfaces
5. **Break down complex tasks** into manageable implementation chunks

### Context Files for Implementation Chat

The following minimal context files should be created and used for implementation tasks:
context/
├── processor_interface.py      # Standard processor contract (25 lines)
├── available_functions.py      # Utility functions reference (30 lines)
├── coding_patterns.py          # Standard patterns and imports (40 lines)
└── dataset_configs/            # Dataset-specific configurations
    └── 10_minutes_air_temperature.yaml


### Implementation Prompt Template

When creating prompts for the implementation chat, use this format:
Task: [Specific implementation task]

Context Files to Attach:

- context/processor_interface.py
- context/available_functions.py
- context/coding_patterns.py
- context/quality_standard.md
- [specific file being modified]


Requirements:

- [Specific functional requirements]
- [Performance constraints]
- [Error handling expectations]


Expected Output:

- [File modifications needed]
- [New files to create]
- [Testing approach]


Architectural Constraints:

- Must implement IDataProcessor interface
- Must return ProcessingResult objects
- Must use dependency injection pattern
- Must follow established logging patterns

### Stage Quality Standard

Stage Quality Standard (applies to every pipeline stage)
1. "Invariants defined (record/file/run-level)"
2. "Schema provided (record-level, JSON Schema if JSONL)"
3. "Property tests (file/run-level rules)"
4. "Golden test (deterministic output for frozen inputs)"
5. "CI gate blocks merge on any failure (format/lint/types/tests)"


### Markdown Output Rules (PM AI — SHORT)

**Goal:** always return **one single fenced markdown block** the user can copy/paste.

## Do this
1. **Wrap everything** in one outer fence: start with `\`\`\`\`markdown` and end with `\`\`\`\`` on its own line.
2. Use **triple backticks** for inner examples (```json, ```bash, …).  
   *Outer uses four backticks; inner uses three.*
3. **Fence lines are clean:** no text or spaces on the same line as the fence.
4. **Don’t use 4+ backticks inside** a 4-backtick block (it would close the outer).
5. To show literal backticks, either **indent by 4 spaces** or **increase the outer fence length**.
6. Prefer `markdown` as the outer language so the UI shows a “markdown” panel.

## Minimal template (copy)
````markdown
# Title

Intro.

## Example
```json
{"ok": true}
```


### One-shot runner
Run the full pipeline (online + offline + tests + push):
# default (Limit=500, includes offline/tests/push)
.\scripts\run_all.ps1





--------------------------------------------------------------------------------------------------------------
---------------------------------------- PART 2: STRATEGIC GOALS ---------------------------------------------
--------------------------------------------------------------------------------------------------------------

## 🎯 Success Criteria for Bulk Ingestion

- [ ] All 1,623 ZIP files parsed successfully  
- [ ] Output uses timestamp-centric JSONL format  
- [ ] Metadata enrichment complete (station + sensor + codes)  
- [ ] Pipeline resumes cleanly after interruption  
- [ ] Memory usage stays < 2GB total  
- [ ] No manual steps required during execution  
- [ ] Dataset can be changed via YAML config only  

---

## 🧱 Design Principles (In Practice)

- ✅ **Fail-fast**: One broken file = stop the run  
- ✅ **File-level tracking** via `progress_tracker.py`  
- ✅ **Streaming design**: record generators, not full-loads  
- ✅ **Modular logic**: clearly separated stages  
- ✅ **Config-driven behavior** using `*.yaml` only  
- ⏳ **Parallel processing**: designed, not yet implemented  

---

## 📊 Performance Metrics (To Track)

(Will be filled once testing begins)

- [ ] Total runtime for all files < 8 hours  
- [ ] Avg memory usage < 2GB  
- [ ] Chunking correct (≤50k records per .jsonl file)  
- [ ] Output validated against schema  
- [ ] All files log success/failure clearly  


--------------------------------------------------------------------------------------------------------------
---------------------------------------- PART 3: DAILY STATUS ------------------------------------------------
---------------------------------------- Last updated: 2025-08-31 --------------------------------------------


# Task List

## ✅ Enforce Stage Quality Standard — Crawler (finish)
- [ ] **Golden test (offline, no network):** `tests/dwd/test_crawler_golden_offline.py` runs the crawler via `run_pipeline.py --mode crawl --source offline --outdir <tmp>` and asserts **byte-identical** outputs to:
  - `tests/dwd/golden/expected/10_minutes_air_temperature_urls.golden.jsonl`
  - `tests/dwd/golden/expected/10_minutes_air_temperature_urls_sample100.golden.jsonl`
- [ ] **CI gate (lean):** `.github/workflows/tests.yaml` runs `black --check .`, `ruff .`, `mypy .`, `pytest -q` (incl. golden). Mark this job **required** for `main`.
- [ ] **Pipeline flag:** add `--validate` to `run_pipeline.py` (crawl mode) to invoke the validator; integrate into `scripts/run_all.ps1`.

---

## Parser v0 — Historical ZIP → Parquet (lazy, all stations)
- [ ] Parse one ZIP (historical) → normalize timestamps (UTC), preserve DWD quality codes, write **Parquet** partitioned as `dataset/station_id/year/`.
- [ ] **Property test:** for 2–3 frozen files: required columns present, ≥1 row, timestamps parse, no duplicate `(station_id, timestamp)`.
- [ ] **Smoke test:** parse a single known ZIP end-to-end into Parquet (temp dir).

---

## Station Metadata Ingest — `meta_data` → Registry
- [ ] Parse station list TXT → structured table with active periods, elevation, lat/lon, name; write Parquet/CSV.
- [ ] Expose simple lookup API/helper; include **change log** info for plotting markers.
- [ ] **Test:** deterministic parse on fixture; assert a few known rows/fields.

---

## API v0 — Timeseries + HDD/CDD
- [ ] `GET /stations/{id}/timeseries?from=&to=&format=json|csv|parquet` (reads Parquet).
- [ ] `GET /stations/{id}/metrics/hdd_cdd?from=&to=&base_heat=18&base_cool=22&agg=daily|monthly` (lazy compute → cache derived Parquet under `/derived/`).
- [ ] **Tests:** API smoke (returns data from cache); micro-fixture correctness for HDD/CDD on a tiny range.

---

## Web UI v0 — Minimal, Useful
- [ ] Station search + line chart (temperature).  
- [ ] Toggles: HDD/CDD overlay; **station metadata change markers**.  
- [ ] **Export** button (CSV/Parquet for current view).

---

## Ops (minimal, solo-friendly)
- [ ] Nightly **pre-warm** job for top station/year pairs.  
- [ ] Single session **log file**; `.env` config; attribution note for DWD (CC-BY-4.0).  
- [ ] README quickstart updated (run crawl offline test, parse one file, start API/UI).

---

### Deferred (explicitly *not* now)
- Downloader golden tests, extended crawler logging, additional datasets, parallelism, heavy infra.


---

### Current Folder Structure

CLIMASTATION-BACKEND/
├── .github/
│   └── workflows/
│       └── tests.yaml
├── .tmp/
│   └── golden_out/
│       ├── 10_minutes_air_temperature_urls_sample100.jsonl
│       └── 10_minutes_air_temperature_urls.jsonl
├── .venv/
├── vscode/
│   ├── launch.json
│   ├── pyproject.toml
│   └── settings.json
├── app/
│   ├── config/
│   │   ├── datasets/
│   │   │  └── 10_minutes_air_temperature.yaml
│   │   └── base_config.yaml
│   ├── main/
│   │   └── run_pipeline.py
│   ├── pipeline/
│   │   ├── crawler.py    (finished and validated)
│   │   ├── downloader.py (successfully tested for small number of files)
│   │   ├── enricher.py   (empty, not started yet)
│   │   ├── extractor.py  (empty, not started yet)
│   │   ├── parser.py     (empty, not started yet)
│   │   └── writer.py     (empty, not started yet)
│   ├── tools/
│   │   ├── gen_available_functions.py (scans codebase and regenerates available_functions.md)
│   │   ├── refresh_fixture.py  (created to generate *_urls_sample100.jsonl from *_urls.jsonl files)
│   │   └── validate_crawler_urls.py (validates *_urls.jsonl files against schemas and contracts)
│   ├── translations/
│   │   ├── meteorological/
│   │   │  ├── __init__.py
│   │   │  ├── data_sources.yaml
│   │   │  ├── equipment.yaml
│   │   │  ├── parameters.yaml
│   │   │  └── quality_codes.yaml
│   │   ├── providers/
│   │   │  └── dwd.yaml
│   │   └── translation_manager.py (finished, but needs validation in practice)
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── config_manager.py (finished, can't tell if it is working as intended or not)
│   │   ├── enhanced_logger.py (finished and is working fine for crawling, needs validation for other tasks)
│   │   ├── file_operations.py (finished, can't tell if it is working as intended or not)
│   │   ├── http_headers.py
│   │   └── progress_tracker.py (finished, but needs validation in practice)
│   └── __init__.py
├── context/             (For AI implementation)
│   ├── available_functions.md
│   ├── coding_patterns.py
│   ├── pm_prompt_playbook.txt
│   ├── processor_interface.py
│   └── quality_standards.md
├── data/
│   └── dwd/
│       ├── 0_debug/
│       ├── 1_crawl_dwd/
│       ├── 2_downloaded_files/
│       └── 3_parsed_files/
├── docs/
│   └── dwd/
│       └── contracts/
│           ├── crawler.md
│           └── downloader.md
├── schemas/
│   └── dwd/
│       └── crawler_urls.schema.json
├── scripts/
│   └── run_all.ps1
├── tests/
│   └── dwd/
│       ├── fixtures/
│       ├── golden/
│       │   └── climate/
│       │       ├── 10_minutes/
│       │       │   ├── air_temperature/
│       │       │   │   ├── historical/
│       │       │   │   │   └── index.html
│       │       │   │   ├── meta_data/
│       │       │   │   │   └── index.html
│       │       │   │   ├── now/
│       │       │   │   │   └── index.html
│       │       │   │   └── recent/
│       │       │   │       └── index.html
│       │       │   │   └── index.html
│       │       │   └── index.html 
│       │       └── index.html
│       ├── test_crawler_golden_offline.py
│       ├── test_validate_crawler_urls.py
│       └── test_validator_fixtures_smoketest.py
├── .gitignore
├── .gitattributes
├── .pre-commit-config.yaml
├── dev_log.md
├── prompt_project_manager.txt
├── pytest.ini
├── README.txt
└── requirements.txt

