# ClimaStation: DWD Climate Data Processing Platform

--------------------------------------------------------------------------------------------------------------
---------------------------------------- PART 1: STABLE DOCUMENTATION ----------------------------------------
--------------------------------------------------------------------------------------------------------------

## Purpose

ClimaStation is a processing platform designed to make climate and weather station data — starting with the DWD (Deutscher Wetterdienst) repository — clean, usable, and accessible. It extracts raw measurement files, enriches them with metadata, and transforms them into structured formats suitable for downstream use.

This project does not aim to serve as a personal analysis tool, but rather to support external users who need high-quality, structured access to climate data.

---

## Intended Audience

ClimaStation is built to support a range of data users, including:

- Researchers and environmental analysts working with time-series climate data
- Policy makers, planners, and NGOs interested in long-term trends and local observations
- Software engineers and climate tech teams integrating climate data into applications
- Educators and journalists looking to explore and explain key environmental patterns

The platform’s design is shaped by the expectations and workflows of these user groups.


---

## Motivation

The DWD provides one of the most extensive public climate datasets in the world. However, the raw data is highly fragmented, sparsely documented, and difficult to work with at scale. ClimaStation aims to reduce this friction by building an end-to-end ingestion and standardization pipeline.

The ultimate goal is to create a robust and extensible system that can:

- Ingest and validate large volumes of raw measurement data
- Enrich records with consistent metadata (stations, sensors, etc.)
- Output structured data in a way that is useful for both humans and machines


---

## The Challenge in Numbers

```
https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/
├── 10_minutes/
│   ├── air_temperature/ (~1,600 files per folder)
│   ├── precipitation/
│   ├── wind/
├── hourly/ (15+ parameters)
├── daily/
├── monthly/
├── annual/
└── 1_minute/, 5_minutes/
```

Rough estimate:

* 500,000+ ZIP files
* Potentially multiple terabytes

---

## Architecture at a Glance

ClimaStation is built as a modular, configuration-driven platform for transforming raw measurement data from the DWD (Deutscher Wetterdienst) into structured, timestamp-centric JSONL records.

The current focus is on building the **bulk historical ingestion pipeline**. This pipeline is optimized to process large volumes of ZIP files from the DWD archive and convert them into clean, enriched output. Other pipelines, such as incremental daily updates, will be implemented later using similar architectural principles.

---

### Bulk Ingestion Pipeline Structure

The bulk pipeline is organized into two layers:

1. **Preprocessing Stages** (batch-oriented)
2. **File Processing Flow** (orchestrated per ZIP file)

---

### 1. Preprocessing Stages

These stages operate in batch mode and prepare the list of files to download and process.

- **Crawler (`crawler.py`)**
  - **Input**: Dataset root path (e.g., `10_minutes/air_temperature`)
  - **Output**: A `urls.jsonl` file containing one entry per downloadable ZIP file

- **Downloader (`downloader.py`)**
  - **Input**: `urls.jsonl` or a filtered subset
  - **Output**: Saves ZIP files to `data/dwd/2_downloaded_files/`

---

### 2. File Processing Flow

This stage is run file-by-file and is orchestrated by `run_pipeline.py`. It avoids unnecessary disk usage by extracting and deleting raw text files on the fly.

Each ZIP file is processed in the following sequence:

1. **Extractor (`extractor.py`)**
   - **Input**: A single ZIP file
   - **Output**: A temporary `.txt` file extracted from the archive

2. **Parser (`parser.py`)**
   - **Input**: The raw `.txt` file, dataset config, and metadata
   - **Output**: Generator of timestamp-centric records (as Python dictionaries)

3. **Enricher (`enricher.py`)**
   - **Input**: Each parsed record
   - **Output**: An enriched record with human-readable names, quality codes, station/sensor info, etc.

4. **Writer (`writer.py`)**
   - **Input**: Enriched records (streamed/generator)
   - **Output**: One or more `.jsonl` output files (e.g., 50,000 records per file)

5. **Cleanup**
   - The temporary `.txt` file is deleted to conserve disk space

---

### Supporting Modules

All pipeline components can import helper logic from:

- **`app/utils/`** — for logging, config loading, file operations, and progress tracking
- **`app/translations/`** — for quality code explanations, parameter mappings, and metadata enrichment

Each module is implemented as a standalone Python script that exposes one or more reusable functions. These are not intended to be executed directly — they are used by the main controller script.

---

### Central Orchestration

The main script `run_pipeline.py` ties everything together:
python app/main/run_pipeline.py --dataset 10_minutes_air_temperature --file-id 00003_19930428_19991231

---

## Data Architecture Decisions

### Record Format: Timestamp-Centric Design

ClimaStation uses a **timestamp-centric record format** where each record represents one timestamp at one station, containing all measured parameters for that time.

**Why Timestamp-Centric:**

- Serves 80% of user needs (researchers, developers, planners) optimally  
- Follows industry standards (Meteostat, Open-Meteo, NOAA patterns)  
- Enables easy multi-variable analysis and visualization  
- Optimized for TimescaleDB's columnar compression  
- Excel-friendly for non-technical users

**Example Record:**
{
  "station_id": "00003",
  "timestamp": "2023-01-15T12:00:00Z",
  "temperature": 15.2,
  "humidity": 78.5,
  "pressure": 1013.2,
  "precipitation": 0.0,
  "quality_codes": {
    "temperature": 1,
    "humidity": 1,
    "pressure": 1
  }
}


---

## Parsing Strategy

* **Input**: Raw DWD station files (various formats, encodings, quality issues)
* **Processing**: Universal parser handling metadata alignment, quality codes, unit conversions
* **Output**: Clean JSONL files with timestamp-centric records
* **Storage**: TimescaleDB for optimal time-series performance

---

## Data Quality Handling

* Preserve original quality codes from DWD
* Handle missing values explicitly (null vs 0 distinction)
* Maintain data integrity without assumptions
* Support error correction workflows

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
---------------------------------------- Last updated: 2025-08-10 --------------------------------------------


## 🔧 Immediate Task — Downloader can’t find/read crawler URLs JSONL

**Problem (symptom):** `download` mode logs “URLs file not found / No candidates to download” right after a successful crawl. Likely a **path/key mismatch** between YAML (`crawler.output_urls_jsonl`) and what `run_pipeline.py`/`downloader.py` read, or a **field-name mismatch** in the JSONL (expected: `url`, `relative_path`, `filename`).

**Objective:** Make `download` mode load the **exact** JSONL written by the crawler and honor `--limit` to fetch a small, deterministic set.

**What to do next (tomorrow):**
1) **Trace path resolution** in `run_pipeline.py` and `downloader.py` (where the URLs file path comes from) and align with `crawler.output_urls_jsonl`.
2) **Verify JSONL schema** used by `load_urls_from_jsonl` (must match `url`, `relative_path`, `filename`) and subfolder filtering behavior.
3) **Tighten error messages**: when the URLs file is missing, log the **exact path** we tried to open.

**Acceptance (quick):**
- After a crawl, `download --dry-run --limit 3` lists a plan with 1–3 items (no network), exits `0`.
- `download --limit 1` actually downloads one file, exits `0`, logs clear counts (attempted/ok/skip/failed).

**Deliverables:**
- Minimal code patch (runner and/or downloader) and, if needed, a tiny YAML key correction.
- One-sentence changelog: which path/key was aligned and where the URLs JSONL is read from.


## 🔧 Immediate Task — Add persistent file logging (runner, crawler, downloader)

**Problem (symptom):** No **log files** are created; only console output. Hard to debug and audit behavior over time.

**Objective:** Enable rotating **file-based logs** for the runner and pipeline components, with structured summaries and error details.

**What to do next (tomorrow):**
1) **Choose a log root** (e.g., `logs/` or `data/logs/`) and add it to base/config; ensure directory creation.
2) **Configure file handler(s)** in `enhanced_logger.py` (rotation, size, count); avoid duplicate handlers on repeated runs.
3) **Ensure components use the logger**: `run_pipeline.py`, `crawler.py`, `downloader.py` call `get_logger(...)` and emit start/end summaries and per-error details (include URL + HTTP status where applicable).
4) **Expose log level** via config (default `INFO`), and document Windows-safe paths.

**Acceptance (quick):**
- Running `crawl` and `download` produces non-empty files like `logs/pipeline.runner.log`, `logs/pipeline.crawler.log`, `logs/pipeline.downloader.log`.
- No duplicate log lines; rotation settings in effect; summaries include counts and output paths.

**Deliverables:**
- Updated logger configuration/code and (if needed) base/config keys for `log_dir` and `log_level`.
- Brief README note: where logs live and how to change level/rotation.


---

### Current Folder Structure

CLIMASTATIONz
├── _legacy/
├── .venv/
├── vscode/
│   ├── launch.json
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
│   │   ├── paths.py   (empty, do we still need this file?)
│   │   └── progress_tracker.py (finished, but needs validation in practice)
│   └── __init__.py
├── context/             (For AI implementation)
│   ├── available_functions.md (updated)
│   ├── coding_patterns.py
│   └── processor_interface.py
├── data/
│   └── dwd/
│       ├── 0_debug/
│       ├── 1_crawl_dwd/
│       ├── 2_downloaded_files/
│       └── 3_parsed_files/
├── .gitignore
├── dev_log.md
├── prompt_project_manager.txt
├── README.txt
└── requirements.txt



