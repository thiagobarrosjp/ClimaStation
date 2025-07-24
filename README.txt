# ClimaStation: DWD Climate Data Processing Platform

---

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

### Pipelines

- **Bulk Historical Ingestion**: One-time, massive. Establishes the baseline.  
- **Incremental Updates**: Keeps everything fresh with daily/weekly syncs.

### Component Overview

```
ClimaStation
├── Orchestrators
│   ├── BulkIngestController
│   └── IncrementalSyncController
├── Dataset Processors (One per dataset)
│   ├── parse_10_minutes_air_temperature_hist.py
│   ├── parse_10_minutes_precipitation.py (future)
├── Shared Components
│   ├── raw_parser.py
│   ├── station_info_parser.py
│   ├── sensor_metadata.py
│   └── zip_handler.py
├── Utilities
│   ├── enhanced_logger.py
│   ├── configuration_manager.py
│   ├── data_validator.py
│   └── performance_monitor.py
```

---

## Data Flow (Simplified)

1. **Download** ZIP from DWD
2. **Extract** contents
3. **Parse** measurements
4. **Enrich** with metadata (stations, sensors)
5. **Validate** completeness & quality
6. **Output** standardized JSONL

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

## Architecture Flexibility

* Station-specific exports (CSV downloads)
* Parameter-specific queries (single variable access)
* Future NetCDF/Parquet exports for power users
* Schema evolution for new DWD parameters

---

## Implementation Status

* ✅ Configuration system (`config_manager.py`)
* ✅ Enhanced logging (`enhanced_logger.py`)
* ✅ File operations utilities (`file_operations.py`)
* ✅ DWD crawler (`crawl_dwd.py`)
* 🔄 Current focus: Progress tracker and parsing system
* ⏳ Next: Database integration and API development

---

## Development Roadmap

### Short-Term

1. Robust single-dataset pipeline (air temperature, 10min, historical)
2. Logging, validation, performance monitoring finalized
3. Start extending to recent/now folders

### Mid-Term

1. Generalize components for precipitation, wind, etc.
2. Build bulk ingestion orchestrator
3. Automate update syncs

### Long-Term

1. Full repository coverage
2. Public API
3. Data visualization portal

---

## Current Focus (2025-07-18)

Perfecting the 10-minute air temperature historical dataset (1,623 files) as a reliable template.

---

## Key Standards

* JSONL Output: timestamp-centric, enriched with metadata.
* Centralized Logging: component-based codes.
* Full Automation: no manual steps allowed.

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

## Success Criteria (Reminder)

* **All 1,623 files parsed successfully**
* **Memory stable < 2GB**
* **Outputs clean, consistent, validated**
* **No manual steps after kickoff**
* **Ready to scale to 500k+ files**

---

## Status (2025-07-18)

Proof-of-concept pipeline nearly done for 10min air temperature historical.
Next step: finalize logging, validate performance, expand to recent/now folders.

---

## Next Steps: Architecture Implementation

### Immediate Priority (Phase 1)
Based on architectural discussions, we're implementing a **Sequential Datasets + Parallel Workers** approach:

**1. Restructure Current Script into Pipeline Orchestrator**
- Transform `parse_10_minutes_air_temperature_hist.py` into proper orchestrator
- Implement clear pipeline stages with fail-fast behavior
- Add file-level progress tracking with SQLite database

**2. Build Core Architecture Components**


Target Architecture:
├── BulkIngestController (master orchestrator)
├── DatasetProcessor (10_minutes_air_temperature)
│   ├── Parallel Worker Pool (4 workers max)
│   │   ├── Worker 1: /historical/ files 1-400
│   │   ├── Worker 2: /historical/ files 401-800
│   │   ├── Worker 3: /recent/ files
│   │   └── Worker 4: /now/ files
│   └── Success Gate (ALL must succeed)
└── Shared Components (raw_parser, station_info, etc.)


**3. Configuration System**
- `base_config.yaml`: Resource limits, paths, failure handling
- `dataset_configs/10_minutes_air_temperature.yaml`: Dataset-specific settings
- Environment overrides for dev/testing

**4. Progress Tracking Database**

file_processing_log:
id | dataset | file_path | status | start_time | end_time | error_msg


**5. Resource Management**

- Max 4 parallel workers (configurable)
- Memory limit: 512MB per worker
- Worker timeout: 30 minutes
- Checkpoint every 100 files


### Key Design Principles

- **Fail-Fast**: Any worker failure stops entire dataset processing
- **File-Level Tracking**: Every ZIP file tracked with status and timing
- **Single-Machine Optimized**: No distributed processing complexity
- **AI-Maintainable**: Clear configuration hierarchy and component interfaces


### Success Metrics for Phase 1

- Process all 1,623 historical files without manual intervention
- Memory usage stays under 2GB total
- Complete processing in under 8 hours
- Zero data loss or corruption
- Full traceability of every file processed

---

## AI Development Workflow (2025-07-22)

### Two-Chat Strategy

This project uses a **dual-chat AI development approach** to optimize context usage and maintain architectural consistency:

**Chat 1: Project Manager Role**
- **Purpose**: Strategic planning, architecture decisions, prompt crafting
- **Context**: This full README.txt file + project overview
- **Responsibilities**:
  - Help define next development steps
  - Create specific, actionable prompts for implementation
  - Ensure architectural consistency across implementations
  - Review and refine implementation approaches
  - Maintain project vision and standards

**Chat 2: Implementation Assistant Role**  
- **Purpose**: Pure code implementation with minimal context
- **Context**: Only interface files + specific task prompt
- **Responsibilities**:
  - Write focused code based on crafted prompts
  - Follow established patterns and interfaces
  - Implement specific features without architectural decisions

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
├── available_functions.py      # Utility functions reference (30 lines)├── coding_patterns.py          # Standard patterns and imports (40 lines)
└── dataset_configs/            # Dataset-specific configurations
└── 10_minutes_air_temperature.yaml


### Implementation Prompt Template

When creating prompts for the implementation chat, use this format:
Task: [Specific implementation task]

Context Files to Attach:

- context/processor_interface.py
- context/available_functions.py
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


### Success Metrics for AI Development

- **Context Efficiency**: Implementation prompts use <150 lines of context
- **Consistency**: All implementations follow established interfaces
- **Focus**: Each chat stays within its defined role
- **Quality**: Generated code integrates seamlessly with existing architecture

---

2025-07-24:
Current folder structure:
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
│   │   └── run_bulk_ingestion.py  (placeholder with legacy code, not working)
│   ├── orchestrators/
│   │   ├── bulk_ingestion_controller.py  (placeholder with legacy code, not working)
│   │   └── dataset_orchestrator.py  (placeholder with legacy code, not working)
│   ├── processors/
│   │   ├── base_processor.py  (placeholder with legacy code, not working)
│   │   └── ten_minutes_air_temperature_processor.py  (placeholder with legacy code, not working)
│   ├── shared/
│   ├── translations/
│   │   ├── meteorological/
│   │   │  ├── __init__.py
│   │   │  ├── data_sources.yaml
│   │   │  ├── equipment.yaml
│   │   │  ├── parameters.yaml
│   │   │  └── quality_codes.yaml
│   │   ├── providers/
│   │   │  └── dwd.yaml
│   │   └── translation_manager.py  (placeholder with legacy code, not working)
│   ├── utils/
│   │   ├── config_manager.py
│   │   ├── dwd_crawler.py
│   │   ├── enhanced_logger.py
│   │   ├── file_operations.py
│   │   └── progress_tracker.py  (placeholder with legacy code, not working)
│   ├── workers/
├── context/
│   ├── available_functions.py/
│   ├── coding_patterns.py/
│   ├── current_task.py/
│   └── processor_interface.py/
├── data/
│   └── dwd/
│       ├── 0_debug/
│       ├── 1_crawl_dwd/
│       ├── 2_downloaded_files/
│       └── 3_parsed_files/
├── .gitignore
├── dev_log.md
├── PromptV0.txt
├── README.txt
└── requirements.txt