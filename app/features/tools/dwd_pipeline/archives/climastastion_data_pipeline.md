# 🌍 ClimaStation Data Processing Pipeline

## 🧠 Project Scope and Vision
ClimaStation is a backend pipeline designed to transform raw datasets from the Deutscher Wetterdienst (DWD) into structured, metadata-enriched records that follow a universal schema. The goal is to unify disparate formats and time ranges into **clean, timestamp-centric JSON records** suitable for querying, visualization, or AI-assisted analysis.  
This pipeline is:
- Modular and schema-first  
- Designed to scale from a few samples to the full DWD repository  
- Intended to support record generation, schema evolution, and metadata validation

## 📁 Folder Structure
All inputs, intermediate files, and outputs are saved under `data/` or `app/features/dwd/`.

### 🔧 `data/` — Working Directory
| Path | Description |
|------|-------------|
| `data/0_debug/` | Logs and debug files from each pipeline step |
| `data/1_structure/` | Folder tree snapshots and URL discovery results |
| `data/2_samples/raw/` | Downloaded `.zip` and `.pdf` samples from DWD |
| `data/3_inspection/` | Structured inspection output from archives |
| `data/4_summaries/` | Combined dataset + station summaries |
| `data/5_matching/` | Metadata-aligned raw files, grouped by station |
| `data/6_fields/` | Canonical field lists per dataset |

### 📚 `app/features/dwd/record_schemas/`
| File | Purpose |
|------|---------|
| `field_map.json` | Maps inconsistent field names to canonical forms |
| `v1_universal_schema.json` | Draft-07 JSON schema for output records |

## 📐 Universal Record Format (V1)
The `v1_universal_schema.json` defines the structure of each parsed record. All scripts that generate or validate records must follow this schema.

### Top-Level Fields:
- `station_id` (string)  
- `timestamp` (ISO 8601 datetime)  
- `parameters`: object of measured values  
- `quality_flag`: object with QN levels (optional)  
- `location`: object with `latitude`, `longitude`, and `elevation_m`  
- `sensor`: optional info about sensors used  
- `metadata`: optional per-field metadata such as `unit`, `description`, `source`  
Each `parameters` key corresponds to a DWD variable code (e.g., `TT_TU` = temperature).  
**Note:** Schema evolution is tracked manually via versioned files (e.g., `v1_universal_schema.json`, `v2_...`).

## 📘 `pdf_description_manual.pretty.json` — Curated Dataset Dictionary
**📍 Path:** `app/features/tools/dwd_pipeline/field_descriptions/pdf_description_manual.pretty.json`

**🧠 Purpose:**  
This file captures a **manually curated summary** of the official dataset descriptions from DWD’s `DESCRIPTION_*.pdf` documents. These PDFs are found in most DWD dataset folders but are hard to parse automatically. Instead of relying on unreliable PDF parsers, this file was created by manually extracting key information and organizing it in JSON format for programmatic use.

**📦 What it contains:**  
For each dataset (e.g., `hourly_air_temperature`), it includes:
- A clear `title` and `dataset_id`
- A list of available `parameters` and their codes
- Detailed descriptions, units, and value formats per parameter
- Quality flag interpretations (e.g., `QN_9 → { 1: "unchecked", 5: "validated" }`)
- Coverage information (e.g., "1961–present"), update cycle notes, and version tags

**📄 Example Structure:**
```json
{
  "hourly_air_temperature": {
    "dataset_id": "obsgermany_climate_hourly_air_temperature",
    "title": "Stündliche Lufttemperatur in 2 m Höhe",
    "parameters": ["TT_TU", "RF_TU", "QN_9"],
    "parameters_detailed": {
      "TT_TU": {
        "description": "Lufttemperatur in 2 m Höhe",
        "unit": "°C",
        "format": "9990.0",
        "type": "number"
      },
      "QN_9": {
        "description": "Qualitätskennzahl für TT_TU und RF_TU",
        "type": "integer",
        "levels": {
          "1": "ungeprüft",
          "5": "geprüft und plausibel"
        }
      }
    },
    "version": "v2024.1",
    "coverage": "1961–present",
    "timestamp_note": "Timestamps refer to end of observation interval"
  }
}
```

**🔧 How it’s used (or will be used):**
- As a **semantic companion** to `field_map.json`
- Planned enrichment of the `metadata` block in parsed records
- Future basis for schema validation or UI explanations

**🧭 When to update:**
- When DWD publishes new dataset versions or updates PDF content

**📌 Status:**
- ✅ Initial version complete
- ⏳ Planned integration into schema generation and record enrichment

## 🧭 Pipeline Overview
1. crawl_dwd.py  
2. download_samples.py  
3. inspect_archives.py  
4. build_station_summary.py  
5. extract_dataset_fields.py  
6. generate_record_schema.py  
7. universal_parser.py 



### 1️⃣ `crawl_dwd.py` — **Repository Discovery**

**🧠 Purpose:**  
This script recursively crawls the public DWD climate repository at:
  `https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/`

Its goals are to:
- Discover all folders and subfolders
- Identify which directories contain data files (e.g., `.zip`, `.txt`, `.pdf`, `.gz`)
- Produce structured outputs for use in downstream steps (such as downloading and inspecting sample datasets)
This is the **first script** in the ClimaStation pipeline. It must be run before any data downloads or metadata extraction occur.

**📥 Input:**
- No local input files required
- Remote input: the live DWD repository URL above

**📤 Output:**
Files are saved in the following folders (created if missing):
| Output File | Location | Description |
|-------------|----------|-------------|
| `*_tree.txt` | `data/1_structure/` | Human-readable tree structure of the DWD climate directory, annotated with file counts and types (e.g. `.zip`, `.pdf`) |
| `*_urls.jsonl` | `data/1_structure/` | One JSON object per folder that contains data, including its URL, file types, and estimated file count |
| `*_structure.json` | `data/1_structure/` | Full JSON representation of the crawled directory tree (folder hierarchy, children, metadata, data file info) |
| `crawl_dwd_debug.log` | `data/0_debug/` | Progress log and crawl diagnostics |

**📦 Required/Created Folders:**
- `data/1_structure/` — for output artifacts  
- `data/0_debug/` — for debug logs
These are created automatically if they do not exist.

**🧭 When to Use:**
- Before downloading: Always run this script before `download_samples.py` so you know which folders contain actual data.
- After DWD updates: Run again to refresh the snapshot if DWD adds or changes datasets.
- For inspection: Use `*_tree.txt` for manual exploration of available folders and files.

**⚙️ Dependencies:**
- `requests` – for HTTP calls  
- `beautifulsoup4` – for parsing HTML directory listings  
- `urllib.parse` – for resolving subfolder URLs  
- `logging`, `datetime`, `json`, `os`, `time`
All dependencies are lightweight and already included in `requirements.txt`.


**💡 Notes:**
- The crawler visits **every subdirectory** under the root, so the process may take several minutes depending on connection and load.
- The output is versioned with timestamps for historical tracking.
- It does **not download any actual data**, only builds a full tree of folder paths and what each folder contains.




### 2️⃣ `download_samples.py` — **Sample Retrieval**

**🧠 Purpose:**  
Downloads a manageable **sample set** of DWD raw archives (`.zip`) and **metadata PDFs** (`DESCRIPTION_*.pdf`) from folders discovered in the previous step.
Helps developers and downstream tools work with a small representative subset of the data.

**📥 Input:**
- `data/1_structure/[timestamp]_urls.jsonl`  
  → List of dataset folders and URLs produced by `crawl_dwd.py`

**📤 Output:**
Files are saved in:
| Output File | Location | Description |
|-------------|----------|-------------|
| `*.zip` | `data/2_samples/raw/` | Up to **2 sample ZIP files** per dataset folder |
| `DESCRIPTION_*.pdf` | `data/2_samples/raw/` | All associated metadata PDFs |
| `downloaded_files.txt` | `data/2_samples/` | Log of downloaded files and their source URLs |
| `download_samples_debug.log` | `data/0_debug/` | Debug info about successes and failures |

**📦 Required/Created Folders:**
- `data/2_samples/raw/` — for downloaded files  
- `data/0_debug/` — for logging  

**🧭 When to Use:**
- After `crawl_dwd.py` has produced `_urls.jsonl`
- When building or testing the archive inspection and schema generation logic
- To limit data volume while maintaining dataset variety

**⚙️ Dependencies:**
- `requests`, `beautifulsoup4`, `urllib.parse`, `glob`, `json`, `os`, `logging`

**💡 Notes:**
- Only downloads **up to 2 ZIP files** per folder to avoid large data volumes  
- PDF files are always downloaded if available  
- Filenames are flattened using the folder prefix to prevent naming conflicts  
  (e.g., `hourly_air_temp_01001.zip`)




  ### 3️⃣ `inspect_archives.py` — **Archive & File Inspection**

**🧠 Purpose:**  
Opens and analyzes all `.zip` archives downloaded in the previous step.  
Its goals are to:
- Classify contents (`raw` vs `metadata`) based on filenames and structure
- Extract structural metadata like headers, sample rows, and station IDs
- Record file coverage and valid date intervals
- Generate summaries for later schema generation and metadata alignment
This is a **required intermediate step** before building field mappings or matching raw data to metadata.

**📥 Input:**
| Input File | Location | Description |
|------------|----------|-------------|
| `*.zip` archives | `data/2_samples/raw/` | From `download_samples.py` |
| `downloaded_files.txt` | `data/2_samples/` | Maps file names to original DWD URLs and folder prefixes |

**📤 Output:**
Files are saved in the following folders:
| Output File | Location | Description |
|-------------|----------|-------------|
| `*_archive_inspection.jsonl` | `data/3_inspection/` | One JSON record per ZIP archive with parsed metadata |
| `*_archive_inspection.pretty.json` | `data/3_inspection/` | Human-readable version of the full archive scan |
| `*_station_and_dataset_summary.pretty.json` | `data/4_summaries/` | Summary of datasets and associated stations/files |
| `inspect_archives_debug.log` | `data/0_debug/` | Logs errors, validation results, and debug messages |

**📦 Required/Created Folders:**
- `data/3_inspection/` — for raw and pretty JSON outputs  
- `data/4_summaries/` — for merged summaries  
- `data/0_debug/` — for logging
All folders are automatically created if missing.

**🧭 When to Use:**
- After `download_samples.py`
- Before generating a schema or parsing any records
- When you want to:
  - Validate the structure of downloaded `.txt` files
  - Extract station IDs and time coverage
  - Distinguish between raw observation files and metadata tables

**⚙️ Dependencies:**
- `zipfile`, `json`, `re`, `datetime`, `logging`, `os`, `collections.defaultdict`
- Local utility functions from `utils.py`:
  - `classify_content()`
  - `extract_dataset_and_variant()`

**💡 Notes:**
- Extracts only the first few lines of each `.txt` file to reduce load  
- Detects mismatches between header and sample row length  
- Recognizes time ranges (`from`, `to`) based on filename patterns  
- Output files are timestamped for tracking



### 4️⃣ `build_station_summary.py` — **Metadata Alignment**

**🧠 Purpose:**  
Aligns raw observation files with their corresponding metadata entries **per station and time interval**.  
It fully or partially parses `.txt` files in metadata ZIPs and uses canonical field mappings to normalize metadata values.

This step generates a complete, merged view of raw-to-metadata connections, forming the **foundation for record generation** and **schema construction**.

**📥 Input:**
| Input File | Location | Description |
|------------|----------|-------------|
| `*_archive_inspection.pretty.json` | `data/3_inspection/` | Full list of raw and metadata files per ZIP archive |
| `*_station_and_dataset_summary.pretty.json` | `data/4_summaries/` | Summary of stations and associated metadata files |
| `*.zip` archives | `data/2_samples/raw/` | Required to read metadata contents for alignment |

**📤 Output:**
| Output File | Location | Description |
|-------------|----------|-------------|
| `station_profile_merged.pretty.json` | `data/5_matching/` | Final merged structure with metadata-matched raw files |
| `station_summary_debug.log` | `data/0_debug/` | Logs matched/unmatched metadata, warnings, and sample structures |

**📦 Required/Created Folders:**
- `data/5_matching/` — for final metadata-aligned profiles  
- `data/0_debug/` — for debug logging

All required folders are created automatically.

**🧭 When to Use:**
- After inspecting archive contents via `inspect_archives.py`
- Before extracting field lists or generating schemas
- When you want to:
  - Match raw observations to valid metadata intervals
  - Preview and normalize metadata fields using a canonical field map

**⚙️ Dependencies:**
- `json`, `zipfile`, `datetime`, `logging`, `pathlib`, `tqdm`
- Local imports:
  - `CANONICAL_FIELD_MAP` and `normalize_metadata_row` from `field_map.py`
  - `match_by_interval` from `utils.py`

**💡 Notes:**
- Fully parses `Metadaten_Parameter_*.txt` to align parameter-specific time ranges  
- Partially parses other metadata files (header + 1 row) just to extract structure  
- Canonical field names are applied to each metadata row  
- Station IDs are validated; files with invalid or missing IDs are skipped  
- Matches are based on **interval overlap** (`from` / `to` dates)




### 5️⃣ `extract_dataset_fields.py` — **Field Inventory**

**🧠 Purpose:**  
Builds a clean inventory of all canonical field names used across each dataset by analyzing:
- Raw field headers
- Matched metadata rows

The result is a structured dictionary `{ dataset_name: [field1, field2, ...] }` which becomes the basis for defining the **Universal Record Schema (V1)**.

**📥 Input:**
| Input File | Location | Description |
|------------|----------|-------------|
| `station_profile_merged.pretty.json` | `data/5_matching/` | Output from `build_station_summary.py`, with raw + metadata field mappings |

**📤 Output:**
| Output File | Location | Description |
|-------------|----------|-------------|
| `dataset_fields.json` | `data/6_fields/` | Clean list of canonical field names grouped by dataset |
| `extract_dataset_fields_debug.log` | `data/0_debug/` | Debug log showing field collection and grouping steps |

**📦 Required/Created Folders:**
- `data/6_fields/` — for the final dataset field list  
- `data/0_debug/` — for debug logs

All folders are created if missing.

**🧭 When to Use:**
- After generating the merged profile in `build_station_summary.py`
- Before generating the full record schema (`generate_record_schema.py`)
- When you want to:
  - Review which fields are used in which datasets
  - Prepare dataset-specific or universal schemas

**⚙️ Dependencies:**
- `json`, `logging`, `pathlib`, `collections.defaultdict`

**💡 Notes:**
- Skips helper fields like `metadata_fields_original` or `metadata_fields_canonical`
- Includes both raw headers and metadata field keys
- Sorts fields alphabetically per dataset for consistency



### 6️⃣ `generate_record_schema.py` — **Schema Generation**

**🧠 Purpose:**  
Generates the official **ClimaStation Universal Record Schema (Version 1)** using JSON Schema format (Draft-07).  
Defines the structure that all parsed records must follow, including required fields and nested components.

This schema acts as a **contract** for downstream processing, validation, and storage.

**📥 Input:**
| Input File | Location | Description |
|------------|----------|-------------|
| `dataset_fields.json` | `data/6_fields/` | Canonical field names grouped by dataset |
| `field_map.json` | `app/features/dwd/record_schemas/` | Maps raw field names to canonical equivalents |

**📤 Output:**
| Output File | Location | Description |
|-------------|----------|-------------|
| `v1_universal_schema.json` | `app/features/dwd/record_schemas/` | Full JSON schema for a single timestamped climate record |
| `generate_record_schema_debug.log` | `data/0_debug/` | Log file for debugging and schema generation steps |

**📦 Required/Created Folders:**
- `app/features/dwd/record_schemas/` — for writing the final schema  
- `data/0_debug/` — for debug output

All folders are created automatically if missing.

**🧭 When to Use:**
- After extracting the dataset field inventory via `extract_dataset_fields.py`
- Before validating or saving parsed records
- When defining API contracts or documentation for downstream consumers

**⚙️ Dependencies:**
- `json`, `logging`, `pathlib`

**💡 Notes:**
- Only includes a basic structure and top-level fields like `station_id`, `timestamp`, `parameters`, `quality_flag`, etc.
- `metadata` field uses static subkeys: `unit`, `beschreibung`, `datenquelle` (to be renamed later)
- Field types are inferred heuristically (e.g., `"timestamp"` → string with `date-time` format)
- Future schema versions may dynamically include parameter-specific metadata keys



## 📝 Open Design Questions and TODOs

- [ ] How should we version schemas? Is `v1_universal_schema.json` enough?
- [ ] The `universal_parser.py` script is not implemented yet. Will it ingest `.txt` into JSON records? Use streaming?
- [ ] Field names in `metadata` are still in German (`beschreibung`, `datenquelle`) → plan for renaming
- [ ] Do we need a database or query engine after schema parsing?
This section can grow over time and serve as a lightweight project management log.
