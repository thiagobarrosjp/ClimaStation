# 🌍 ClimaStation Data Processing Pipeline

**Purpose:**  
Transform fragmented, heterogeneous DWD climate datasets into **clean, timestamp-centric records** enriched with metadata using a modular, schema-first approach.

---

## 🧭 Pipeline Overview

### 1️⃣ `crawl_dwd.py` — **Repository Mapping**

**Purpose:**  
Discover and map the directory structure of the DWD repository and identify folders containing raw or metadata files.

**Input:**  
- DWD repository root  
  `https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/`

**Output:**  
- `data/1_structure/[timestamp]_tree.txt`  
- `data/1_structure/[timestamp]_urls.jsonl`  
- `data/1_structure/[timestamp]_structure.json`  
- Debug: `data/0_debug/crawl_dwd_debug.log`

---

### 2️⃣ `download_samples.py` — **Sample Retrieval**

**Purpose:**  
Download up to 2 `.zip` sample files and relevant `DESCRIPTION_*.pdf` files per dataset folder.

**Input:**  
- `*_urls.jsonl` from `data/1_structure/`

**Output:**  
- `.zip` and `.pdf` files → `data/2_samples/raw/`  
- Download log → `data/2_samples/downloaded_files.txt`  
- Debug: `data/0_debug/download_samples_debug.log`

---

### 3️⃣ `inspect_archives.py` — **Archive & File Inspection**

**Purpose:**  
Open `.zip` archives, extract `.txt` files, classify as raw/metadata, and extract structural metadata.

**Input:**  
- Archives → `data/2_samples/raw/`  
- Mapping → `data/2_samples/downloaded_files.txt`

**Output:**  
- JSONL file → `data/3_inspection/[timestamp]_archive_inspection.jsonl`  
- Pretty version → `data/3_inspection/[timestamp]_archive_inspection.pretty.json`  
- Combined summary → `data/4_summaries/[timestamp]_station_and_dataset_summary.pretty.json`  
- Debug: `data/0_debug/inspect_archives_debug.log`

**Key features:**  
- Classifies raw vs. metadata using filename patterns and content  
- Extracts headers, station ID, and sample rows  
- Extracts `from`/`to` dates (for raw files) from filename  
- Validates:  
  - Station ID format (must be 5-digit numeric)  
  - Header vs. sample row column count

---

### 4️⃣ `build_station_summary.py` — **Metadata Alignment**

**Purpose:**  
Match raw data files to metadata files based on station ID and time interval overlap. Normalize field names using the canonical map.

**Input:**  
- `data/4_summaries/*_station_and_dataset_summary.pretty.json`  
- `data/3_inspection/*_archive_inspection.pretty.json`  
- `data/2_samples/raw/` (archives)  
- Canonical map: `record_schemas/field_map.json`

**Output:**  
- `data/5_matching/station_profile_merged.pretty.json`  
- Debug: `data/0_debug/station_summary_debug.log`

**Key logic:**  
- Builds metadata row intervals per station and parameter  
- Validates metadata rows:  
  - `stations_id` is numeric  
  - `from` and `to` fields are present and well-formed  
  - Header row matches row length  
- **Optimized metadata parsing**:  
  - Parses *all rows* only for `Metadaten_Parameter_*.txt`  
  - For all other metadata files, parses only the header and a single sample row  
  - Logs `[STRUCTURE_SAMPLE]` for these partial parses

---

### 5️⃣ `extract_dataset_fields.py` — **Field Inventory**

**Purpose:**  
Extract all canonical field names across datasets (raw + metadata), grouped by dataset.

**Input:**  
- `data/5_matching/station_profile_merged.pretty.json`

**Output:**  
- `data/6_fields/dataset_fields.json`  
- Debug: `data/0_debug/extract_dataset_fields_debug.log`

**Key logic:**  
- Gathers all `raw_fields_canonical` from each file  
- Adds all metadata keys that occur in matched metadata rows  
- Deduplicates and sorts field names per dataset

---

### 6️⃣ `pdf_description_manual.pretty.json` — **Curated Dataset Dictionary**

**Purpose:**  
Provide a centralized, high-precision description of all DWD datasets using metadata from official `DESCRIPTION_*.pdf` files.

**Input Sources:**  
- Official DWD documentation PDFs (manual extraction)  
- Human-curated format descriptions (unit, type, formatting, missing values)

**Content:**  
Each dataset includes:
- `title`, `citation`, `dataset_id`, `version`, `publication_date`  
- `dataset_description`: processing notes, coverage, etc.  
- `parameters`: brief list of variable names  
- `units`: list of distinct units  
- `timestamp_note`, `qn_levels`: optional metadata  
- `parameters_detailed`: structured block per field, e.g.:

```json
"parameters_detailed": {
  "TT_TU": {
    "description": "Air temperature at 2 m height",
    "unit": "°C",
    "type": "number",
    "format": "9990.0",
    "missing_value": -999
  }
}
