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

**Key structures:**  
- `tree_structure` (dict) — nested folder structure  
- `url_records` (list) — dataset folders + file stats

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

**Key logic:**  
- `sanitize_filename()` flattens folder paths into filenames  
- Filters file extensions + extracts representative samples

---

### 3️⃣ `inspect_archives.py` — **Archive & File Inspection**

**Purpose:**  
Open `.zip` archives, extract `.txt` files, classify as raw/metadata, and extract structural metadata.

**Input:**  
- Archives → `data/2_samples/raw/`  
- Mapping → `data/2_samples/downloaded_files.txt`

**Output:**  
- Per-archive JSONL → `data/3_inspection/[timestamp]_archive_inspection.jsonl`  
- Pretty-printed archive report → `..._archive_inspection.pretty.json`  
- Dataset summary → `..._dataset_summary.pretty.json`  
- Station summary → `..._station_summary.pretty.json`  
- Debug: `data/0_debug/inspect_archives_debug.log`

**Key tasks:**  
- Detect classification via filename + content  
- Extract headers, date ranges, and station IDs  
- Group results by dataset and station

---

### 4️⃣ `build_station_summary.py` — **Metadata Alignment**

**Purpose:**  
Match raw files to metadata entries by station ID and overlapping date intervals. Normalize field names.

**Input:**  
- `data/3_inspection/*_station_summary.pretty.json`  
- `data/3_inspection/*_archive_inspection.pretty.json`  
- `data/2_samples/raw/` (archives)  
- `record_schemas/field_map.json`

**Output:**  
- `data/5_matching/station_profile.pretty.json`  
- `data/5_matching/station_profile_canonical.pretty.json`  
- Debug: `data/0_debug/station_summary_debug.log`

**Key logic:**  
- `zip_index`: maps `.txt` → `.zip`  
- `parse_metadata_lines()`: parses and filters metadata  
- `normalize_metadata_row()`: renames fields using `CANONICAL_FIELD_MAP`

---

### 5️⃣ `extract_dataset_fields.py` — **Field Inventory**

**Purpose:**  
Extract all canonical raw and metadata fields per dataset.

**Input:**  
- `data/5_matching/station_profile_canonical.pretty.json`

**Output:**  
- `data/6_fields/dataset_fields.json`  
- Debug: `data/0_debug/extract_dataset_fields_debug.log`

**Key logic:**  
- Scans raw fields and matched metadata keys  
- Groups + deduplicates by dataset

---

## ✅ Summary

The ClimaStation pipeline transforms raw `.zip` files into structured, metadata-rich records. It enables:

- Accurate schema inference
- Sensor-context alignment via metadata
- Schema evolution tracking (`record_schemas/`)
- Dataset-specific field inventories for QA/QC

This pipeline is designed to support:
- Schema validation
- Parsing into unified record formats
- Scaling to other countries' data repositories

---

## 🔧 Suggestions for Refactoring (Next Steps)

### ✅ 1. Consolidate Overlapping Outputs (💡 Priority: Medium)
- Combine `station_profile.pretty.json` + `station_profile_canonical.pretty.json` into a single file with two blocks: `"raw_fields"` and `"canonical_fields"`.
- Merge `dataset_summary.pretty.json` + `station_summary.pretty.json` into one `summary.json`, grouped by dataset → station.

### ✅ 2. Extract Shared Logic (💡 Priority: High)
Create a shared module, e.g. `dwd_pipeline/utils/`, to hold:
- `classify_content()` (from `inspect_archives.py`)
- `extract_dataset_and_variant()`
- `parse_metadata_lines()` and `match_by_interval()`
- Canonical field normalization

This reduces duplication and enables unit testing.

### ✅ 3. Add Early Validation (💡 Priority: Medium)
- Check for valid station ID format (`5 digits`, numeric) early
- Validate raw vs. metadata headers align (same number of fields)
- Warn if expected metadata fields (`stations_id`, `from`, `to`) are missing

### ✅ 4. Optional: Output Previews or Summaries (💡 Later)
- Create compact CSV or Markdown previews of large JSON structures
- Use these for documentation or manual inspection

---

Let me know if you'd like this saved as your `dwd_pipeline/README.md`, or if you want to add a visual pipeline diagram.
