# ClimaStation Backend

The **ClimaStation Backend** is a data processing pipeline designed to collect, normalize, and serve historical and real-time climate data—starting with observational datasets from the **Deutscher Wetterdienst (DWD)**. Its goal is to turn fragmented weather data into a unified, queryable, and analysis-ready format.

---

## 🌍 Purpose

This backend enables:
- Scalable ingestion of climate records (hourly, daily, 10-minute, etc.)
- Conversion into a **universal record format** for downstream use
- Extensible data matching, enrichment, and inspection tools
- Long-term support for multiple data providers (starting with DWD)

---

## 🗂️ Folder Structure

CLIMASTATION-BACKEND/
│
├── app/
│ ├── features/
│ │ ├── dwd/
│ │ │ └── record_schemas/
│ │ │ ├── field_map.json # Canonical raw-to-normalized field mapping
│ │ │ ├── v1_universal_schema.json # JSON schema for all parsed records
│ │ │ └── README.md
│ │ └── tools/
│ │ └── dwd_pipeline/
│ │ ├── crawl_dwd.py # Crawl directory tree from DWD servers
│ │ ├── download_samples.py # Download a sample set of data files
│ │ ├── inspect_archives.py # Inspect archive structure & content
│ │ ├── extract_dataset_fields.py# Extract parameter names from samples
│ │ ├── build_station_summary.py # Summarize metadata per station/dataset
│ │ ├── generate_record_schema.py# Generate schema candidates from fields
│ │ ├── universal_parser.py # Convert raw files to universal format
│ │ ├── utils.py # Shared utilities
│ │ ├── field_descriptions/
│ │ │ └── pdf_description_manual.pretty.json
│ │ └── README.md # Tool-specific documentation
│ └── init.py
│
├── data/
│ ├── 0_debug/ # Debug logs from each script
│ ├── 1_structure/ # Crawled directory trees and URL lists
│ ├── 2_samples/ # Downloaded samples and references
│ ├── 3_inspection/ # Archive content inspection output
│ ├── 4_summaries/ # Per-dataset/station summaries
│ ├── 5_matching/ # Station-dataset merge and ID resolution
│ ├── 6_fields/ # Extracted dataset fields and schema mapping
│ └── README.md # Data folder guide
│
├── tests/
│ └── test_dwd_pipeline.py # Basic unit tests for pipeline logic
│
├── dev_log.md # Development notes and changelog
├── requirements.txt # Python dependencies
├── .env # Local environment variables
├── .gitignore
└── .vscode/
└── settings.json



---

## 🧩 Core Concept: Universal Record Format

All raw datasets are transformed into a shared output structure called the **Universal Record Format**. This guarantees that all downstream tools—analytics, APIs, visualizations—can work with consistent fields.

See: `app/features/dwd/record_schemas/v1_universal_schema.json`

---

## 🚀 Pipeline Overview

Each pipeline step can be executed individually:
1. **Crawl DWD Tree** – `crawl_dwd.py`
2. **Download Samples** – `download_samples.py`
3. **Inspect Archives** – `inspect_archives.py`
4. **Extract Fields** – `extract_dataset_fields.py`
5. **Summarize Metadata** – `build_station_summary.py`
6. **Generate Record Schema** – `generate_record_schema.py`
7. **Parse to Universal Format** – `universal_parser.py`

All scripts produce intermediate debug logs and output files under `data/`.

---

## 📚 Field Reference & Mappings

- `field_map.json` defines how raw headers and metadata are renamed.
- `pdf_description_manual.pretty.json` contains structured field documentation extracted from DWD PDFs.
- `generate_record_schema.py` uses these to produce schema versions per dataset.

---

## 🧪 Testing

Tests are located in `tests/`. Run them via:

```bash
pytest tests/

