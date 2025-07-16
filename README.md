ClimaStation: Project Overview

Purpose
ClimaStation is a Germany-focused web platform designed to make DWD's historical climate data easily discoverable and usable. The platform will offer faceted and map-based search, quick previews, and both one-click and custom downloads (CSV, NetCDF, JSON), alongside a RESTful API. It is aimed at both casual users, via interactive charts, and power users, via bulk and automated access, as well as pre-processed summaries like climatologies and anomalies.
At the time of writing, ClimaStation is still in the planning and backend development phase.

Folder Structure and Future Expansion
All DWD data currently resides under data/germany/. As we integrate additional sources (MeteoSwiss, KNMI, NOAA, etc.), each will have its own directory structure (e.g., data/switzerland/, data/netherlands/). Each country's crawler and parser modules will operate independently within their respective folders, keeping logic isolated and the overall structure clear. This approach anticipates the varied formats and metadata conventions of future datasets.

Data Characteristics and Scale
The DWD observations_germany/climate/ repository is substantial. For instance, the 10-minute air temperature dataset alone holds 1,622 ZIP archives (each with ~300,000 lines of raw text). Equivalent volumes exist for hourly, daily, grid fields, and derived parameters. Compressed, these datasets total tens of gigabytes; uncompressed and with added metadata, easily 100 GB+.

Infrastructure Considerations
Initial processing targets a Hetzner CPX21 instance (3 vCPU, 4 GB RAM, 80 GB SSD):
* Streaming & Chunked Parsing: Line-by-line or small-batch reads; avoid loading entire files into memory; output partial JSONL segments.
* Parallel Processing: Utilize multiple processes or threads where possible while managing RAM constraints.
* Efficient PostgreSQL Loading: Use COPY from CSV/JSONL; disable indexes during bulk load; re-index post-import.
For the historic bulk import, temporarily scaling up infrastructure (e.g., 8–16 vCPU, 32 GB RAM) or running multiple CPX21 workers in parallel is recommended. Once the data resides in PostgreSQL, daily updates (from DWD's "now" and "recent" folders) will be far less demanding and easily handled by the base server.

Update Pipeline
As DWD continues to publish new observations, ClimaStation will maintain an incremental update pipeline alongside the historic bulk import. After the initial PostgreSQL load, a lightweight updater will regularly fetch, parse, and insert new "now" and "recent" files. The update cadence (hourly, daily, weekly) will be tuned based on data volume and freshness needs. Temporary infrastructure scaling is an option for parallelizing heavy parsing loads without impacting production.

Automation and Validation
Given this is a solo project (mechatronics engineer leveraging AI, not a dev team), the entire workflow must be fully automated:
* Stream and chunk raw archives without loading entire files.
* Validate checksums and line counts post-parse to ensure 1:1 correspondence with source data.
* Log and alert on mismatches to catch errors early.
Maintaining a complete local copy of DWD files allows for re-validation of parsing logic at any time, ensuring PostgreSQL reflects source data with 100% fidelity.

Core Data Model: Timestamp-Centric Records
ClimaStation’s data model centers on timestamps because DWD’s climate observations are inherently time series data. Each observation links a station ID, a precise timestamp, and measured parameters. This structure offers:
* Natural Alignment with Queries: Supports time-based analysis, exploration, and visualization.
* Uniform Schema: Enables consistent handling across 10-minute, hourly, daily datasets.
* Efficient Storage: Facilitates ingestion, indexing, and querying in PostgreSQL.
* Supports Updates: Accommodates both historic and incremental data additions.
* Minimizes Redundancy: Metadata is stored separately and linked via station_id.
* Facilitates Merging: Simplifies aligning parameters and integrating external datasets.

Parsing Strategy: Grouping by Metadata Validity Intervals
Initially, metadata was attached to each timestamped record. While conceptually simple, this caused severe inefficiencies:
* Parsing slowed due to repeated lookups.
* File sizes ballooned with redundant metadata.
Problem: Metadata validity changes over time. Parameters like air pressure (PP_10) may vary in:
* Units or sensors used
* Time conventions (MEZ, UTC)
* Infrastructure (AFMS2, Messnetz2000)
These changes are recorded in metadata files as date ranges. A single metadata block is valid only within its defined interval.

Solution:
* Group raw data under the metadata block valid for its time range.
* Each output block contains:
- Metadata valid for the interval
- list of timestamped measurements nested inside

Benefits:
* Efficiency: Metadata stored once per interval.
* Accuracy: Measurements tied explicitly to valid metadata.
* Clarity: Changes or gaps in metadata are transparent.
* Future Flexibility: PostgreSQL can reconstruct enriched records via joins on station_id and timestamp range.

Handling Gaps: Raw data and metadata intervals may not align perfectly. Additional intervals are created to explicitly state when no metadata is available.

Manifest Maintenance
We maintain a JSONL manifest at data/germany/dwd_urls.jsonl:
{
  "url": "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/ten-minute/air_temperature/",
  "contains": [".zip", ".txt"],
  "estimated_files": 1622
}
This serves as a snapshot of DWD’s structure and expands as new folders (e.g., hourly, daily, grid) are onboarded.

Dataset Structure
Most DWD datasets follow this convention:
* recent/: Latest complete snapshot (raw data)
* history/: Bulk archive (raw data)
* now/: High-frequency updates (raw data)
* metadata/: Station definitions, schemas, quality flags
Only metadata/ holds descriptive context; others contain raw measurements.

Current Testing Focus
Validation starts with 10_minutes_air_temperature/history. Once reliable, the pipeline will extend to other parameters (hourly, daily, grids).

Coding Conventions
Script Headers: State purpose, input/output paths, usage example.
File Naming:
- Stable filenames unless functionally redefined
- Snake_case for consistency
- Names aligned to primary class/function (e.g., bulk_importer.py)

Folder Structure (VS Code / GitHub)
CLIMASTATION-BACKEND
├── .venv/
├── .vscode/
│   ├── .env
│   ├── launch.json
│   └── settings.json
├── app/
│   ├── config/
│   │   ├── __init__.py
│   │   └── ten_minutes_air_temperature_config.py
│   ├── features/
│   │   ├── dwd/
│   │   │   ├── record_schemas/
│   │   │   │   ├── field_map.json
│   │   │   │   ├── field_map.py
│   │   │   │   └── v1_universal_schema.json
│   │   └── tools/
│   │       └── dwd_pipeline/
│   │           ├── field_descriptions/
│   │           │   └── pdf_description_manual.pretty.json
│   │           └── legacy/
│   │               ├── climastation_data_pipeline.md
│   │               ├── crawl_dwd.py
│   │               ├── download_samples.py
│   │               ├── parse_germany_10_minutes_air_temperature.py
│   │               └── utils.py
│   ├── io_helpers/
│   │   └── zip_handler.py
│   ├── main/
│   │   ├── jsonl_to_pretty_json.py
│   │   ├── parse_10_minutes_air_temperature_akt.py
│   │   ├── parse_10_minutes_air_temperature_hist.py
│   │   └── parse_10_minutes_air_temperature_now.py
│   ├── parsing/
│   │   ├── raw_parser.py
│   │   ├── sensor_metadata.py
│   │   └── station_info_parser.py
│   └── utils/
│       └── logger.py
├── data/
│   ├── 0_debug/
│   │   ├── jsonl_to_pretty_json.debug.log
│   │   └── parse_germany_10_minutes_air_temperature.debug.log
│   ├── 1_crawl_dwd/
│   │   ├── create_dwd_folder_structure.py
│   │   ├── dwd_structure.json
│   │   ├── dwd_tree.txt
│   │   └── dwd_urls.jsonl
│   ├── 2_downloaded_files/
│   └── 3_parsed_files/
├── .gitignore
├── dev_log.md
├── README.md
└── requirements.txt

Included Files and Samples
* parse_10_minutes_air_temperature_hist.py
* raw_parser.py
* sensor_metadata.py
* station_info_parser.py
* zip_handler.py
* logger.py
* ten_minutes_air_temperature_config.py
* jsonl_to_pretty_json.py
* dwd_urls.jsonl

Metadata Samples:
* Metadaten_Geographie_00003.txt
* Metadaten_Geraete_Lufttemperatur_00003.txt
* Metadaten_Geraete_Momentane_Temperatur_In_5cm_00003.txt
* Metadaten_Geraete_Rel_Feuchte_00003.txt
* Metadaten_Parameter_00003.txt
* Metadaten_Stationsname_Betreibername_00003.txt

Raw Data Sample:
* 10minutenwerte_TU_00003_19930428_19991231_hist.zip
* Contains: produkt_zehn_min_tu_19930428_19991231_00003.txt
* First ten rows of text file:
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

Station List Sample:
zehn_min_tu_Beschreibung_Stationen.txt
* First ten rows of text file:
Stations_id von_datum bis_datum Stationshoehe geoBreite geoLaenge Stationsname Bundesland Abgabe
----------- --------- --------- ------------- --------- --------- ----------------------------------------- ---------- ------
00003 19930429 20110331            202     50.7827    6.0941 Aachen                                   Nordrhein-Westfalen                      Frei                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
00044 20070209 20250710             44     52.9336    8.2370 Großenkneten                             Niedersachsen                            Frei                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
00071 20091201 20191231            759     48.2156    8.9784 Albstadt-Badkap                          Baden-Württemberg                        Frei                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
00073 20070215 20250710            374     48.6183   13.0620 Aldersbach-Kramersepp                    Bayern                                   Frei                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
00078 20041012 20250709             64     52.4853    7.9125 Alfhausen                                Niedersachsen                            Frei                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
00091 20020821 20250710            304     50.7446    9.3450 Alsfeld-Eifa                             Hessen                                   Frei                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
00096 20190410 20250710             50     52.9437   12.8518 Neuruppin-Alt Ruppin                     Brandenburg                              Frei                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
00102 20250410 20250710              0     53.8633    8.1275 Leuchtturm Alte Weser                    Niedersachsen                            Frei



**DEVELOPMENT APPROACH DECISION (2025-01-16)**

The ClimaStation pipeline development follows a "Fix-First with Strategic Evaluation" approach rather than immediate scaling to parameter-agnostic processing.

**Phase 1: Foundation Validation** - Audit current parsing bugs against real-world data samples and assess alignment with platform goals. Determine if issues are implementation problems (fixable within current architecture) or fundamental design flaws requiring architectural changes.

**Phase 2: Stabilization Before Scaling** - Fix parsing robustness for 10-minute air temperature data to achieve 100% success rate on representative samples, create comprehensive test suite, and validate output format meets platform requirements.

**Phase 3: Informed Scaling** - Only after achieving a stable, working solution for one parameter (10-minute air temperature), proceed with generalizing the pipeline to become parameter-agnostic across the DWD data ecosystem.

**Rationale**: Scaling a buggy system multiplies problems across parameters. Real-world edge cases and failure modes from the current implementation will inform better abstractions and architectural decisions for the scalable system. One working parameter provides more value than multiple broken ones.

**Current Focus**: Resolve parsing issues with 10-minute air temperature historical data before expanding to other weather parameters or time resolutions.