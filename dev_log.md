2025-06-18:  
Today I purchased the domain "climatestation.com" for the platform. I ordered it from www.united-domains.de. 
For the first year I'll pay 12€ and for the second year and beyond it will cost 27€ per year.
This is a summary of the current state of the project:

ClimaStation is a public-facing, academically credible platform designed to provide parsed, clean, and accessible weather station data. 
It begins by processing raw datasets from the Deutscher Wetterdienst (DWD) and aims to expand to include national and international sources over time.
The platform will serve as a foundational infrastructure tool for developers, researchers, educators, and institutions who need high-quality weather and climate data for analysis,
visualization, or integration into other systems.
Unlike existing datasets that are often fragmented, poorly documented, or difficult to parse, ClimaStation focuses on:
- High data usability: ready-to-use formats (CSV, JSON, API endpoints)
- Clean, standardized structure: reliable metadata, station info, and time series
- Scalability: built to grow from Germany to a global dataset scope
- Neutrality and openness: no climate change advocacy, just data
The long-term vision is to make ClimaStation a go-to resource mentioned alongside trusted platforms like Meteostat, Copernicus, or NOAA, offering both depth and simplicity in climate data access.


2025-06-21:  
Backend server setup.  
Provider: Hetzner Cloud  
Location: Nuremberg (Germany)  
Server Type: CPX21 (3 vCPU, 4GB RAM, 80GB SSD)  
OS: Ubuntu 24.04 LTS (64-bit)  
Networking: Public IPv4 & IPv6 enabled  
SSH Access: Enabled via RSA key (climastation)  
Volumes: None  
Firewall: Not yet configured  
Backups: Disabled  
Cloud Config: Not used  
Server Name: climastation-backend  
Status: Running  


2025-06-22:  
Local development environment:
- Installed Python 3 locally on Windows, verified that both pip and venv work.
- Installed Visual Studio Code and configured it for Python development.
- Created the main project folder: climastation-backend.
- Built and tested the first FastAPI app locally using a simple main.py file:

Deployment to Hetzner Cloud:
- Installed Python, pip, and virtualenv on the Hetzner Ubuntu server.
- Created the remote project directory: /var/www/climastation.
- Transferred the local project from Windows to the server using scp with SSH key authentication.
- Set up a Python virtual environment on the server and installed FastAPI and Uvicorn.

Running the server:
- Launched the FastAPI server with public access using:
- uvicorn main:app --host 0.0.0.0 --port 8000
- Verified successful deployment by accessing:
        http://<server-ip>:8000 — root API endpoint
        http://<server-ip>:8000/docs — auto-generated Swagger documentation
Running the server in the background:
- Installed screen on the server to allow background execution.
- Started a named screen session (screen -S climastation), activated the virtual environment, and launched the FastAPI app.
- Detached the session using Ctrl + A, D so the app remains active even after logout.

Replaced `screen` with a `systemd` service to run the FastAPI backend.
- Created system user `climastation` and granted ownership of the project directory.
- Configured `climastation.service` to run the app via Uvicorn from within the virtual environment.
- Fixed an initial path error and resolved a port conflict caused by a previously running process on port 8000.
- Verified that the backend is now running reliably under `systemd` and starts automatically on reboot.


2025-06-23:  
VS Code - Core Plugins for Python  + FastAPI
- Installed and authenticated Windsurf (formerly known as Codeium).
- Installed Continue and linked to OpenAI.
- VS Code configured for formatting, linting, testing.
- Installed Python tooling like black, flake8 and pytest in my local virtual environment (venv).
- Created new API key from OpenAI and linked with Continue (name of API key is "continue-dev").


2025-06-24:  
GitHub Repository created.
- Created a new repository under my GitHub account: thiagobarrosjp/ClimaStation.
- Added and committed the initial README.md file.
- Installed dir-tree to create tree diagrams in VS Code.
- Created and committed the following backend folder structure:
<pre>
CLIMASTATION-BACKEND
    - .vscode/
        -- settings.json
    - app/
        -- core/
            --- __init__.py
            --- config.py
            --- database.py
        -- features/
            --- dwd/
                ---- __init__.py
                ---- dependencies.py
                ---- models.py
                ---- router.py
                ---- schemas.py
                ---- service.py
                ---- utils.py			
            --- __init__.py
        -- __init__.py
        -- main.py
    - tests/
        -- test_dwd_service.py	
    - .env
    - .gitignore
    - README.md
    - requirements.txt
</pre>
FastAPI development:
- Tested FastAPI locally with Uvicorn.
- Encountered an issue where the app loaded but the root endpoint `/` would hang indefinitely.
- Used a temporary `minimal_main.py` file to verify that FastAPI and Uvicorn were working correctly.
- Identified the issue as an empty `router.py` file causing an import failure inside `main.py`.
- Fixed the problem by defining a simple test route in `router.py` and successfully served `/dwd/test`.
- Verified that both the root endpoint (`/`) and the router endpoint (`/dwd/test`) are working correctly.


2025-06-25:  
- Decided on a timestamp-centric record format.
- Record format needs to be flexible and ready to evolve as new parameters, edge cases, or formats appear.
- Record format Version 1 Schema which is saved in file schemas.py :
{
  "station_id": 44,
  "station_name": "Großenkneten",
  "timestamp": "2023-12-23T00:10:00Z",
  "TT_10": 3.3,
  "RF_10": 89.7,
  "quality_flag": 3,
  "location": { "latitude": 52.9336, "longitude": 8.2370, "elevation_m": 44 },
  "sensor": { "TT_10": "PT 100", "RF_10": "EE33" }
}
- Adapted Folder Structure:
<pre>
CLIMASTATION-BACKEND		
    - .vscode/
        -- settings.json
    - app/
        -- core/
            --- __init__.py
            --- config.py
            --- database.py
        -- features/
            --- dwd/
                ---- __init__.py
                ---- dependencies.py
                ---- downloader.py
                ---- parser.py
                ---- models.py
                ---- router.py
                ---- schemas.py
                ---- service.py
                ---- utils.py			
            --- __init__.py        
        -- __init__.py
        -- main.py
    - data/
        -- raw/
        -- processed/        
    - tests/
        -- test_dwd_service.py	
    - .env
    - .gitignore
    - README.md
    - DEVELOPMENT_LOG.md
    - requirements.txt
</pre>
- Decided to start downloading and parsing data from this path:
https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/now/
- Started implementing code for downloader.py to download the .zip files.
- Started implementing code for parser.py to unzip and parse the raw data into the record format.
- Record format described in schemas.py.
- Decided on the upfront parsing strategy. I would download and parse all data upfront, storing results in a readily queryable form.
- The timestamp-centric record format must be designed and optimized to accomodate all current and future data types.
This must happen before I start downloading and parsing raw data en masse.

Next Step:
- Design a timestamp-centric universal record format.
- Collect and classify data samples (1 or 2 files) from each DWD data type.
- Collect from historical and recent sources.
- Classify them by frequency (10min, hourly, etc.), parameters (TT_10, RF_10, etc.), or format variation (column names, deilimiters, encoding)
- Design a record schema that handles the superset.
- Create a record format spec document.


2025-06-26:  
- Extended the folder structure to add a recurring DWD repository crawler as a built-in feature.
<pre>
CLIMASTATION-BACKEND		
    - .vscode/
        -- settings.json
    - app/
        -- core/
            --- __init__.py
            --- config.py
            --- database.py
        -- features/
            --- dwd/
                ---- __init__.py
                ---- dependencies.py
                ---- downloader.py
                ---- parser.py
                ---- models.py
                ---- router.py
                ---- schemas.py
                ---- service.py
                ---- utils.py			
            --- __init__.py
        -- tools/
            --- dwd_crawler/
                ---- __init__.py
                ---- crawl_dwd.sh
                ---- README.md
        -- __init__.py
        -- main.py
    - data/
        -- raw/
        -- processed/
        -- dwd_structure_logs/
            --- [timestamp]_tree.txt
            --- [timestamp]_urls.txt
    - tests/
        -- test_dwd_service.py	
    - .env
    - .gitignore
    - README.md
    - DEVELOPMENT_LOG.md
    - requirements.txt
</pre>
- Installed Git Bash.
- Installed Scoop and used that to install wget.

2025-06-27:  
- Replaced the original shell-based crawler (`crawl_dwd.sh`) with a Python-based crawler (`crawl_dwd.py`) that recursively identifies DWD folders containing raw `.zip` or `.gz` datasets.  
- The crawler is now restricted to the official DWD climate data path: `/climate_environment/CDC/observations_germany/`, avoiding unrelated datasets like CAP alerts or metadata directories.  
- Output is a clean, timestamped folder list saved to `data/dwd_structure_logs/`.  
- Updated folder structure to reflect the new crawler design and upcoming automation:  
<pre>
CLIMASTATION-BACKEND		
    - .vscode/
        -- settings.json
    - app/
        -- core/
            --- __init__.py
            --- config.py
            --- database.py
        -- features/
            --- dwd/
                ---- __init__.py
                ---- dependencies.py
                ---- downloader.py
                ---- models.py
                ---- parser.py
                ---- record_format_spec.md  
                ---- models.py
                ---- router.py
                ---- schemas.py
                ---- service.py
                ---- utils.py			
            --- __init__.py
        -- tools/
            --- dwd_crawler/
                ---- __init__.py  
                ---- analyze_samples.py  
                ---- crawl_dwd.py  
                ---- download_samples.py  
                ---- README.md  
                ---- record_validator.py                  
            --- schema_pipeline/
        -- __init__.py
        -- main.py
    - data/
        -- raw/
        -- processed/
        -- dwd_structure_logs/
            --- [timestamp]_tree.txt
            --- [timestamp]_urls.txt
        -- dwd_structure_samples/
            --- [folder_name_1]/
            --- [folder_name_2]/
    - tests/
        -- test_dwd_service.py	
    - .env
    - .gitignore
    - README.md
    - DEVELOPMENT_LOG.md
    - requirements.txt
</pre>
  
Next Step: Automate the sample–analyze–adapt pipeline  
- Download representative samples from each identified folder  
- Unzip and extract the raw files  
- Parse header and data rows  
- Attempt to map parsed rows to the current `AirTemperatureRecord`  
- Log whether each sample fits the schema or highlights structural mismatches  
- Use this process to iteratively evolve a universal timestamp-centric record format compatible with all relevant DWD datasets

2025-06-28:  
- Started implementing and testing a pipeline that automates the sample–analyze–adapt process.  
- Added components to parse and validate header and data rows, as well as metadata.  
- Downloaded one sample from each folder and extracted the raw files.  
- However the generated record formats were not matching the current schema.  
- I also noticed that the pipeline was not parsing and validating the metadata correctly.  
- The downloaded samples exceeded the file size limit from GitHub and I ran into difficulties pushing the changes to the repository.  
- Many changes from today and yesterday had to be reverted.  


2025-06-29:  
- Changed the approach to develop and implement the automated pipeline.  
- We are now focusing on implementing the pipeline for only this small part of the DWD repository:  
https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/  
- We removed every folder and file that is not necessary to implement this new version of the pipeline.
<pre>
CLIMASTATION-BACKEND		
    - .vscode/
        -- settings.json
    - app/
        -- features/
            --- dwd/
                ---- __init__.py
                ---- downloader.py
                ---- parser.py
                ---- record_format_spec.md  
                ---- schemas.py		
            --- __init__.py
        -- __init__.py
    - data/  
        -- air_temperature_10min/
            --- docs/  
            --- historical/  
            --- meta_data/  
            --- now/  
            --- recent/  
    - tests/
        -- test_air_temperature_parser.py	
    - .env
    - .gitignore
    - README.md
    - DEVELOPMENT_LOG.md
    - requirements.txt
</pre>
- Created downloader.py from scratch. This time it will only download a small subset of files from this DWD path:  
https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/  
- To be specific, we downloaded one or two zip files from the raw data foldrs as well as from the metadata folder.  
- Updated record_format_spec.md to reflect the changes to the pipeline.  
- Created new version of parser.py to unzip, extract text files, read and parse the first few rows to the record format defined in schemas.py.  
- Record format in schemas.py was also updated.
- Updated .gitignore to avoid uploading unnecessary data to GitHub.  
- Created parse_station_metadata.py to parse the metadata files from the folder defined above.  
- Parsed metadata is stored in data/air_temperature_10min/station_air_temperature_metadata.json.  

2025-06-30:
- Unfortunately, I had to revert the progress we made in the last few days, again, due to problems with GitHub.  
- After a bit of brainstorming, we decided again to describe the iterative process to design the universal record format.  
- Step 1: Discovery Phase  
- Use the crawler to identify all relevant folders across the DWD repository.  
- Download one or two representative files (raw + metadata) from each folder.  
- Focus on broad variation coverage: frequencies, parameters, formats, encoding.  
- Step 2: Validation Loop  
- Attempt to parse each sample into the current version of the record schema.  
- Log parsing results (successfully mapped fields, missing or unmatched fields, format/encoding inconsistencies).  
- Iteratively evolve the schema (add new fields or nested structures, refactor field types or units, make fields optional if incosistently present).  
- Step 3: Stability Check  
- When all representative samples parse without critical mismatches, freeze the schema.  
- This frozen schema becomes the universal record format (Version 1).  
- From this point, large-scale parsing and storage can begin.  
- Additional Notes:  
- Schema versions are stored in a dedicated folder (record_schemas/) to track evolution over time.  
- This approach ensures backward- and forward-compatibility and avoids costly re-parsing.  
- This new structure is the starting point for the iterative pipeline.
<pre>
CLIMASTATION-BACKEND		
    - .vscode\
        -- settings.json
    - app\
        -- features\
            --- dwd\  
                ---- record_schemas\ 
                    ----- README.md   
                    ----- v0_initial_schema.json  
                ---- __init__.py
                ---- metadata_parser.py  
                ---- record_validator.py  
                ---- schemas.py	            
            --- tools\
                ---- dwd_crawler\ 
                    ---- __init__.py 
                    ---- analyze_samples.py                       
                    ---- crawl_dwd.py  
                    ---- download_samples.py   
                    ---- inspect_archives.py                      
                    ---- README.md                      
            --- __init__.py  
        -- __init__.py
    - data\          
        -- dwd_structure_logs\  
            --- [timestamp]_structure.json  
            --- [timestamp]_tree.txt  
            --- [timestamp]_urls.jsonl  
        -- dwd_validation_logs\ 
            --- [timestamp]_archive_inspection.jsonl  
            --- [timestamp]_archive_inspection.pretty.json  
        -- raw\      
        -- README.md       
    - tests\
        -- test_dwd_pipeline.py	
    - .env
    - .gitignore
    - DEVELOPMENT_LOG.md  
    - README.md  
    - requirements.txt
</pre>


2025-07-01:  
- Pipeline restricted to the DWD path:  
https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/    
- crawl_dwd.py has been modified to create a tree structure of the DWD repository.  
- download_samples.py has been created from scratch and it only download samples of zip files.  
- inspect_archives.py has been created from scratch. This file outputs one [timestamp]_archive_inspection.jsonl file and one [timestamp]_archive_inspection.pretty.json file.  
- Adapted inspect_archives.py so that the output identifies the dataset and the variant of each zip file. Besides, each text file classified as metadata or raw data.  
- Created the concept of a dataset_key, joining dataset and classification.  
- Adapted inspection_archives.py so that the output includes the dataset_key. 
- The output of inspect_archives.py also includes [timestamp]_dataset_summary.pretty.json and [timestamp]_station_summary.pretty.json files.  
- The example below shows how the output of inspect_archives.py looks like.
<pre>  
{
    "zip_file": "10_minutes_air_temperature_historical_10minutenwerte_TU_00003_19930428_19991231_hist.zip",
    "source_url_path": "10_minutes/air_temperature/historical",
    "dataset": "10_minutes_air_temperature",
    "variant": "historical",
    "entries": [
      {
        "filename": "produkt_zehn_min_tu_19930428_19991231_00003.txt",
        "lines": 3,
        "header": "STATIONS_ID;MESS_DATUM;QN;PP_10;TT_10;TM5_10;RF_10;TD_10",
        "sample_row": "3;199304281230;    1;  987.3;  24.9;  28.4;  23.0;   2.4",
        "dataset": "10_minutes_air_temperature",
        "classification": "raw",
        "dataset_key": "10_minutes_air_temperature_raw",
        "station_id": "3"
      }
    ]
  },
  {
    "zip_file": "10_minutes_air_temperature_historical_10minutenwerte_TU_00003_20000101_20091231_hist.zip",
    "source_url_path": "10_minutes/air_temperature/historical",
    "dataset": "10_minutes_air_temperature",
    "variant": "historical",
    "entries": [
      {
        "filename": "produkt_zehn_min_tu_20000101_20091231_00003.txt",
        "lines": 3,
        "header": "STATIONS_ID;MESS_DATUM;  QN;PP_10;TT_10;TM5_10;RF_10;TD_10;eor",
        "sample_row": "3;199912312300;    1;  997.3;   4.1;   3.6;  87.0;   2.1;eor",
        "dataset": "10_minutes_air_temperature",
        "classification": "raw",
        "dataset_key": "10_minutes_air_temperature_raw",
        "station_id": "3"
      }
    ]
  },
  - The example below shows how the [timestamp]_dataset_summary.pretty.json file looks like.  
  "3": {
    "10_minutes_air_temperature": {
      "10_minutes_air_temperature_raw": [
        "produkt_zehn_min_tu_19930428_19991231_00003.txt",
        "produkt_zehn_min_tu_20000101_20091231_00003.txt"
      ],
      "10_minutes_air_temperature_metadata": [
        "Metadaten_Geographie_00003.txt",
        "Metadaten_Geraete_Lufttemperatur_00003.txt",
        "Metadaten_Geraete_Momentane_Temperatur_In_5cm_00003.txt",
        "Metadaten_Geraete_Rel_Feuchte_00003.txt",
        "Metadaten_Parameter_00003.txt",
        "Metadaten_Stationsname_Betreibername_00003.txt"
      ]
    },
</pre>
2025-07-02:  
- Fixed a bug in download_samples.py that prevented [timestamp]_archive_inspection.jsonl from being loaded.  
- Important note: each raw data file may correlate with multiple rows from each metadata file, based on timestamps and overlaps.  
- The goal of the station_summary.pretty.json is to allow to map the raw data files to the metadata files.  
- This mapping will become the foundation for automatic parsing and conversion.
- Instead of checking metadata row by row during parsing, we pre-split the metadata into time intervals, then we apply them in batches.
- Step-by-step strategy:
-- Before parsing the raw file, we build a sorted list of time intervals based on metadata.
-- Then we parse the raw data file line by line, but we only check metadata when the timestamp enters a new interval.
-- We also maintain a current active metadata block and we apply this block to all rows until the timestamp exceeds the interval.
-- The sorted list of time intervals (derived from the metadata) should ideally live in memory, and be constructed before parsing begins, as a per-station, per-metadata-type dictionary.  
- We should build and store the metadata intervals in the station_summary.pretty.json file. Then, we load them into memory once at the beginning of each parsing job.  
- Here are a points that can be improved in the station_summary.json file:
-- Time interval indexing: For each raw file, we can eventually annotate the covered time interval like the example below:
<pre>
        "produkt_zehn_min_tu_20000101_20091231_00003.txt": {
        "from": "2000-01-01",
        "to": "2009-12-31"
        }
-- Metadata row matching: Instead of just listing metatada filenames, we could match which rows of those metadata files apply to each raw file (based on date). For example:
        "Metadaten_Geographie_00003.txt": [
        {
            "from": "2000-01-01",
            "to": "2008-10-07",
            "latitude": 50.7827,
            "longitude": 6.0941,
            "elevation": 202.0
        },
        {
            "from": "2008-10-08",
            "to": "2009-12-31",
            "latitude": 50.7827,
            "longitude": 6.0941,
            "elevation": 202.0
        }
        ]
</pre>
-- Reverse lookup map: We can precompute this code below. This would speed up inspection or linking operations, especially if we process thousands of files in batch jobs.
<pre>
        "produkt_zehn_min_tu_20000101_20091231_00003.txt": {
        "station": "3",
        "dataset": "10_minutes_air_temperature"
        }
</pre>
-- Standardized field lists: We can add parsed fields from the header like this:  
<pre>
"fields": ["STATIONS_ID", "MESS_DATUM", "QN", "PP_10", "TT_10", ...]  
</pre>
-- Optional field "station_name": We already have this in sample rows. We could extract once and include for clarity:
<pre>       
"station_name": "Aachen"
</pre>
- Updated inspect_archives.py to add "from" and "to" timestamps from the raw data files in the station_summary.json file.  
- New file build_station_summary.py to unload functions from inspect_archives.py and add new features.  
- Suggested Rule of Thumb: If it doesn't require opening and inspecting archive structure of filenames, it doesn't belong in inspect_archives.py.  
- Suggested responsibilities: inspect_archives.py detects structure, saves raw inspection summaries. build_station_summary.py parses content of metadata files and aligns to raw data temporally.  
- Important observation: The file Metadaten_Parameter_xxxxx.txt is not a simple list of time intervals for a station, but a multi-dimensional table, where each parameter (e.g. TT_10, RF_10, PP_10) has its own independent timeline and metadata.  
- Any attempt to attach a single parameterbeschreibung, einheit, or datenquelle to a raw file based on time range alone will fail. Because there may be multiple parameters, each with their own intervals, overlapping within the same file.  
- This requires a change in logic. Instead of matching just by time range per raw file, we must group the metadata entries by parameter. Then for each raw file, we look at its header and for each parameter in the raw file, we find the time-relevant metadata row. Finally, we match these parameter-specific metadata blocks separately.
- I've noticed that the build_station_summary.py is not parsing metadata files correctly. I will look into this.
- This is the current folder structure:
<pre>
CLIMASTATION-BACKEND		
    - .vscode\
        -- settings.json
    - app\
        -- features\
            --- dwd\  
                ---- record_schemas\ 
                    ----- README.md   
                    ----- v0_initial_schema.json  
                ---- __init__.py
                ---- metadata_parser.py  
                ---- record_validator.py  
                ---- schemas.py	            
            --- tools\
                ---- dwd_crawler\ 
                    ---- __init__.py 
                    ---- build_station_summary.py                       
                    ---- crawl_dwd.py  
                    ---- download_samples.py   
                    ---- inspect_archives.py                      
                    ---- README.md                      
            --- __init__.py  
        -- __init__.py
    - data\          
        -- dwd_structure_logs\  
            --- [timestamp]_structure.json  
            --- [timestamp]_tree.txt  
            --- [timestamp]_urls.jsonl  
        -- dwd_validation_logs\ 
            --- [timestamp]_archive_inspection.jsonl  
            --- [timestamp]_archive_inspection.pretty.json  
            --- [timestamp]_dataset_summary.pretty.json  
            --- [timestamp]_station_summary.pretty.json  
            --- station_profile.pretty.json  
        -- raw\  
            --- downloaded_files.txt   
        -- README.md       
    - tests\
        -- test_dwd_pipeline.py	
    - .env
    - .gitignore
    - DEVELOPMENT_LOG.md  
    - README.md  
    - requirements.txt
</pre>
- The output from build_station_summary.py seems to be correct.
- Important observation: The union of all fields from the raw data header and the matched metadata fields from all relevant files forms the complete record format for that specific raw data file.  
- Repeating this process for all raw files will reveal all unique field names that ever appear. This approach allow us to derive empirically the universal record schema for the DWD dataset. 
- We are now expanding the folder structure to include a canonical mapping dictionary.  
- This dictionary will act as a translation layer to clean up the raw metadata field names and unify them under a consistent schema. 
- The map will be created using a two-level strategy. 
- First, we manually create a static core map with the known fields. This file is called field_map.json.  
- Then we implement an automatic method to identify and log unknown fields. These would be manually added to field_map.json or ignored.  
- The current folder structure:
<pre>
CLIMASTATION-BACKEND		
    - .vscode\
        -- settings.json
    - app\
        -- features\
            --- dwd\  
                ---- record_schemas\ 
                    ----- README.md   
                    ----- field_map.json  
                    ----- field_map.py  
                    ----- v0_initial_schema.json  
                ---- __init__.py
                ---- metadata_parser.py  
                ---- record_validator.py  
                ---- schemas.py	            
            --- tools\
                ---- dwd_crawler\ 
                    ---- __init__.py 
                    ---- build_station_summary.py                       
                    ---- crawl_dwd.py  
                    ---- download_samples.py   
                    ---- extract_dataset_fields.py  
                    ---- inspect_archives.py                      
                    ---- README.md                      
            --- __init__.py  
        -- __init__.py
    - data\          
        -- dwd_structure_logs\  
            --- [timestamp]_structure.json  
            --- [timestamp]_tree.txt  
            --- [timestamp]_urls.jsonl  
        -- dwd_validation_logs\ 
            --- [timestamp]_archive_inspection.jsonl  
            --- [timestamp]_archive_inspection.pretty.json  
            --- [timestamp]_dataset_summary.pretty.json  
            --- [timestamp]_station_summary.pretty.json  
            --- station_profile_canonical_pretty.json  
            --- dataset_fields.json  
            --- station_profile.pretty.json  
            --- station_summary_debug.log  
        -- raw\  
            --- downloaded_files.txt   
        -- README.md       
    - tests\
        -- test_dwd_pipeline.py	
    - .env
    - .gitignore
    - DEVELOPMENT_LOG.md  
    - README.md  
    - requirements.txt
</pre>

2025-07-03:
- Major restructuring of the pipeline for improved modularity and traceability.
- Created a new folder structure (`data/0_debug` to `data/6_fields`) that reflects the pipeline stages more clearly. Each processing script now stores its outputs and debug logs in well-defined locations.
- All relevant scripts (`crawl_dwd.py`, `download_samples.py`, `inspect_archives.py`, `build_station_summary.py`, `extract_dataset_fields.py`) were updated to:
  - Save outputs in their corresponding numbered folder.
  - Create and overwrite their own debug logs (e.g. `crawl_dwd_debug.log`) to ease troubleshooting.
- Updated `crawl_dwd.py` to:
  - Skip the root node and simplify tree structure serialization.
  - Write debug information to a dedicated log file.
  - Fix minor bugs in top-level folder crawling.
- Updated `download_samples.py` to:
  - Save samples and download logs under `data/2_samples/`.
  - Include its own debug log for download failures.
- Updated `inspect_archives.py` to:
  - Save all outputs under `data/3_inspection/` (bugfix: previously summary files were saved in the wrong folder).
  - Use structured debug logging for better error tracking.
- Updated `build_station_summary.py` to:
  - Write to `data/5_matching/`.
  - Add detailed debugging of the metadata matching process.
- Updated `extract_dataset_fields.py` to:
  - Save dataset field inventory in `data/6_fields/`.
  - Add debug logging (e.g. missing field names, unusual structure).
- README.md inside `dwd_pipeline/` was rewritten and improved, including documentation for each script and a summary of next goals.

### ✅ What Worked Well:
- Debug logs helped uncover early-stage bugs faster (especially crawl and metadata matching issues).
- New folder structure makes it easier to keep outputs organized and understand the flow of the pipeline.

---

### 📌 Suggested Next Steps for 2025-07-04:
1. **Consolidate overlapping files**:
   - Merge `station_profile.pretty.json` and `station_profile_canonical.pretty.json`.
   - Consider combining `dataset_summary` and `station_summary` into one grouped file.
2. **Extract reusable utilities**:
   - Move duplicate logic (e.g. `classify_content`, date matching, zip reading) into a new `dwd_pipeline/utils/` module.
3. **Add validation checks**:
   - Validate station IDs, header alignment, and field consistency earlier in the pipeline (during inspection or metadata parsing).
4. **Rename raw sample files to preserve original folder structure** more clearly (consider reversing prefix or using slugified subfolder paths).
5. **Revisit logging**:
   - Introduce logging levels to filter critical/debug/info messages easily.
   - Add summary stats at the end of each debug file (e.g. # files parsed, # matched, # skipped).
6. **Optional:** Add CLI arguments to each script to control output paths, timestamp usage, or debug verbosity.

 
- The folder structure was changed:
- The current folder structure:
<pre>
CLIMASTATION-BACKEND		
    - .vscode\
        -- settings.json
    - app\
        -- features\
            --- dwd\  
                ---- record_schemas\ 
                    ----- README.md   
                    ----- field_map.json  
                    ----- field_map.py  
                    ----- v0_initial_schema.json  
                ---- __init__.py
                ---- metadata_parser.py  
                ---- record_validator.py  
                ---- schemas.py	            
            --- tools\
                ---- dwd_pipeline\ 
                    ---- __init__.py 
                    ---- build_station_summary.py                       
                    ---- crawl_dwd.py  
                    ---- download_samples.py   
                    ---- extract_dataset_fields.py  
                    ---- inspect_archives.py                      
                    ---- README.md                      
            --- __init__.py  
        -- __init__.py
    - data\          
        -- 0_debug\  
            --- crawl_dwd_debug.log  
            --- download_samples_debug.log  
            --- extract_dataset_fields_debug.log  
            --- inspect_archives_debug.log  
            --- station_summary_debug.log    
        -- 1_structure\    
            --- [timestamp]_structure.json  
            --- [timestamp]_tree.txt  
            --- [timestamp]_urls.jsonl    
        -- 2_samples\    
            --- raw\
            --- downloaded_files.txt              
        -- 3_inspection\ 
            --- [timestamp]_archive_inspection.jsonl
            --- [timestamp]_archive_inspection.pretty.json
        -- 4_summaries\  
            --- [timestamp]_dataset_summary.pretty.json
            --- [timestamp]_station_summary.pretty.json
        -- 5_matching\  
            --- station_profile_canonical.pretty.json  
            --- station_profile.pretty.json  
        -- 6_fields  
            --- dataset_fields.json
        -- README.md       
    - tests\
        -- test_dwd_pipeline.py	
    - .env
    - .gitignore
    - dev_log.md  
    - README.md  
    - requirements.txt
</pre>

- 2025-07-04:  
- Merge station_profile.pretty.json and station_profile_canonical.pretty.json into one file with two keys: "raw_fields" and "canonical_fields".  
- Merge dataset_summary.pretty.json and station_summary.pretty.json into one station_and_dataset_summary.pretty.json file.
- Added utils.py module to `dwd_pipeline/` to contain reusable functions for inspecting archives, extracting dataset fields, and matching metadata.
- Updated `build_station_summary.py` to sample only the header and one row from all metadata files **except** for `Metadaten_Parameter_*.txt`, which are still fully parsed.
- Introduced `[COMMENTARY]` and `[STRUCTURE_SAMPLE]` log labels in `station_summary_debug.log` to distinguish ignorable rows from actual warnings.
- Dramatically reduced log file size and improved parsing performance for large metadata archives.
- The current folder structure:
<pre>
CLIMASTATION-BACKEND		
    - .vscode\
        -- settings.json
    - app\
        -- features\
            --- dwd\  
                ---- record_schemas\ 
                    ----- README.md   
                    ----- field_map.json  
                    ----- field_map.py  
                    ----- v0_initial_schema.json  
                ---- __init__.py
                ---- metadata_parser.py  
                ---- record_validator.py  
                ---- schemas.py            
            --- tools\
                ---- dwd_pipeline\ 
                    ---- __init__.py 
                    ---- build_station_summary.py                       
                    ---- crawl_dwd.py  
                    ---- download_samples.py   
                    ---- extract_dataset_fields.py  
                    ---- inspect_archives.py                      
                    ---- README.md     
                    ---- utils.py                   
            --- __init__.py  
        -- __init__.py
    - data\          
        -- 0_debug\  
            --- crawl_dwd_debug.log  
            --- download_samples_debug.log  
            --- extract_dataset_fields_debug.log  
            --- inspect_archives_debug.log  
            --- station_summary_debug.log    
        -- 1_structure\    
            --- [timestamp]_structure.json  
            --- [timestamp]_tree.txt  
            --- [timestamp]_urls.jsonl    
        -- 2_samples\    
            --- raw\
            --- downloaded_files.txt              
        -- 3_inspection\ 
            --- [timestamp]_archive_inspection.jsonl
            --- [timestamp]_archive_inspection.pretty.json
        -- 4_summaries\  
            --- [timestamp]_station_and dataset_summary.pretty.json  
        -- 5_matching\  
            --- station_profile.merged.pretty.json  
        -- 6_fields  
            --- dataset_fields.json
        -- README.md       
    - tests\
        -- test_dwd_pipeline.py	
    - .env
    - .gitignore
    - dev_log.md  
    - README.md  
    - requirements.txt


- 2025-07-05:  
- Deprecated `extract_pdf_metadata.py` and all its output files (`description_metadata.json`, `dataset_info.json`, `field_schema_extended.json`).
- Manually curated a new reference file: `pdf_description_manual.pretty.json` located at `tools/dwd_pipeline/field_descriptions/`
  - Contains structured metadata extracted from official DWD `DESCRIPTION_*.pdf` files
  - Includes: title, citation, dataset ID, version, publication date, parameters, timestamp format, QN-levels, and description summary
- Determined that many fields like quality flags, timestamp formats, and temporal coverage are common across datasets and don’t need individualized parsing
- Validated 30+ DWD PDF files manually and excluded 4 special cases that don’t follow the standard structure (multi-annual mean datasets and subdaily template)
- Created pdf_description_manual.pretty.json. 
- Updated `README.md` in `tools/dwd_pipeline/` to reflect this final schema approach
- The current folder structure:
<pre>
CLIMASTATION-BACKEND		
    - .vscode\
        -- settings.json
    - app\
        -- features\
            --- dwd\  
                ---- record_schemas\ 
                    ----- field_map.json  
                    ----- field_map.py  
                    ----- README.md  
                    ----- v1_universal_schema.json  
                ---- __init__.py
                ---- metadata_parser.py  
                ---- record_validator.py  
                ---- schemas.py            
            --- tools\
                ---- dwd_pipeline\ 
                    ---- field_descriptions\
                        ----- pdf_description_manual.pretty.json
                    ---- __init__.py 
                    ---- build_station_summary.py                       
                    ---- crawl_dwd.py  
                    ---- download_samples.py   
                    ---- extract_dataset_fields.py  
                    ---- inspect_archives.py                      
                    ---- README.md     
                    ---- utils.py                   
            --- __init__.py  
        -- __init__.py
    - data\          
        -- 0_debug\  
            --- crawl_dwd_debug.log  
            --- download_samples_debug.log  
            --- extract_dataset_fields_debug.log  
            --- inspect_archives_debug.log  
            --- station_summary_debug.log    
        -- 1_structure\    
            --- [timestamp]_structure.json  
            --- [timestamp]_tree.txt  
            --- [timestamp]_urls.jsonl    
        -- 2_samples\    
            --- raw\
            --- downloaded_files.txt              
        -- 3_inspection\ 
            --- [timestamp]_archive_inspection.jsonl
            --- [timestamp]_archive_inspection.pretty.json
        -- 4_summaries\  
            --- [timestamp]_station_and dataset_summary.pretty.json  
        -- 5_matching\  
            --- station_profile.merged.pretty.json  
        -- 6_fields  
            --- dataset_fields.json
        -- README.md       
    - tests\
        -- test_dwd_pipeline.py	
    - .env
    - .gitignore
    - dev_log.md  
    - README.md  
    - requirements.txt
</pre>


- 2025-07-06:
- Struggled a lot trying to automate the pipeline to create a decend record format. But the AI assistant is driving me crazy.
- I managed to make the AI manually create a parsed JSON record from the raw data, enriched with metadata. It is not looking very good, but it's a start. I need to improve this record and work backwards from here. 
- This is the example we have so far:
<pre>
{
  "station_id": "00003",
  "station_name": "Aachen",
  "timestamp": "1993-04-28T12:30:00",
  "dataset": "10_minutes_air_temperature",
  "variant": "historical",
  "parameters": {
    "air_pressure_hpa": 987.3,
    "air_temperature_C": 24.9,
    "5min_mean_temperature_C": 28.4,
    "relative_humidity_percent": 23.0,
    "dew_point_temperature_C": 2.4
  },
  "quality_flag": {
    "quality_flag": 1
  },
  "location": {
    "latitude": 50.7827,
    "longitude": 6.0941,
    "station_altitude_m": 202.0
  },
  "sensor": {
    "air_temperature_C": {
      "device_type": "PT 100 (Luft)",
      "sensor_height_above_ground_m": 2.0,
      "measurement_method": "Temperaturmessung, elektr."
    },
    "5min_mean_temperature_C": {
      "device_type": "PT 100 (Luft)",
      "sensor_height_above_ground_m": 0.05,
      "measurement_method": "Temperaturmessung, elektr."
    },
    "relative_humidity_percent": {
      "device_type": "HYGROMER MP100",
      "sensor_height_above_ground_m": 2.0,
      "measurement_method": "Feuchtemessung, elektr."
    },
    "air_pressure_hpa": null
  },
  "metadata": {
    "air_pressure_hpa": {
      "unit": "hPa",
      "parameter_description": "Luftdruck in Stationshoehe der voran. 10 min",
      "data_source": "10-Minutenwerte von automatischen Stationen der 1. Generation (MIRIAM/AFMS2, ESAU-Daten bis 31.12.1999 (Zeitbezug ist MEZ)",
      "time_note": "HHMM MEZ"
    },
    "air_temperature_C": {
      "unit": "°C",
      "parameter_description": "momentane Lufttemperatur in 2m Hoehe",
      "data_source": "10-Minutenwerte von automatischen Stationen der 1. Generation (MIRIAM/AFMS2, ESAU-Daten bis 31.12.1999 (Zeitbezug ist MEZ)",
      "time_note": "HHMM MEZ"
    },
    "5min_mean_temperature_C": {
      "unit": "°C",
      "parameter_description": "Momentane Temperatur in 5 cm Hoehe 10min",
      "data_source": "10-Minutenwerte von automatischen Stationen der 1. Generation (MIRIAM/AFMS2, ESAU-Daten bis 31.12.1999 (Zeitbezug ist MEZ)",
      "time_note": "HHMM MEZ"
    },
    "relative_humidity_percent": {
      "unit": "%",
      "parameter_description": "relative Feucht in 2m Hoehe",
      "data_source": "10-Minutenwerte von automatischen Stationen der 1. Generation (MIRIAM/AFMS2, ESAU-Daten bis 31.12.1999 (Zeitbezug ist MEZ)",
      "time_note": "HHMM MEZ"
    },
    "dew_point_temperature_C": {
      "unit": "°C",
      "parameter_description": "Taupunkttemperatur in 2m Hoehe",
      "data_source": "10-Minutenwerte von automatischen Stationen der 1. Generation (MIRIAM/AFMS2, ESAU-Daten bis 31.12.1999 (Zeitbezug ist MEZ)",
      "time_note": "HHMM MEZ"
    }
  }
}
</pre>

- 2025-07-07:
- Created the v1_universal_schema.json manually and used it to create the example below.
- A few comments on the example:
-- description: value taken from the field "title" in pdf_description_manual.pretty.json.
-- state: value listed in the text file in this path:
https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/historical/zehn_min_tu_Beschreibung_Stationen.txt

<pre>
{
  "station": {
    "station_id": "00003",
    "station_name": "Aachen"
  },
  "dataset": {
    "name": "10_minutes_air_temperature",
    "variant": "historical",
    "description": "10-minute station observations of air temperature for Germany"
  },
  "timestamp": {
    "value": "1993-04-28T12:30:00",
    "time_reference": "MEZ",
    "utc_offset": "+01:00"
  },
  "parameters": {
    "air_pressure_hpa": 987.3,
    "air_temperature_C": 24.9,
    "5min_mean_temperature_C": 28.4,
    "relative_humidity_percent": 23.0,
    "dew_point_temperature_C": 2.4
  },
  "quality_flag": 1,
  "location": {
    "latitude": 50.7827,
    "longitude": 6.0941,
    "station_altitude_m": 202.0,
    "country": "Germany",
    "state": "Nordrhein-Westfalen"
  },
  "sensor": {
    "air_temperature_C": "device_type: PT 100 (Luft); sensor_height_above_ground_m: 2.0; measurement_method: Temperaturmessung, elektr.",
    "5min_mean_temperature_C": "device_type: PT 100 (Luft); sensor_height_above_ground_m: 0.05; measurement_method: Temperaturmessung, elektr.",
    "relative_humidity_percent": "device_type: HYGROMER MP100; sensor_height_above_ground_m: 2.0; measurement_method: Feuchtemessung, elektr.",
    "air_pressure_hpa": null
  },
  "metadata": {
    "air_pressure_hpa": {
      "unit": "hPa",
      "parameter_description": "Luftdruck in Stationshoehe der voran. 10 min",
      "data_source": "10-Minutenwerte von automatischen Stationen der 1. Generation (MIRIAM/AFMS2, ESAU-Daten bis 31.12.1999 (Zeitbezug ist MEZ)",
      "time_note": "HHMM MEZ"
    },
    "air_temperature_C": {
      "unit": "°C",
      "parameter_description": "momentane Lufttemperatur in 2m Hoehe",
      "data_source": "10-Minutenwerte von automatischen Stationen der 1. Generation (MIRIAM/AFMS2, ESAU-Daten bis 31.12.1999 (Zeitbezug ist MEZ)",
      "time_note": "HHMM MEZ"
    },
    "5min_mean_temperature_C": {
      "unit": "°C",
      "parameter_description": "Momentane Temperatur in 5 cm Hoehe 10min",
      "data_source": "10-Minutenwerte von automatischen Stationen der 1. Generation (MIRIAM/AFMS2, ESAU-Daten bis 31.12.1999 (Zeitbezug ist MEZ)",
      "time_note": "HHMM MEZ"
    },
    "relative_humidity_percent": {
      "unit": "%",
      "parameter_description": "relative Feucht in 2m Hoehe",
      "data_source": "10-Minutenwerte von automatischen Stationen der 1. Generation (MIRIAM/AFMS2, ESAU-Daten bis 31.12.1999 (Zeitbezug ist MEZ)",
      "time_note": "HHMM MEZ"
    },
    "dew_point_temperature_C": {
      "unit": "°C",
      "parameter_description": "Taupunkttemperatur in 2m Hoehe",
      "data_source": "10-Minutenwerte von automatischen Stationen der 1. Generation (MIRIAM/AFMS2, ESAU-Daten bis 31.12.1999 (Zeitbezug ist MEZ)",
      "time_note": "HHMM MEZ"
    }
  }
}

</pre>