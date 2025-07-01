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
                    ---- README.md                      
            --- __init__.py  
        -- __init__.py
    - data\          
        -- dwd_structure_logs\  
        -- dwd_validation_logs\ 
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
