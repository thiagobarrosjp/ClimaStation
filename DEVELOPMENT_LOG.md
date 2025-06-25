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
- Record format Version 1 Schema:
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
