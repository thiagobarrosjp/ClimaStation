import os
import requests
import zipfile

DWD_URL = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/now/"
DOWNLOAD_DIR = "data/raw"

def download_and_extract_zip(station_id: str = "00044") -> str:
    zip_filename = f"10minutenwerte_TU_{station_id}_now.zip"
    zip_url = f"{DWD_URL}{zip_filename}"
    local_zip_path = os.path.join(DOWNLOAD_DIR, zip_filename)

    # Download the ZIP file
    response = requests.get(zip_url)
    response.raise_for_status()
    with open(local_zip_path, "wb") as f:
        f.write(response.content)

    # Extract the ZIP file
    with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
        zip_ref.extractall(DOWNLOAD_DIR)

    # Find the extracted .txt file
    for name in zip_ref.namelist():
        if name.endswith(".txt"):
            return os.path.join(DOWNLOAD_DIR, name)

    raise FileNotFoundError("No .txt file found in the downloaded ZIP.")
