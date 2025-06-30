"""
download_samples.py — Step 2: Sample Downloader for 10-Minute DWD Subset

This script downloads representative raw and metadata samples from each data type 
found under the following DWD path:

https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/

Each subfolder (e.g., air_temperature, cloudiness) is expected to contain:
- historical/
- now/
- recent/
- meta_data/

The script downloads up to 2 raw data files (.zip or .gz) from the raw folders, 
and 1 file from the meta_data folder. All samples are saved under: data/raw/

Author: [Your Name]
"""

import os
import time
import requests
from bs4 import Tag
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE_URL = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/"
TARGET_FOLDERS = ["historical/", "now/", "recent/", "meta_data/"]
VALID_RAW_EXTENSIONS = [".zip", ".gz"]
VALID_META_EXTENSIONS = [".txt", ".xml", ".zip", ".gz"]
SAMPLES_PER_FOLDER = 2
RATE_LIMIT = 0.5  # seconds

def get_links(url):
    try:
        time.sleep(RATE_LIMIT)
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        hrefs = []
        for a in soup.find_all("a"):
            if isinstance(a, Tag):  # 🛡️ Ensures `.get()` is valid
                href = a.get("href")
                if isinstance(href, str) and href:
                    hrefs.append(href)

        return hrefs
    except Exception as e:
        print(f"[ERROR] Cannot access {url}: {e}")
        return []

def download_file(url, dest_path):
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    if os.path.exists(dest_path):
        print(f"[SKIP] Already exists: {dest_path}")
        return
    try:
        print(f"[⬇️] Downloading: {url}")
        with requests.get(url, stream=True, timeout=15) as r:
            r.raise_for_status()
            with open(dest_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
    except Exception as e:
        print(f"[ERROR] Failed to download {url}: {e}")

def get_sample_files(url, valid_exts, max_files):
    links = get_links(url)
    files = [link for link in links if any(link.endswith(ext) for ext in valid_exts)]
    return files[:max_files]

def sanitize_folder_name(url):
    return url.strip("/").split("/")[-1]

def main():
    # Step 1: List all subfolders under 10_minutes/
    subfolders = get_links(BASE_URL)
    data_types = [f for f in subfolders if f.endswith("/") and f not in TARGET_FOLDERS]

    for data_type in data_types:
        full_type_url = urljoin(BASE_URL, data_type)
        local_dir = os.path.join("data", "raw", sanitize_folder_name(data_type))

        for folder in TARGET_FOLDERS:
            full_folder_url = urljoin(full_type_url, folder)
            ext_filter = VALID_META_EXTENSIONS if folder == "meta_data/" else VALID_RAW_EXTENSIONS
            max_files = 1 if folder == "meta_data/" else SAMPLES_PER_FOLDER

            sample_files = get_sample_files(full_folder_url, ext_filter, max_files)

            for filename in sample_files:
                file_url = urljoin(full_folder_url, filename)
                destination = os.path.join(local_dir, folder.strip("/"), filename)
                download_file(file_url, destination)

    print("\n✅ Sample download complete.")

if __name__ == "__main__":
    main()
