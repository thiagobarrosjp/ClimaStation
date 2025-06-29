"""
downloader.py

Downloads a minimal set of representative sample files from the DWD's
10-minute air temperature dataset, located at:

    https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/

The script downloads:
    - 2 sample .zip files each from:
        * /historical/
        * /recent/
        * /now/
        * /meta_data/
    - Both .pdf documentation files from the root folder

Files are saved to:
    data/air_temperature_10min/
        ├── historical/
        ├── recent/
        ├── now/
        ├── meta_data/
        └── docs/

This script is meant to support development of a parser and record format for
a single DWD dataset before generalizing the pipeline to broader coverage.
"""

import os
import requests
from bs4 import BeautifulSoup
from bs4.element import Tag
from urllib.parse import urljoin

BASE_URL = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/"
LOCAL_BASE = "data/air_temperature_10min"

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def list_files(url, file_ext=None):
    """Return all filenames ending in file_ext at the given index URL."""
    resp = requests.get(url)
    if resp.status_code != 200:
        raise Exception(f"Failed to access {url}")
    soup = BeautifulSoup(resp.text, "html.parser")
    files = []
    for link in soup.find_all("a"):
        if not isinstance(link, Tag):
            continue
        href = link.get("href")
        if not href:
            continue
        href = str(href)
        if href.endswith("/") or href.startswith("?"):
            continue
        if file_ext is None or href.lower().endswith(file_ext):
            files.append(href)
    return files


def download_file(url, save_path):
    if os.path.exists(save_path):
        print(f"File already exists, skipping: {save_path}")
        return
    print(f"Downloading {url}")
    r = requests.get(url, stream=True)
    if r.status_code == 200:
        with open(save_path, 'wb') as f:
            for chunk in r.iter_content(1024):
                f.write(chunk)
    else:
        print(f"Failed to download {url}")

def download_zip_samples(subfolder, limit=2):
    print(f"\n== Downloading from: {subfolder}/ ==")
    url = urljoin(BASE_URL, f"{subfolder}/")
    files = list_files(url, file_ext=".zip")[:limit]
    target_dir = os.path.join(LOCAL_BASE, subfolder)
    ensure_dir(target_dir)
    for file in files:
        full_url = urljoin(url, file)
        save_path = os.path.join(target_dir, file)
        download_file(full_url, save_path)

def download_root_docs():
    print(f"\n== Downloading documentation PDFs from root folder ==")
    files = list_files(BASE_URL, file_ext=".pdf")
    target_dir = os.path.join(LOCAL_BASE, "docs")
    ensure_dir(target_dir)
    for file in files:
        full_url = urljoin(BASE_URL, file)
        save_path = os.path.join(target_dir, file)
        download_file(full_url, save_path)

if __name__ == "__main__":
    download_zip_samples("historical", limit=2)
    download_zip_samples("recent", limit=2)
    download_zip_samples("now", limit=2)
    download_zip_samples("meta_data", limit=2)
    download_root_docs()
