"""
download_samples.py

This script reads the output from `crawl_dwd.py` — specifically the `*_urls.jsonl` file —
and downloads 1–2 representative raw data files (only zip files) from each directory
that was identified to contain datasets.

Downloaded files are saved to the `data/raw/` directory using a *flattened naming scheme*:
the original folder path is encoded into the filename by replacing slashes with underscores.

Example:
    Original path:   climate/10_minutes/air_temperature/recent/station_xyz.zip
    Saved as:        data/raw/10_minutes_air_temperature_recent_station_xyz.zip

This avoids the need for subdirectories and keeps all sample files in one place for easier manual review.

A summary log of all downloaded files is saved to: data/raw/downloaded_files.txt

Author: ClimaStation Team
Date: 2025-07-01
"""

import os
import json
import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from bs4.element import Tag

# === Configuration ===
URLS_JSONL_PATH = "data/dwd_structure_logs/2025-07-01_*.jsonl"  # Replace with actual filename
BASE_URL = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/"
RAW_DATA_DIR = "data/raw"
MAX_FILES_PER_FOLDER = 2
LOG_FILE_PATH = os.path.join(RAW_DATA_DIR, "downloaded_files.txt")

os.makedirs(RAW_DATA_DIR, exist_ok=True)

def sanitize_filename(prefix: str, filename: str) -> str:
    """Convert folder prefix + filename to safe flat name"""
    prefix_clean = prefix.replace("/", "_")
    return f"{prefix_clean}_{filename}"

def download_file(url: str, output_path: str):
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        with open(output_path, "wb") as f:
            f.write(resp.content)
        print(f"✅ Downloaded: {output_path}")
        return True
    except Exception as e:
        print(f"❌ Failed to download {url}: {e}")
        return False

def run():
    matched_file = None
    # Locate the actual *_urls.jsonl file if wildcard is present
    if "*" in URLS_JSONL_PATH:
        from glob import glob
        matches = glob(URLS_JSONL_PATH)
        if not matches:
            print("❌ No matching *_urls.jsonl file found.")
            return
        matched_file = matches[0]
    else:
        matched_file = URLS_JSONL_PATH

    with open(matched_file, "r", encoding="utf-8") as f:
        for line in f:
            record = json.loads(line)
            url = record["url"]
            prefix = record["prefix"]

            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, "html.parser")

                links = []
                for tag in soup.find_all("a", href=True):
                    if isinstance(tag, Tag):
                        href = tag.get("href")
                        if isinstance(href, str) and href.endswith(".zip"):
                            links.append(href)

                sample_files = links[:MAX_FILES_PER_FOLDER]
            except Exception as e:
                print(f"⚠️ Skipping folder {url}: {e}")
                continue

            for fname in sample_files:
                file_url = urljoin(url, str(fname))
                flat_name = sanitize_filename(prefix, fname)
                save_path = os.path.join(RAW_DATA_DIR, flat_name)

                if download_file(file_url, save_path):
                    with open(LOG_FILE_PATH, "a", encoding="utf-8") as log_f:
                        log_f.write(f"{prefix} -> {flat_name} -> {file_url}\n")

if __name__ == "__main__":
    run()
