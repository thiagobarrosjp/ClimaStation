"""
download_samples.py

This script reads the output from `crawl_dwd.py` — specifically the `*_urls.jsonl` file —
and downloads 1–2 representative raw data files (only zip files) from each directory
that was identified to contain datasets.

It also downloads all `DESCRIPTION_*.pdf` files found in these folders.

Downloaded files are saved to the `data/raw/` directory using a *flattened naming scheme*:
the original folder path is encoded into the filename by replacing slashes with underscores.

Examples:
    - Zip file: 10_minutes/air_temperature/recent/xyz.zip → 10_minutes_air_temperature_recent_xyz.zip
    - PDF file: 10_minutes/air_temperature/DESCRIPTION_abc.pdf → 10_minutes_air_temperature_DESCRIPTION_abc.pdf

This avoids the need for subdirectories and keeps all sample files in one place.

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
from glob import glob

# === Configuration ===
# Dynamically select the most recent *_urls.jsonl file
url_files = glob("data/dwd_structure_logs/*_urls.jsonl")
if not url_files:
    print("❌ No *_urls.jsonl files found.")
    exit(1)

URLS_JSONL_PATH = max(url_files, key=os.path.getmtime)
print(f"✅ Using latest URL file: {URLS_JSONL_PATH}")
BASE_URL = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/"
RAW_DATA_DIR = "data/raw"
MAX_ZIP_FILES_PER_FOLDER = 2
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
    # Resolve wildcard in file path
    matches = glob(URLS_JSONL_PATH)
    if not matches:
        print("❌ No matching *_urls.jsonl file found.")
        return

    matched_file = matches[0]
    print(f"📥 Reading folder list from: {matched_file}")

    with open(matched_file, "r", encoding="utf-8") as f:
        for line in f:
            record = json.loads(line)
            url = record["url"]
            prefix = record["prefix"]

            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, "html.parser")

                zip_links = []
                pdf_links = []

                for tag in soup.find_all("a", href=True):
                    if isinstance(tag, Tag):
                        href = tag.get("href")
                        if not isinstance(href, str):
                            continue
                        if href.endswith(".zip"):
                            zip_links.append(href)
                        elif href.startswith("DESCRIPTION_") and href.endswith(".pdf"):
                            pdf_links.append(href)

                sample_zips = zip_links[:MAX_ZIP_FILES_PER_FOLDER]
                all_targets = [(fname, "zip") for fname in sample_zips] + [(fname, "pdf") for fname in pdf_links]
            except Exception as e:
                print(f"⚠️ Skipping folder {url}: {e}")
                continue

            for fname, filetype in all_targets:
                file_url = urljoin(url, str(fname))
                flat_name = sanitize_filename(prefix, fname)
                save_path = os.path.join(RAW_DATA_DIR, flat_name)

                if download_file(file_url, save_path):
                    with open(LOG_FILE_PATH, "a", encoding="utf-8") as log_f:
                        log_f.write(f"{prefix} -> {flat_name} -> {file_url}\n")

if __name__ == "__main__":
    run()
