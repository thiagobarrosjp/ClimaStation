"""
Script: download_samples.py
Module: dwd_pipeline

Purpose:
    Downloads a representative sample of raw dataset archives and metadata PDFs
    from the DWD climate repository, based on the structure mapped by `crawl_dwd.py`.

Input:
    - data/1_structure/[timestamp]_urls.jsonl
        → List of dataset folders with download URLs and metadata

Output:
    - data/2_samples/raw/
        → Up to 2 `.zip` raw data archives per dataset folder
        → All available `DESCRIPTION_*.pdf` files
    - data/2_samples/downloaded_files.txt
        → Log of downloaded files with source URL and filename
    - data/0_debug/download_samples_debug.log
        → Debug output including failed downloads

Notes:
    - Zip files retain their original filenames
    - PDF files are flattened with folder prefix to avoid conflicts
"""


import os
import json
import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from bs4.element import Tag
from glob import glob
import logging

# === Paths and Config ===
STRUCTURE_DIR = "data/1_structure"
SAMPLES_DIR = "data/2_samples"
RAW_DATA_DIR = os.path.join(SAMPLES_DIR, "raw")
DEBUG_LOG_PATH = "data/0_debug/download_samples_debug.log"
MAX_ZIP_FILES_PER_FOLDER = 2

os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs("data/0_debug", exist_ok=True)

# === Overwrite debug log ===
if os.path.exists(DEBUG_LOG_PATH):
    os.remove(DEBUG_LOG_PATH)

# === Logging setup ===
logging.basicConfig(
    filename=DEBUG_LOG_PATH,
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.info("=== Starting download_samples.py ===")

# === Select most recent urls.jsonl ===
url_files = glob(os.path.join(STRUCTURE_DIR, "*_urls.jsonl"))
if not url_files:
    print("❌ No *_urls.jsonl files found in data/1_structure/")
    exit(1)

URLS_JSONL_PATH = max(url_files, key=os.path.getmtime)
print(f"✅ Using latest URL file: {URLS_JSONL_PATH}")
LOG_FILE_PATH = os.path.join(SAMPLES_DIR, "downloaded_files.txt")

# Clear log from previous runs
if os.path.exists(LOG_FILE_PATH):
    os.remove(LOG_FILE_PATH)

def sanitize_filename(prefix: str, filename: str) -> str:
    """Convert folder prefix + filename to safe flat name"""
    prefix_clean = prefix.replace("/", "_")
    return f"{prefix_clean}_{filename}"

def download_file(url: str, output_path: str) -> bool:
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        with open(output_path, "wb") as f:
            f.write(resp.content)
        print(f"✅ Downloaded: {output_path}")
        logging.info(f"Downloaded: {output_path}")
        return True
    except Exception as e:
        print(f"❌ Failed to download {url}: {e}")
        logging.warning(f"Failed to download {url}: {e}")
        return False

def run():
    print(f"📥 Reading folder list from: {URLS_JSONL_PATH}")
    with open(URLS_JSONL_PATH, "r", encoding="utf-8") as f:
        for line in f:
            try:
                record = json.loads(line)
                url = record["url"]
                prefix = record["prefix"]

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

                for fname, filetype in all_targets:
                    file_url = urljoin(url, fname)
                    if filetype == "zip":
                        save_path = os.path.join(RAW_DATA_DIR, fname)  # keep original name
                    else:
                        flat_name = sanitize_filename(prefix, fname)
                        save_path = os.path.join(RAW_DATA_DIR, flat_name)

                    if download_file(file_url, save_path):
                        with open(LOG_FILE_PATH, "a", encoding="utf-8") as log_f:
                            log_f.write(f"{prefix} -> {os.path.basename(save_path)} -> {file_url}\n")

            except Exception as e:
                print(f"⚠️ Skipping folder {record.get('url', '?')}: {e}")
                logging.error(f"Failed to process folder: {e}")
                continue

if __name__ == "__main__":
    run()
