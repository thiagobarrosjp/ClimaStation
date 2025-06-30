"""
crawl_dwd.py — Step 1: DWD Repository Crawler (Part of the ClimaStation Pipeline)

This script performs a recursive crawl of the official Deutscher Wetterdienst (DWD) open data repository,
starting from the base climate observations path. It identifies all subdirectories containing raw climate 
data files (.zip or .gz) and saves a timestamped list of their URLs for later processing.

📌 Purpose:
- Step 1 of the ClimaStation pipeline.
- Output: list of folders containing downloadable datasets.

Key Features:
- Only crawls within the defined BASE_URL.
- Detects folders with .zip or .gz files (raw climate data).
- Logs errors and rate-limits requests.
- Skips known irrelevant directories (e.g., /readme/).
- Saves results in `data/dwd_structure_logs/`.

"""

import requests
from bs4 import BeautifulSoup, Tag
from urllib.parse import urljoin
import time
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configuration
BASE_URL = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/"
VALID_EXTENSIONS = [".zip", ".gz"]
EXCLUDE_FOLDERS = {"readme/", "cap_alerts/", "metadata/", "index.html"}
MAX_DEPTH = 10
RATE_LIMIT_DELAY = 0.5  # seconds

# Global state
VISITED = set()
FOLDER_HITS = []
ERROR_LOG = []

# Session with retry logic
session = requests.Session()
retry = Retry(connect=3, backoff_factor=1, status_forcelist=[502, 503, 504])
adapter = HTTPAdapter(max_retries=retry)
session.mount("http://", adapter)
session.mount("https://", adapter)


def is_within_base(url, base):
    return url.startswith(base)


def is_downloadable(href):
    return any(href.lower().endswith(ext) for ext in VALID_EXTENSIONS)


def is_folder(href):
    return href.endswith("/") and not href.startswith("/")


def crawl(url, depth=0):
    if url in VISITED or depth > MAX_DEPTH:
        return
    VISITED.add(url)

    try:
        time.sleep(RATE_LIMIT_DELAY)
        response = session.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        # Collect hrefs as strings only
        hrefs = []
        for a in soup.find_all("a"):
            if isinstance(a, Tag):  # ✔ Ensure it's a Tag (and helps type checker)
                raw_href = a.get("href")
                if isinstance(raw_href, str) and raw_href:
                    hrefs.append(raw_href)

    except Exception as e:
        msg = f"[ERROR] {url}: {e}"
        print(msg)
        ERROR_LOG.append(msg)
        return

    # Check if this folder contains any downloadable files
    if any(is_downloadable(href) for href in hrefs):
        print(f"[FOUND] {url}")
        FOLDER_HITS.append(url)
        return

    # Recurse into subfolders
    for href in hrefs:
        if is_folder(href):
            sub_url = urljoin(url, href)
            if not is_within_base(sub_url, BASE_URL):
                continue
            if any(excl in sub_url for excl in EXCLUDE_FOLDERS):
                continue
            crawl(sub_url, depth + 1)



if __name__ == "__main__":
    print(f"🌐 Starting crawl from {BASE_URL}\n")
    crawl(BASE_URL)

    out_dir = os.path.join(os.path.dirname(__file__), "../../../data/dwd_structure_logs")
    os.makedirs(out_dir, exist_ok=True)
    timestamp = time.strftime("%Y%m%d_%H%M%S")

    # Save folder list
    out_path = os.path.join(out_dir, f"{timestamp}_folders.txt")
    with open(out_path, "w", encoding="utf-8") as f:
        for folder in sorted(FOLDER_HITS):
            f.write(folder + "\n")

    # Save error log
    if ERROR_LOG:
        err_path = os.path.join(out_dir, f"{timestamp}_errors.txt")
        with open(err_path, "w", encoding="utf-8") as f:
            for err in ERROR_LOG:
                f.write(err + "\n")

    print(f"\n✅ Done. Found {len(FOLDER_HITS)} folders.")
    print(f"📁 Saved folder list to: {out_path}")
    if ERROR_LOG:
        print(f"⚠️  Logged {len(ERROR_LOG)} errors to: {err_path}")
