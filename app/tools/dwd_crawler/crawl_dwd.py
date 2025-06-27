# crawl_dwd.py

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
import os

BASE_URL = "https://opendata.dwd.de/climate_environment/CDC/"
VALID_EXTENSIONS = [".zip", ".gz"]
VISITED = set()
FOLDER_HITS = []

def is_within_base(url, base):
    return url.startswith(base)

def is_downloadable(href):
    return any(href.lower().endswith(ext) for ext in VALID_EXTENSIONS)

def is_folder(href):
    return href.endswith("/") and not href.startswith("/")

def crawl(url, depth=0, max_depth=10):
    if url in VISITED or depth > max_depth:
        return
    VISITED.add(url)

    try:
        time.sleep(0.5)  # Rate-limiting for DWD servers
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        hrefs = [a.get("href") for a in soup.find_all("a") if a.get("href")]
    except Exception as e:
        print(f"[ERROR] Failed at {url}: {e}")
        return

    if any(is_downloadable(href) for href in hrefs):
        print(f"[FOUND] {url}")
        FOLDER_HITS.append(url)
        return

    for href in hrefs:
        if is_folder(href):
            sub_url = urljoin(url, href)
            if is_within_base(sub_url, BASE_URL):  # <--- Prevents going outside base
                crawl(sub_url, depth + 1, max_depth)

if __name__ == "__main__":
    print(f"🌐 Starting crawl from {BASE_URL}\n")
    crawl(BASE_URL)

    out_dir = os.path.join(os.path.dirname(__file__), "../../../data/dwd_structure_logs")
    os.makedirs(out_dir, exist_ok=True)

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(out_dir, f"{timestamp}_folders.txt")

    with open(out_path, "w", encoding="utf-8") as f:
        for folder in sorted(FOLDER_HITS):
            f.write(folder + "\n")

    print(f"\n✅ Done. Found {len(FOLDER_HITS)} folders. Saved to {out_path}")
 

