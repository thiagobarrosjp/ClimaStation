"""
Script: crawl_dwd.py
Module: dwd_pipeline

Purpose:
    Recursively crawls the official DWD climate data repository:
    https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/

    Identifies all folders and subfolders, determines which ones contain downloadable datasets
    (.zip, .gz, .txt, .pdf), and produces a structured snapshot of the repository for downstream processing.

Output:
    - data/germany/1_crawl_dwd/dwd_tree.txt
        → Human-readable tree with folder hierarchy and file type annotations
    - data/germany/1_crawl_dwd/dwd_urls.jsonl
        → JSONL list of dataset-relevant folders and file stats
    - data/germany/1_crawl_dwd/dwd_structure.json
        → Full JSON tree of the folder structure, with metadata

Debug:
    - data/0_debug/crawl_dwd_debug.log
        → Logs warnings, progress, and structural metadata

Notes:
    - This script is the **first step** of the ClimaStation pipeline.
    - Output filenames are fixed for consistency and reusability.
"""

import os
import requests
from bs4 import BeautifulSoup, Tag
from urllib.parse import urljoin
from datetime import datetime
import json
from typing import List, Dict, cast
import time
import logging

# === Config ===
BASE_URL = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/"
STRUCTURE_DIR = "data/germany/1_crawl_dwd"
DEBUG_LOG_PATH = "data/germany/0_debug/crawl_dwd_debug.log"

os.makedirs(STRUCTURE_DIR, exist_ok=True)
os.makedirs("data/germany/0_debug", exist_ok=True)

# === Logging ===
if os.path.exists(DEBUG_LOG_PATH):
    os.remove(DEBUG_LOG_PATH)
logging.basicConfig(
    filename=DEBUG_LOG_PATH,
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.info("=== Starting crawl_dwd.py ===")


class DWDCrawler:
    def __init__(self):
        self.tree_structure: Dict[str, Dict] = {}
        self.url_records = []
        self.crawled_count = 0

    def crawl(self, url: str, path_segments: List[str]):
        self.crawled_count += 1
        if self.crawled_count % 10 == 0:
            print(f"Crawled {self.crawled_count} directories...")

        current_path = "/".join(path_segments)
        depth = len(path_segments)
        logging.debug(f"Crawling URL: {url}")

        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
        except Exception as e:
            msg = f"Failed to crawl {url}: {e}"
            logging.warning(msg)
            print(msg)
            return

        soup = BeautifulSoup(response.text, "html.parser")
        hrefs = []

        for a in soup.find_all("a", href=True):
            if isinstance(a, Tag):
                href = a.attrs.get("href")
                if isinstance(href, str) and href != "../":
                    hrefs.append(href)

        subfolders = [h for h in hrefs if h.endswith("/")]
        data_files = [h for h in hrefs if h.endswith((".zip", ".gz", ".txt", ".pdf"))]

        if current_path not in self.tree_structure:
            self.tree_structure[current_path] = {
                "name": current_path,
                "children": [],
                "has_data": False,
                "data_info": None,
                "url": url,
                "depth": depth
            }

        if data_files:
            file_exts = list({os.path.splitext(f)[1] for f in data_files})
            pdf_count = sum(1 for f in data_files if f.endswith(".pdf"))
            self.tree_structure[current_path]["has_data"] = True
            self.tree_structure[current_path]["data_info"] = {
                "file_types": file_exts,
                "file_count": len(data_files),
                "pdf_count": pdf_count if pdf_count > 0 else None
            }
            logging.debug(f"Found data files at {current_path}: {file_exts}")

            record = {
                "url": url,
                "prefix": current_path,
                "contains": file_exts,
                "estimated_files": len(data_files),
            }
            self.url_records.append(record)

        for folder in subfolders:
            child_name = folder.rstrip("/")
            child_path = f"{current_path}/{child_name}"
            if child_path not in self.tree_structure[current_path]["children"]:
                self.tree_structure[current_path]["children"].append(child_path)

            next_url = urljoin(url, folder)
            next_segments = path_segments + [child_name]
            self.crawl(next_url, next_segments)

    def generate_tree_lines(self, path: str, prefix: str = "", is_last: bool = True) -> List[str]:
        lines = []
        if path not in self.tree_structure:
            return lines

        node = self.tree_structure[path]
        connector = "└── " if is_last else "├── "
        name = node["name"]
        if node["has_data"]:
            data_info = node["data_info"]
            name += f" 📊 ({data_info['file_count']} files: {', '.join(data_info['file_types'])})"
        lines.append(f"{prefix}{connector}{name}")
        child_prefix = prefix + ("    " if is_last else "│   ")

        children = sorted(node["children"])
        for i, child in enumerate(children):
            is_last_child = (i == len(children) - 1)
            child_lines = self.generate_tree_lines(child, child_prefix, is_last_child)
            lines.extend(child_lines)
        return lines

    def save_outputs(self):
        tree_path = os.path.join(STRUCTURE_DIR, "dwd_tree.txt")
        urls_path = os.path.join(STRUCTURE_DIR, "dwd_urls.jsonl")
        structure_path = os.path.join(STRUCTURE_DIR, "dwd_structure.json")

        top_level_paths = sorted(p for p in self.tree_structure if "/" not in p)
        all_tree_lines = []
        for i, path in enumerate(top_level_paths):
            is_last = i == len(top_level_paths) - 1
            all_tree_lines.extend(self.generate_tree_lines(path, "", is_last))

        logging.debug(f"Tree keys (top): {top_level_paths}")
        logging.debug(f"URL records: {len(self.url_records)}")

        with open(tree_path, "w", encoding="utf-8") as f:
            f.write("DWD Climate Data Directory Structure\n")
            f.write("=" * 50 + "\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total directories: {len(self.tree_structure)}\n")
            f.write(f"Directories with data: {len(self.url_records)}\n\n")
            f.write("\n".join(all_tree_lines))

        with open(urls_path, "w", encoding="utf-8") as f:
            for record in self.url_records:
                json.dump(record, f)
                f.write("\n")

        with open(structure_path, "w", encoding="utf-8") as f:
            json.dump(self.tree_structure, f, indent=2)

        logging.info("Saved outputs from crawl_dwd.py")
        print(f"📄 Tree structure saved to: {tree_path}")
        print(f"📄 URL records saved to: {urls_path}")
        print(f"📄 Full structure saved to: {structure_path}")


if __name__ == "__main__":
    print("🌐 Starting DWD climate data crawler...")
    start_time = time.time()

    crawler = DWDCrawler()

    try:
        root_response = requests.get(BASE_URL)
        root_response.raise_for_status()
        root_soup = BeautifulSoup(root_response.text, "html.parser")

        top_level_folders = []
        for a in root_soup.find_all("a", href=True):
            tag = cast(Tag, a)
            href = tag.get("href")
            if isinstance(href, str) and href.endswith("/") and href != "../":
                top_level_folders.append(href.rstrip("/"))

    except Exception as e:
        print(f"❌ Failed to fetch top-level folders: {e}")
        top_level_folders = []

    for folder in top_level_folders:
        full_url = urljoin(BASE_URL, folder + "/")
        crawler.crawl(full_url, [folder])

    print("🌳 Generating tree structure...")
    crawler.save_outputs()

    elapsed_time = time.time() - start_time
    print(f"✅ Crawling completed in {elapsed_time:.2f} seconds")
    print(f"📊 Statistics:")
    print(f"   - Total directories: {len(crawler.tree_structure)}")
    print(f"   - Directories with data: {len(crawler.url_records)}")
    print(f"   - Requests made: {crawler.crawled_count}")
