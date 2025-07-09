"""
ClimaStation Utility Script: Create Folder Structure from DWD JSON Description

This script reads a DWD structure definition from a JSON file (`dwd_structure.json`)
and creates a matching folder hierarchy under:

    - data/germany/2_downloaded_files/
    - data/germany/3_parsed_files/   (mirrored structure with 'parsed_' prefix at each level, excluding meta_data)

Only folders marked with `"has_data": true` are created.
Also logs all created folders to a debug log file.

Usage:
    1. Place this script and `dwd_structure.json` in data/germany/1_crawl_dwd/
    2. Run from terminal:
        cd data/germany/1_crawl_dwd
        python create_dwd_folder_structure.py
"""

import os
import json

SCRIPT_DIR = os.path.dirname(__file__)
STRUCTURE_FILE = os.path.join(SCRIPT_DIR, "dwd_structure.json")

BASE_RAW_OUTPUT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", "2_downloaded_files"))
BASE_PARSED_OUTPUT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", "3_parsed_files"))
DEBUG_LOG_PATH = os.path.abspath(os.path.join(SCRIPT_DIR, "..", "0_debug", "debug.log"))

def collect_valid_paths(node, collected):
    if isinstance(node, dict):
        if node.get("has_data") is True and "name" in node:
            collected.append(node["name"])
        for value in node.values():
            collect_valid_paths(value, collected)
    elif isinstance(node, list):
        for item in node:
            collect_valid_paths(item, collected)

def create_folder_structure():
    print("📂 Reading structure from:", STRUCTURE_FILE)
    with open(STRUCTURE_FILE, "r", encoding="utf-8") as f:
        structure_data = json.load(f)

    valid_paths = []
    collect_valid_paths(structure_data, valid_paths)

    created_raw = 0
    created_parsed = 0
    log_lines = []

    for rel_path in valid_paths:
        # Create raw folder
        raw_folder = os.path.join(BASE_RAW_OUTPUT, rel_path)
        os.makedirs(raw_folder, exist_ok=True)
        created_raw += 1
        log_lines.append(f"[RAW]    {raw_folder}")

        # Skip creating parsed folders for any 'meta_data' paths
        if "meta_data" in rel_path:
            continue

        # Create parsed folder with 'parsed_' prefix for each part
        parts = rel_path.strip("/").split("/")
        parsed_parts = [f"parsed_{p}" for p in parts]
        parsed_folder = os.path.join(BASE_PARSED_OUTPUT, *parsed_parts)
        os.makedirs(parsed_folder, exist_ok=True)
        created_parsed += 1
        log_lines.append(f"[PARSED] {parsed_folder}")

    # Write debug log
    os.makedirs(os.path.dirname(DEBUG_LOG_PATH), exist_ok=True)
    with open(DEBUG_LOG_PATH, "w", encoding="utf-8") as log_file:
        log_file.write("Created Folder Log\n")
        log_file.write("===================\n")
        log_file.write("\n".join(log_lines))

    print(f"✅ Created {created_raw} raw folders in {BASE_RAW_OUTPUT}")
    print(f"✅ Created {created_parsed} parsed folders in {BASE_PARSED_OUTPUT}")
    print(f"🪵 Debug log written to {DEBUG_LOG_PATH}")

if __name__ == "__main__":
    create_folder_structure()
