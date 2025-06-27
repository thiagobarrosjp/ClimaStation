import os
import re
import requests
from pathlib import Path

# === CONFIG ===
URL_LOGS_DIR = Path("data/dwd_structure_logs")
SAMPLES_DIR = Path("data/dwd_structure_samples")
MAX_DOWNLOADS_PER_FOLDER = 1

# === HELPERS ===
def find_latest_folders_file():
    txt_files = sorted(URL_LOGS_DIR.glob("*_folders.txt"), reverse=True)
    return txt_files[0] if txt_files else None

def extract_folder_from_url(url):
    match = re.search(r"CDC/(.+)/[^/]+$", url)
    return match.group(1).replace("/", "_") if match else "unknown_folder"

def download_file(url, output_path):
    try:
        print(f"→ Downloading: {url}")
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"✓ Saved to {output_path}")
    except Exception as e:
        print(f"✗ Failed to download {url}: {e}")

# === MAIN ===
def main():
    folders_file = find_latest_folders_file()
    if not folders_file:
        print("No *_folders.txt file found in logs.")
        return

    print(f"Using folder list: {folders_file.name}")
    folder_download_count = {}

    with open(folders_file, "r") as f:
        for line in f:
            url = line.strip()
            if not (url.endswith(".zip") or url.endswith(".gz")):
                continue

            folder_key = extract_folder_from_url(url)
            if folder_download_count.get(folder_key, 0) >= MAX_DOWNLOADS_PER_FOLDER:
                continue

            folder_path = SAMPLES_DIR / folder_key
            folder_path.mkdir(parents=True, exist_ok=True)

            filename = url.split("/")[-1]
            output_path = folder_path / filename

            if output_path.exists():
                print(f"⏩ Already exists: {output_path}")
                folder_download_count[folder_key] = folder_download_count.get(folder_key, 0) + 1
                continue

            download_file(url, output_path)
            folder_download_count[folder_key] = folder_download_count.get(folder_key, 0) + 1

    print("\nDone. Downloads per folder:")
    for folder, count in folder_download_count.items():
        print(f"  {folder}: {count}")

if __name__ == "__main__":
    main()
