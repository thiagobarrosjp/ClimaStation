# io/zip_handler.py

import zipfile
from pathlib import Path
from typing import List


def extract_txt_files_from_zip(zip_path: Path, extract_to: Path) -> List[Path]:
    """
    Extracts all .txt files from the given zip archive into the specified folder.
    Returns a list of extracted file paths.
    """
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        members = [f for f in zip_ref.namelist() if f.endswith(".txt")]
        zip_ref.extractall(extract_to, members)
    return [extract_to / m for m in members]
