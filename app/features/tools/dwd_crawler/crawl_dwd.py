# Enhanced version with PDF support, better tree visualization, and progress tracking

import os
import requests
from bs4 import BeautifulSoup
from bs4.element import Tag
from urllib.parse import urljoin
from datetime import datetime
import json
from typing import List, Dict, Set
import time

BASE_URL = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/"
LOG_DIR = "data/dwd_structure_logs"

os.makedirs(LOG_DIR, exist_ok=True)

class DWDCrawler:
    def __init__(self):
        self.tree_structure: Dict[str, Dict] = {}
        self.url_records = []
        self.crawled_count = 0
        
    def crawl(self, url: str, path_segments: List[str]):
        self.crawled_count += 1
        if self.crawled_count % 10 == 0:
            print(f"Crawled {self.crawled_count} directories...")
            
        depth = len(path_segments)
        
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
        except Exception as e:
            print(f"Failed to crawl {url}: {e}")
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

        # Build tree structure
        current_path = "/".join(path_segments) if path_segments else "root"
        
        # Initialize current node
        if current_path not in self.tree_structure:
            self.tree_structure[current_path] = {
                "name": current_path if current_path != "root" else "root",
                "children": [],
                "has_data": False,
                "data_info": None,
                "url": url,
                "depth": depth
            }

        # If this folder contains data files, record it
        if data_files:
            file_exts = list({os.path.splitext(f)[1] for f in data_files})
            pdf_count = sum(1 for f in data_files if f.endswith(".pdf"))
            self.tree_structure[current_path]["has_data"] = True
            self.tree_structure[current_path]["data_info"] = {
                "file_types": file_exts,
                "file_count": len(data_files),
                "pdf_count": pdf_count if pdf_count > 0 else None
            }
            
            record = {
                "url": url,
                "prefix": current_path,
                "contains": file_exts,
                "estimated_files": len(data_files),
            }
            self.url_records.append(record)

        # Add subfolders as children and crawl them
        for folder in subfolders:
            child_name = folder.rstrip("/")
            child_path = f"{current_path}/{child_name}" if current_path != "root" else child_name
            
            # Add to children list
            if child_path not in [child["path"] for child in self.tree_structure[current_path]["children"]]:
                self.tree_structure[current_path]["children"].append({
                    "name": child_path,
                    "path": child_path
                })
            
            # Recursively crawl
            next_url = urljoin(url, folder)
            next_segments = path_segments + [child_name]
            self.crawl(next_url, next_segments)

    def generate_tree_lines(self, path: str = "root", prefix: str = "", is_last: bool = True) -> List[str]:
        """Generate tree lines with proper ASCII tree structure"""
        lines = []
        
        if path not in self.tree_structure:
            return lines
            
        node = self.tree_structure[path]
        
        if path != "root":
            # Create the tree connector
            connector = "└── " if is_last else "├── "
            
            # Add data indicator
            name = node["name"]
            if node["has_data"]:
                data_info = node["data_info"]
                name += f" 📊 ({data_info['file_count']} files: {', '.join(data_info['file_types'])})"
            
            lines.append(f"{prefix}{connector}{name}")
            
            # Update prefix for children
            child_prefix = prefix + ("    " if is_last else "│   ")
        else:
            child_prefix = ""
        
        # Sort children alphabetically
        children = sorted(node["children"], key=lambda x: x["name"])
        
        for i, child in enumerate(children):
            is_last_child = (i == len(children) - 1)
            child_lines = self.generate_tree_lines(
                child["path"], 
                child_prefix, 
                is_last_child
            )
            lines.extend(child_lines)
        
        return lines

    def save_outputs(self):
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        tree_path = os.path.join(LOG_DIR, f"{timestamp}_tree.txt")
        urls_path = os.path.join(LOG_DIR, f"{timestamp}_urls.jsonl")
        structure_path = os.path.join(LOG_DIR, f"{timestamp}_structure.json")

        # Generate tree lines
        tree_lines = self.generate_tree_lines()

        # Save tree structure
        with open(tree_path, "w", encoding="utf-8") as f:
            f.write("DWD Climate Data Directory Structure\n")
            f.write("=" * 50 + "\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total directories: {len(self.tree_structure)}\n")
            f.write(f"Directories with data: {len(self.url_records)}\n\n")
            f.write("\n".join(tree_lines))

        # Save URL records
        with open(urls_path, "w", encoding="utf-8") as f:
            for record in self.url_records:
                json.dump(record, f)
                f.write("\n")

        # Save full structure as JSON
        with open(structure_path, "w", encoding="utf-8") as f:
            json.dump(self.tree_structure, f, indent=2)

        print(f"📄 Tree structure saved to: {tree_path}")
        print(f"📄 URL records saved to: {urls_path}")
        print(f"📄 Full structure saved to: {structure_path}")

if __name__ == "__main__":
    print("🌐 Starting DWD climate data crawler...")
    start_time = time.time()
    
    crawler = DWDCrawler()
    crawler.crawl(BASE_URL, path_segments=[])
    
    print("🌳 Generating tree structure...")
    crawler.save_outputs()
    
    elapsed_time = time.time() - start_time
    print(f"✅ Crawling completed in {elapsed_time:.2f} seconds")
    print(f"📊 Statistics:")
    print(f"   - Total directories: {len(crawler.tree_structure)}")
    print(f"   - Directories with data: {len(crawler.url_records)}")
    print(f"   - Requests made: {crawler.crawled_count}")
