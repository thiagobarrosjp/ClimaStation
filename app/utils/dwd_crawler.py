"""
ClimaStation DWD Repository Crawler

SCRIPT IDENTIFICATION: DWD10TAH3W (DWD Crawler)

PURPOSE:
Modernized crawler for the official DWD climate data repository that integrates
with the ClimaStation architecture. Recursively crawls the broader DWD repository
scope to identify downloadable datasets and produces structured output for the
download pipeline.

RESPONSIBILITIES:
- Crawl DWD repository with configurable scope and depth
- Identify folders containing downloadable datasets (.zip, .gz, .txt, .pdf)
- Generate structured output compatible with download pipeline
- Integrate with configuration system and enhanced logging
- Support progress tracking and error recovery
- Follow dependency injection patterns for reusability

USAGE:
    from app.utils.dwd_crawler import crawl_dwd_repository
    from app.utils.enhanced_logger import get_logger
    from app.utils.config_manager import load_config
    
    logger = get_logger("CRAWLER")
    config = load_config("10_minutes_air_temperature", logger)
    
    # Crawl repository
    results = crawl_dwd_repository(config, logger)

OUTPUT FORMAT:
- dwd_urls.jsonl: JSONL records with {url, prefix, contains, estimated_files}
- dwd_tree.txt: Human-readable directory tree structure
- dwd_structure.json: Complete JSON tree with metadata

INTEGRATION:
- Uses config_manager for paths and crawler settings
- Uses enhanced_logger with CRAWLER component code
- Follows dependency injection patterns
- Compatible with existing download pipeline expectations
"""

import requests
from bs4 import BeautifulSoup, Tag
from urllib.parse import urljoin
from datetime import datetime
import json
import time
import os
from pathlib import Path
from typing import List, Dict, Optional, Any, Tuple, cast
from dataclasses import dataclass

# Import ClimaStation utilities
from .enhanced_logger import StructuredLoggerAdapter
from .config_manager import ConfigurationError

@dataclass
class CrawlResult:
    """Results from DWD repository crawling operation."""
    tree_structure: Dict[str, Dict[str, Any]]
    url_records: List[Dict[str, Any]]
    crawled_count: int
    directories_with_data: int
    elapsed_time: float
    output_files: Dict[str, Path]

class DWDRepositoryCrawler:
    """
    Modernized DWD repository crawler with ClimaStation integration.
    
    Follows dependency injection pattern and integrates with the configuration
    and logging systems. Supports configurable crawling parameters and produces
    output compatible with the existing download pipeline.
    """
    
    def __init__(self, config: Dict[str, Any], logger: StructuredLoggerAdapter):
        """
        Initialize DWD crawler with configuration and logger.
        
        Args:
            config: Configuration dictionary from config manager
            logger: StructuredLoggerAdapter instance with CRAWLER component
        """
        self.config = config
        self.logger = logger
        
        # Get crawler configuration with defaults
        crawler_config = config.get('crawler', {})
        self.base_url = crawler_config.get('base_url', 
            'https://opendata.dwd.de/climate_environment/CDC/observations_germany/')
        self.max_depth = crawler_config.get('max_depth', 10)
        self.request_timeout = crawler_config.get('request_timeout_seconds', 30)
        self.request_delay = crawler_config.get('request_delay_seconds', 0.1)
        self.max_retries = crawler_config.get('max_retries', 3)
        
        # Get output paths from DWD paths configuration
        try:
            if 'dwd_paths' in config:
                self.output_dir = Path(config['dwd_paths']['crawl_data'])
                self.debug_dir = Path(config['dwd_paths']['debug'])
            else:
                # Fallback to legacy paths
                self.output_dir = Path("data/dwd/1_crawl_dwd")
                self.debug_dir = Path("data/dwd/0_debug")
        except KeyError as e:
            raise ConfigurationError(f"Missing required path configuration: {e}")
        
        # Ensure output directories exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.debug_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize crawling state
        self.tree_structure: Dict[str, Dict[str, Any]] = {}
        self.url_records: List[Dict[str, Any]] = []
        self.crawled_count = 0
        self.start_time = 0.0
        
        self.logger.info("DWD crawler initialized", extra={
            "component": "CRAWLER",
            "structured_data": {
                "base_url": self.base_url,
                "output_dir": str(self.output_dir),
                "max_depth": self.max_depth,
                "request_timeout": self.request_timeout
            }
        })
    
    def _make_request(self, url: str, retries: int = 0) -> Optional[requests.Response]:
        """
        Make HTTP request with retry logic and error handling.
        
        Args:
            url: URL to request
            retries: Current retry attempt
            
        Returns:
            Response object or None if failed
        """
        try:
            response = requests.get(url, timeout=self.request_timeout)
            response.raise_for_status()
            
            # Add delay between requests to be respectful
            if self.request_delay > 0:
                time.sleep(self.request_delay)
            
            return response
            
        except requests.exceptions.RequestException as e:
            if retries < self.max_retries:
                self.logger.warning(f"Request failed, retrying ({retries + 1}/{self.max_retries}): {url}", extra={
                    "component": "CRAWLER",
                    "structured_data": {
                        "url": url,
                        "error": str(e),
                        "retry_attempt": retries + 1
                    }
                })
                time.sleep(2 ** retries)  # Exponential backoff
                return self._make_request(url, retries + 1)
            else:
                self.logger.error(f"Request failed after {self.max_retries} retries: {url}", extra={
                    "component": "CRAWLER",
                    "structured_data": {
                        "url": url,
                        "error": str(e),
                        "final_failure": True
                    }
                })
                return None
    
    def _parse_directory_listing(self, response: requests.Response) -> Tuple[List[str], List[str]]:
        """
        Parse HTML directory listing to extract folders and data files.
        
        Args:
            response: HTTP response containing HTML directory listing
            
        Returns:
            Tuple of (subfolders, data_files)
        """
        soup = BeautifulSoup(response.text, "html.parser")
        hrefs = []
        
        for a in soup.find_all("a", href=True):
            if isinstance(a, Tag):
                href = a.attrs.get("href")
                if isinstance(href, str) and href != "../":
                    hrefs.append(href)
        
        subfolders = [h for h in hrefs if h.endswith("/")]
        data_files = [h for h in hrefs if h.endswith((".zip", ".gz", ".txt", ".pdf"))]
        
        return subfolders, data_files
    
    def _crawl_directory(self, url: str, path_segments: List[str]) -> None:
        """
        Recursively crawl a directory and its subdirectories.
        
        Args:
            url: URL to crawl
            path_segments: Path segments representing current location
        """
        self.crawled_count += 1
        current_path = "/".join(path_segments)
        depth = len(path_segments)
        
        # Check depth limit
        if depth > self.max_depth:
            self.logger.warning(f"Maximum depth reached, skipping: {current_path}", extra={
                "component": "CRAWLER",
                "structured_data": {"path": current_path, "depth": depth, "max_depth": self.max_depth}
            })
            return
        
        # Progress logging
        if self.crawled_count % 50 == 0:
            elapsed = time.time() - self.start_time
            self.logger.info(f"Crawling progress: {self.crawled_count} directories", extra={
                "component": "CRAWLER",
                "structured_data": {
                    "crawled_count": self.crawled_count,
                    "elapsed_seconds": round(elapsed, 2),
                    "current_path": current_path
                }
            })
        
        self.logger.debug(f"Crawling directory: {current_path}", extra={
            "component": "CRAWLER",
            "structured_data": {"url": url, "path": current_path, "depth": depth}
        })
        
        # Make request
        response = self._make_request(url)
        if response is None:
            return
        
        # Parse directory listing
        try:
            subfolders, data_files = self._parse_directory_listing(response)
        except Exception as e:
            self.logger.error(f"Failed to parse directory listing: {url}", extra={
                "component": "CRAWLER",
                "structured_data": {"url": url, "error": str(e)}
            })
            return
        
        # Initialize tree structure entry
        if current_path not in self.tree_structure:
            self.tree_structure[current_path] = {
                "name": current_path,
                "children": [],
                "has_data": False,
                "data_info": None,
                "url": url,
                "depth": depth
            }
        
        # Process data files if found
        if data_files:
            file_exts = list({os.path.splitext(f)[1] for f in data_files})
            pdf_count = sum(1 for f in data_files if f.endswith(".pdf"))
            
            self.tree_structure[current_path]["has_data"] = True
            self.tree_structure[current_path]["data_info"] = {
                "file_types": file_exts,
                "file_count": len(data_files),
                "pdf_count": pdf_count if pdf_count > 0 else None
            }
            
            # Create URL record for download pipeline
            record = {
                "url": url,
                "prefix": current_path,
                "contains": file_exts,
                "estimated_files": len(data_files),
            }
            self.url_records.append(record)
            
            self.logger.debug(f"Found data files at {current_path}", extra={
                "component": "CRAWLER",
                "structured_data": {
                    "path": current_path,
                    "file_types": file_exts,
                    "file_count": len(data_files)
                }
            })
        
        # Process subdirectories
        for folder in subfolders:
            child_name = folder.rstrip("/")
            child_path = f"{current_path}/{child_name}"
            
            if child_path not in self.tree_structure[current_path]["children"]:
                self.tree_structure[current_path]["children"].append(child_path)
            
            next_url = urljoin(url, folder)
            next_segments = path_segments + [child_name]
            self._crawl_directory(next_url, next_segments)
    
    def _generate_tree_lines(self, path: str, prefix: str = "", is_last: bool = True) -> List[str]:
        """
        Generate human-readable tree structure lines.
        
        Args:
            path: Path in tree structure
            prefix: Current line prefix
            is_last: Whether this is the last item at current level
            
        Returns:
            List of formatted tree lines
        """
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
            child_lines = self._generate_tree_lines(child, child_prefix, is_last_child)
            lines.extend(child_lines)
        
        return lines
    
    def _save_outputs(self) -> Dict[str, Path]:
        """
        Save crawling results to output files.
        
        Returns:
            Dictionary mapping output type to file path
        """
        output_files = {
            "tree": self.output_dir / "dwd_tree.txt",
            "urls": self.output_dir / "dwd_urls.jsonl",
            "structure": self.output_dir / "dwd_structure.json"
        }
        
        # Generate tree structure
        top_level_paths = sorted(p for p in self.tree_structure if "/" not in p)
        all_tree_lines = []
        for i, path in enumerate(top_level_paths):
            is_last = i == len(top_level_paths) - 1
            all_tree_lines.extend(self._generate_tree_lines(path, "", is_last))
        
        # Save tree structure
        with open(output_files["tree"], "w", encoding="utf-8") as f:
            f.write("DWD Climate Data Directory Structure\n")
            f.write("=" * 50 + "\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Base URL: {self.base_url}\n")
            f.write(f"Total directories: {len(self.tree_structure)}\n")
            f.write(f"Directories with data: {len(self.url_records)}\n")
            f.write(f"Crawl time: {time.time() - self.start_time:.2f} seconds\n\n")
            f.write("\n".join(all_tree_lines))
        
        # Save URL records (JSONL format for pipeline compatibility)
        with open(output_files["urls"], "w", encoding="utf-8") as f:
            for record in self.url_records:
                json.dump(record, f)
                f.write("\n")
        
        # Save full structure
        with open(output_files["structure"], "w", encoding="utf-8") as f:
            json.dump(self.tree_structure, f, indent=2)
        
        self.logger.info("Crawl outputs saved successfully", extra={
            "component": "CRAWLER",
            "structured_data": {
                "tree_file": str(output_files["tree"]),
                "urls_file": str(output_files["urls"]),
                "structure_file": str(output_files["structure"]),
                "url_records_count": len(self.url_records)
            }
        })
        
        return output_files
    
    def crawl_repository(self) -> CrawlResult:
        """
        Crawl the DWD repository and return results.
        
        Returns:
            CrawlResult with crawling statistics and output file paths
            
        Raises:
            Exception: If crawling fails critically
        """
        self.start_time = time.time()
        
        self.logger.info("Starting DWD repository crawl", extra={
            "component": "CRAWLER",
            "structured_data": {
                "base_url": self.base_url,
                "max_depth": self.max_depth,
                "output_dir": str(self.output_dir)
            }
        })
        
        try:
            # Get top-level directories
            root_response = self._make_request(self.base_url)
            if root_response is None:
                raise Exception(f"Failed to access base URL: {self.base_url}")
            
            subfolders, _ = self._parse_directory_listing(root_response)
            top_level_folders = [folder.rstrip("/") for folder in subfolders]
            
            self.logger.info(f"Found {len(top_level_folders)} top-level directories", extra={
                "component": "CRAWLER",
                "structured_data": {
                    "top_level_count": len(top_level_folders),
                    "directories": top_level_folders[:10]  # Log first 10 to avoid spam
                }
            })
            
            # Crawl each top-level directory
            for folder in top_level_folders:
                full_url = urljoin(self.base_url, folder + "/")
                self._crawl_directory(full_url, [folder])
            
            # Save outputs
            output_files = self._save_outputs()
            
            elapsed_time = time.time() - self.start_time
            
            result = CrawlResult(
                tree_structure=self.tree_structure,
                url_records=self.url_records,
                crawled_count=self.crawled_count,
                directories_with_data=len(self.url_records),
                elapsed_time=elapsed_time,
                output_files=output_files
            )
            
            self.logger.info("DWD repository crawl completed successfully", extra={
                "component": "CRAWLER",
                "structured_data": {
                    "total_directories": len(self.tree_structure),
                    "directories_with_data": len(self.url_records),
                    "requests_made": self.crawled_count,
                    "elapsed_time": round(elapsed_time, 2)
                }
            })
            
            return result
            
        except Exception as e:
            elapsed_time = time.time() - self.start_time
            self.logger.error("DWD repository crawl failed", extra={
                "component": "CRAWLER",
                "structured_data": {
                    "error": str(e),
                    "elapsed_time": round(elapsed_time, 2),
                    "crawled_count": self.crawled_count
                }
            })
            raise

def crawl_dwd_repository(config: Dict[str, Any], logger: StructuredLoggerAdapter) -> CrawlResult:
    """
    Crawl DWD repository using configuration and logger.
    
    This is the main function that should be used by other components to crawl
    the DWD repository. It follows the dependency injection pattern and integrates
    with the ClimaStation architecture.
    
    Args:
        config: Configuration dictionary from config manager
        logger: StructuredLoggerAdapter instance (should use CRAWLER component)
        
    Returns:
        CrawlResult with crawling statistics and output file paths
        
    Raises:
        ConfigurationError: If configuration is invalid
        Exception: If crawling fails
        
    Example:
        from app.utils.enhanced_logger import get_logger
        from app.utils.config_manager import load_config
        from app.utils.dwd_crawler import crawl_dwd_repository
        
        logger = get_logger("CRAWLER")
        config = load_config("10_minutes_air_temperature", logger)
        result = crawl_dwd_repository(config, logger)
        
        print(f"Crawled {result.directories_with_data} directories with data")
        print(f"Output saved to: {result.output_files['urls']}")
    """
    crawler = DWDRepositoryCrawler(config, logger)
    return crawler.crawl_repository()

# Example usage and testing
if __name__ == "__main__":
    import sys
    from pathlib import Path
    
    # Add parent directory to path for imports
    sys.path.append(str(Path(__file__).parent.parent))
    
    try:
        from utils.enhanced_logger import get_logger
        from utils.config_manager import load_config
        
        print("Testing ClimaStation DWD Crawler")
        print("=" * 50)
        
        # Set up logging and config
        logger = get_logger("CRAWLER")
        
        # Create test configuration
        test_config = {
            'dwd_paths': {
                'crawl_data': 'data/dwd/1_crawl_dwd',
                'debug': 'data/dwd/0_debug'
            },
            'crawler': {
                'base_url': 'https://opendata.dwd.de/climate_environment/CDC/observations_germany/',
                'max_depth': 10,  # Limited depth for testing
                'request_timeout_seconds': 30,
                'request_delay_seconds': 0.1,
                'max_retries': 3
            }
        }
        
        print("\n1. Testing DWD repository crawling:")
        print(f"   Base URL: {test_config['crawler']['base_url']}")
        print(f"   Max depth: {test_config['crawler']['max_depth']}")
        
        # Run crawler
        result = crawl_dwd_repository(test_config, logger)
        
        print(f"\n✅ Crawling completed successfully!")
        print(f"   Total directories: {len(result.tree_structure)}")
        print(f"   Directories with data: {result.directories_with_data}")
        print(f"   Requests made: {result.crawled_count}")
        print(f"   Elapsed time: {result.elapsed_time:.2f} seconds")
        
        print(f"\n📄 Output files:")
        for output_type, file_path in result.output_files.items():
            print(f"   {output_type}: {file_path}")
            if file_path.exists():
                size = file_path.stat().st_size
                print(f"      Size: {size:,} bytes")
        
        # Test URL records format
        if result.url_records:
            print(f"\n📊 Sample URL record:")
            sample_record = result.url_records[0]
            for key, value in sample_record.items():
                print(f"   {key}: {value}")
        
        print(f"\n✅ All DWD crawler tests completed successfully!")
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Make sure config_manager.py and enhanced_logger.py are available")
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
