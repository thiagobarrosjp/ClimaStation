"""
ClimaStation Configuration Manager

SCRIPT IDENTIFICATION: DWD10TAH3C (Configuration Manager)

PURPOSE:
Centralized configuration loading and management for the ClimaStation platform.
Handles layered configuration (base + dataset-specific) and provides typed
access to configuration values with caching and proper error handling.

RESPONSIBILITIES:
- Load and parse YAML configuration files
- Merge base configuration with dataset-specific overrides
- Provide typed configuration objects for different components
- Cache configurations to avoid repeated file reads
- Convert string paths to Path objects automatically
- Handle configuration file errors gracefully with proper logging

USAGE:
    logger = get_logger("config_test")
    config = load_config("10_minutes_air_temperature", logger)
    paths = get_data_paths(logger)

CONFIGURATION HIERARCHY:
1. base_config.yaml - Global settings for all processing
2. datasets/{name}.yaml - Dataset-specific overrides and settings
3. Merged result with dataset settings taking precedence

ERROR HANDLING:
- Missing configuration files raise ConfigurationError with CONFIG component logging
- Invalid YAML syntax raises ConfigurationError with detailed messages
- Missing required configuration keys raise ConfigurationError
- Graceful handling of missing dataset configs (falls back to base only)
"""

import yaml
from pathlib import Path
from typing import Dict, Any, Optional
import logging
from functools import lru_cache

class ConfigurationError(Exception):
    """Raised when configuration loading or validation fails"""
    pass

# Global cache for configurations to avoid repeated file reads
_config_cache: Dict[str, Optional[Dict[str, Any]]] = {}
_base_config_cache: Optional[Dict[str, Any]] = None

def _load_base_config(logger: logging.Logger, config_root: Optional[Path] = None) -> Dict[str, Any]:
    """
    Load base configuration file with caching.
    
    Args:
        logger: Logger instance for error reporting
        config_root: Root directory for configuration files (defaults to app/config)
        
    Returns:
        Dictionary containing base configuration
        
    Raises:
        ConfigurationError: If base config file is missing or invalid
    """
    global _base_config_cache
    
    if _base_config_cache is not None:
        return _base_config_cache
    
    config_root = config_root or Path("app/config")
    config_path = config_root / "base_config.yaml"
    
    if not config_path.exists():
        error_msg = f"Base configuration file not found: {config_path}"
        logger.error(error_msg, extra={"component": "CONFIG"})
        raise ConfigurationError(error_msg)
        
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            
        if not config:
            error_msg = f"Base configuration file is empty: {config_path}"
            logger.error(error_msg, extra={"component": "CONFIG"})
            raise ConfigurationError(error_msg)
            
        # Convert string paths to Path objects
        if 'paths' in config:
            for key, value in config['paths'].items():
                if isinstance(value, str):
                    config['paths'][key] = Path(value)
        
        _base_config_cache = config
        logger.info(f"Base configuration loaded successfully from {config_path}", 
                   extra={"component": "CONFIG"})
        return config
        
    except yaml.YAMLError as e:
        error_msg = f"Invalid YAML in base configuration: {e}"
        logger.error(error_msg, extra={"component": "CONFIG"})
        raise ConfigurationError(error_msg)
    except Exception as e:
        error_msg = f"Error loading base configuration: {e}"
        logger.error(error_msg, extra={"component": "CONFIG"})
        raise ConfigurationError(error_msg)

def _load_dataset_config(dataset_name: str, logger: logging.Logger, 
                        config_root: Optional[Path] = None) -> Optional[Dict[str, Any]]:
    """
    Load dataset-specific configuration with caching.
    
    Args:
        dataset_name: Name of the dataset (e.g., "10_minutes_air_temperature")
        logger: Logger instance for error reporting
        config_root: Root directory for configuration files (defaults to app/config)
        
    Returns:
        Dictionary containing dataset configuration or None if not found
        
    Raises:
        ConfigurationError: If dataset config file exists but is invalid
    """
    if dataset_name in _config_cache:
        return _config_cache[dataset_name]
    
    config_root = config_root or Path("app/config")
    config_path = config_root / "datasets" / f"{dataset_name}.yaml"
    
    if not config_path.exists():
        logger.warning(f"Dataset configuration file not found: {config_path}. Using base config only.", 
                      extra={"component": "CONFIG"})
        _config_cache[dataset_name] = None
        return None
        
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            
        if not config:
            error_msg = f"Dataset configuration file is empty: {config_path}"
            logger.error(error_msg, extra={"component": "CONFIG"})
            raise ConfigurationError(error_msg)
            
        # Convert string paths to Path objects in dataset config
        if 'paths' in config:
            for key, value in config['paths'].items():
                if isinstance(value, str):
                    config['paths'][key] = Path(value)
                    
        _config_cache[dataset_name] = config
        logger.info(f"Dataset configuration loaded successfully: {dataset_name}", 
                   extra={"component": "CONFIG"})
        return config
        
    except yaml.YAMLError as e:
        error_msg = f"Invalid YAML in dataset configuration {dataset_name}: {e}"
        logger.error(error_msg, extra={"component": "CONFIG"})
        raise ConfigurationError(error_msg)
    except Exception as e:
        error_msg = f"Error loading dataset configuration {dataset_name}: {e}"
        logger.error(error_msg, extra={"component": "CONFIG"})
        raise ConfigurationError(error_msg)

def load_config(dataset_name: str, logger: logging.Logger, 
                config_root: Optional[Path] = None) -> Dict[str, Any]:
    """
    Load merged configuration (base + dataset-specific).
    
    This is one of the required functions from available_functions.py.
    Loads base configuration and merges it with dataset-specific settings,
    with dataset settings taking precedence over base settings.
    
    Args:
        dataset_name: Name of the dataset (e.g., "10_minutes_air_temperature")
        logger: Logger instance for error reporting
        config_root: Root directory for configuration files (defaults to app/config)
        
    Returns:
        Dictionary containing merged configuration with Path objects for paths
        
    Raises:
        ConfigurationError: If base config is missing or invalid, or if dataset config is invalid
    """
    try:
        # Load base configuration (required)
        base_config = _load_base_config(logger, config_root)
        
        # Load dataset configuration (optional)
        dataset_config = _load_dataset_config(dataset_name, logger, config_root)
        
        if dataset_config is None:
            logger.info(f"Using base configuration only for dataset: {dataset_name}", 
                       extra={"component": "CONFIG"})
            return base_config.copy()
        
        # Merge configurations - dataset settings override base settings
        merged_config = base_config.copy()
        
        # Deep merge for nested dictionaries
        for key, value in dataset_config.items():
            if key in merged_config and isinstance(merged_config[key], dict) and isinstance(value, dict):
                merged_config[key] = {**merged_config[key], **value}
            else:
                merged_config[key] = value
        
        logger.info(f"Configuration merged successfully for dataset: {dataset_name}", 
                   extra={"component": "CONFIG"})
        return merged_config
        
    except Exception as e:
        error_msg = f"Failed to load configuration for dataset {dataset_name}: {str(e)}"
        logger.error(error_msg, extra={"component": "CONFIG"})
        raise ConfigurationError(error_msg) from e

def get_data_paths(logger: logging.Logger, config_root: Optional[Path] = None) -> Dict[str, Path]:
    """
    Get standardized data directory paths.
    
    This is one of the required functions from available_functions.py.
    Returns all configured paths as Path objects for consistent file handling.
    
    Args:
        logger: Logger instance for error reporting
        config_root: Root directory for configuration files (defaults to app/config)
        
    Returns:
        Dictionary mapping path names to Path objects
        
    Raises:
        ConfigurationError: If base config is missing or paths section is invalid
    """
    try:
        base_config = _load_base_config(logger, config_root)
        
        if 'paths' not in base_config:
            error_msg = "Missing 'paths' section in base configuration"
            logger.error(error_msg, extra={"component": "CONFIG"})
            raise ConfigurationError(error_msg)
        
        paths = base_config['paths']
        
        # Ensure all paths are Path objects
        path_objects = {}
        for key, value in paths.items():
            if isinstance(value, (str, Path)):
                path_objects[key] = Path(value)
            else:
                error_msg = f"Invalid path configuration for '{key}': {value}"
                logger.error(error_msg, extra={"component": "CONFIG"})
                raise ConfigurationError(error_msg)
        
        logger.info(f"Data paths loaded successfully: {list(path_objects.keys())}", 
                   extra={"component": "CONFIG"})
        return path_objects
        
    except Exception as e:
        error_msg = f"Failed to load data paths: {str(e)}"
        logger.error(error_msg, extra={"component": "CONFIG"})
        raise ConfigurationError(error_msg) from e

def clear_config_cache():
    """
    Clear the configuration cache.
    
    Useful for development/testing when configuration files are modified
    and need to be reloaded.
    """
    global _config_cache, _base_config_cache
    _config_cache.clear()
    _base_config_cache = None

# Legacy ConfigManager class for backward compatibility
class ConfigManager:
    """
    Legacy configuration manager class.
    
    Maintained for backward compatibility with existing code.
    New code should use the load_config() and get_data_paths() functions directly.
    """
    
    def __init__(self, config_root: Optional[Path] = None, logger: Optional[logging.Logger] = None):
        """
        Initialize configuration manager.
        
        Args:
            config_root: Root directory for configuration files (defaults to app/config)
            logger: Logger instance (creates basic logger if not provided)
        """
        self.config_root = config_root or Path("app/config")
        self.logger = logger or logging.getLogger("climastation.config")
        
    def load_dataset_config(self, dataset_name: str) -> Dict[str, Any]:
        """Load merged configuration for a dataset"""
        return load_config(dataset_name, self.logger, self.config_root)
    
    def get_data_paths(self) -> Dict[str, Path]:
        """Get standardized data directory paths"""
        return get_data_paths(self.logger, self.config_root)

# Example usage and testing
if __name__ == "__main__":
    # Set up basic logging for testing
    logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
    logger = logging.getLogger("config_test")
    
    try:
        print("Testing ClimaStation Configuration Manager")
        print("=" * 50)
        
        # Test data paths loading
        print("\n1. Testing get_data_paths():")
        paths = get_data_paths(logger)
        for name, path in paths.items():
            print(f"   {name}: {path} (type: {type(path).__name__})")
        
        # Test base configuration only
        print("\n2. Testing load_config() with non-existent dataset:")
        config = load_config("non_existent_dataset", logger)
        print(f"   Loaded config keys: {list(config.keys())}")
        
        # Test dataset-specific configuration
        print("\n3. Testing load_config() with 10-minute air temperature dataset:")
        config = load_config("10_minutes_air_temperature", logger)
        print(f"   Loaded config keys: {list(config.keys())}")
        
        if 'dataset' in config:
            print(f"   Dataset name: {config['dataset']['name']}")
            print(f"   Dataset script code: {config['dataset']['script_code']}")
        
        if 'processing' in config:
            print(f"   Max workers: {config['processing']['max_workers']}")
            if 'parallel_folders' in config['processing']:
                print(f"   Parallel folders: {config['processing']['parallel_folders']}")
        
        # Test caching
        print("\n4. Testing configuration caching:")
        config2 = load_config("10_minutes_air_temperature", logger)
        print(f"   Same object reference: {config is config2}")
        
        # Test legacy ConfigManager
        print("\n5. Testing legacy ConfigManager:")
        config_manager = ConfigManager(logger=logger)
        legacy_config = config_manager.load_dataset_config("10_minutes_air_temperature")
        legacy_paths = config_manager.get_data_paths()
        print(f"   Legacy config keys: {list(legacy_config.keys())}")
        print(f"   Legacy paths: {list(legacy_paths.keys())}")
        
        print("\n✅ All configuration tests passed successfully!")
        
    except ConfigurationError as e:
        print(f"\n❌ Configuration error: {e}")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
