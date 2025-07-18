"""
ClimaStation Configuration Manager

SCRIPT IDENTIFICATION: DWD10TAH3C (Configuration Manager)

PURPOSE:
Centralized configuration loading and management for the ClimaStation platform.
Handles layered configuration (base + dataset-specific) and provides typed
access to configuration values.

RESPONSIBILITIES:
- Load and parse YAML configuration files
- Merge base configuration with dataset-specific overrides
- Provide typed configuration objects for different components
- Validate configuration completeness and correctness
- Handle configuration file errors gracefully

USAGE:
    config_manager = ConfigManager()
    processing_config = config_manager.get_processing_config()
    dataset_config = config_manager.load_dataset_config("10_minutes_air_temperature")

CONFIGURATION HIERARCHY:
1. base_config.yaml - Global settings for all processing
2. datasets/{name}.yaml - Dataset-specific overrides and settings
3. Environment variables (future) - Runtime overrides

ERROR HANDLING:
- Missing configuration files raise ConfigurationError
- Invalid YAML syntax raises ConfigurationError  
- Missing required configuration keys raise ConfigurationError
- Provides detailed error messages for troubleshooting

DEPENDENCIES:
- PyYAML for configuration file parsing
- pathlib for file path handling
- dataclasses for typed configuration objects
"""

import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass

class ConfigurationError(Exception):
    """Raised when configuration loading or validation fails"""
    pass

@dataclass
class ProcessingConfig:
    """Typed configuration for processing parameters"""
    max_workers: int
    memory_limit_mb: int
    worker_timeout_minutes: int
    checkpoint_interval: int

@dataclass
class PathConfig:
    """Typed configuration for file system paths"""
    raw_data: Path
    output: Path
    logs: Path
    progress_db: Path

@dataclass
class FailureHandlingConfig:
    """Typed configuration for error handling behavior"""
    stop_on_dataset_failure: bool
    max_retries: int
    retry_delay_seconds: int

class ConfigManager:
    """
    Central configuration management for ClimaStation.
    
    Loads and provides access to both base configuration and dataset-specific
    configuration with proper error handling and validation.
    """
    
    def __init__(self, config_root: Optional[Path] = None):
        """
        Initialize configuration manager.
        
        Args:
            config_root: Root directory for configuration files (defaults to ./config)
        """
        self.config_root = config_root or Path("config")
        self.base_config = self._load_base_config()
        
    def _load_base_config(self) -> Dict[str, Any]:
        """
        Load base configuration file.
        
        Returns:
            Dictionary containing base configuration
            
        Raises:
            ConfigurationError: If base config file is missing or invalid
        """
        config_path = self.config_root / "base_config.yaml"
        
        if not config_path.exists():
            raise ConfigurationError(f"Base configuration file not found: {config_path}")
            
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                
            if not config:
                raise ConfigurationError(f"Base configuration file is empty: {config_path}")
                
            return config
            
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML in base configuration: {e}")
        except Exception as e:
            raise ConfigurationError(f"Error loading base configuration: {e}")
    
    def load_dataset_config(self, dataset_name: str) -> Dict[str, Any]:
        """
        Load dataset-specific configuration.
        
        Args:
            dataset_name: Name of the dataset (e.g., "10_minutes_air_temperature")
            
        Returns:
            Dictionary containing dataset configuration
            
        Raises:
            ConfigurationError: If dataset config file is missing or invalid
        """
        config_path = self.config_root / "datasets" / f"{dataset_name}.yaml"
        
        if not config_path.exists():
            raise ConfigurationError(f"Dataset configuration file not found: {config_path}")
            
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                
            if not config:
                raise ConfigurationError(f"Dataset configuration file is empty: {config_path}")
                
            return config
            
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML in dataset configuration: {e}")
        except Exception as e:
            raise ConfigurationError(f"Error loading dataset configuration: {e}")
    
    def get_processing_config(self) -> ProcessingConfig:
        """
        Get typed processing configuration.
        
        Returns:
            ProcessingConfig object with processing parameters
            
        Raises:
            ConfigurationError: If required processing configuration is missing
        """
        try:
            proc = self.base_config['processing']
            return ProcessingConfig(
                max_workers=proc['max_workers'],
                memory_limit_mb=proc['memory_limit_mb'],
                worker_timeout_minutes=proc['worker_timeout_minutes'],
                checkpoint_interval=proc['checkpoint_interval']
            )
        except KeyError as e:
            raise ConfigurationError(f"Missing required processing configuration: {e}")
    
    def get_path_config(self) -> PathConfig:
        """
        Get typed path configuration.
        
        Returns:
            PathConfig object with file system paths
            
        Raises:
            ConfigurationError: If required path configuration is missing
        """
        try:
            paths = self.base_config['paths']
            return PathConfig(
                raw_data=Path(paths['raw_data']),
                output=Path(paths['output']),
                logs=Path(paths['logs']),
                progress_db=Path(paths['progress_db'])
            )
        except KeyError as e:
            raise ConfigurationError(f"Missing required path configuration: {e}")
    
    def get_failure_handling_config(self) -> FailureHandlingConfig:
        """
        Get typed failure handling configuration.
        
        Returns:
            FailureHandlingConfig object with error handling parameters
            
        Raises:
            ConfigurationError: If required failure handling configuration is missing
        """
        try:
            failure = self.base_config['failure_handling']
            return FailureHandlingConfig(
                stop_on_dataset_failure=failure['stop_on_dataset_failure'],
                max_retries=failure['max_retries'],
                retry_delay_seconds=failure['retry_delay_seconds']
            )
        except KeyError as e:
            raise ConfigurationError(f"Missing required failure handling configuration: {e}")
    
    def get_logging_config(self) -> Dict[str, Any]:
        """
        Get logging configuration.
        
        Returns:
            Dictionary containing logging configuration
            
        Raises:
            ConfigurationError: If required logging configuration is missing
        """
        try:
            return self.base_config['logging']
        except KeyError as e:
            raise ConfigurationError(f"Missing required logging configuration: {e}")


# Example usage and testing
if __name__ == "__main__":
    try:
        config_manager = ConfigManager()
        
        # Test base configuration loading
        processing_config = config_manager.get_processing_config()
        print(f"Max workers: {processing_config.max_workers}")
        
        path_config = config_manager.get_path_config()
        print(f"Raw data path: {path_config.raw_data}")
        
        # Test dataset configuration loading
        dataset_config = config_manager.load_dataset_config("10_minutes_air_temperature")
        print(f"Dataset name: {dataset_config['dataset']['name']}")
        
        print("✅ Configuration manager test successful")
        
    except ConfigurationError as e:
        print(f"❌ Configuration error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")