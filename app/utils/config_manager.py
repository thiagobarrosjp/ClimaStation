"""
ClimaStation Configuration Manager

SCRIPT IDENTIFICATION: DWD10TAH2T (Configuration Manager)

PURPOSE:
Centralized configuration management for the ClimaStation platform.
Handles loading and validation of YAML configuration files for both
base system settings and dataset-specific configurations.

RESPONSIBILITIES:
- Load and parse YAML configuration files
- Provide unified access to base and dataset configurations
- Validate configuration structure and required fields
- Handle configuration file errors gracefully
- Support configuration inheritance and overrides

CONFIGURATION STRUCTURE:
- base_config.yaml: System-wide settings (paths, logging, etc.)
- {dataset_name}.yaml: Dataset-specific settings (source paths, processing options)

USAGE:
    config_manager = ConfigManager()
    base_config = config_manager.get_base_config()
    dataset_config = config_manager.get_dataset_config("10_minutes_air_temperature")

ERROR HANDLING:
- FileNotFoundError for missing configuration files
- yaml.YAMLError for malformed YAML syntax
- Detailed error messages with file paths and context

THREAD SAFETY:
Configuration loading is stateless and thread-safe.
Multiple instances can safely load configurations concurrently.
"""

import yaml
from pathlib import Path
from typing import Dict, Any


class ConfigManager:
    """
    Manages loading and accessing configuration files for the ClimaStation platform.
    
    Provides centralized access to both base system configuration and 
    dataset-specific configurations with proper error handling and validation.
    """
    
    def __init__(self, config_dir: str = "configs"):
        """
        Initialize the ConfigManager with the directory containing configuration files.
        
        Args:
            config_dir: Directory where configuration files are stored
        """
        self.config_dir = Path(config_dir)
        self.base_config_path = self.config_dir / "base_config.yaml"
    
    def _load_config(self, config_path: Path) -> Dict[str, Any]:
        """
        Load a YAML configuration file from the specified path.
        
        Args:
            config_path: Path to the YAML configuration file
            
        Returns:
            Dictionary containing the configuration data
        
        Raises:
            FileNotFoundError: If the configuration file does not exist
            yaml.YAMLError: If there is an error parsing the YAML file
        """
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
            return config or {}  # Return empty dict if file is empty
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Error parsing YAML file: {config_path}\n{e}")
    
    def _load_base_config(self) -> Dict[str, Any]:
        """
        Load the base configuration.
        
        Returns:
            Dictionary containing the base configuration
        """
        return self._load_config(self.base_config_path)
    
    def _load_dataset_config(self, dataset_name: str) -> Dict[str, Any]:
        """
        Load the configuration for a specific dataset.
        
        Args:
            dataset_name: Name of the dataset
            
        Returns:
            Dictionary containing the dataset configuration
        
        Raises:
            FileNotFoundError: If the dataset configuration file does not exist
        """
        dataset_config_path = self.config_dir / f"{dataset_name}.yaml"
        return self._load_config(dataset_config_path)
    
    def get_base_config(self) -> Dict[str, Any]:
        """
        Get the base configuration.
        
        Returns:
            Dictionary containing base configuration
        """
        return self._load_base_config()
    
    def get_dataset_config(self, dataset_name: str) -> Dict[str, Any]:
        """
        Get configuration for a specific dataset.
        
        Args:
            dataset_name: Name of the dataset
        
        Returns:
            Dictionary containing dataset configuration
        """
        return self._load_dataset_config(dataset_name)
    
    def validate_base_config(self, config: Dict[str, Any]) -> bool:
        """
        Validate that base configuration has required fields.
        
        Args:
            config: Base configuration dictionary
            
        Returns:
            True if configuration is valid
            
        Raises:
            ValueError: If required fields are missing
        """
        required_fields = ['paths', 'logging']
        required_paths = ['progress_db', 'temp_dir']
        
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required field in base config: {field}")
        
        for path_field in required_paths:
            if path_field not in config.get('paths', {}):
                raise ValueError(f"Missing required path in base config: paths.{path_field}")
        
        return True
    
    def validate_dataset_config(self, config: Dict[str, Any], dataset_name: str) -> bool:
        """
        Validate that dataset configuration has required fields.
        
        Args:
            config: Dataset configuration dictionary
            dataset_name: Name of the dataset for error messages
            
        Returns:
            True if configuration is valid
            
        Raises:
            ValueError: If required fields are missing
        """
        required_fields = ['source', 'output', 'processing']
        
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required field in {dataset_name} config: {field}")
        
        # Validate source configuration
        if 'base_path' not in config.get('source', {}):
            raise ValueError(f"Missing required field in {dataset_name} config: source.base_path")
        
        # Validate output configuration
        if 'base_path' not in config.get('output', {}):
            raise ValueError(f"Missing required field in {dataset_name} config: output.base_path")
        
        return True
    
    def get_validated_base_config(self) -> Dict[str, Any]:
        """
        Get base configuration with validation.
        
        Returns:
            Validated base configuration dictionary
            
        Raises:
            ValueError: If configuration is invalid
        """
        config = self.get_base_config()
        self.validate_base_config(config)
        return config
    
    def get_validated_dataset_config(self, dataset_name: str) -> Dict[str, Any]:
        """
        Get dataset configuration with validation.
        
        Args:
            dataset_name: Name of the dataset
            
        Returns:
            Validated dataset configuration dictionary
            
        Raises:
            ValueError: If configuration is invalid
        """
        config = self.get_dataset_config(dataset_name)
        self.validate_dataset_config(config, dataset_name)
        return config


# Example usage and testing
if __name__ == "__main__":
    try:
        # Test configuration manager
        config_manager = ConfigManager()
        
        # Test base config loading
        base_config = config_manager.get_base_config()
        print(f"✅ Loaded base config: {len(base_config)} sections")
        
        # Test dataset config loading
        dataset_config = config_manager.get_dataset_config("10_minutes_air_temperature")
        print(f"✅ Loaded dataset config: {len(dataset_config)} sections")
        
        print("✅ Configuration manager test successful")
        
    except Exception as e:
        print(f"❌ Configuration manager test failed: {e}")
