"""
Meteorological Translation Files

YAML-based translation system for meteorological parameters, equipment,
data sources, and quality indicators.
"""

from pathlib import Path
import yaml
from typing import Dict, Any

def load_all_meteorological_translations() -> Dict[str, Any]:
    """Load all meteorological translation files."""
    translations_dir = Path(__file__).parent
    
    translations = {}
    translation_files = [
        'parameters.yaml',
        'equipment.yaml', 
        'data_sources.yaml',
        'quality_codes.yaml'
    ]
    
    for filename in translation_files:
        file_path = translations_dir / filename
        if file_path.exists():
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    category = filename.replace('.yaml', '')
                    translations[category] = yaml.safe_load(f)
            except Exception as e:
                print(f"Error loading {filename}: {e}")
                translations[category] = {}
        else:
            category = filename.replace('.yaml', '')
            translations[category] = {}
    
    return translations

__all__ = ['load_all_meteorological_translations']
