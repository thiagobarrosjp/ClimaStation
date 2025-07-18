"""
ClimaStation Translation Manager

SCRIPT IDENTIFICATION: DWDTRANS1T (Translation Manager)

PURPOSE:
Centralized translation management system for the ClimaStation platform.
Handles loading, caching, and retrieval of multilingual translations for
meteorological terms, equipment descriptions, data sources, and provider-specific
terminology across different weather data providers.

RESPONSIBILITIES:
- Load and cache translation files from YAML sources
- Provide unified API for translation lookups
- Handle fallback mechanisms for missing translations
- Support multiple languages and data providers
- Validate translation completeness and consistency
- Enable easy extension for new providers and domains

SUPPORTED CATEGORIES:
- Meteorological parameters (temperature, pressure, humidity, etc.)
- Equipment and sensor types
- Data source descriptions
- Quality codes and flags
- Provider-specific terminology

USAGE:
    translator = TranslationManager()
    
    # Get parameter translation
    temp_en = translator.get_parameter("TT_10", "en")
    temp_de = translator.get_parameter("TT_10", "de")
    
    # Get equipment translation
    sensor_en = translator.get_equipment("PT 100 (Luft)", "en")
    
    # Get bilingual metadata
    metadata = translator.get_bilingual_metadata("TT_10", "parameter")

ARCHITECTURE:
- YAML files for maintainable translation storage
- Lazy loading with caching for performance
- Hierarchical fallback system (specific → general → default)
- Extensible design for multiple providers and domains

ERROR HANDLING:
- Graceful fallback to original text if translation missing
- Detailed logging for missing translations
- Validation warnings for incomplete translation sets
"""

import yaml
from pathlib import Path
from typing import Dict, Any, Optional, List, Union
import logging
from functools import lru_cache


class TranslationManager:
    """
    Manages multilingual translations for meteorological and provider-specific terms.
    
    Provides centralized access to translations with caching, fallback mechanisms,
    and support for multiple data providers and domains.
    """
    
    def __init__(self, translations_dir: Optional[Path] = None):
        """
        Initialize the translation manager.
        
        Args:
            translations_dir: Path to translations directory (defaults to current package)
        """
        if translations_dir is None:
            translations_dir = Path(__file__).parent
        
        self.translations_dir = translations_dir
        self.logger = logging.getLogger("climastation.translations")
        
        # Cache for loaded translations
        self._cache: Dict[str, Dict[str, Any]] = {}
        
        # Supported languages
        self.supported_languages = ["en", "de"]
        self.default_language = "en"
        
        self.logger.info("Initialized TranslationManager")
    
    @lru_cache(maxsize=128)
    def _load_yaml_file(self, file_path: Path) -> Dict[str, Any]:
        """
        Load and cache a YAML translation file.
        
        Args:
            file_path: Path to YAML file
            
        Returns:
            Dictionary containing translations
        """
        try:
            if not file_path.exists():
                self.logger.warning(f"Translation file not found: {file_path}")
                return {}
            
            with open(file_path, 'r', encoding='utf-8') as f:
                content = yaml.safe_load(f) or {}
            
            self.logger.debug(f"Loaded translation file: {file_path}")
            return content
            
        except Exception as e:
            self.logger.error(f"Error loading translation file {file_path}: {e}")
            return {}
    
    def _get_translation_file(self, category: str, subcategory: Optional[str] = None) -> Path:
        """
        Get path to translation file based on category.
        
        Args:
            category: Main category (meteorological, providers)
            subcategory: Subcategory (parameters, equipment, etc.)
            
        Returns:
            Path to translation file
        """
        if subcategory:
            return self.translations_dir / category / f"{subcategory}.yaml"
        else:
            return self.translations_dir / f"{category}.yaml"
    
    def _get_translations(self, category: str, subcategory: Optional[str] = None) -> Dict[str, Any]:
        """
        Get translations for a specific category with caching.
        
        Args:
            category: Main category
            subcategory: Subcategory
            
        Returns:
            Dictionary of translations
        """
        cache_key = f"{category}_{subcategory}" if subcategory else category
        
        if cache_key not in self._cache:
            file_path = self._get_translation_file(category, subcategory)
            self._cache[cache_key] = self._load_yaml_file(file_path)
        
        return self._cache[cache_key]
    
    def get_parameter(self, parameter_code: str, language: str = "en") -> str:
        """
        Get translation for a meteorological parameter.
        
        Args:
            parameter_code: Parameter code (e.g., "TT_10", "PP_10")
            language: Target language code
            
        Returns:
            Translated parameter description
        """
        translations = self._get_translations("meteorological", "parameters")
        
        # Look for parameter in translations
        param_data = translations.get(parameter_code, {})
        
        if isinstance(param_data, dict):
            # New format with language keys
            translation = param_data.get(language)
            if translation:
                return translation
            
            # Fallback to default language
            if language != self.default_language:
                fallback = param_data.get(self.default_language)
                if fallback:
                    self.logger.warning(f"Using fallback language for parameter {parameter_code}")
                    return fallback
        
        # Fallback to original parameter code
        self.logger.warning(f"No translation found for parameter: {parameter_code}")
        return parameter_code
    
    def get_equipment(self, equipment_name: str, language: str = "en") -> str:
        """
        Get translation for equipment/sensor type.
        
        Args:
            equipment_name: Equipment name (e.g., "PT 100 (Luft)")
            language: Target language code
            
        Returns:
            Translated equipment description
        """
        translations = self._get_translations("meteorological", "equipment")
        
        # Look for equipment in translations
        equipment_data = translations.get(equipment_name, {})
        
        if isinstance(equipment_data, dict):
            translation = equipment_data.get(language)
            if translation:
                return translation
            
            # Fallback to default language
            if language != self.default_language:
                fallback = equipment_data.get(self.default_language)
                if fallback:
                    return fallback
        
        # Fallback to original equipment name
        self.logger.warning(f"No translation found for equipment: {equipment_name}")
        return equipment_name
    
    def get_data_source(self, source_description: str, language: str = "en") -> str:
        """
        Get translation for data source description.
        
        Args:
            source_description: Original data source description
            language: Target language code
            
        Returns:
            Translated data source description
        """
        translations = self._get_translations("meteorological", "data_sources")
        
        # Look for exact match first
        source_data = translations.get(source_description, {})
        
        if isinstance(source_data, dict):
            translation = source_data.get(language)
            if translation:
                return translation
        
        # Try partial matching for long descriptions
        for key, value in translations.items():
            if key in source_description and isinstance(value, dict):
                translation = value.get(language)
                if translation:
                    return translation
        
        # Fallback to original description
        self.logger.warning(f"No translation found for data source: {source_description[:50]}...")
        return source_description
    
    def get_quality_code(self, quality_code: str, language: str = "en") -> str:
        """
        Get translation for quality code.
        
        Args:
            quality_code: Quality code (e.g., "1", "2", "3")
            language: Target language code
            
        Returns:
            Translated quality description
        """
        translations = self._get_translations("meteorological", "quality_codes")
        
        quality_data = translations.get(str(quality_code), {})
        
        if isinstance(quality_data, dict):
            translation = quality_data.get(language)
            if translation:
                return translation
        
        # Fallback to original code
        return f"Quality code {quality_code}"
    
    def get_provider_term(self, provider: str, term: str, language: str = "en") -> str:
        """
        Get translation for provider-specific term.
        
        Args:
            provider: Provider name (e.g., "dwd", "noaa")
            term: Term to translate
            language: Target language code
            
        Returns:
            Translated term
        """
        translations = self._get_translations("providers", provider.lower())
        
        term_data = translations.get(term, {})
        
        if isinstance(term_data, dict):
            translation = term_data.get(language)
            if translation:
                return translation
        
        # Fallback to original term
        return term
    
    def get_bilingual_metadata(self, key: str, category: str, subcategory: Optional[str] = None) -> Dict[str, str]:
        """
        Get bilingual metadata for a term.
        
        Args:
            key: Term key to look up
            category: Translation category
            subcategory: Translation subcategory
            
        Returns:
            Dictionary with 'en', 'de', and 'original' keys
        """
        result = {"original": key}
        
        # Get translations for both languages
        for lang in self.supported_languages:
            if category == "parameter":
                result[lang] = self.get_parameter(key, lang)
            elif category == "equipment":
                result[lang] = self.get_equipment(key, lang)
            elif category == "data_source":
                result[lang] = self.get_data_source(key, lang)
            elif category == "quality_code":
                result[lang] = self.get_quality_code(key, lang)
            else:
                result[lang] = key
        
        return result
    
    def validate_translations(self) -> Dict[str, List[str]]:
        """
        Validate translation completeness and consistency.
        
        Returns:
            Dictionary with validation results and missing translations
        """
        issues = {
            "missing_translations": [],
            "incomplete_languages": [],
            "empty_files": []
        }
        
        # Check each translation category
        categories = [
            ("meteorological", "parameters"),
            ("meteorological", "equipment"),
            ("meteorological", "data_sources"),
            ("meteorological", "quality_codes"),
            ("providers", "dwd")
        ]
        
        for category, subcategory in categories:
            translations = self._get_translations(category, subcategory)
            
            if not translations:
                issues["empty_files"].append(f"{category}/{subcategory}")
                continue
            
            # Check each term for language completeness
            for term, data in translations.items():
                if isinstance(data, dict):
                    for lang in self.supported_languages:
                        if lang not in data or not data[lang]:
                            issues["missing_translations"].append(f"{category}/{subcategory}: {term} ({lang})")
                else:
                    issues["incomplete_languages"].append(f"{category}/{subcategory}: {term}")
        
        return issues
    
    def get_available_parameters(self) -> List[str]:
        """
        Get list of all available parameter codes.
        
        Returns:
            List of parameter codes
        """
        translations = self._get_translations("meteorological", "parameters")
        return list(translations.keys())
    
    def get_available_equipment(self) -> List[str]:
        """
        Get list of all available equipment types.
        
        Returns:
            List of equipment names
        """
        translations = self._get_translations("meteorological", "equipment")
        return list(translations.keys())
    
    def clear_cache(self):
        """Clear the translation cache."""
        self._cache.clear()
        self._load_yaml_file.cache_clear()
        self.logger.info("Translation cache cleared")


# Global instance for easy access
_global_translator = None

def get_translator() -> TranslationManager:
    """
    Get global translator instance (singleton pattern).
    
    Returns:
        Global TranslationManager instance
    """
    global _global_translator
    if _global_translator is None:
        _global_translator = TranslationManager()
    return _global_translator


# Example usage and testing
if __name__ == "__main__":
    try:
        # Test translation manager
        translator = TranslationManager()
        
        # Test parameter translation
        temp_en = translator.get_parameter("TT_10", "en")
        temp_de = translator.get_parameter("TT_10", "de")
        print(f"TT_10: EN='{temp_en}', DE='{temp_de}'")
        
        # Test bilingual metadata
        metadata = translator.get_bilingual_metadata("TT_10", "parameter")
        print(f"Bilingual metadata: {metadata}")
        
        # Validate translations
        issues = translator.validate_translations()
        if any(issues.values()):
            print(f"Translation issues found: {issues}")
        else:
            print("✅ All translations validated successfully")
        
        print("✅ Translation manager test successful")
        
    except Exception as e:
        print(f"❌ Translation manager test failed: {e}")
