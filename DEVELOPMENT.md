# ClimaStation Development Standards

**Last Updated:** 2025-12-10  
**Version:** 1.3

This document defines development standards for the ClimaStation project. Claude Code and all contributors should follow these guidelines.

---

## Table of Contents

0. [Getting Started](#0-getting-started)
1. [Project Structure](#1-project-structure)
2. [Code Documentation](#2-code-documentation)
3. [Testing Standards](#3-testing-standards)
4. [Git Practices](#4-git-practices)
5. [Claude Code Instructions](#5-claude-code-instructions)
6. [Error Handling](#6-error-handling)
7. [Dependencies](#7-dependencies)

---

## 0. Getting Started

**Read these files in order when starting a new session:**

| Order | File | Purpose |
|-------|------|---------|
| 1 | `STATUS.md` | Current progress, what to work on next |
| 2 | `docs/ClimaStation_Context.md` | Project overview, architecture, decisions |
| 3 | `docs/processing-details.md` | Data transformation specifications |
| 4 | `DEVELOPMENT.md` | This file — coding standards |
| 5 | Relevant `schemas/*.yaml` | Schema for the specific task |

**Then:**
- Check `STATUS.md` "Next Up" section for current tasks
- Update `STATUS.md` when tasks are completed
- Note any blockers or questions in `STATUS.md`

---

## 1. Project Structure

```
climastation/
├── README.md                 # Project overview, setup instructions
├── CHANGELOG.md              # Version history
├── DEVELOPMENT.md            # This file - development standards
├── STATUS.md                 # ✅ Current progress, next tasks (Claude Code updates)
├── requirements.txt          # Python dependencies
│
├── docs/                     # 🔒 READ-ONLY for Claude Code
│   ├── ClimaStation_Context.md
│   └── processing-details.md
│
├── schemas/                  # 🔒 READ-ONLY for Claude Code
│   ├── dwd_10min_air_temperature_schema.yaml
│   ├── dwd_10min_air_temperature_metadata_schema.yaml
│   └── dwd_10min_air_temperature_aggregates_schema.yaml
│
├── reference/                # 🔒 READ-ONLY for Claude Code
│   ├── station_bundesland.csv
│   └── translations.json
│
├── src/                      # ✅ Claude Code works here
│   ├── __init__.py
│   ├── parsers/
│   │   ├── __init__.py
│   │   ├── raw_data.py
│   │   ├── geography.py
│   │   ├── parameters.py
│   │   ├── devices.py
│   │   └── station_info.py
│   ├── converters/
│   │   ├── __init__.py
│   │   └── jsonl_to_parquet.py
│   ├── aggregators/
│   │   ├── __init__.py
│   │   └── compute_aggregates.py
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── timestamps.py
│   │   └── translations.py
│   └── api/
│       ├── __init__.py
│       └── main.py
│
├── frontend/                 # ✅ Claude Code works here
│   └── ...                   # React application
│
├── tests/                    # ✅ Claude Code works here
│   ├── fixtures/             # Sample DWD files for testing
│   │   ├── Metadaten_Geographie_00003.txt
│   │   ├── Metadaten_Parameter_00003.txt
│   │   └── ...
│   ├── test_parsers.py
│   ├── test_converters.py
│   └── test_aggregators.py
│
├── data/                     # Data directory (not in git)
│   ├── downloads/            # Raw DWD ZIP files
│   ├── temp/                 # JSONL intermediate files
│   └── output/               # Final Parquet files
│
└── .gitignore
```

### File Permissions for Claude Code

| Directory | Permission | Reason |
|-----------|------------|--------|
| `STATUS.md` | ✅ Read-write | Progress tracking, update when tasks complete |
| `docs/` | 🔒 Read-only | Specifications, ask before modifying |
| `schemas/` | 🔒 Read-only | YAML contracts, ask before modifying |
| `reference/` | 🔒 Read-only | Lookup tables, ask before modifying |
| `src/` | ✅ Read-write | Main development area |
| `frontend/` | ✅ Read-write | Frontend development |
| `tests/` | ✅ Read-write | Test files |
| `data/` | ✅ Read-write | Data processing |

---

## 2. Code Documentation

### Module Docstrings

Every Python file starts with a docstring explaining its purpose:

```python
"""
Geography metadata parser.

Parses Metadaten_Geographie_XXXXX.txt files from DWD ZIP archives
and returns normalized intervals for stations.jsonl.

Reference: docs/processing-details.md, section "Metadaten_Geographie"
"""
```

### Function Docstrings

Every function has a docstring with Args, Returns, and Raises (if applicable):

```python
def parse_geography_file(filepath: str) -> list[dict]:
    """
    Parse a DWD geography metadata file into intervals.
    
    Args:
        filepath: Path to Metadaten_Geographie_XXXXX.txt
        
    Returns:
        List of interval dicts with keys: stations_id, valid_from, 
        valid_to, latitude, longitude, elevation_m, station_name
        
    Raises:
        ValueError: If file format is unexpected
        FileNotFoundError: If filepath does not exist
        
    Example:
        >>> intervals = parse_geography_file("data/Metadaten_Geographie_00003.txt")
        >>> len(intervals)
        6
    """
```

### Type Hints

Use Python type hints for all function signatures:

```python
from datetime import datetime
from typing import Optional
from pathlib import Path


def convert_timestamp(
    mess_datum: int, 
    era: str
) -> tuple[datetime, datetime]:
    """Convert MESS_DATUM to MEZ and UTC timestamps."""
    ...


def load_translations(filepath: Path | str) -> dict[str, dict[str, str]]:
    """Load translations from JSON file."""
    ...
```

### Inline Comments

Use sparingly. Explain *why*, not *what*:

```python
# Good: explains WHY
# MEZ is UTC+1 fixed (no daylight saving), so we subtract 1 hour
timestamp_utc = timestamp_mez - timedelta(hours=1)

# Bad: explains WHAT (obvious from code)
# Subtract one hour from timestamp
timestamp_utc = timestamp_mez - timedelta(hours=1)
```

---

## 3. Testing Standards

### Test Files

Each module in `src/` should have corresponding tests:

```
src/parsers/geography.py  →  tests/test_parsers.py::test_geography_*
src/converters/jsonl_to_parquet.py  →  tests/test_converters.py
```

### Test Fixtures

Sample DWD files for station 3 are stored in `tests/fixtures/`:

```
tests/fixtures/
├── Metadaten_Geographie_00003.txt
├── Metadaten_Parameter_00003.txt
├── Metadaten_Stationsname_Betreibername_00003.txt
├── Metadaten_Geraete_Lufttemperatur_00003.txt
├── Metadaten_Geraete_Momentane_Temperatur_In_5cm_00003.txt
├── Metadaten_Geraete_Rel_Feuchte_00003.txt
└── produkt_zehn_min_tu_19930428_19991231_00003.txt (sample rows)
```

### Test Structure

```python
# tests/test_parsers.py

import pytest
from datetime import datetime
from src.parsers.geography import parse_geography_file


class TestGeographyParser:
    """Tests for geography metadata parser."""
    
    def test_station_3_interval_count(self):
        """Station 3 should have 6 geography intervals."""
        result = parse_geography_file("tests/fixtures/Metadaten_Geographie_00003.txt")
        assert len(result) == 6
    
    def test_station_3_first_interval(self):
        """First interval should start 1891-01-01."""
        result = parse_geography_file("tests/fixtures/Metadaten_Geographie_00003.txt")
        assert result[0]["valid_from"] == datetime(1891, 1, 1, 0, 0, 0)
        assert result[0]["latitude"] == 50.7833
        assert result[0]["longitude"] == 6.0833
    
    def test_invalid_file_raises_error(self):
        """Parser should fail fast on invalid input."""
        with pytest.raises(ValueError):
            parse_geography_file("tests/fixtures/invalid_file.txt")
```

### Testing Workflow

When creating new code:

1. Write the code
2. Write tests for expected behavior
3. Run tests: `pytest tests/ -v`
4. Fix any failures
5. Show test output before marking complete

### Validation During Development

For quick validation during development, include a `__main__` block:

```python
if __name__ == "__main__":
    # Quick test with real file
    result = parse_geography_file("tests/fixtures/Metadaten_Geographie_00003.txt")
    print(f"Found {len(result)} intervals")
    for i, interval in enumerate(result):
        print(f"  {i+1}. {interval['valid_from']} to {interval['valid_to']}")
        print(f"      lat={interval['latitude']}, lon={interval['longitude']}")
```

---

## 4. Git Practices

### Commit Messages

Use clear, imperative messages:

```
Good:
  Add geography metadata parser
  Fix timestamp conversion for MEZ era
  Add tests for parameter parser
  
Bad:
  added stuff
  WIP
  fix
  updates
```

### Commit Size

One logical change per commit:

```
Good:
  Commit 1: "Add geography parser"
  Commit 2: "Add tests for geography parser"
  Commit 3: "Add parameter parser"
  
Bad:
  Commit 1: "Add geography parser, parameter parser, tests, and fix bug"
```

### Workflow

1. Make changes
2. Review changes: `git diff`
3. Stage changes: `git add <files>`
4. Review staged: `git diff --staged`
5. Commit: `git commit -m "Clear message"`
6. Push: `git push origin main`

### What NOT to Commit

The `.gitignore` should include:

```
# Data files (too large)
data/downloads/
data/temp/
data/output/

# Python
__pycache__/
*.pyc
.venv/
venv/

# IDE
.vscode/
.idea/

# OS
.DS_Store
Thumbs.db

# Environment
.env
```

---

## 5. Claude Code Instructions

At the start of each Claude Code session, these rules apply:

### File Access

```
READ-ONLY (do not modify without asking):
- docs/ClimaStation_Context.md
- docs/processing-details.md
- schemas/*.yaml
- reference/*.csv
- reference/*.json
- DEVELOPMENT.md

WRITABLE (update as part of workflow):
- STATUS.md (mark tasks done, add blockers)
- src/**
- tests/**
- frontend/**

If you find issues or conflicts with READ-ONLY files:
1. Do NOT modify them
2. Explain the issue
3. Wait for approval before making changes
```

### Work Incrementally

**Do NOT produce large blocks of code all at once.**

Work in small, understandable steps:

```
For each task:
1. Implement ONE piece (one function, one small file)
2. Show what you created
3. Explain briefly what it does
4. Run/test it
5. Wait for OK before continuing

Example — building a parser:
  Step 1: Parse one metadata file → show output → wait for OK
  Step 2: Add translation lookup → show output → wait for OK
  Step 3: Add error handling → show output → wait for OK
  
NOT: Create 6 parsers with 500 lines → "Done!"
```

This ensures:
- Owner understands every piece of code
- Mistakes are caught early
- Progress is visible and verifiable

### Development Workflow

```
When writing code:
1. Read relevant specs from docs/ and schemas/
2. Write code following standards in this file
3. Add appropriate tests
4. Run tests and show output
5. Fix any errors
6. Show final result before marking complete

When ready to commit:
1. Stage changes: git add <files>
2. Show diff: git diff --staged
3. WAIT for approval before committing
4. Do NOT push without explicit approval
```

### Asking for Clarification

```
If requirements are unclear:
- Ask before implementing
- Don't guess at business logic
- Reference specific sections of docs if confused
```

---

## 6. Error Handling

### Fail Fast

Stop on errors, don't silently skip:

```python
# Good: fail fast
def parse_row(line: str) -> dict:
    parts = line.strip().split(";")
    if len(parts) != 8:
        raise ValueError(f"Expected 8 columns, got {len(parts)}: {line}")
    ...

# Bad: silent skip
def parse_row(line: str) -> dict | None:
    parts = line.strip().split(";")
    if len(parts) != 8:
        return None  # Problem hidden!
    ...
```

### Clear Error Messages

Include context in error messages:

```python
# Good: context included
raise ValueError(
    f"Overlapping intervals in Metadaten_Parameter_00003.txt for TT_10: "
    f"interval ending {end_date} overlaps with interval starting {start_date}"
)

# Bad: no context
raise ValueError("Invalid data")
```

### Logging

Use logging for non-fatal warnings:

```python
import logging

logger = logging.getLogger(__name__)

# Warning for data quality issues
if not metadata_matched:
    logger.warning(
        f"Orphan data: station {station_id}, timestamp {timestamp} "
        f"has no matching metadata interval"
    )
```

---

## 7. Dependencies

### Python Version

Python 3.11 or higher.

### Core Dependencies

```
# requirements.txt

# Data processing
pandas>=2.0.0
pyarrow>=14.0.0
duckdb>=0.9.0

# API
fastapi>=0.104.0
uvicorn>=0.24.0

# Testing
pytest>=7.4.0

# Utilities
pyyaml>=6.0.0
python-dateutil>=2.8.0
```

### Installing Dependencies

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or: venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt
```

---

## Version History

| Version | Date | Summary |
|---------|------|---------|
| 1.3 | 2025-12-10 | Added "Work Incrementally" section — small steps, wait for OK |
| 1.2 | 2025-12-10 | STATUS.md at root level in project structure |
| 1.1 | 2025-12-10 | Added Getting Started section, clarified STATUS.md is writable |
| 1.0 | 2025-12-09 | Initial development standards |
