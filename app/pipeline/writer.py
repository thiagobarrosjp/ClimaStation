from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Tuple
from .types import ParsedRow

__all__ = ["WriterReport", "write_parquet"]

@dataclass(frozen=True, slots=True)
class WriterReport:
    rows_in: int = 0
    rows_written: int = 0
    files: int = 0
    output_paths: Tuple[Path, ...] = ()

def write_parquet(rows: Iterable[ParsedRow], out_root: Path) -> WriterReport:
    """
    Contract v1 — see docs/contracts/writer.md

    Writes Parquet and a JSON manifest per file (later).
    """
    # Walking skeleton: do nothing yet; implement in Slice C.
    return WriterReport()
