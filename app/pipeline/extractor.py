from __future__ import annotations
from pathlib import Path
from typing import Iterable
from .types import SourceMeta

__all__ = ["extract_lines"]

def extract_lines(zip_path: Path) -> tuple[SourceMeta, Iterable[tuple[int, str]]]:
    """
    Contract v1 — see docs/contracts/extractor.md

    Returns:
        (meta, lines): meta is SourceMeta; lines is an iterable of (row_no, raw_line).
    """
    # Walking skeleton: compile-time stub. Implement in Slice A.
    raise NotImplementedError("Extractor not implemented yet")
