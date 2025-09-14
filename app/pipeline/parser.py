from __future__ import annotations
from datetime import datetime
from typing import Iterable
from .types import SourceMeta, ParsedRow

__all__ = ["parse_air_temperature_10min"]

def parse_air_temperature_10min(
    lines: Iterable[tuple[int, str]],
    meta: SourceMeta,
    *,
    now_utc: datetime,
) -> Iterable[ParsedRow]:
    """
    Contract v1 — see docs/contracts/parser.md

    Yields:
        ParsedRow dicts (normalized), streaming.
    """
    # Walking skeleton: empty generator for now; implement in Slice B.
    yield from ()
