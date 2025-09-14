import inspect
from pathlib import Path
from datetime import datetime, UTC  # Python 3.11+
from app.pipeline import extract_lines, parse_air_temperature_10min, write_parquet, WriterReport

def test_extract_lines_signature():
    sig = inspect.signature(extract_lines)
    params = list(sig.parameters.values())
    assert len(params) == 1
    ann = params[0].annotation
    if isinstance(ann, str):
        # robust to 'from __future__ import annotations'
        assert ann in ("Path", "pathlib.Path")
    else:
        assert ann in (Path, inspect._empty)

def test_parser_signature_and_is_iterable():
    sig = inspect.signature(parse_air_temperature_10min)
    params = list(sig.parameters.values())
    assert params[2].kind == inspect.Parameter.KEYWORD_ONLY
    # timezone-aware UTC to avoid deprecation warning
    out = parse_air_temperature_10min([], meta=None, now_utc=datetime.now(UTC))  # type: ignore[arg-type]
    iter(out)

def test_writer_returns_report():
    report = write_parquet([], Path("out"))
    assert isinstance(report, WriterReport)
    assert report.rows_written == 0
