"""\
Offline Golden Test — DWD Crawler (historical + meta_data, all stations)

Run locally:
    pytest -q tests/dwd/test_crawler_golden_offline.py

This test runs a fully OFFLINE crawl via the runner and checks the outputs
against frozen goldens (byte-identical after optional UTF-8 BOM normalization).
It also invokes the schema/contract validator to ensure invariants hold.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Tuple
import subprocess as sp

import pytest

# Ensure repo root is on sys.path so `import app...` works regardless of CWD
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Public runner APIs
from app.main.run_pipeline import run_crawl_mode, serve_directory_http  # type: ignore

# Logger (fallback if enhanced logger isn't present in this repo state)
try:
    from app.utils.enhanced_logger import get_logger  # type: ignore
except Exception:  # pragma: no cover - fallback for local runs
    import logging
    def get_logger(name: str):
        logger = logging.getLogger(name)
        if not logger.handlers:
            logger.addHandler(logging.StreamHandler())
        logger.setLevel(logging.INFO)
        return logger

# Validator (prefer in-process; fall back to `python -m`)
try:
    from app.tools import validate_crawler_urls as validator  # type: ignore
except Exception:
    validator = None  # type: ignore

DATASET = "10_minutes_air_temperature"
FIXTURE_ROOT = Path("tests/dwd/golden/climate/10_minutes/air_temperature/")
GOLDEN_DIR = Path("tests/dwd/golden/expected/")
GOLDEN_FULL = GOLDEN_DIR / f"{DATASET}_urls.golden.jsonl"
GOLDEN_SAMPLE = GOLDEN_DIR / f"{DATASET}_urls_sample100.golden.jsonl"


def _read_bytes(p: Path) -> bytes:
    return p.read_bytes()


def _strip_utf8_bom(b: bytes) -> bytes:
    # Normalize optional UTF-8 BOM for robust comparison across editors/OS
    return b[3:] if b.startswith(b"\xef\xbb\xbf") else b


def _first_last_lines(p: Path) -> Tuple[str, str]:
    # Use utf-8-sig so BOM (if present) is ignored when reading text lines
    with p.open("r", encoding="utf-8-sig") as f:
        first = f.readline().rstrip("\n")
        last = ""
        for line in f:
            last = line
        return first, last.rstrip("\n")


def _run_validator(input_path: Path) -> None:
    """Run the crawler manifest validator; raise on any failure."""
    try:
        if validator is not None:  # type: ignore
            rc = validator.main(["--input", str(input_path)])  # type: ignore[attr-defined]
            if rc == 0:
                return
    except Exception:
        pass
    proc = sp.run(
        [sys.executable, "-m", "app.tools.validate_crawler_urls", "--input", str(input_path)],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, (
        "Validator failed for "
        f"{input_path} (returncode {proc.returncode}).\n"
        f"STDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
    )


def test_crawler_offline_historical_and_metadata_golden_byte_identical(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Offline crawl fixtures and compare outputs byte-identically to goldens."""

    # Preconditions: fixtures + goldens present
    assert FIXTURE_ROOT.exists(), f"Fixture root not found: {FIXTURE_ROOT}"
    assert GOLDEN_FULL.exists(), f"Missing golden: {GOLDEN_FULL}"
    assert GOLDEN_SAMPLE.exists(), f"Missing golden: {GOLDEN_SAMPLE}"

    logger = get_logger("tests.crawler.golden")

    # Monkeypatch the runner's config to a deterministic OFFLINE setup
    from app.main import run_pipeline as rp  # type: ignore

    def _fake_load_config(dataset_name: str, _logger):
        assert dataset_name == DATASET
        out_urls = tmp_path / f"{DATASET}_urls.jsonl"
        return {
            "name": DATASET,
            "crawler": {
                # Canonical base must include the dataset root so final URLs are correct
                "canonical_base_url": "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/",
                # Offline server wiring (the runner will serve from here)
                "offline_server_root": str(FIXTURE_ROOT.parent.parent.parent),  # → tests/dwd/golden/
                "offline_relpath": "climate/10_minutes/air_temperature/",
                # Discover exactly these two subsets and only the expected file types
                "subfolders": {"historical": "historical", "meta_data": "meta_data"},
                "include_extensions": [".zip", ".txt"],
                # Deterministic sample size
                "sample_size": 100,
                # These are retained for compatibility; runner/crawler may reference them
                "base_url": "https://opendata.dwd.de/",
                "root_path": "climate/10_minutes/air_temperature/",
                # Where the crawler writes its full manifest by default
                "output_urls_jsonl": str(out_urls),
            },
            "downloader": {"root_dir": str(tmp_path / "downloads")},
            "dwd_paths": {"crawl_data": str(tmp_path)},
        }

    monkeypatch.setattr(rp, "_load_config", _fake_load_config, raising=True)

    # Prove the fixture server works (runner will spin its own instance later)
    with serve_directory_http(FIXTURE_ROOT) as (host, port):
        assert isinstance(host, str) and isinstance(port, int) and port > 0

    # Execute the offline crawl via the runner
    rc = run_crawl_mode(
        dataset_name=DATASET,
        logger=logger,  # type: ignore[arg-type]
        source="offline",
        subfolder=None,  # discover both historical/ and meta_data/
        outdir=str(tmp_path),
    )
    assert rc == 0, "run_crawl_mode returned non-zero exit code in offline mode"

    # Resolve produced outputs
    produced_full = tmp_path / f"{DATASET}_urls.jsonl"
    produced_sample = tmp_path / f"{DATASET}_urls_sample100.jsonl"

    # Guardrails: existence + non-empty
    assert produced_full.exists() and produced_full.stat().st_size > 0
    assert produced_sample.exists() and produced_sample.stat().st_size > 0

    # Validate both manifests
    _run_validator(produced_full)
    _run_validator(produced_sample)

    # Byte-identical compare (normalize optional BOM only)
    prod_bytes = _strip_utf8_bom(_read_bytes(produced_full))
    gold_bytes = _strip_utf8_bom(_read_bytes(GOLDEN_FULL))
    assert prod_bytes == gold_bytes, (
        "Full URLs manifest differs from golden (byte mismatch).\n"
        f"Produced: {produced_full}\nGolden:   {GOLDEN_FULL}"
    )

    prod_s_bytes = _strip_utf8_bom(_read_bytes(produced_sample))
    gold_s_bytes = _strip_utf8_bom(_read_bytes(GOLDEN_SAMPLE))
    assert prod_s_bytes == gold_s_bytes, (
        "Sample100 manifest differs from golden (byte mismatch).\n"
        f"Produced: {produced_sample}\nGolden:   {GOLDEN_SAMPLE}"
    )

    # Helpful diffs when bytes mismatch elsewhere: first/last lines
    p_first, p_last = _first_last_lines(produced_full)
    g_first, g_last = _first_last_lines(GOLDEN_FULL)
    assert p_first == g_first
    assert p_last == g_last

    ps_first, ps_last = _first_last_lines(produced_sample)
    gs_first, gs_last = _first_last_lines(GOLDEN_SAMPLE)
    assert ps_first == gs_first
    assert ps_last == gs_last
