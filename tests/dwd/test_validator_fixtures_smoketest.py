# tests/dwd/test_validator_fixtures_smoketest.py  (move it out of fixtures/)
import json, sys, subprocess, pytest
from pathlib import Path

FIXTURES_ROOT = Path("tests/dwd/fixtures")
VALIDATOR = Path("app/tools/validate_crawler_urls.py")
SCHEMA = Path("schemas/dwd/crawler_urls.schema.json")

def _iter_fixtures():
    return sorted(FIXTURES_ROOT.rglob("*_urls_sample*.jsonl")) if FIXTURES_ROOT.exists() else []

@pytest.mark.smoke
@pytest.mark.parametrize("manifest", _iter_fixtures(), ids=lambda p: str(p))
def test_all_dataset_fixtures_are_valid(manifest: Path, tmp_path: Path):
    assert VALIDATOR.exists(), f"Validator missing at {VALIDATOR}"
    assert SCHEMA.exists(), f"Schema missing at {SCHEMA}"

    report_path = tmp_path / (manifest.name + ".validation.json")  # keep reports out of repo
    proc = subprocess.run(
        [sys.executable, str(VALIDATOR),
         "--input", str(manifest),
         "--schema", str(SCHEMA),
         "--report", str(report_path)],
        capture_output=True, text=True, check=False
    )
    assert proc.returncode == 0, f"{manifest}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"

    rep = json.loads(report_path.read_text(encoding="utf-8"))
    total = sum(1 for _ in manifest.open(encoding="utf-8"))
    assert rep["status"] == "ok"
    assert rep["invalid_lines"] == 0
    assert rep["total_lines"] == total
    assert not rep["duplicates"]
    assert not rep["unsorted"]["is_unsorted"]
