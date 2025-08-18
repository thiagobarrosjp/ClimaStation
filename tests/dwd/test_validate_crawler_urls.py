import json
import sys
import subprocess
from pathlib import Path
from typing import List, Dict, Any, Tuple

import pytest


# ---------------------------
# Helpers
# ---------------------------

REQUIRED_TOP_KEYS = {
    "input_path",
    "schema_path",
    "total_lines",
    "valid_lines",
    "invalid_lines",
    "duplicates",
    "unsorted",
    "errors_sample",
    "status",
    "elapsed_seconds",
    "generated_at",
}


def project_root() -> Path:
    """Resolve repo root robustly from tests nested under tests/…/.

    We climb upwards until we find a directory containing the 'app' folder
    (which should exist in this repo). Fallback to three levels up.
    """
    here = Path(__file__).resolve()
    p = here
    for _ in range(6):  # go up to 6 levels just in case
        if (p / "app").exists():
            return p
        p = p.parent
    # Fallback: tests/dwd/<file> -> repo root is three levels up
    return here.parent.parent.parent


def resolver() -> Tuple[Path, Path]:
    """Resolve paths for the validator script and schema.

    Returns (script_path, schema_path). If the canonical schema path
    does not exist, a temporary stub schema will be created by tests instead.
    """
    root = project_root()
    script = root / "app" / "tools" / "validate_crawler_urls.py"
    schema = root / "schemas" / "dwd" / "crawler_urls.schema.json"
    if not script.exists():
        pytest.fail(f"Validator script not found at expected path: {script}")
    return script, schema


def write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as f:
        for obj in rows:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def run_validator(input_path: Path, schema_path: Path) -> subprocess.CompletedProcess:
    script_path, _ = resolver()
    cmd = [sys.executable, str(script_path), "--input", str(input_path), "--schema", str(schema_path)]
    return subprocess.run(cmd, capture_output=True, text=True)


def read_report_for(input_path: Path) -> Dict[str, Any]:
    report_path = Path(str(input_path) + ".validation.json")
    assert report_path.exists(), f"Expected report at {report_path}"
    with report_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    # Top-level keys must exist
    missing = REQUIRED_TOP_KEYS.difference(data.keys())
    assert not missing, f"Report missing keys: {sorted(missing)}\nReport: {data}"
    return data


def rec(url_root: str, rel: str, fn: str, extra: Dict[str, Any] | None = None) -> Dict[str, Any]:
    obj = {"url": f"{url_root}{rel}{fn}", "relative_path": rel, "filename": fn}
    if extra:
        obj.update(extra)
    return obj


# Common base for synthetic URLs
URL_BASE = "https://example.com/root/"


# ---------------------------
# Tests
# ---------------------------


def test_happy_path_ok_exit_zero(tmp_path: Path):
    script_path, schema_path = resolver()
    # If schema missing in repo, create a dummy file to satisfy CLI arg
    if not schema_path.exists():
        schema_path = tmp_path / "crawler_urls.schema.json"
        schema_path.write_text("{}", encoding="utf-8")

    input_file = tmp_path / "ok.jsonl"
    rows = [
        rec(URL_BASE, "", "a.zip"),
        rec(URL_BASE, "historical/", "b.zip"),
        rec(URL_BASE, "historical/", "c.zip"),
    ]
    # Ensure sorted by (relative_path, filename)
    rows = sorted(rows, key=lambda r: (r["relative_path"], r["filename"]))
    write_jsonl(input_file, rows)

    proc = run_validator(input_file, schema_path)
    assert proc.returncode == 0, proc.stderr

    report = read_report_for(input_file)
    assert report["status"] == "ok"
    assert report["total_lines"] == 3
    assert report["valid_lines"] == 3
    assert report["invalid_lines"] == 0
    assert report["duplicates"] == []
    assert report["unsorted"]["is_unsorted"] is False


def test_reject_http_scheme_exit_four(tmp_path: Path):
    _, schema_path = resolver()
    if not schema_path.exists():
        schema_path = tmp_path / "crawler_urls.schema.json"
        schema_path.write_text("{}", encoding="utf-8")

    input_file = tmp_path / "bad_http.jsonl"
    bad = {"url": "http://example.com/root/historical/b.zip", "relative_path": "historical/", "filename": "b.zip"}
    write_jsonl(input_file, [bad])

    proc = run_validator(input_file, schema_path)
    assert proc.returncode == 4, proc.stderr

    report = read_report_for(input_file)
    assert report["status"] == "failed"
    assert report["invalid_lines"] == 1
    # Errors should mention HTTPS requirement
    joined = json.dumps(report["errors_sample"])  # easy contains check
    assert "https://" in joined.lower()


def test_reject_directory_url_exit_four(tmp_path: Path):
    _, schema_path = resolver()
    if not schema_path.exists():
        schema_path = tmp_path / "crawler_urls.schema.json"
        schema_path.write_text("{}", encoding="utf-8")

    input_file = tmp_path / "dir_url.jsonl"
    # URL path ends with '/'
    bad = {"url": URL_BASE + "historical/", "relative_path": "historical/", "filename": "ignored.zip"}
    write_jsonl(input_file, [bad])

    proc = run_validator(input_file, schema_path)
    assert proc.returncode == 4, proc.stderr

    report = read_report_for(input_file)
    assert report["status"] == "failed"
    assert report["invalid_lines"] == 1
    msg = json.dumps(report["errors_sample"]).lower()
    assert "ends with '/'" in msg or "directory" in msg


def test_bad_relative_path_missing_trailing_slash_exit_four(tmp_path: Path):
    _, schema_path = resolver()
    if not schema_path.exists():
        schema_path = tmp_path / "crawler_urls.schema.json"
        schema_path.write_text("{}", encoding="utf-8")

    input_file = tmp_path / "bad_relpath.jsonl"
    # Non-empty relative_path without trailing slash
    bad = rec(URL_BASE, "historical", "b.zip")
    write_jsonl(input_file, [bad])

    proc = run_validator(input_file, schema_path)
    assert proc.returncode == 4, proc.stderr

    report = read_report_for(input_file)
    assert report["status"] == "failed"
    assert report["invalid_lines"] == 1
    msg = json.dumps(report["errors_sample"]).lower()
    assert "relative_path" in msg and "end with '/'" in msg


def test_duplicate_pair_detected_exit_four(tmp_path: Path):
    _, schema_path = resolver()
    if not schema_path.exists():
        schema_path = tmp_path / "crawler_urls.schema.json"
        schema_path.write_text("{}", encoding="utf-8")

    input_file = tmp_path / "dups.jsonl"
    # Two identical (relative_path, filename) pairs
    rows = [
        rec(URL_BASE, "historical/", "a.zip"),
        rec(URL_BASE, "historical/", "a.zip"),  # duplicate
        rec(URL_BASE, "historical/", "b.zip"),
    ]
    # Keep sorted order to avoid triggering unsorted check
    rows = sorted(rows, key=lambda r: (r["relative_path"], r["filename"]))
    write_jsonl(input_file, rows)

    proc = run_validator(input_file, schema_path)
    assert proc.returncode == 4, proc.stderr

    report = read_report_for(input_file)
    assert report["status"] == "failed"
    assert report["invalid_lines"] == 0  # records themselves are valid
    assert report["duplicates"], "Expected duplicates to be reported"
    # Ensure the duplicate pair is the expected one
    dup0 = report["duplicates"][0]
    assert dup0["relative_path"] == "historical/"
    assert dup0["filename"] == "a.zip"
    assert isinstance(dup0["first_seen_line"], int) and isinstance(dup0["duplicate_line"], int)


def test_unsorted_detected_exit_four(tmp_path: Path):
    _, schema_path = resolver()
    if not schema_path.exists():
        schema_path = tmp_path / "crawler_urls.schema.json"
        schema_path.write_text("{}", encoding="utf-8")

    input_file = tmp_path / "unsorted.jsonl"
    # Intentionally out of order: (historical/, b.zip) should come after (historical/, a.zip)
    rows = [
        rec(URL_BASE, "historical/", "b.zip"),
        rec(URL_BASE, "historical/", "a.zip"),  # out of order
        rec(URL_BASE, "recent/", "z.zip"),
    ]
    write_jsonl(input_file, rows)

    proc = run_validator(input_file, schema_path)
    assert proc.returncode == 4, proc.stderr

    report = read_report_for(input_file)
    assert report["status"] == "failed"
    assert report["unsorted"]["is_unsorted"] is True
    fb = report["unsorted"]["first_bad_pair"]
    assert isinstance(fb, dict) and "prev" in fb and "curr" in fb
    assert fb["prev"]["relative_path"] == "historical/"
    assert fb["prev"]["filename"] == "b.zip"
    assert fb["curr"]["relative_path"] == "historical/"
    assert fb["curr"]["filename"] == "a.zip"
