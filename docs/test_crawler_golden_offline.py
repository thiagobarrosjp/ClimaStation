import json, os, sys, socket, threading, contextlib, http.server, time, difflib
from pathlib import Path
import pytest
import subprocess as sp

SITE_ROOT = Path("tests/dwd/golden")
EXPECTED = Path("tests/dwd/golden/expected/10_minutes_air_temperature_urls.golden.jsonl")
VALIDATOR = Path("app/tools/validate_crawler_urls.py")
SCHEMA = Path("schemas/dwd/crawler_urls.schema.json")
DATASET = "10_minutes_air_temperature"
CANONICAL = ("https://opendata.dwd.de/climate_environment/CDC/observations_germany/"
             "climate/10_minutes/air_temperature/")

@contextlib.contextmanager
def serve_dir(root: Path):
    """Serve a directory on 127.0.0.1:<freeport> using http.server; yield base URL."""
    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *a, **kw):
            super().__init__(*a, directory=str(root), **kw)
        def log_message(self, *args, **kwargs):  # keep test output quiet
            pass
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        host, port = s.getsockname()
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", port), Handler)
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    try:
        yield f"http://127.0.0.1:{port}/climate/10_minutes/air_temperature/"
    finally:
        httpd.shutdown()
        httpd.server_close()
        t.join(timeout=2)

def _run(cmd, **kw):
    return sp.run(cmd, text=True, capture_output=True, **kw)

@pytest.mark.golden
def test_crawler_golden_offline(tmp_path: Path):
    assert SITE_ROOT.exists(), "golden site missing"
    assert EXPECTED.exists(), "expected golden JSONL missing; create it from a manual run"
    assert VALIDATOR.exists(), "validator missing"
    assert SCHEMA.exists(), "schema missing"

    with serve_dir(SITE_ROOT) as base:
        env = os.environ.copy()
        env["CLIMASTATION_CRAWLER_BASE_URL"] = base
        env["CLIMASTATION_CANONICAL_BASE_URL"] = CANONICAL

        outdir = tmp_path / "out"
        outdir.mkdir(parents=True, exist_ok=True)

        # Run crawler via runner
        proc = _run([sys.executable, "-m", "app.main.run_pipeline",
                     "--mode", "crawl", "--dataset", DATASET,
                     "--outdir", str(outdir), "--throttle", "0"], env=env)
        assert proc.returncode == 0, f"crawler failed:\n{proc.stdout}\n{proc.stderr}"

        full = outdir / f"{DATASET}_urls.jsonl"
        sample = outdir / f"{DATASET}_urls_sample100.jsonl"
        assert full.exists(), "full manifest not written"
        assert sample.exists(), "sample manifest not written"

        # Validate (read JSON report from stdout)
        for p in (full, sample):
            v = _run([sys.executable, str(VALIDATOR), "--input", str(p), "--schema", str(SCHEMA)])
            assert v.returncode == 0, f"validator failed for {p}:\n{v.stdout}\n{v.stderr}"
            rep = json.loads(v.stdout)
            assert rep["status"] == "ok" and rep["invalid_lines"] == 0, f"invalid lines in {p}"

        # Byte-compare full manifest to expected golden (LF normalized)
        actual = full.read_text(encoding="utf-8").replace("\r\n", "\n")
        expected = EXPECTED.read_text(encoding="utf-8").replace("\r\n", "\n")
        if actual != expected:
            diff = "".join(difflib.unified_diff(
                expected.splitlines(True), actual.splitlines(True),
                fromfile="golden", tofile="actual"
            ))
            pytest.fail("Golden mismatch:\n" + diff)
