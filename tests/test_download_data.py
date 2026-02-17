import json
from pathlib import Path
import types
import sys
import pytest
import importlib.util


def load_module():
    script = Path(__file__).resolve().parents[1] / "scripts" / "download_data.py"
    spec = importlib.util.spec_from_file_location("download_data", script)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod

    spec.loader.exec_module(mod)  # type: ignore
    return mod


@pytest.fixture
def mod():
    return load_module()


def test_failure_does_not_overwrite_current(tmp_path, monkeypatch, mod):
    # Arrange: fake repo layout
    repo = tmp_path
    scripts = repo / "scripts"
    scripts.mkdir()
    data_processed = repo / "data" / "processed"
    current = data_processed / "current"
    current.mkdir(parents=True)
    sentinel = current / "sentinel.txt"
    sentinel.write_text("KEEP_ME")

    # Patch __file__ resolution assumptions by monkeypatching Path(...) usage:
    # Easiest: monkeypatch the repo_root computed in main() by patching __file__
    # Instead: patch mod.Path(__file__).resolve().parents[1] usage by setting mod.__file__
    mod.__file__ = str(scripts / "download_data.py")

    # Force API success but issuer failure
    def ok_api(stage_dir, logger):
        out = stage_dir / "offers"
        out.mkdir(parents=True, exist_ok=True)
        p = out / "creditcardbonuses_offers.json"
        mod.atomic_write_json(p, {"offers": [{"x": 1}]})
        return [{"x": 1}], 1, [p]

    def fail_issuers(stage_dir, logger, issuers=None):
        raise RuntimeError("boom")

    monkeypatch.setattr(mod, "run_creditcardbonuses_api", ok_api)
    monkeypatch.setattr(mod, "run_issuer_scrapers", fail_issuers)

    # Run with args (simulate CLI)
    monkeypatch.setattr(
        mod,
        "parse_args",
        lambda: types.SimpleNamespace(
            sources="api,issuers",
            issuers="",
            nerdwallet_selenium=False,
            out_dir="data/processed",
            manifest_name="manifest_latest.json",
            log_level="ERROR",
            log_file="",
            fail_fast=False,
        ),
    )

    rc = mod.main()

    # Assert: failure exit code and current preserved
    assert rc == 1
    assert sentinel.read_text() == "KEEP_ME"
    assert not (current / "manifest_latest.json").exists()


def test_success_writes_manifest(tmp_path, monkeypatch, mod):
    repo = tmp_path
    (repo / "scripts").mkdir()
    mod.__file__ = str(repo / "scripts" / "download_data.py")

    def ok_api(stage_dir, logger):
        out = stage_dir / "offers"
        out.mkdir(parents=True, exist_ok=True)
        p = out / "creditcardbonuses_offers.json"
        mod.atomic_write_json(p, {"offers": [{"x": 1}, {"x": 2}]})
        return [{"x": 1}, {"x": 2}], 2, [p]

    def ok_issuers(stage_dir, logger, issuers=None):
        out = stage_dir / "offers"
        out.mkdir(parents=True, exist_ok=True)
        p = out / "issuer_chase_offers.json"
        mod.atomic_write_json(p, {"offers": [{"y": 1}]})
        return [{"y": 1}], 1, [p]

    monkeypatch.setattr(mod, "run_creditcardbonuses_api", ok_api)
    monkeypatch.setattr(mod, "run_issuer_scrapers", ok_issuers)

    monkeypatch.setattr(
        mod,
        "parse_args",
        lambda: types.SimpleNamespace(
            sources="api,issuers",
            issuers="",
            nerdwallet_selenium=False,
            out_dir="data/processed",
            manifest_name="manifest_latest.json",
            log_level="ERROR",
            log_file="",
            fail_fast=False,
        ),
    )

    rc = mod.main()
    assert rc == 0

    current = repo / "data" / "processed" / "current"
    manifest = current / "manifest_latest.json"
    assert manifest.exists()

    m = json.loads(manifest.read_text())
    assert m["run_id"]
    assert any(s["name"] == "api:creditcardbonuses" and s["ok"] for s in m["sources"])
    assert any(s["name"] == "scrape:issuers" and s["ok"] for s in m["sources"])
    paths = [f["path"] for f in m["files"]]
    assert "offers/creditcardbonuses_offers.json" in paths
    assert "offers/issuer_chase_offers.json" in paths
