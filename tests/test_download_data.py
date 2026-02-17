import importlib.util
import json
import sys
import types
from pathlib import Path

import pytest


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


def _args(
    *,
    sources: str,
    out_dir: str = "data/processed",
    issuers: str = "",
    include_raw: bool = False,
    nerdwallet_selenium: bool = False,
    manifest_name: str = "manifest_latest.json",
    log_level: str = "ERROR",
    log_file: str = "",
    fail_fast: bool = False,
    # new synthetic args
    num_users: int = 10,
    history_months: int = 2,
    seed: int = 123,
    synthetic_format: str = "csv",
):
    return types.SimpleNamespace(
        sources=sources,
        issuers=issuers,
        nerdwallet_selenium=nerdwallet_selenium,
        out_dir=out_dir,
        manifest_name=manifest_name,
        log_level=log_level,
        log_file=log_file,
        fail_fast=fail_fast,
        include_raw=include_raw,
        num_users=num_users,
        history_months=history_months,
        seed=seed,
        synthetic_format=synthetic_format,
    )


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

    # Force __file__ based repo_root inside main()
    mod.__file__ = str(scripts / "download_data.py")

    # API ok, issuers fail -> should NOT commit
    def ok_api(stage_dir, logger, include_raw=False):
        out = stage_dir / "offers"
        out.mkdir(parents=True, exist_ok=True)
        p = out / "creditcardbonuses_offers.json"
        mod.atomic_write_json(p, {"offers": [{"x": 1}]})
        return [{"x": 1}], 1, [p]

    def fail_issuers(stage_dir, logger, issuers=None):
        raise RuntimeError("boom")

    monkeypatch.setattr(mod, "run_creditcardbonuses_api", ok_api)
    monkeypatch.setattr(mod, "run_issuer_scrapers", fail_issuers)

    monkeypatch.setattr(mod, "parse_args", lambda: _args(sources="api,issuers"))

    rc = mod.main()

    assert rc == 1
    assert sentinel.read_text() == "KEEP_ME"
    assert not (current / "manifest_latest.json").exists()


def test_success_writes_manifest_including_synthetic(tmp_path, monkeypatch, mod):
    repo = tmp_path
    (repo / "scripts").mkdir()
    mod.__file__ = str(repo / "scripts" / "download_data.py")

    # Stub API
    def ok_api(stage_dir, logger, include_raw=False):
        out = stage_dir / "offers"
        out.mkdir(parents=True, exist_ok=True)
        p = out / "creditcardbonuses_offers.json"
        mod.atomic_write_json(p, {"offers": [{"x": 1}, {"x": 2}]})
        return [{"x": 1}, {"x": 2}], 2, [p]

    # Stub issuers
    def ok_issuers(stage_dir, logger, issuers=None):
        out = stage_dir / "offers"
        out.mkdir(parents=True, exist_ok=True)
        p = out / "issuer_chase_offers.json"
        mod.atomic_write_json(p, {"offers": [{"y": 1}]})
        return [{"y": 1}], 1, [p]

    # Stub synthetic generators
    def ok_synth(stage_dir, logger, num_users, history_months, seed, fmt="csv"):
        out = stage_dir / "synthetic"
        out.mkdir(parents=True, exist_ok=True)

        p1 = out / "user_profiles.csv"
        p2 = out / "user_cards.csv"
        p3 = out / "transactions.csv"
        p4 = out / "synthetic_meta.json"

        mod.atomic_write_bytes(p1, b"user_id,archetype\nu1,a\n")
        mod.atomic_write_bytes(p2, b"user_id,card_id\nu1,c1\n")
        mod.atomic_write_bytes(p3, b"transaction_id,user_id\n t1,u1\n")
        mod.atomic_write_json(
            p4,
            {
                "source": "synthetic",
                "fetched_at": mod.utc_now_iso(),
                "params": {
                    "num_users": num_users,
                    "history_months": history_months,
                    "seed": seed,
                    "format": fmt,
                },
                "counts": {"profiles": 1, "user_cards": 1, "transactions": 1},
            },
        )

        preview = [{"user_id": "u1"}]
        total_records = 3
        return preview, total_records, [p1, p2, p3, p4]

    monkeypatch.setattr(mod, "run_creditcardbonuses_api", ok_api)
    monkeypatch.setattr(mod, "run_issuer_scrapers", ok_issuers)
    monkeypatch.setattr(mod, "run_synthetic_generators", ok_synth)

    monkeypatch.setattr(
        mod,
        "parse_args",
        lambda: _args(
            sources="api,issuers,synthetic",
            num_users=5,
            history_months=2,
            seed=7,
            synthetic_format="csv",
        ),
    )

    rc = mod.main()
    assert rc == 0

    current = repo / "data" / "processed" / "current"
    manifest = current / "manifest_latest.json"
    assert manifest.exists()

    m = json.loads(manifest.read_text())

    # Sources
    assert any(s["name"] == "api:creditcardbonuses" and s["ok"] for s in m["sources"])
    assert any(s["name"] == "scrape:issuers" and s["ok"] for s in m["sources"])
    assert any(s["name"] == "generate:synthetic" and s["ok"] for s in m["sources"])

    # Files recorded in manifest
    paths = [f["path"] for f in m["files"]]
    assert "offers/creditcardbonuses_offers.json" in paths
    assert "offers/issuer_chase_offers.json" in paths
    assert "synthetic/user_profiles.csv" in paths
    assert "synthetic/user_cards.csv" in paths
    assert "synthetic/transactions.csv" in paths
    assert "synthetic/synthetic_meta.json" in paths
