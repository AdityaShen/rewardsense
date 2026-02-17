#!/usr/bin/env python3
"""
RewardSense - Unified Data Download Orchestration Script

Acceptance criteria:
- Single command downloads all required data.
- Failed downloads don't corrupt existing data (atomic staging + commit).
- Download status clearly logged (console + optional log file).
"""

from __future__ import annotations
import argparse
import dataclasses
import datetime as dt
import hashlib
import json
import logging
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Make "src/" importable when running as: python3 scripts/download_data.py
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# -----------------------------
# Utilities
# -----------------------------


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def atomic_write_bytes(dest: Path, data: bytes) -> None:
    """
    Atomic file write: write to temp file in same directory then os.replace.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=str(dest.parent), delete=False) as tf:
        tmp_path = Path(tf.name)
        tf.write(data)
        tf.flush()
        os.fsync(tf.fileno())
    os.replace(str(tmp_path), str(dest))


def atomic_write_json(dest: Path, obj: Any) -> None:
    atomic_write_bytes(
        dest, (json.dumps(obj, indent=2, ensure_ascii=False) + "\n").encode("utf-8")
    )


def safe_rmtree(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)


def setup_logging(log_level: str, log_file: Optional[Path]) -> logging.Logger:
    logger = logging.getLogger("download_data")
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    logger.handlers.clear()

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    ch.setLevel(logger.level)
    logger.addHandler(ch)

    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        fh.setLevel(logger.level)
        logger.addHandler(fh)

    return logger


@dataclasses.dataclass
class SourceResult:
    name: str
    ok: bool
    started_at: str
    finished_at: str
    duration_s: float
    records: int = 0
    output_files: List[str] = dataclasses.field(default_factory=list)
    error: Optional[str] = None


# -----------------------------
# Source runners
# -----------------------------


def run_creditcardbonuses_api(
    stage_dir: Path, logger: logging.Logger
) -> Tuple[List[Dict[str, Any]], int, List[Path]]:
    """
    Fetch normalized offers from CreditCardBonusesClient and write JSON to stage_dir.
    Returns: (records_as_dicts, record_count, files_written)
    """
    # Import inside function so CLI works even if deps are missing for a source
    from src.data_pipeline.api_fetcher.credit_card_bonuses_api import (
        CreditCardBonusesClient,
    )

    out_dir = stage_dir / "offers"
    out_dir.mkdir(parents=True, exist_ok=True)

    client = CreditCardBonusesClient()
    logger.info("API: fetching normalized offers from CreditCardBonuses...")
    offers = client.fetch_as_dicts()  # list[dict]
    n = len(offers)

    out_path = out_dir / "creditcardbonuses_offers.json"
    atomic_write_json(
        out_path,
        {"source": "creditcardbonuses", "fetched_at": utc_now_iso(), "offers": offers},
    )

    return offers, n, [out_path]


def run_issuer_scrapers(
    stage_dir: Path, logger: logging.Logger, issuers: Optional[List[str]] = None
) -> Tuple[List[Dict[str, Any]], int, List[Path]]:
    """
    Run issuer scrapers and write aggregated JSON to stage_dir.

    Note: Existing scrapers return dicts; we persist raw scraped dicts here.
    Later stories can normalize them into CardOffer.
    """
    from src.data_pipeline.scrapers.issuer_scrapers import (
        ChaseScraper,
        AmexScraper,
        CitiScraper,
        CapitalOneScraper,
        DiscoverScraper,
    )

    out_dir = stage_dir / "offers"
    out_dir.mkdir(parents=True, exist_ok=True)

    available = {
        "chase": ChaseScraper,
        "amex": AmexScraper,
        "citi": CitiScraper,
        "capitalone": CapitalOneScraper,
        "discover": DiscoverScraper,
    }

    selected = issuers or list(available.keys())
    unknown = [x for x in selected if x.lower() not in available]
    if unknown:
        raise ValueError(
            f"Unknown issuers: {unknown}. Valid: {sorted(available.keys())}"
        )

    all_rows: List[Dict[str, Any]] = []
    files_written: List[Path] = []

    for issuer in selected:
        issuer_key = issuer.lower()
        scraper_cls = available[issuer_key]
        scraper = scraper_cls()
        logger.info("Scraper: %s - scraping card listings...", issuer_key)

        rows = scraper.scrape_all_cards()  # expected list[dict]
        if not isinstance(rows, list):
            raise TypeError(
                f"Issuer scraper {issuer_key} returned {type(rows)}; expected list of dicts"
            )

        # Persist per-issuer output (helps debugging + atomic commit)
        out_path = out_dir / f"issuer_{issuer_key}_offers.json"
        payload = {
            "source": f"issuer:{issuer_key}",
            "fetched_at": utc_now_iso(),
            "offers": rows,
        }
        atomic_write_json(out_path, payload)

        files_written.append(out_path)
        all_rows.extend(rows)

    return all_rows, len(all_rows), files_written


def run_nerdwallet_scraper(
    stage_dir: Path, logger: logging.Logger, use_selenium: bool = False
) -> Tuple[List[Dict[str, Any]], int, List[Path]]:
    """
    Run NerdWallet scraper (requests+bs4 by default, selenium optional) and write JSON to stage_dir.
    """
    out_dir = stage_dir / "offers"
    out_dir.mkdir(parents=True, exist_ok=True)

    if use_selenium:
        from src.data_pipeline.scrapers.nerdwallet_scraper import (
            NerdWalletSeleniumScraper as NerdWalletScraper,
        )

        logger.info("Scraper: NerdWallet (selenium) - starting...")
    else:
        from src.data_pipeline.scrapers.nerdwallet_scraper import NerdWalletScraper

        logger.info("Scraper: NerdWallet (requests) - starting...")

    scraper = NerdWalletScraper()
    rows = scraper.scrape_all_cards()
    if not isinstance(rows, list):
        raise TypeError(
            f"NerdWallet scraper returned {type(rows)}; expected list of dicts"
        )

    out_path = out_dir / "nerdwallet_offers.json"
    payload = {"source": "nerdwallet", "fetched_at": utc_now_iso(), "offers": rows}
    atomic_write_json(out_path, payload)

    return rows, len(rows), [out_path]


# -----------------------------
# Orchestrator
# -----------------------------


def make_manifest(
    run_id: str,
    started_at: str,
    finished_at: str,
    results: List[SourceResult],
    committed_dir: Path,
) -> Dict[str, Any]:
    files: List[Dict[str, Any]] = []
    for p in committed_dir.rglob("*"):
        if p.is_file():
            rel = str(p.relative_to(committed_dir))
            files.append(
                {
                    "path": rel,
                    "bytes": p.stat().st_size,
                    "sha256": sha256_file(p),
                }
            )

    return {
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "sources": [dataclasses.asdict(r) for r in results],
        "artifact_root": str(committed_dir),
        "files": sorted(files, key=lambda x: x["path"]),
    }


def commit_stage_to_processed(
    stage_dir: Path, processed_dir: Path, logger: logging.Logger
) -> None:
    """
    Atomic commit strategy:
    - Stage contains new files in a temporary directory.
    - We commit by replacing a 'processed/current' directory via os.replace:
        processed/current_tmp -> processed/current
    This ensures partial output never replaces the existing current set.
    """
    processed_dir.mkdir(parents=True, exist_ok=True)
    current = processed_dir / "current"
    tmp_target = processed_dir / f"current_tmp_{int(time.time())}"

    # Copy stage to tmp_target, then atomic swap
    if tmp_target.exists():
        safe_rmtree(tmp_target)
    shutil.copytree(stage_dir, tmp_target)

    # Atomic replace current (directory replace works if on same filesystem; on Windows can be tricky)
    # We implement: move current -> backup, move tmp -> current, then delete backup.
    backup = processed_dir / f"current_backup_{int(time.time())}"
    if current.exists():
        os.replace(str(current), str(backup))
    os.replace(str(tmp_target), str(current))
    safe_rmtree(backup)

    logger.info("Committed processed data to: %s", current)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="RewardSense unified data download orchestrator"
    )
    p.add_argument(
        "--sources",
        default="all",
        help="Comma-separated list of sources: all, api, issuers, nerdwallet",
    )
    p.add_argument(
        "--issuers",
        default="",
        help="Comma-separated issuer list for issuers source (e.g., chase,amex,citi). Empty = all.",
    )
    p.add_argument(
        "--nerdwallet-selenium",
        action="store_true",
        help="Use Selenium-based NerdWallet scraper (if available).",
    )
    p.add_argument(
        "--out-dir",
        default="data/processed",
        help="Output directory root (will create <out-dir>/current).",
    )
    p.add_argument(
        "--manifest-name",
        default="manifest_latest.json",
        help="Manifest file name written under <out-dir>/current/",
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity.",
    )
    p.add_argument(
        "--log-file",
        default="",
        help="Optional log file path (e.g., logs/download_data.log).",
    )
    p.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop immediately on first source failure (default: continue and report failures).",
    )
    return p.parse_args()


def resolve_sources(arg: str) -> List[str]:
    s = [x.strip().lower() for x in arg.split(",") if x.strip()]
    if not s:
        return []
    if s == ["all"]:
        return ["api", "issuers", "nerdwallet"]
    return s


def main() -> int:
    args = parse_args()
    sources = resolve_sources(args.sources)

    log_file = Path(args.log_file) if args.log_file.strip() else None
    logger = setup_logging(args.log_level, log_file)

    repo_root = Path(__file__).resolve().parents[1]  # scripts/.. = repo root
    processed_dir = (repo_root / args.out_dir).resolve()
    run_id = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    started_at = utc_now_iso()

    logger.info("Run ID: %s", run_id)
    logger.info("Selected sources: %s", sources)
    logger.info("Output root: %s", processed_dir)

    # Staging directory lives in the same filesystem so directory swaps are atomic
    stage_parent = processed_dir.parent / ".staging"
    stage_parent.mkdir(parents=True, exist_ok=True)
    stage_dir = stage_parent / f"run_{run_id}"

    # Ensure clean stage
    safe_rmtree(stage_dir)
    stage_dir.mkdir(parents=True, exist_ok=True)

    results: List[SourceResult] = []

    def run_one(name: str, fn, *fn_args, **fn_kwargs) -> bool:
        t0 = time.time()
        s0 = utc_now_iso()
        logger.info("==== START source=%s ====", name)
        try:
            _rows, n, files = fn(*fn_args, **fn_kwargs)
            ok = True
            err = None
        except Exception as e:
            ok = False
            n = 0
            files = []
            err = f"{type(e).__name__}: {e}"
            logger.error("Source failed: %s | %s", name, err)
            logger.debug("Exception details", exc_info=True)
        t1 = time.time()
        s1 = utc_now_iso()
        res = SourceResult(
            name=name,
            ok=ok,
            started_at=s0,
            finished_at=s1,
            duration_s=round(t1 - t0, 3),
            records=n,
            output_files=(
                [str(Path(f).relative_to(stage_dir)) for f in files] if files else []
            ),
            error=err,
        )
        results.append(res)
        logger.info(
            "==== END source=%s ok=%s records=%d duration=%.3fs ====",
            name,
            ok,
            n,
            (t1 - t0),
        )
        return ok

    # Execute requested sources
    all_ok = True

    if "api" in sources:
        ok = run_one(
            "api:creditcardbonuses", run_creditcardbonuses_api, stage_dir, logger
        )
        all_ok = all_ok and ok
        if args.fail_fast and not ok:
            safe_rmtree(stage_dir)
            return 2

    if "issuers" in sources:
        issuers = [
            x.strip().lower() for x in args.issuers.split(",") if x.strip()
        ] or None
        ok = run_one(
            "scrape:issuers", run_issuer_scrapers, stage_dir, logger, issuers=issuers
        )
        all_ok = all_ok and ok
        if args.fail_fast and not ok:
            safe_rmtree(stage_dir)
            return 2

    if "nerdwallet" in sources:
        ok = run_one(
            "scrape:nerdwallet",
            run_nerdwallet_scraper,
            stage_dir,
            logger,
            use_selenium=args.nerdwallet_selenium,
        )
        all_ok = all_ok and ok
        if args.fail_fast and not ok:
            safe_rmtree(stage_dir)
            return 2

    finished_at = utc_now_iso()

    # If any failures occurred, do NOT commit stage; keep existing processed/current intact.
    if not all_ok:
        logger.error(
            "One or more sources failed. NOT committing outputs. Existing data remains unchanged."
        )
        # Still write a run report into staging for debugging
        report = {
            "run_id": run_id,
            "started_at": started_at,
            "finished_at": finished_at,
            "committed": False,
            "sources": [dataclasses.asdict(r) for r in results],
        }
        atomic_write_json(stage_dir / "run_report.json", report)
        logger.info(
            "Wrote failure run report to staging: %s", stage_dir / "run_report.json"
        )
        # Clean staging to avoid clutter (optional). Comment out if you want to keep failed stages.
        safe_rmtree(stage_dir)
        return 1

    # Commit stage -> processed/current atomically
    commit_stage_to_processed(stage_dir, processed_dir, logger)

    # Write manifest AFTER commit so it reflects committed files
    committed_current = processed_dir / "current"
    manifest = make_manifest(
        run_id, started_at, finished_at, results, committed_current
    )
    atomic_write_json(committed_current / args.manifest_name, manifest)
    logger.info("Wrote manifest: %s", committed_current / args.manifest_name)

    # Also save a timestamped manifest for auditability
    atomic_write_json(committed_current / f"manifest_{run_id}.json", manifest)

    # Cleanup staging
    safe_rmtree(stage_dir)
    logger.info("Done. All sources succeeded.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
