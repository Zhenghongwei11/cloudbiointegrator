#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def collect_summaries(root: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    runs_dir = root / "results" / "runs"
    if not runs_dir.exists():
        return rows
    for json_path in sorted(runs_dir.glob("*/*_summary.json")):
        try:
            payload = json.loads(json_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        stage = str(payload.get("stage") or json_path.name.replace("_summary.json", ""))
        dataset_id = str(payload.get("dataset_id") or "")
        rows.append(
            {
                "stage": stage,
                "dataset_id": dataset_id,
                "artifact_path": str(json_path.relative_to(root)),
                "sha256": sha256_bytes(json_path.read_bytes()),
                "run_id": json_path.parent.name,
            }
        )
    return rows


def latest_per_key(rows: list[dict[str, str]]) -> dict[tuple[str, str], dict[str, str]]:
    # run_id starts with a UTC timestamp (YYYYMMDDTHHMMSSZ-...), so lexicographic
    # order is chronological; keep the newest run per (stage, dataset_id).
    by_key: dict[tuple[str, str], dict[str, str]] = {}
    for r in rows:
        key = (r["stage"], r["dataset_id"])
        cur = by_key.get(key)
        if cur is None or r["run_id"] > cur["run_id"]:
            by_key[key] = r
    return by_key


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Record a SHA-256 manifest of run-scoped outcome summaries (results/runs/)."
    )
    ap.add_argument("--root", default=str(ROOT), help="Project root (default: repo root).")
    ap.add_argument(
        "--out",
        default="results/runs/manifest.tsv",
        help="Output manifest path relative to --root.",
    )
    args = ap.parse_args()

    root = Path(args.root).resolve()
    rows = latest_per_key(collect_summaries(root))
    out_path = root / args.out
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["stage", "dataset_id", "artifact_path", "sha256", "run_id"],
            delimiter="\t",
        )
        writer.writeheader()
        for key in sorted(rows):
            writer.writerow(rows[key])
    print(f"OK: wrote {out_path.relative_to(root)} ({len(rows)} latest run-scoped artifacts)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
