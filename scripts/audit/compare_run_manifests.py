#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path


def load(path: Path) -> dict[tuple[str, str], dict[str, str]]:
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        out: dict[tuple[str, str], dict[str, str]] = {}
        for row in reader:
            key = (row["stage"], row["dataset_id"])
            out[key] = {k: (v or "") for k, v in row.items()}
    return out


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Compare two run-scoped artifact manifests (baseline vs other); exit 1 on mismatch/missing."
    )
    ap.add_argument("baseline", help="Baseline manifest TSV (committed).")
    ap.add_argument("other", help="Other manifest TSV (e.g., CI run).")
    args = ap.parse_args()

    base = load(Path(args.baseline))
    other = load(Path(args.other))

    rc = 0
    n_match = n_mismatch = n_missing = n_extra = 0
    keys = sorted(set(base) | set(other))
    for key in keys:
        stage, ds = key
        if key not in base:
            print(f"WARN  extra in other: stage={stage} dataset={ds}")
            n_extra += 1
            continue
        if key not in other:
            print(f"FAIL  missing in other: stage={stage} dataset={ds}")
            n_missing += 1
            rc = 1
            continue
        b, o = base[key], other[key]
        if b["sha256"] == o["sha256"]:
            print(f"PASS  {stage} / {ds}  sha256={b['sha256'][:16]}...")
            n_match += 1
        else:
            print(f"FAIL  {stage} / {ds}")
            print(f"  baseline={b['sha256']}  ({b['artifact_path']})")
            print(f"  other   ={o['sha256']}  ({o['artifact_path']})")
            n_mismatch += 1
            rc = 1

    print(f"\nSummary: matched={n_match} mismatched={n_mismatch} missing={n_missing} extra={n_extra}")
    if rc == 0:
        print("RESULT: PASS — run-scoped artifacts are byte-identical across environments")
    else:
        print("RESULT: FAIL — run-scoped artifacts differ across environments")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
