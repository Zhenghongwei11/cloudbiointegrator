#!/usr/bin/env python3
from __future__ import annotations

import argparse
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


DEFAULT_FILES = [
    # Figures (prefer current publication output; F4 may be frozen separately)
    "plots/publication/png/F1_system_contract.png",
    "plots/publication/png/F2_reproducibility.png",
    "plots/publication/png/F3_scrna_benchmark.png",
    "plots/publication/png/F4_spatial_benchmark.png",
    "plots/publication/png/F5_ops_benchmark.png",
    "plots/publication/png/F6_robustness_matrix.png",
    # Evidence tables
    "results/audit/reproducibility_checks.tsv",
    "results/benchmarks/runtime_cost_failure.tsv",
    "results/benchmarks/robustness_matrix.tsv",
    "results/benchmarks/biological_output_concordance.tsv",
]


def _add_file(z: zipfile.ZipFile, src: Path, arc: str) -> None:
    z.write(src, arcname=arc)


def main() -> int:
    ap = argparse.ArgumentParser(description="Build a small HF-demo bundle zip (figures + key TSV tables).")
    ap.add_argument(
        "--output",
        default="docs/submissions/PLOS_Computational_Biology/attachments/cloudbiointegrator_demo_bundle.zip",
        help="Output zip path.",
    )
    ap.add_argument(
        "--prefer-frozen-f4",
        action="store_true",
        help="If set and frozen F4 exists, include frozen F4 instead of plots/publication/png/F4_spatial_benchmark.png.",
    )
    args = ap.parse_args()

    out = (ROOT / args.output).resolve()
    out.parent.mkdir(parents=True, exist_ok=True)

    files = list(DEFAULT_FILES)
    if args.prefer_frozen_f4:
        frozen_f4 = ROOT / "plots" / "publication" / "frozen" / "png" / "F4_spatial_benchmark.png"
        if frozen_f4.exists():
            # Replace the default F4 if present.
            files = [f for f in files if not f.endswith("/F4_spatial_benchmark.png")]
            files.insert(3, "plots/publication/frozen/png/F4_spatial_benchmark.png")

    missing: list[str] = []
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as z:
        for rel in files:
            src = (ROOT / rel).resolve()
            if not src.exists():
                missing.append(rel)
                continue
            _add_file(z, src, arc=rel)

        z.writestr(
            "README.txt",
            "CloudBioIntegrator HF demo bundle.\n"
            "Contains publication PNGs (F1-F6) and key TSV evidence tables.\n"
            "Upload this zip to the Hugging Face Space viewer.\n",
        )

    if missing:
        print("WARN: missing files (not included):")
        for m in missing:
            print(f"  - {m}")

    print(f"OK: wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
