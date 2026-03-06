#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import shutil
import time
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]

PRODUCT_DOCS = [
    # Product/runtime docs (NOT manuscript/submission artifacts)
    "CLAIMS.tsv",
    "EVALUATION_MATRIX.tsv",
    "FIGURE_PROVENANCE.tsv",
    # AI disclosure: prompt used to generate the conceptual schematic for Figure 1.
    # Kept in the review bundle for transparency; does not affect quantitative figure panels.
    "F1_ai_figure_prompt.docx",
    "METHOD_LIBRARY_SCRNA.md",
    "METHOD_LIBRARY_VISIUM.md",
    "DATA_PLAN.md",
    "COMPUTE_PLAN.md",
    "CLOUD_RUNBOOK.md",
    "FINAL_CLOUD_REPRO_CHECKLIST.md",
    "REVIEWER_RUN_CHECKLIST.md",
    "PRODUCT_RUNBOOK.md",
    "NOT_APPLICABLE.md",
    "REVIEW_BUNDLE_POLICY.md",
]


def sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _iter_required_audit_zips(set_tsv: Path) -> list[Path]:
    required: list[Path] = []
    with set_tsv.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            if (row.get("required") or "").strip().lower() != "yes":
                continue
            local_target = (row.get("local_target") or "").strip()
            if not local_target:
                continue
            required.append((ROOT / local_target).resolve())
    return required


def _should_skip(path: Path) -> bool:
    name = path.name
    if name == ".DS_Store":
        return True
    if name.startswith("._"):
        return True
    return False


def add_file(zf: zipfile.ZipFile, src: Path, arcname: str) -> None:
    if _should_skip(src):
        return
    zf.write(src, arcname=arcname)


def add_tree(zf: zipfile.ZipFile, src_dir: Path, arc_prefix: str) -> None:
    for p in sorted(src_dir.rglob("*")):
        if not p.is_file():
            continue
        if _should_skip(p):
            continue
        rel = p.relative_to(src_dir)
        add_file(zf, p, f"{arc_prefix}/{rel.as_posix()}")


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Build docs/review_bundle/ with zip + checksums (product reproducibility bundle)."
    )
    ap.add_argument("--out-dir", default="docs/review_bundle", help="Output directory (default: docs/review_bundle).")
    ap.add_argument(
        "--set-tsv",
        default="docs/SUBMISSION_AUDIT_SET.tsv",
        help="Audit subset TSV (default: docs/SUBMISSION_AUDIT_SET.tsv).",
    )
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing review_bundle.zip/checksums.sha256.")
    args = ap.parse_args()

    out_dir = (ROOT / args.out_dir).resolve()
    set_tsv = (ROOT / args.set_tsv).resolve()

    if not set_tsv.exists():
        raise SystemExit(f"missing audit set TSV: {set_tsv}")

    required_audits = _iter_required_audit_zips(set_tsv)
    missing = [p for p in required_audits if (not p.exists() or p.stat().st_size == 0)]
    if missing:
        msg = "\n".join(f"- {p}" for p in missing)
        raise SystemExit(
            "Missing required audit zip(s). Pull them first (or rerun cloud upload), then retry.\n"
            f"Missing:\n{msg}\n\n"
            "Tip: bash scripts/cloud/pull_submission_audits.sh"
        )

    out_dir.mkdir(parents=True, exist_ok=True)
    bundle_zip = out_dir / "review_bundle.zip"
    checksums = out_dir / "checksums.sha256"
    readme = out_dir / "README.md"

    if not args.overwrite and (bundle_zip.exists() or checksums.exists()):
        raise SystemExit(f"Refusing to overwrite existing outputs under: {out_dir} (pass --overwrite)")

    # Fresh rebuild
    if bundle_zip.exists():
        bundle_zip.unlink()
    if checksums.exists():
        checksums.unlink()

    timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    readme.write_text(
        "\n".join(
            [
                "# Review Bundle (Product Reproducibility)",
                "",
                f"Generated: `{timestamp}`",
                "",
                "This folder is a reviewer-facing **product reproducibility** bundle:",
                "- `review_bundle.zip` (all artifacts)",
                "- `checksums.sha256` (SHA-256 for every file in the zip, by path)",
                "",
                "## Contents",
                "- Product docs (from `docs/`, excluding manuscript/submission drafts and citation-verification materials)",
                "- Publication figures (`plots/publication/pdf` + `plots/publication/png`)",
                "- Results tables (`results/`)",
                "- Submission audit zips listed in `docs/SUBMISSION_AUDIT_SET.tsv`",
                "",
                "## Explicitly Excluded",
                "- Manuscript drafts (Markdown/DOCX), cover letters, and journal-specific submission packages",
                "- Any scripts whose sole purpose is manuscript formatting/conversion",
                "- Citation verification logs and literature-benchmarking notes",
                "",
                "## Reproduction",
                "See `docs/FINAL_CLOUD_REPRO_CHECKLIST.md` for the fresh-VM rerun protocol.",
                "",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    # Build zip deterministically-ish (sorted traversal).
    with zipfile.ZipFile(bundle_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        # Top-level index
        add_file(zf, set_tsv, "docs/SUBMISSION_AUDIT_SET.tsv")
        for name in PRODUCT_DOCS:
            p = ROOT / "docs" / name
            if p.exists():
                add_file(zf, p, f"docs/{name}")

        # Data manifest (small, essential for reproduction)
        data_manifest = ROOT / "data" / "manifest.tsv"
        if data_manifest.exists():
            add_file(zf, data_manifest, "data/manifest.tsv")

        # Schemas (contract for allowed operations)
        schema = ROOT / "schemas" / "action_schema_v1.json"
        if schema.exists():
            add_file(zf, schema, "schemas/action_schema_v1.json")

        # Figures
        fig_dir = ROOT / "plots" / "publication"
        if fig_dir.exists():
            add_tree(zf, fig_dir, "plots/publication")

        # Results tables (entire tree is small by design in this repo contract)
        results_dir = ROOT / "results"
        if results_dir.exists():
            add_tree(zf, results_dir, "results")

        # Audit zips
        for p in required_audits:
            add_file(zf, p, f"docs/audit_runs_submission/{p.name}")

        # Bundle README
        add_file(zf, readme, "docs/review_bundle/README.md")

    # Write checksums for the zip contents (path-based, stable order).
    entries: list[str] = []
    with zipfile.ZipFile(bundle_zip, "r") as zf:
        for info in sorted(zf.infolist(), key=lambda i: i.filename):
            if info.is_dir():
                continue
            with zf.open(info, "r") as f:
                h = hashlib.sha256()
                for chunk in iter(lambda: f.read(1024 * 1024), b""):
                    h.update(chunk)
            entries.append(f"{h.hexdigest()}  {info.filename}")
    checksums.write_text("\n".join(entries) + "\n", encoding="utf-8")

    # Convenience copy of the zip checksum.
    (out_dir / "review_bundle.zip.sha256").write_text(f"{sha256_path(bundle_zip)}  review_bundle.zip\n", encoding="utf-8")

    print(f"OK: wrote {bundle_zip}")
    print(f"OK: wrote {checksums}")
    print(f"OK: wrote {readme}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
