#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import shutil
from pathlib import Path


def open_text_maybe_gz(path: Path, mode: str):
    if path.name.endswith(".gz"):
        return gzip.open(path, mode, encoding="utf-8")
    return path.open(mode, encoding="utf-8")


def open_bin_maybe_gz(path: Path, mode: str):
    if path.name.endswith(".gz"):
        return gzip.open(path, mode)
    return path.open(mode)


def copy_maybe_gz(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    with open_bin_maybe_gz(src, "rb") as r, open_bin_maybe_gz(dst, "wb") as w:
        shutil.copyfileobj(r, w)


def find_one(parent: Path, candidates: list[str]) -> Path:
    for name in candidates:
        p = parent / name
        if p.exists():
            return p
    raise FileNotFoundError(f"missing required file in {parent}: one of {candidates}")


def convert_genes_to_features(genes_path: Path, features_path: Path) -> None:
    features_path.parent.mkdir(parents=True, exist_ok=True)
    with open_text_maybe_gz(genes_path, "rt") as r, open_text_maybe_gz(features_path, "wt") as w:
        for line in r:
            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) == 1:
                gene_id = parts[0]
                gene_name = parts[0]
            else:
                gene_id, gene_name = parts[0], parts[1]
            w.write(f"{gene_id}\t{gene_name}\tGene Expression\n")


def main() -> int:
    p = argparse.ArgumentParser(
        description="Convert a Cell Ranger v1 'filtered_gene_bc_matrices/<genome>/' folder into v3-style filtered_feature_bc_matrix/."
    )
    p.add_argument("--input-dir", required=True, help="Path to folder containing matrix.mtx + barcodes.tsv + genes.tsv (optionally .gz).")
    p.add_argument("--output-dir", required=True, help="Path to output filtered_feature_bc_matrix/ directory.")
    args = p.parse_args()

    in_dir = Path(args.input_dir).resolve()
    out_dir = Path(args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    matrix = find_one(in_dir, ["matrix.mtx", "matrix.mtx.gz"])
    barcodes = find_one(in_dir, ["barcodes.tsv", "barcodes.tsv.gz"])
    genes = find_one(in_dir, ["genes.tsv", "genes.tsv.gz"])

    # Keep output gz-ness consistent with input matrix.
    out_gz = matrix.name.endswith(".gz")
    matrix_out = out_dir / ("matrix.mtx.gz" if out_gz else "matrix.mtx")
    barcodes_out = out_dir / ("barcodes.tsv.gz" if barcodes.name.endswith(".gz") else "barcodes.tsv")
    features_out = out_dir / ("features.tsv.gz" if out_gz else "features.tsv")

    copy_maybe_gz(matrix, matrix_out)
    copy_maybe_gz(barcodes, barcodes_out)
    convert_genes_to_features(genes, features_out)

    print(f"OK: wrote {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

