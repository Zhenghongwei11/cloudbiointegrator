#!/usr/bin/env python3
from __future__ import annotations

import csv
import gzip
from pathlib import Path
from typing import Iterable

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
SUMMARY_PATH = ROOT / "results" / "dataset_summary.tsv"


def _count_lines(path: Path) -> int:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as f:
        return sum(1 for _ in f)


def count_barcodes(matrix_dir: Path) -> int:
    for name in ("barcodes.tsv.gz", "barcodes.tsv"):
        p = matrix_dir / name
        if p.exists():
            return _count_lines(p)
    raise FileNotFoundError(f"barcodes.tsv(.gz) not found under {matrix_dir}")


def count_features(matrix_dir: Path) -> int:
    for name in ("features.tsv.gz", "features.tsv", "genes.tsv.gz", "genes.tsv"):
        p = matrix_dir / name
        if p.exists():
            return _count_lines(p)
    raise FileNotFoundError(f"features/genes TSV not found under {matrix_dir}")


def count_visium_in_tissue(spatial_dir: Path) -> int:
    for name in ("tissue_positions_list.csv", "tissue_positions.csv"):
        p = spatial_dir / name
        if p.exists():
            with p.open("r", encoding="utf-8") as f:
                reader = csv.reader(f)
                first = next(reader)
                if len(first) >= 2 and first[1].lower() == "in_tissue":
                    rows = reader
                else:
                    rows = [first, *reader]
                return sum(1 for row in rows if len(row) >= 2 and row[1] == "1")
    raise FileNotFoundError(f"tissue_positions(.csv) not found under {spatial_dir}")


def _sum(values: Iterable[int | float]) -> int:
    return int(sum(v for v in values if v is not None))


def main() -> int:
    if not SUMMARY_PATH.exists():
        raise FileNotFoundError(f"missing {SUMMARY_PATH}")

    df = pd.read_csv(SUMMARY_PATH, sep="\t")
    for col in ("n_samples", "n_donors", "n_cells_or_spots", "n_genes"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    pbmc3k_dir = ROOT / "data" / "smoke" / "pbmc3k_real" / "filtered_feature_bc_matrix"
    pbmc10k_dir = ROOT / "data" / "smoke" / "pbmc10k_v3_real" / "filtered_feature_bc_matrix"
    visium_ln_spatial = ROOT / "data" / "smoke" / "visium_human_lymph_node_real" / "spatial"

    pbmc3k_cells = count_barcodes(pbmc3k_dir)
    pbmc10k_cells = count_barcodes(pbmc10k_dir)
    pbmc3k_genes = count_features(pbmc3k_dir)
    pbmc10k_genes = count_features(pbmc10k_dir)
    visium_ln_spots = count_visium_in_tissue(visium_ln_spatial)

    dataset_fill = {
        "10x_PBMC_3k_scRNA_2016_S3": {
            "n_samples": 1,
            "n_cells_or_spots": pbmc3k_cells,
            "n_genes": pbmc3k_genes,
        },
        "10x_PBMC_10k_v3_scRNA_2018_S3": {
            "n_samples": 1,
            "n_cells_or_spots": pbmc10k_cells,
            "n_genes": pbmc10k_genes,
        },
        "10x_PBMC_10k_scRNA": {
            "n_samples": 1,
            "n_cells_or_spots": pbmc10k_cells,
            "n_genes": pbmc10k_genes,
        },
        "PBMC3K_PLUS_PBMC10K_INTEGRATION": {
            "n_samples": 2,
            "n_cells_or_spots": _sum([pbmc3k_cells, pbmc10k_cells]),
        },
        "10x_PBMC3K_PBMC10K_V3_INTEGRATION_PAIR_S3": {
            "n_samples": 2,
            "n_cells_or_spots": _sum([pbmc3k_cells, pbmc10k_cells]),
        },
        "10x_Visium_Human_Lymph_Node_1p1_cf": {
            "n_samples": 1,
            "n_cells_or_spots": visium_ln_spots,
        },
    }

    for dataset_id, fill in dataset_fill.items():
        mask = df["dataset_id"] == dataset_id
        if not mask.any():
            continue
        for col, val in fill.items():
            if col not in df.columns:
                continue
            df.loc[mask & df[col].isna(), col] = val

    df.to_csv(SUMMARY_PATH, sep="\t", index=False)
    print(f"OK: updated {SUMMARY_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
