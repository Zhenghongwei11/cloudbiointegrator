#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import time
from pathlib import Path


def open_text(path: Path):
    import gzip

    if path.name.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open("r", encoding="utf-8")


def read_10x_any(dir_path: Path):
    import gzip

    import anndata as ad
    import numpy as np
    from scipy import io as sio
    from scipy import sparse

    def pick(names: list[str]) -> Path:
        for n in names:
            p = dir_path / n
            if p.exists():
                return p
        raise FileNotFoundError(f"missing required 10x file in {dir_path}: one of {names}")

    mtx = pick(["matrix.mtx.gz", "matrix.mtx"])
    feats = pick(["features.tsv.gz", "features.tsv", "genes.tsv.gz", "genes.tsv"])
    bcs = pick(["barcodes.tsv.gz", "barcodes.tsv"])

    def open_bin(p: Path):
        if p.name.endswith(".gz"):
            return gzip.open(p, "rb")
        return p.open("rb")

    with open_bin(mtx) as f:
        mat = sio.mmread(f)
    if not sparse.issparse(mat):
        mat = sparse.csr_matrix(mat)
    X = mat.T.tocsr()  # cells x genes

    gene_symbols: list[str] = []
    with open_text(feats) as f:
        reader = csv.reader(f, delimiter="\t")
        for row in reader:
            if not row:
                continue
            if len(row) == 1:
                gene_symbols.append(row[0])
            else:
                gene_symbols.append(row[1])

    barcodes: list[str] = []
    with open_text(bcs) as f:
        for line in f:
            s = line.strip()
            if s:
                barcodes.append(s)

    if X.shape[0] != len(barcodes):
        raise ValueError(f"10x barcodes mismatch: X has {X.shape[0]} cells but barcodes has {len(barcodes)}")
    if X.shape[1] != len(gene_symbols):
        raise ValueError(f"10x features mismatch: X has {X.shape[1]} genes but features has {len(gene_symbols)}")

    adata = ad.AnnData(X=X)
    adata.obs_names = np.array(barcodes, dtype=str)
    adata.var_names = np.array(gene_symbols, dtype=str)
    try:
        adata.var_names_make_unique()
    except Exception:
        pass
    return adata


def read_visium_positions(spatial_dir: Path):
    import pandas as pd

    pos_csv = spatial_dir / "tissue_positions.csv"
    pos_list = spatial_dir / "tissue_positions_list.csv"
    if pos_csv.exists():
        df = pd.read_csv(pos_csv)
    elif pos_list.exists():
        df = pd.read_csv(
            pos_list,
            header=None,
            names=[
                "barcode",
                "in_tissue",
                "array_row",
                "array_col",
                "pxl_row_in_fullres",
                "pxl_col_in_fullres",
            ],
        )
    else:
        raise FileNotFoundError(f"missing tissue_positions*.csv under {spatial_dir}")

    df["barcode"] = df["barcode"].astype(str)
    df["in_tissue"] = df["in_tissue"].astype(int)
    df["array_row"] = df["array_row"].astype(int)
    df["array_col"] = df["array_col"].astype(int)
    return df


def main() -> int:
    p = argparse.ArgumentParser(description="Run Tangram mapping for Visium using a scRNA reference + labels.")
    p.add_argument("--visium-dir", required=True)
    p.add_argument("--scrna-dir", required=True)
    p.add_argument("--labels-tsv", required=True, help="TSV with columns: barcode, label (scRNA cell labels).")
    p.add_argument("--dataset-id", default="")
    p.add_argument("--reference-dataset-id", default="")
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--n_hvg", type=int, default=2000, help="Number of HVGs to use for mapping (speeds up).")
    p.add_argument("--max_cells", type=int, default=3000, help="Downsample scRNA cells for speed (0=disable).")
    p.add_argument("--device", default="", help="torch device, e.g. cpu or cuda:0 (default: auto).")
    p.add_argument("--out-weights-tsv", required=True)
    p.add_argument("--out-json", required=True)
    args = p.parse_args()

    visium_dir = Path(args.visium_dir)
    scrna_dir = Path(args.scrna_dir)
    labels_path = Path(args.labels_tsv)
    if not visium_dir.exists():
        raise FileNotFoundError(f"--visium-dir not found: {visium_dir}")
    if not scrna_dir.exists():
        raise FileNotFoundError(f"--scrna-dir not found: {scrna_dir}")
    if not labels_path.exists():
        raise FileNotFoundError(f"--labels-tsv not found: {labels_path}")

    try:
        import numpy as np
        import pandas as pd
        import scanpy as sc
    except Exception as e:
        raise RuntimeError(f"missing python deps (scanpy stack required): {e}")

    try:
        import torch
        import tangram as tg
    except Exception as e:
        raise RuntimeError(
            "Missing Tangram dependencies. Install `torch` + `tangram-sc` in the container image for this method.\n"
            f"Import error: {e}"
        )

    # Load data
    adata_sc = read_10x_any(scrna_dir)
    labels_df = pd.read_csv(labels_path, sep="\t")
    if "barcode" not in labels_df.columns or "label" not in labels_df.columns:
        raise ValueError("--labels-tsv must have columns: barcode, label")
    labels_df["barcode"] = labels_df["barcode"].astype(str)
    labels_df["label"] = labels_df["label"].astype(str)
    labels_df = labels_df.set_index("barcode", drop=True)

    # Align labels to adata_sc
    overlap = adata_sc.obs_names.intersection(labels_df.index.astype(str))
    if overlap.size < 50:
        raise ValueError("too few overlapping scRNA barcodes between matrix and labels (need >=50)")
    adata_sc = adata_sc[overlap, :].copy()
    adata_sc.obs["cell_type"] = labels_df.loc[overlap, "label"].to_numpy()

    if args.max_cells and adata_sc.n_obs > args.max_cells:
        rng = np.random.default_rng(args.seed)
        idx = rng.choice(adata_sc.n_obs, size=args.max_cells, replace=False)
        adata_sc = adata_sc[idx, :].copy()

    # Visium
    adata_sp = read_10x_any(visium_dir / "filtered_feature_bc_matrix")
    pos = read_visium_positions(visium_dir / "spatial")
    pos = pos[pos["in_tissue"] == 1].copy()
    if pos.shape[0] < 50:
        raise ValueError("too few in-tissue spots (need >=50)")

    pos = pos.set_index("barcode", drop=False)
    overlap_spots = adata_sp.obs_names.intersection(pos.index.astype(str))
    if overlap_spots.size < 50:
        raise ValueError("too few overlapping Visium barcodes between matrix and spatial positions (need >=50)")
    adata_sp = adata_sp[overlap_spots, :].copy()
    pos = pos.loc[overlap_spots, :]
    adata_sp.obsm["spatial"] = pos[["array_col", "array_row"]].to_numpy()

    # Basic preprocessing
    sc.pp.filter_genes(adata_sc, min_cells=3)
    sc.pp.filter_genes(adata_sp, min_cells=3)

    # Restrict to shared genes and HVGs (from scRNA)
    genes_shared = np.intersect1d(adata_sc.var_names.astype(str), adata_sp.var_names.astype(str))
    if genes_shared.size < 1000:
        raise ValueError(f"low gene overlap between scRNA and Visium: {genes_shared.size} (<1000)")
    adata_sc = adata_sc[:, genes_shared].copy()
    adata_sp = adata_sp[:, genes_shared].copy()

    # HVGs
    n_hvg = int(args.n_hvg)
    if n_hvg and adata_sc.n_vars > n_hvg:
        sc.pp.normalize_total(adata_sc, target_sum=1e4)
        sc.pp.log1p(adata_sc)
        sc.pp.highly_variable_genes(adata_sc, flavor="cell_ranger", n_top_genes=min(n_hvg, adata_sc.n_vars))
        hvg = adata_sc.var["highly_variable"].to_numpy()
        adata_sc = adata_sc[:, hvg].copy()
        adata_sp = adata_sp[:, adata_sc.var_names].copy()

    # Tangram recommends setting .X to dense log counts
    for a in (adata_sc, adata_sp):
        sc.pp.normalize_total(a, target_sum=1e4)
        sc.pp.log1p(a)

    device = args.device.strip()
    if not device:
        device = "cuda:0" if torch.cuda.is_available() else "cpu"

    # Map clusters (cell types) to space for speed and stability.
    tg.pp_adatas(adata_sc, adata_sp)
    t0 = time.time()
    ad_map = tg.map_cells_to_space(adata_sc, adata_sp, mode="clusters", cluster_label="cell_type", device=device)
    wall = time.time() - t0

    # Project cluster annotations to get per-spot cell-type scores/proportions.
    tg.project_cell_annotations(ad_map, adata_sp, annotation="cell_type")

    # Find the projected matrix in obsm (Tangram stores it under a fixed key in most versions).
    obsm_key = None
    for k in ["tangram_ct_pred", "tangram_cluster_pred", "tangram_pred", "tangram_cell_type_pred"]:
        if k in adata_sp.obsm:
            obsm_key = k
            break
    if obsm_key is None:
        # Fallback: pick any 2D numeric matrix with column names matching cell types.
        for k, v in adata_sp.obsm.items():
            try:
                arr = np.asarray(v)
            except Exception:
                continue
            if arr.ndim == 2 and arr.shape[0] == adata_sp.n_obs and arr.shape[1] >= 2:
                obsm_key = k
                break
    if obsm_key is None:
        raise RuntimeError("Tangram did not produce a projected cell-type matrix in adata_sp.obsm")

    mat = adata_sp.obsm[obsm_key]
    mat = np.asarray(mat)
    # Normalize rows to sum to 1 for comparable 'proportion-like' outputs.
    rs = mat.sum(axis=1, keepdims=True)
    rs[rs == 0] = 1.0
    P = mat / rs

    # Column names: Tangram may store them in .uns or in ad_map. Prefer cell types from scRNA.
    cell_types = sorted(adata_sc.obs["cell_type"].astype(str).unique().tolist())
    if P.shape[1] != len(cell_types):
        # Fallback: make generic names.
        cell_types = [f"ct_{i}" for i in range(P.shape[1])]

    out_long = []
    for i, bc in enumerate(adata_sp.obs_names.astype(str).tolist()):
        for j, ct in enumerate(cell_types):
            out_long.append(
                {
                    "dataset_id": args.dataset_id,
                    "reference_dataset_id": args.reference_dataset_id,
                    "barcode": bc,
                    "cell_type": ct,
                    "weight": float(P[i, j]),
                }
            )

    out_path = Path(args.out_weights_tsv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(out_long).to_csv(out_path, sep="\t", index=False)

    entropy = -np.sum(P * np.log(np.clip(P, 1e-12, 1.0)), axis=1)
    max_w = np.max(P, axis=1)
    metrics = {
        "n_spots_in_tissue": int(adata_sp.n_obs),
        "n_cell_types": int(P.shape[1]),
        "gene_overlap": int(adata_sc.n_vars),
        "mean_entropy": float(np.mean(entropy)),
        "mean_max_weight": float(np.mean(max_w)),
        "wall_time_s": float(wall),
        "device": device,
        "obsm_key": obsm_key,
    }

    versions = {
        "tangram": getattr(tg, "__version__", ""),
        "torch": getattr(torch, "__version__", ""),
    }

    out_json = Path(args.out_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps({"metrics": metrics, "versions": versions, "notes": "mode=clusters"}, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

