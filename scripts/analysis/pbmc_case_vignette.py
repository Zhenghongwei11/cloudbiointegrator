#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]


def read_10x_any(dir_path: Path):
    # Local copy of the 10x reader used by the pipeline (gz + non-gz; v1 and v3 layouts).
    import csv as _csv
    import gzip as _gzip

    import anndata as ad
    import numpy as _np
    from scipy import io as _io
    from scipy import sparse as _sparse

    def _pick(names: list[str]) -> Path:
        for n in names:
            p = dir_path / n
            if p.exists():
                return p
        raise FileNotFoundError(f"missing required 10x file in {dir_path}: one of {names}")

    mtx = _pick(["matrix.mtx.gz", "matrix.mtx"])
    feats = _pick(["features.tsv.gz", "features.tsv", "genes.tsv.gz", "genes.tsv"])
    bcs = _pick(["barcodes.tsv.gz", "barcodes.tsv"])

    def _open_text(p: Path):
        if p.name.endswith(".gz"):
            return _gzip.open(p, "rt", encoding="utf-8")
        return p.open("r", encoding="utf-8")

    def _open_bin(p: Path):
        if p.name.endswith(".gz"):
            return _gzip.open(p, "rb")
        return p.open("rb")

    with _open_bin(mtx) as f:
        mat = _io.mmread(f)
    if not _sparse.issparse(mat):
        mat = _sparse.csr_matrix(mat)
    X = mat.T.tocsr()

    gene_symbols: list[str] = []
    with _open_text(feats) as f:
        reader = _csv.reader(f, delimiter="\t")
        for row in reader:
            if not row:
                continue
            if len(row) == 1:
                gene_symbols.append(row[0])
            else:
                gene_symbols.append(row[1])

    barcodes: list[str] = []
    with _open_text(bcs) as f:
        for line in f:
            line = line.strip()
            if line:
                barcodes.append(line)

    if X.shape[0] != len(barcodes):
        raise ValueError(f"10x barcodes mismatch: X has {X.shape[0]} cells but barcodes has {len(barcodes)}")
    if X.shape[1] != len(gene_symbols):
        raise ValueError(f"10x features mismatch: X has {X.shape[1]} genes but features has {len(gene_symbols)}")

    adata = ad.AnnData(X=X)
    adata.obs_names = _np.array(barcodes, dtype=str)
    adata.var_names = _np.array(gene_symbols, dtype=str)
    try:
        adata.var_names_make_unique()
    except Exception:
        pass
    return adata


def ensure_tsv_with_header(path: Path, header: list[str], *, overwrite: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if (not overwrite) and path.exists() and path.stat().st_size > 0:
        return
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(header)


def stable_json_sha256(obj: Any) -> str:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(s).hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate a PBMC case vignette table (Monocytes) for the manuscript.")
    ap.add_argument(
        "--input-dir",
        required=True,
        help="Workspace-relative path to filtered_feature_bc_matrix/.",
    )
    ap.add_argument("--dataset-id", required=True)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument(
        "--out-tsv",
        default="results/figures/pbmc_case_vignette.tsv",
        help="Output TSV (default: results/figures/pbmc_case_vignette.tsv).",
    )
    ap.add_argument(
        "--out-tsv-full",
        default="results/figures/pbmc_cluster_label_crosstab.tsv",
        help="Optional full cluster-by-label crosstab TSV (default: results/figures/pbmc_cluster_label_crosstab.tsv).",
    )
    ap.add_argument("--overwrite", action="store_true", help="Overwrite outputs instead of appending.")
    args = ap.parse_args()

    input_dir = (ROOT / args.input_dir).resolve()
    if not input_dir.exists():
        raise SystemExit(f"--input-dir not found: {input_dir}")

    try:
        import numpy as np
        import pandas as pd
        import scanpy as sc
    except Exception as e:
        raise SystemExit(f"missing python deps (run via Docker): {e}")

    # Read and run a minimal baseline workflow consistent with the repo defaults.
    adata = read_10x_any(input_dir)
    adata.var["mt"] = adata.var_names.str.upper().str.startswith("MT-")
    sc.pp.calculate_qc_metrics(adata, qc_vars=["mt"], inplace=True)

    rng = np.random.default_rng(int(args.seed))
    perm = rng.permutation(adata.n_obs)
    adata = adata[perm, :].copy()

    min_genes = 200
    max_pct_mt = 20.0
    sc.pp.filter_cells(adata, min_genes=min_genes)
    adata = adata[adata.obs["pct_counts_mt"] <= max_pct_mt, :].copy()
    sc.pp.filter_genes(adata, min_cells=3)

    try:
        adata.layers["counts"] = adata.X.copy()
    except Exception:
        pass

    sc.pp.normalize_total(adata, target_sum=1e4)
    sc.pp.log1p(adata)
    adata_for_anno = adata.copy()

    # HVGs + cluster (Scanpy baseline default)
    sc.pp.highly_variable_genes(adata, flavor="cell_ranger", n_top_genes=2000, layer="counts")
    adata = adata[:, adata.var["highly_variable"]].copy()
    sc.pp.scale(adata, max_value=10)
    sc.tl.pca(adata, svd_solver="arpack", random_state=int(args.seed))
    sc.pp.neighbors(adata, n_neighbors=15, n_pcs=30, random_state=int(args.seed))
    sc.tl.leiden(adata, resolution=0.5, random_state=int(args.seed), key_added="cluster")

    clusters = adata.obs["cluster"].astype(str).to_numpy()
    adata_for_anno.obs["cluster"] = clusters

    # CellTypist (Immune_All_Low)
    try:
        import celltypist
    except Exception as e:
        raise SystemExit(f"missing celltypist (run via Docker): {e}")

    model_path = ROOT / "data" / "references" / "celltypist" / "Immune_All_Low.pkl"
    if not model_path.exists():
        fetcher = ROOT / "scripts" / "data" / "fetch_celltypist_model.py"
        code = __import__("subprocess").call(["python3", str(fetcher), "--model", "Immune_All_Low.pkl"], cwd=str(ROOT))
        if code != 0:
            raise SystemExit("failed to fetch CellTypist model")

    pred = celltypist.annotate(adata_for_anno, model=str(model_path), majority_voting=True)
    labels_obj = pred.predicted_labels
    if hasattr(labels_obj, "ndim") and getattr(labels_obj, "ndim") == 2:
        try:
            cols = list(getattr(labels_obj, "columns", []))
            if "majority_voting" in cols:
                labels_obj = labels_obj["majority_voting"]
            elif "predicted_labels" in cols:
                labels_obj = labels_obj["predicted_labels"]
            else:
                labels_obj = labels_obj.iloc[:, 0]
        except Exception:
            labels_obj = labels_obj.iloc[:, 0]
    labels = labels_obj.astype(str).to_numpy()
    adata_for_anno.obs["celltypist_label"] = labels

    # Cluster-by-label distribution (full crosstab for transparency).
    df = pd.DataFrame({"cluster": clusters, "label": labels})
    tab = pd.crosstab(df["cluster"], df["label"])

    # Pick a monocyte-focused cluster for a concrete PBMC vignette.
    mono_cols = [c for c in tab.columns if "monocyte" in str(c).lower() or "monocytes" in str(c).lower()]
    if not mono_cols:
        raise SystemExit("no monocyte-like labels detected from CellTypist; cannot build vignette")
    mono_counts = tab[mono_cols].sum(axis=1)
    best_cluster = str(mono_counts.idxmax())
    cluster_n = int(tab.loc[best_cluster].sum())
    mono_n = int(mono_counts.loc[best_cluster])
    mono_frac = float(mono_n / cluster_n) if cluster_n else float("nan")

    label_counts = tab.loc[best_cluster, mono_cols]
    best_label = str(label_counts.idxmax())
    best_label_n = int(label_counts.max())

    # Marker genes for the selected cluster (on log1p-normalized space).
    marker_genes = ""
    try:
        sc.tl.rank_genes_groups(adata_for_anno, groupby="cluster", method="t-test", n_genes=50, use_raw=False)
        mdf = sc.get.rank_genes_groups_df(adata_for_anno, group=best_cluster)
        mdf = mdf.dropna()
        top = mdf.head(8)
        marker_genes = ";".join(top["names"].astype(str).tolist())
    except Exception:
        marker_genes = ""

    # Vignette row (submission-facing; keep it compact and citeable).
    params = {
        "qc_min_genes": min_genes,
        "qc_max_pct_mt": max_pct_mt,
        "hvg_flavor": "cell_ranger",
        "hvg_n_top": 2000,
        "neighbors_k": 15,
        "pca_n_pcs": 30,
        "leiden_resolution": 0.5,
        "celltypist_model": "Immune_All_Low.pkl",
        "majority_voting": True,
        "seed": int(args.seed),
    }
    header = [
        "dataset_id",
        "case_id",
        "cluster_id",
        "cluster_n_cells",
        "celltypist_label",
        "dominant_label_n_cells",
        "dominant_label_frac_in_cluster",
        "monocyte_total_n_cells",
        "monocyte_total_frac_in_cluster",
        "top_marker_genes",
        "params_sha256",
        "notes",
    ]
    out_tsv = (ROOT / args.out_tsv).resolve()
    ensure_tsv_with_header(out_tsv, header, overwrite=bool(args.overwrite))
    dominant_frac = float(best_label_n / cluster_n) if cluster_n else float("nan")
    row = {
        "dataset_id": args.dataset_id,
        "case_id": "monocytes",
        "cluster_id": best_cluster,
        "cluster_n_cells": str(cluster_n),
        "celltypist_label": best_label,
        "dominant_label_n_cells": str(best_label_n),
        "dominant_label_frac_in_cluster": f"{dominant_frac:.4f}",
        "monocyte_total_n_cells": str(mono_n),
        "monocyte_total_frac_in_cluster": f"{mono_frac:.4f}",
        "top_marker_genes": marker_genes,
        "params_sha256": stable_json_sha256(params),
        "notes": "Case vignette: cluster with highest monocyte-labeled fraction (CellTypist) under Scanpy baseline.",
    }
    with out_tsv.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header, delimiter="\t", extrasaction="ignore")
        w.writerow(row)

    # Full crosstab output (wide TSV).
    out_full = (ROOT / args.out_tsv_full).resolve()
    out_full.parent.mkdir(parents=True, exist_ok=True)
    tab.to_csv(out_full, sep="\t")

    print(f"OK: wrote vignette row to {out_tsv}")
    print(f"OK: wrote full crosstab to {out_full}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
