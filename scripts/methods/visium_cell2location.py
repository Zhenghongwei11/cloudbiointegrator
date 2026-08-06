#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import random
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


def _pick_obsm_cell_abundance(adata, cell_types: list[str]):
    import numpy as np

    keys = [
        "means_cell_abundance_w_sf",
        "q05_cell_abundance_w_sf",
        "means_cell_abundance",
        "q05_cell_abundance",
    ]
    for k in keys:
        if k in getattr(adata, "obsm", {}):
            mat = adata.obsm[k]
            try:
                import pandas as pd

                if isinstance(mat, pd.DataFrame):
                    cols = list(mat.columns.astype(str))
                    arr = mat.to_numpy()
                    if len(cols) == arr.shape[1]:
                        return k, arr, cols
            except Exception:
                pass
            arr = np.asarray(mat)
            cols = list(cell_types) if len(cell_types) == arr.shape[1] else [f"ct_{i}" for i in range(arr.shape[1])]
            return k, arr, cols
    # Fallback: any 2D numeric with spot count.
    for k, v in getattr(adata, "obsm", {}).items():
        try:
            arr = np.asarray(v)
        except Exception:
            continue
        if arr.ndim == 2 and arr.shape[0] == adata.n_obs and arr.shape[1] >= 2:
            cols = list(cell_types) if len(cell_types) == arr.shape[1] else [f"ct_{i}" for i in range(arr.shape[1])]
            return k, arr, cols
    raise RuntimeError("cell2location did not produce an abundance matrix in adata_sp.obsm")


def main() -> int:
    p = argparse.ArgumentParser(description="Run cell2location for Visium using a scRNA reference + labels.")
    p.add_argument("--visium-dir", required=True)
    p.add_argument("--scrna-dir", required=True)
    p.add_argument("--labels-tsv", required=True, help="TSV with columns: barcode, label (scRNA cell labels).")
    p.add_argument("--dataset-id", default="")
    p.add_argument("--reference-dataset-id", default="")
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--max_cells", type=int, default=20000, help="Downsample scRNA cells for speed (0=disable).")
    p.add_argument("--max_spots", type=int, default=8000, help="Downsample in-tissue spots for speed (0=disable).")
    p.add_argument("--regression-max-epochs", type=int, default=250, help="RegressionModel epochs (reference signature).")
    p.add_argument("--max-epochs", type=int, default=2000, help="Cell2location training epochs (spatial mapping).")
    p.add_argument("--num-samples", type=int, default=200, help="Posterior export samples (both models).")
    p.add_argument("--n-cells-per-location", type=int, default=30, help="Cell2location N_cells_per_location hyperparam.")
    p.add_argument("--detection-alpha", type=float, default=20.0, help="Cell2location detection_alpha hyperparam.")
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
    except Exception as e:
        raise RuntimeError(f"missing torch dependency (required): {e}")

    try:
        import cell2location  # noqa: F401
        from cell2location.models import Cell2location, RegressionModel
    except Exception as e:
        raise RuntimeError(
            "Missing cell2location dependencies. Build/run with an image that includes `cell2location`.\n"
            f"Import error: {e}"
        )

    # Numerical guard: some pyro/torch combos can yield an Exponential sample of exactly 0.0
    # under certain deterministic RNG states, which then triggers invalid Gamma parameterization
    # in cell2location's generative model (rate==0.0). Clamp Exponential samples away from 0.
    #
    # This is a minimal stability fix and is recorded in the runner JSON for auditability.
    exp_sample_min = 1e-8
    gamma_rate_min = 1e-8
    gamma_concentration_min = 1e-8
    gamma_poisson_min = 1e-8
    try:
        import pyro.distributions as dist

        _OrigExp = dist.Exponential

        class _SafeExponential(_OrigExp):  # type: ignore[misc]
            def sample(self, sample_shape=torch.Size()):  # type: ignore[override]
                x = super().sample(sample_shape)
                return torch.clamp(x, min=exp_sample_min)

            def rsample(self, sample_shape=torch.Size()):  # type: ignore[override]
                x = super().rsample(sample_shape)
                return torch.clamp(x, min=exp_sample_min)

        dist.Exponential = _SafeExponential  # type: ignore[assignment]

        _OrigGamma = dist.Gamma

        def _clamp_param(val, min_val: float, like=None):
            if torch.is_tensor(val):
                return torch.clamp(val, min=min_val)
            if torch.is_tensor(like):
                return torch.clamp(torch.as_tensor(val, device=like.device, dtype=like.dtype), min=min_val)
            return torch.clamp(torch.as_tensor(val), min=min_val)

        def _safe_gamma(concentration, rate, validate_args=None):
            concentration = _clamp_param(concentration, gamma_concentration_min)
            rate = _clamp_param(rate, gamma_rate_min, like=concentration)
            return _OrigGamma(concentration, rate, validate_args=validate_args)

        dist.Gamma = _safe_gamma  # type: ignore[assignment]

        _OrigGammaPoisson = dist.GammaPoisson

        def _safe_gamma_poisson(concentration, rate, validate_args=None):
            concentration = _clamp_param(concentration, gamma_poisson_min)
            rate = _clamp_param(rate, gamma_poisson_min, like=concentration)
            concentration = torch.nan_to_num(
                concentration, nan=gamma_poisson_min, posinf=gamma_poisson_min, neginf=gamma_poisson_min
            )
            rate = torch.nan_to_num(rate, nan=gamma_poisson_min, posinf=gamma_poisson_min, neginf=gamma_poisson_min)
            return _OrigGammaPoisson(concentration, rate, validate_args=validate_args)

        dist.GammaPoisson = _safe_gamma_poisson  # type: ignore[assignment]
    except Exception:
        pass

    # Determinism controls (best-effort; full determinism is not guaranteed for GPU training).
    seed = int(args.seed)
    random.seed(seed)
    np.random.seed(seed)
    try:
        torch.manual_seed(seed)
        if torch.cuda.is_available():
            torch.cuda.manual_seed_all(seed)
    except Exception:
        pass

    # Work around rare-but-fatal pathological initial RNG draws in downstream pyro/scvi codepaths
    # (e.g., an Exponential sample evaluating to exactly 0.0 and violating Gamma constraints).
    # This keeps runs deterministic while advancing the RNG state away from a problematic initial draw.
    try:
        _ = torch.rand(1024, device=("cuda:0" if torch.cuda.is_available() else "cpu"))
    except Exception:
        pass

    torch_device = args.device.strip()
    if not torch_device:
        torch_device = "cuda:0" if torch.cuda.is_available() else "cpu"
    use_gpu = torch_device.startswith("cuda")
    if use_gpu and not torch.cuda.is_available():
        raise RuntimeError(f"requested device={torch_device} but torch.cuda.is_available()==False")

    # scvi-tools/lightning trainer kwargs (cell2location forwards **kwargs internally).
    # Avoid legacy `use_gpu=` kwargs, which break on newer lightning/scvi stacks.
    # scvi-tools expects `accelerator` + `device`, where `device` is passed as Trainer.devices.
    # For GPU, pass device=1 (single GPU); for CPU, device=1 (single process).
    trainer_kwargs: dict[str, object] = {"accelerator": "gpu" if use_gpu else "cpu", "device": 1}

    # Load scRNA reference counts + labels
    adata_sc = read_10x_any(scrna_dir)
    labels_df = pd.read_csv(labels_path, sep="\t")
    if "barcode" not in labels_df.columns or "label" not in labels_df.columns:
        raise ValueError("--labels-tsv must have columns: barcode, label")
    labels_df["barcode"] = labels_df["barcode"].astype(str)
    labels_df["label"] = labels_df["label"].astype(str)
    labels_df = labels_df.set_index("barcode", drop=True)

    overlap = adata_sc.obs_names.intersection(labels_df.index.astype(str))
    if overlap.size < 50:
        raise ValueError("too few overlapping scRNA barcodes between matrix and labels (need >=50)")
    adata_sc = adata_sc[overlap, :].copy()
    adata_sc.obs["cell_type"] = labels_df.loc[overlap, "label"].to_numpy()

    if args.max_cells and adata_sc.n_obs > args.max_cells:
        rng = np.random.default_rng(seed)
        idx = rng.choice(adata_sc.n_obs, size=int(args.max_cells), replace=False)
        adata_sc = adata_sc[idx, :].copy()

    # Drop rare cell types to avoid unstable parameter estimates (and NaNs).
    min_cells_per_type = 5
    try:
        ct_counts = adata_sc.obs["cell_type"].value_counts()
        keep_ct = ct_counts[ct_counts >= min_cells_per_type].index
        adata_sc = adata_sc[adata_sc.obs["cell_type"].isin(keep_ct)].copy()
    except Exception:
        pass
    if adata_sc.n_obs < 50:
        raise ValueError("too few scRNA cells after cell-type filtering (need >=50)")

    # Load Visium (in-tissue) spots
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

    if args.max_spots and adata_sp.n_obs > args.max_spots:
        rng = np.random.default_rng(seed)
        idx = rng.choice(adata_sp.n_obs, size=int(args.max_spots), replace=False)
        adata_sp = adata_sp[idx, :].copy()

    # Shared genes
    sc.pp.filter_genes(adata_sc, min_cells=3)
    sc.pp.filter_genes(adata_sp, min_cells=3)
    genes_shared = np.intersect1d(adata_sc.var_names.astype(str), adata_sp.var_names.astype(str))
    if genes_shared.size < 1000:
        raise ValueError(f"low gene overlap between scRNA and Visium: {genes_shared.size} (<1000)")
    adata_sc = adata_sc[:, genes_shared].copy()
    adata_sp = adata_sp[:, genes_shared].copy()

    # Remove empty cells/genes after intersection to avoid zero totals -> NaNs.
    sc.pp.filter_cells(adata_sc, min_counts=1)
    sc.pp.filter_cells(adata_sp, min_counts=1)
    sc.pp.filter_genes(adata_sc, min_cells=1)
    sc.pp.filter_genes(adata_sp, min_cells=1)
    if adata_sc.n_obs < 50:
        raise ValueError("too few scRNA cells after filtering (need >=50)")
    if adata_sp.n_obs < 50:
        raise ValueError("too few in-tissue spots after filtering (need >=50)")
    genes_shared = np.intersect1d(adata_sc.var_names.astype(str), adata_sp.var_names.astype(str))
    if genes_shared.size < 1000:
        raise ValueError(f"low gene overlap after filtering: {genes_shared.size} (<1000)")
    adata_sc = adata_sc[:, genes_shared].copy()
    adata_sp = adata_sp[:, genes_shared].copy()

    # cell2location expects raw counts; ensure they are accessible.
    try:
        adata_sc.layers["counts"] = adata_sc.X.copy()
        adata_sp.layers["counts"] = adata_sp.X.copy()
    except Exception:
        pass

    # Prepare reference signatures
    t0 = time.time()
    RegressionModel.setup_anndata(adata_sc, labels_key="cell_type")
    reg = RegressionModel(adata_sc)
    reg.train(max_epochs=int(args.regression_max_epochs), **trainer_kwargs)
    adata_sc = reg.export_posterior(
        adata_sc,
        sample_kwargs={"num_samples": int(args.num_samples), "batch_size": 2500},
    )
    if "means_per_cluster_mu_fg" not in adata_sc.varm:
        raise RuntimeError("expected reference signature in adata_sc.varm['means_per_cluster_mu_fg'] (cell2location API change?)")
    cell_types = list(adata_sc.uns.get("mod", {}).get("factor_names", []))
    if not cell_types:
        # Fallback: derive from labels
        cell_types = sorted(adata_sc.obs["cell_type"].astype(str).unique().tolist())
    inf_aver = pd.DataFrame(adata_sc.varm["means_per_cluster_mu_fg"], index=adata_sc.var_names, columns=cell_types)
    inf_aver_min = 1e-6
    inf_aver_clipped = False
    try:
        inf_aver = inf_aver.replace([np.inf, -np.inf], np.nan)
        if inf_aver.isna().to_numpy().any():
            inf_aver = inf_aver.fillna(inf_aver_min)
        if (inf_aver <= 0).to_numpy().any():
            inf_aver = inf_aver.clip(lower=inf_aver_min)
            inf_aver_clipped = True
    except Exception:
        pass

    # Fit cell2location on spatial data
    adata_sp.obs["sample"] = "sample"
    Cell2location.setup_anndata(adata_sp, batch_key="sample")
    c2l = Cell2location(
        adata_sp,
        cell_state_df=inf_aver,
        N_cells_per_location=int(args.n_cells_per_location),
        detection_alpha=float(args.detection_alpha),
    )
    c2l.train(max_epochs=int(args.max_epochs), batch_size=None, train_size=1, **trainer_kwargs)
    adata_sp = c2l.export_posterior(
        adata_sp,
        sample_kwargs={"num_samples": int(args.num_samples), "batch_size": adata_sp.n_obs},
    )
    wall = time.time() - t0

    # Extract abundance estimates and normalize to proportions for comparability.
    obsm_key, abund, cols = _pick_obsm_cell_abundance(adata_sp, cell_types)
    abund = np.asarray(abund)
    abund[abund < 0] = 0
    rs = abund.sum(axis=1, keepdims=True)
    rs[rs == 0] = 1.0
    P = abund / rs

    out_long = []
    barcodes = adata_sp.obs_names.astype(str).tolist()
    for i, bc in enumerate(barcodes):
        for j, ct in enumerate(cols):
            out_long.append(
                {
                    "dataset_id": args.dataset_id,
                    "reference_dataset_id": args.reference_dataset_id,
                    "barcode": bc,
                    "cell_type": str(ct),
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
        "device": torch_device,
        "obsm_key": obsm_key,
        "numerical_guard_exponential_sample_min": float(exp_sample_min),
        "numerical_guard_gamma_rate_min": float(gamma_rate_min),
        "numerical_guard_gamma_concentration_min": float(gamma_concentration_min),
        "numerical_guard_gamma_poisson_min": float(gamma_poisson_min),
        "numerical_guard_inf_aver_clipped": bool(inf_aver_clipped),
        "numerical_guard_inf_aver_min": float(inf_aver_min),
        "regression_max_epochs": int(args.regression_max_epochs),
        "max_epochs": int(args.max_epochs),
        "num_samples": int(args.num_samples),
        "n_cells_per_location": int(args.n_cells_per_location),
        "detection_alpha": float(args.detection_alpha),
        "max_cells": int(args.max_cells),
        "max_spots": int(args.max_spots),
    }

    versions = {
        "cell2location": getattr(__import__("cell2location"), "__version__", ""),
        "torch": getattr(torch, "__version__", ""),
    }

    out_json = Path(args.out_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps({"metrics": metrics, "versions": versions, "notes": "outputs normalized per spot"}, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
