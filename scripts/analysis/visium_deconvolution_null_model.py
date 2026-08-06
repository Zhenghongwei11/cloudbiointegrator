#!/usr/bin/env python3
"""Permutation null model for spotwise deconvolution concordance.

For each method pair (RCTD/Tangram, Tangram/cell2location,
RCTD/cell2location), we compare the observed spotwise cosine distribution
(L1-normalized composition vectors, shared cell types; same definition as
scripts/pipeline/run.py::_weights_concordance_cosine_by_spot_summary)
against a null distribution in which one method's composition vectors are
randomly reassigned to spots. Reassignment preserves each spot's marginal
composition vector but destroys spot-level correspondence.

Output columns (results/benchmarks/visium_cosine_null_model.tsv):
  pair, n_spots,
  observed_median, observed_q25, observed_q75,
  null_median_of_perm_medians, null_q25_of_perm_medians, null_q95_of_perm_medians,
  pooled_null_q99,               # 99th percentile of all permuted per-spot cosines
  perm_p_value,                  # P(null permutation median >= observed median)
  frac_spots_above_null_q99,     # fraction of observed spots above pooled null q99
  n_perm
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]

WEIGHTS = {
    "rctd": ROOT / "results" / "figures" / "visium_celltype_weights_rctd.tsv",
    "tangram": ROOT / "results" / "figures" / "visium_celltype_weights_tangram.tsv",
    "cell2location": ROOT / "results" / "figures" / "visium_celltype_weights_cell2location.tsv",
}
DEFAULT_OUT = ROOT / "results" / "benchmarks" / "visium_cosine_null_model.tsv"
CELL2LOCATION_PREFIX = "meanscell_abundance_w_sf_"

PAIRS = [
    ("rctd", "tangram"),
    ("tangram", "cell2location"),
    ("rctd", "cell2location"),
]


def _load_pivoted(path: Path, strip_prefix: str | None = None) -> pd.DataFrame:
    df = pd.read_csv(path, sep="\t")
    if strip_prefix:
        df["cell_type"] = df["cell_type"].str.replace(strip_prefix, "", regex=False)
    return df.pivot_table(index="barcode", columns="cell_type", values="weight", aggfunc="mean").fillna(0.0)


def _cosine_rows(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    num = np.sum(a * b, axis=1)
    den = np.sqrt(np.sum(a * a, axis=1)) * np.sqrt(np.sum(b * b, axis=1))
    den = np.where(den == 0, np.nan, den)
    cos = num / den
    return cos[np.isfinite(cos)]


def _aligned_matrices(a_path: Path, b_path: Path, b_strip_prefix: str | None = None) -> tuple[np.ndarray, np.ndarray, int]:
    aw = _load_pivoted(a_path)
    bw = _load_pivoted(b_path, strip_prefix=b_strip_prefix)
    spots = aw.index.intersection(bw.index)
    cts = aw.columns.intersection(bw.columns)
    if len(spots) < 50 or len(cts) < 5:
        raise RuntimeError(f"insufficient overlap: {len(spots)} spots, {len(cts)} cell types")
    a = aw.loc[spots, cts].to_numpy(dtype=float)
    b = bw.loc[spots, cts].to_numpy(dtype=float)
    a = a / np.clip(a.sum(axis=1, keepdims=True), 1e-12, None)
    b = b / np.clip(b.sum(axis=1, keepdims=True), 1e-12, None)
    return a, b, a.shape[0]


def run_pair(a: np.ndarray, b: np.ndarray, n_perm: int, rng: np.random.Generator) -> dict:
    n = a.shape[0]
    observed = _cosine_rows(a, b)

    perm_medians = np.empty(n_perm, dtype=float)
    pooled: list[np.ndarray] = []
    for k in range(n_perm):
        perm = rng.permutation(n)
        vals = _cosine_rows(a, b[perm])
        perm_medians[k] = float(np.median(vals))
        pooled.append(vals)
    pooled_all = np.concatenate(pooled)

    n_ge = int(np.sum(perm_medians >= np.median(observed)))
    p_value = float((n_ge + 1) / (n_perm + 1))  # +1 correction: empirical p cannot be 0
    null_q99 = float(np.quantile(pooled_all, 0.99))
    frac_above = float(np.mean(observed > null_q99))

    return {
        "observed_median": float(np.median(observed)),
        "observed_q25": float(np.quantile(observed, 0.25)),
        "observed_q75": float(np.quantile(observed, 0.75)),
        "null_median_of_perm_medians": float(np.median(perm_medians)),
        "null_q25_of_perm_medians": float(np.quantile(perm_medians, 0.25)),
        "null_q95_of_perm_medians": float(np.quantile(perm_medians, 0.95)),
        "pooled_null_q99": null_q99,
        "perm_p_value": p_value,
        "frac_spots_above_null_q99": frac_above,
        "n_perm": n_perm,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--n-perm", type=int, default=1000)
    ap.add_argument("--seed", type=int, default=20260806)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = ap.parse_args()

    rng = np.random.default_rng(args.seed)
    rows: list[dict] = []
    for a_id, b_id in PAIRS:
        b_strip = CELL2LOCATION_PREFIX if b_id == "cell2location" else None
        a, b, n_spots = _aligned_matrices(WEIGHTS[a_id], WEIGHTS[b_id], b_strip_prefix=b_strip)
        summary = run_pair(a, b, args.n_perm, rng)
        rows.append({"pair": f"{a_id}_vs_{b_id}", "n_spots": n_spots, "seed": args.seed, **summary})

    out = pd.DataFrame(rows)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out, sep="\t", index=False)
    print(f"wrote {args.out}")
    print(out.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
