#!/usr/bin/env python3
"""Regenerate results/benchmarks/visium_concordance_stratified.tsv.

Spotwise cosine similarity between RCTD and Tangram composition vectors
(L1-normalized, shared cell types), stratified by the top-weight RCTD
cell-type label per spot. The cosine computation matches the definition
used in scripts/pipeline/run.py::_weights_concordance_cosine_by_spot_summary.

Strata:
  - layer_like_top1_by_RCTD: top RCTD label starts with "L" followed by a
    digit (L2_3 IT, L4, L5 IT, L5 PT, L6 CT, L6 IT, L6b). "Lamp5" is an
    inhibitory-neuron marker, not a layer label, and is excluded.
  - glia_top1_by_RCTD: top RCTD label in the glia-associated set
    (Astro, Oligo, Macrophage/Microglia, Endo, Peri, VLMC).
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]

DEFAULT_RCTD = ROOT / "results" / "figures" / "visium_celltype_weights_rctd.tsv"
DEFAULT_TANGRAM = ROOT / "results" / "figures" / "visium_celltype_weights_tangram.tsv"
DEFAULT_OUT = ROOT / "results" / "benchmarks" / "visium_concordance_stratified.tsv"

GLIA_LABELS = {"Astro", "Oligo", "Macrophage", "Endo", "Peri", "VLMC"}
_LAYER_RE = re.compile(r"^L[0-9]")


def _load_weights_long(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, sep="\t")
    return df


def _pivot_spot_by_celltype(df: pd.DataFrame) -> pd.DataFrame:
    return df.pivot_table(index="barcode", columns="cell_type", values="weight", aggfunc="mean").fillna(0.0)


def _cosine_matrix(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Per-row cosine similarity between two L1-normalized matrices."""
    num = np.sum(a * b, axis=1)
    den = np.sqrt(np.sum(a * a, axis=1)) * np.sqrt(np.sum(b * b, axis=1))
    den = np.where(den == 0, np.nan, den)
    cos = num / den
    return cos


def build_stratified(rctd_path: Path, tangram_path: Path) -> pd.DataFrame:
    rctd = _load_weights_long(rctd_path)
    tang = _load_weights_long(tangram_path)

    rctd_w = _pivot_spot_by_celltype(rctd)
    tang_w = _pivot_spot_by_celltype(tang)

    spots = rctd_w.index.intersection(tang_w.index)
    cts = rctd_w.columns.intersection(tang_w.columns)
    if len(spots) < 50 or len(cts) < 5:
        raise RuntimeError(f"insufficient overlap: {len(spots)} spots, {len(cts)} shared cell types")

    rw = rctd_w.loc[spots, cts].to_numpy(dtype=float)
    tw = tang_w.loc[spots, cts].to_numpy(dtype=float)
    rw = rw / np.clip(rw.sum(axis=1, keepdims=True), 1e-12, None)
    tw = tw / np.clip(tw.sum(axis=1, keepdims=True), 1e-12, None)

    cos = _cosine_matrix(rw, tw)
    finite = np.isfinite(cos)
    if finite.sum() < 200:
        raise RuntimeError(f"too few finite cosine values: {finite.sum()}")

    top1_rctd = np.asarray(cts)[np.argmax(rw[finite], axis=1)]
    layer_like = np.asarray([bool(_LAYER_RE.match(x)) for x in top1_rctd])
    glia = np.asarray([x in GLIA_LABELS for x in top1_rctd])
    cos_f = cos[finite]

    rows = []

    def add_stratum(stratum: str, mask: np.ndarray, definition: str) -> None:
        vals = cos_f[mask]
        rows.append(
            {
                "dataset_id": "Mouse_Brain_Visium_10x",
                "pair": "rctd_vs_tangram",
                "stratum": stratum,
                "n": int(vals.size),
                "median": float(np.median(vals)),
                "q1": float(np.quantile(vals, 0.25)),
                "q3": float(np.quantile(vals, 0.75)),
                "definition": definition,
            }
        )

    add_stratum(
        "layer_like_top1_by_RCTD",
        layer_like,
        "layer_like_top1_by_RCTD",
    )
    add_stratum(
        "non_layer_like_top1_by_RCTD",
        ~layer_like,
        "non_layer_like_top1_by_RCTD",
    )
    add_stratum(
        "glia_top1_by_RCTD",
        glia,
        "glia_top1_by_RCTD",
    )
    add_stratum(
        "non_glia_top1_by_RCTD",
        ~glia,
        "non_glia_top1_by_RCTD",
    )

    out = pd.DataFrame(rows)
    out = out[["dataset_id", "pair", "stratum", "n", "median", "q1", "q3", "definition"]]
    return out.sort_values("stratum").reset_index(drop=True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--rctd", type=Path, default=DEFAULT_RCTD)
    ap.add_argument("--tangram", type=Path, default=DEFAULT_TANGRAM)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = ap.parse_args()

    out = build_stratified(args.rctd, args.tangram)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out, sep="\t", index=False)
    print(f"wrote {args.out}")
    print(out.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
