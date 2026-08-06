#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class Spot:
    barcode: str
    x_fullres: float
    y_fullres: float


def _read_spots(path: Path, spot_id: str) -> tuple[pd.DataFrame, Spot]:
    df = pd.read_csv(path, sep="\t")
    if "barcode" not in df.columns:
        raise ValueError("visium_spots.tsv missing column: barcode")
    row = df[df["barcode"] == spot_id]
    if row.empty:
        raise ValueError(f"spot not found in {path}: {spot_id}")
    r0 = row.iloc[0]
    s = Spot(
        barcode=str(r0["barcode"]),
        x_fullres=float(r0["pxl_col_in_fullres"]),
        y_fullres=float(r0["pxl_row_in_fullres"]),
    )
    return df, s


def _nearest_neighbors(df: pd.DataFrame, center: Spot, k: int) -> list[tuple[str, float]]:
    xy = df.set_index("barcode")[["pxl_col_in_fullres", "pxl_row_in_fullres"]].astype(float)
    p = np.array([center.x_fullres, center.y_fullres], dtype=float)
    d = np.sqrt(((xy.values - p) ** 2).sum(axis=1))
    dd = pd.Series(d, index=xy.index).sort_values()
    # First item is the center itself (distance=0).
    neighbors = [(str(i), float(dd.loc[i])) for i in dd.index if str(i) != center.barcode]
    return neighbors[:k]

def _dist_point_to_segment(px: np.ndarray, a: np.ndarray, b: np.ndarray) -> np.ndarray:
    # px: (N,2); a,b: (2,)
    ab = b - a
    denom = float(np.dot(ab, ab))
    if denom <= 0:
        return np.sqrt(((px - a) ** 2).sum(axis=1))
    t = ((px - a) @ ab) / denom
    t = np.clip(t, 0.0, 1.0)
    proj = a + np.outer(t, ab)
    return np.sqrt(((px - proj) ** 2).sum(axis=1))


def _hull_distance_metrics(df: pd.DataFrame) -> pd.DataFrame:
    # Returns a dataframe keyed by barcode with two columns:
    # - dist_to_hull_fullres_px
    # - hull_dist_percentile (0 = boundary, 1 = deepest interior)
    from scipy.spatial import ConvexHull  # type: ignore

    xy = df[["barcode", "pxl_col_in_fullres", "pxl_row_in_fullres"]].copy()
    xy["pxl_col_in_fullres"] = pd.to_numeric(xy["pxl_col_in_fullres"], errors="coerce")
    xy["pxl_row_in_fullres"] = pd.to_numeric(xy["pxl_row_in_fullres"], errors="coerce")
    xy = xy.dropna()
    pts = xy[["pxl_col_in_fullres", "pxl_row_in_fullres"]].to_numpy(dtype=float)
    if len(pts) < 3:
        raise ValueError("Need >=3 spots to compute convex hull distance.")

    hull = ConvexHull(pts)
    hv = hull.vertices.tolist()
    # Close the polygon explicitly.
    hv2 = hv + [hv[0]]

    dmin = np.full(shape=(len(pts),), fill_value=np.inf, dtype=float)
    for i in range(len(hv2) - 1):
        a = pts[hv2[i]]
        b = pts[hv2[i + 1]]
        d = _dist_point_to_segment(pts, a, b)
        dmin = np.minimum(dmin, d)

    # Percentile rank: boundary ~ 0, interior ~ 1.
    order = np.argsort(dmin)
    ranks = np.empty_like(order, dtype=float)
    ranks[order] = np.linspace(0.0, 1.0, num=len(dmin), endpoint=True)

    out = pd.DataFrame(
        {
            "barcode": xy["barcode"].astype(str).to_list(),
            "dist_to_hull_fullres_px": dmin,
            "hull_dist_percentile": ranks,
        }
    )
    return out.set_index("barcode")


def _load_visium_counts(h5_path: Path):
    # Keep this dependency inside the function so local imports don't require scanpy.
    import scanpy as sc  # type: ignore

    ad = sc.read_10x_h5(str(h5_path))
    # Ensure gene symbols are accessible as var_names.
    try:
        ad.var_names_make_unique()
    except Exception:
        pass
    return ad


def _normalize_barcode_index(barcodes: list[str]) -> list[str]:
    # 10x Visium barcodes are often stored as "ACGT...-1".
    return [str(b) for b in barcodes]


def main() -> int:
    ap = argparse.ArgumentParser(description="Extract raw marker UMI counts for a target Visium spot and neighbors.")
    ap.add_argument(
        "--h5",
        default="data/smoke/visium_mouse_brain_real/V1_Mouse_Brain_Sagittal_Anterior_filtered_feature_bc_matrix.h5",
        help="10x filtered_feature_bc_matrix.h5 path.",
    )
    ap.add_argument(
        "--spots-tsv",
        default="results/figures/visium_spots.tsv",
        help="Spot metadata TSV containing fullres pixel coordinates.",
    )
    ap.add_argument("--spot-id", default="ACGTGACAAAGTAAGT-1", help="Target spot barcode.")
    ap.add_argument("--genes", default="Meis2,Serpinf1", help="Comma-separated marker genes to extract.")
    ap.add_argument("--k", type=int, default=6, help="Number of nearest-neighbor spots to report.")
    ap.add_argument("--dataset-id", default="Mouse_Brain_Visium_10x", help="Dataset id to record in output.")
    ap.add_argument(
        "--out-tsv",
        default="results/figures/visium_forensic_marker_counts.tsv",
        help="Output TSV (wide format: one row per spot).",
    )
    args = ap.parse_args()

    h5_path = Path(args.h5)
    spots_path = Path(args.spots_tsv)
    out_path = Path(args.out_tsv)
    genes = [g.strip() for g in args.genes.split(",") if g.strip()]
    if not genes:
        raise SystemExit("No genes provided.")

    spots_df, center = _read_spots(spots_path, args.spot_id)
    neighbors = _nearest_neighbors(spots_df, center, k=args.k)
    spot_list = [(center.barcode, 0.0, "target")] + [(b, dist, "neighbor") for b, dist in neighbors]

    hull_metrics = _hull_distance_metrics(spots_df)

    ad = _load_visium_counts(h5_path)
    obs_names = _normalize_barcode_index(list(ad.obs_names))
    ad.obs_names = obs_names

    # Map barcodes -> row index, preserving order of spot_list.
    missing = [b for b, _, _ in spot_list if b not in ad.obs_names]
    if missing:
        raise SystemExit(f"Missing barcodes in H5 matrix: {missing}")

    var_names = [str(v) for v in ad.var_names]
    missing_genes = [g for g in genes if g not in var_names]
    if missing_genes:
        # Be explicit: reviewers will care if a claimed marker isn't in the matrix.
        raise SystemExit(f"Missing genes in H5 matrix: {missing_genes}")

    # Slice to spots x genes. Keep raw UMI counts.
    # ad.X can be sparse; use toarray for tiny selection.
    Xi = ad[ [b for b, _, _ in spot_list], genes ].X
    try:
        Xi = Xi.toarray()
    except Exception:
        Xi = np.asarray(Xi)

    rows = []
    for i, (b, dist, kind) in enumerate(spot_list):
        d = {"dataset_id": args.dataset_id, "barcode": b, "kind": kind, "nn_distance_fullres_px": dist}
        if b in hull_metrics.index:
            d["dist_to_hull_fullres_px"] = float(hull_metrics.loc[b, "dist_to_hull_fullres_px"])
            d["hull_dist_percentile"] = float(hull_metrics.loc[b, "hull_dist_percentile"])
        for j, g in enumerate(genes):
            d[f"{g}_umi"] = int(Xi[i, j])
        rows.append(d)

    out = pd.DataFrame(rows)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, sep="\t", index=False)

    # Print a compact summary for quick inspection.
    print(f"Wrote: {out_path}")
    print(out.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
