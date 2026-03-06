from __future__ import annotations

import argparse
import warnings

import numpy as np
import pandas as pd
from scipy.spatial.distance import cosine

warnings.filterwarnings("ignore")

def load_pivot(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, sep='\t')
    pivot = df.pivot(index='barcode', columns='cell_type', values='weight').fillna(0)
    return pivot

def topk_dict(row: pd.Series, k: int = 3) -> dict[str, float]:
    s = row.sort_values(ascending=False).head(k)
    return {str(i): float(v) for i, v in s.items()}

def main() -> int:
    ap = argparse.ArgumentParser(description="Forensic audit of per-spot discordance (RCTD vs Tangram).")
    ap.add_argument("--rctd", default="results/figures/visium_celltype_weights_rctd.tsv")
    ap.add_argument("--tangram", default="results/figures/visium_celltype_weights_tangram.tsv")
    ap.add_argument("--top-k", type=int, default=5, help="Number of most-discrepant spots to report.")
    ap.add_argument("--out-tsv", default="", help="Optional output TSV path to write the table.")
    ap.add_argument("--dataset-id", default="Mouse_Brain_Visium_10x", help="Dataset id to record in the output table.")
    args = ap.parse_args()

    rctd = load_pivot(args.rctd)
    tangram = load_pivot(args.tangram)

    # Union of all cell types
    all_types = sorted(list(set(rctd.columns) | set(tangram.columns)))

    # Reindex to have same columns
    rctd = rctd.reindex(columns=all_types, fill_value=0.0)
    tangram = tangram.reindex(columns=all_types, fill_value=0.0)

    # Normalize
    rctd = rctd.div(rctd.sum(axis=1), axis=0).fillna(0)
    tangram = tangram.div(tangram.sum(axis=1), axis=0).fillna(0)

    common = rctd.index.intersection(tangram.index)
    r_sub = rctd.loc[common]
    t_sub = tangram.loc[common]

    cosines = []
    for b in common:
        v1 = r_sub.loc[b].values
        v2 = t_sub.loc[b].values
        if np.sum(v1) == 0 or np.sum(v2) == 0:
            cosines.append(0.0)
            continue
        c = 1.0 - cosine(v1, v2)
        cosines.append(c)

    df_audit = pd.DataFrame({"barcode": common, "cosine": cosines})
    worst = df_audit.sort_values("cosine").head(args.top_k).reset_index(drop=True)

    out_rows = []
    print(f"--- Forensic Audit: Top {args.top_k} Discrepant Spots ---")
    for _, row in worst.iterrows():
        b = row["barcode"]
        cosv = float(row["cosine"])
        r_top = r_sub.loc[b].sort_values(ascending=False).head(3)
        t_top = t_sub.loc[b].sort_values(ascending=False).head(3)
        print(f"\nSpot: {b} | Cosine: {cosv:.4f}")
        print(f"  RCTD Top: {r_top.to_dict()}")
        print(f"  Tangram Top: {t_top.to_dict()}")

        out_rows.append(
            {
                "dataset_id": args.dataset_id,
                "barcode": b,
                "cosine": cosv,
                "rctd_top1_cell_type": r_top.index[0],
                "rctd_top1_weight": float(r_top.iloc[0]),
                "rctd_top2_cell_type": r_top.index[1] if len(r_top) > 1 else "",
                "rctd_top2_weight": float(r_top.iloc[1]) if len(r_top) > 1 else np.nan,
                "rctd_top3_cell_type": r_top.index[2] if len(r_top) > 2 else "",
                "rctd_top3_weight": float(r_top.iloc[2]) if len(r_top) > 2 else np.nan,
                "tangram_top1_cell_type": t_top.index[0],
                "tangram_top1_weight": float(t_top.iloc[0]),
                "tangram_top2_cell_type": t_top.index[1] if len(t_top) > 1 else "",
                "tangram_top2_weight": float(t_top.iloc[1]) if len(t_top) > 1 else np.nan,
                "tangram_top3_cell_type": t_top.index[2] if len(t_top) > 2 else "",
                "tangram_top3_weight": float(t_top.iloc[2]) if len(t_top) > 2 else np.nan,
            }
        )

    if args.out_tsv:
        out = pd.DataFrame(out_rows)
        out.to_csv(args.out_tsv, sep="\t", index=False)
        print(f"\nWrote: {args.out_tsv}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
