#!/usr/bin/env python3
from __future__ import annotations

import math
import random
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
OUT_PATH = ROOT / "results" / "effect_sizes" / "claim_effects.tsv"


def wilson_ci(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if n == 0:
        return (math.nan, math.nan)
    phat = k / n
    denom = 1 + (z**2) / n
    center = (phat + (z**2) / (2 * n)) / denom
    half = (z / denom) * math.sqrt((phat * (1 - phat) / n) + (z**2) / (4 * n**2))
    return (max(0.0, center - half), min(1.0, center + half))


def bootstrap_mean(values: list[float], n_boot: int = 2000, seed: int = 0) -> tuple[float, float]:
    if len(values) == 0:
        return (math.nan, math.nan)
    if len(values) == 1:
        return (values[0], values[0])
    rnd = random.Random(seed)
    means = []
    for _ in range(n_boot):
        sample = [values[rnd.randrange(len(values))] for _ in range(len(values))]
        means.append(sum(sample) / len(sample))
    means.sort()
    low_idx = int(0.025 * n_boot)
    high_idx = int(0.975 * n_boot)
    return (means[low_idx], means[high_idx])


def main() -> int:
    rows = []

    # C1: reproducibility pass rate (audit checks)
    repro = pd.read_csv(ROOT / "results" / "audit" / "reproducibility_checks.tsv", sep="\t")
    repro = repro[repro["pass"].isin([0, 1])]
    k = int(repro["pass"].sum())
    n = int(repro.shape[0])
    rate = k / n if n else math.nan
    ci_low, ci_high = wilson_ci(k, n)
    rows.append(
        {
            "claim_id": "C1_repro_audit",
            "dataset_id": "multi_run_summary",
            "outcome": "repro_check_pass_rate",
            "model": "reproducibility_checks",
            "effect_type": "proportion",
            "effect": f"{rate:.6f}" if n else "",
            "ci_lower": f"{ci_low:.6f}" if n else "",
            "ci_upper": f"{ci_high:.6f}" if n else "",
            "pvalue": "",
            "fdr": "",
            "n": str(n),
            "notes": "Wilson 95% CI over all reproducibility_checks rows with pass in {0,1}.",
        }
    )

    # C2: biological concordance metrics (mean per dataset + metric)
    bio = pd.read_csv(ROOT / "results" / "benchmarks" / "biological_output_concordance.tsv", sep="\t")
    bio = bio.dropna(subset=["value"])

    # Some concordance rows are already reported as a summary statistic with an IQR in the source table
    # (e.g., spotwise cosine similarity for Visium deconvolution weight vectors). For these, we forward
    # the recorded summary rather than averaging across heterogeneous method-pairs.
    special = bio[bio["concordance_metric"] == "median_cosine_by_spot"].copy()
    if not special.empty:
        for _, r in special.iterrows():
            dataset_id = str(r["dataset_id"])
            value = float(r["value"])
            ci_low = r.get("ci_low")
            ci_high = r.get("ci_high")
            n_units = r.get("n_units")
            notes = str(r.get("notes") or "")
            pair = ""
            if "pair=" in notes:
                pair = notes.split("pair=", 1)[1].split(";", 1)[0].strip()
            outcome = "concordance_median_cosine_by_spot"
            if pair:
                outcome = f"{outcome}_{pair}"
            rows.append(
                {
                    "claim_id": "C2_expert_comparable",
                    "dataset_id": dataset_id,
                    "outcome": outcome,
                    "model": "biological_output_concordance",
                    "effect_type": "median_iqr",
                    "effect": f"{value:.6f}",
                    "ci_lower": f"{float(ci_low):.6f}" if pd.notna(ci_low) and str(ci_low) != "" else "",
                    "ci_upper": f"{float(ci_high):.6f}" if pd.notna(ci_high) and str(ci_high) != "" else "",
                    "pvalue": "",
                    "fdr": "",
                    "n": str(int(n_units)) if pd.notna(n_units) and str(n_units) != "" else "",
                    "notes": notes or "Forwarded median + IQR from biological_output_concordance.tsv.",
                }
            )

    regular = bio[bio["concordance_metric"] != "median_cosine_by_spot"].copy()

    # Avoid mixing distinct ARI semantics into a single pooled mean.
    # Example: baseline-vs-Harmony ARI and scVI-vs-Harmony ARI are different
    # questions and should be summarized separately.
    def _metric_key(row: pd.Series) -> str:
        metric = str(row.get("concordance_metric") or "")
        if metric != "ARI":
            return metric

        notes = str(row.get("notes") or "").lower()
        method_id = str(row.get("method_id") or "").lower()
        reference_type = str(row.get("reference_type") or "").lower()

        if (
            "baseline clusters vs harmony clusters" in notes
            or (method_id == "harmony" and reference_type in {"scanpy-standard", "seurat-v5-standard"})
        ):
            return "ARI_baseline_vs_harmony"
        if "scvi clusters vs harmony clusters" in notes or (method_id == "scvi" and reference_type == "harmony"):
            return "ARI_scvi_vs_harmony"
        return "ARI_other"

    regular["metric_key"] = regular.apply(_metric_key, axis=1)

    for (dataset_id, metric), sub in regular.groupby(["dataset_id", "metric_key"], dropna=False):
        values = [float(v) for v in sub["value"].tolist()]
        mean_val = sum(values) / len(values) if values else math.nan
        ci_low, ci_high = bootstrap_mean(values)
        rows.append(
            {
                "claim_id": "C2_expert_comparable",
                "dataset_id": dataset_id,
                "outcome": f"concordance_{metric}",
                "model": "biological_output_concordance",
                "effect_type": "mean",
                "effect": f"{mean_val:.6f}" if values else "",
                "ci_lower": f"{ci_low:.6f}" if values else "",
                "ci_upper": f"{ci_high:.6f}" if values else "",
                "pvalue": "",
                "fdr": "",
                "n": str(len(values)),
                "notes": "Bootstrap 95% CI (row-level resampling); low n for some metrics.",
            }
        )

    # C3: ops success rate (runtime_cost_failure) + v0/v1 split
    ops = pd.read_csv(ROOT / "results" / "benchmarks" / "runtime_cost_failure.tsv", sep="\t")
    ops = ops[ops["status"].isin(["ok", "fail"])]

    def _append_ops_row(df: pd.DataFrame, label: str, note_suffix: str) -> None:
        k = int((df["status"] == "ok").sum())
        n = int(df.shape[0])
        rate = k / n if n else math.nan
        ci_low, ci_high = wilson_ci(k, n)
        rows.append(
            {
                "claim_id": "C3_ops_improvement",
                "dataset_id": label,
                "outcome": "run_success_rate",
                "model": "runtime_cost_failure",
                "effect_type": "proportion",
                "effect": f"{rate:.6f}" if n else "",
                "ci_lower": f"{ci_low:.6f}" if n else "",
                "ci_upper": f"{ci_high:.6f}" if n else "",
                "pvalue": "",
                "fdr": "",
                "n": str(n),
                "notes": f"Wilson 95% CI over runtime_cost_failure rows with status in {{ok,fail}}; {note_suffix}.",
            }
        )

    _append_ops_row(ops, "multi_run_summary", "all runs")
    v1_mask = ops["notes"].fillna("").str.contains("phase=v1")
    _append_ops_row(ops[~v1_mask], "multi_run_summary_v0", "v0 baseline (phase!=v1)")
    _append_ops_row(ops[v1_mask], "multi_run_summary_v1", "v1 post-fix (phase=v1)")

    out_df = pd.DataFrame(rows)[
        [
            "claim_id",
            "dataset_id",
            "outcome",
            "model",
            "effect_type",
            "effect",
            "ci_lower",
            "ci_upper",
            "pvalue",
            "fdr",
            "n",
            "notes",
        ]
    ]
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(OUT_PATH, sep="\t", index=False)
    print(f"OK: wrote {OUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
