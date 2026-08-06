#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
from statistics import median

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "results" / "analysis"
POSITIONING_PATH = OUT_DIR / "tool_positioning_matrix.tsv"
EMPIRICAL_PATH = OUT_DIR / "external_comparison_summary.tsv"


def _median_metric(
    frame: pd.DataFrame,
    dataset_id: str,
    task: str,
    method_id: str,
    metric_id: str,
    n_units: int | None = None,
) -> float:
    sub = frame[
        (frame["dataset_id"] == dataset_id)
        & (frame["task"] == task)
        & (frame["method_id"] == method_id)
        & (frame["metric_id"] == metric_id)
    ].copy()
    if n_units is not None:
        sub = sub[sub["n_units"] == n_units]
    values = sub["metric_value"].astype(float).tolist()
    if not values:
        raise ValueError(f"Missing values for {dataset_id} {task} {method_id} {metric_id}")
    return float(median(values))


def _ops_summary(frame: pd.DataFrame, dataset_ids: list[str], method_ids: list[str]) -> tuple[int, int, float]:
    sub = frame[
        frame["dataset_id"].isin(dataset_ids)
        & frame["method_id"].isin(method_ids)
        & frame["status"].isin(["ok", "fail"])
    ].copy()
    ok = sub[sub["status"] == "ok"]
    return int(ok.shape[0]), int(sub.shape[0]), float(ok["wall_time_s"].astype(float).median())


def build_positioning_table() -> pd.DataFrame:
    rows = [
        {
            "adjacent_system": "Galaxy",
            "layer": "analysis platform",
            "supports_scRNA": "yes",
            "supports_Visium": "tool-dependent",
            "standardized_preprocessing": "tool-dependent",
            "standardized_output_tables": "partial",
            "cross_method_benchmarking": "no",
            "runtime_failure_logging": "partial",
            "robustness_audit": "no",
            "artifact_checksum_manifest": "no",
            "reviewer_ready_bundle": "no",
            "evidence_refs": "[19-21]",
            "notes": "Broad biomedical platform with reproducible execution, but single-cell and spatial outputs depend on selected tools and histories rather than a fixed audit schema.",
        },
        {
            "adjacent_system": "Snakemake",
            "layer": "workflow engine",
            "supports_scRNA": "user-defined",
            "supports_Visium": "user-defined",
            "standardized_preprocessing": "user-defined",
            "standardized_output_tables": "user-defined",
            "cross_method_benchmarking": "no",
            "runtime_failure_logging": "partial",
            "robustness_audit": "no",
            "artifact_checksum_manifest": "user-defined",
            "reviewer_ready_bundle": "user-defined",
            "evidence_refs": "[14]",
            "notes": "Execution substrate rather than a domain-specific single-cell benchmark framework; audit outputs must be authored by the workflow developer.",
        },
        {
            "adjacent_system": "Scanpy/Seurat analyst-built workflows",
            "layer": "toolkit workflow",
            "supports_scRNA": "yes",
            "supports_Visium": "partial",
            "standardized_preprocessing": "user-defined",
            "standardized_output_tables": "user-defined",
            "cross_method_benchmarking": "no",
            "runtime_failure_logging": "no",
            "robustness_audit": "no",
            "artifact_checksum_manifest": "no",
            "reviewer_ready_bundle": "no",
            "evidence_refs": "[7,8]",
            "notes": "Widely used analysis toolkits, but cross-method comparison and provenance packaging remain analyst-assembled rather than native outputs.",
        },
        {
            "adjacent_system": "scIB",
            "layer": "benchmark resource",
            "supports_scRNA": "yes",
            "supports_Visium": "no",
            "standardized_preprocessing": "partial",
            "standardized_output_tables": "yes",
            "cross_method_benchmarking": "yes",
            "runtime_failure_logging": "no",
            "robustness_audit": "partial",
            "artifact_checksum_manifest": "no",
            "reviewer_ready_bundle": "no",
            "evidence_refs": "[32]",
            "notes": "Strong scRNA integration benchmarking resource, but not a dual-modality workflow with manuscript-facing audit bundles.",
        },
        {
            "adjacent_system": "CloudBioIntegrator",
            "layer": "auditable workflow framework",
            "supports_scRNA": "yes",
            "supports_Visium": "yes",
            "standardized_preprocessing": "yes",
            "standardized_output_tables": "yes",
            "cross_method_benchmarking": "yes",
            "runtime_failure_logging": "yes",
            "robustness_audit": "yes",
            "artifact_checksum_manifest": "yes",
            "reviewer_ready_bundle": "yes",
            "evidence_refs": "this study",
            "notes": "Targets the combination of standardized dual-modality workflows, benchmark outputs, failure-aware auditing, and reviewer-verifiable artifact packaging.",
        },
    ]
    return pd.DataFrame(rows)


def build_empirical_table() -> pd.DataFrame:
    method_benchmark = pd.read_csv(ROOT / "results" / "benchmarks" / "method_benchmark.tsv", sep="\t")
    runtime = pd.read_csv(ROOT / "results" / "benchmarks" / "runtime_cost_failure.tsv", sep="\t")
    claim_effects = pd.read_csv(ROOT / "results" / "effect_sizes" / "claim_effects.tsv", sep="\t")

    rows = []

    pbmc3k_scanpy_cells = _median_metric(
        method_benchmark,
        "10x_PBMC_3k_scRNA_2016_S3",
        "qc+cluster",
        "scanpy-standard",
        "n_cells_after_qc",
    )
    pbmc3k_seurat_cells = _median_metric(
        method_benchmark,
        "10x_PBMC_3k_scRNA_2016_S3",
        "qc+cluster",
        "seurat-v5-standard",
        "n_cells_after_qc",
    )
    pbmc3k_scanpy_counts = _median_metric(
        method_benchmark,
        "10x_PBMC_3k_scRNA_2016_S3",
        "qc+cluster",
        "scanpy-standard",
        "median_total_counts",
    )
    pbmc3k_seurat_counts = _median_metric(
        method_benchmark,
        "10x_PBMC_3k_scRNA_2016_S3",
        "qc+cluster",
        "seurat-v5-standard",
        "median_total_counts",
    )
    pbmc3k_scanpy_genes = _median_metric(
        method_benchmark,
        "10x_PBMC_3k_scRNA_2016_S3",
        "qc+cluster",
        "scanpy-standard",
        "median_n_genes_by_counts",
    )
    pbmc3k_seurat_genes = _median_metric(
        method_benchmark,
        "10x_PBMC_3k_scRNA_2016_S3",
        "qc+cluster",
        "seurat-v5-standard",
        "median_n_genes_by_counts",
    )
    pbmc3k_scanpy_clusters = _median_metric(
        method_benchmark,
        "10x_PBMC_3k_scRNA_2016_S3",
        "qc+cluster",
        "scanpy-standard",
        "n_clusters",
    )
    pbmc3k_seurat_clusters = _median_metric(
        method_benchmark,
        "10x_PBMC_3k_scRNA_2016_S3",
        "qc+cluster",
        "seurat-v5-standard",
        "n_clusters",
    )
    rows.append(
        {
            "comparison_block": "nominal_scRNA_baselines",
            "dataset_id": "10x_PBMC_3k_scRNA_2016_S3",
            "methods_compared": "Scanpy standard vs Seurat v5 standard",
            "metric_summary": (
                f"Median nominal outputs were nearly identical under the same declared inputs: cells after QC "
                f"{pbmc3k_scanpy_cells:.0f} vs {pbmc3k_seurat_cells:.0f}, median counts {pbmc3k_scanpy_counts:.1f} vs "
                f"{pbmc3k_seurat_counts:.1f}, median genes {pbmc3k_scanpy_genes:.0f} vs {pbmc3k_seurat_genes:.0f}, "
                f"and median cluster count {pbmc3k_scanpy_clusters:.0f} vs {pbmc3k_seurat_clusters:.0f}."
            ),
            "runtime_summary": "This block targets nominal output comparability rather than pipeline wall-time.",
            "interpretation": "Standardized wrappers recover expected baseline behavior without claiming algorithmic superiority.",
            "supporting_files": "results/benchmarks/method_benchmark.tsv",
        }
    )

    baseline_mixing = _median_metric(
        method_benchmark,
        "10x_PBMC3K_PBMC10K_V3_INTEGRATION_PAIR_S3",
        "integration",
        "scanpy-standard",
        "batch_mixing_nn_frac_mean",
    )
    harmony_mixing = _median_metric(
        method_benchmark,
        "10x_PBMC3K_PBMC10K_V3_INTEGRATION_PAIR_S3",
        "integration",
        "harmony",
        "batch_mixing_nn_frac_mean",
    )
    scvi_mixing = _median_metric(
        method_benchmark,
        "10x_PBMC3K_PBMC10K_V3_INTEGRATION_PAIR_S3",
        "integration",
        "scvi",
        "batch_mixing_nn_frac_mean",
    )
    harmony_row = claim_effects[
        (claim_effects["dataset_id"] == "10x_PBMC3K_PBMC10K_V3_INTEGRATION_PAIR_S3")
        & (claim_effects["outcome"] == "concordance_ARI_baseline_vs_harmony")
    ].iloc[0]
    scvi_row = claim_effects[
        (claim_effects["dataset_id"] == "10x_PBMC3K_PBMC10K_V3_INTEGRATION_PAIR_S3")
        & (claim_effects["outcome"] == "concordance_ARI_scvi_vs_harmony")
    ].iloc[0]
    harmony_ok, harmony_all, harmony_runtime = _ops_summary(
        runtime,
        ["10x_PBMC3K_PBMC10K_V3_INTEGRATION_PAIR_S3", "PBMC3K_PLUS_PBMC10K_INTEGRATION"],
        ["harmony"],
    )
    scvi_ok, scvi_all, scvi_runtime = _ops_summary(
        runtime,
        ["10x_PBMC3K_PBMC10K_V3_INTEGRATION_PAIR_S3", "PBMC3K_PLUS_PBMC10K_INTEGRATION"],
        ["scvi", "advanced:scvi"],
    )
    rows.append(
        {
            "comparison_block": "integration_tradeoffs",
            "dataset_id": "10x_PBMC3K_PBMC10K_V3_INTEGRATION_PAIR_S3",
            "methods_compared": "Baseline scanpy-standard vs Harmony vs scVI",
            "metric_summary": (
                f"Batch-mixing medians were {baseline_mixing:.3f} for the baseline embedding, {harmony_mixing:.3f} for Harmony, "
                f"and {scvi_mixing:.3f} for scVI. Harmony remained close to the baseline clustering structure "
                f"(ARI {float(harmony_row['effect']):.3f}, 95% CI {float(harmony_row['ci_lower']):.3f}-{float(harmony_row['ci_upper']):.3f}), "
                f"while scVI remained close to Harmony (ARI {float(scvi_row['effect']):.3f}, 95% CI {float(scvi_row['ci_lower']):.3f}-{float(scvi_row['ci_upper']):.3f})."
            ),
            "runtime_summary": (
                f"Successful-run median wall time was {harmony_runtime:.1f} s for Harmony and {scvi_runtime:.1f} s for scVI; "
                f"success rates were {harmony_ok}/{harmony_all} and {scvi_ok}/{scvi_all}, respectively."
            ),
            "interpretation": "The framework captures method-level trade-offs between mixing, structural agreement, and operational cost under standardized inputs.",
            "supporting_files": "results/benchmarks/method_benchmark.tsv; results/benchmarks/runtime_cost_failure.tsv; results/effect_sizes/claim_effects.tsv",
        }
    )

    visium_cos_rt = claim_effects[
        (claim_effects["dataset_id"] == "Mouse_Brain_Visium_10x")
        & (claim_effects["outcome"] == "concordance_median_cosine_by_spot_rctd_vs_tangram")
    ].iloc[0]
    visium_cos_tc = claim_effects[
        (claim_effects["dataset_id"] == "Mouse_Brain_Visium_10x")
        & (claim_effects["outcome"] == "concordance_median_cosine_by_spot_tangram_vs_cell2location")
    ].iloc[0]
    visium_cos_rc = claim_effects[
        (claim_effects["dataset_id"] == "Mouse_Brain_Visium_10x")
        & (claim_effects["outcome"] == "concordance_median_cosine_by_spot_rctd_vs_cell2location")
    ].iloc[0]
    rctd_entropy = _median_metric(
        method_benchmark,
        "Mouse_Brain_Visium_10x",
        "visium_deconvolution",
        "rctd",
        "mean_entropy",
        n_units=2695,
    )
    tangram_entropy = _median_metric(
        method_benchmark,
        "Mouse_Brain_Visium_10x",
        "visium_deconvolution",
        "tangram",
        "mean_entropy",
        n_units=2695,
    )
    cell2_entropy = _median_metric(
        method_benchmark,
        "Mouse_Brain_Visium_10x",
        "visium_deconvolution",
        "cell2location",
        "mean_entropy",
        n_units=2695,
    )
    rctd_ok, rctd_all, rctd_runtime = _ops_summary(runtime, ["Mouse_Brain_Visium_10x"], ["deconvolution:rctd"])
    tangram_ok, tangram_all, tangram_runtime = _ops_summary(runtime, ["Mouse_Brain_Visium_10x"], ["deconvolution:tangram"])
    cell2_ok, cell2_all, cell2_runtime = _ops_summary(runtime, ["Mouse_Brain_Visium_10x"], ["deconvolution:cell2location"])
    rows.append(
        {
            "comparison_block": "visium_deconvolution_tradeoffs",
            "dataset_id": "Mouse_Brain_Visium_10x",
            "methods_compared": "RCTD vs Tangram vs cell2location",
            "metric_summary": (
                f"Pairwise spotwise cosine medians were {float(visium_cos_rt['effect']):.3f} (IQR {float(visium_cos_rt['ci_lower']):.3f}-{float(visium_cos_rt['ci_upper']):.3f}) "
                f"for RCTD/Tangram, {float(visium_cos_tc['effect']):.3f} ({float(visium_cos_tc['ci_lower']):.3f}-{float(visium_cos_tc['ci_upper']):.3f}) "
                f"for Tangram/cell2location, and {float(visium_cos_rc['effect']):.3f} ({float(visium_cos_rc['ci_lower']):.3f}-{float(visium_cos_rc['ci_upper']):.3f}) "
                f"for RCTD/cell2location. Representative mean-entropy medians were {rctd_entropy:.3f}, {tangram_entropy:.3f}, and {cell2_entropy:.3f}, respectively."
            ),
            "runtime_summary": (
                f"Successful-run median wall time was {rctd_runtime:.1f} s for RCTD, {tangram_runtime:.1f} s for Tangram, and {cell2_runtime:.1f} s for cell2location; "
                f"success rates were {rctd_ok}/{rctd_all}, {tangram_ok}/{tangram_all}, and {cell2_ok}/{cell2_all}, respectively."
            ),
            "interpretation": "Different spatial mapping models lead to materially different composition vectors and operational footprints, which motivates explicit audit outputs rather than a single-method claim.",
            "supporting_files": "results/benchmarks/method_benchmark.tsv; results/benchmarks/runtime_cost_failure.tsv; results/effect_sizes/claim_effects.tsv",
        }
    )

    return pd.DataFrame(rows)


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    build_positioning_table().to_csv(POSITIONING_PATH, sep="\t", index=False)
    build_empirical_table().to_csv(EMPIRICAL_PATH, sep="\t", index=False)
    print(f"OK: wrote {POSITIONING_PATH}")
    print(f"OK: wrote {EMPIRICAL_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
