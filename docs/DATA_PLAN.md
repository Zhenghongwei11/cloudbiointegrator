# DATA_PLAN (main evaluation starts from Cell Ranger count matrix)

## Primary principle (reviewer-facing)
Main evaluation will start from **10x Genomics Cell Ranger count matrix** (and optionally upstream FASTQ), so preprocessing/QC decisions are standardized, reproducible, and auditable.

## Scope decision (v1)
- Spatial transcriptomics in v1 is limited to **10x Visium** datasets processed via **Cell Ranger spatial** output.
- Non-Visium platforms (e.g., Slide-seq, Stereo-seq) are out of scope for the first paper to avoid confounding heterogeneity and to keep the benchmark defensible.

## Dataset roles

### Role: smoke-test (end-to-end correctness)
Goal: verify the pipeline contract (inputs → anchor tables → report) and audit outputs.

Candidates (from `docs/DATASET_LANDSCAPE.tsv`):
- `10x_PBMC_3k_scRNA_2016`
- `10x_PBMC_10k_scRNA`

### Role: large-scale + heterogeneity (cloud scaling + integration)
Goal: test scalability, batch correction/integration, and heterogeneity-aware reporting.

Candidates:
- `Tabula_Sapiens`
- `HCA_Immune_Census (Cumulus benchmark)` (linking to the Cumulus paper as an anchor; dataset access link to be made explicit)

### Role: spatial module (deconvolution / mapping)
Goal: demonstrate spatial deconvolution/mapping component under realistic Visium workloads.

Candidates:
- `Mouse_Brain_Visium_10x`
- `Human_Breast_Cancer_Visium_10x`

Reference (tissue-matched; primary choice for mouse brain):
- `Allen_Cortex_scRNA_SeuratV5_Reference_RDS` (Seurat RDS reference used in Seurat spatial mapping vignette; use `subclass` labels via `--reference-labels-tsv`).

v0 implementation note:
- For the first Visium baseline (ingestion + QC + coordinate-clustering + audit), we use a canonical 10x example dataset:
  - `10x_Visium_Human_Lymph_Node_1p1_cf` (registered in `data/manifest.tsv`)
  - This is a “baseline gates” dataset; deconvolution/mapping benchmarks remain planned for the mouse brain primary.

### Role: robustness / “real world”
Goal: stress test metadata variability, missing labels, and “non-ideal” datasets without hand-holding.

Candidates:
- `GEO_scRNA_disease_case_control`
- `ArrayExpress_scRNA`

## Primary dataset selection (initial)

We will use **two primary anchors** (one scRNA, one spatial) to avoid a “single dataset” critique:

1) scRNA primary: `10x_PBMC_10k_scRNA` (fast, canonical, Cell Ranger outputs)
2) spatial primary: `Mouse_Brain_Visium_10x` (canonical spatial; Cell Ranger outputs)

Replication will include `Tabula_Sapiens` plus at least one additional Visium dataset (e.g., breast cancer) and one “messy real-world” public repository dataset.

## Controls / negative controls

- Label permutation controls for any supervised metric (annotation accuracy).
- Donor/site holdout splits where available (avoid leakage across batches).
- Downsampling sensitivity (cells/spots and genes) to show robustness.

## Deliverables (tables as source of truth)

- `results/dataset_summary.tsv` (per dataset: modality, size, QC stats, cohorts)
- `results/audit/reproducibility_checks.tsv` (rerun consistency + hashes)
- `results/benchmarks/method_benchmark.tsv` (metrics × methods × datasets)
