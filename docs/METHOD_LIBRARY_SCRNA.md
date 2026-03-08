# METHOD_LIBRARY_SCRNA (v0)

This document defines the **deployable scRNA method library** used by the CloudBioIntegrator pipeline.

## Scope

- Input (reviewer-facing): **10x Cell Ranger** `filtered_feature_bc_matrix/`
- Output: benchmark tables under `results/benchmarks/` + audit bundles under `docs/audit_runs/`
- Non-goals: PHI ingestion, clinical decision support

## Method packs

Method packs define what the agent is allowed to run (predeclared; no runtime invention).

### Pack: `baseline` (CPU-first)

Current v0 implementation:
- `scanpy-standard`: normalization + HVGs + PCA + neighbors + Leiden + UMAP
- `seurat-v5-standard`: LogNormalize + HVGs + PCA + neighbors + Leiden + UMAP (R/Seurat v5)
- Optional annotation: `celltypist` (PBMC-style default model; see audit logs)

Notes:
- The Seurat baseline runner currently supports `--annotate none` (use `--runner scanpy` for CellTypist).

### Pack: `advanced` (resource-aware; benchmarked vs baseline)

Current v0 implementation:
- Multi-input batch integration compare:
  - baseline: `scanpy-standard`
  - advanced: `harmonypy` (Harmony on PCA; see `docs/ALGORITHM_LANDSCAPE.md`)
- Optional advanced runner: `scvi` (compute-tier gated; requires torch + scvi-tools)
- Writes side-by-side benchmark rows plus lightweight concordance/robustness rows:
  - cluster concordance (ARI)
  - robustness: scVI HVG/2 cluster stability (ARI)
  - optional label-based proxy: neighbor label purity using pinned CellTypist labels

Planned (subsequent milestone):
- semi-supervised integration: `scanvi`
- reference mapping: `scarches`
- optional denoising: `cellbender` (compute-tier gated)

## Environments and reproducibility

- Primary execution path is containerized (`Dockerfile`).
- Every run writes an audit bundle (`docs/audit_runs/<run_id>.zip`) capturing:
  - git commit
  - environment fingerprint
  - logs and checksums
  - benchmark tables (source of truth for figures)

## Datasets (v0 benchmark set)

Registered in `data/manifest.tsv`:
- `10x_PBMC_3k_scRNA_2016_S3` (small; Cell Ranger v1 layout; requires conversion)
- `10x_PBMC_10k_v3_scRNA_2018_S3` (medium; v3 layout; direct)
- `10x_PBMC3K_PBMC10K_V3_INTEGRATION_PAIR_S3` (multi-input pair for integration compares)

## How to run (local or cloud)

Fetch dataset tarballs (and optionally extract):
- `python3 scripts/data/fetch_dataset.py --dataset-id 10x_PBMC_10k_v3_scRNA_2018_S3 --extract`

PBMC3k (older Cell Ranger v1 layout) is standardized automatically on extract:
- `python3 scripts/data/fetch_dataset.py --dataset-id 10x_PBMC_3k_scRNA_2016_S3 --extract`

Run a scRNA pack:
- `make scrna ARGS="--input-dir data/smoke/pbmc10k_v3_real/filtered_feature_bc_matrix --dataset-id 10x_PBMC_10k_v3_scRNA_2018_S3 --method-pack baseline --annotate celltypist"`

Run the Seurat baseline runner:
- `make scrna ARGS="--input-dir data/smoke/pbmc3k_real/filtered_feature_bc_matrix --dataset-id 10x_PBMC_3k_scRNA_2016_S3 --method-pack baseline --runner seurat --annotate none --seed 0"`

Run an integration compare (advanced pack; multi-input):
- Recommended for `make`: comma-separated inputs become batches
  - `make scrna ARGS="--input-dir data/smoke/pbmc3k_real/filtered_feature_bc_matrix,data/smoke/pbmc10k_v3_real/filtered_feature_bc_matrix --dataset-id 10x_PBMC3K_PBMC10K_V3_INTEGRATION_PAIR_S3 --method-pack advanced --annotate celltypist --seed 0"`
- Also supported (when running `python3 ...` directly): semicolon-separated inputs become batches

Run scVI integration (advanced pack; requires scVI image + compute-tier):
- Build/run on GPU (recommended):
  - `IMAGE_TAG=cloudbiointegrator:scvi DOCKERFILE=Dockerfile.scvi DOCKER_TARGET=scvi-gpu DOCKER_GPU=1 SCRNA_ARGS="--input-dir data/smoke/pbmc3k_real/filtered_feature_bc_matrix,data/smoke/pbmc10k_v3_real/filtered_feature_bc_matrix --dataset-id 10x_PBMC3K_PBMC10K_V3_INTEGRATION_PAIR_S3 --method-pack advanced --runner scvi --compute-tier gpu --annotate celltypist --scvi-max-epochs 50 --scvi-n-latent 30 --seed 0" bash scripts/cloud/run_on_vm.sh`
- CPU (guardrailed; small only):
  - `make scrna ARGS="--input-dir data/smoke/pbmc3k_real/filtered_feature_bc_matrix,data/smoke/pbmc10k_v3_real/filtered_feature_bc_matrix --dataset-id 10x_PBMC3K_PBMC10K_V3_INTEGRATION_PAIR_S3 --method-pack advanced --runner scvi --compute-tier cpu --scvi-max-cells 5000 --seed 0"`

Download only the needed CellTypist model (recommended; avoids downloading the full model zoo):
- `python3 scripts/data/fetch_celltypist_model.py --model Immune_All_Low.pkl`

## Runner interface contract (v0)

Inputs (required):
- `filtered_feature_bc_matrix/` directory (10x Cell Ranger output; gz OK)
- CLI parameters:
  - `--dataset-id` (stable identifier written into results tables)
  - `--method-pack` (`baseline` or `advanced`)
  - `--runner` (`scanpy` or `seurat`) for the baseline pack; `scvi` for advanced multi-input
  - `--input-dir` accepts either:
    - a single directory, or
    - multiple directories separated by `,` (recommended for `make`) or `;` (treated as separate batches for integration compare)
  - `--seed` (determinism control)
  - `--compute-tier` (`cpu` or `gpu`) for advanced methods requiring special resources

Outputs (required contract tables; appended per run):
- `results/benchmarks/method_benchmark.tsv`
- `results/benchmarks/runtime_cost_failure.tsv`
- `results/benchmarks/biological_output_concordance.tsv` (when applicable)
- `results/benchmarks/robustness_matrix.tsv` (when applicable)
- `results/audit/reproducibility_checks.tsv`
- `docs/audit_runs/<run_id>.zip` (audit bundle)

## Failure modes (expected)

- Missing Python deps (Scanpy/CellTypist): run via Docker image (recommended).
- CellTypist model download blocked/offline: pipeline records an `annotation_error` metric row and continues with clustering/QC metrics.
- Large datasets exceed RAM: treat as expected and record `runtime_cost_failure.tsv` rows; move to higher-memory VM per `docs/COMPUTE_PLAN.md`.
