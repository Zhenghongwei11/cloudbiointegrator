# METHOD_LIBRARY_VISIUM (v0)

This document defines the **deployable 10x Visium (Space Ranger) method library** used by the CloudBioIntegrator pipeline.

## Scope

- Input (reviewer-facing): **10x Space Ranger** output subset:
  - `filtered_feature_bc_matrix/`
  - `spatial/` (required: `tissue_positions_list.csv` or `tissue_positions.csv`, plus `scalefactors_json.json`)
- Output: benchmark tables under `results/benchmarks/` + Visium anchor tables under `results/figures/` + audit bundles under `docs/audit_runs/`
- Non-goals: PHI ingestion, clinical decision support

Notes:
- Some 10x public Visium examples ship counts as `*filtered_feature_bc_matrix.h5` instead of an extracted `filtered_feature_bc_matrix/` folder. The pipeline accepts either (as long as the matching `spatial/` is present).

## Method packs

### Pack: `baseline` (CPU-first)

Current v0 implementation (`scanpy-visium-baseline`):
- Reads matrix and spot coordinates from Space Ranger outputs
- Uses **in-tissue spots** (`in_tissue==1`) as the default analysis set
- QC summaries: spot counts, UMI/gene distributions, mitochondrial % (when feasible)
- Simple spatial clustering:
  - kNN graph on **array grid coordinates** (`array_row/array_col`)
  - Leiden clustering with fixed seed
- Exports a minimal spot-level anchor table:
  - `results/figures/visium_spots.tsv` (coords + QC + cluster)
- Appends benchmark rows to:
  - `results/benchmarks/method_benchmark.tsv` (task=`visium_qc+cluster`)
  - `results/benchmarks/runtime_cost_failure.tsv`

### Pack: `deconvolution` (baseline + advanced)

Implemented v0.1 (this milestone):
- Baseline (CPU/R): **RCTD** (`spacexr`) using a scRNA reference and CellTypist-derived cell labels
- Advanced (PyTorch): **Tangram** (`tangram-sc`) using the same scRNA reference + labels
- Optional (PyTorch, uncertainty-aware): **cell2location** (requires an image that includes `cell2location`; selected via `--runner cell2location` or `--runner all`)

Inputs (in addition to Space Ranger outputs):
- scRNA reference: 10x `filtered_feature_bc_matrix/`
- Reference labels:
  - default (demo/immune): CellTypist-derived labels (pinned model; see audit logs)
  - tissue-matched (recommended for non-immune tissues): provide `--reference-labels-tsv` with columns `{barcode, label}`

Outputs (anchors):
- `results/figures/visium_reference_cell_labels.tsv` (reference scRNA cell → label)
- `results/figures/visium_celltype_weights_rctd.tsv` (spot × cell_type weights; long format)
- `results/figures/visium_celltype_weights_tangram.tsv` (spot × cell_type weights; long format)
- `results/figures/visium_celltype_weights_cell2location.tsv` (spot × cell_type weights; long format; only when `--runner` includes cell2location)

Notes:
- Cross-method concordance is recorded as a **sanity check** in `results/benchmarks/biological_output_concordance.tsv` (not ground-truth accuracy).
- If a Visium public example provides counts only as `*filtered_feature_bc_matrix.h5`, the deconvolution pack will materialize a minimal `filtered_feature_bc_matrix/` folder from the H5 before running RCTD/Tangram.

## Dataset (v0 benchmark set)

Registered in `data/manifest.tsv`:
- `10x_Visium_Human_Lymph_Node_1p1_cf` (10x canonical Visium sample; `filtered_feature_bc_matrix` + `spatial/`)
- Reference scRNA for the v0 deconvolution demo: `10x_PBMC_3k_scRNA_2016_S3`

## How to run

Fetch and extract the dataset (downloads both matrix and spatial bundles):
- `python3 scripts/data/fetch_dataset.py --dataset-id 10x_Visium_Human_Lymph_Node_1p1_cf --extract`

Run Visium baseline (inside Docker is recommended):
- `make visium ARGS="--input-dir data/smoke/visium_human_lymph_node_real --dataset-id 10x_Visium_Human_Lymph_Node_1p1_cf --method-pack baseline --seed 0 --organism human --tissue lymph_node"`

Run Visium deconvolution/mapping (requires a scRNA reference):
- `make visium ARGS="--input-dir data/smoke/visium_human_lymph_node_real --dataset-id 10x_Visium_Human_Lymph_Node_1p1_cf --method-pack deconvolution --reference-scrna-dir data/smoke/pbmc3k_real/filtered_feature_bc_matrix --reference-dataset-id 10x_PBMC_3k_scRNA_2016_S3 --compute-tier cpu --seed 0 --organism human --tissue lymph_node"`

Run Visium deconvolution with cell2location (requires an image that includes cell2location deps):
- Build with deps enabled:
  - `docker build -t cloudbiointegrator:cell2location --build-arg INSTALL_CELL2LOCATION=1 .`
- Run with the explicit runner selector:
  - `make visium ARGS="--input-dir data/smoke/visium_human_lymph_node_real --dataset-id 10x_Visium_Human_Lymph_Node_1p1_cf --method-pack deconvolution --runner cell2location --reference-scrna-dir data/smoke/pbmc3k_real/filtered_feature_bc_matrix --reference-dataset-id 10x_PBMC_3k_scRNA_2016_S3 --compute-tier cpu --seed 0 --organism human --tissue lymph_node"`

Run Visium deconvolution/mapping with externally provided reference labels (recommended for non-immune tissues):
- `make visium ARGS="--input-dir <visium_outs> --dataset-id <visium_id> --method-pack deconvolution --reference-scrna-dir <ref_filtered_feature_bc_matrix> --reference-dataset-id <ref_id> --reference-labels-tsv <path_to_labels_tsv> --compute-tier cpu --seed 0 --organism <organism> --tissue <tissue>"`

Mouse brain primary (recommended reference + labels):
- Fetch the Seurat vignette reference RDS:
  - `python3 scripts/data/fetch_dataset.py --dataset-id Allen_Cortex_scRNA_SeuratV5_Reference_RDS`
- Prepare 10x-style reference + labels (uses `subclass`):
  - `Rscript scripts/data/prepare_allen_cortex_reference.R --rds data/references/allen_cortex/allen_cortex.rds --out-root data/references/allen_cortex/prepared --label-col subclass --gzip 1`
- Run deconvolution against the primary Visium anchor:
  - `make visium ARGS="--input-dir data/smoke/visium_mouse_brain_real --dataset-id Mouse_Brain_Visium_10x --method-pack deconvolution --reference-scrna-dir data/references/allen_cortex/prepared/filtered_feature_bc_matrix --reference-dataset-id Allen_Cortex_scRNA_SeuratV5_Reference_RDS --reference-labels-tsv data/references/allen_cortex/prepared/reference_labels.tsv --compute-tier cpu --seed 0 --organism mouse --tissue brain"`

Notes:
- The recommended “product runtime” path is Dockerized (`Dockerfile`), with audit bundles enabled by default.
- A Visium run produces `docs/audit_runs/<run_id>.zip` capturing logs, checksums, and all result tables.

## Failure modes (expected)

- Missing `spatial/` metadata: the runner fails with a clear “which file is missing” error.
- Matrix/spatial mismatch: if barcodes in the matrix are missing from `tissue_positions*`, the runner errors (prevents silent mis-joins).
- Large Visium datasets exceed RAM: record as a failure in `results/benchmarks/runtime_cost_failure.tsv` and move to a higher-memory VM (see `docs/COMPUTE_PLAN.md`).
