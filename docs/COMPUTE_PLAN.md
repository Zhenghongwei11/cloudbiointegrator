# COMPUTE_PLAN (cloud-first; v1 paper scope)

## Scope decision (v1)
- Inputs: **10x Cell Ranger count matrix** (and optionally upstream FASTQ).
- Spatial: **10x Visium** with **Cell Ranger spatial** outputs only.
- Goal: produce benchmark tables + audit artifacts; not clinical decision support.

## Workload breakdown (high level)

### 1) Ingestion + QC (light–medium)
Inputs:
- `filtered_feature_bc_matrix/` (matrix.mtx.gz, features.tsv.gz, barcodes.tsv.gz) or HDF5 equivalent
- spatial: `filtered_feature_bc_matrix/` + `spatial/` image metadata (Visium)

Compute characteristics:
- CPU-bound, moderate RAM
- I/O dominates for large matrices

### 2) scRNA core analysis (medium–heavy)
Components (v1 benchmark set):
- baseline pipelines (Seurat/Scanpy)
- integration (Seurat anchors, Harmony, scVI)

Compute characteristics:
- Harmony/Seurat: CPU + RAM
- scVI: benefits from GPU; feasible on CPU for small datasets but slow for larger atlases

### 3) Spatial deconvolution/mapping (medium–heavy)
Components:
- SPOTlight, RCTD (CPU/R)
- cell2location (often GPU beneficial)
- Tangram (GPU beneficial for larger problems)
- CARD (CPU/R)

Compute-tier policy (v0):
- Default `--compute-tier cpu` runs the full baseline+advanced pack on CPU for small/medium demos.
- `--compute-tier gpu` is recommended for larger atlases (Tangram/cell2location); the pipeline will prefer CUDA when available.
- For `scvi` (advanced scRNA runner), CPU is **guardrailed** (blocked above a fixed cell cap); use GPU for real benchmarks.

### 4) Benchmarking + repeated runs (heavy by repetition)
The heaviest cost driver is **reruns** across:
- datasets × methods × seeds × stress tests

## Target environments

### A) Local dev (smoke tests only)
Purpose: fast iteration on report templates and provenance; not for large-scale benchmarks.

Suggested:
- 8–16 CPU cores
- 32–64 GB RAM
- optional GPU

### B) Cloud “standard” runner (v1 baseline)
Purpose: run 10x PBMC + Visium mouse brain benchmarks end-to-end.

Suggested:
- 16–32 vCPU
- 128 GB RAM
- optional 1× GPU (midrange) for scVI/cell2location/Tangram
- storage: 500 GB fast SSD (workspace + caches)

Practical note (v0 implementation):
- scVI runner is packaged in a separate GPU-capable container (`Dockerfile.scvi`) and should be run with Docker GPU support (`--gpus all`) on a GPU VM.

### C) Cloud “scale” runner (Tabula Sapiens / large atlas replication)
Purpose: large-scale integration benchmark and robustness trials.

Suggested:
- 32–64 vCPU
- 256–512 GB RAM (depending on chosen representation)
- 1–2× GPU (optional but recommended for scVI/cell2location)
- storage: 1–2 TB SSD

## Cost control strategy
- Cache intermediate representations (normalized matrices, embeddings) with content hashes.
- Separate “selection” runs (quick) from “final benchmark” runs (frozen).
- Enforce per-run budgets: max wall time, max RAM, max GPU-hours.
- Record estimated cost per dataset in `results/benchmarks/runtime_cost_failure.tsv`.

## Storage & data governance
- Store only public benchmark data for paper runs.
- For any private data (future), require explicit opt-in and separate tenant/project.
- Never log raw private sequences; logs only contain dataset IDs and checksums.

## Downsampled smoke test (required before heavy runs)
Purpose: validate pipeline correctness, provenance, and report generation.

Plan:
- Default smoke path uses a **bundled toy 10x-like matrix** under `data/smoke/` for contract validation on any machine.
- For paper-facing smoke runs, use `10x_PBMC_3k_scRNA_2016` and a canonical Visium Space Ranger output (v0 baseline gates: `10x_Visium_Human_Lymph_Node_1p1_cf`; later primary: mouse brain).
- Downsample cells/spots to ~10–20% and genes to HVGs-only.
- Run 1 seed, 1 method per family.

Expected hardware/time (smoke, local dev):
- CPU: any modern laptop/desktop CPU
- RAM: < 4 GB for “ingest-only” smoke; < 16–32 GB for light scRNA baseline steps
- Wall time: ~1–5 minutes for ingest-only; ~10–60 minutes if running baseline scRNA + one spatial method (dataset-dependent)

Expected artifacts:
- `results/audit/reproducibility_checks.tsv` (single-run entry)
- `results/figures/` anchor tables for plots
- `results/benchmarks/*` (minimal stub tables with schema-correct columns)

## Failure handling / reliability
- All runs must be restartable from checkpoints.
- Record exit codes, exceptions, and “partial completion” status per stage.
- Stress-test missing metadata and malformed input early (fail fast with actionable errors).
