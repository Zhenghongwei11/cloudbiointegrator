# REAL_SMOKE_DATASETS (public, small, reviewer-friendly)

This project’s smoke runs are **contract validation + reproducibility evidence**. For reviewer-facing reproducibility, at least one smoke should use a real public dataset.

## scRNA: 10x PBMC 3k (cellranger v1 layout; stable S3)
- URL: `https://s3-us-west-2.amazonaws.com/10x.files/samples/cell/pbmc3k/pbmc3k_filtered_gene_bc_matrices.tar.gz`
- Extract path: `filtered_gene_bc_matrices/hg19/`
- Convert to our v1 entrypoint by copying into:
  - `filtered_feature_bc_matrix/matrix.mtx`
  - `filtered_feature_bc_matrix/barcodes.tsv`
  - `filtered_feature_bc_matrix/features.tsv` (copy from `genes.tsv`)

## scRNA: 10x PBMC 10k v3 (cellranger v3 feature-bc; stable S3)
- URL: `https://s3-us-west-2.amazonaws.com/10x.files/samples/cell-exp/3.0.0/pbmc_10k_v3/pbmc_10k_v3_filtered_feature_bc_matrix.tar.gz`
- Extract path: `filtered_feature_bc_matrix/` (gzipped files)
- Use directly as our v1 entrypoint: `filtered_feature_bc_matrix/{matrix.mtx.gz,barcodes.tsv.gz,features.tsv.gz}`

## scRNA integration (multi-input): PBMC3k + PBMC10k v3 (system-focused pair)
This is a small, reviewer-friendly **multi-input integration** stress test built from two canonical 10x PBMC matrices.

- Batch 1: `10x_PBMC_3k_scRNA_2016_S3` → `data/smoke/pbmc3k_real/filtered_feature_bc_matrix`
- Batch 2: `10x_PBMC_10k_v3_scRNA_2018_S3` → `data/smoke/pbmc10k_v3_real/filtered_feature_bc_matrix`
- Manifest bundle id (for convenience): `10x_PBMC3K_PBMC10K_V3_INTEGRATION_PAIR_S3`

## Spatial (Visium): v1 plan
Visium real-data smoke will be added after scRNA smoke is reproducible on cloud:
- Input contract: `filtered_feature_bc_matrix/` + `spatial/` from Cell Ranger spatial.
- Target: one small public Visium dataset (mouse brain) for smoke + audit.
