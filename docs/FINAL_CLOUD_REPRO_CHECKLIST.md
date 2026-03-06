# FINAL_CLOUD_REPRO_CHECKLIST (submission-ready)

Purpose: one-click, reviewer-facing cloud rerun that reproduces tables, figures, and audit bundles on a fresh VM.

## Prereqs (once per reviewer VM)
- GCP VM with GPU (L4/A10) + sufficient disk (>=500 GB) and RAM (>=128 GB recommended).
- `gcloud` + `gsutil` authenticated on the VM.
- Docker installed with NVIDIA runtime if using GPU.

## 1) Start VM and prepare workspace
```bash
gcloud compute instances start cloudbiointegrator-visium-c2l-1 --zone us-central1-a

gcloud compute ssh cloudbiointegrator-visium-c2l-1 --zone us-central1-a
# on VM
cd ~
rm -rf cloudbiointegrator

git clone <REPO_URL> cloudbiointegrator
cd cloudbiointegrator

git checkout 13fefc851a7559dceee0133f45963965b8520904
```

## 2) Fetch public datasets (authoritative sources)
```bash
python3 scripts/data/fetch_dataset.py --dataset-id 10x_PBMC_10k_scRNA --extract
python3 scripts/data/fetch_dataset.py --dataset-id 10x_PBMC_3k_scRNA_2016_S3 --extract
python3 scripts/data/fetch_dataset.py --dataset-id 10x_Visium_Human_Lymph_Node_1p1_cf --extract
python3 scripts/data/fetch_dataset.py --dataset-id Mouse_Brain_Visium_10x --extract
python3 scripts/data/fetch_dataset.py --dataset-id Allen_Cortex_scRNA_SeuratV5_Reference_RDS --extract
```

## 3) Baseline CPU runs (scrna + visium)
```bash
IMAGE_TAG=cloudbiointegrator:smoke \
SCRNA_ARGS="--input-dir data/smoke/pbmc10k_v3_real/filtered_feature_bc_matrix --dataset-id 10x_PBMC_10k_scRNA --method-pack baseline --seed 0 --organism human --tissue blood" \
VISIUM_ARGS="--input-dir data/smoke/visium_mouse_brain_real --dataset-id Mouse_Brain_Visium_10x --method-pack baseline --seed 0 --organism mouse --tissue brain" \
GCS_BUCKET=gs://<your-gcs-bucket>/cloudbiointegrator/ \
  bash scripts/cloud/run_on_vm.sh
```

## 4) scVI GPU integration run
```bash
IMAGE_TAG=cloudbiointegrator:scvi-gpu \
DOCKERFILE=Dockerfile.scvi \
DOCKER_TARGET=scvi-gpu \
DOCKER_GPU=1 \
DOCKER_BUILD_ARGS="--build-arg INSTALL_CELL2LOCATION=1" \
SCRNA_ARGS="--input-dir data/smoke/pbmc3k_real/filtered_feature_bc_matrix,data/smoke/pbmc10k_v3_real/filtered_feature_bc_matrix --dataset-id 10x_PBMC3K_PBMC10K_V3_INTEGRATION_PAIR_S3 --method-pack advanced --runner scvi --compute-tier gpu --annotate celltypist --scvi-max-epochs 50 --scvi-n-latent 30 --seed 0" \
GCS_BUCKET=gs://<your-gcs-bucket>/cloudbiointegrator/ \
  bash scripts/cloud/run_on_vm.sh
```

## 5) Visium deconvolution (RCTD + Tangram + cell2location)
```bash
IMAGE_TAG=cloudbiointegrator:scvi-gpu \
DOCKERFILE=Dockerfile.scvi \
DOCKER_TARGET=scvi-gpu \
DOCKER_GPU=1 \
DOCKER_BUILD_ARGS="--build-arg INSTALL_CELL2LOCATION=1" \
VISIUM_ARGS="--input-dir data/smoke/visium_mouse_brain_real --dataset-id Mouse_Brain_Visium_10x --method-pack deconvolution --runner all --reference-scrna-dir data/references/allen_cortex/prepared/filtered_feature_bc_matrix --reference-dataset-id Allen_Cortex_scRNA_SeuratV5_Reference_RDS --reference-labels-tsv data/references/allen_cortex/prepared/reference_labels.tsv --compute-tier gpu --seed 0 --organism mouse --tissue brain --cell2location-max-epochs 400 --cell2location-regression-max-epochs 100 --cell2location-num-samples 50 --cell2location-max-cells 10000 --cell2location-max-spots 3000" \
GCS_BUCKET=gs://<your-gcs-bucket>/cloudbiointegrator/ \
  bash scripts/cloud/run_on_vm.sh
```

## 6) Figures + audit
```bash
make figures
make audit
```

## 7) Upload final artifacts
```bash
gsutil -m cp docs/audit_runs/*.zip gs://<your-gcs-bucket>/cloudbiointegrator/audit_runs/
gsutil -m cp plots/publication/pdf/*.pdf plots/publication/png/*.png gs://<your-gcs-bucket>/cloudbiointegrator/figures/
```

## 8) Record evidence
- Update `docs/audit_runs_gcp/README.md` with the new run_id(s).
- Append new rows to `results/benchmarks/runtime_cost_failure.tsv` (phase=vFinal if needed).
- Recompute `results/effect_sizes/claim_effects.tsv` via `python3 scripts/analysis/compute_claim_effects.py`.

## 9) Stop VM to control cost
```bash
gcloud compute instances stop cloudbiointegrator-visium-c2l-1 --zone us-central1-a
```
