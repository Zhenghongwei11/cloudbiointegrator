# CLOUD_RUNBOOK (GCP VM smoke deployment)

Goal: run `skeleton`/`smoke` on a **fresh GCP VM** and export an audit bundle (`docs/audit_runs/<run_id>.zip`) that a reviewer can use to verify what ran.

## What reviewers should be able to do
- Clone the repo at a tagged release.
- Build the container.
- Run `make skeleton`, `make smoke`, `make validate`.
- Inspect `docs/audit_runs/<run_id>/meta.json`, `checksums.sha256`, and the generated figures/tables.

## Prerequisites
- A GCP project with billing enabled (free credits OK).
- IAM permission to create a VM (Compute Engine) and open SSH.
- Local machine has `gcloud` installed and authenticated.

## Recommended VM spec (smoke only)
- Machine type: `e2-standard-4` (4 vCPU, 16 GB) is enough for skeleton/smoke contract validation.
- Disk: 50–100 GB standard persistent disk.
- OS image: Debian 12 or Ubuntu 22.04 LTS.

## Create VM (example)
Replace `PROJECT_ID`, `ZONE`, `VM_NAME`.

```bash
gcloud config set project PROJECT_ID
gcloud compute instances create VM_NAME \
  --zone=ZONE \
  --machine-type=e2-standard-4 \
  --boot-disk-size=80GB \
  --image-family=debian-12 \
  --image-project=debian-cloud
```

## SSH into VM
```bash
gcloud compute ssh VM_NAME --zone=ZONE
```

## On the VM: install Docker
Debian/Ubuntu example:
```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl git
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker $USER
newgrp docker
docker --version
```

## On the VM: clone repo (tagged)
```bash
git clone REPO_URL sf-agent
cd sf-agent
git checkout TAG_OR_COMMIT
```

## Build the container
```bash
docker build -t sf-agent:smoke .
```

### Optional: build a cell2location-enabled image
`cell2location` is an optional Visium deconvolution runner with extra Python deps. To keep the default image lean/reproducible, it is **off by default**.

Build it explicitly:
```bash
docker build -t sf-agent:cell2location --build-arg INSTALL_CELL2LOCATION=1 .
```

### Optional: enable GPU torch wheels (for Tangram/cell2location on L4/A10/etc.)
By default, the image installs CPU torch wheels for portability. On a GPU VM (with `docker run --gpus all` working), build with CUDA torch wheels:

```bash
docker build -t sf-agent:cell2location-gpu \
  --build-arg INSTALL_CELL2LOCATION=1 \
  --build-arg INSTALL_CUDA_TORCH=1 \
  .
```

## Run skeleton/smoke/validate (write to mounted output)
This pattern makes the outputs persist on the VM filesystem:

```bash
mkdir -p /tmp/sf-agent-run
docker run --rm \
  -u "$(id -u):$(id -g)" \
  -v "$PWD:/app" \
  -w /app \
  sf-agent:smoke \
  bash -lc "make skeleton && make validate && make smoke && make validate"
```

Expected outputs:
- `plots/publication/*`
- `results/*`
- `docs/audit_runs/<run_id>/` and `docs/audit_runs/<run_id>.zip`

## Real public smoke dataset (recommended)
For reviewer-facing reproducibility, run at least one smoke using a **real public 10x matrix** (not the toy bundled matrix).

We recommend the classic 10x PBMC 3k matrix hosted on a stable S3 endpoint (cellranger v1 layout):

```bash
mkdir -p data/smoke/pbmc3k_real
cd data/smoke/pbmc3k_real
curl -L -o pbmc3k_filtered_gene_bc_matrices.tar.gz \
  https://s3-us-west-2.amazonaws.com/10x.files/samples/cell/pbmc3k/pbmc3k_filtered_gene_bc_matrices.tar.gz
tar -xzf pbmc3k_filtered_gene_bc_matrices.tar.gz
mkdir -p filtered_feature_bc_matrix
cp -v filtered_gene_bc_matrices/hg19/matrix.mtx filtered_feature_bc_matrix/
cp -v filtered_gene_bc_matrices/hg19/barcodes.tsv filtered_feature_bc_matrix/
cp -v filtered_gene_bc_matrices/hg19/genes.tsv filtered_feature_bc_matrix/features.tsv
cd ../../..
```

Then run smoke pointing at that folder:

```bash
make smoke ARGS="--input-dir=data/smoke/pbmc3k_real/filtered_feature_bc_matrix --dataset-id=10x_PBMC_3k_scRNA_2016_S3"
make validate
```

## scRNA method-pack benchmark (baseline; Scanpy + optional CellTypist)

This is the first “real method library” milestone (beyond ingest-only smoke). It appends benchmark rows and writes audit bundles.

On the VM, fetch datasets and the single required CellTypist model file:

```bash
python3 scripts/data/fetch_dataset.py --dataset-id 10x_PBMC_3k_scRNA_2016_S3 --extract
python3 scripts/data/convert_10x_gene_bc_to_feature_bc.py \
  --input-dir data/smoke/pbmc3k_real/filtered_gene_bc_matrices/hg19 \
  --output-dir data/smoke/pbmc3k_real/filtered_feature_bc_matrix

python3 scripts/data/fetch_dataset.py --dataset-id 10x_PBMC_10k_v3_scRNA_2018_S3 --extract
python3 scripts/data/fetch_celltypist_model.py --model Immune_All_Low.pkl
```

Run the baseline pack:

```bash
make scrna ARGS="--input-dir data/smoke/pbmc3k_real/filtered_feature_bc_matrix --dataset-id 10x_PBMC_3k_scRNA_2016_S3 --method-pack baseline --annotate celltypist --seed 0"
make scrna ARGS="--input-dir data/smoke/pbmc10k_v3_real/filtered_feature_bc_matrix --dataset-id 10x_PBMC_10k_v3_scRNA_2018_S3 --method-pack baseline --annotate celltypist --seed 0"
make validate
```

Expected new outputs:
- `results/benchmarks/method_benchmark.tsv` (QC + clustering + annotation metrics)
- `results/benchmarks/biological_output_concordance.tsv` (e.g., label–cluster concordance)
- `results/benchmarks/robustness_matrix.tsv` (e.g., seed stability)
- `docs/audit_runs/<run_id>.zip`

## Visium method-pack baseline (10x Space Ranger outputs)

The v0 Visium baseline is intentionally minimal and audit-first:
- entrypoint: Space Ranger outputs (`filtered_feature_bc_matrix/` + `spatial/`)
- baseline runner: QC summaries + coordinate-graph clustering

On the VM, fetch and extract the canonical Visium dataset:

```bash
python3 scripts/data/fetch_dataset.py --dataset-id 10x_Visium_Human_Lymph_Node_1p1_cf --extract
```

Run the Visium baseline pack:

```bash
make visium ARGS="--input-dir data/smoke/visium_human_lymph_node_real --dataset-id 10x_Visium_Human_Lymph_Node_1p1_cf --method-pack baseline --seed 0 --organism human --tissue lymph_node"
make validate
```

Expected new outputs:
- `results/figures/visium_spots.tsv` (spot-level anchor table)
- `results/benchmarks/method_benchmark.tsv` (task=`visium_qc+cluster`)
- `docs/audit_runs/<run_id>.zip`

## Export audit bundles for reviewers
Option A: upload the `.zip` bundles to a public/controlled location (recommended).

### Upload to GCS (optional)
```bash
gsutil mb -p PROJECT_ID -l REGION gs://YOUR_BUCKET
gsutil cp docs/audit_runs/*.zip gs://YOUR_BUCKET/audit_runs/
```
Default backup location (current working bucket):
- `gs://cloudbioagent-backup-quick-ray-450709-f2-20260208144149`
- Manifest + checksums: `docs/CLOUD_BACKUP_MANIFEST.tsv`

## Cleanup (avoid cost)
From your local machine:
```bash
gcloud compute instances delete VM_NAME --zone=ZONE
```

## Notes / limitations
- This runbook is for **smoke** only. Heavy benchmarks require a larger VM and (optionally) GPU.
- Do not include private/PHI data in audit bundles.

## Q1 benchmark suite (manifest-driven)
For paper-facing “Q1 benchmark style” density (many reruns/seeds across scRNA + Visium + robustness anchors), use:
- `docs/Q1_BENCHMARK_RUNBOOK.md`
- `bash scripts/cloud/run_q1_benchmark_suite.sh` (or `make q1-bench`)

## Host the MVP web UI on a VM (single-tenant)
If you want a “product surface”, you can run the MVP UI on the same VM.

Prereqs:
- Docker installed (same as above).
- Python 3 + pip installed on the VM.

Steps (on VM, in the repo root):
```bash
make app
```

Then expose port 8000 (example, GCP):
- open the VM firewall for TCP:8000 to your IP (recommended), or use an SSH tunnel:
```bash
gcloud compute ssh VM_NAME --zone=ZONE -- -L 8000:127.0.0.1:8000
```
Open `http://127.0.0.1:8000` locally.

### Run a reviewer-friendly UI job (public preset; no uploads)
Recommended for audit evidence without large browser uploads:
1) Build the pipeline image once on the VM:
```bash
docker build -t sf-agent:smoke .
```
2) Start the UI:
```bash
export CBA_IMAGE_TAG=sf-agent:smoke
export CBA_ALLOWED_MOUNTS="/tmp:/home:/mnt:/data"
make app
```
3) In the UI, submit a job using **Dataset manifest ID** (examples):
- scRNA baseline demo: `10x_PBMC_3k_scRNA_2016_S3`
- Visium baseline demo: `10x_Visium_Human_Lymph_Node_1p1_cf`

The runner will fetch the dataset inside the container via `scripts/data/fetch_dataset.py` and generate:
- `runs/<run_id>/artifacts/<run_id>.zip` (audit bundle)
- `runs/<run_id>/artifacts/publication_*.pdf/png` (figures)
