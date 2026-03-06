# CloudBioIntegrator

CloudBioIntegrator is a containerized method library and reproducibility bundle for scRNA-seq and 10x Visium spatial transcriptomics workflows.

## What This Repository Contains

- Reproducibility-first pipeline scripts (`scripts/`)
- Contract schemas (`schemas/`)
- Evidence tables (`results/`) and publication figures (`plots/publication/`) generated from those tables
- Minimal reviewer-oriented documentation (`docs/`)

This public code release intentionally excludes journal submission drafts and other portal-specific files.

## Quickstart (Docker)

Requirements: Docker Desktop/Engine.

This page is a minimal **usage** and **reproduce/replicate** guide for the included evidence tables and figures.
The container provides a pinned **environment** for consistent execution.

### Option A: Pull a Prebuilt Image (Recommended)

If a tagged image is available on GHCR, pull it:

```bash
docker pull ghcr.io/zhenghongwei11/cloudbiointegrator:v0.1.7
```

Then run:

```bash
docker run --rm -v "$PWD:/work" -w /work ghcr.io/zhenghongwei11/cloudbiointegrator:v0.1.7 \
  bash -lc "make skeleton && make validate"
```

### Option B: Build Locally

```bash
# 1) Build the image
docker build -t cloudbiointegrator:local .

# 2) Minimal contract validation + figure regeneration from included evidence tables
docker run --rm -v "$PWD:/work" -w /work cloudbiointegrator:local \
  bash -lc "make skeleton && make validate"
```

From scratch (optional, pulls public example data and runs a minimal smoke path):

```bash
docker run --rm -v "$PWD:/work" -w /work cloudbiointegrator:local \
  bash -lc "make smoke && make validate"
```

## Expected Results

After a successful run you should see updated files under:

- `results/` (tables)
- `plots/publication/` (figures)

If you need to install dependencies outside Docker, use the Dockerfile as the environment specification.

## Evidence Tables

All quantitative claims are backed by TSV tables under `results/`.
Column and metric definitions are documented in:

- `results/audit/data_dictionary.tsv`

## Figures

Publication-ready figures are under:

- `plots/publication/png/`
- `plots/publication/pdf/`

## License

See `LICENSE`.
