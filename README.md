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

