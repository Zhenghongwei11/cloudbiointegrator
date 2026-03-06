# Reviewer Reproduction Checklist (Docker)

This project ships as a containerized method library for scRNA-seq + Visium, with reviewer-facing artifacts
(figures + TSV evidence tables) emitted as zip bundles.

This checklist is intentionally minimal and deterministic: it tells you exactly what to run, what you should
expect to see on disk, and how to verify that the outputs correspond to the manuscript figures/tables.

## Prerequisites

- Docker Desktop (or Docker Engine) running
- Disk: >= 30 GiB free recommended for the first build (no cache)
- OS: macOS/Linux recommended; Windows supported via Docker Desktop

## Quick Reproduction (Smoke + Review Bundle)

From the repository root:

```bash
# 1) Build the image
docker build -t cloudbiointegrator:local .

# 2) Run minimal end-to-end checks (writes outputs into your working directory)
docker run --rm -v "$PWD:/work" -w /work cloudbiointegrator:local \
  bash -lc "make skeleton && make validate && make smoke && make validate && make review-bundle"
```

### Expected Outputs (On Your Host Filesystem)

- `docs/audit_runs/<RUN_ID>-skeleton-*.zip`
- `docs/audit_runs/<RUN_ID>-smoke-*.zip`
- `docs/review_bundle/review_bundle.zip`
- `docs/review_bundle/checksums.sha256`
- `docs/review_bundle/review_bundle.zip.sha256`

### What To Inspect (Reviewer-Facing Evidence)

The bundle `docs/review_bundle/review_bundle.zip` must contain:

- Figures (PNG):
  - `plots/publication/png/F1_system_contract.png`
  - `plots/publication/png/F2_reproducibility.png`
  - `plots/publication/png/F3_scrna_benchmark.png`
  - `plots/publication/png/F4_spatial_benchmark.png`
  - `plots/publication/png/F5_ops_benchmark.png`
  - `plots/publication/png/F6_robustness_matrix.png`
- Evidence tables (TSV):
  - `results/audit/reproducibility_checks.tsv`
  - `results/benchmarks/runtime_cost_failure.tsv`
  - `results/benchmarks/robustness_matrix.tsv`
  - `results/benchmarks/biological_output_concordance.tsv`

These TSV tables are the source-of-truth for any numeric claims in the manuscript. Figures are generated
from these tables.

For column/metric definitions (data dictionary), see:
- `results/audit/data_dictionary.tsv`

### Integrity Verification (Hash-Based)

```bash
# Bundle checksum (single-file)
shasum -a 256 docs/review_bundle/review_bundle.zip

# Internal checksums file (files inside the bundle, including TSV/PNG)
cat docs/review_bundle/checksums.sha256
```

## Optional Modules (Heavier Runners)

The default image includes Visium baseline deconvolution (RCTD via `spacexr`) and Tangram.

The GPU-heavy `cell2location` runner is OFF by default. Enable it only if needed:

```bash
docker build -t cloudbiointegrator:cell2location --build-arg INSTALL_CELL2LOCATION=1 .
```

## Troubleshooting

- If the first build fails due to transient upstream downloads, re-run `docker build ...`.
- If you are low on disk, reclaim space:
  - `docker builder prune -af`
  - `docker system prune -af`
