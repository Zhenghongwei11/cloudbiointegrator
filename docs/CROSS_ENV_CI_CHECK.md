# Cross-Environment Reproducibility Check (local Docker vs GitHub Actions)

This adds a **real, reviewer-facing cross-environment byte-level check** that does not require
paid cloud compute. It replaces the earlier ledger-only framing with per-run scoped artifacts
whose SHA-256 are compared between your local Docker environment and a clean GitHub Actions
runner (a genuinely different environment).

## Two ways to run the cross-environment check

- **Option A (automatic, free): GitHub Actions.** Commit the baseline, push, and the workflow
  re-runs skeleton+smoke on a clean ubuntu runner and compares byte-for-byte. No cloud bill.
- **Option B (manual, also free): a second computer.** Any other machine with Docker installed
  (a lab workstation, a colleague's laptop, a school server) works exactly the same way and is
  arguably an even more convincing "different environment". Steps:

  ```bash
  # On the second machine, with the same git commit checked out:
  docker build -t cloudbiointegrator:second .
  docker run --rm -v "$PWD:/work" -w /work cloudbiointegrator:second \
    bash -lc "make skeleton && make smoke && \
              python3 scripts/audit/record_run_manifest.py --out results/runs/second_manifest.tsv"
  # Copy results/runs/second_manifest.tsv back to the main machine and compare:
  python3 scripts/audit/compare_run_manifests.py \
    results/runs/baseline_manifest.tsv results/runs/second_manifest.tsv
  ```

  If the two machines have different OS/architecture (e.g., macOS arm64 vs Linux x86_64),
  a PASS is a much stronger cross-environment statement than two runs on the same machine.

> Note (2026-08): Hugging Face **free** Spaces no longer host Docker/Gradio code — only Static
> Spaces are free; Docker Spaces require a PRO subscription. Do not spend time on the HF free
> route; use Option A or B instead. Google Colab also works (see `docs/COLAB_CROSS_ENV_CHECK.md`).

## What is compared

Every pipeline stage now writes a **run-scoped outcome summary** to:

`results/runs/<run_id>/<stage>_summary.json`

The JSON contains only deterministic logical content (metrics, concordance, versions, params) —
**no timestamps, no wall times, no run ids** — so the same stage + dataset + pinned runtime
produces byte-identical files in any environment. `scripts/audit/record_run_manifest.py`
hashes the latest summary per (stage, dataset_id) into a manifest; the CI workflow compares
that manifest against the committed local baseline.

## One-time local setup (free; uses your existing Docker)

```bash
# 1) Build or pull the runtime image once
docker build -t cloudbiointegrator:local .

# 2) Run the minimal reproducible set and record the LOCAL baseline
docker run --rm -v "$PWD:/work" -w /work cloudbiointegrator:local \
  bash -lc "make skeleton && make smoke && \
            python3 scripts/audit/record_run_manifest.py --out results/runs/baseline_manifest.tsv"

# 3) Commit the baseline + workflow
git add results/runs/baseline_manifest.tsv .github/workflows/repro-check.yml \
  scripts/audit/record_run_manifest.py scripts/audit/compare_run_manifests.py
git commit -m "feat(audit): run-scoped cross-environment reproducibility baseline"
git push
```

## What happens on push (or manual run)

GitHub Actions (ubuntu-latest, free for public repos) checks out the same commit, runs
`make skeleton && make validate && make smoke && make validate` inside the same pinned
container image, records `results/runs/ci_manifest.tsv`, and compares it with
`results/runs/baseline_manifest.tsv`:

- Every `(stage, dataset_id)` present in the baseline must also be present in CI;
- SHA-256 of the run-scoped summaries must be identical;
- Any mismatch fails the job — that is the cross-environment drift signal.

## Interpretation

- **PASS**: the declared minimal workflow produced byte-identical run-scoped outcomes in two
  independent environments. This is the evidence the manuscript's C1 claim should reference.
- **FAIL**: environment drift (dependency versions, nondeterminism, or a real code bug). Fix
  the drift, rerun locally, and only then update the baseline (update means "we accept this as
  the new pinned truth", not "ignore the failure").

## Correctness anchor

Cross-environment agreement proves **reproducibility**, not **correctness** (a stable bug
reproduces too). The workflow therefore also asserts that the toy smoke outcome equals the
expected values (80 cells, 50 genes, 200 matrix entries). Keep this anchor whenever the smoke
dataset or ingest contract changes.

## Known boundary (honest scope)

This check covers the **minimal reproducible path** (skeleton + smoke). The paper's heavier
artifacts (benchmark tables, robustness matrix, deconvolution weights) are cumulative
append-only tables whose byte-level digests legitimately differ across runs; their
cross-environment claim is **semantic concordance** (documented in `docs/ENV_COMPARE_*`) plus
per-run traceability, not byte-level identity. If you want byte-level coverage for scRNA/Visium
baselines, add the CPU-only `make scrna`/`make visium` targets to this workflow (they emit
run-scoped summaries too); GPU-tier methods stay out by design.

## Optional extension

To cover scRNA baseline and Visium baseline in the same check, add the corresponding `make`
targets to the workflow (both are CPU-only):

```bash
make scrna ARGS="--input-dir data/smoke/pbmc3k_real/... --dataset-id 10x_PBMC_3k_scRNA_2016_S3"
make visium ARGS="--input-dir data/smoke/visium_human_lymph_node_real --dataset-id 10x_Visium_Human_Lymph_Node_1p1_cf"
```

GPU-tier methods (scVI, cell2location) stay out of this minimal check by design.
