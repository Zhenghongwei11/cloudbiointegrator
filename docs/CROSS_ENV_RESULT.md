# Cross-Environment Check: PASS (2026-08-06)

## What was verified

The minimal reproducible path (`make skeleton && make smoke`, toy 10x matrix
`SMOKE_TOY_10X_MTX`: 80 cells, 50 genes, 200 matrix entries) was executed in **three
independent environments (two local container stacks + one cloud container)**, and the
run-scoped outcome summaries were compared byte-for-byte via
`scripts/audit/compare_run_manifests.py`.

## Environments

| | Environment 1 (baseline) | Environment 2 | Environment 3 (cloud) |
|---|---|---|
| Image | `cloudbiointegrator:local` (rocker/r2u, Ubuntu 24.04, Python 3.11, R installed) | `python:3.12-slim` (Debian, Python 3.12, no R) | ModelScope 创空间 Gradio 容器 (Python 3.12, no R) |
| Host | macOS (Docker Desktop) | macOS (Docker Desktop, same host) | ModelScope 云端 (lgmoon/cbi-cross-env-check) |
| Dependencies | pinned via image | ad-hoc pip install | platform image + requirements.txt |
| R figure scripts | run | skipped (WARN) | skipped (WARN) |

## Result

All three environments produced **byte-identical** run-scoped summaries for the declared
minimal path:

```
PASS  skeleton / NA                  sha256=c1da5ef81534edbf...
PASS  smoke_ingest / SMOKE_TOY_10X_MTX  sha256=21e6c59047c96b3e...
RESULT: PASS — run-scoped artifacts are byte-identical across environments
```

Baseline manifest: `results/runs/baseline_manifest.tsv` (run 20260806T0421xx, local image).
Second manifest: `results/runs/env2_manifest.tsv` (run 20260806T0442xx, python:3.12-slim).
Cloud manifest: `results/runs/modelscope_manifest.tsv` (run 20260806T0553xx, ModelScope 创空间
`https://modelscope.cn/studios/lgmoon/cbi-cross-env-check`, triggered via the app's Gradio
`/do_check` endpoint on 2026-08-06).

## Honest interpretation

- This proves byte-identical outcomes across **three different container/OS/Python stacks**,
  including a real cloud environment (ModelScope 创空间, Linux x86_64) that is physically
  separate from the local Mac.
- Env 1 vs Env 2 ran on the same host (different container stacks); Env 3 ran on ModelScope
  cloud — the combination covers both "different runtime stack" and "different machine/cloud".
- The summaries contain no software versions by design, so the check isolates output
  determinism rather than environment equality.

## Reproduce

```bash
# Env 2 (second container stack):
docker run --rm -v "$PWD:/work" -w /work python:3.12-slim bash -lc \
  "apt-get update -qq && apt-get install -y -qq make >/dev/null && \
   pip install --quiet anndata scipy numpy pandas scikit-learn matplotlib && \
   make skeleton && make smoke && \
   python3 scripts/audit/record_run_manifest.py --out results/runs/env2_manifest.tsv"

# Compare:
python3 scripts/audit/compare_run_manifests.py \
  results/runs/baseline_manifest.tsv results/runs/env2_manifest.tsv
```
