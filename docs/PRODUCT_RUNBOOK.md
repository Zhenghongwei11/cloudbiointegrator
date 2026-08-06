# PRODUCT_RUNBOOK (MVP web UI)

This project’s reviewer-facing “product” is the **constrained pipeline + Docker + audit bundles**. The web UI is a thin wrapper that submits **whitelisted** jobs and returns artifacts (figures + audit zip).

## What the MVP UI is (and is not)

**Is**
- Single-user / single-VM job launcher.
- Browser UI to upload/attach 10x inputs and run existing method packs.
- Produces **audit-first** artifacts (tables + `plots/publication/` + `docs/audit_runs/<run_id>.zip`).

**Is not**
- Multi-tenant SaaS (no accounts/RBAC/billing).
- Clinical decision support.
- A guarantee that GPU-tier methods run on laptops (compute-tier gating applies).

## Run locally

Prereqs:
- Python 3 + pip
- Docker Desktop (for running the pipeline container)

Start the UI:
```bash
make app
```

Then open:
- `http://127.0.0.1:8000`

Environment variables (optional):
- `CBA_IMAGE_TAG` (default: `sf-agent:smoke`)
- `CBA_DOCKER_BIN` (default: `docker`)
- `CBA_RUNS_DIR` (default: `./runs`)
- `CBA_HOST` (default: `127.0.0.1`)
- `CBA_PORT` (default: `8000`)
- `CBA_MAX_UPLOAD_MB` (default: `8192`)
- `CBA_ALLOWED_MOUNTS` (optional allowlist for host-path mounts; `:`-separated prefixes)

## How jobs run
- The UI writes a `runs/<run_id>/job.json` manifest.
- A runner exports a clean repo workspace using `git archive HEAD`.
- The runner extracts uploads into `data/user_uploads/<run_id>/...` inside the workspace.
- For large datasets, the runner can mount a host path read-only into the container (recommended on VMs).
- The runner executes the pipeline **inside Docker** (the same entrypoint as CLI runs).
- The runner collects artifacts into `runs/<run_id>/artifacts/`.

## Input modes (recommended)

1) **Zip upload** (small datasets / demos)
- Use the browser upload fields.
- Subject to `CBA_MAX_UPLOAD_MB`.

2) **Host-path mount** (large real datasets; VM recommended)
- Put your dataset on the VM filesystem.
- Provide an absolute path in the UI (dataset path / reference path fields).
- Optionally set `CBA_ALLOWED_MOUNTS` to restrict which host prefixes are mountable.

3) **GCS URI** (cloud-first)
- Provide a `gs://...` URI.
- The runner downloads into the isolated workspace before running.
- Requires `gsutil` or `gcloud` auth + access on the host.
Default backup bucket for reviewer artifacts:
- `gs://cloudbioagent-backup-quick-ray-450709-f2-20260208144149`
- Manifest + checksums: `docs/CLOUD_BACKUP_MANIFEST.tsv`

4) **Manifest preset** (public datasets; best for reviewers)
- Provide a `dataset_manifest_id` / `reference_manifest_id`.
- The container runs `python3 scripts/data/fetch_dataset.py --dataset-id <id> --extract` before the pipeline stage.

## Reproducibility handoff (reviewer)
For peer review, the recommended artifacts are:
- an audit zip (e.g., `runs/<run_id>/artifacts/<run_id>.zip`)
- the `job.json` (included under `docs/jobs/<run_id>/job.json` inside the audit workspace)

The reviewer can replay the job using the CLI by re-running the same pipeline command(s) recorded in the logs, on the same git commit, using the same container tag.

### Replay a job manifest
If you have a `job.json` (e.g., extracted from an audit zip):
```bash
python3 scripts/app/execute_job.py --job-json /path/to/job.json
```

Notes:
- Replay works best when the job uses `dataset_path` (VM mount) or `dataset_gcs_uri`.
- If the job relied on browser uploads (`dataset_zip`), the original zip is not included in audit zips by default.
