#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
import shlex
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class ManifestRow:
    run_group: str
    task_type: str  # scrna|visium
    dataset_id: str
    input_dir: str
    method_pack: str
    runner: str
    compute_tier: str
    seed_spec: str
    extra_args: str


def _utc_run_id(prefix: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{ts}-{prefix}"


def _parse_seed_spec(seed_spec: str) -> list[int]:
    s = (seed_spec or "").strip()
    if not s:
        return [0]
    if ".." in s:
        a, b = s.split("..", 1)
        lo = int(a.strip())
        hi = int(b.strip())
        if hi < lo:
            raise ValueError(f"invalid seed_spec range: {seed_spec!r}")
        return list(range(lo, hi + 1))
    if "," in s:
        out: list[int] = []
        for part in s.split(","):
            part = part.strip()
            if part:
                out.append(int(part))
        return out or [0]
    return [int(s)]


def _read_manifest(path: Path) -> list[ManifestRow]:
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader((ln for ln in f if ln.strip() and not ln.lstrip().startswith("#")), delimiter="\t")
        required = {
            "run_group",
            "task_type",
            "dataset_id",
            "input_dir",
            "method_pack",
            "runner",
            "compute_tier",
            "seed_spec",
            "extra_args",
        }
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise SystemExit(f"manifest missing columns: {sorted(missing)}")

        rows: list[ManifestRow] = []
        for r in reader:
            rows.append(
                ManifestRow(
                    run_group=(r.get("run_group") or "").strip(),
                    task_type=(r.get("task_type") or "").strip(),
                    dataset_id=(r.get("dataset_id") or "").strip(),
                    input_dir=(r.get("input_dir") or "").strip(),
                    method_pack=(r.get("method_pack") or "").strip(),
                    runner=(r.get("runner") or "").strip(),
                    compute_tier=(r.get("compute_tier") or "").strip(),
                    seed_spec=(r.get("seed_spec") or "").strip(),
                    extra_args=(r.get("extra_args") or "").strip(),
                )
            )
        return rows


def _run(cmd: list[str], *, cwd: Path, dry_run: bool, log_file: Path | None) -> None:
    line = shlex.join(cmd)
    print(f"[q1] $ {line}")
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        with log_file.open("a", encoding="utf-8") as f:
            f.write(f"$ {line}\n")
    if dry_run:
        return
    subprocess.check_call(cmd, cwd=str(cwd))


def _docker_run(
    inner_cmd: list[str],
    *,
    docker_bin: str,
    image: str,
    gpu: bool,
    dry_run: bool,
    log_file: Path | None,
) -> None:
    uid = os.getuid() if hasattr(os, "getuid") else 1000
    gid = os.getgid() if hasattr(os, "getgid") else 1000

    inner = shlex.join(inner_cmd)
    cmd = [
        docker_bin,
        "run",
        "--rm",
        "-u",
        f"{uid}:{gid}",
        "-v",
        f"{ROOT}:/app",
        "-w",
        "/app",
    ]
    if gpu:
        cmd += ["--gpus", "all"]
    cmd += [image, "bash", "-lc", inner]
    _run(cmd, cwd=ROOT, dry_run=dry_run, log_file=log_file)


def _ensure_datasets(dataset_ids: set[str], *, dry_run: bool, log_file: Path | None) -> None:
    # Only fetch dataset_ids that exist in data/manifest.tsv. Some suite rows use
    # "logical" dataset_id labels (e.g., integration scenario names) that map to
    # already-fetched inputs.
    manifest_path = ROOT / "data" / "manifest.tsv"
    known: set[str] = set()
    with manifest_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            ds = (row.get("dataset_id") or "").strip()
            if ds:
                known.add(ds)

    for ds in sorted(dataset_ids):
        if ds not in known:
            print(f"[q1] skip fetch (not in data/manifest.tsv): {ds}")
            continue
        # fetch_dataset.py is idempotent and verifies sha256.
        _run(
            ["python3", "scripts/data/fetch_dataset.py", "--dataset-id", ds, "--extract"],
            cwd=ROOT,
            dry_run=dry_run,
            log_file=log_file,
        )


def _ensure_allen_reference_prepared(
    *,
    docker: bool,
    docker_bin: str,
    image_main: str,
    gpu: bool,
    dry_run: bool,
    log_file: Path | None,
) -> None:
    labels = ROOT / "data" / "references" / "allen_cortex" / "prepared" / "reference_labels.tsv"
    matrix_dir = ROOT / "data" / "references" / "allen_cortex" / "prepared" / "filtered_feature_bc_matrix"
    if labels.exists() and matrix_dir.exists():
        print("[q1] OK: Allen cortex reference already prepared")
        return

    # Ensure the RDS exists via manifest.
    _run(
        ["python3", "scripts/data/fetch_dataset.py", "--dataset-id", "Allen_Cortex_scRNA_SeuratV5_Reference_RDS"],
        cwd=ROOT,
        dry_run=dry_run,
        log_file=log_file,
    )

    prep_cmd = ["Rscript", "scripts/data/prepare_allen_cortex_reference.R"]
    if docker:
        # No GPU needed for reference preparation.
        _docker_run(prep_cmd, docker_bin=docker_bin, image=image_main, gpu=False, dry_run=dry_run, log_file=log_file)
    else:
        _run(prep_cmd, cwd=ROOT, dry_run=dry_run, log_file=log_file)


def main() -> int:
    ap = argparse.ArgumentParser(description="Run the manifest-driven Q1 benchmark suite (Docker + GPU friendly).")
    ap.add_argument("--manifest", default="runs/q1_benchmark_manifest.tsv", help="TSV manifest path.")
    ap.add_argument("--run-id", default="", help="Optional run id for logs/outputs (default: timestamped).")
    ap.add_argument("--only-group", default="", help="Only run rows where run_group contains this substring.")
    ap.add_argument("--max-runs", type=int, default=0, help="Stop after N expanded runs (0=all).")
    ap.add_argument("--skip-fetch", action="store_true", help="Skip manifest dataset fetch/extract.")

    ap.add_argument("--docker", action="store_true", default=True, help="Run pipeline steps in Docker (recommended).")
    ap.add_argument("--no-docker", dest="docker", action="store_false", help="Run on host (requires deps installed).")
    ap.add_argument("--docker-bin", default="docker")
    ap.add_argument("--image-main", default="sf-agent:q1")
    ap.add_argument("--image-scvi", default="sf-agent:scvi-gpu")
    ap.add_argument("--skip-build", action="store_true", help="Skip building Docker images.")
    ap.add_argument("--gpu", action="store_true", default=True, help="Enable --gpus all for gpu-tier rows.")
    ap.add_argument("--no-gpu", dest="gpu", action="store_false")

    ap.add_argument("--skip-post", action="store_true", help="Skip figures/review-bundle/doc export steps.")
    ap.add_argument("--dry-run", action="store_true", help="Print commands without executing.")
    args = ap.parse_args()

    manifest_path = (ROOT / args.manifest).resolve()
    if not manifest_path.exists():
        raise SystemExit(f"missing manifest: {manifest_path}")

    run_id = args.run_id.strip() or _utc_run_id("q1-benchmark-suite")
    run_dir = ROOT / "runs" / run_id
    log_file = run_dir / "job.log"

    rows = _read_manifest(manifest_path)
    if args.only_group:
        rows = [r for r in rows if args.only_group in r.run_group]
        if not rows:
            raise SystemExit(f"no rows matched --only-group={args.only_group!r}")

    # Decide which datasets to fetch from data/manifest.tsv.
    # Note: deconvolution needs the Allen cortex reference, but we prepare it separately.
    dataset_ids: set[str] = set()
    for r in rows:
        if r.dataset_id:
            dataset_ids.add(r.dataset_id)
    # Multi-batch integration compare uses a derived layout; ensure it gets materialized.
    if any("pbmc_integration_pair_real" in r.input_dir for r in rows):
        dataset_ids.add("10x_PBMC3K_PBMC10K_V3_INTEGRATION_PAIR_S3")

    print(f"[q1] run_id={run_id}")
    print(f"[q1] manifest={manifest_path.relative_to(ROOT)} rows={len(rows)}")
    print(f"[q1] docker={args.docker} gpu={args.gpu}")

    if not args.skip_fetch:
        _ensure_datasets(dataset_ids, dry_run=args.dry_run, log_file=log_file)

    if args.docker and not args.skip_build:
        # Build main image with CUDA torch to accelerate Tangram when a GPU is present.
        _run(
            [
                args.docker_bin,
                "build",
                "-f",
                "Dockerfile",
                "-t",
                args.image_main,
                "--build-arg",
                "INSTALL_CUDA_TORCH=1",
                ".",
            ],
            cwd=ROOT,
            dry_run=args.dry_run,
            log_file=log_file,
        )
        # Build scVI GPU image (used only for runner=scvi).
        _run(
            [
                args.docker_bin,
                "build",
                "-f",
                "Dockerfile.scvi",
                "-t",
                args.image_scvi,
                "--target",
                "scvi-gpu",
                ".",
            ],
            cwd=ROOT,
            dry_run=args.dry_run,
            log_file=log_file,
        )

    # Prepare deconvolution reference (only if any visium deconv row exists).
    if any(r.task_type == "visium" and r.method_pack == "deconvolution" for r in rows):
        _ensure_allen_reference_prepared(
            docker=args.docker,
            docker_bin=args.docker_bin,
            image_main=args.image_main,
            gpu=args.gpu,
            dry_run=args.dry_run,
            log_file=log_file,
        )

    # Bootstrap required anchor tables/figures for a fresh checkout.
    skeleton_cmd = ["python3", "scripts/pipeline/run.py", "skeleton"]
    if args.docker:
        _docker_run(skeleton_cmd, docker_bin=args.docker_bin, image=args.image_main, gpu=False, dry_run=args.dry_run, log_file=log_file)
    else:
        _run(skeleton_cmd, cwd=ROOT, dry_run=args.dry_run, log_file=log_file)

    # Always validate contract once at the start.
    validate_cmd = ["python3", "scripts/pipeline/validate_contract.py"]
    if args.docker:
        _docker_run(validate_cmd, docker_bin=args.docker_bin, image=args.image_main, gpu=False, dry_run=args.dry_run, log_file=log_file)
    else:
        _run(validate_cmd, cwd=ROOT, dry_run=args.dry_run, log_file=log_file)

    expanded = 0
    for row in rows:
        seeds = _parse_seed_spec(row.seed_spec)
        for seed in seeds:
            expanded += 1
            if args.max_runs and expanded > args.max_runs:
                print(f"[q1] reached --max-runs={args.max_runs}; stopping")
                break

            extra = shlex.split(row.extra_args) if row.extra_args else []

            if row.task_type == "scrna":
                cmd = [
                    "python3",
                    "scripts/pipeline/run.py",
                    "scrna",
                    "--input-dir",
                    row.input_dir,
                    "--dataset-id",
                    row.dataset_id,
                    "--method-pack",
                    row.method_pack,
                    "--runner",
                    row.runner,
                    "--compute-tier",
                    row.compute_tier,
                    "--seed",
                    str(seed),
                ] + extra

                if args.docker:
                    image = args.image_scvi if row.runner == "scvi" else args.image_main
                    use_gpu = args.gpu and (row.compute_tier == "gpu" or row.runner == "scvi")
                    _docker_run(cmd, docker_bin=args.docker_bin, image=image, gpu=use_gpu, dry_run=args.dry_run, log_file=log_file)
                else:
                    _run(cmd, cwd=ROOT, dry_run=args.dry_run, log_file=log_file)

            elif row.task_type == "visium":
                cmd = [
                    "python3",
                    "scripts/pipeline/run.py",
                    "visium",
                    "--input-dir",
                    row.input_dir,
                    "--dataset-id",
                    row.dataset_id,
                    "--method-pack",
                    row.method_pack,
                    "--runner",
                    row.runner,
                    "--compute-tier",
                    row.compute_tier,
                    "--seed",
                    str(seed),
                ]
                if row.method_pack == "deconvolution":
                    cmd += [
                        "--reference-scrna-dir",
                        "data/references/allen_cortex/prepared/filtered_feature_bc_matrix",
                        "--reference-dataset-id",
                        "Allen_Cortex_scRNA_SeuratV5_Reference_RDS",
                        "--reference-labels-tsv",
                        "data/references/allen_cortex/prepared/reference_labels.tsv",
                    ]
                cmd += extra

                if args.docker:
                    use_gpu = args.gpu and (row.compute_tier == "gpu")
                    _docker_run(cmd, docker_bin=args.docker_bin, image=args.image_main, gpu=use_gpu, dry_run=args.dry_run, log_file=log_file)
                else:
                    _run(cmd, cwd=ROOT, dry_run=args.dry_run, log_file=log_file)
            else:
                raise SystemExit(f"invalid task_type in manifest: {row.task_type!r}")

            # Validate after each run to keep tables schema-correct.
            if args.docker:
                _docker_run(validate_cmd, docker_bin=args.docker_bin, image=args.image_main, gpu=False, dry_run=args.dry_run, log_file=log_file)
            else:
                _run(validate_cmd, cwd=ROOT, dry_run=args.dry_run, log_file=log_file)

        if args.max_runs and expanded >= args.max_runs:
            break

    if args.skip_post:
        print("[q1] done (post steps skipped)")
        return 0

    # Final publication artifacts (figures + review bundle). Keep these in Docker for reproducibility.
    post_cmds = [
        ["python3", "scripts/pipeline/run.py", "figures"],
        ["python3", "scripts/pipeline/run.py", "review-bundle"],
        ["python3", "scripts/pipeline/validate_contract.py"],
    ]
    for cmd in post_cmds:
        if args.docker:
            _docker_run(cmd, docker_bin=args.docker_bin, image=args.image_main, gpu=False, dry_run=args.dry_run, log_file=log_file)
        else:
            _run(cmd, cwd=ROOT, dry_run=args.dry_run, log_file=log_file)

    # Optional: export docx on the host (common on macOS); the Docker image does not include python-docx.
    try:
        import docx  # noqa: F401

        export_cmd = [
            "python3",
            "scripts/manuscript/export_docx.py",
            "--input",
            "docs/MANUSCRIPT_DRAFT.md",
            "--output",
            "output/doc/CloudBioAgent_Manuscript_embedded_figures.docx",
            "--figures",
            "plots/publication/png",
        ]
        _run(export_cmd, cwd=ROOT, dry_run=args.dry_run, log_file=log_file)
    except Exception:
        print("[q1] note: skipping docx export (python-docx not available on this host)")

    print("[q1] OK: suite complete")
    print(f"[q1] figures: plots/publication/png/")
    print(f"[q1] reviewer bundle: docs/review_bundle/review_bundle.zip")
    print(f"[q1] log: {log_file.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
