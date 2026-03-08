#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import tarfile
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


ROOT = Path(__file__).resolve().parents[2]
RUNS_DIR = Path(os.environ.get("CBA_RUNS_DIR", str(ROOT / "runs"))).resolve()
ALLOWED_MOUNT_PREFIXES = [p.strip() for p in os.environ.get("CBA_ALLOWED_MOUNTS", "").split(":") if p.strip()]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def job_paths(run_id: str) -> Dict[str, Path]:
    base = RUNS_DIR / run_id
    return {
        "base": base,
        "uploads": base / "uploads",
        "artifacts": base / "artifacts",
        "status": base / "status.json",
        "job": base / "job.json",
        "log": base / "job.log",
        "workspace": base / "workspace",
    }


def set_status(run_id: str, status: str, extra: Optional[Dict[str, Any]] = None) -> None:
    paths = job_paths(run_id)
    payload: Dict[str, Any] = {"run_id": run_id, "status": status, "updated_at": utc_now_iso()}
    if extra:
        payload.update(extra)
    write_json(paths["status"], payload)

def safe_slug(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return "job"
    s = re.sub(r"[^A-Za-z0-9._-]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s or "job"


def make_run_id(prefix: str) -> str:
    t = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{t}-{safe_slug(prefix)}-{os.urandom(5).hex()}"


def _safe_extract_zip(zip_path: Path, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        for zi in zf.infolist():
            name = zi.filename
            # Basic traversal protections.
            if name.startswith("/") or name.startswith("\\"):
                raise ValueError(f"invalid zip entry (absolute path): {name}")
            norm = Path(name)
            if any(part == ".." for part in norm.parts):
                raise ValueError(f"invalid zip entry (path traversal): {name}")
            # Reject symlinks (common attack vector).
            is_symlink = (zi.external_attr >> 16) & 0o170000 == 0o120000
            if is_symlink:
                raise ValueError(f"invalid zip entry (symlink not allowed): {name}")
        zf.extractall(out_dir)


def _validate_mount_path(p: str) -> Path:
    if not p:
        raise ValueError("empty mount path")
    path = Path(p).expanduser().resolve()
    if not path.is_absolute():
        raise ValueError(f"mount path must be absolute: {p}")
    if not path.exists():
        raise FileNotFoundError(f"mount path not found: {path}")
    if ALLOWED_MOUNT_PREFIXES:
        allowed = [str(Path(pref).expanduser().resolve()) for pref in ALLOWED_MOUNT_PREFIXES]
        if not any(str(path).startswith(a) for a in allowed):
            raise PermissionError(f"mount path not allowed by CBA_ALLOWED_MOUNTS: {path} (allowed: {allowed})")
    return path


def _map_host_to_container(found: Path, host_root: Path, container_root: str) -> str:
    try:
        rel = found.relative_to(host_root)
    except Exception as e:
        raise ValueError(f"cannot map {found} under mount root {host_root}: {e}")
    if str(rel) == ".":
        return container_root
    return str((Path(container_root) / rel).as_posix())


def _download_gcs(uri: str, out_dir: Path, log_path: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    if not uri.startswith("gs://"):
        raise ValueError(f"not a GCS URI: {uri}")
    cmds = [
        (["gsutil", "-m", "cp", "-r", uri, str(out_dir)], "gsutil"),
        (["gcloud", "storage", "cp", "--recursive", uri, str(out_dir)], "gcloud storage (recursive)"),
        (["gcloud", "storage", "cp", uri, str(out_dir)], "gcloud storage (file)"),
    ]
    with log_path.open("a", encoding="utf-8") as logf:
        for cmd, label in cmds:
            try:
                logf.write(f"[runner] {utc_now_iso()} gcs download via {label}: {' '.join(cmd)}\n")
                logf.flush()
                subprocess.check_call(cmd, stdout=logf, stderr=logf)
                return
            except FileNotFoundError:
                continue
            except subprocess.CalledProcessError:
                continue
    raise RuntimeError("failed to download from GCS (need gsutil or gcloud + auth + access)")


def _materialize_gcs_input(uri: str, out_dir: Path, log_path: Path) -> Path:
    _download_gcs(uri, out_dir, log_path)
    zips = [p for p in out_dir.rglob("*.zip") if p.is_file()]
    if len(zips) == 1:
        extracted = out_dir / "extracted"
        _safe_extract_zip(zips[0], extracted)
        return extracted
    return out_dir


def _find_manifest_row(dataset_id: str) -> dict[str, str]:
    import csv

    manifest = ROOT / "data" / "manifest.tsv"
    with manifest.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            if (row.get("dataset_id") or "") == dataset_id:
                return {k: (v or "").strip() for k, v in row.items()}
    raise FileNotFoundError(f"dataset_id not found in data/manifest.tsv: {dataset_id}")


def _predict_entrypoint_from_manifest(dataset_id: str) -> str:
    """
    Predict the pipeline entrypoint path for a manifest dataset_id after
    `scripts/data/fetch_dataset.py --dataset-id <id> --extract`.
    """
    row = _find_manifest_row(dataset_id)
    local_raw = (row.get("local_path") or "").strip()
    modality = (row.get("modality") or "").strip().lower()
    if not local_raw:
        raise ValueError(f"manifest row missing local_path for {dataset_id}")
    locals_ = [p.strip() for p in local_raw.split(";") if p.strip()]
    first = Path(locals_[0])
    parent = first.parent
    if modality == "visium":
        # Visium inputs are placed next to the artifact(s) under the Space Ranger output dir.
        return str(parent.as_posix())
    # Default: scRNA-seq (10x matrix dir under extract root).
    return str((parent / "filtered_feature_bc_matrix").as_posix())


def _find_10x_matrix_dir(root: Path) -> Path:
    required_any = [
        ("matrix.mtx", "features.tsv", "barcodes.tsv"),
        ("matrix.mtx.gz", "features.tsv.gz", "barcodes.tsv.gz"),
        ("matrix.mtx.gz", "features.tsv", "barcodes.tsv"),
        ("matrix.mtx", "features.tsv.gz", "barcodes.tsv"),
        ("matrix.mtx", "genes.tsv", "barcodes.tsv"),
        ("matrix.mtx.gz", "genes.tsv.gz", "barcodes.tsv.gz"),
        ("matrix.mtx.gz", "genes.tsv", "barcodes.tsv"),
        ("matrix.mtx", "genes.tsv.gz", "barcodes.tsv"),
    ]

    def ok(p: Path) -> bool:
        for a, b, c in required_any:
            if (p / a).exists() and (p / b).exists() and (p / c).exists():
                return True
        return False

    # Common: filtered_feature_bc_matrix/
    if (root / "filtered_feature_bc_matrix").is_dir() and ok(root / "filtered_feature_bc_matrix"):
        return root / "filtered_feature_bc_matrix"

    # Otherwise search.
    for p in [root] + [d for d in root.rglob("*") if d.is_dir()]:
        if ok(p):
            return p
    raise FileNotFoundError("Could not locate a 10x filtered_feature_bc_matrix directory in the uploaded zip.")


def _find_visium_spaceranger_dir(root: Path) -> Path:
    # Space Ranger output dir: spatial/ plus matrix (dir or h5)
    for p in [root] + [d for d in root.rglob("*") if d.is_dir()]:
        if not (p / "spatial").is_dir():
            continue
        if (p / "filtered_feature_bc_matrix").is_dir():
            return p
        if list(p.glob("*filtered_feature_bc_matrix.h5")):
            return p
    raise FileNotFoundError("Could not locate a Space Ranger output directory (needs spatial/ + matrix) in the zip.")


def _git_export_to(workspace: Path) -> None:
    if workspace.exists():
        shutil.rmtree(workspace)
    workspace.mkdir(parents=True, exist_ok=True)

    proc = subprocess.Popen(["git", "archive", "HEAD"], cwd=str(ROOT), stdout=subprocess.PIPE)
    assert proc.stdout is not None
    with tarfile.open(fileobj=proc.stdout, mode="r|") as tf:
        tf.extractall(path=str(workspace))
    rc = proc.wait()
    if rc != 0:
        raise RuntimeError(f"git archive failed with code {rc}")
    _overlay_working_tree(workspace)


def _overlay_working_tree(workspace: Path) -> None:
    try:
        out = subprocess.check_output(
            ["git", "ls-files", "-m", "-o", "--exclude-standard"],
            cwd=str(ROOT),
        ).decode("utf-8")
    except Exception:
        return
    for rel in [p.strip() for p in out.splitlines() if p.strip()]:
        src = ROOT / rel
        dst = workspace / rel
        if src.is_dir():
            if dst.exists():
                shutil.rmtree(dst)
            shutil.copytree(src, dst)
        elif src.is_file():
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)


def _docker_image_exists(docker_bin: str, image_tag: str) -> bool:
    try:
        subprocess.check_output([docker_bin, "image", "inspect", image_tag], stderr=subprocess.STDOUT)
        return True
    except Exception:
        return False


def _run_docker_job(
    *,
    docker_bin: str,
    image_tag: str,
    workspace: Path,
    inner_cmd: str,
    log_path: Path,
    mounts: list[tuple[Path, str]],
) -> int:
    uid = os.getuid() if hasattr(os, "getuid") else 1000
    gid = os.getgid() if hasattr(os, "getgid") else 1000
    cmd = [
        docker_bin,
        "run",
        "--rm",
        "-u",
        f"{uid}:{gid}",
        "-v",
        f"{workspace}:/app",
        "-w",
        "/app",
    ]
    for host_path, container_path in mounts:
        cmd += ["-v", f"{host_path}:{container_path}:ro"]
    cmd += [
        image_tag,
        "bash",
        "-lc",
        inner_cmd,
    ]
    with log_path.open("a", encoding="utf-8") as logf:
        logf.write(f"[runner] {utc_now_iso()} docker: {' '.join(cmd)}\n")
        logf.flush()
        proc = subprocess.Popen(cmd, stdout=logf, stderr=logf)
        return int(proc.wait())


def _copy_artifacts(run_id: str, workspace: Path, artifacts_dir: Path, log_path: Path) -> List[str]:
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    copied: List[str] = []

    # Prefer the explicitly named audit zip when present.
    audit_zip = workspace / "docs" / "audit_runs" / f"{run_id}.zip"
    if audit_zip.exists():
        dst = artifacts_dir / audit_zip.name
        shutil.copy2(audit_zip, dst)
        copied.append(dst.name)

    # Also copy any other newly created audit zips.
    for z in sorted((workspace / "docs" / "audit_runs").glob("*.zip")) if (workspace / "docs" / "audit_runs").exists() else []:
        dst = artifacts_dir / z.name
        if dst.exists():
            continue
        shutil.copy2(z, dst)
        copied.append(dst.name)

    # Copy publication figures (PDF/PNG) if present.
    pub = workspace / "plots" / "publication"
    if pub.exists():
        for p in sorted(pub.rglob("*")):
            if not p.is_file():
                continue
            if p.suffix.lower() not in {".pdf", ".png"}:
                continue
            rel = p.relative_to(pub)
            out = artifacts_dir / f"publication_{str(rel).replace(os.sep, '_')}"
            if not out.exists():
                shutil.copy2(p, out)
                copied.append(out.name)

    with log_path.open("a", encoding="utf-8") as logf:
        logf.write(f"[runner] copied artifacts: {copied}\n")
    return copied


def build_inner_cmd(job: Dict[str, Any], *, dataset_dir: str, dataset2_dir: str, ref_dir: str, ref_labels: str) -> str:
    profile = job["profile"]
    dataset_id = job["dataset_id"]
    seed = int(job.get("seed", 0))
    compute_tier = job.get("compute_tier", "cpu")
    organism = job.get("organism", "human")
    tissue = job.get("tissue", "")

    parts: List[str] = []
    parts.append("set -euo pipefail")
    parts.append("mkdir -p logs")
    parts.append(f"echo \"[job] profile={profile} dataset_id={dataset_id} seed={seed} tier={compute_tier}\" | tee -a logs/ui_job.log")
    inputs = job.get("inputs", {})
    for key in ("dataset_manifest_id", "dataset2_manifest_id", "reference_manifest_id"):
        dsid = (inputs.get(key) or "").strip()
        if dsid:
            parts.append(f"python3 scripts/data/fetch_dataset.py --dataset-id {dsid} --extract")
    parts.append("python3 scripts/pipeline/validate_contract.py --skip-figures")

    if profile == "scrna_baseline_scanpy_celltypist":
        parts.append(
            "python3 scripts/pipeline/run.py scrna "
            f"--input-dir {dataset_dir} --dataset-id {dataset_id} "
            "--method-pack baseline --runner scanpy --annotate celltypist "
            f"--seed {seed} --organism {organism} --tissue {tissue!s}"
        )
    elif profile == "scrna_baseline_seurat_v5":
        parts.append(
            "python3 scripts/pipeline/run.py scrna "
            f"--input-dir {dataset_dir} --dataset-id {dataset_id} "
            "--method-pack baseline --runner seurat --annotate none "
            f"--seed {seed} --organism {organism} --tissue {tissue!s}"
        )
    elif profile == "visium_baseline":
        parts.append(
            "python3 scripts/pipeline/run.py visium "
            f"--input-dir {dataset_dir} --dataset-id {dataset_id} "
            "--method-pack baseline "
            f"--seed {seed} --organism {organism} --tissue {tissue!s}"
        )
    elif profile == "visium_deconvolution_rctd_tangram":
        if not ref_dir or not ref_labels:
            raise ValueError("visium deconvolution requires reference_zip and reference_labels_tsv")
        ref_id = job.get("reference_dataset_id", "user_reference")
        parts.append(
            "python3 scripts/pipeline/run.py visium "
            f"--input-dir {dataset_dir} --dataset-id {dataset_id} "
            "--method-pack deconvolution "
            f"--reference-scrna-dir {ref_dir} --reference-dataset-id {ref_id} "
            f"--reference-labels-tsv {ref_labels} "
            f"--compute-tier {compute_tier} --seed {seed} --organism {organism} --tissue {tissue!s}"
        )
    elif profile == "scrna_advanced_scvi":
        if not dataset2_dir:
            raise ValueError("scVI profile requires dataset2_zip (second batch input)")
        input_multi = f"{dataset_dir};{dataset2_dir}"
        parts.append(
            "python3 scripts/pipeline/run.py scrna "
            f"--input-dir {input_multi} --dataset-id {dataset_id} "
            "--method-pack advanced --runner scvi --annotate celltypist "
            f"--compute-tier {compute_tier} --seed {seed} --organism {organism} --tissue {tissue!s}"
        )
    else:
        raise ValueError(f"unknown profile: {profile}")

    parts.append("python3 scripts/pipeline/run.py figures")
    parts.append("python3 scripts/pipeline/validate_contract.py")
    parts.append(
        "python3 scripts/pipeline/run.py audit "
        f"--run-id {job['run_id']} "
        "--include schemas/action_schema_v1.json "
        "--include docs/FIGURE_PROVENANCE.tsv "
        "--include docs/CLAIMS.tsv "
        "--include results "
        "--include plots/publication "
        "--include logs "
        "--include docs/jobs"
    )
    parts.append("echo \"[job] done\" | tee -a logs/ui_job.log")
    return " && ".join(parts)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", help="Existing run_id under runs/<run_id>/ (UI mode).")
    parser.add_argument("--job-json", help="Replay mode: path to a job.json (e.g., extracted from an audit zip).")
    args = parser.parse_args()

    if not args.run_id and not args.job_json:
        raise SystemExit("Provide either --run-id (UI mode) or --job-json (replay mode).")

    if args.job_json:
        job_src = Path(args.job_json).expanduser().resolve()
        if not job_src.exists():
            raise FileNotFoundError(f"--job-json not found: {job_src}")
        job = read_json(job_src)
        run_id = str(job.get("run_id") or "").strip() or make_run_id(job.get("profile") or "replay")
        paths = job_paths(run_id)
        paths["uploads"].mkdir(parents=True, exist_ok=True)
        paths["artifacts"].mkdir(parents=True, exist_ok=True)
        job_path = paths["job"]
        write_json(job_path, job)
        with paths["log"].open("a", encoding="utf-8") as logf:
            logf.write(f"[runner] {utc_now_iso()} replay from job.json: {job_src}\n")
    else:
        run_id = args.run_id
        paths = job_paths(run_id)
        job_path = paths["job"]
        if not job_path.exists():
            raise FileNotFoundError(f"job.json not found for run_id={run_id}")
        job = read_json(job_path)

    docker_bin = job.get("docker", {}).get("docker_bin", "docker")
    image_tag = job.get("docker", {}).get("image_tag", "cloudbiointegrator:smoke")

    set_status(run_id, "running", {"started_at": utc_now_iso()})

    try:
        _git_export_to(paths["workspace"])
        # Include untracked helper scripts required by the job runner.
        for rel in ["scripts/data"]:
            src = ROOT / rel
            dst = paths["workspace"] / rel
            if src.exists() and not dst.exists():
                shutil.copytree(src, dst)

        # Copy job manifest into the workspace for audit.
        jobs_dir = paths["workspace"] / "docs" / "jobs" / run_id
        jobs_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(job_path, jobs_dir / "job.json")

        # Materialize inputs into workspace paths and/or mounts.
        uploads = paths["uploads"]
        dataset_root = paths["workspace"] / "data" / "user_uploads" / run_id / "dataset"
        dataset2_root = paths["workspace"] / "data" / "user_uploads" / run_id / "dataset2"
        ref_root = paths["workspace"] / "data" / "user_uploads" / run_id / "reference"
        mounts: list[tuple[Path, str]] = []

        inputs = job.get("inputs", {})

        dataset_zip = uploads / "dataset.zip"
        dataset_path = (inputs.get("dataset_path") or "").strip()
        dataset_gcs_uri = (inputs.get("dataset_gcs_uri") or "").strip()
        dataset_manifest_id = (inputs.get("dataset_manifest_id") or "").strip()
        dataset_mount_root: Optional[Path] = None
        dataset_mount_container = "/inputs/dataset"
        if dataset_zip.exists() and dataset_zip.stat().st_size > 0:
            _safe_extract_zip(dataset_zip, dataset_root)
        elif dataset_path:
            dataset_mount_root = _validate_mount_path(dataset_path)
            mounts.append((dataset_mount_root, dataset_mount_container))
            dataset_root = dataset_mount_root
        elif dataset_gcs_uri:
            dataset_root = _materialize_gcs_input(dataset_gcs_uri, dataset_root, paths["log"])
        elif dataset_manifest_id:
            dataset_root = paths["workspace"] / _predict_entrypoint_from_manifest(dataset_manifest_id)
        else:
            raise FileNotFoundError("missing dataset input (zip/path/gcs/manifest)")

        dataset2_zip = uploads / "dataset2.zip"
        dataset2_path = (inputs.get("dataset2_path") or "").strip()
        dataset2_gcs_uri = (inputs.get("dataset2_gcs_uri") or "").strip()
        dataset2_manifest_id = (inputs.get("dataset2_manifest_id") or "").strip()
        dataset2_mount_root: Optional[Path] = None
        dataset2_mount_container = "/inputs/dataset2"
        if dataset2_zip.exists() and dataset2_zip.stat().st_size > 0:
            _safe_extract_zip(dataset2_zip, dataset2_root)
        elif dataset2_path:
            dataset2_mount_root = _validate_mount_path(dataset2_path)
            mounts.append((dataset2_mount_root, dataset2_mount_container))
            dataset2_root = dataset2_mount_root
        elif dataset2_gcs_uri:
            dataset2_root = _materialize_gcs_input(dataset2_gcs_uri, dataset2_root, paths["log"])
        elif dataset2_manifest_id:
            dataset2_root = paths["workspace"] / _predict_entrypoint_from_manifest(dataset2_manifest_id)
        else:
            dataset2_root = Path()

        reference_zip = uploads / "reference.zip"
        reference_path = (inputs.get("reference_path") or "").strip()
        reference_gcs_uri = (inputs.get("reference_gcs_uri") or "").strip()
        reference_manifest_id = (inputs.get("reference_manifest_id") or "").strip()
        reference_mount_root: Optional[Path] = None
        reference_mount_container = "/inputs/reference"
        if reference_zip.exists() and reference_zip.stat().st_size > 0:
            _safe_extract_zip(reference_zip, ref_root)
        elif reference_path:
            reference_mount_root = _validate_mount_path(reference_path)
            mounts.append((reference_mount_root, reference_mount_container))
            ref_root = reference_mount_root
        elif reference_gcs_uri:
            ref_root = _materialize_gcs_input(reference_gcs_uri, ref_root, paths["log"])
        elif reference_manifest_id:
            ref_root = paths["workspace"] / _predict_entrypoint_from_manifest(reference_manifest_id)
        else:
            ref_root = Path()

        labels_tsv = uploads / "reference_labels.tsv"
        labels_path = (inputs.get("reference_labels_path") or "").strip()
        labels_gcs_uri = (inputs.get("reference_labels_gcs_uri") or "").strip()
        labels_rel = ""
        if labels_tsv.exists() and labels_tsv.stat().st_size > 0:
            labels_rel = str((Path("data") / "user_uploads" / run_id / "reference_labels.tsv").as_posix())
            shutil.copy2(labels_tsv, paths["workspace"] / labels_rel)
        elif labels_path:
            host = _validate_mount_path(labels_path)
            mounts.append((host, "/inputs/reference_labels.tsv"))
            labels_rel = "/inputs/reference_labels.tsv"
        elif labels_gcs_uri:
            dst = paths["workspace"] / "data" / "user_uploads" / run_id / "reference_labels.tsv"
            _download_gcs(labels_gcs_uri, dst.parent, paths["log"])
            if dst.exists():
                labels_rel = dst.relative_to(paths["workspace"]).as_posix()
            else:
                cands = [p for p in dst.parent.rglob("*") if p.is_file() and p.suffix.lower() in {".tsv", ".txt"}]
                if not cands:
                    raise FileNotFoundError("reference labels not found after GCS download")
                labels_rel = cands[0].relative_to(paths["workspace"]).as_posix()

        # Locate required input dirs (relative to workspace root).
        profile = job["profile"]
        if profile.startswith("scrna_"):
            if dataset_manifest_id:
                d1_rel = _predict_entrypoint_from_manifest(dataset_manifest_id)
            else:
                d1 = _find_10x_matrix_dir(dataset_root)
                if dataset_mount_root is not None:
                    d1_rel = _map_host_to_container(d1, dataset_mount_root, dataset_mount_container)
                else:
                    d1_rel = d1.relative_to(paths["workspace"]).as_posix()
            d2_rel = ""
            if profile == "scrna_advanced_scvi":
                if not dataset2_root:
                    raise ValueError("scVI profile requires dataset2 input (zip/path/gcs)")
                if dataset2_manifest_id:
                    d2_rel = _predict_entrypoint_from_manifest(dataset2_manifest_id)
                else:
                    d2 = _find_10x_matrix_dir(dataset2_root)
                    if dataset2_mount_root is not None:
                        d2_rel = _map_host_to_container(d2, dataset2_mount_root, dataset2_mount_container)
                    else:
                        d2_rel = d2.relative_to(paths["workspace"]).as_posix()
            ref_rel = ""
        else:
            if dataset_manifest_id:
                d1_rel = _predict_entrypoint_from_manifest(dataset_manifest_id)
            else:
                sr = _find_visium_spaceranger_dir(dataset_root)
                if dataset_mount_root is not None:
                    d1_rel = _map_host_to_container(sr, dataset_mount_root, dataset_mount_container)
                else:
                    d1_rel = sr.relative_to(paths["workspace"]).as_posix()
            d2_rel = ""
            ref_rel = ""
            if profile == "visium_deconvolution_rctd_tangram":
                if not ref_root:
                    raise ValueError("visium deconvolution requires reference input (zip/path/gcs)")
                if not labels_rel:
                    raise ValueError("visium deconvolution requires reference labels (file/path/gcs)")
                if reference_manifest_id:
                    ref_rel = _predict_entrypoint_from_manifest(reference_manifest_id)
                else:
                    ref_mat = _find_10x_matrix_dir(ref_root)
                    if reference_mount_root is not None:
                        ref_rel = _map_host_to_container(ref_mat, reference_mount_root, reference_mount_container)
                    else:
                        ref_rel = ref_mat.relative_to(paths["workspace"]).as_posix()

        inner = build_inner_cmd(job, dataset_dir=d1_rel, dataset2_dir=d2_rel, ref_dir=ref_rel, ref_labels=labels_rel)

        if not _docker_image_exists(docker_bin, image_tag):
            raise RuntimeError(
                f"Docker image not found: {image_tag}. Build it first (Dockerfile) or set CBA_IMAGE_TAG."
            )

        rc = _run_docker_job(
            docker_bin=docker_bin,
            image_tag=image_tag,
            workspace=paths["workspace"],
            inner_cmd=inner,
            log_path=paths["log"],
            mounts=mounts,
        )
        copied = _copy_artifacts(run_id, paths["workspace"], paths["artifacts"], paths["log"])

        if rc != 0:
            set_status(run_id, "failed", {"exit_code": rc, "artifacts": copied})
            return rc

        set_status(run_id, "completed", {"exit_code": 0, "artifacts": copied, "completed_at": utc_now_iso()})
        return 0
    except Exception as e:
        set_status(run_id, "failed", {"error": str(e)})
        with paths["log"].open("a", encoding="utf-8") as logf:
            logf.write(f"[runner] ERROR: {e}\n")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
