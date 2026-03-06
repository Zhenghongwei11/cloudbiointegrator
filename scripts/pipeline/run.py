#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import gzip
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.request
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[2]

class ComputeGateError(RuntimeError):
    def __init__(self, *, failure_type: str, message: str):
        super().__init__(message)
        self.failure_type = failure_type


REQUIRED_TABLES: list[Path] = [
    Path("results/dataset_summary.tsv"),
    Path("results/audit/reproducibility_checks.tsv"),
    Path("results/benchmarks/method_benchmark.tsv"),
    Path("results/benchmarks/biological_output_concordance.tsv"),
    Path("results/benchmarks/runtime_cost_failure.tsv"),
    Path("results/benchmarks/robustness_matrix.tsv"),
    Path("results/figures/F1_system_contract.tsv"),
]


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def run_cmd(args: list[str], cwd: Path | None = None) -> tuple[int, str]:
    try:
        out = subprocess.check_output(args, cwd=str(cwd) if cwd else None, stderr=subprocess.STDOUT)
        return 0, out.decode("utf-8", errors="replace").strip()
    except FileNotFoundError as e:
        return 127, str(e)
    except subprocess.CalledProcessError as e:
        out = (e.output or b"").decode("utf-8", errors="replace").strip()
        return e.returncode, out


def git_commit() -> str:
    code, out = run_cmd(["git", "rev-parse", "HEAD"], cwd=ROOT)
    return out if code == 0 else "UNKNOWN"


def env_fingerprint() -> dict[str, Any]:
    return {
        # NOTE: Do not include timestamps here.
        # This fingerprint is hashed into env_hash and should be stable across reruns
        # when the environment is unchanged. Per-run time is already recorded in
        # results/audit/reproducibility_checks.tsv as timestamp_utc.
        "python_version": sys.version.replace("\n", " "),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "uname": " ".join(platform.uname()),
    }


def sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def ensure_parents(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_tsv_row(path: Path, row: dict[str, Any]) -> None:
    ensure_parents(path)
    with path.open("r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="\t")
        header = next(reader)
    with path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header, delimiter="\t", extrasaction="ignore")
        writer.writerow(row)

def tsv_has_value(path: Path, column: str, value: str) -> bool:
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            if (row.get(column) or "") == value:
                return True
    return False


def tsv_last_matching_row(path: Path, column: str, value: str) -> dict[str, str] | None:
    last: dict[str, str] | None = None
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            if (row.get(column) or "") == value:
                last = {k: (v or "") for k, v in row.items()}
    return last


def read_tsv_header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="\t")
        return next(reader)


def stable_json_hash(obj: Any) -> str:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(s).hexdigest()


def make_run_id(prefix: str) -> str:
    t = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    seed = os.urandom(8).hex()
    short = hashlib.sha256(f"{prefix}:{t}:{seed}".encode("utf-8")).hexdigest()[:10]
    return f"{t}-{prefix}-{short}"


def load_action_schema() -> dict[str, Any]:
    path = ROOT / "schemas" / "action_schema_v1.json"
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_action_contract_anchor() -> None:
    path = ROOT / "results" / "figures" / "F1_system_contract.tsv"
    header = read_tsv_header(path)
    schema = load_action_schema()
    actions = schema.get("allowed_actions", [])
    if not actions:
        return
    # Only append if table is still header-only.
    with path.open("r", encoding="utf-8") as f:
        lines = f.readlines()
    if len(lines) <= 1:
        with path.open("a", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=header, delimiter="\t", extrasaction="ignore")
            for act in actions:
                row = {
                    "action_id": act.get("action_id", ""),
                    "action_name": act.get("action_name", ""),
                    "input_artifacts": ";".join(act.get("inputs", [])),
                    "output_tables": ";".join(act.get("outputs", [])),
                    "determinism_controls": ";".join(act.get("determinism_controls", [])),
                    "allowed_methods": ";".join(act.get("allowed_methods", [])),
                    "notes": (act.get("notes", "") + f" | schema_version={schema.get('schema_version','')}").strip(),
                }
                writer.writerow(row)


def build_env_hash(env: dict[str, Any]) -> str:
    return stable_json_hash(env)


def download_file(url: str, dest: Path) -> None:
    ensure_parents(dest)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as r:
        with dest.open("wb") as f:
            shutil.copyfileobj(r, f)


def read_10x_any(dir_path: Path):
    import csv as _csv
    import gzip as _gzip

    import anndata as ad
    import numpy as _np
    from scipy import io as _io
    from scipy import sparse as _sparse

    def _pick(names: list[str]) -> Path:
        for n in names:
            p = dir_path / n
            if p.exists():
                return p
        raise FileNotFoundError(f"missing required 10x file in {dir_path}: one of {names}")

    mtx = _pick(["matrix.mtx.gz", "matrix.mtx"])
    feats = _pick(["features.tsv.gz", "features.tsv", "genes.tsv.gz", "genes.tsv"])
    bcs = _pick(["barcodes.tsv.gz", "barcodes.tsv"])

    def _open_text(p: Path):
        if p.name.endswith(".gz"):
            return _gzip.open(p, "rt", encoding="utf-8")
        return p.open("r", encoding="utf-8")

    def _open_bin(p: Path):
        if p.name.endswith(".gz"):
            return _gzip.open(p, "rb")
        return p.open("rb")

    with _open_bin(mtx) as f:
        mat = _io.mmread(f)
    if not _sparse.issparse(mat):
        mat = _sparse.csr_matrix(mat)
    # 10x: genes x cells -> AnnData expects cells x genes
    X = mat.T.tocsr()

    gene_ids: list[str] = []
    gene_symbols: list[str] = []
    with _open_text(feats) as f:
        reader = _csv.reader(f, delimiter="\t")
        for row in reader:
            if not row:
                continue
            if len(row) == 1:
                gene_ids.append(row[0])
                gene_symbols.append(row[0])
            else:
                gene_ids.append(row[0])
                gene_symbols.append(row[1])

    barcodes: list[str] = []
    with _open_text(bcs) as f:
        for line in f:
            line = line.strip()
            if line:
                barcodes.append(line)

    if X.shape[0] != len(barcodes):
        raise ValueError(f"10x barcodes mismatch: X has {X.shape[0]} cells but barcodes has {len(barcodes)}")
    if X.shape[1] != len(gene_symbols):
        raise ValueError(f"10x features mismatch: X has {X.shape[1]} genes but features has {len(gene_symbols)}")

    adata_local = ad.AnnData(X=X)
    adata_local.obs_names = _np.array(barcodes, dtype=str)
    adata_local.var_names = _np.array(gene_symbols, dtype=str)
    adata_local.var["gene_id"] = _np.array(gene_ids, dtype=str)
    return adata_local


def extract_tar_gz(archive: Path, dest_dir: Path) -> None:
    ensure_parents(dest_dir / "x")
    with tarfile.open(archive, "r:gz") as tf:
        tf.extractall(dest_dir)


def open_text_maybe_gz(path: Path) -> Iterable[str]:
    if path.name.endswith(".gz"):
        with gzip.open(path, "rt", encoding="utf-8", errors="replace") as f:
            for line in f:
                yield line.rstrip("\n")
    else:
        with path.open("r", encoding="utf-8", errors="replace") as f:
            for line in f:
                yield line.rstrip("\n")


def parse_mtx_dimensions(matrix_mtx_gz: Path) -> tuple[int, int, int]:
    n_rows = n_cols = n_entries = 0
    it = iter(open_text_maybe_gz(matrix_mtx_gz))
    for line in it:
        if not line.startswith("%"):
            parts = line.split()
            if len(parts) >= 3:
                n_rows, n_cols, n_entries = (int(parts[0]), int(parts[1]), int(parts[2]))
            break
    return n_rows, n_cols, n_entries


def compute_basic_10x_stats(matrix_dir: Path) -> dict[str, Any]:
    matrix = matrix_dir / "matrix.mtx.gz"
    features = matrix_dir / "features.tsv.gz"
    barcodes = matrix_dir / "barcodes.tsv.gz"
    if not matrix.exists():
        matrix = matrix_dir / "matrix.mtx"
    if not features.exists():
        features = matrix_dir / "features.tsv"
    if not barcodes.exists():
        barcodes = matrix_dir / "barcodes.tsv"
    if not (matrix.exists() and features.exists() and barcodes.exists()):
        raise FileNotFoundError(f"Expected 10x matrix files under: {matrix_dir}")

    n_genes = sum(1 for _ in open_text_maybe_gz(features))
    n_cells = sum(1 for _ in open_text_maybe_gz(barcodes))
    n_rows, n_cols, n_entries = parse_mtx_dimensions(matrix)

    return {
        "n_cells_or_spots": n_cells,
        "n_genes": n_genes,
        "matrix_n_rows": n_rows,
        "matrix_n_cols": n_cols,
        "matrix_n_entries": n_entries,
    }


def write_repro_check(
    *,
    run_id: str,
    dataset_id: str,
    stage: str,
    env_hash: str,
    seed: int,
    output_table_path: str,
    notes: str = "",
    pass_flag: bool = True,
    fail_reason: str = "",
    wall_time_s: float | None = None,
    peak_ram_gb: float | None = None,
) -> None:
    out_path = ROOT / output_table_path
    output_sha = sha256_path(out_path) if out_path.exists() else ""
    row = {
        "run_id": run_id,
        "timestamp_utc": utc_now_iso(),
        "dataset_id": dataset_id,
        "stage": stage,
        "env_hash": env_hash,
        "container_image": "",
        "git_commit": git_commit(),
        "seed": seed,
        "action_schema_version": "v1",
        "params_hash": "",
        "output_table_path": output_table_path,
        "output_sha256": output_sha,
        "pass": "1" if pass_flag else "0",
        "fail_reason": fail_reason,
        "wall_time_s": f"{wall_time_s:.3f}" if wall_time_s is not None else "",
        "peak_ram_gb": f"{peak_ram_gb:.3f}" if peak_ram_gb is not None else "",
        "notes": notes,
    }
    write_tsv_row(ROOT / "results" / "audit" / "reproducibility_checks.tsv", row)


def rscript_available() -> bool:
    code, _ = run_cmd(["Rscript", "--version"])
    return code == 0


def run_figures(outdir: str) -> None:
    frozen_dir = ROOT / outdir / "frozen"
    frozen_f1_pdf = frozen_dir / "pdf" / "F1_system_contract.pdf"
    frozen_f1_png = frozen_dir / "png" / "F1_system_contract.png"
    use_frozen_f1 = frozen_f1_pdf.exists() and frozen_f1_png.exists()
    frozen_f4_pdf = frozen_dir / "pdf" / "F4_spatial_benchmark.pdf"
    frozen_f4_png = frozen_dir / "png" / "F4_spatial_benchmark.png"
    use_frozen_f4 = frozen_f4_pdf.exists() and frozen_f4_png.exists()

    scripts = [
        # F1 can be frozen to preserve the finalized schematic across rebuilds.
        ("python3", ["scripts/figures/F1_roadmap.py", "--outdir", outdir])
        if not use_frozen_f1
        else ("python3", ["-c", "print('INFO: using frozen F1 from plots/publication/frozen/')"]),
        ("Rscript", ["scripts/figures/F2_reproducibility.R", f"--outdir={outdir}"]),
        ("Rscript", ["scripts/figures/F3_scrna_benchmark.R", f"--outdir={outdir}"]),
        # F4 can be frozen to prevent accidental style regressions during figure iteration.
        (
            "Rscript",
            ["scripts/figures/F4_spatial_benchmark.R", f"--outdir={outdir}"],
        )
        if not use_frozen_f4
        else ("python3", ["-c", "print('INFO: using frozen F4 from plots/publication/frozen/')"]),
        ("Rscript", ["scripts/figures/F5_ops_benchmark.R", f"--outdir={outdir}"]),
        ("Rscript", ["scripts/figures/F6_robustness_matrix.R", f"--outdir={outdir}"]),
    ]
    for tool, cmd in scripts:
        if tool == "Rscript" and not rscript_available():
            # Allow Python-only images (e.g., GPU-tier method images) to run the pipeline and
            # still emit benchmark/audit artifacts. R figures can be generated later using
            # the main (R-enabled) image.
            print("WARN: Rscript not found; skipping R figure scripts in this environment.")
            continue
        code, out = run_cmd([tool, *cmd], cwd=ROOT)
        if code != 0:
            raise RuntimeError(f"Figure script failed: {tool} {' '.join(cmd)}\n{out}")

    if use_frozen_f4:
        (ROOT / outdir / "pdf").mkdir(parents=True, exist_ok=True)
        (ROOT / outdir / "png").mkdir(parents=True, exist_ok=True)
        shutil.copy2(frozen_f4_pdf, ROOT / outdir / "pdf" / "F4_spatial_benchmark.pdf")
        shutil.copy2(frozen_f4_png, ROOT / outdir / "png" / "F4_spatial_benchmark.png")

    if use_frozen_f1:
        (ROOT / outdir / "pdf").mkdir(parents=True, exist_ok=True)
        (ROOT / outdir / "png").mkdir(parents=True, exist_ok=True)
        shutil.copy2(frozen_f1_pdf, ROOT / outdir / "pdf" / "F1_system_contract.pdf")
        shutil.copy2(frozen_f1_png, ROOT / outdir / "png" / "F1_system_contract.png")


def build_audit_bundle(run_id: str, include_paths: list[str]) -> Path:
    script = ROOT / "scripts" / "audit" / "build_audit_bundle.py"
    args = ["python3", str(script), "--run-id", run_id]
    for p in include_paths:
        args += ["--include", p]
    code, out = run_cmd(args, cwd=ROOT)
    if code != 0:
        raise RuntimeError(f"audit bundle failed:\n{out}")
    bundle_dir = ROOT / "docs" / "audit_runs" / run_id
    return bundle_dir


def cmd_skeleton(_: argparse.Namespace) -> int:
    run_id = make_run_id("skeleton")
    seed = 0
    env = env_fingerprint()
    env_hash = build_env_hash(env)

    t0 = time.time()
    for rel in REQUIRED_TABLES:
        abs_path = ROOT / rel
        if not abs_path.exists():
            raise FileNotFoundError(f"Missing required table (expected in repo): {rel}")

    write_action_contract_anchor()
    run_figures(outdir="plots/publication")

    # Minimal audit bundle + reproducibility record.
    build_audit_bundle(
        run_id,
        include_paths=[
            "schemas/action_schema_v1.json",
            "docs/FIGURE_PROVENANCE.tsv",
            "docs/CLAIMS.tsv",
            "results",
            "plots/publication",
            "logs",
        ],
    )
    wall = time.time() - t0
    write_repro_check(
        run_id=run_id,
        dataset_id="NA",
        stage="skeleton",
        env_hash=env_hash,
        seed=seed,
        output_table_path="results/figures/F1_system_contract.tsv",
        notes="skeleton contract validation + placeholder figures",
        pass_flag=True,
        wall_time_s=wall,
    )
    print(f"OK: skeleton run_id={run_id}")
    return 0


def cmd_smoke(args: argparse.Namespace) -> int:
    run_id = make_run_id("smoke")
    seed = 0
    env = env_fingerprint()
    env_hash = build_env_hash(env)

    dataset_id = args.dataset_id or "SMOKE_TOY_10X_MTX"
    url = args.url or ""

    t0 = time.time()
    tmp_root = Path(tempfile.mkdtemp(prefix="smoke_10x_"))
    try:
        if args.input_dir:
            matrix_dir = (ROOT / args.input_dir).resolve()
            if not matrix_dir.exists():
                raise FileNotFoundError(f"--input-dir not found: {matrix_dir}")
        else:
            if not url:
                raise ValueError("smoke requires --input-dir or --url")
            archive = tmp_root / "pbmc_3k_filtered_feature_bc_matrix.tar.gz"
            extract_dir = tmp_root / "extracted"
            download_file(url, archive)
            extract_tar_gz(archive, extract_dir)

            # The tarball contains a single folder named filtered_feature_bc_matrix/
            matrix_dir = extract_dir / "filtered_feature_bc_matrix"
            if not matrix_dir.exists():
                candidates = list(extract_dir.glob("**/filtered_feature_bc_matrix"))
                if candidates:
                    matrix_dir = candidates[0]
        stats = compute_basic_10x_stats(matrix_dir)

        ds_row = {
            "dataset_id": dataset_id,
            "modality": "scRNA-seq",
            "organism": "human",
            "tissue": "PBMC",
            "assay_platform": "10x Chromium",
            "input_artifact": "filtered_feature_bc_matrix/",
            "entrypoint": "cellranger_mtx",
            "role": "smoke",
            "n_samples": "",
            "n_donors": "",
            "n_cells_or_spots": stats["n_cells_or_spots"],
            "n_genes": stats["n_genes"],
            "reference_genome": "",
            "primary_citation": "",
            "source_url": url,
            "license": "",
            "qc_summary": f"mtx_rows={stats['matrix_n_rows']};mtx_cols={stats['matrix_n_cols']};mtx_nnz={stats['matrix_n_entries']}",
            "notes": "downloaded by smoke run; no heavy compute",
        }
        write_tsv_row(ROOT / "results" / "dataset_summary.tsv", ds_row)

        wall = time.time() - t0
        runtime_row = {
            "dataset_id": dataset_id,
            "modality": "scRNA-seq",
            "method_id": "python-ingest",
            "run_id": run_id,
            "status": "ok",
            "failure_type": "",
            "wall_time_s": f"{wall:.3f}",
            "peak_ram_gb": "",
            "peak_disk_gb": "",
            "cpu_hours": "",
            "gpu_hours": "",
            "estimated_cost_usd": "",
            "cost_model": "",
            "notes": "smoke ingest only",
        }
        write_tsv_row(ROOT / "results" / "benchmarks" / "runtime_cost_failure.tsv", runtime_row)

        write_repro_check(
            run_id=run_id,
            dataset_id=dataset_id,
            stage="smoke_ingest",
            env_hash=env_hash,
            seed=seed,
            output_table_path="results/dataset_summary.tsv",
            notes="smoke ingest of 10x matrix; writes dataset_summary + runtime table",
            pass_flag=True,
            wall_time_s=wall,
        )

        write_action_contract_anchor()
        run_figures(outdir=args.outdir)
        build_audit_bundle(
            run_id,
            include_paths=[
                "schemas/action_schema_v1.json",
                "docs/FIGURE_PROVENANCE.tsv",
                "results/dataset_summary.tsv",
                "results/audit/reproducibility_checks.tsv",
                "results/benchmarks/runtime_cost_failure.tsv",
                "plots/publication",
                "logs",
            ],
        )
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)

    print(f"OK: smoke run_id={run_id}")
    return 0


def cmd_figures(args: argparse.Namespace) -> int:
    write_action_contract_anchor()
    run_figures(outdir=args.outdir)
    print("OK: figures")
    return 0


def cmd_audit(args: argparse.Namespace) -> int:
    run_id = args.run_id or make_run_id("audit")
    build_audit_bundle(run_id, include_paths=args.include)
    print(f"OK: audit bundle run_id={run_id}")
    return 0


def cmd_review_bundle(args: argparse.Namespace) -> int:
    # Reviewer-facing bundle lives under docs/review_bundle/ (spec requirement).
    # We keep an audit bundle too (for consistency), but the review bundle is the handoff artifact.
    run_id = args.run_id or make_run_id("review")
    build_audit_bundle(run_id, include_paths=args.include)

    # Build/refresh docs/review_bundle/review_bundle.zip from the local submission audit set + figures + tables.
    # Use overwrite to make the command idempotent in CI/reviewer reruns.
    code, out = run_cmd([sys.executable, "scripts/audit/build_review_bundle.py", "--overwrite"], cwd=ROOT)
    if code != 0:
        raise RuntimeError(f"review-bundle build failed (exit={code}):\n{out}")

    print(f"OK: review bundle run_id={run_id} (see docs/review_bundle/)")
    return 0


def _method_version_safe(pkgs: list[str]) -> str:
    try:
        from importlib import metadata

        parts: list[str] = []
        for p in pkgs:
            try:
                parts.append(f"{p}={metadata.version(p)}")
            except metadata.PackageNotFoundError:
                parts.append(f"{p}=MISSING")
        return ";".join(parts)
    except Exception:
        return "UNKNOWN"


def _run_scrna_pack_scanpy(
    matrix_dir: Path,
    seed: int,
    annotate: str,
) -> dict[str, Any]:
    # Import only when needed so the base repo remains lightweight.
    try:
        import numpy as np
        import pandas as pd
        import scanpy as sc
    except Exception as e:
        raise RuntimeError(
            "Missing scRNA method-pack dependencies. "
            "Run via Docker (recommended) or install python deps.\n"
            f"Import error: {e}"
        )

    rng = np.random.default_rng(seed)

    # Read 10x matrix (supports gz or plain files; v1 and v3 layouts)
    adata = read_10x_any(matrix_dir)
    try:
        adata.var_names_make_unique()
    except Exception:
        pass

    # QC annotations
    adata.var["mt"] = adata.var_names.str.upper().str.startswith("MT-")
    sc.pp.calculate_qc_metrics(adata, qc_vars=["mt"], inplace=True)

    # Deterministic-ish: shuffle cells with fixed seed before filtering (stable ordering)
    perm = rng.permutation(adata.n_obs)
    adata = adata[perm, :].copy()

    # Basic QC defaults (v0). These will later be parameterized + audited.
    min_genes = 200
    max_pct_mt = 20.0
    sc.pp.filter_cells(adata, min_genes=min_genes)
    adata = adata[adata.obs["pct_counts_mt"] <= max_pct_mt, :].copy()
    sc.pp.filter_genes(adata, min_cells=3)

    # Preserve raw counts for methods that require them (e.g., seurat_v3 HVGs).
    try:
        adata.layers["counts"] = adata.X.copy()
    except Exception:
        pass

    # Normalize + HVGs + embed + cluster
    sc.pp.normalize_total(adata, target_sum=1e4)
    sc.pp.log1p(adata)

    # Annotation expects log1p-normalized expression (not scaled). Use a copy before HVG/scale.
    adata_for_anno = adata.copy()

    # Use cell_ranger HVGs to avoid compiled optional deps (e.g., scikit-misc for seurat_v3).
    sc.pp.highly_variable_genes(adata, flavor="cell_ranger", n_top_genes=2000, layer="counts")
    adata = adata[:, adata.var["highly_variable"]].copy()
    sc.pp.scale(adata, max_value=10)
    sc.tl.pca(adata, svd_solver="arpack", random_state=seed)
    sc.pp.neighbors(adata, n_neighbors=15, n_pcs=30, random_state=seed)
    sc.tl.leiden(adata, resolution=0.5, random_state=seed, key_added="cluster")
    sc.tl.umap(adata, random_state=seed)

    clusters = adata.obs["cluster"].astype(str)
    n_clusters = int(clusters.nunique())

    # Robustness: clustering stability under a seed perturbation (same graph, different Leiden init).
    try:
        from sklearn.metrics import adjusted_rand_score

        sc.tl.leiden(adata, resolution=0.5, random_state=seed + 1, key_added="cluster_seed_plus_1")
        clusters2 = adata.obs["cluster_seed_plus_1"].astype(str)
        ari_seed = float(adjusted_rand_score(clusters, clusters2))
    except Exception:
        ari_seed = float("nan")

    metrics: dict[str, Any] = {
        "n_cells_after_qc": int(adata.n_obs),
        "n_genes_after_filter": int(adata.n_vars),
        "median_total_counts": float(pd.to_numeric(adata.obs["total_counts"]).median()),
        "median_n_genes_by_counts": float(pd.to_numeric(adata.obs["n_genes_by_counts"]).median()),
        "median_pct_counts_mt": float(pd.to_numeric(adata.obs["pct_counts_mt"]).median()),
        "n_clusters": n_clusters,
        "qc_min_genes": min_genes,
        "qc_max_pct_mt": max_pct_mt,
    }

    anno_summary: dict[str, Any] = {}
    concordance: dict[str, Any] = {"ari_cluster_seed_plus_1": ari_seed}
    if annotate == "celltypist":
        try:
            import celltypist
            from sklearn.metrics import normalized_mutual_info_score

            # Avoid downloading the full CellTypist model zoo: prefer a single local model file.
            model_path = ROOT / "data" / "references" / "celltypist" / "Immune_All_Low.pkl"
            if not model_path.exists():
                fetcher = ROOT / "scripts" / "data" / "fetch_celltypist_model.py"
                code, out = run_cmd(["python3", str(fetcher), "--model", "Immune_All_Low.pkl"], cwd=ROOT)
                if code != 0:
                    raise RuntimeError(f"failed to fetch CellTypist model:\n{out}")

            pred = celltypist.annotate(adata_for_anno, model=str(model_path), majority_voting=True)
            labels_obj = pred.predicted_labels
            # CellTypist may return a Series or a DataFrame depending on settings/version.
            if hasattr(labels_obj, "ndim") and getattr(labels_obj, "ndim") == 2:
                try:
                    cols = list(getattr(labels_obj, "columns", []))
                    if "majority_voting" in cols:
                        labels_obj = labels_obj["majority_voting"]
                    elif "predicted_labels" in cols:
                        labels_obj = labels_obj["predicted_labels"]
                    else:
                        labels_obj = labels_obj.iloc[:, 0]
                except Exception:
                    labels_obj = labels_obj.iloc[:, 0]
            labels = labels_obj.astype(str)
            anno_summary = {
                "n_cell_types_pred": int(getattr(labels, "nunique", lambda: len(set(labels)) )()),
            }
            try:
                concordance["nmi_celltypist_vs_cluster"] = float(
                    normalized_mutual_info_score(labels, clusters)
                )
            except Exception:
                concordance["nmi_celltypist_vs_cluster"] = float("nan")
        except Exception as e:
            anno_summary = {"annotation_error": str(e)}

    return {
        "metrics": metrics,
        "annotation": anno_summary,
        "concordance": concordance,
        "versions": {
            "scanpy_stack": _method_version_safe(["scanpy", "anndata", "numpy", "scipy", "pandas", "scikit-learn"]),
            "annotation_stack": _method_version_safe(["celltypist"]) if annotate == "celltypist" else "",
        },
    }


def _run_scrna_pack_seurat(
    matrix_dir: Path,
    seed: int,
) -> dict[str, Any]:
    # Run a pinned Seurat baseline via Rscript and return a summary JSON.
    rscript = shutil.which("Rscript")
    if not rscript:
        raise RuntimeError("Rscript not found; required for Seurat baseline runner.")

    script = ROOT / "scripts" / "methods" / "scrna_seurat_v5.R"
    if not script.exists():
        raise FileNotFoundError(f"missing runner script: {script}")

    with tempfile.TemporaryDirectory(prefix="seurat_run_") as td:
        out_json = Path(td) / "seurat_summary.json"
        cmd = [
            rscript,
            str(script),
            f"--input-dir={str(matrix_dir)}",
            f"--out-json={str(out_json)}",
            f"--seed={seed}",
        ]
        code, out = run_cmd(cmd, cwd=ROOT)
        if code != 0:
            raise RuntimeError(f"Seurat runner failed:\n{out}")
        try:
            data = json.loads(out_json.read_text(encoding="utf-8"))
        except Exception as e:
            raise RuntimeError(f"failed to parse Seurat runner output JSON: {e}")
        return data


def _mean_median_neighbor_batch_mixing(adata, batch_key: str) -> tuple[float, float]:
    try:
        import numpy as np
    except Exception as e:
        raise RuntimeError(f"missing numpy for mixing score: {e}")
    if "connectivities" not in adata.obsp:
        raise ValueError("missing connectivities; run neighbors first")
    conn = adata.obsp["connectivities"].tocsr()
    batches = adata.obs[batch_key].astype(str).to_numpy()
    fracs: list[float] = []
    for i in range(conn.shape[0]):
        start = conn.indptr[i]
        end = conn.indptr[i + 1]
        neigh = conn.indices[start:end]
        if neigh.size == 0:
            continue
        # Exclude self if present.
        neigh = neigh[neigh != i]
        if neigh.size == 0:
            continue
        fracs.append(float(np.mean(batches[neigh] != batches[i])))
    if not fracs:
        return float("nan"), float("nan")
    return float(np.mean(fracs)), float(np.median(fracs))

def _mean_neighbor_label_purity(adata, label_key: str) -> float:
    """
    Mean fraction of neighbors that share the cell's label.
    Uses the current neighbors graph in adata.obsp["connectivities"].
    """
    try:
        import numpy as np
    except Exception as e:
        raise RuntimeError(f"missing numpy for label purity: {e}")
    if "connectivities" not in adata.obsp:
        raise ValueError("missing connectivities; run neighbors first")
    if label_key not in adata.obs:
        return float("nan")

    conn = adata.obsp["connectivities"].tocsr()
    labels = adata.obs[label_key].astype(str).to_numpy()
    fracs: list[float] = []
    for i in range(conn.shape[0]):
        start = conn.indptr[i]
        end = conn.indptr[i + 1]
        neigh = conn.indices[start:end]
        if neigh.size == 0:
            continue
        neigh = neigh[neigh != i]
        if neigh.size == 0:
            continue
        fracs.append(float(np.mean(labels[neigh] == labels[i])))
    if not fracs:
        return float("nan")
    return float(np.mean(fracs))


def _scrna_multi_batch_harmony_compare(
    input_dirs: list[Path],
    batch_ids: list[str],
    seed: int,
    annotate: str,
) -> dict[str, Any]:
    try:
        import numpy as np
        import pandas as pd
        import scanpy as sc
    except Exception as e:
        raise RuntimeError(
            "Missing scRNA integration dependencies. Run via Docker (recommended) or install python deps.\n"
            f"Import error: {e}"
        )

    try:
        import anndata as ad
    except Exception as e:
        raise RuntimeError(f"missing anndata: {e}")

    if len(input_dirs) != len(batch_ids):
        raise ValueError("input_dirs and batch_ids length mismatch")
    if len(input_dirs) < 2:
        raise ValueError("integration compare requires >=2 batches")

    adatas = []
    for p, bid in zip(input_dirs, batch_ids):
        a = read_10x_any(p)
        try:
            a.var_names_make_unique()
        except Exception:
            pass
        a.obs["batch"] = bid
        adatas.append(a)

    adata = ad.concat(adatas, join="outer", merge="first", label="batch", keys=batch_ids, index_unique="-")
    try:
        adata.var_names_make_unique()
    except Exception:
        pass

    # QC annotations
    adata.var["mt"] = adata.var_names.str.upper().str.startswith("MT-")
    sc.pp.calculate_qc_metrics(adata, qc_vars=["mt"], inplace=True)

    # QC defaults (v0)
    min_genes = 200
    max_pct_mt = 20.0
    sc.pp.filter_cells(adata, min_genes=min_genes)
    adata = adata[adata.obs["pct_counts_mt"] <= max_pct_mt, :].copy()
    sc.pp.filter_genes(adata, min_cells=3)

    # Preserve counts and normalize/log
    try:
        adata.layers["counts"] = adata.X.copy()
    except Exception:
        pass
    sc.pp.normalize_total(adata, target_sum=1e4)
    sc.pp.log1p(adata)
    adata_for_anno = adata.copy()

    labels = None
    anno_summary: dict[str, Any] = {}
    if annotate == "celltypist":
        try:
            import celltypist

            model_path = ROOT / "data" / "references" / "celltypist" / "Immune_All_Low.pkl"
            if not model_path.exists():
                fetcher = ROOT / "scripts" / "data" / "fetch_celltypist_model.py"
                code, out = run_cmd(["python3", str(fetcher), "--model", "Immune_All_Low.pkl"], cwd=ROOT)
                if code != 0:
                    raise RuntimeError(f"failed to fetch CellTypist model:\n{out}")
            pred = celltypist.annotate(adata_for_anno, model=str(model_path), majority_voting=True)
            labels_obj = pred.predicted_labels
            if hasattr(labels_obj, "ndim") and getattr(labels_obj, "ndim") == 2:
                labels_obj = labels_obj.iloc[:, 0]
            labels = labels_obj.astype(str)
            adata.obs["celltypist_label"] = labels.to_numpy()
            anno_summary = {"n_cell_types_pred": int(getattr(labels, "nunique", lambda: len(set(labels)))())}
        except Exception as e:
            anno_summary = {"annotation_error": str(e)}

    # HVGs from counts
    sc.pp.highly_variable_genes(adata, flavor="cell_ranger", n_top_genes=2000, layer="counts")
    adata = adata[:, adata.var["highly_variable"]].copy()
    sc.pp.scale(adata, max_value=10)
    sc.tl.pca(adata, svd_solver="arpack", random_state=seed)

    # Baseline: use PCA directly
    baseline_t0 = time.time()
    sc.pp.neighbors(adata, n_neighbors=15, n_pcs=30, use_rep="X_pca", random_state=seed)
    sc.tl.leiden(adata, resolution=0.5, random_state=seed, key_added="cluster_baseline")
    sc.tl.umap(adata, random_state=seed)
    try:
        adata.obsm["X_umap_baseline"] = np.asarray(adata.obsm["X_umap"]).copy()
    except Exception:
        pass
    base_mix_mean, base_mix_median = _mean_median_neighbor_batch_mixing(adata, "batch")
    base_purity = _mean_neighbor_label_purity(adata, "celltypist_label") if labels is not None else float("nan")
    baseline_wall = time.time() - baseline_t0

    # Harmony: correct PCA with batch covariate, then re-run neighbors/cluster.
    harmony_t0 = time.time()
    try:
        import harmonypy as hm
    except Exception as e:
        raise RuntimeError(f"missing harmonypy: {e}")

    meta = adata.obs[["batch"]].copy()
    pca = np.asarray(adata.obsm["X_pca"])
    try:
        ho = hm.run_harmony(pca, meta, vars_use=["batch"], random_state=seed)
    except TypeError:
        ho = hm.run_harmony(pca, meta, vars_use=["batch"])
    z = np.asarray(ho.Z_corr)
    if z.shape[0] == pca.shape[0]:
        z_corr = z
    elif z.shape[1] == pca.shape[0]:
        z_corr = z.T
    else:
        raise ValueError(f"unexpected harmony Z_corr shape={z.shape} for pca shape={pca.shape}")
    adata.obsm["X_pca_harmony"] = z_corr

    sc.pp.neighbors(adata, n_neighbors=15, use_rep="X_pca_harmony", random_state=seed)
    sc.tl.leiden(adata, resolution=0.5, random_state=seed, key_added="cluster_harmony")
    # UMAP on harmony neighbors; do not overwrite baseline embedding.
    sc.tl.umap(adata, random_state=seed, neighbors_key=None)
    try:
        adata.obsm["X_umap_harmony"] = np.asarray(adata.obsm["X_umap"]).copy()
        if "X_umap_baseline" in adata.obsm:
            adata.obsm["X_umap"] = np.asarray(adata.obsm["X_umap_baseline"]).copy()
    except Exception:
        pass
    harm_mix_mean, harm_mix_median = _mean_median_neighbor_batch_mixing(adata, "batch")
    harm_purity = _mean_neighbor_label_purity(adata, "celltypist_label") if labels is not None else float("nan")
    harmony_wall = time.time() - harmony_t0

    # Cluster concordance between methods
    try:
        from sklearn.metrics import adjusted_rand_score

        ari_clusters = float(
            adjusted_rand_score(adata.obs["cluster_baseline"].astype(str), adata.obs["cluster_harmony"].astype(str))
        )
    except Exception:
        ari_clusters = float("nan")

    return {
        "n_cells_after_qc": int(adata.n_obs),
        "n_genes_after_filter": int(adata.n_vars),
        "n_batches": int(pd.Series(adata.obs["batch"].astype(str)).nunique()),
        "n_clusters_baseline": int(pd.Series(adata.obs["cluster_baseline"].astype(str)).nunique()),
        "n_clusters_harmony": int(pd.Series(adata.obs["cluster_harmony"].astype(str)).nunique()),
        "batch_mixing_baseline_mean": base_mix_mean,
        "batch_mixing_baseline_median": base_mix_median,
        "batch_mixing_harmony_mean": harm_mix_mean,
        "batch_mixing_harmony_median": harm_mix_median,
        "label_purity_baseline_mean": base_purity,
        "label_purity_harmony_mean": harm_purity,
        "ari_clusters_baseline_vs_harmony": ari_clusters,
        "wall_baseline_s": baseline_wall,
        "wall_harmony_s": harmony_wall,
        "annotation": anno_summary,
        "versions": {
            "scanpy_stack": _method_version_safe(["scanpy", "anndata", "numpy", "scipy", "pandas", "scikit-learn"]),
            "harmonypy": _method_version_safe(["harmonypy"]),
            "celltypist": _method_version_safe(["celltypist"]) if annotate == "celltypist" else "",
        },
        "notes": f"qc_min_genes={min_genes};qc_max_pct_mt={max_pct_mt};hvg_flavor=cell_ranger",
    }


def _scrna_multi_batch_scvi_compare(
    *,
    input_dirs: list[Path],
    batch_ids: list[str],
    seed: int,
    annotate: str,
    compute_tier: str,
    scvi_n_hvg: int,
    scvi_n_latent: int,
    scvi_max_epochs: int,
    scvi_max_cells: int,
) -> dict[str, Any]:
    """
    Multi-batch integration compare: baseline (PCA) vs Harmony vs scVI (latent).
    Includes scVI robustness perturbation HVG/2 (ARI between clusterings).
    """
    # Fail fast when scVI is requested but deps/compute are missing.
    try:
        import torch
    except Exception as e:
        raise ComputeGateError(failure_type="missing_dependency", message=f"scVI requires torch; import failed: {e}")
    try:
        import scvi
    except Exception as e:
        raise ComputeGateError(failure_type="missing_dependency", message=f"scVI requires scvi-tools; import failed: {e}")

    if compute_tier == "gpu":
        if not getattr(torch, "cuda", None) or not torch.cuda.is_available():
            raise ComputeGateError(
                failure_type="missing_gpu",
                message="--compute-tier gpu requested but torch.cuda.is_available()==False (no GPU visible to the container).",
            )
    elif compute_tier != "cpu":
        raise ValueError(f"unexpected compute_tier={compute_tier}")

    try:
        import numpy as np
        import pandas as pd
        import scanpy as sc
    except Exception as e:
        raise RuntimeError(
            "Missing scRNA integration dependencies. Run via Docker (recommended) or install python deps.\n"
            f"Import error: {e}"
        )

    try:
        import anndata as ad
    except Exception as e:
        raise RuntimeError(f"missing anndata: {e}")

    try:
        from sklearn.metrics import adjusted_rand_score
    except Exception as e:
        raise RuntimeError(f"missing sklearn for ARI: {e}")

    if len(input_dirs) != len(batch_ids):
        raise ValueError("input_dirs and batch_ids length mismatch")
    if len(input_dirs) < 2:
        raise ValueError("scVI integration compare requires >=2 batches")

    adatas = []
    for p, bid in zip(input_dirs, batch_ids):
        a = read_10x_any(p)
        try:
            a.var_names_make_unique()
        except Exception:
            pass
        a.obs["batch"] = bid
        adatas.append(a)

    adata = ad.concat(adatas, join="outer", merge="first", label="batch", keys=batch_ids, index_unique="-")
    try:
        adata.var_names_make_unique()
    except Exception:
        pass

    # QC annotations
    adata.var["mt"] = adata.var_names.str.upper().str.startswith("MT-")
    sc.pp.calculate_qc_metrics(adata, qc_vars=["mt"], inplace=True)

    # QC defaults (v0)
    min_genes = 200
    max_pct_mt = 20.0
    sc.pp.filter_cells(adata, min_genes=min_genes)
    adata = adata[adata.obs["pct_counts_mt"] <= max_pct_mt, :].copy()
    sc.pp.filter_genes(adata, min_cells=3)

    # Deterministic cell cap (applies to both tiers, but CPU is additionally guarded).
    if scvi_max_cells and int(adata.n_obs) > int(scvi_max_cells):
        rng = np.random.default_rng(seed)
        batches = adata.obs["batch"].astype(str).to_numpy()
        uniq = pd.Series(batches).unique().tolist()
        n_batches = max(1, len(uniq))
        cap_per = int(np.ceil(int(scvi_max_cells) / n_batches))
        keep: list[int] = []
        for b in uniq:
            idx = np.where(batches == b)[0]
            if idx.size == 0:
                continue
            take = min(int(idx.size), cap_per)
            keep.extend(rng.choice(idx, size=take, replace=False).tolist())
        keep = sorted(set(keep))
        adata = adata[keep, :].copy()

    if compute_tier == "cpu" and int(adata.n_obs) > 5000:
        raise ComputeGateError(
            failure_type="resource_cap",
            message=f"CPU scVI run blocked by cap: n_cells={int(adata.n_obs)} > 5000. Use --compute-tier gpu or lower --scvi-max-cells.",
        )

    # Preserve counts and normalize/log for baseline/harmony.
    try:
        adata.layers["counts"] = adata.X.copy()
    except Exception:
        pass
    sc.pp.normalize_total(adata, target_sum=1e4)
    sc.pp.log1p(adata)
    adata_for_anno = adata.copy()

    labels = None
    anno_summary: dict[str, Any] = {}
    if annotate == "celltypist":
        try:
            import celltypist

            model_path = ROOT / "data" / "references" / "celltypist" / "Immune_All_Low.pkl"
            if not model_path.exists():
                fetcher = ROOT / "scripts" / "data" / "fetch_celltypist_model.py"
                code, out = run_cmd(["python3", str(fetcher), "--model", "Immune_All_Low.pkl"], cwd=ROOT)
                if code != 0:
                    raise RuntimeError(f"failed to fetch CellTypist model:\n{out}")
            pred = celltypist.annotate(adata_for_anno, model=str(model_path), majority_voting=True)
            labels_obj = pred.predicted_labels
            if hasattr(labels_obj, "ndim") and getattr(labels_obj, "ndim") == 2:
                labels_obj = labels_obj.iloc[:, 0]
            labels = labels_obj.astype(str)
            adata.obs["celltypist_label"] = labels.to_numpy()
            anno_summary = {"n_cell_types_pred": int(getattr(labels, "nunique", lambda: len(set(labels)))())}
        except Exception as e:
            anno_summary = {"annotation_error": str(e)}

    # Keep a pre-HVG copy for scVI robustness perturbation (HVG/2).
    adata_pre_hvg = adata.copy()

    # HVGs for baseline/harmony (same set used for the main scVI run).
    n_hvg = int(scvi_n_hvg) if scvi_n_hvg else 2000
    sc.pp.highly_variable_genes(
        adata,
        flavor="cell_ranger",
        n_top_genes=min(n_hvg, int(adata.n_vars)),
        layer="counts",
    )
    adata = adata[:, adata.var["highly_variable"]].copy()
    sc.pp.scale(adata, max_value=10)
    sc.tl.pca(adata, svd_solver="arpack", random_state=seed)

    # Baseline: PCA neighbors/cluster
    baseline_t0 = time.time()
    sc.pp.neighbors(adata, n_neighbors=15, n_pcs=30, use_rep="X_pca", random_state=seed)
    sc.tl.leiden(adata, resolution=0.5, random_state=seed, key_added="cluster_baseline")
    base_mix_mean, base_mix_median = _mean_median_neighbor_batch_mixing(adata, "batch")
    base_purity = _mean_neighbor_label_purity(adata, "celltypist_label") if labels is not None else float("nan")
    baseline_wall = time.time() - baseline_t0

    # Harmony on PCA
    harmony_t0 = time.time()
    try:
        import harmonypy as hm
    except Exception as e:
        raise RuntimeError(f"missing harmonypy: {e}")

    meta = adata.obs[["batch"]].copy()
    pca = np.asarray(adata.obsm["X_pca"])
    try:
        ho = hm.run_harmony(pca, meta, vars_use=["batch"], random_state=seed)
    except TypeError:
        ho = hm.run_harmony(pca, meta, vars_use=["batch"])
    z = np.asarray(ho.Z_corr)
    if z.shape[0] == pca.shape[0]:
        z_corr = z
    elif z.shape[1] == pca.shape[0]:
        z_corr = z.T
    else:
        raise ValueError(f"unexpected harmony Z_corr shape={z.shape} for pca shape={pca.shape}")
    adata.obsm["X_pca_harmony"] = z_corr

    sc.pp.neighbors(adata, n_neighbors=15, use_rep="X_pca_harmony", random_state=seed)
    sc.tl.leiden(adata, resolution=0.5, random_state=seed, key_added="cluster_harmony")
    harm_mix_mean, harm_mix_median = _mean_median_neighbor_batch_mixing(adata, "batch")
    harm_purity = _mean_neighbor_label_purity(adata, "celltypist_label") if labels is not None else float("nan")
    harmony_wall = time.time() - harmony_t0

    ari_baseline_vs_harmony = float(
        adjusted_rand_score(adata.obs["cluster_baseline"].astype(str), adata.obs["cluster_harmony"].astype(str))
    )

    # scVI training + clustering helper.
    scvi.settings.seed = int(seed)
    try:
        torch.manual_seed(int(seed))
    except Exception:
        pass
    try:
        torch.cuda.manual_seed_all(int(seed))
    except Exception:
        pass

    def _train_scvi_and_cluster(adata_scvi: Any) -> tuple[Any, float]:
        scvi.model.SCVI.setup_anndata(adata_scvi, layer="counts", batch_key="batch")
        model = scvi.model.SCVI(adata_scvi, n_latent=int(scvi_n_latent))
        t_scvi = time.time()
        use_gpu = compute_tier == "gpu"
        try:
            model.train(max_epochs=int(scvi_max_epochs), use_gpu=use_gpu, early_stopping=False)
        except TypeError:
            try:
                model.train(max_epochs=int(scvi_max_epochs), use_gpu=use_gpu)
            except TypeError:
                model.train(max_epochs=int(scvi_max_epochs))
        wall_scvi = time.time() - t_scvi
        latent = model.get_latent_representation()
        adata_scvi.obsm["X_scvi"] = np.asarray(latent)
        sc.pp.neighbors(adata_scvi, n_neighbors=15, use_rep="X_scvi", random_state=seed)
        sc.tl.leiden(adata_scvi, resolution=0.5, random_state=seed, key_added="cluster_scvi")
        return adata_scvi, wall_scvi

    # scVI main (HVG=n_hvg)
    adata_scvi = adata.copy()
    adata_scvi, scvi_wall = _train_scvi_and_cluster(adata_scvi)
    scvi_mix_mean, scvi_mix_median = _mean_median_neighbor_batch_mixing(adata_scvi, "batch")
    scvi_purity = _mean_neighbor_label_purity(adata_scvi, "celltypist_label") if labels is not None else float("nan")
    ari_scvi_vs_harmony = float(
        adjusted_rand_score(adata_scvi.obs["cluster_scvi"].astype(str), adata.obs["cluster_harmony"].astype(str))
    )

    # scVI robustness: HVG/2
    n_hvg_half = max(200, int(n_hvg // 2))
    adata_half = adata_pre_hvg.copy()
    sc.pp.highly_variable_genes(
        adata_half,
        flavor="cell_ranger",
        n_top_genes=min(n_hvg_half, int(adata_half.n_vars)),
        layer="counts",
    )
    adata_half = adata_half[:, adata_half.var["highly_variable"]].copy()
    adata_half, scvi_wall_hvg_half = _train_scvi_and_cluster(adata_half)
    ari_scvi_vs_hvg_half = float(
        adjusted_rand_score(
            adata_scvi.obs["cluster_scvi"].astype(str),
            adata_half.obs["cluster_scvi"].astype(str),
        )
    )

    return {
        "n_cells_after_qc": int(adata.n_obs),
        "n_genes_after_filter": int(adata.n_vars),
        "n_batches": int(pd.Series(adata.obs["batch"].astype(str)).nunique()),
        "batch_mixing_baseline_mean": base_mix_mean,
        "batch_mixing_baseline_median": base_mix_median,
        "batch_mixing_harmony_mean": harm_mix_mean,
        "batch_mixing_harmony_median": harm_mix_median,
        "batch_mixing_scvi_mean": scvi_mix_mean,
        "batch_mixing_scvi_median": scvi_mix_median,
        "label_purity_baseline_mean": base_purity,
        "label_purity_harmony_mean": harm_purity,
        "label_purity_scvi_mean": scvi_purity,
        "ari_clusters_baseline_vs_harmony": ari_baseline_vs_harmony,
        "ari_clusters_scvi_vs_harmony": ari_scvi_vs_harmony,
        "ari_clusters_scvi_vs_scvi_hvg_half": ari_scvi_vs_hvg_half,
        "wall_baseline_s": baseline_wall,
        "wall_harmony_s": harmony_wall,
        "wall_scvi_s": scvi_wall,
        "wall_scvi_hvg_half_s": scvi_wall_hvg_half,
        "annotation": anno_summary,
        "versions": {
            "scanpy_stack": _method_version_safe(["scanpy", "anndata", "numpy", "scipy", "pandas", "scikit-learn"]),
            "harmonypy": _method_version_safe(["harmonypy"]),
            "celltypist": _method_version_safe(["celltypist"]) if annotate == "celltypist" else "",
            "scvi_tools": _method_version_safe(["scvi-tools"]),
            "torch": _method_version_safe(["torch"]),
        },
        "notes": (
            f"qc_min_genes={min_genes};qc_max_pct_mt={max_pct_mt};"
            f"hvg={n_hvg};scvi_latent={scvi_n_latent};scvi_epochs={scvi_max_epochs};compute_tier={compute_tier}"
        ),
    }


def _read_visium_spatial_dir(spatial_dir: Path) -> "tuple[dict[str, Any], Any]":
    """
    Read Space Ranger spatial metadata needed for a minimal Visium baseline.

    Required (v0):
      - tissue_positions.csv OR tissue_positions_list.csv
      - scalefactors_json.json
    """
    try:
        import pandas as pd
    except Exception as e:
        raise RuntimeError(f"missing pandas for Visium spatial parsing: {e}")

    if not spatial_dir.exists():
        raise FileNotFoundError(f"missing spatial/ dir: {spatial_dir}")

    pos_csv = spatial_dir / "tissue_positions.csv"
    pos_list_csv = spatial_dir / "tissue_positions_list.csv"
    scalefactors = spatial_dir / "scalefactors_json.json"

    if not scalefactors.exists():
        raise FileNotFoundError(f"missing required spatial metadata: {scalefactors}")
    if pos_csv.exists():
        df = pd.read_csv(pos_csv)
    elif pos_list_csv.exists():
        df = pd.read_csv(
            pos_list_csv,
            header=None,
            names=[
                "barcode",
                "in_tissue",
                "array_row",
                "array_col",
                "pxl_row_in_fullres",
                "pxl_col_in_fullres",
            ],
        )
    else:
        raise FileNotFoundError(
            f"missing required tissue positions file: expected {pos_csv.name} or {pos_list_csv.name} under {spatial_dir}"
        )

    # Normalize column names and types (Space Ranger versions differ slightly).
    rename = {}
    for c in df.columns:
        if str(c).strip().lower() == "barcode":
            rename[c] = "barcode"
        if str(c).strip().lower() == "in_tissue":
            rename[c] = "in_tissue"
    if rename:
        df = df.rename(columns=rename)

    required_cols = {"barcode", "in_tissue", "array_row", "array_col", "pxl_row_in_fullres", "pxl_col_in_fullres"}
    missing = [c for c in sorted(required_cols) if c not in set(df.columns)]
    if missing:
        raise ValueError(f"tissue positions file missing columns: {missing}")

    df["barcode"] = df["barcode"].astype(str)
    df["in_tissue"] = df["in_tissue"].astype(int)

    try:
        scale = json.loads(scalefactors.read_text(encoding="utf-8"))
    except Exception as e:
        raise ValueError(f"failed to parse scalefactors_json.json: {e}")

    return scale, df


def _run_visium_pack_baseline(
    *,
    outs_dir: Path,
    seed: int,
) -> dict[str, Any]:
    try:
        import numpy as np
        import pandas as pd
        import scanpy as sc
    except Exception as e:
        raise RuntimeError(
            "Missing Visium method-pack dependencies. Run via Docker (recommended) or install python deps.\n"
            f"Import error: {e}"
        )

    matrix_dir = outs_dir / "filtered_feature_bc_matrix"
    spatial_dir = outs_dir / "spatial"
    matrix_h5 = outs_dir / "filtered_feature_bc_matrix.h5"
    if not matrix_dir.exists():
        # Space Ranger public examples are sometimes shipped as an H5 count matrix file
        # alongside spatial/ (instead of an extracted filtered_feature_bc_matrix/ folder).
        if not matrix_h5.exists():
            candidates = sorted(outs_dir.glob("*filtered_feature_bc_matrix.h5"))
            if candidates:
                matrix_h5 = candidates[0]
        if not matrix_h5.exists():
            raise FileNotFoundError(
                "missing required matrix artifact: expected filtered_feature_bc_matrix/ "
                f"or *filtered_feature_bc_matrix.h5 under {outs_dir}"
            )

    scale, tissue = _read_visium_spatial_dir(spatial_dir)

    if matrix_dir.exists():
        adata = read_10x_any(matrix_dir)
        matrix_stats = compute_basic_10x_stats(matrix_dir)
    else:
        # scanpy reads 10x H5 and yields the same barcode index semantics we need.
        adata = sc.read_10x_h5(str(matrix_h5))
        try:
            nnz = int(getattr(adata.X, "nnz"))
        except Exception:
            nnz = ""
        matrix_stats = {
            "matrix_n_rows": int(adata.n_vars),
            "matrix_n_cols": int(adata.n_obs),
            "matrix_n_entries": nnz,
            "n_cells_or_spots": int(adata.n_obs),
            "n_genes": int(adata.n_vars),
        }
    try:
        adata.var_names_make_unique()
    except Exception:
        pass

    # Join spatial metadata to barcodes in the matrix.
    tissue = tissue.set_index("barcode", drop=False)
    obs = pd.DataFrame(index=pd.Index(adata.obs_names.astype(str), name="barcode"))
    obs["barcode"] = obs.index.astype(str)
    obs = obs.join(
        tissue[
            [
                "in_tissue",
                "array_row",
                "array_col",
                "pxl_row_in_fullres",
                "pxl_col_in_fullres",
            ]
        ],
        how="left",
    )
    missing_spots = int(obs["in_tissue"].isna().sum())
    if missing_spots > 0:
        raise ValueError(
            f"{missing_spots} barcodes in filtered_feature_bc_matrix are missing from tissue positions; "
            "input is likely inconsistent. Ensure the matrix and spatial/ come from the same Space Ranger run."
        )
    adata.obs = obs

    # Use in-tissue spots for baseline clustering/QC summaries (reviewer-facing default).
    n_spots_total = int(adata.n_obs)
    n_spots_in_tissue = int((adata.obs["in_tissue"] == 1).sum())
    if n_spots_in_tissue == 0:
        raise ValueError("no in-tissue spots found (in_tissue==1); check Space Ranger spatial metadata")

    adata = adata[adata.obs["in_tissue"] == 1, :].copy()

    # QC metrics (mt% when feasible)
    adata.var["mt"] = adata.var_names.str.upper().str.startswith("MT-")
    sc.pp.calculate_qc_metrics(adata, qc_vars=["mt"], inplace=True)

    # Preserve counts then normalize/log for stable numeric behavior.
    try:
        adata.layers["counts"] = adata.X.copy()
    except Exception:
        pass
    sc.pp.normalize_total(adata, target_sum=1e4)
    sc.pp.log1p(adata)

    # Spatial coords (array grid) for neighbors + clustering.
    adata.obsm["X_spatial"] = np.asarray(adata.obs[["array_col", "array_row"]].to_numpy())
    sc.pp.neighbors(adata, n_neighbors=6, use_rep="X_spatial", random_state=seed)
    sc.tl.leiden(adata, resolution=0.5, random_state=seed, key_added="cluster")

    clusters = adata.obs["cluster"].astype(str)

    metrics = {
        "n_spots_total": n_spots_total,
        "n_spots_in_tissue": int(adata.n_obs),
        "n_genes": int(adata.n_vars),
        "median_total_counts": float(pd.to_numeric(adata.obs["total_counts"]).median()),
        "median_n_genes_by_counts": float(pd.to_numeric(adata.obs["n_genes_by_counts"]).median()),
        "median_pct_counts_mt": float(pd.to_numeric(adata.obs.get("pct_counts_mt", pd.Series([float("nan")]))).median()),
        "n_clusters": int(clusters.nunique()),
    }

    # Anchor table for figures/reporting (spot-level, lightweight)
    anchor = pd.DataFrame(
        {
            "dataset_id": "",
            "barcode": adata.obs["barcode"].astype(str).to_list(),
            "array_row": adata.obs["array_row"].astype(int).to_list(),
            "array_col": adata.obs["array_col"].astype(int).to_list(),
            "pxl_row_in_fullres": adata.obs["pxl_row_in_fullres"].astype(int).to_list(),
            "pxl_col_in_fullres": adata.obs["pxl_col_in_fullres"].astype(int).to_list(),
            "total_counts": pd.to_numeric(adata.obs["total_counts"]).to_list(),
            "n_genes_by_counts": pd.to_numeric(adata.obs["n_genes_by_counts"]).to_list(),
            "pct_counts_mt": pd.to_numeric(adata.obs.get("pct_counts_mt", pd.Series([float("nan")] * adata.n_obs))).to_list(),
            "cluster": clusters.to_list(),
        }
    )

    return {
        "metrics": metrics,
        "anchor_spots": anchor,
        "scalefactors": scale,
        "matrix_stats": matrix_stats,
        "versions": {
            "scanpy_stack": _method_version_safe(["scanpy", "anndata", "numpy", "scipy", "pandas", "scikit-learn"]),
        },
    }

def _ensure_visium_filtered_feature_bc_matrix_dir(*, outs_dir: Path) -> Path:
    """
    Ensure Space Ranger-like `filtered_feature_bc_matrix/` exists under outs_dir.

    Some 10x public Visium examples ship counts as `*filtered_feature_bc_matrix.h5`.
    Our baseline runner can read the H5 directly, but the deconvolution runners
    (RCTD/Tangram) expect the folder layout. For those cases, materialize a
    minimal 10x MTX folder from the H5.
    """
    ff = outs_dir / "filtered_feature_bc_matrix"
    if ff.exists():
        return ff

    h5 = outs_dir / "filtered_feature_bc_matrix.h5"
    if not h5.exists():
        candidates = sorted(outs_dir.glob("*filtered_feature_bc_matrix.h5"))
        if candidates:
            h5 = candidates[0]
    if not h5.exists():
        raise FileNotFoundError(
            "missing required matrix artifact for deconvolution: expected filtered_feature_bc_matrix/ "
            f"or *filtered_feature_bc_matrix.h5 under {outs_dir}"
        )

    try:
        import gzip as _gzip

        import numpy as _np
        import scanpy as sc
        from scipy import io as _io
        from scipy import sparse as _sparse
    except Exception as e:
        raise RuntimeError(f"cannot materialize filtered_feature_bc_matrix from H5 (missing deps): {e}")

    ff.mkdir(parents=True, exist_ok=True)

    adata = sc.read_10x_h5(str(h5))
    try:
        adata.var_names_make_unique()
    except Exception:
        pass

    X = adata.X
    if _sparse.issparse(X):
        mat = X.T.tocoo()
    else:
        mat = _sparse.coo_matrix(_np.asarray(X)).T

    # matrix.mtx.gz (genes x cells)
    tmp_mtx = ff / "matrix.mtx"
    _io.mmwrite(str(tmp_mtx), mat)
    with tmp_mtx.open("rb") as f_in, _gzip.open(ff / "matrix.mtx.gz", "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    tmp_mtx.unlink(missing_ok=True)

    # features.tsv.gz (gene_id, gene_symbol, feature_type)
    gene_symbols = [str(x) for x in adata.var_names.astype(str).tolist()]
    gene_ids_col = None
    for k in ["gene_ids", "gene_id", "id"]:
        if k in getattr(adata.var, "columns", []):
            gene_ids_col = k
            break
    if gene_ids_col:
        gene_ids = [str(x) for x in adata.var[gene_ids_col].astype(str).tolist()]
    else:
        gene_ids = gene_symbols
    tmp_feats = ff / "features.tsv"
    with tmp_feats.open("w", encoding="utf-8") as f:
        for gid, sym in zip(gene_ids, gene_symbols):
            f.write(f"{gid}\t{sym}\tGene Expression\n")
    with tmp_feats.open("rb") as f_in, _gzip.open(ff / "features.tsv.gz", "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    tmp_feats.unlink(missing_ok=True)

    # barcodes.tsv.gz
    barcodes = [str(x) for x in adata.obs_names.astype(str).tolist()]
    tmp_bcs = ff / "barcodes.tsv"
    tmp_bcs.write_text("\n".join(barcodes) + "\n", encoding="utf-8")
    with tmp_bcs.open("rb") as f_in, _gzip.open(ff / "barcodes.tsv.gz", "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    tmp_bcs.unlink(missing_ok=True)

    return ff


def _build_celltypist_labels_tsv(*, matrix_dir: Path, seed: int, out_tsv: Path) -> dict[str, Any]:
    """
    Build scRNA reference labels via CellTypist from a 10x matrix dir and write a lightweight TSV:
      barcode \\t label

    This is used as a reference-label source for Visium deconvolution/mapping methods (RCTD/Tangram).
    """
    try:
        import numpy as np
        import pandas as pd
        import scanpy as sc
    except Exception as e:
        raise RuntimeError(
            "Missing scRNA dependencies for reference labeling. Run via Docker (recommended) or install python deps.\n"
            f"Import error: {e}"
        )

    try:
        import celltypist
    except Exception as e:
        raise RuntimeError(f"Missing CellTypist: {e}")

    if not matrix_dir.exists():
        raise FileNotFoundError(f"missing reference matrix dir: {matrix_dir}")

    rng = np.random.default_rng(seed)
    adata = read_10x_any(matrix_dir)
    try:
        adata.var_names_make_unique()
    except Exception:
        pass

    adata.var["mt"] = adata.var_names.str.upper().str.startswith("MT-")
    sc.pp.calculate_qc_metrics(adata, qc_vars=["mt"], inplace=True)

    perm = rng.permutation(adata.n_obs)
    adata = adata[perm, :].copy()

    min_genes = 200
    max_pct_mt = 20.0
    sc.pp.filter_cells(adata, min_genes=min_genes)
    adata = adata[adata.obs["pct_counts_mt"] <= max_pct_mt, :].copy()
    sc.pp.filter_genes(adata, min_cells=3)

    sc.pp.normalize_total(adata, target_sum=1e4)
    sc.pp.log1p(adata)

    model_path = ROOT / "data" / "references" / "celltypist" / "Immune_All_Low.pkl"
    if not model_path.exists():
        fetcher = ROOT / "scripts" / "data" / "fetch_celltypist_model.py"
        code, out = run_cmd(["python3", str(fetcher), "--model", "Immune_All_Low.pkl"], cwd=ROOT)
        if code != 0:
            raise RuntimeError(f"failed to fetch CellTypist model:\n{out}")

    pred = celltypist.annotate(adata, model=str(model_path), majority_voting=True)
    labels_obj = pred.predicted_labels
    if hasattr(labels_obj, "ndim") and getattr(labels_obj, "ndim") == 2:
        try:
            cols = list(getattr(labels_obj, "columns", []))
            if "majority_voting" in cols:
                labels_obj = labels_obj["majority_voting"]
            elif "predicted_labels" in cols:
                labels_obj = labels_obj["predicted_labels"]
            else:
                labels_obj = labels_obj.iloc[:, 0]
        except Exception:
            labels_obj = labels_obj.iloc[:, 0]
    labels = labels_obj.astype(str)
    # RCTD forbids certain characters (e.g., '/'). Normalize labels for cross-tool compatibility.
    labels = labels.apply(
        lambda s: (
            str(s)
            .strip()
            .replace("/", "_")
            .replace("\\", "_")
            .replace("|", "_")
            .replace(":", "_")
            .replace(";", "_")
            .replace(" ", "_")
        )
    )
    try:
        labels = labels.str.replace("__", "_", regex=False)
        while True:
            new = labels.str.replace("__", "_", regex=False)
            if new.equals(labels):
                break
            labels = new
    except Exception:
        pass

    out_tsv.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"barcode": adata.obs_names.astype(str).to_numpy(), "label": labels.to_numpy()}).to_csv(
        out_tsv, sep="\t", index=False
    )

    return {
        "n_cells_labeled": int(adata.n_obs),
        "n_labels": int(getattr(labels, "nunique", lambda: len(set(labels)))()),
        "qc_min_genes": min_genes,
        "qc_max_pct_mt": max_pct_mt,
        "versions": {"celltypist": _method_version_safe(["celltypist"])},
        "notes": "model=Immune_All_Low.pkl;majority_voting=TRUE",
    }


def _run_visium_pack_rctd(
    *,
    outs_dir: Path,
    reference_scrna_dir: Path,
    reference_labels_tsv: Path,
    dataset_id: str,
    reference_dataset_id: str,
    seed: int,
    out_weights_tsv: Path,
) -> dict[str, Any]:
    rscript = shutil.which("Rscript")
    if not rscript:
        raise RuntimeError("Rscript not found; required for RCTD runner.")

    script = ROOT / "scripts" / "methods" / "visium_rctd.R"
    if not script.exists():
        raise FileNotFoundError(f"missing runner script: {script}")

    with tempfile.TemporaryDirectory(prefix="visium_rctd_") as td:
        out_json = Path(td) / "rctd_summary.json"
        cmd = [
            rscript,
            str(script),
            f"--visium-dir={str(outs_dir)}",
            f"--scrna-dir={str(reference_scrna_dir)}",
            f"--labels-tsv={str(reference_labels_tsv)}",
            f"--dataset-id={dataset_id}",
            f"--reference-dataset-id={reference_dataset_id}",
            f"--out-weights-tsv={str(out_weights_tsv)}",
            f"--out-json={str(out_json)}",
            f"--seed={seed}",
        ]
        code, out = run_cmd(cmd, cwd=ROOT)
        if code != 0:
            raise RuntimeError(f"RCTD runner failed:\n{out}")
        try:
            data = json.loads(out_json.read_text(encoding="utf-8"))
        except Exception as e:
            raise RuntimeError(f"failed to parse RCTD runner output JSON: {e}")
        return data


def _run_visium_pack_tangram(
    *,
    outs_dir: Path,
    reference_scrna_dir: Path,
    reference_labels_tsv: Path,
    dataset_id: str,
    reference_dataset_id: str,
    seed: int,
    out_weights_tsv: Path,
    compute_tier: str,
    n_hvg: int = 2000,
    max_cells: int = 3000,
) -> dict[str, Any]:
    script = ROOT / "scripts" / "methods" / "visium_tangram.py"
    if not script.exists():
        raise FileNotFoundError(f"missing runner script: {script}")

    with tempfile.TemporaryDirectory(prefix="visium_tangram_") as td:
        out_json = Path(td) / "tangram_summary.json"
        device = "cuda:0" if compute_tier == "gpu" else "cpu"
        cmd = [
            "python3",
            str(script),
            "--visium-dir",
            str(outs_dir),
            "--scrna-dir",
            str(reference_scrna_dir),
            "--labels-tsv",
            str(reference_labels_tsv),
            "--dataset-id",
            dataset_id,
            "--reference-dataset-id",
            reference_dataset_id,
            "--seed",
            str(seed),
            "--n_hvg",
            str(int(n_hvg)),
            "--max_cells",
            str(int(max_cells)),
            "--device",
            device,
            "--out-weights-tsv",
            str(out_weights_tsv),
            "--out-json",
            str(out_json),
        ]
        code, out = run_cmd(cmd, cwd=ROOT)
        if code != 0:
            raise RuntimeError(f"Tangram runner failed:\n{out}")
        try:
            data = json.loads(out_json.read_text(encoding="utf-8"))
        except Exception as e:
            raise RuntimeError(f"failed to parse Tangram runner output JSON: {e}")
        return data


def _run_visium_pack_cell2location(
    *,
    outs_dir: Path,
    reference_scrna_dir: Path,
    reference_labels_tsv: Path,
    dataset_id: str,
    reference_dataset_id: str,
    seed: int,
    out_weights_tsv: Path,
    compute_tier: str,
    regression_max_epochs: int = 250,
    max_epochs: int = 2000,
    num_samples: int = 200,
    n_cells_per_location: int = 30,
    detection_alpha: float = 20.0,
    max_cells: int = 20000,
    max_spots: int = 8000,
) -> dict[str, Any]:
    script = ROOT / "scripts" / "methods" / "visium_cell2location.py"
    if not script.exists():
        raise FileNotFoundError(f"missing runner script: {script}")

    # Keep device selection consistent with other torch-based runners.
    device = "cuda:0" if compute_tier == "gpu" else "cpu"

    with tempfile.TemporaryDirectory(prefix="visium_cell2location_") as td:
        out_json = Path(td) / "cell2location_summary.json"
        cmd = [
            "python3",
            str(script),
            "--visium-dir",
            str(outs_dir),
            "--scrna-dir",
            str(reference_scrna_dir),
            "--labels-tsv",
            str(reference_labels_tsv),
            "--dataset-id",
            dataset_id,
            "--reference-dataset-id",
            reference_dataset_id,
            "--seed",
            str(seed),
            "--regression-max-epochs",
            str(int(regression_max_epochs)),
            "--max-epochs",
            str(int(max_epochs)),
            "--num-samples",
            str(int(num_samples)),
            "--n-cells-per-location",
            str(int(n_cells_per_location)),
            "--detection-alpha",
            str(float(detection_alpha)),
            "--max_cells",
            str(int(max_cells)),
            "--max_spots",
            str(int(max_spots)),
            "--device",
            device,
            "--out-weights-tsv",
            str(out_weights_tsv),
            "--out-json",
            str(out_json),
        ]
        code, out = run_cmd(cmd, cwd=ROOT)
        if code != 0:
            raise RuntimeError(f"cell2location runner failed:\n{out}")
        try:
            data = json.loads(out_json.read_text(encoding="utf-8"))
        except Exception as e:
            raise RuntimeError(f"failed to parse cell2location runner output JSON: {e}")
        return data


def _load_weights_long(path: Path):
    try:
        import pandas as pd
    except Exception as e:
        raise RuntimeError(f"missing pandas for weights parsing: {e}")
    df = pd.read_csv(path, sep="\t")
    required = {"barcode", "cell_type", "weight"}
    if not required.issubset(set(df.columns)):
        raise ValueError(f"weights table missing required columns {sorted(required)}: {path}")
    df["barcode"] = df["barcode"].astype(str)
    df["cell_type"] = df["cell_type"].astype(str)
    df["weight"] = pd.to_numeric(df["weight"])
    return df


def _weights_concordance_pearson_mean(a_tsv: Path, b_tsv: Path) -> float:
    try:
        import numpy as np
        import pandas as pd
    except Exception as e:
        raise RuntimeError(f"missing deps for concordance: {e}")
    a = _load_weights_long(a_tsv)
    b = _load_weights_long(b_tsv)
    a_w = a.pivot_table(index="barcode", columns="cell_type", values="weight", aggfunc="mean").fillna(0.0)
    b_w = b.pivot_table(index="barcode", columns="cell_type", values="weight", aggfunc="mean").fillna(0.0)
    spots = a_w.index.intersection(b_w.index)
    cts = a_w.columns.intersection(b_w.columns)
    if len(spots) < 50 or len(cts) < 2:
        return float("nan")
    a_w = a_w.loc[spots, cts]
    b_w = b_w.loc[spots, cts]
    cors: list[float] = []
    for ct in cts:
        x = np.asarray(a_w[ct], dtype=float)
        y = np.asarray(b_w[ct], dtype=float)
        if np.std(x) == 0 or np.std(y) == 0:
            continue
        cors.append(float(np.corrcoef(x, y)[0, 1]))
    if not cors:
        return float("nan")
    return float(np.mean(cors))


def _weights_concordance_cosine_by_spot_summary(a_tsv: Path, b_tsv: Path) -> tuple[float, float, float, int]:
    """
    Spotwise composition concordance between two deconvolution outputs.

    Returns (median, q25, q75, n_spots) for cosine similarity computed on
    L1-normalized per-spot weight vectors over shared cell types.
    """
    try:
        import numpy as np
    except Exception as e:
        raise RuntimeError(f"missing deps for concordance: {e}")

    a = _load_weights_long(a_tsv)
    b = _load_weights_long(b_tsv)

    a_w = a.pivot_table(index="barcode", columns="cell_type", values="weight", aggfunc="mean").fillna(0.0)
    b_w = b.pivot_table(index="barcode", columns="cell_type", values="weight", aggfunc="mean").fillna(0.0)

    spots = a_w.index.intersection(b_w.index)
    cts = a_w.columns.intersection(b_w.columns)
    if len(spots) < 50 or len(cts) < 5:
        return (float("nan"), float("nan"), float("nan"), 0)

    a_w = a_w.loc[spots, cts]
    b_w = b_w.loc[spots, cts]

    aw = np.asarray(a_w.to_numpy(), dtype=float)
    bw = np.asarray(b_w.to_numpy(), dtype=float)

    # L1-normalize per spot so methods are comparable even if raw weights are not.
    aw = aw / np.clip(aw.sum(axis=1, keepdims=True), 1e-12, None)
    bw = bw / np.clip(bw.sum(axis=1, keepdims=True), 1e-12, None)

    num = np.sum(aw * bw, axis=1)
    den = np.sqrt(np.sum(aw * aw, axis=1)) * np.sqrt(np.sum(bw * bw, axis=1))
    den = np.where(den == 0, np.nan, den)
    cos = num / den
    cos = cos[np.isfinite(cos)]
    if cos.size < 200:
        return (float("nan"), float("nan"), float("nan"), int(cos.size))

    med = float(np.median(cos))
    q25 = float(np.quantile(cos, 0.25))
    q75 = float(np.quantile(cos, 0.75))
    return (med, q25, q75, int(cos.size))


def cmd_visium(args: argparse.Namespace) -> int:
    run_id = make_run_id(f"visium_{args.method_pack}")
    seed = int(args.seed)
    env = env_fingerprint()
    env_hash = build_env_hash(env)

    dataset_id = args.dataset_id
    outs_dir = (ROOT / args.input_dir).resolve()
    if not outs_dir.exists():
        raise FileNotFoundError(f"--input-dir not found: {outs_dir}")

    t0 = time.time()
    status = "ok"
    failure_type = ""
    try:
        ds_path = ROOT / "results" / "dataset_summary.tsv"
        mb_path = ROOT / "results" / "benchmarks" / "method_benchmark.tsv"
        rt_path = ROOT / "results" / "benchmarks" / "runtime_cost_failure.tsv"
        bc_path = ROOT / "results" / "benchmarks" / "biological_output_concordance.tsv"

        # Always compute the baseline anchor (spot coords/QC/cluster). This keeps downstream methods
        # anchored to the same in-tissue spot set and produces a stable plotting table.
        base = _run_visium_pack_baseline(outs_dir=outs_dir, seed=seed)

        # dataset summary registration (append-only; add if missing or incomplete)
        matrix_stats = base.get("matrix_stats") or {}
        last_ds = tsv_last_matching_row(ds_path, "dataset_id", dataset_id) if dataset_id else None
        needs_ds_row = False
        if dataset_id and not last_ds:
            needs_ds_row = True
        elif last_ds:
            if (last_ds.get("n_cells_or_spots", "").strip() == "") or (last_ds.get("n_genes", "").strip() == ""):
                needs_ds_row = True
        if dataset_id and needs_ds_row:
            ds_row = {
                "dataset_id": dataset_id,
                "modality": "Visium",
                "organism": args.organism,
                "tissue": args.tissue,
                "assay_platform": "10x Visium",
                "input_artifact": str(Path(args.input_dir)),
                "entrypoint": "spaceranger_outs",
                "role": "benchmark",
                "n_samples": "",
                "n_donors": "",
                "n_cells_or_spots": base["metrics"].get("n_spots_in_tissue", ""),
                "n_genes": base["metrics"].get("n_genes", ""),
                "reference_genome": "",
                "primary_citation": "",
                "source_url": "",
                "license": "",
                "qc_summary": (
                    f"mtx_rows={matrix_stats.get('matrix_n_rows','')};mtx_cols={matrix_stats.get('matrix_n_cols','')};"
                    f"mtx_nnz={matrix_stats.get('matrix_n_entries','')};"
                    f"n_spots_total={base['metrics'].get('n_spots_total','')};"
                    f"n_spots_in_tissue={base['metrics'].get('n_spots_in_tissue','')}"
                ),
                "notes": "registered by visium method-pack run",
            }
            write_tsv_row(ds_path, ds_row)

        # Write baseline spot anchor table
        spots_path = ROOT / "results" / "figures" / "visium_spots.tsv"
        base["anchor_spots"]["dataset_id"] = dataset_id
        ensure_parents(spots_path)
        base["anchor_spots"].to_csv(spots_path, sep="\t", index=False)

        # Baseline pack: only QC+cluster
        if args.method_pack == "baseline":
            wall = time.time() - t0
            method_id = "scanpy-visium-baseline"
            for metric_id in [
                "n_spots_total",
                "n_spots_in_tissue",
                "median_total_counts",
                "median_n_genes_by_counts",
                "median_pct_counts_mt",
                "n_clusters",
            ]:
                write_tsv_row(
                    mb_path,
                    {
                        "dataset_id": dataset_id,
                        "modality": "Visium",
                        "task": "visium_qc+cluster",
                        "method_id": method_id,
                        "method_version": base["versions"]["scanpy_stack"],
                        "baseline_flag": "1",
                        "metric_id": metric_id,
                        "metric_value": base["metrics"].get(metric_id, ""),
                        "metric_ci_low": "",
                        "metric_ci_high": "",
                        "metric_unit": "",
                        "eval_split": "in_tissue",
                        "replicate_id": run_id,
                        "n_units": base["metrics"].get("n_spots_in_tissue", ""),
                        "notes": "neighbors=6 on array coords; leiden_res=0.5",
                    },
                )

            write_tsv_row(
                rt_path,
                {
                    "dataset_id": dataset_id,
                    "modality": "Visium",
                    "method_id": f"{args.method_pack}:{method_id}",
                    "run_id": run_id,
                    "status": "ok",
                    "failure_type": "",
                    "wall_time_s": f"{wall:.3f}",
                    "peak_ram_gb": "",
                    "peak_disk_gb": "",
                    "cpu_hours": "",
                    "gpu_hours": "",
                    "estimated_cost_usd": "",
                    "cost_model": "",
                    "notes": "v0 visium baseline pack",
                },
            )

            write_repro_check(
                run_id=run_id,
                dataset_id=dataset_id,
                stage=f"visium_{args.method_pack}",
                env_hash=env_hash,
                seed=seed,
                output_table_path="results/benchmarks/method_benchmark.tsv",
                notes=f"visium method-pack={args.method_pack}",
                pass_flag=True,
                wall_time_s=wall,
            )
        else:
            # Deconvolution/mapping pack (baseline RCTD + advanced Tangram)
            if not args.reference_scrna_dir:
                raise ValueError("--reference-scrna-dir is required for --method-pack deconvolution")
            reference_scrna_dir = (ROOT / args.reference_scrna_dir).resolve()
            if not reference_scrna_dir.exists():
                raise FileNotFoundError(f"--reference-scrna-dir not found: {reference_scrna_dir}")
            reference_dataset_id = args.reference_dataset_id
            compute_tier = getattr(args, "compute_tier", "cpu")
            runner_sel = getattr(args, "runner", "default")
            run_rctd = runner_sel in {"default", "rctd", "all"}
            run_tangram = runner_sel in {"default", "tangram", "all"}
            run_cell2location = runner_sel in {"cell2location", "all"}
            tangram_n_hvg = int(getattr(args, "tangram_n_hvg", "2000"))
            tangram_max_cells = int(getattr(args, "tangram_max_cells", "3000"))
            cell2_max_epochs = int(getattr(args, "cell2location_max_epochs", "2000"))
            cell2_num_samples = int(getattr(args, "cell2location_num_samples", "200"))
            cell2_n_cells_per_loc = int(getattr(args, "cell2location_n_cells_per_location", "30"))
            cell2_detection_alpha = float(getattr(args, "cell2location_detection_alpha", "20.0"))
            cell2_reg_max_epochs = int(getattr(args, "cell2location_regression_max_epochs", "250"))
            cell2_max_cells = int(getattr(args, "cell2location_max_cells", "20000"))
            cell2_max_spots = int(getattr(args, "cell2location_max_spots", "8000"))

            # Deconvolution runners expect Space Ranger folder layout. If the Visium target ships as H5 counts,
            # materialize a minimal filtered_feature_bc_matrix/ directory.
            _ensure_visium_filtered_feature_bc_matrix_dir(outs_dir=outs_dir)

            labels_path = ROOT / "results" / "figures" / "visium_reference_cell_labels.tsv"
            labels_source = "celltypist"
            ref_label_meta: dict[str, Any] = {}

            # Reference labels:
            # - Default: CellTypist (Immune_All_Low) for PBMC-style demos
            # - Override: user-provided TSV with columns {barcode, label} for tissue-matched references
            provided_labels = getattr(args, "reference_labels_tsv", "") or ""
            if provided_labels.strip():
                src = (ROOT / provided_labels).resolve()
                if not src.exists():
                    raise FileNotFoundError(f"--reference-labels-tsv not found: {src}")
                ensure_parents(labels_path)
                shutil.copy2(src, labels_path)
                labels_source = "provided_tsv"

                # Lightweight metadata for audit notes (do not load large objects).
                try:
                    with labels_path.open("r", encoding="utf-8") as f:
                        reader = csv.DictReader(f, delimiter="\t")
                        if "barcode" not in (reader.fieldnames or []) or "label" not in (reader.fieldnames or []):
                            raise ValueError("labels TSV missing required columns: barcode, label")
                        n = 0
                        for _ in reader:
                            n += 1
                    ref_label_meta = {"n_cells_labeled": str(n)}
                except Exception as e:
                    raise ValueError(f"invalid --reference-labels-tsv format: {e}") from e
            else:
                # Reference labels via CellTypist (lightweight TSV)
                ref_label_meta = _build_celltypist_labels_tsv(matrix_dir=reference_scrna_dir, seed=seed, out_tsv=labels_path)

            # Baseline: RCTD (optional; controlled by --runner)
            rctd = {}
            rctd_wall = float("nan")
            rctd_weights = ROOT / "results" / "figures" / "visium_celltype_weights_rctd.tsv"
            if run_rctd:
                rctd_t0 = time.time()
                rctd = _run_visium_pack_rctd(
                    outs_dir=outs_dir,
                    reference_scrna_dir=reference_scrna_dir,
                    reference_labels_tsv=labels_path,
                    dataset_id=dataset_id,
                    reference_dataset_id=reference_dataset_id,
                    seed=seed,
                    out_weights_tsv=rctd_weights,
                )
                rctd_wall = time.time() - rctd_t0

            tang = {}
            tang_wall = float("nan")
            tang_status = "skip"
            tang_failure = ""
            tang_exc: Exception | None = None
            tang_failure_msg = ""
            tangram_weights = ROOT / "results" / "figures" / "visium_celltype_weights_tangram.tsv"
            if run_tangram:
                # Advanced: Tangram (CPU by default; uses GPU if compute_tier==gpu and CUDA is available)
                tang_status = "ok"
                tang_t0 = time.time()
                try:
                    tang = _run_visium_pack_tangram(
                        outs_dir=outs_dir,
                        reference_scrna_dir=reference_scrna_dir,
                        reference_labels_tsv=labels_path,
                        dataset_id=dataset_id,
                        reference_dataset_id=reference_dataset_id,
                        seed=seed,
                        out_weights_tsv=tangram_weights,
                        compute_tier=compute_tier,
                        n_hvg=tangram_n_hvg,
                        max_cells=tangram_max_cells,
                    )
                    tang_wall = time.time() - tang_t0

                    # Robustness (v0): Tangram sensitivity to halving the HVG set.
                    tangram_weights_half = ROOT / "results" / "figures" / "visium_celltype_weights_tangram_hvg_half.tsv"
                    tang_half = _run_visium_pack_tangram(
                        outs_dir=outs_dir,
                        reference_scrna_dir=reference_scrna_dir,
                        reference_labels_tsv=labels_path,
                        dataset_id=dataset_id,
                        reference_dataset_id=reference_dataset_id,
                        seed=seed + 1,
                        out_weights_tsv=tangram_weights_half,
                        compute_tier=compute_tier,
                        n_hvg=max(200, tangram_n_hvg // 2),
                        max_cells=tangram_max_cells,
                    )
                    robust = _weights_concordance_pearson_mean(tangram_weights, tangram_weights_half)
                    if robust == robust:
                        write_tsv_row(
                            ROOT / "results" / "benchmarks" / "robustness_matrix.tsv",
                            {
                                "dataset_id": dataset_id,
                                "modality": "Visium",
                                "method_id": "tangram",
                                "perturbation_id": "hvg_half",
                                "severity": "low",
                                "metric_id": "mean_pearson_by_cell_type",
                                "metric_value": robust,
                                "delta_vs_nominal": "",
                                "pass": "1" if float(robust) >= 0.90 else "0",
                                "failure_reason": "" if float(robust) >= 0.90 else "low concordance under hvg_half",
                                "n_units": (tang_half.get("metrics") or {}).get("n_spots_in_tissue", ""),
                                "notes": f"hvg={tangram_n_hvg} vs hvg={max(200, tangram_n_hvg // 2)};max_cells={tangram_max_cells}",
                            },
                        )
                except Exception as e:
                    tang_status = "fail"
                    tang_failure = type(e).__name__
                    tang_failure_msg = str(e)[:800]
                    tang_exc = e
                    tang_wall = time.time() - tang_t0

            # Optional: cell2location (uncertainty-aware probabilistic mapping; compute-tier gated)
            cell2 = {}
            cell2_wall = float("nan")
            cell2_status = "skip"
            cell2_failure = ""
            cell2_exc: Exception | None = None
            cell2_failure_msg = ""
            cell2_weights = ROOT / "results" / "figures" / "visium_celltype_weights_cell2location.tsv"
            if run_cell2location:
                cell2_status = "ok"
                cell2_t0 = time.time()
                try:
                    # A conservative CPU cap to avoid accidental multi-hour runs on large datasets.
                    n_spots = int((base.get("metrics") or {}).get("n_spots_in_tissue") or 0)
                    if compute_tier == "cpu" and cell2_max_spots and n_spots and n_spots > cell2_max_spots:
                        raise ComputeGateError(
                            failure_type="resource_cap",
                            message=f"cell2location cpu cap: n_spots_in_tissue={n_spots} > max_spots={cell2_max_spots}",
                        )
                    cell2 = _run_visium_pack_cell2location(
                        outs_dir=outs_dir,
                        reference_scrna_dir=reference_scrna_dir,
                        reference_labels_tsv=labels_path,
                        dataset_id=dataset_id,
                        reference_dataset_id=reference_dataset_id,
                        seed=seed,
                        out_weights_tsv=cell2_weights,
                        compute_tier=compute_tier,
                        regression_max_epochs=cell2_reg_max_epochs,
                        max_epochs=cell2_max_epochs,
                        num_samples=cell2_num_samples,
                        n_cells_per_location=cell2_n_cells_per_loc,
                        detection_alpha=cell2_detection_alpha,
                        max_cells=cell2_max_cells,
                        max_spots=cell2_max_spots,
                    )
                except Exception as e:
                    cell2_status = "fail"
                    cell2_exc = e
                    cell2_failure_msg = str(e)[:800]
                    if isinstance(e, ComputeGateError):
                        cell2_failure = e.failure_type
                    else:
                        msg = str(e)
                        if "Missing cell2location dependencies" in msg:
                            cell2_failure = "missing_dependency"
                        elif "torch.cuda.is_available()==False" in msg:
                            cell2_failure = "missing_gpu"
                        else:
                            cell2_failure = "runtime_error"
                    cell2 = {}
                finally:
                    cell2_wall = time.time() - cell2_t0

            # Method benchmark table (quality proxies)
            method_rows: list[tuple[str, dict[str, Any], str, str]] = []
            if run_rctd:
                method_rows.append(("rctd", rctd, "1", f"reference={reference_dataset_id};labels={labels_source}"))
            if tang_status == "ok":
                method_rows.append(
                    ("tangram", tang, "0", f"reference={reference_dataset_id};labels={labels_source};compute_tier={compute_tier}")
                )
            if cell2_status == "ok":
                method_rows.append(
                    (
                        "cell2location",
                        cell2,
                        "0",
                        f"reference={reference_dataset_id};labels={labels_source};compute_tier={compute_tier}",
                    )
                )

            for metric_id in ["n_cell_types", "gene_overlap", "mean_entropy", "mean_max_weight"]:
                for method_id, payload, baseline_flag, notes in method_rows:
                    versions = payload.get("versions") or {}
                    metrics = payload.get("metrics") or {}
                    if method_id == "rctd":
                        method_version = versions.get("spacexr", "")
                    elif method_id == "tangram":
                        method_version = versions.get("tangram", "")
                    else:
                        method_version = versions.get("cell2location", "")
                    write_tsv_row(
                        mb_path,
                        {
                            "dataset_id": dataset_id,
                            "modality": "Visium",
                            "task": "visium_deconvolution",
                            "method_id": method_id,
                            "method_version": method_version,
                            "baseline_flag": baseline_flag,
                            "metric_id": metric_id,
                            "metric_value": metrics.get(metric_id, ""),
                            "metric_ci_low": "",
                            "metric_ci_high": "",
                            "metric_unit": "",
                            "eval_split": "in_tissue",
                            "replicate_id": run_id,
                            "n_units": metrics.get("n_spots_in_tissue", ""),
                            "notes": notes,
                        },
                    )

            # Cross-method concordance on inferred proportions (sanity check; not ground-truth accuracy)
            concord_pairs: list[tuple[str, Path, str, Path, str]] = []
            if run_rctd and tang_status == "ok" and tangram_weights.exists() and rctd_weights.exists():
                concord_pairs.append(("rctd", rctd_weights, "tangram", tangram_weights, "RCTD vs Tangram"))
            if run_rctd and cell2_status == "ok" and cell2_weights.exists() and rctd_weights.exists():
                concord_pairs.append(("rctd", rctd_weights, "cell2location", cell2_weights, "RCTD vs cell2location"))
            if tang_status == "ok" and tangram_weights.exists() and cell2_status == "ok" and cell2_weights.exists():
                concord_pairs.append(("tangram", tangram_weights, "cell2location", cell2_weights, "Tangram vs cell2location"))

            for a_id, a_path, b_id, b_path2, label in concord_pairs:
                pair_token = f"pair={a_id}_vs_{b_id}"
                conc = _weights_concordance_pearson_mean(a_path, b_path2)
                if conc == conc:
                    n_units = (
                        (rctd.get("metrics") or {}).get("n_spots_in_tissue", "")
                        or (tang.get("metrics") or {}).get("n_spots_in_tissue", "")
                        or (cell2.get("metrics") or {}).get("n_spots_in_tissue", "")
                    )
                    write_tsv_row(
                        bc_path,
                        {
                            "dataset_id": dataset_id,
                            "modality": "Visium",
                            "method_id": "visium_deconvolution_pack",
                            "output_type": "celltype_proportions",
                            "reference_type": "method_compare",
                            "concordance_metric": "mean_pearson_by_cell_type",
                            "value": conc,
                            "ci_low": "",
                            "ci_high": "",
                            "n_units": n_units,
                            "notes": f"{pair_token}; {label} on shared cell types (per-cell-type Pearson, mean)",
                        },
                    )

                cos_med, cos_q25, cos_q75, cos_n = _weights_concordance_cosine_by_spot_summary(a_path, b_path2)
                if cos_med == cos_med and cos_n > 0:
                    write_tsv_row(
                        bc_path,
                        {
                            "dataset_id": dataset_id,
                            "modality": "Visium",
                            "method_id": "visium_deconvolution_pack",
                            "output_type": "celltype_proportions",
                            "reference_type": "method_compare",
                            "concordance_metric": "median_cosine_by_spot",
                            "value": cos_med,
                            "ci_low": cos_q25,
                            "ci_high": cos_q75,
                            "n_units": cos_n,
                            "notes": f"{pair_token}; {label} spotwise cosine similarity across shared cell types (L1-normalized), median and IQR",
                        },
                    )

            write_tsv_row(
                rt_path,
                {
                    "dataset_id": dataset_id,
                    "modality": "Visium",
                    "method_id": "deconvolution:rctd",
                    "run_id": run_id,
                    "status": "ok" if run_rctd else "skip",
                    "failure_type": "" if run_rctd else "runner_selection",
                    "wall_time_s": f"{rctd_wall:.3f}" if run_rctd else "",
                    "peak_ram_gb": "",
                    "peak_disk_gb": "",
                    "cpu_hours": "",
                    "gpu_hours": "",
                    "estimated_cost_usd": "",
                    "cost_model": "",
                    "notes": f"reference={reference_dataset_id};labels={labels_source}" + ("" if run_rctd else ";skipped_by_runner"),
                },
            )
            write_tsv_row(
                rt_path,
                {
                    "dataset_id": dataset_id,
                    "modality": "Visium",
                    "method_id": "deconvolution:tangram",
                    "run_id": run_id,
                    "status": "ok" if tang_status == "ok" else ("fail" if tang_status == "fail" else "skip"),
                    "failure_type": tang_failure if tang_status == "fail" else "",
                    "wall_time_s": "" if tang_status == "skip" else f"{tang_wall:.3f}",
                    "peak_ram_gb": "",
                    "peak_disk_gb": "",
                    "cpu_hours": "",
                    "gpu_hours": "",
                    "estimated_cost_usd": "",
                    "cost_model": "",
                    "notes": (
                        f"reference={reference_dataset_id};labels={labels_source};compute_tier={compute_tier};runner={runner_sel}"
                        + (f";error={tang_failure_msg}" if tang_status == "fail" and tang_failure_msg else "")
                    ),
                },
            )
            write_tsv_row(
                rt_path,
                {
                    "dataset_id": dataset_id,
                    "modality": "Visium",
                    "method_id": "deconvolution:cell2location",
                    "run_id": run_id,
                    "status": cell2_status,
                    "failure_type": cell2_failure,
                    "wall_time_s": "" if cell2_status == "skip" else f"{cell2_wall:.3f}",
                    "peak_ram_gb": "",
                    "peak_disk_gb": "",
                    "cpu_hours": "",
                    "gpu_hours": "",
                    "estimated_cost_usd": "",
                    "cost_model": "",
                    "notes": (
                        f"reference={reference_dataset_id};labels={labels_source};compute_tier={compute_tier};runner={runner_sel}"
                        + (f";error={cell2_failure_msg}" if cell2_status == "fail" and cell2_failure_msg else "")
                    ),
                },
            )

            if runner_sel == "cell2location" and cell2_exc is not None:
                raise cell2_exc

            if tang_exc is not None:
                raise tang_exc

            wall = time.time() - t0
            write_repro_check(
                run_id=run_id,
                dataset_id=dataset_id,
                stage=f"visium_{args.method_pack}",
                env_hash=env_hash,
                seed=seed,
                output_table_path="results/benchmarks/method_benchmark.tsv",
                notes=(
                    f"visium method-pack={args.method_pack};reference={reference_dataset_id};"
                    f"labels_source={labels_source};ref_labels_n={ref_label_meta.get('n_cells_labeled','')}"
                ),
                pass_flag=True,
                wall_time_s=wall,
            )

        write_action_contract_anchor()
        run_figures(outdir=args.outdir)
        build_audit_bundle(
            run_id,
            include_paths=[
                "schemas/action_schema_v1.json",
                "docs/FIGURE_PROVENANCE.tsv",
                "docs/CLAIMS.tsv",
                "results",
                "plots/publication",
                "logs",
            ],
        )
    except Exception as e:
        status = "fail"
        failure_type = type(e).__name__
        wall = time.time() - t0
        runtime_row = {
            "dataset_id": dataset_id,
            "modality": "Visium",
            "method_id": f"{args.method_pack}:visium",
            "run_id": run_id,
            "status": status,
            "failure_type": failure_type,
            "wall_time_s": f"{wall:.3f}",
            "peak_ram_gb": "",
            "peak_disk_gb": "",
            "cpu_hours": "",
            "gpu_hours": "",
            "estimated_cost_usd": "",
            "cost_model": "",
            "notes": str(e)[:2000],
        }
        write_tsv_row(ROOT / "results" / "benchmarks" / "runtime_cost_failure.tsv", runtime_row)
        raise

    print(f"OK: visium method-pack={args.method_pack} run_id={run_id}")
    return 0

def cmd_scrna(args: argparse.Namespace) -> int:
    run_id = make_run_id(f"scrna_{args.method_pack}")
    seed = int(args.seed)
    env = env_fingerprint()
    env_hash = build_env_hash(env)

    dataset_id = args.dataset_id
    input_dir_str = args.input_dir
    input_dirs: list[Path] = []
    if ";" in input_dir_str:
        input_dirs = [(ROOT / p.strip()).resolve() for p in input_dir_str.split(";") if p.strip()]
    elif "," in input_dir_str:
        # Comma is shell-safe and recommended when passing via `make` (semicolon can be treated as a command separator).
        input_dirs = [(ROOT / p.strip()).resolve() for p in input_dir_str.split(",") if p.strip()]
    else:
        input_dirs = [(ROOT / input_dir_str).resolve()]
    for d in input_dirs:
        if not d.exists():
            raise FileNotFoundError(f"--input-dir not found: {d}")

    annotate = args.annotate
    runner = getattr(args, "runner", "scanpy")
    compute_tier = getattr(args, "compute_tier", "cpu")
    scvi_n_hvg = int(getattr(args, "scvi_n_hvg", 2000))
    scvi_n_latent = int(getattr(args, "scvi_n_latent", 30))
    scvi_max_epochs = int(getattr(args, "scvi_max_epochs", 50))
    scvi_max_cells = int(getattr(args, "scvi_max_cells", 0))

    t0 = time.time()
    status = "ok"
    failure_type = ""
    try:
        stats = compute_basic_10x_stats(input_dirs[0]) if len(input_dirs) == 1 else None

        ds_path = ROOT / "results" / "dataset_summary.tsv"
        if dataset_id and not tsv_has_value(ds_path, "dataset_id", dataset_id):
            ds_row = {
                "dataset_id": dataset_id,
                "modality": "scRNA-seq",
                "organism": args.organism,
                "tissue": args.tissue,
                "assay_platform": "10x Chromium",
                "input_artifact": str(Path(args.input_dir)),
                "entrypoint": "cellranger_mtx",
                "role": "benchmark",
                "n_samples": "",
                "n_donors": "",
                "n_cells_or_spots": (stats["n_cells_or_spots"] if stats else ""),
                "n_genes": (stats["n_genes"] if stats else ""),
                "reference_genome": "",
                "primary_citation": "",
                "source_url": "",
                "license": "",
                "qc_summary": (
                    f"mtx_rows={stats['matrix_n_rows']};mtx_cols={stats['matrix_n_cols']};mtx_nnz={stats['matrix_n_entries']}"
                    if stats
                    else "multi-input integration run"
                ),
                "notes": "registered by scrna method-pack run",
            }
            write_tsv_row(ds_path, ds_row)
        mb_path = ROOT / "results" / "benchmarks" / "method_benchmark.tsv"
        bc_path = ROOT / "results" / "benchmarks" / "biological_output_concordance.tsv"
        rm_path = ROOT / "results" / "benchmarks" / "robustness_matrix.tsv"

        if len(input_dirs) >= 2:
            if args.method_pack != "advanced":
                raise ValueError("multi-input integration requires --method-pack advanced")

            batch_ids = [f"batch{i+1}" for i in range(len(input_dirs))]
            if runner == "scvi":
                out_int = _scrna_multi_batch_scvi_compare(
                    input_dirs=input_dirs,
                    batch_ids=batch_ids,
                    seed=seed,
                    annotate=annotate,
                    compute_tier=compute_tier,
                    scvi_n_hvg=scvi_n_hvg,
                    scvi_n_latent=scvi_n_latent,
                    scvi_max_epochs=scvi_max_epochs,
                    scvi_max_cells=scvi_max_cells,
                )
            else:
                out_int = _scrna_multi_batch_harmony_compare(
                    input_dirs=input_dirs,
                    batch_ids=batch_ids,
                    seed=seed,
                    annotate=annotate,
                )

            # method_benchmark: baseline vs harmony
            write_tsv_row(
                mb_path,
                {
                    "dataset_id": dataset_id,
                    "modality": "scRNA-seq",
                    "task": "integration",
                    "method_id": "scanpy-standard",
                    "method_version": out_int["versions"]["scanpy_stack"],
                    "baseline_flag": "1",
                    "metric_id": "batch_mixing_nn_frac_mean",
                    "metric_value": out_int["batch_mixing_baseline_mean"],
                    "metric_ci_low": "",
                    "metric_ci_high": "",
                    "metric_unit": "",
                    "eval_split": "all",
                    "replicate_id": run_id,
                    "n_units": out_int["n_cells_after_qc"],
                    "notes": out_int["notes"],
                },
            )
            if annotate == "celltypist" and str(out_int.get("label_purity_baseline_mean", "")) not in {"", "nan"}:
                write_tsv_row(
                    mb_path,
                    {
                        "dataset_id": dataset_id,
                        "modality": "scRNA-seq",
                        "task": "integration",
                        "method_id": "scanpy-standard",
                        "method_version": out_int["versions"]["scanpy_stack"],
                        "baseline_flag": "1",
                        "metric_id": "label_purity_nn_frac_mean",
                        "metric_value": out_int["label_purity_baseline_mean"],
                        "metric_ci_low": "",
                        "metric_ci_high": "",
                        "metric_unit": "",
                        "eval_split": "all",
                        "replicate_id": run_id,
                        "n_units": out_int["n_cells_after_qc"],
                        "notes": "proxy using pinned CellTypist labels (no tuning on target)",
                    },
                )
            write_tsv_row(
                mb_path,
                {
                    "dataset_id": dataset_id,
                    "modality": "scRNA-seq",
                    "task": "integration",
                    "method_id": "harmony",
                    "method_version": out_int["versions"]["harmonypy"],
                    "baseline_flag": "0",
                    "metric_id": "batch_mixing_nn_frac_mean",
                    "metric_value": out_int["batch_mixing_harmony_mean"],
                    "metric_ci_low": "",
                    "metric_ci_high": "",
                    "metric_unit": "",
                    "eval_split": "all",
                    "replicate_id": run_id,
                    "n_units": out_int["n_cells_after_qc"],
                    "notes": out_int["notes"],
                },
            )
            if annotate == "celltypist" and str(out_int.get("label_purity_harmony_mean", "")) not in {"", "nan"}:
                write_tsv_row(
                    mb_path,
                    {
                        "dataset_id": dataset_id,
                        "modality": "scRNA-seq",
                        "task": "integration",
                        "method_id": "harmony",
                        "method_version": out_int["versions"]["harmonypy"],
                        "baseline_flag": "0",
                        "metric_id": "label_purity_nn_frac_mean",
                        "metric_value": out_int["label_purity_harmony_mean"],
                        "metric_ci_low": "",
                        "metric_ci_high": "",
                        "metric_unit": "",
                        "eval_split": "all",
                        "replicate_id": run_id,
                        "n_units": out_int["n_cells_after_qc"],
                        "notes": "proxy using pinned CellTypist labels (no tuning on target)",
                    },
                )
            write_tsv_row(
                mb_path,
                {
                    "dataset_id": dataset_id,
                    "modality": "scRNA-seq",
                    "task": "integration",
                    "method_id": "harmony",
                    "method_version": out_int["versions"]["harmonypy"],
                    "baseline_flag": "0",
                    "metric_id": "ari_clusters_vs_baseline",
                    "metric_value": out_int["ari_clusters_baseline_vs_harmony"],
                    "metric_ci_low": "",
                    "metric_ci_high": "",
                    "metric_unit": "",
                    "eval_split": "all",
                    "replicate_id": run_id,
                    "n_units": out_int["n_cells_after_qc"],
                    "notes": "ARI between baseline and harmony clusters",
                },
            )

            # concordance
            if str(out_int["ari_clusters_baseline_vs_harmony"]) != "nan":
                write_tsv_row(
                    bc_path,
                    {
                        "dataset_id": dataset_id,
                        "modality": "scRNA-seq",
                        "method_id": "harmony",
                        "output_type": "cluster",
                        "reference_type": "scanpy-standard",
                        "concordance_metric": "ARI",
                        "value": out_int["ari_clusters_baseline_vs_harmony"],
                        "ci_low": "",
                        "ci_high": "",
                        "n_units": out_int["n_cells_after_qc"],
                        "notes": "baseline clusters vs harmony clusters",
                    },
                )

            # robustness: seed perturbation already handled in single-pack; for integration v0, record mixing delta as a robustness-style row
            delta = out_int["batch_mixing_harmony_mean"] - out_int["batch_mixing_baseline_mean"]
            write_tsv_row(
                rm_path,
                {
                    "dataset_id": dataset_id,
                    "modality": "scRNA-seq",
                    "method_id": "harmony",
                    "perturbation_id": "integration_vs_baseline",
                    "severity": "na",
                    "metric_id": "delta_batch_mixing_mean",
                    "metric_value": delta,
                    "delta_vs_nominal": "",
                    "pass": "",
                    "failure_reason": "",
                    "notes": "positive indicates more cross-batch neighbors on average",
                },
            )

            runtime_path = ROOT / "results" / "benchmarks" / "runtime_cost_failure.tsv"
            write_tsv_row(
                runtime_path,
                {
                    "dataset_id": dataset_id,
                    "modality": "scRNA-seq",
                    "method_id": "scanpy-standard",
                    "run_id": run_id,
                    "status": "ok",
                    "failure_type": "",
                    "wall_time_s": f"{out_int['wall_baseline_s']:.3f}",
                    "peak_ram_gb": "",
                    "peak_disk_gb": "",
                    "cpu_hours": "",
                    "gpu_hours": "",
                    "estimated_cost_usd": "",
                    "cost_model": "",
                    "notes": "integration scenario (baseline stage only)",
                },
            )
            write_tsv_row(
                runtime_path,
                {
                    "dataset_id": dataset_id,
                    "modality": "scRNA-seq",
                    "method_id": "harmony",
                    "run_id": run_id,
                    "status": "ok",
                    "failure_type": "",
                    "wall_time_s": f"{out_int['wall_harmony_s']:.3f}",
                    "peak_ram_gb": "",
                    "peak_disk_gb": "",
                    "cpu_hours": "",
                    "gpu_hours": "",
                    "estimated_cost_usd": "",
                    "cost_model": "",
                    "notes": "integration stage (Harmony on PCA)",
                },
            )

            if runner == "scvi":
                # scVI benchmark rows
                write_tsv_row(
                    mb_path,
                    {
                        "dataset_id": dataset_id,
                        "modality": "scRNA-seq",
                        "task": "integration",
                        "method_id": "scvi",
                        "method_version": f"{out_int['versions'].get('scvi_tools','')};{out_int['versions'].get('torch','')}",
                        "baseline_flag": "0",
                        "metric_id": "batch_mixing_nn_frac_mean",
                        "metric_value": out_int["batch_mixing_scvi_mean"],
                        "metric_ci_low": "",
                        "metric_ci_high": "",
                        "metric_unit": "",
                        "eval_split": "all",
                        "replicate_id": run_id,
                        "n_units": out_int["n_cells_after_qc"],
                        "notes": out_int["notes"],
                    },
                )
                if annotate == "celltypist" and str(out_int.get("label_purity_scvi_mean", "")) not in {"", "nan"}:
                    write_tsv_row(
                        mb_path,
                        {
                            "dataset_id": dataset_id,
                            "modality": "scRNA-seq",
                            "task": "integration",
                            "method_id": "scvi",
                            "method_version": f"{out_int['versions'].get('scvi_tools','')};{out_int['versions'].get('torch','')}",
                            "baseline_flag": "0",
                            "metric_id": "label_purity_nn_frac_mean",
                            "metric_value": out_int["label_purity_scvi_mean"],
                            "metric_ci_low": "",
                            "metric_ci_high": "",
                            "metric_unit": "",
                            "eval_split": "all",
                            "replicate_id": run_id,
                            "n_units": out_int["n_cells_after_qc"],
                            "notes": "proxy using pinned CellTypist labels (no tuning on target)",
                        },
                    )
                write_tsv_row(
                    mb_path,
                    {
                        "dataset_id": dataset_id,
                        "modality": "scRNA-seq",
                        "task": "integration",
                        "method_id": "scvi",
                        "method_version": f"{out_int['versions'].get('scvi_tools','')};{out_int['versions'].get('torch','')}",
                        "baseline_flag": "0",
                        "metric_id": "ari_clusters_vs_harmony",
                        "metric_value": out_int["ari_clusters_scvi_vs_harmony"],
                        "metric_ci_low": "",
                        "metric_ci_high": "",
                        "metric_unit": "",
                        "eval_split": "all",
                        "replicate_id": run_id,
                        "n_units": out_int["n_cells_after_qc"],
                        "notes": "ARI between scVI clusters and harmony clusters",
                    },
                )
                # concordance (scVI vs harmony)
                if str(out_int["ari_clusters_scvi_vs_harmony"]) != "nan":
                    write_tsv_row(
                        bc_path,
                        {
                            "dataset_id": dataset_id,
                            "modality": "scRNA-seq",
                            "method_id": "scvi",
                            "output_type": "cluster",
                            "reference_type": "harmony",
                            "concordance_metric": "ARI",
                            "value": out_int["ari_clusters_scvi_vs_harmony"],
                            "ci_low": "",
                            "ci_high": "",
                            "n_units": out_int["n_cells_after_qc"],
                            "notes": "scVI clusters vs harmony clusters",
                        },
                    )
                # robustness (HVG/2) for scVI
                if str(out_int.get("ari_clusters_scvi_vs_scvi_hvg_half", "")) not in {"", "nan"}:
                    ari_h = float(out_int["ari_clusters_scvi_vs_scvi_hvg_half"])
                    pass_flag = "1" if ari_h >= 0.90 else "0"
                    write_tsv_row(
                        rm_path,
                        {
                            "dataset_id": dataset_id,
                            "modality": "scRNA-seq",
                            "method_id": "scvi",
                            "perturbation_id": "hvg_half",
                            "severity": "low",
                            "metric_id": "ARI_cluster",
                            "metric_value": ari_h,
                            "delta_vs_nominal": f"{ari_h - 1.0:.4f}",
                            "pass": pass_flag,
                            "failure_reason": "" if pass_flag == "1" else "ARI<0.90 under HVG/2 perturbation",
                            "notes": "scVI clustering stability: HVG vs HVG/2",
                        },
                    )
                # runtime rows
                write_tsv_row(
                    runtime_path,
                    {
                        "dataset_id": dataset_id,
                        "modality": "scRNA-seq",
                        "method_id": "scvi",
                        "run_id": run_id,
                        "status": "ok",
                        "failure_type": "",
                        "wall_time_s": f"{out_int['wall_scvi_s']:.3f}",
                        "peak_ram_gb": "",
                        "peak_disk_gb": "",
                        "cpu_hours": "",
                        "gpu_hours": "",
                        "estimated_cost_usd": "",
                        "cost_model": "",
                        "notes": f"compute_tier={compute_tier};hvg={scvi_n_hvg};latent={scvi_n_latent};epochs={scvi_max_epochs}",
                    },
                )
                write_tsv_row(
                    runtime_path,
                    {
                        "dataset_id": dataset_id,
                        "modality": "scRNA-seq",
                        "method_id": "scvi",
                        "run_id": run_id,
                        "status": "ok",
                        "failure_type": "",
                        "wall_time_s": f"{out_int['wall_scvi_hvg_half_s']:.3f}",
                        "peak_ram_gb": "",
                        "peak_disk_gb": "",
                        "cpu_hours": "",
                        "gpu_hours": "",
                        "estimated_cost_usd": "",
                        "cost_model": "",
                        "notes": "perturbation=hvg_half",
                    },
                )

            wall = time.time() - t0
            write_repro_check(
                run_id=run_id,
                dataset_id=dataset_id,
                stage="scrna_integration_compare",
                env_hash=env_hash,
                seed=seed,
                output_table_path="results/benchmarks/method_benchmark.tsv",
                notes=f"multi-input integration compare; runner={runner}; annotate={annotate}; inputs={len(input_dirs)}",
                pass_flag=True,
                wall_time_s=wall,
            )
        else:
            if runner == "seurat":
                if annotate != "none":
                    raise ValueError("Seurat baseline runner currently supports --annotate none (use --runner scanpy for CellTypist).")
                out = _run_scrna_pack_seurat(matrix_dir=input_dirs[0], seed=seed)
                method_id = "seurat-v5-standard"
                method_version = f"Seurat={out.get('versions',{}).get('seurat','')};SeuratObject={out.get('versions',{}).get('seuratobject','')};R={out.get('versions',{}).get('r','')}"
            else:
                out = _run_scrna_pack_scanpy(matrix_dir=input_dirs[0], seed=seed, annotate=annotate)
                method_id = "scanpy-standard"
                method_version = out["versions"]["scanpy_stack"]

            wall = time.time() - t0

            # Benchmarks (qc + clustering)
            for metric_id in [
                "n_cells_after_qc",
                "median_total_counts",
                "median_n_genes_by_counts",
                "median_pct_counts_mt",
                "n_clusters",
            ]:
                write_tsv_row(
                    mb_path,
                    {
                        "dataset_id": dataset_id,
                        "modality": "scRNA-seq",
                        "task": "qc+cluster",
                        "method_id": method_id,
                        "method_version": method_version,
                        "baseline_flag": "1" if args.method_pack == "baseline" else "0",
                        "metric_id": metric_id,
                        "metric_value": (out.get("metrics") or {}).get(metric_id, ""),
                        "metric_ci_low": "",
                        "metric_ci_high": "",
                        "metric_unit": "",
                        "eval_split": "all",
                        "replicate_id": run_id,
                        "n_units": (out.get("metrics") or {}).get("n_cells_after_qc", ""),
                        "notes": (out.get("notes") or f"seed={seed}"),
                    },
                )

            # Annotation (optional)
            if runner == "scanpy" and annotate == "celltypist":
                anno = out.get("annotation") or {}
                if "annotation_error" in anno:
                    metric_rows = [
                        {
                            "metric_id": "annotation_error",
                            "metric_value": anno.get("annotation_error", ""),
                        }
                    ]
                else:
                    metric_rows = [
                        {
                            "metric_id": "n_cell_types_pred",
                            "metric_value": anno.get("n_cell_types_pred", ""),
                        }
                    ]
                for r in metric_rows:
                    write_tsv_row(
                        mb_path,
                        {
                            "dataset_id": dataset_id,
                            "modality": "scRNA-seq",
                            "task": "annotation",
                            "method_id": "celltypist",
                            "method_version": out["versions"]["annotation_stack"],
                            "baseline_flag": "0",
                            "metric_id": r["metric_id"],
                            "metric_value": r["metric_value"],
                            "metric_ci_low": "",
                            "metric_ci_high": "",
                            "metric_unit": "",
                            "eval_split": "all",
                            "replicate_id": run_id,
                            "n_units": (out.get("metrics") or {}).get("n_cells_after_qc", ""),
                            "notes": "model=Immune_All_Low.pkl;majority_voting=TRUE (PBMC-style default)",
                        },
                    )

            # Biological output concordance (lightweight v0)
            conc = out.get("concordance") or {}
            nmi = conc.get("nmi_celltypist_vs_cluster", "")
            if runner == "scanpy" and annotate == "celltypist" and nmi != "" and str(nmi) != "nan":
                write_tsv_row(
                    bc_path,
                    {
                        "dataset_id": dataset_id,
                        "modality": "scRNA-seq",
                        "method_id": method_id,
                        "output_type": "celltypist_label",
                        "reference_type": "cluster",
                        "concordance_metric": "NMI",
                        "value": nmi,
                        "ci_low": "",
                        "ci_high": "",
                        "n_units": (out.get("metrics") or {}).get("n_cells_after_qc", ""),
                        "notes": "within-run concordance between predicted labels and unsupervised clusters",
                    },
                )

            # Robustness matrix (v0: seed perturbation stability)
            ari = conc.get("ari_cluster_seed_plus_1", "")
            if ari != "" and str(ari) != "nan":
                pass_flag = "1" if float(ari) >= 0.90 else "0"
                write_tsv_row(
                    rm_path,
                    {
                        "dataset_id": dataset_id,
                        "modality": "scRNA-seq",
                        "method_id": method_id,
                        "perturbation_id": "seed_plus_1",
                        "severity": "low",
                        "metric_id": "ARI_cluster",
                        "metric_value": ari,
                        "delta_vs_nominal": f"{float(ari) - 1.0:.4f}",
                        "pass": pass_flag,
                        "failure_reason": "" if pass_flag == "1" else "ARI<0.90 under seed perturbation",
                        "notes": "same neighbors graph; Leiden random_state seed vs seed+1",
                    },
                )

            runtime_row = {
                "dataset_id": dataset_id,
                "modality": "scRNA-seq",
                "method_id": f"{args.method_pack}:{method_id}",
                "run_id": run_id,
                "status": "ok",
                "failure_type": "",
                "wall_time_s": f"{wall:.3f}",
                "peak_ram_gb": "",
                "peak_disk_gb": "",
                "cpu_hours": "",
                "gpu_hours": "",
                "estimated_cost_usd": "",
                "cost_model": "",
                "notes": "v0 scanpy pack",
            }
            write_tsv_row(ROOT / "results" / "benchmarks" / "runtime_cost_failure.tsv", runtime_row)

            write_repro_check(
                run_id=run_id,
                dataset_id=dataset_id,
                stage=f"scrna_{args.method_pack}",
                env_hash=env_hash,
                seed=seed,
                output_table_path="results/benchmarks/method_benchmark.tsv",
                notes=f"scRNA method-pack={args.method_pack}; runner={runner}; annotate={annotate}",
                pass_flag=True,
                wall_time_s=wall,
            )

        write_action_contract_anchor()
        run_figures(outdir=args.outdir)
        build_audit_bundle(
            run_id,
            include_paths=[
                "schemas/action_schema_v1.json",
                "docs/FIGURE_PROVENANCE.tsv",
                "docs/CLAIMS.tsv",
                "results",
                "plots/publication",
                "logs",
            ],
        )
    except Exception as e:
        status = "fail"
        if isinstance(e, ComputeGateError):
            failure_type = e.failure_type
        else:
            failure_type = "runtime_error"
        wall = time.time() - t0
        requested = "scanpy-standard"
        if len(input_dirs) >= 2 and args.method_pack == "advanced":
            requested = "scvi" if runner == "scvi" else "harmony"
        elif runner == "seurat":
            requested = "seurat-v5-standard"
        runtime_row = {
            "dataset_id": dataset_id,
            "modality": "scRNA-seq",
            "method_id": f"{args.method_pack}:{requested}",
            "run_id": run_id,
            "status": status,
            "failure_type": failure_type,
            "wall_time_s": f"{wall:.3f}",
            "peak_ram_gb": "",
            "peak_disk_gb": "",
            "cpu_hours": "",
            "gpu_hours": "",
            "estimated_cost_usd": "",
            "cost_model": "",
            "notes": f"{type(e).__name__}: {str(e)[:1900]}",
        }
        write_tsv_row(ROOT / "results" / "benchmarks" / "runtime_cost_failure.tsv", runtime_row)
        raise

    print(f"OK: scrna method-pack={args.method_pack} run_id={run_id}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="run.py", description="Pipeline entrypoint (skeleton + smoke).")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_skeleton = sub.add_parser("skeleton", help="Validate contracts and generate placeholder deliverables.")
    p_skeleton.set_defaults(func=cmd_skeleton)

    p_smoke = sub.add_parser("smoke", help="Run a minimal real data path and record audit metadata.")
    p_smoke.add_argument("--outdir", default="plots/publication", help="Output dir for figures.")
    p_smoke.add_argument(
        "--input-dir",
        default="data/smoke/pbmc_toy/filtered_feature_bc_matrix",
        help="Workspace-relative path to filtered_feature_bc_matrix/ (default: bundled toy matrix).",
    )
    p_smoke.add_argument("--dataset-id", default="", help="Dataset id for logging (default: SMOKE_TOY_10X_MTX).")
    p_smoke.add_argument("--url", default="", help="Optional remote tar.gz URL (if you prefer to download).")
    p_smoke.set_defaults(func=cmd_smoke)

    p_fig = sub.add_parser("figures", help="Generate figures from current results tables.")
    p_fig.add_argument("--outdir", default="plots/publication", help="Output dir for figures.")
    p_fig.set_defaults(func=cmd_figures)

    p_audit = sub.add_parser("audit", help="Build an audit bundle from current workspace.")
    p_audit.add_argument("--run-id", default="", help="Run id (default: auto).")
    p_audit.add_argument("--include", action="append", default=["results", "plots/publication", "docs"], help="Paths to include.")
    p_audit.set_defaults(func=cmd_audit)

    p_review = sub.add_parser("review-bundle", help="Alias for audit bundle creation.")
    p_review.add_argument("--run-id", default="", help="Run id (default: auto).")
    p_review.add_argument("--include", action="append", default=["results", "plots/publication", "docs"], help="Paths to include.")
    p_review.set_defaults(func=cmd_review_bundle)

    p_scrna = sub.add_parser("scrna", help="Run scRNA method pack (baseline/advanced) and append benchmark rows.")
    p_scrna.add_argument("--outdir", default="plots/publication", help="Output dir for figures.")
    p_scrna.add_argument("--input-dir", required=True, help="Workspace-relative path to filtered_feature_bc_matrix/.")
    p_scrna.add_argument("--dataset-id", required=True, help="Dataset id (used in results tables).")
    p_scrna.add_argument("--method-pack", choices=["baseline", "advanced"], default="baseline")
    p_scrna.add_argument(
        "--runner",
        choices=["scanpy", "seurat", "scvi"],
        default="scanpy",
        help="Runner implementation. scvi is only supported for multi-input `--method-pack advanced` runs.",
    )
    p_scrna.add_argument("--annotate", choices=["none", "celltypist"], default="celltypist")
    p_scrna.add_argument("--compute-tier", choices=["cpu", "gpu"], default="cpu", help="Compute tier hint for advanced methods.")
    p_scrna.add_argument("--scvi-n-hvg", default="2000", help="scVI HVG count (advanced pack; runner scvi).")
    p_scrna.add_argument("--scvi-n-latent", default="30", help="scVI latent dimension (advanced pack; runner scvi).")
    p_scrna.add_argument("--scvi-max-epochs", default="50", help="scVI max epochs (advanced pack; runner scvi).")
    p_scrna.add_argument("--scvi-max-cells", default="0", help="Downsample total cells for scVI speed (0=disable).")
    p_scrna.add_argument("--seed", default="0")
    p_scrna.add_argument("--organism", default="human")
    p_scrna.add_argument("--tissue", default="PBMC")
    p_scrna.set_defaults(func=cmd_scrna)

    p_visium = sub.add_parser("visium", help="Run Visium method pack (baseline) starting from Space Ranger outputs.")
    p_visium.add_argument("--outdir", default="plots/publication", help="Output dir for figures.")
    p_visium.add_argument(
        "--input-dir",
        required=True,
        help=(
            "Workspace-relative path to a Space Ranger output directory containing "
            "filtered_feature_bc_matrix/ (or *filtered_feature_bc_matrix.h5) and spatial/."
        ),
    )
    p_visium.add_argument("--dataset-id", required=True, help="Dataset id (used in results tables).")
    p_visium.add_argument("--method-pack", choices=["baseline", "deconvolution"], default="baseline")
    p_visium.add_argument(
        "--reference-scrna-dir",
        default="",
        help="10x scRNA reference `filtered_feature_bc_matrix/` dir (required for --method-pack deconvolution).",
    )
    p_visium.add_argument(
        "--reference-dataset-id",
        default="",
        help="Reference dataset id (written into notes/anchors for deconvolution runs).",
    )
    p_visium.add_argument(
        "--reference-labels-tsv",
        default="",
        help="Optional TSV with columns {barcode, label} for the scRNA reference (bypasses CellTypist).",
    )
    p_visium.add_argument("--compute-tier", choices=["cpu", "gpu"], default="cpu", help="Compute tier hint for advanced methods.")
    p_visium.add_argument(
        "--runner",
        choices=["default", "rctd", "tangram", "cell2location", "all"],
        default="default",
        help=(
            "Deconvolution runner selector (only used with --method-pack deconvolution). "
            "default=rctd+tangram; rctd=baseline only; all=rctd+tangram+cell2location."
        ),
    )
    p_visium.add_argument("--tangram-n-hvg", default="2000", help="Tangram HVG count (deconvolution pack).")
    p_visium.add_argument("--tangram-max-cells", default="3000", help="Tangram max reference cells (deconvolution pack).")
    p_visium.add_argument("--cell2location-max-epochs", default="2000", help="cell2location max epochs (deconvolution pack).")
    p_visium.add_argument(
        "--cell2location-regression-max-epochs",
        default="250",
        help="cell2location RegressionModel max epochs for reference signatures (deconvolution pack).",
    )
    p_visium.add_argument("--cell2location-num-samples", default="200", help="cell2location posterior samples (deconvolution pack).")
    p_visium.add_argument(
        "--cell2location-n-cells-per-location",
        default="30",
        help="cell2location N_cells_per_location hyperparam (deconvolution pack).",
    )
    p_visium.add_argument(
        "--cell2location-detection-alpha",
        default="20.0",
        help="cell2location detection_alpha hyperparam (deconvolution pack).",
    )
    p_visium.add_argument("--cell2location-max-cells", default="20000", help="cell2location max scRNA cells (0=disable).")
    p_visium.add_argument("--cell2location-max-spots", default="8000", help="cell2location max in-tissue spots (0=disable).")
    p_visium.add_argument("--seed", default="0")
    p_visium.add_argument("--organism", default="human")
    p_visium.add_argument("--tissue", default="Visium")
    p_visium.set_defaults(func=cmd_visium)

    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
