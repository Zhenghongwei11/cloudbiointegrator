#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import os
import shutil
import tarfile
import subprocess
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".partial")
    ua = "Mozilla/5.0"

    curl = shutil.which("curl")
    if curl:
        # If a partial download exists, resume it (when the server supports range requests).
        # curl is substantially more robust for large downloads (retries, TLS, resumes).
        resume_args: list[str] = []
        if tmp.exists() and tmp.stat().st_size > 0:
            resume_args = ["-C", "-"]
        else:
            if tmp.exists():
                tmp.unlink()
        cmd = [
            curl,
            "-L",
            "--fail",
            "--retry",
            "5",
            "--retry-delay",
            "3",
            "--retry-all-errors",
            "--connect-timeout",
            "30",
            "-H",
            f"User-Agent: {ua}",
            *resume_args,
            "-o",
            str(tmp),
            url,
        ]
        subprocess.check_call(cmd)
        tmp.replace(dest)
        return

    req = urllib.request.Request(url, headers={"User-Agent": ua})
    for attempt in range(1, 4):
        if tmp.exists():
            tmp.unlink()
        try:
            with urllib.request.urlopen(req, timeout=300) as r, tmp.open("wb") as f:
                expected_len = 0
                try:
                    expected_len = int(r.headers.get("Content-Length") or "0")
                except Exception:
                    expected_len = 0
                written = 0
                while True:
                    chunk = r.read(1024 * 1024)
                    if not chunk:
                        break
                    f.write(chunk)
                    written += len(chunk)
            if expected_len and written != expected_len:
                raise IOError(f"incomplete download: expected {expected_len} bytes, got {written}")
            tmp.replace(dest)
            return
        except Exception:
            if tmp.exists():
                tmp.unlink()
            if attempt >= 3:
                raise


def extract_tar_gz(archive: Path, dest_dir: Path) -> None:
    dest_dir.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive, "r:gz") as tf:
        tf.extractall(path=dest_dir)


def _pick_10x_matrix_dir(root: Path) -> Path | None:
    """
    Find a directory that looks like a 10x matrix folder:
    - matrix.mtx(.gz)
    - features.tsv(.gz) OR genes.tsv(.gz)
    - barcodes.tsv(.gz)
    """
    candidates: list[Path] = []
    for p in root.rglob("*"):
        if not p.is_dir():
            continue
        has_mtx = (p / "matrix.mtx").exists() or (p / "matrix.mtx.gz").exists()
        has_feats = (
            (p / "features.tsv").exists()
            or (p / "features.tsv.gz").exists()
            or (p / "genes.tsv").exists()
            or (p / "genes.tsv.gz").exists()
        )
        has_bcs = (p / "barcodes.tsv").exists() or (p / "barcodes.tsv.gz").exists()
        if has_mtx and has_feats and has_bcs:
            candidates.append(p)
    if not candidates:
        return None
    for p in candidates:
        if p.name == "filtered_feature_bc_matrix":
            return p
    return sorted(candidates)[0]


def ensure_filtered_feature_bc_matrix(extract_root: Path) -> None:
    """
    Some older 10x public tarballs (e.g., PBMC3k 2016) extract to
    `filtered_gene_bc_matrices/<genome>/`. Our pipeline standardizes on
    `filtered_feature_bc_matrix/` (modern Cell Ranger convention).
    """
    ff = extract_root / "filtered_feature_bc_matrix"
    if ff.exists():
        return
    fg = extract_root / "filtered_gene_bc_matrices"
    if not fg.exists():
        return
    src = _pick_10x_matrix_dir(fg)
    if src is None:
        return

    ff.mkdir(parents=True, exist_ok=True)
    for name in ["matrix.mtx", "matrix.mtx.gz", "barcodes.tsv", "barcodes.tsv.gz"]:
        p = src / name
        if p.exists():
            shutil.copy2(p, ff / name)
    if (src / "features.tsv.gz").exists():
        shutil.copy2(src / "features.tsv.gz", ff / "features.tsv.gz")
    elif (src / "features.tsv").exists():
        shutil.copy2(src / "features.tsv", ff / "features.tsv")
    elif (src / "genes.tsv.gz").exists():
        shutil.copy2(src / "genes.tsv.gz", ff / "features.tsv.gz")
    elif (src / "genes.tsv").exists():
        shutil.copy2(src / "genes.tsv", ff / "features.tsv")


def _symlink_or_copy_dir(src: Path, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() or dest.is_symlink():
        if dest.is_symlink() or dest.is_file():
            dest.unlink()
        else:
            shutil.rmtree(dest)
    try:
        # Prefer a relative symlink so the derived layout works both on-host
        # and inside containers where the repo may be mounted at a different
        # absolute path (e.g., /home/... vs /app).
        rel = os.path.relpath(src, start=dest.parent)
        os.symlink(rel, dest, target_is_directory=True)
    except Exception:
        shutil.copytree(src, dest)


def find_manifest_row(dataset_id: str) -> dict[str, str]:
    manifest = ROOT / "data" / "manifest.tsv"
    with manifest.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            if row.get("dataset_id") == dataset_id:
                return {k: (v or "") for k, v in row.items()}
    raise SystemExit(f"dataset_id not found in data/manifest.tsv: {dataset_id}")


def main() -> int:
    p = argparse.ArgumentParser(description="Fetch a dataset from data/manifest.tsv and (optionally) extract it.")
    p.add_argument("--dataset-id", required=True)
    p.add_argument("--force", action="store_true", help="Re-download even if local file exists.")
    p.add_argument(
        "--extract",
        action="store_true",
        help="If the dataset includes tarball(s) (.tar.gz/.tgz), extract them next to the archive. Non-tar files are left as-is.",
    )
    args = p.parse_args()

    row = find_manifest_row(args.dataset_id)
    url_raw = row.get("source_url", "").strip()
    local_raw = row.get("local_path", "").strip()
    sha_raw = row.get("sha256", "").strip()
    bytes_raw = row.get("bytes", "").strip()

    if not url_raw or not local_raw:
        raise SystemExit("manifest row missing source_url or local_path")

    urls = [u.strip() for u in url_raw.split(";") if u.strip()]
    locals_ = [p.strip() for p in local_raw.split(";") if p.strip()]
    shas = [s.strip() for s in sha_raw.split(";")] if sha_raw else ["" for _ in urls]
    sizes = [s.strip() for s in bytes_raw.split(";")] if bytes_raw else ["" for _ in urls]

    if len(urls) != len(locals_):
        raise SystemExit("manifest row has mismatched counts: source_url vs local_path (use ';' to separate)")
    if sha_raw and len(shas) != len(urls):
        raise SystemExit("manifest row has mismatched counts: sha256 vs source_url (use ';' to separate)")
    if bytes_raw and len(sizes) != len(urls):
        raise SystemExit("manifest row has mismatched counts: bytes vs source_url (use ';' to separate)")

    for i, (url, local_path) in enumerate(zip(urls, locals_)):
        expected_sha = shas[i] if i < len(shas) else ""
        expected_bytes = sizes[i] if i < len(sizes) else ""

        dest = ROOT / local_path
        if args.force and dest.exists():
            dest.unlink()

        if not dest.exists():
            print(f"Downloading {args.dataset_id} [{i+1}/{len(urls)}] -> {dest}")
            download(url, dest)
        else:
            print(f"OK: already exists: {dest}")

        got_sha = sha256_path(dest)
        got_bytes = dest.stat().st_size

        if expected_sha and got_sha != expected_sha:
            raise SystemExit(f"sha256 mismatch for {dest} expected={expected_sha} got={got_sha}")
        if expected_bytes and int(expected_bytes) != got_bytes:
            raise SystemExit(f"bytes mismatch for {dest} expected={expected_bytes} got={got_bytes}")

        print(f"OK: sha256={got_sha}")
        print(f"OK: bytes={got_bytes}")

        if args.extract:
            # Some datasets (e.g., 10x Visium public examples) ship as a mix of a
            # matrix file (often .h5) and a companion spatial tarball. In that
            # case, --extract should unpack only the tarball(s) and keep other
            # files untouched.
            if dest.name.endswith(".tar.gz") or dest.name.endswith(".tgz"):
                out_dir = dest.parent
                print(f"Extracting {dest} -> {out_dir}")
                extract_tar_gz(dest, out_dir)
                ensure_filtered_feature_bc_matrix(out_dir)
                print("OK: extracted")
            else:
                print(f"OK: no extract needed for non-tar file: {dest.name}")

    # Derived convenience: materialize a stable on-disk multi-batch layout for integration compares.
    if args.extract and args.dataset_id == "10x_PBMC3K_PBMC10K_V3_INTEGRATION_PAIR_S3":
        batch_root = ROOT / "data" / "smoke" / "pbmc_integration_pair_real"
        src1 = ROOT / "data" / "smoke" / "pbmc3k_real" / "filtered_feature_bc_matrix"
        src2 = ROOT / "data" / "smoke" / "pbmc10k_v3_real" / "filtered_feature_bc_matrix"
        if not src1.exists():
            raise SystemExit(f"expected batch1 matrix dir not found (extract PBMC3k first): {src1}")
        if not src2.exists():
            raise SystemExit(f"expected batch2 matrix dir not found (extract PBMC10k v3 first): {src2}")
        _symlink_or_copy_dir(src1, batch_root / "batch1" / "filtered_feature_bc_matrix")
        _symlink_or_copy_dir(src2, batch_root / "batch2" / "filtered_feature_bc_matrix")
        print(f"OK: materialized multi-batch layout at {batch_root}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
