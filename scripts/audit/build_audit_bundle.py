#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import time
import zipfile
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]


def run_cmd(args: list[str]) -> tuple[int, str]:
    try:
        out = subprocess.check_output(args, cwd=str(ROOT), stderr=subprocess.STDOUT)
        return 0, out.decode("utf-8", errors="replace").strip()
    except subprocess.CalledProcessError as e:
        out = (e.output or b"").decode("utf-8", errors="replace").strip()
        return e.returncode, out
    except FileNotFoundError as e:
        return 127, str(e)


def sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def env_snapshot() -> dict[str, Any]:
    code, commit = run_cmd(["git", "rev-parse", "HEAD"])
    code_docker, docker_ver = run_cmd(["docker", "--version"])
    os_release = ""
    os_release_path = Path("/etc/os-release")
    if os_release_path.exists():
        try:
            os_release = os_release_path.read_text(encoding="utf-8", errors="replace").strip()
        except Exception:
            os_release = ""
    return {
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "git_commit": commit if code == 0 else "UNKNOWN",
        "python_version": sys.version.replace("\n", " "),
        "platform": platform.platform(),
        "uname": " ".join(platform.uname()),
        "docker_version": docker_ver if code_docker == 0 else "",
        "os_release": os_release,
    }


def copy_includes(bundle_dir: Path, includes: list[str]) -> list[Path]:
    copied: list[Path] = []
    for inc in includes:
        src = (ROOT / inc).resolve()
        if not src.exists():
            continue
        rel = src.relative_to(ROOT) if str(src).startswith(str(ROOT)) else Path("external") / src.name
        dst = bundle_dir / "workspace" / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        if src.is_dir():
            if dst.exists():
                shutil.rmtree(dst)
            ignore = None
            # Prevent recursive self-inclusion when users include "docs/" in a bundle
            # (bundle lives under docs/audit_runs/<run_id>/).
            if rel == Path("docs"):
                ignore = shutil.ignore_patterns("audit_runs", "review_bundle")
            shutil.copytree(src, dst, ignore=ignore)
        else:
            shutil.copy2(src, dst)
        copied.append(dst)
    return copied


def write_checksums(bundle_dir: Path) -> Path:
    manifest = bundle_dir / "checksums.sha256"
    entries: list[str] = []
    for p in sorted((bundle_dir / "workspace").rglob("*")):
        if p.is_file():
            rel = p.relative_to(bundle_dir)
            entries.append(f"{sha256_path(p)}  {rel}")
    manifest.write_text("\n".join(entries) + "\n", encoding="utf-8")
    return manifest


def zip_bundle(bundle_dir: Path) -> Path:
    zip_path = bundle_dir.with_suffix(".zip")
    if zip_path.exists():
        zip_path.unlink()
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for p in sorted(bundle_dir.rglob("*")):
            if p.is_file():
                zf.write(p, arcname=str(p.relative_to(bundle_dir.parent)))
    return zip_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Build audit bundle under docs/audit_runs/<run_id>/")
    parser.add_argument("--run-id", required=True, help="Run identifier.")
    parser.add_argument("--include", action="append", default=[], help="Workspace-relative path to include (repeatable).")
    args = parser.parse_args()

    run_id = args.run_id
    bundle_dir = ROOT / "docs" / "audit_runs" / run_id
    bundle_dir.mkdir(parents=True, exist_ok=True)

    (bundle_dir / "meta.json").write_text(json.dumps(env_snapshot(), indent=2), encoding="utf-8")
    copy_includes(bundle_dir, args.include)
    write_checksums(bundle_dir)
    zip_path = zip_bundle(bundle_dir)

    print(str(bundle_dir))
    print(str(zip_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
