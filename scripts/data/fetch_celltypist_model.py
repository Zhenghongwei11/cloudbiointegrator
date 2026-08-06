#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
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
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as r, dest.open("wb") as f:
        shutil.copyfileobj(r, f)


def main() -> int:
    p = argparse.ArgumentParser(description="Download a single CellTypist model (avoid downloading all models).")
    p.add_argument("--model", default="Immune_All_Low.pkl", help="Model filename from the CellTypist model list.")
    p.add_argument(
        "--dest",
        default="data/references/celltypist",
        help="Destination directory (workspace-relative) for storing the model file.",
    )
    p.add_argument("--force", action="store_true")
    args = p.parse_args()

    model_file = args.model
    dest_dir = (ROOT / args.dest).resolve()
    dest_path = dest_dir / model_file

    if dest_path.exists() and not args.force:
        print(f"OK: already exists: {dest_path}")
        print(f"sha256={sha256_path(dest_path)}")
        return 0

    index_url = "https://celltypist.cog.sanger.ac.uk/models/models.json"
    req = urllib.request.Request(index_url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        obj = json.loads(r.read().decode("utf-8"))
    models = obj.get("models", [])
    url = ""
    for m in models:
        if (m.get("filename") or "") == model_file:
            url = m.get("url") or ""
            break
    if not url:
        raise SystemExit(f"model not found in models.json: {model_file}")

    print(f"Downloading {model_file} -> {dest_path}")
    download(url, dest_path)
    print(f"OK: sha256={sha256_path(dest_path)}")
    print(f"OK: bytes={dest_path.stat().st_size}")
    print(f"source_url={url}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

