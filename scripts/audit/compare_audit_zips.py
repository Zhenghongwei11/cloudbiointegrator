#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import zipfile
from pathlib import Path
from typing import Any


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _find_one(z: zipfile.ZipFile, suffix: str) -> str:
    matches = [n for n in z.namelist() if n.endswith(suffix)]
    if not matches:
        raise FileNotFoundError(f"{suffix} not found in audit zip")
    if len(matches) > 1:
        # Pick the first; audit zips are expected to contain a single run root.
        return matches[0]
    return matches[0]


def _run_id(z: zipfile.ZipFile) -> str:
    metas = [n for n in z.namelist() if n.endswith("/meta.json")]
    if not metas:
        return ""
    return metas[0].split("/meta.json")[0]


def read_meta(z: zipfile.ZipFile) -> dict[str, Any]:
    metas = [n for n in z.namelist() if n.endswith("/meta.json")]
    if not metas:
        return {}
    return json.loads(z.read(metas[0]).decode("utf-8", errors="replace"))


def read_dataset_summary_row(z: zipfile.ZipFile, dataset_id: str) -> dict[str, str]:
    files = [n for n in z.namelist() if n.endswith("/workspace/results/dataset_summary.tsv")]
    if not files:
        raise FileNotFoundError("workspace/results/dataset_summary.tsv not found in audit zip")
    content = z.read(files[0]).decode("utf-8", errors="replace").splitlines()
    reader = csv.DictReader(content, delimiter="\t")
    rows = [r for r in reader if r.get("dataset_id") == dataset_id]
    if not rows:
        raise ValueError(f"dataset_id not found in dataset_summary: {dataset_id}")
    return rows[-1]


def read_tsv_rows(z: zipfile.ZipFile, relpath: str) -> list[dict[str, str]]:
    name = _find_one(z, f"/workspace/{relpath}")
    content = z.read(name).decode("utf-8", errors="replace").splitlines()
    reader = csv.DictReader(content, delimiter="\t")
    return list(reader)


def _float_or_none(x: str) -> float | None:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if s == "" or s.lower() == "nan":
            return None
        return float(s)
    except Exception:
        return None


def _nearly_equal(a: str, b: str, *, atol: float = 1e-6, rtol: float = 1e-6) -> bool:
    fa = _float_or_none(a)
    fb = _float_or_none(b)
    if fa is None or fb is None:
        return (str(a).strip() == str(b).strip())
    diff = abs(fa - fb)
    return diff <= atol + rtol * max(abs(fa), abs(fb))


def compare_method_benchmark(local_rows: list[dict[str, str]], gcp_rows: list[dict[str, str]], dataset_id: str) -> tuple[bool, list[str]]:
    # Compare the semantic (run-id independent) content of method_benchmark rows for a dataset.
    # Ignore: replicate_id, notes, method_version (may legitimately differ across envs).
    keys = ["task", "method_id", "metric_id"]
    local = [r for r in local_rows if r.get("dataset_id") == dataset_id]
    gcp = [r for r in gcp_rows if r.get("dataset_id") == dataset_id]

    idx_l: dict[tuple[str, str, str], dict[str, str]] = {tuple(r.get(k, "") for k in keys): r for r in local}
    idx_g: dict[tuple[str, str, str], dict[str, str]] = {tuple(r.get(k, "") for k in keys): r for r in gcp}

    all_keys = sorted(set(idx_l.keys()) | set(idx_g.keys()))
    diffs: list[str] = []
    ok = True
    for k in all_keys:
        rl = idx_l.get(k)
        rg = idx_g.get(k)
        if rl is None:
            ok = False
            diffs.append(f"missing in local: task={k[0]} method={k[1]} metric={k[2]}")
            continue
        if rg is None:
            ok = False
            diffs.append(f"missing in gcp: task={k[0]} method={k[1]} metric={k[2]}")
            continue
        vl = rl.get("metric_value", "")
        vg = rg.get("metric_value", "")
        if not _nearly_equal(vl, vg):
            ok = False
            diffs.append(f"value mismatch: task={k[0]} method={k[1]} metric={k[2]} local={vl} gcp={vg}")
    return ok, diffs


def main() -> int:
    p = argparse.ArgumentParser(
        description="Compare two audit bundles (zip) for key artifacts + semantic (row-level) concordance on core tables."
    )
    p.add_argument("--local", required=True, help="Local audit zip path.")
    p.add_argument("--gcp", required=True, help="GCP audit zip path.")
    p.add_argument("--dataset-id", required=True, help="Dataset id to compare.")
    p.add_argument("--out", default="", help="Optional output markdown path.")
    args = p.parse_args()

    local_zip = Path(args.local)
    gcp_zip = Path(args.gcp)
    dsid = args.dataset_id

    artifacts = [
        "results/dataset_summary.tsv",
        "results/audit/reproducibility_checks.tsv",
        "results/figures/F1_system_contract.tsv",
        "results/benchmarks/method_benchmark.tsv",
        "results/benchmarks/runtime_cost_failure.tsv",
        "results/benchmarks/biological_output_concordance.tsv",
        "results/benchmarks/robustness_matrix.tsv",
    ]

    with zipfile.ZipFile(local_zip) as zl, zipfile.ZipFile(gcp_zip) as zg:
        ml = read_meta(zl)
        mg = read_meta(zg)
        run_l = _run_id(zl)
        run_g = _run_id(zg)

        # dataset summary row (stable fields)
        rl = read_dataset_summary_row(zl, dsid)
        rg = read_dataset_summary_row(zg, dsid)

        # artifact hashes (raw file bytes in zip)
        art_rows: list[dict[str, str]] = []
        for a in artifacts:
            row = {"artifact": a, "local_sha256": "", "gcp_sha256": "", "match": "", "notes": ""}
            try:
                row["local_sha256"] = _sha256_bytes(zl.read(_find_one(zl, f"/workspace/{a}")))
            except Exception as e:
                row["local_sha256"] = ""
                row["notes"] = f"local missing: {type(e).__name__}"
            try:
                row["gcp_sha256"] = _sha256_bytes(zg.read(_find_one(zg, f"/workspace/{a}")))
            except Exception as e:
                row["gcp_sha256"] = ""
                row["notes"] = (row["notes"] + "; " if row["notes"] else "") + f"gcp missing: {type(e).__name__}"
            if row["local_sha256"] and row["gcp_sha256"]:
                row["match"] = "YES" if row["local_sha256"] == row["gcp_sha256"] else "NO"
            else:
                row["match"] = "NA"
            art_rows.append(row)

        # semantic concordance: method_benchmark rows (if present)
        mb_ok: bool | None
        mb_diffs: list[str]
        try:
            mb_local = read_tsv_rows(zl, "results/benchmarks/method_benchmark.tsv")
            mb_gcp = read_tsv_rows(zg, "results/benchmarks/method_benchmark.tsv")
            mb_ok, mb_diffs = compare_method_benchmark(mb_local, mb_gcp, dsid)
        except Exception as e:
            mb_ok = None
            mb_diffs = [f"method_benchmark compare unavailable: {type(e).__name__}: {e}"]

    keys = [
        "n_cells_or_spots",
        "n_genes",
        "qc_summary",
        "source_url",
        "entrypoint",
        "assay_platform",
        "organism",
        "tissue",
    ]
    diffs = {k: (rl.get(k, ""), rg.get(k, "")) for k in keys if rl.get(k, "") != rg.get(k, "")}

    md = []
    md.append("# Audit bundle comparison")
    md.append("")
    md.append(f"- Local: `{local_zip}`")
    md.append(f"- GCP: `{gcp_zip}`")
    md.append(f"- Dataset ID: `{dsid}`")
    md.append("")
    md.append("## Environment (meta.json)")
    md.append(f"- Local run_id: `{run_l}`")
    md.append(f"- GCP run_id: `{run_g}`")
    md.append(f"- Local git_commit: `{ml.get('git_commit','')}`")
    md.append(f"- GCP git_commit: `{mg.get('git_commit','')}`")
    md.append(f"- Local platform: {ml.get('platform','')}")
    md.append(f"- Local python: {ml.get('python_version','')}")
    md.append(f"- GCP platform: {mg.get('platform','')}")
    md.append(f"- GCP python: {mg.get('python_version','')}")
    md.append("")

    md.append("## Key artifact hashes (raw file bytes)")
    md.append("")
    md.append("| Artifact | Local sha256 | Cloud sha256 | Match? | Notes |")
    md.append("|---|---|---|---|---|")
    for r in art_rows:
        md.append(
            f"| `{r['artifact']}` | `{r['local_sha256']}` | `{r['gcp_sha256']}` | {r['match']} | {r['notes']} |"
        )
    md.append("")
    md.append("Note: some artifacts are expected to differ byte-for-byte across runs because they embed run_id/timestamps (e.g., reproducibility_checks, runtime rows).")
    md.append("")

    md.append("## dataset_summary row concordance")
    if not diffs:
        md.append("Result: **MATCH** for all tracked fields.")
    else:
        md.append("Result: **DIFF**")
        for k, (a, b) in diffs.items():
            md.append(f"- `{k}`")
            md.append(f"  - local: `{a}`")
            md.append(f"  - gcp: `{b}`")
    md.append("")

    md.append("## method_benchmark semantic concordance")
    if mb_ok is None:
        md.append("Result: **NA**")
        for d in mb_diffs:
            md.append(f"- {d}")
    elif mb_ok:
        md.append("Result: **MATCH** (row-level values match after ignoring run-id dependent fields).")
    else:
        md.append("Result: **DIFF**")
        for d in mb_diffs[:50]:
            md.append(f"- {d}")
        if len(mb_diffs) > 50:
            md.append(f"- ... ({len(mb_diffs) - 50} more)")
    md.append("")

    out_text = "\n".join(md) + "\n"
    if args.out:
        Path(args.out).write_text(out_text, encoding="utf-8")
    else:
        print(out_text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
