#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


EXPECTED_HEADERS: dict[str, list[str]] = {
    "results/dataset_summary.tsv": [
        "dataset_id",
        "modality",
        "organism",
        "tissue",
        "assay_platform",
        "input_artifact",
        "entrypoint",
        "role",
        "n_samples",
        "n_donors",
        "n_cells_or_spots",
        "n_genes",
        "reference_genome",
        "primary_citation",
        "source_url",
        "license",
        "qc_summary",
        "notes",
    ],
    "results/audit/reproducibility_checks.tsv": [
        "run_id",
        "timestamp_utc",
        "dataset_id",
        "stage",
        "env_hash",
        "container_image",
        "git_commit",
        "seed",
        "action_schema_version",
        "params_hash",
        "output_table_path",
        "output_sha256",
        "pass",
        "fail_reason",
        "wall_time_s",
        "peak_ram_gb",
        "notes",
    ],
    "results/benchmarks/method_benchmark.tsv": [
        "dataset_id",
        "modality",
        "task",
        "method_id",
        "method_version",
        "baseline_flag",
        "metric_id",
        "metric_value",
        "metric_ci_low",
        "metric_ci_high",
        "metric_unit",
        "eval_split",
        "replicate_id",
        "n_units",
        "notes",
    ],
    "results/benchmarks/biological_output_concordance.tsv": [
        "dataset_id",
        "modality",
        "method_id",
        "output_type",
        "reference_type",
        "concordance_metric",
        "value",
        "ci_low",
        "ci_high",
        "n_units",
        "notes",
    ],
    "results/benchmarks/runtime_cost_failure.tsv": [
        "dataset_id",
        "modality",
        "method_id",
        "run_id",
        "status",
        "failure_type",
        "wall_time_s",
        "peak_ram_gb",
        "peak_disk_gb",
        "cpu_hours",
        "gpu_hours",
        "estimated_cost_usd",
        "cost_model",
        "notes",
    ],
    "results/benchmarks/robustness_matrix.tsv": [
        "dataset_id",
        "modality",
        "method_id",
        "perturbation_id",
        "severity",
        "metric_id",
        "metric_value",
        "delta_vs_nominal",
        "pass",
        "failure_reason",
        "notes",
    ],
    "results/figures/F1_system_contract.tsv": [
        "action_id",
        "action_name",
        "input_artifacts",
        "output_tables",
        "determinism_controls",
        "allowed_methods",
        "notes",
    ],
}


EXPECTED_FIGURES: list[str] = [
    "F1_system_contract",
    "F2_reproducibility",
    "F3_scrna_benchmark",
    "F4_spatial_benchmark",
    "F5_ops_benchmark",
    "F6_robustness_matrix",
]


def read_header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="\t")
        return next(reader)


def fail(msg: str) -> int:
    print(f"ERROR: {msg}", file=sys.stderr)
    return 1

def warn(msg: str) -> None:
    print(f"WARN: {msg}", file=sys.stderr)


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate pipeline contract (tables + schema + figure outputs).")
    parser.add_argument("--skip-figures", action="store_true", help="Do not require plots/publication outputs.")
    args = parser.parse_args()

    rc = 0

    schema_path = ROOT / "schemas" / "action_schema_v1.json"
    if not schema_path.exists():
        return fail("missing schemas/action_schema_v1.json")
    try:
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
    except Exception as e:
        return fail(f"failed to parse action schema: {e}")
    if schema.get("schema_version") != "v1":
        rc |= fail("action_schema_v1.json schema_version != v1")
    if not schema.get("allowed_actions"):
        rc |= fail("action_schema_v1.json allowed_actions is empty")

    for rel, expected in EXPECTED_HEADERS.items():
        p = ROOT / rel
        if not p.exists():
            rc |= fail(f"missing required table: {rel}")
            continue
        got = read_header(p)
        if got != expected:
            rc |= fail(f"header mismatch for {rel}\n  expected: {expected}\n  got:      {got}")

    prov = ROOT / "docs" / "FIGURE_PROVENANCE.tsv"
    if not prov.exists():
        rc |= fail("missing docs/FIGURE_PROVENANCE.tsv")
    else:
        with prov.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                script = row.get("script_path", "")
                anchors = row.get("anchor_tables", "")
                if script:
                    sp = ROOT / script
                    if not sp.exists():
                        rc |= fail(f"missing figure script: {script}")
                if anchors:
                    for a in anchors.split(";"):
                        a = a.strip()
                        if not a:
                            continue
                        ap = ROOT / a
                        if not ap.exists():
                            # Some figure panels depend on anchor tables that are produced only after
                            # running specific method packs on real datasets (e.g., spatial weights
                            # under results/figures/). Those should be referenced in provenance, but
                            # are not guaranteed to exist in a fresh checkout.
                            #
                            # Contract tables (EXPECTED_HEADERS) MUST always exist.
                            if a in EXPECTED_HEADERS:
                                rc |= fail(f"missing anchor table: {a}")
                            else:
                                warn(f"optional anchor table missing (ok in fresh checkout): {a}")

    if not args.skip_figures:
        for fig in EXPECTED_FIGURES:
            pdf = ROOT / "plots" / "publication" / "pdf" / f"{fig}.pdf"
            png = ROOT / "plots" / "publication" / "png" / f"{fig}.png"
            if not pdf.exists():
                rc |= fail(f"missing figure PDF: {pdf.relative_to(ROOT)}")
            if not png.exists():
                rc |= fail(f"missing figure PNG: {png.relative_to(ROOT)}")

    if rc == 0:
        print("OK: contract validation passed")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
