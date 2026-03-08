#!/usr/bin/env bash
set -euo pipefail

# One-click manifest-driven Q1 benchmark runner.
#
# Default:
#   bash scripts/cloud/run_q1_benchmark_suite.sh
#
# Dry run:
#   bash scripts/cloud/run_q1_benchmark_suite.sh --dry-run --max-runs 2

python3 scripts/cloud/run_q1_benchmark_suite.py "$@"

