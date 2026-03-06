#!/usr/bin/env bash
set -euo pipefail

# Pull only the paper-facing audit zip subset listed in docs/SUBMISSION_AUDIT_SET.tsv.
# Usage:
#   bash scripts/cloud/pull_submission_audits.sh
#   bash scripts/cloud/pull_submission_audits.sh <set_tsv> <out_dir>

SET_TSV="${1:-docs/SUBMISSION_AUDIT_SET.tsv}"
OUT_DIR="${2:-docs/audit_runs_submission}"

if [[ ! -f "${SET_TSV}" ]]; then
  echo "ERROR: set file not found: ${SET_TSV}" >&2
  exit 1
fi

mkdir -p "${OUT_DIR}"

echo "[pull] set=${SET_TSV}"
echo "[pull] out=${OUT_DIR}"

awk -F'\t' 'NR>1 && $6=="yes" {print $4}' "${SET_TSV}" | while read -r uri; do
  [[ -n "${uri}" ]] || continue
  file="${uri##*/}"
  dest="${OUT_DIR}/${file}"
  if [[ -f "${dest}" ]]; then
    echo "[skip] ${file} (exists)"
    continue
  fi
  echo "[copy] ${uri}"
  gsutil cp "${uri}" "${dest}"
done

echo "[done] pulled files:"
ls -lh "${OUT_DIR}"/*.zip
