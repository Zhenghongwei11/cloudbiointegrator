#!/usr/bin/env bash
set -euo pipefail

# Idempotent cloud-VM runner: build container, run skeleton/smoke/validate, collect audit zips.
#
# Usage (on VM):
#   bash scripts/cloud/run_on_vm.sh
#
# Optional env vars:
#   IMAGE_TAG=cloudbiointegrator:smoke
#   RUN_OUT=/tmp/cloudbiointegrator-run
#   GCS_BUCKET=gs://your-bucket/path   (requires gsutil configured)
#   S3_URI=s3://bucket/prefix          (requires aws cli configured)
#   SCRNA_ARGS="--input-dir ... --dataset-id ... --method-pack baseline ..."
#   VISIUM_ARGS="--input-dir ... --dataset-id ... --method-pack baseline ..."
#   FETCH_DATASET_ID="dataset_id_from_data_manifest"   (optional; runs scripts/data/fetch_dataset.py --extract on the host before container runs)

IMAGE_TAG="${IMAGE_TAG:-cloudbiointegrator:smoke}"
RUN_OUT="${RUN_OUT:-/tmp/cloudbiointegrator-run}"
SMOKE_ARGS="${SMOKE_ARGS:-}"
SCRNA_ARGS="${SCRNA_ARGS:-}"
VISIUM_ARGS="${VISIUM_ARGS:-}"
FETCH_DATASET_ID="${FETCH_DATASET_ID:-}"
DOCKER_BIN="${DOCKER_BIN:-docker}"
DOCKERFILE="${DOCKERFILE:-Dockerfile}"
DOCKER_GPU="${DOCKER_GPU:-0}"
DOCKER_TARGET="${DOCKER_TARGET:-}"
DOCKER_BUILD_ARGS="${DOCKER_BUILD_ARGS:-}"

mkdir -p "${RUN_OUT}"

if [[ -n "${FETCH_DATASET_ID}" ]]; then
  if [[ "${FETCH_DATASET_ID}" == *";"* ]]; then
    IFS=';' read -r -a _IDS <<< "${FETCH_DATASET_ID}"
    for _ID in "${_IDS[@]}"; do
      _ID="$(echo "${_ID}" | xargs)"
      if [[ -n "${_ID}" ]]; then
        echo "[cloud] fetch dataset: ${_ID}"
        python3 scripts/data/fetch_dataset.py --dataset-id "${_ID}" --extract
      fi
    done
  else
    echo "[cloud] fetch dataset: ${FETCH_DATASET_ID}"
    python3 scripts/data/fetch_dataset.py --dataset-id "${FETCH_DATASET_ID}" --extract
  fi
fi

echo "[cloud] docker build: ${IMAGE_TAG} (dockerfile=${DOCKERFILE} target=${DOCKER_TARGET:-<default>})"
BUILD_ARGS=()
if [[ -n "${DOCKER_TARGET}" ]]; then
  BUILD_ARGS+=(--target "${DOCKER_TARGET}")
fi
if [[ -n "${DOCKER_BUILD_ARGS}" ]]; then
  # Intentionally allow simple whitespace-separated args (no quoting).
  # shellcheck disable=SC2206
  BUILD_ARGS+=(${DOCKER_BUILD_ARGS})
fi
${DOCKER_BIN} build -f "${DOCKERFILE}" -t "${IMAGE_TAG}" "${BUILD_ARGS[@]}" .

echo "[cloud] run skeleton/smoke/validate"
SMOKE_CMD="make smoke"
if [[ -n "${SMOKE_ARGS}" ]]; then
  SMOKE_CMD="make smoke ARGS=\"${SMOKE_ARGS}\""
fi
INNER="make skeleton && make validate && ${SMOKE_CMD} && make validate"
if [[ -n "${SCRNA_ARGS}" ]]; then
  INNER="${INNER} && make scrna ARGS=\"${SCRNA_ARGS}\" && make validate"
fi
if [[ -n "${VISIUM_ARGS}" ]]; then
  INNER="${INNER} && make visium ARGS=\"${VISIUM_ARGS}\" && make validate"
fi
# Run as the current VM user to avoid root-owned artifacts on the host filesystem.
GPU_ARGS=()
if [[ "${DOCKER_GPU}" == "1" ]]; then
  GPU_ARGS+=(--gpus all)
fi
${DOCKER_BIN} run --rm \
  -u "$(id -u):$(id -g)" \
  -v "$(pwd):/app" \
  -w /app \
  "${GPU_ARGS[@]}" \
  "${IMAGE_TAG}" \
  bash -lc "${INNER}"

echo "[cloud] collect audit zips"
mkdir -p "${RUN_OUT}/audit_runs"
cp -v docs/audit_runs/*.zip "${RUN_OUT}/audit_runs/" || true

if [[ -n "${GCS_BUCKET:-}" ]]; then
  echo "[cloud] uploading to GCS: ${GCS_BUCKET}"
  gsutil -m cp "${RUN_OUT}/audit_runs/"*.zip "${GCS_BUCKET%/}/audit_runs/" || true
fi

if [[ -n "${S3_URI:-}" ]]; then
  echo "[cloud] uploading to S3: ${S3_URI}"
  aws s3 cp "${RUN_OUT}/audit_runs/" "${S3_URI%/}/audit_runs/" --recursive || true
fi

echo "[cloud] done. audit zips at: ${RUN_OUT}/audit_runs"
