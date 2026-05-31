#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="/scratch/nt612/Github/Latent Insider Information Shocks"
PYTHON_BIN="${PYTHON_BIN:-/home/nt612/.conda/envs/ml_core/bin/python}"

cd "$PROJECT_ROOT"

mkdir -p logs manifests artifacts/{raw,processed,intermediate,tables,figures_static,figures_html,models}

export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export NUMEXPR_MAX_THREADS=20

stamp="$(date +%Y%m%d_%H%M%S)"
echo "Starting LIIS pipeline at $(date)"
echo "Project root: $PROJECT_ROOT"
echo "Python: $PYTHON_BIN"
echo "SLURM_JOB_ID=${SLURM_JOB_ID:-none}"
echo "Host: $(hostname)"
nvidia-smi -L || true

"$PYTHON_BIN" scripts/liis_pipeline.py --root "$PROJECT_ROOT" --phase all --include-taq \
  > "logs/full_pipeline_${stamp}.out" \
  2> "logs/full_pipeline_${stamp}.err"

echo "Finished LIIS pipeline at $(date)"
