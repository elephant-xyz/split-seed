#!/usr/bin/env bash
set -euo pipefail

# Starts a Glue job per CSV file found under a local directory, mapping to
# input URIs s3://<INPUT_PREFIX>/<basename.csv> and output buckets
# s3://elephant-input-<sanitized-basename>-county/.
#
# Env vars:
#   JOB_NAME        Glue job name (default: glue-row-splitter)
#   LOCAL_DIR       Local dir with CSVs (default: ~/Downloads/counties)
#   INPUT_PREFIX    s3 prefix for inputs (default: s3://elephant-county-inputs)
#   AWS_REGION      AWS region (default: us-east-1)
#   AWS_PROFILE     AWS profile to use (optional)

JOB_NAME=${JOB_NAME:-glue-row-splitter}
LOCAL_DIR=${LOCAL_DIR:-$HOME/Downloads/counties}
INPUT_PREFIX=${INPUT_PREFIX:-s3://elephant-county-inputs}
REGION=${AWS_REGION:-${REGION:-us-east-1}}

if ! command -v aws >/dev/null 2>&1; then
  echo "aws CLI not found. Please install and configure AWS CLI." >&2
  exit 1
fi

shopt -s nullglob
CSV_FILES=("${LOCAL_DIR}"/*.csv)
if (( ${#CSV_FILES[@]} == 0 )); then
  echo "No CSV files found at ${LOCAL_DIR}/*.csv" >&2
  exit 1
fi

echo "Region: ${REGION} | Job: ${JOB_NAME} | Local: ${LOCAL_DIR} | Input prefix: ${INPUT_PREFIX}"
[[ -n "${AWS_PROFILE:-}" ]] && echo "Using AWS profile: ${AWS_PROFILE}"

RUNS_FILE="job_runs_$(date +%Y%m%d_%H%M%S).txt"
declare -a STARTS=()

for p in "${CSV_FILES[@]}"; do
  base=$(basename "$p")
  name_no_ext=${base%.csv}
  sanitized=$(echo "$name_no_ext" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9-]+/-/g; s/^-+|-+$//g; s/-+/-/g')
  [[ -z "$sanitized" ]] && sanitized="unnamed"
  bucket="elephant-input-${sanitized}-county"
  input_uri="${INPUT_PREFIX%/}/${base}"
  output_uri="s3://${bucket}/"

  echo "[START] ${base}\n  Input : ${input_uri}\n  Output: ${output_uri}\n  Job   : ${JOB_NAME} (${REGION})"

  if out=$(AWS_REGION="$REGION" JOB_NAME="$JOB_NAME" INPUT_S3_URI="$input_uri" OUTPUT_S3_URI="$output_uri" bash ./bin/start_job.sh 2>&1); then
    echo "$out"
    run_id=$(echo "$out" | sed -nE 's/.*"JobRunId"\s*:\s*"([^"]+)".*/\1/p' | head -n1)
    echo "${base}: ${run_id}" | tee -a "$RUNS_FILE"
    STARTS+=("${base}: ${run_id}")
  else
    echo "[ERR] Failed to start ${base}" >&2
    echo "$out" >&2
    echo "${base}: FAILED" | tee -a "$RUNS_FILE"
  fi
  echo
done

echo "Summary (written to ${RUNS_FILE}):"
for s in "${STARTS[@]}"; do echo " - ${s}"; done

