#!/usr/bin/env bash
set -euo pipefail

# Starts the Glue row-splitter job with input/output S3 URIs.

JOB_NAME=${JOB_NAME:-}
INPUT_S3_URI=${INPUT_S3_URI:-}
OUTPUT_S3_URI=${OUTPUT_S3_URI:-}
REGION=${AWS_REGION:-${REGION:-us-east-1}}
ID_COLUMN=${ID_COLUMN:-source_identifier}
INCLUDE_HEADER=${INCLUDE_HEADER:-true}
DELIMITER=${DELIMITER:-,}
QUOTECHAR=${QUOTECHAR:-\"}
TIMEOUT=${TIMEOUT:-10080}

if ! command -v aws >/dev/null 2>&1; then
  echo "aws CLI not found. Please install and configure AWS CLI." >&2
  exit 1
fi

usage() {
  cat <<EOF
Usage: JOB_NAME=<name> INPUT_S3_URI=s3://bucket/input.csv OUTPUT_S3_URI=s3://bucket/prefix/ [options] ./bin/start_job.sh

Env options:
  ID_COLUMN       Column used for file names (default: source_identifier)
  INCLUDE_HEADER  Include header in each output file (default: true)
  DELIMITER       CSV delimiter (default: ,)
  QUOTECHAR       CSV quote character (default: ")
  AWS_REGION      AWS region (default: us-east-1)
EOF
}

if [[ -z "${JOB_NAME}" || -z "${INPUT_S3_URI}" || -z "${OUTPUT_S3_URI}" ]]; then
  usage >&2
  exit 1
fi

echo "Starting Glue job ${JOB_NAME} in ${REGION}"

# Build arguments JSON to safely handle spaces
ARGS_JSON='{'
ARGS_JSON+="\"--input_s3_uri\":\"${INPUT_S3_URI}\"," 
ARGS_JSON+="\"--output_s3_uri\":\"${OUTPUT_S3_URI}\"," 
ARGS_JSON+="\"--id_column\":\"${ID_COLUMN}\"," 
ARGS_JSON+="\"--include_header\":\"${INCLUDE_HEADER}\"," 
ARGS_JSON+="\"--delimiter\":\"${DELIMITER}\""
# Only include quotechar if explicitly provided (avoid JSON escaping issues)
if [[ -n "${QUOTECHAR:-}" && "${QUOTECHAR}" != '"' ]]; then
  ARGS_JSON+=",\"--quotechar\":\"${QUOTECHAR}\""
fi
ARGS_JSON+='}'

set -x
aws glue start-job-run \
  --job-name "${JOB_NAME}" \
  --region "${REGION}" \
  --arguments "${ARGS_JSON}" \
  --timeout "${TIMEOUT}"
set +x
