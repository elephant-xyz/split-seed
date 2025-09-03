#!/usr/bin/env bash
set -euo pipefail

# Deploys the CloudFormation stack for the Glue row-splitter job and uploads the script.

STACK_NAME=${STACK_NAME:-glue-row-splitter}
REGION=${AWS_REGION:-${REGION:-us-east-1}}
JOB_NAME=${JOB_NAME:-$STACK_NAME}
SCRIPT_KEY=${SCRIPT_KEY:-glue/scripts/split_csv_rows.py}

if ! command -v aws >/dev/null 2>&1; then
  echo "aws CLI not found. Please install and configure AWS CLI." >&2
  exit 1
fi

echo "Region: ${REGION}"
echo "Stack:  ${STACK_NAME}"
echo "Job:    ${JOB_NAME}"
echo "Script key: ${SCRIPT_KEY}"

echo "Deploying CloudFormation stack ${STACK_NAME}"
aws cloudformation deploy \
  --stack-name "${STACK_NAME}" \
  --region "${REGION}" \
  --capabilities CAPABILITY_NAMED_IAM \
  --template-file infrastructure/cloudformation/glue-row-splitter.yaml \
  --parameter-overrides \
    JobName="${JOB_NAME}" \
    ScriptS3Key="${SCRIPT_KEY}"

echo "Deployed. Fetching outputs..."
ARTIFACT_BUCKET=$(aws cloudformation describe-stacks \
  --stack-name "${STACK_NAME}" \
  --region "${REGION}" \
  --query 'Stacks[0].Outputs[?OutputKey==`ArtifactsBucketName`].OutputValue | [0]' \
  --output text)

if [[ -z "${ARTIFACT_BUCKET}" || "${ARTIFACT_BUCKET}" == "None" || "${ARTIFACT_BUCKET}" == "null" ]]; then
  echo "Failed to resolve ArtifactsBucketName from stack outputs." >&2
  aws cloudformation describe-stacks --stack-name "${STACK_NAME}" --region "${REGION}" --output table || true
  exit 1
fi

echo "Uploading Glue script to s3://${ARTIFACT_BUCKET}/${SCRIPT_KEY}"
aws s3 cp glue_scripts/split_csv_rows.py "s3://${ARTIFACT_BUCKET}/${SCRIPT_KEY}" --region "${REGION}"

echo "Done. Script location: s3://${ARTIFACT_BUCKET}/${SCRIPT_KEY}"
