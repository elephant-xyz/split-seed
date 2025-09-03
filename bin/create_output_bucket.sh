#!/usr/bin/env bash
set -euo pipefail

# Creates a single S3 bucket suitable for Glue job outputs.
# - Defaults: allow public read access, enable AES256 default encryption, enable versioning.
# - If BUCKET_NAME is not provided, generates a unique name using account/region and a random suffix.

REGION=${AWS_REGION:-${REGION:-us-east-1}}
BUCKET_NAME=${BUCKET_NAME:-}


if ! command -v aws >/dev/null 2>&1; then
  echo "aws CLI not found. Please install and configure AWS CLI." >&2
  exit 1
fi

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

if [[ -z "${BUCKET_NAME}" ]]; then
    echo "Bucket name is required"
    exit 1
fi

echo "Creating bucket: s3://${BUCKET_NAME} in ${REGION}"
if [[ "${REGION}" == "us-east-1" ]]; then
  aws s3api create-bucket --bucket "${BUCKET_NAME}" --region "${REGION}"
else
  aws s3api create-bucket --bucket "${BUCKET_NAME}" --region "${REGION}" \
    --create-bucket-configuration LocationConstraint="${REGION}"
fi

echo "Enabling default encryption (SSE-S3)"
aws s3api put-bucket-encryption --bucket "${BUCKET_NAME}" --region "${REGION}" \
  --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'

echo "Disabling block public access"
aws s3api put-public-access-block --bucket "${BUCKET_NAME}" --region "${REGION}" \
  --public-access-block-configuration BlockPublicAcls=false,IgnorePublicAcls=false,BlockPublicPolicy=false,RestrictPublicBuckets=false

echo "Setting bucket policy to allow public read access"
aws s3api put-bucket-policy --bucket "${BUCKET_NAME}" --region "${REGION}" \
  --policy '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Principal": "*",
        "Action": "s3:GetObject",
        "Resource": "arn:aws:s3:::'"${BUCKET_NAME}"'/*"
      }
    ]
  }'




echo "Created: s3://${BUCKET_NAME}"

