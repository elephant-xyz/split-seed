AWS Glue CSV Row Splitter
=========================

This project deploys an AWS Glue Python shell job that:

- Reads a CSV from an input `s3://.../file.csv`.
- Creates a CSV per row, named by the `source_identifier` column (configurable), under an output `s3://.../prefix/`.
- Optionally includes the header row in each output file.
- Writes a `_MANIFEST.json` to the output prefix summarizing created files.

What’s Included
---------------

- `glue_scripts/split_csv_rows.py`: Job script (no external dependencies).
- `infrastructure/cloudformation/glue-row-splitter.yaml`: CloudFormation to create the Glue Job and IAM Role.
- `bin/deploy.sh`: Uploads the script to S3 and deploys the stack.
- `bin/start_job.sh`: Starts the job with input/output S3 URIs.
- `bin/create_output_bucket.sh`: Creates one output bucket with good defaults.
- `bin/create_output_buckets.sh`: Creates many output buckets quickly.

Prerequisites
-------------

- AWS account with permissions to create IAM roles, Glue jobs, and access S3.
- AWS CLI configured (`aws configure`).

Deploy
------

1) Choose a region.

2) Run deploy (no artifact/input/output buckets required; the stack creates an artifacts bucket for the script, and the role has broad S3 permissions by design):

```
AWS_REGION=<region> \
STACK_NAME=glue-row-splitter \
JOB_NAME=glue-row-splitter \
./bin/deploy.sh
```

This deploys the CloudFormation stack, creates an S3 bucket for artifacts, and uploads `glue_scripts/split_csv_rows.py` to `s3://<artifacts-bucket>/glue/scripts/split_csv_rows.py` automatically.

Run a Job
---------

```
JOB_NAME=glue-row-splitter \
INPUT_S3_URI=s3://my-input-bucket/path/to/input.csv \
OUTPUT_S3_URI=s3://my-output-bucket/prefix/ \
AWS_REGION=<region> \
ID_COLUMN=source_identifier \
INCLUDE_HEADER=true \
./bin/start_job.sh
```

Create Output Buckets
---------------------

- Single bucket (auto-generated name if not provided):

```
AWS_REGION=<region> BUCKET_NAME=<optional-explicit-name> ./bin/create_output_bucket.sh
```

- Many buckets (fast provisioning with sensible defaults):

```
AWS_REGION=<region> COUNT=10 PREFIX=my-output ./bin/create_output_buckets.sh
```

These scripts create S3 buckets with public access blocked, default SSE-S3 encryption, and versioning enabled.

Script Arguments
----------------

- `--input_s3_uri`: Input CSV S3 URI (required).
- `--output_s3_uri`: Output S3 URI prefix (required). Trailing `/` optional.
- `--id_column`: Column used for naming files (default: `source_identifier`). Missing values fall back to `row-<index>`.
- `--include_header`: `true|false` include header row in each output (default: `true`).
- `--delimiter`: CSV delimiter (default: `,`).
- `--quotechar`: CSV quote char (default: `"`).

Notes
-----

- The job streams the input from S3, and writes one file per row. Large inputs will create many small objects.
- If multiple rows share the same `id_column` value, suffixes like `-1`, `-2` are appended.
- The Glue Job uses Python shell with `MaxCapacity=0.0625` DPU by default for cost efficiency.
- By default, the IAM role has broad S3 access for convenience. If you prefer least-privilege, switch the template back to per-bucket permissions.
