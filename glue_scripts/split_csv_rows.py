import argparse
import csv
import io
import json
import logging
import os
import re
import sys
import time
import traceback
from typing import Tuple
from types import SimpleNamespace

import boto3
from pyspark.context import SparkContext  # type: ignore
from awsglue.context import GlueContext  # type: ignore
from awsglue.job import Job  # type: ignore


logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)


def parse_s3_uri(uri: str) -> Tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {uri}")
    _, _sep, rest = uri.partition("s3://")
    parts = rest.split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    return bucket, key


def normalize_prefix(prefix: str) -> str:
    # Ensure trailing slash for prefix semantics
    if prefix and not prefix.endswith("/"):
        return prefix + "/"
    return prefix


def safe_filename(name: str) -> str:
    # Replace disallowed chars with underscore; keep alnum, dash, underscore, dot
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", name.strip())
    cleaned = cleaned.strip("._-")
    return cleaned or "unnamed"


def str2bool(value: str) -> bool:
    return str(value).lower() in {"1", "true", "t", "yes", "y"}


def main():
    logger.info("Starting split_csv_rows.py (PySpark)")
    # Prefer AWS Glue's getResolvedOptions per official docs to parse
    # job parameters that are passed in as --key value pairs.
    args_ns = None
    try:
        from awsglue.utils import getResolvedOptions

        required = ["JOB_NAME", "input_s3_uri", "output_s3_uri"]
        params = getResolvedOptions(sys.argv, required)

        # Try to resolve optional parameters if present; ignore if absent.
        optional = [
            "JOB_NAME",
            "id_column",
            "delimiter",
            "quotechar",
            "include_header",
            "encoding",
            "write_run_log",
            "log_level",
        ]
        for name in optional:
            try:
                params.update(getResolvedOptions(sys.argv, [name]))
            except Exception:
                pass

        # Defaults for any missing optionals
        args_ns = SimpleNamespace(
            job_name=params.get("JOB_NAME", "unknown"),
            input_s3_uri=params["input_s3_uri"],
            output_s3_uri=params["output_s3_uri"],
            id_column=params.get("id_column", "source_identifier"),
            delimiter=params.get("delimiter", ","),
            quotechar=params.get("quotechar", '"'),
            include_header=params.get("include_header", "true"),
            encoding=params.get("encoding", "utf-8"),
            log_level=params.get("log_level", os.environ.get("LOG_LEVEL", "INFO")),
            write_run_log=params.get("write_run_log", "true"),
        )
        logger.debug("Parsed Glue parameters via getResolvedOptions: %s", params)
    except Exception as e:
        # Fallback for local runs or environments without awsglue module
        logger.debug("getResolvedOptions unavailable or failed (%s). Falling back to argparse.", e)
        parser = argparse.ArgumentParser(description="Split CSV into per-row CSV files in S3.")
        parser.add_argument("--input_s3_uri", required=True, help="s3://bucket/key.csv of the input CSV")
        parser.add_argument("--output_s3_uri", required=True, help="s3://bucket/prefix/ for output files")
        parser.add_argument("--id_column", default="source_identifier", help="Column used to name each file")
        parser.add_argument("--delimiter", default=",", help="CSV delimiter (default: ,)")
        parser.add_argument("--quotechar", default='"', help='CSV quotechar (default: ")')
        parser.add_argument("--include_header", default="true", help="Include header row in each output file")
        parser.add_argument("--encoding", default="utf-8", help="Input/output text encoding")
        parser.add_argument(
            "--log_level",
            default=os.environ.get("LOG_LEVEL", "INFO"),
            help="Logging level (DEBUG, INFO, WARNING, ERROR)",
        )
        parser.add_argument(
            "--write_run_log", default="true", help="Write RUN_SUMMARY.json and RUN_FAILED.txt to output prefix"
        )

        # Allow unknown Glue flags without failing
        parsed, unknown = parser.parse_known_args()
        if unknown:
            logger.debug("Ignoring unknown Glue/extra args: %s", unknown)
        args_ns = SimpleNamespace(
            job_name=os.environ.get("JOB_NAME", "local"),
            input_s3_uri=parsed.input_s3_uri,
            output_s3_uri=parsed.output_s3_uri,
            id_column=parsed.id_column,
            delimiter=parsed.delimiter,
            quotechar=parsed.quotechar,
            include_header=parsed.include_header,
            encoding=parsed.encoding,
            log_level=parsed.log_level,
            write_run_log=parsed.write_run_log,
        )

    # Normalize types and apply defaults
    include_header = str2bool(args_ns.include_header)
    write_run_log = str2bool(args_ns.write_run_log)
    # Update logger level if provided
    try:
        logger.setLevel(getattr(logging, str(args_ns.log_level).upper()))
    except Exception:
        logger.setLevel(logging.INFO)

    # Configure logging

    input_bucket, input_key = parse_s3_uri(args_ns.input_s3_uri)
    output_bucket, output_prefix = parse_s3_uri(args_ns.output_s3_uri)
    output_prefix = normalize_prefix(output_prefix)

    # Initialize Glue/Spark job context for metrics and proper lifecycle
    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    try:
        job.init(args_ns.job_name or "job", dict())
    except Exception:
        # If job_name wasn't provided (local run), ignore
        pass

    s3 = boto3.client("s3")
    start_ts = time.time()

    logger.info(
        "Job parameters: input=%s output=%s id_column=%s delimiter='%s' quotechar='%s' include_header=%s",
        args_ns.input_s3_uri,
        args_ns.output_s3_uri,
        args_ns.id_column,
        args_ns.delimiter,
        args_ns.quotechar,
        include_header,
    )

    manifest = []
    name_counts = {}
    total = 0
    fieldnames = None
    manifest_key = None

    try:
        logger.info("Reading input CSV from s3://%s/%s", input_bucket, input_key)
        resp = s3.get_object(Bucket=input_bucket, Key=input_key)
        size = resp.get("ContentLength")
        logger.info("Input object size: %s bytes", size)
        body = resp["Body"]

        # Use TextIOWrapper to stream decode bytes to text
        text_stream = io.TextIOWrapper(body, encoding=args_ns.encoding)

        reader = csv.DictReader(text_stream, delimiter=args_ns.delimiter, quotechar=args_ns.quotechar)
        fieldnames = reader.fieldnames
        logger.info("Detected %d columns: %s", 0 if not fieldnames else len(fieldnames or []), fieldnames)
        if not fieldnames:
            raise RuntimeError("No CSV header/fieldnames detected. Ensure the input has a header row.")

        for idx, row in enumerate(reader, start=1):
            total += 1
            raw_name = row.get(args_ns.id_column) or f"row-{idx}"
            base_name = safe_filename(str(raw_name)) or f"row-{idx}"
            # Handle duplicates within this run
            count = name_counts.get(base_name, 0)
            name_counts[base_name] = count + 1
            final_name = base_name if count == 0 else f"{base_name}-{count}"
            out_key = f"{output_prefix}{final_name}.csv"

            buf = io.StringIO()
            writer = csv.DictWriter(
                buf,
                fieldnames=fieldnames,
                delimiter=args_ns.delimiter,
                quotechar=args_ns.quotechar,
                lineterminator="\n",
            )
            if include_header:
                writer.writeheader()
            writer.writerow(row)

            data = buf.getvalue().encode(args_ns.encoding)

            s3.put_object(Bucket=output_bucket, Key=out_key, Body=data, ContentType="text/csv")
            manifest.append(
                {
                    "s3_uri": f"s3://{output_bucket}/{out_key}",
                    "row_index": idx,
                    "source_identifier": raw_name,
                }
            )

            if idx % 1000 == 0:
                logger.info("Processed %d rows...", idx)

        # Write manifest
        manifest_key = f"{output_prefix}_MANIFEST.json"
        s3.put_object(
            Bucket=output_bucket,
            Key=manifest_key,
            Body=json.dumps(
                {
                    "input": args_ns.input_s3_uri,
                    "output_prefix": f"s3://{output_bucket}/{output_prefix}",
                    "files": manifest,
                    "total_rows": total,
                    "id_column": args_ns.id_column,
                },
                indent=2,
            ).encode("utf-8"),
            ContentType="application/json",
        )

        duration = time.time() - start_ts
        logger.info(
            "Done. Wrote %d row files under s3://%s/%s (manifest: s3://%s/%s) in %.2fs",
            total,
            output_bucket,
            output_prefix,
            output_bucket,
            manifest_key,
            duration,
        )

        if write_run_log:
            summary = {
                "status": "SUCCEEDED",
                "started_at_epoch": start_ts,
                "duration_seconds": duration,
                "input": args_ns.input_s3_uri,
                "output_prefix": f"s3://{output_bucket}/{output_prefix}",
                "total_rows": total,
                "id_column": args_ns.id_column,
                "fieldnames": fieldnames,
                "manifest": f"s3://{output_bucket}/{manifest_key}",
            }
            s3.put_object(
                Bucket=output_bucket,
                Key=f"{output_prefix}RUN_SUMMARY.json",
                Body=json.dumps(summary, indent=2).encode("utf-8"),
                ContentType="application/json",
            )

    except Exception as e:
        tb = traceback.format_exc()
        logger.error("Job failed: %s", e)
        logger.error(tb)
        if write_run_log:
            try:
                fail_msg = {
                    "status": "FAILED",
                    "error": str(e),
                    "traceback": tb,
                    "input": args_ns.input_s3_uri,
                    "output_prefix": f"s3://{output_bucket}/{output_prefix}",
                    "processed_rows": total,
                    "id_column": args_ns.id_column,
                    "fieldnames": fieldnames,
                }
                s3.put_object(
                    Bucket=output_bucket,
                    Key=f"{output_prefix}RUN_FAILED.txt",
                    Body=json.dumps(fail_msg, indent=2).encode("utf-8"),
                    ContentType="text/plain",
                )
            except Exception:
                # Best-effort; if even this fails, we can't do more.
                pass
        # Re-raise to ensure the Glue job is marked as failed
        raise
    finally:
        try:
            job.commit()
        except Exception:
            pass


if __name__ == "__main__":
    main()
