from fastapi import APIRouter, HTTPException, Query
import os
import json
import gzip
import logging
import traceback
import sys
import boto3
from botocore.client import Config
import pandas as pd
import pyarrow.fs as fs
from fastavro import reader as avro_reader

# Internal imports
from ...core.catalog_client import get_catalog_client

# Configure logging
logger = logging.getLogger(__name__)

router = APIRouter(prefix="", tags=["avro"])


def get_s3_client():
    """Returns a configured Boto3 S3 client for Cloudflare R2."""
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("ENDPOINT"),
        aws_access_key_id=os.getenv("ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"),
        config=Config(signature_version="s3v4"),
        region_name="auto"
    )

def get_s3_fs():
    """Returns a configured PyArrow S3FileSystem for Cloudflare R2."""
    return fs.S3FileSystem(
        access_key=os.getenv("ACCESS_KEY_ID"),
        secret_key=os.getenv("SECRET_ACCESS_KEY"),
        endpoint_override=os.getenv("ENDPOINT"),
        scheme="https"
    )


@router.get("/avro/list")
def iceberg_avro_files(
    namespace: str = Query("pos_transactions"),
    table_name: str = Query("transaction")
):
    try:
        catalog = get_catalog_client()
        table_identifier = f"{namespace}.{table_name}"
        tbl = catalog.load_table(table_identifier)

        result = []
        # logger.info(f"Scanning snapshots for table: {table_identifier}")
        
        # Note: Iterate through snapshots. If performance is an issue, consider limiting to the latest snapshot.
        for snap in tbl.snapshots():
            for manifest in snap.manifests(tbl.io):
                manifest_entries = manifest.fetch_manifest_entry(tbl.io)

                for entry in manifest_entries:
                    data_file = entry.data_file

                    if data_file.file_path.lower().endswith(".avro"):
                        result.append({
                            "snapshot_id": snap.snapshot_id,
                            "manifest": manifest.manifest_path,
                            "avro_file": data_file.file_path,
                            "record_count": data_file.record_count,
                        })
        
        return {
            "success": True,
            "table": table_identifier,
            "total_avro_files": len(result),
            "files": result
        }

    except Exception as e:
        logger.error(f"Error listing Avro files: {str(e)}")
        exc_type, _, tb = sys.exc_info()
        raise HTTPException(
            status_code=500,
            detail={
                "error": str(e),
                "type": exc_type.__name__ if exc_type else "Unknown",
                "line": tb.tb_lineno if tb else 0,
                "trace": traceback.format_exc()
            }
        )



def sanitize_record(record):
    """
    Recursively sanitizes a record to ensure all bytes are convertible to strings
    for JSON serialization.
    """
    if isinstance(record, dict):
        return {k: sanitize_record(v) for k, v in record.items()}
    elif isinstance(record, list):
        return [sanitize_record(v) for v in record]
    elif isinstance(record, bytes):
        try:
            return record.decode('utf-8')
        except UnicodeDecodeError:
            return f"<bytes len={len(record)}>"
    else:
        return record


@router.get("/avro/read")
def read_avro(
    s3_path: str = Query(..., description="Full s3:// path to Avro file")
):
    try:
        if not s3_path.startswith("s3://"):
             raise HTTPException(400, "Path must start with s3://")

        s3_fs = get_s3_fs()
        
        path_without_scheme = s3_path.replace("s3://", "", 1)

        with s3_fs.open_input_file(path_without_scheme) as f:
            avro_iter = avro_reader(f)

            sample = []
            count = 0
            
            # Read first 5 rows for sample
            for row in avro_iter:
                if count < 5:
                    sample.append(sanitize_record(row))
                count += 1

        return {
            "rows": count,
            "sample": sample
        }

    except Exception as e:
        logger.error(f"Error reading Avro file: {e}")
        return {"error": str(e)}


@router.get("/avro/{s3_path:path}")
def read_s3_path_content(s3_path: str):
    """
    Generic endpoint to read file content from S3. 
    Detects file type by extension (.avro vs .json/other).
    """
    try:
        if not s3_path.startswith("s3://"):
            raise HTTPException(400, "Path must start with s3://")

        # Handle Avro files
        if s3_path.lower().endswith(".avro"):
            s3_fs = get_s3_fs()
            path_without_scheme = s3_path.replace("s3://", "", 1)
            
            try:
                with s3_fs.open_input_file(path_without_scheme) as f:
                    avro_iter = avro_reader(f)
                    sample = []
                    count = 0
                    for row in avro_iter:
                        if count < 5:
                            sample.append(sanitize_record(row))
                        count += 1
                return {
                    "type": "avro",
                    "rows": count,
                    "sample": sample
                }
            except Exception as e:
                logger.error(f"Error reading Avro file at {s3_path}: {e}")
                raise HTTPException(500, f"Error reading Avro file: {str(e)}")

        # Handle JSON/Metadata files
        # Parse bucket and key
        parts = s3_path[5:].split("/", 1)
        if len(parts) != 2:
             raise HTTPException(400, "Invalid S3 path format")
             
        bucket = parts[0]
        key = parts[1]

        s3 = get_s3_client()
        
        obj = s3.get_object(Bucket=bucket, Key=key)
        raw = obj["Body"].read()
        
        # Handle GZIP compression if present
        try:
             decoded = gzip.decompress(raw)
        except OSError:
             # Not gzipped
             decoded = raw
             
        try:
            data = json.loads(decoded)
        except (json.JSONDecodeError, UnicodeDecodeError):
             raise HTTPException(
                 400, 
                 "File content is not valid JSON. Ensure you are accessing a JSON file or an Avro file with .avro extension."
             )

        df = pd.json_normalize(data)
        return df.to_dict(orient="records")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error reading file content: {e}")
        raise HTTPException(500, str(e))