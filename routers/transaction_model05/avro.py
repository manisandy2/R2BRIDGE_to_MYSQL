from fastapi import APIRouter, HTTPException
import duckdb
from pyarrow.dataset import partitioning
from fastapi.encoders import jsonable_encoder
import pyarrow as pa
from fastavro import reader
from ...mysql_creds import *
from botocore.client import Config
from ...core.catalog_client import get_catalog_client
from fastapi import APIRouter,HTTPException,Query
import traceback
import sys



router = APIRouter(prefix="", tags=["avro"])
@router.get("/avro/list")
def iceberg_avro_files(
    namespace: str = Query(...),
    table_name: str = Query(...)
):
    try:
        catalog = get_catalog_client()
        table_identifier = f"{namespace}.{table_name}"
        tbl = catalog.load_table(table_identifier)

        result = []
        for snap in tbl.snapshots():
            print("snap",snap.snapshot_id)
            for manifest in snap.manifests(tbl.io):
                print("manifest:",manifest.manifest_path)
                # data_files = manifest.fetch_manifest_entry(tbl.io)
                manifest_entries = manifest.fetch_manifest_entry(tbl.io)

                for entry in manifest_entries:  # entry is ManifestEntry
                    data_file = entry.data_file  # this is DataFile object

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

    # except Exception as e:
    #     raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        exc_type, exc_obj, tb = sys.exc_info()
        line_number = tb.tb_lineno
        error_trace = traceback.format_exc()

        raise HTTPException(
            status_code=500,
            detail={
                "error": str(e),
                "type": str(exc_type.__name__),
                "line": line_number,
                "trace": error_trace
            }
        )


@router.get("/avro/{s3_path:path}")
def read_metadata_json(s3_path: str):
    import gzip, json, pandas as pd
    from fastapi import HTTPException
    import boto3

    # s3 = boto3.client("s3")
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("ENDPOINT"),
        aws_access_key_id=os.getenv("ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"),
        config=Config(signature_version="s3v4"),
        region_name="auto"
    )

    try:
        if not s3_path.startswith("s3://"):
            raise HTTPException(400, "Path must start with s3://")

        bucket_key = s3_path[5:].split("/", 1)
        bucket = bucket_key[0]
        key = bucket_key[1]

        obj = s3.get_object(Bucket=bucket, Key=key)
        raw = obj["Body"].read()
        data = json.loads(gzip.decompress(raw))

        df = pd.json_normalize(data)
        return df.to_dict(orient="records")

    except Exception as e:
        raise HTTPException(500, str(e))

@router.get("/avro/read")
def read_avro(file_path: str = Query(..., description="Path to avro file")):
    try:
        with pa.memory_map(file_path, "r") as f:
            table = reader(f)

        num_rows = table.num_rows
        sample_data = table.to_pylist()[:5]  # first 5 rows

        return {
            "rows": num_rows,
            "sample": sample_data
        }
    except Exception as e:
        return {"error": str(e)}