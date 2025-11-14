from ...mysql_creds import *
from ...core.catalog_client import get_catalog_client
from fastapi import APIRouter,HTTPException,Query
import time

router = APIRouter(prefix="", tags=["parquet"])
@router.get("/parquet/list")
def list_parquet(
        namespace: str = Query("pos_transactions"),
        table_name: str = Query("iceberg_with_partitioning")
):
    """
    List parquet files for given Iceberg table (from manifest)
    """
    start = time.perf_counter()
    catalog = get_catalog_client()

    try:
        tbl = catalog.load_table(f"{namespace}.{table_name}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"load table failed: {str(e)}")

    files = []
    snap = tbl.current_snapshot()
    print("snap",snap)
    if not snap:
        return {"files": []}

    for manifest in snap.manifests(tbl.io):
        print("manifest:",manifest)
        m = manifest.fetch_manifest_entry(tbl.io)
        entries = manifest.fetch_manifest_entry(tbl.io)

        for e in entries:  # e is ManifestEntry
            df = e.data_file  # df is DataFile
            files.append({
                "path": df.file_path,
                "rows": df.record_count,
                "size_bytes": df.file_size_in_bytes,
            })

    return {
        "count": len(files),
        "files": files,
        "time_seconds": round(time.perf_counter() - start, 3) }



@router.get("/parquet/read")
def read_parquet(path: str = Query(..., description="Full s3:// R2 parquet path"), limit: int = Query(10)):

    import pyarrow.parquet as pq
    import pyarrow.fs as fs
    import os

    try:
        s3fs = fs.S3FileSystem(
            access_key=os.getenv("ACCESS_KEY_ID"),
            secret_key=os.getenv("SECRET_ACCESS_KEY"),
            endpoint_override=os.getenv("ENDPOINT"),
        )

        # ✅ Normalize path for PyArrow: remove s3:// prefix
        if path.startswith("s3://"):
            path = path.replace("s3://", "", 1)

        # read
        table = pq.read_table(path, filesystem=s3fs)
        df = table.to_pandas().head(limit)

        return {
            "status": "success",
            "path": path,
            "row_count_file": table.num_rows,
            "sample_rows": df.to_dict(orient="records")
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read parquet: {str(e)}")

# not working will check
@router.post("/parquet/merge")
def merge_parquet(
    namespace: str = Query("pos_transactions"),
    table_name: str = Query("iceberg_with_partitioning"),
    max_merge_rows: int = Query(20000, description="merge all files smaller than this row count")
):
    import time
    import pyarrow.parquet as pq
    import pyarrow.fs as fs
    import pyarrow as pa

    start = time.perf_counter()
    catalog = get_catalog_client()
    tbl = catalog.load_table(f"{namespace}.{table_name}")

    snap = tbl.current_snapshot()
    if not snap:
        raise HTTPException(status_code=400, detail="No snapshot yet")

    # list small files
    small_files = []
    for manifest in snap.manifests(tbl.io):
        entries = manifest.fetch_manifest_entry(tbl.io)
        for e in entries:
            df = e.data_file
            if df.record_count < max_merge_rows:
                small_files.append(df.file_path)

    if not small_files:
        return {"message": "no small files to merge"}

    # read small files
    s3fs = fs.S3FileSystem(
        access_key=os.getenv("ACCESS_KEY_ID"),
        secret_key=os.getenv("SECRET_ACCESS_KEY"),
        endpoint_override=os.getenv("ENDPOINT"),
    )


    tables = []
    for f in small_files:
        print(f"f: {f[5:]}")
        t = pq.read_table(f[5:], filesystem=s3fs)
        tables.append(t)

    merged = pa.concat_tables(tables)

    # write merged file to table (append)
    tbl.append(merged)

    return {
        "merged_files_count": len(small_files),
        "new_file_rows": merged.num_rows,
        "time_seconds": round(time.perf_counter() - start, 3),
    }


@router.post("/parquet/merge-parquet")
def merge_parquet(
    namespace: str = Query("pos_transactions"),
    table_name: str = Query("iceberg_with_partitioning"),
    max_rows: int = Query(20000, description="merge files smaller than this row count"),
):
    import pyarrow.parquet as pq
    import pyarrow.fs as fs
    import pyarrow as pa
    import os
    import time

    start = time.perf_counter()
    catalog = get_catalog_client()
    tbl = catalog.load_table(f"{namespace}.{table_name}")

    snap = tbl.current_snapshot()
    if not snap:
        raise HTTPException(status_code=400, detail="no snapshot")

    # 1) find small files
    small_files = []
    for manifest in snap.manifests(tbl.io):
        entries = manifest.fetch_manifest_entry(tbl.io)
        for e in entries:
            df = e.data_file
            if df.record_count < max_rows:
                small_files.append(df.file_path)

    if not small_files:
        return {"message": "no small files to merge"}

    # 2) read small files
    s3fs = fs.S3FileSystem(
        access_key=os.getenv("ACCESS_KEY_ID"),
        secret_key=os.getenv("SECRET_ACCESS_KEY"),
        endpoint_override=os.getenv("ENDPOINT"),
    )

    tables = []
    for p in small_files:
        raw = p.replace("s3://", "", 1) if p.startswith("s3://") else p
        t = pq.read_table(raw, filesystem=s3fs)
        tables.append(t)

    merged = pa.concat_tables(tables)

    # 3) append new merged file
    tbl.append(merged)

    # 4) delete old small files (AFTER append)
    bucket = os.getenv("BUCKET_NAME")
    r2 = get_catalog_client()

    for p in small_files:
        key = p.replace(f"s3://{bucket}/", "")
        try:
            r2.delete_object(Bucket=bucket, Key=key)
            print("deleted:", key)
        except Exception as e:
            print("delete failed:", key, e)

    return {
        "merged_files_count": len(small_files),
        "new_file_rows": merged.num_rows,
        "seconds": round(time.perf_counter() - start, 3),
        "status": "merged + old files deleted"
    }