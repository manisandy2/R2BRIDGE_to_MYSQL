from fastapi import APIRouter,HTTPException,Query,Body
from typing import List
import time
import json
import decimal
import datetime
from ..core.r2_client import get_r2_client
from ..core.mysql_client import MysqlCatalog
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

router = APIRouter(prefix="/DataStore", tags=["Store"])

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        if isinstance(obj, bytes):
            return obj.decode("utf-8", errors="ignore")
        return super().default(obj)


@router.post("/Normal/Create")
def create_table_json_store(

        bucket_name: str = Query(..., title="Bucket Name"),
        dbname:str = Query(..., description="Database name"),
        bucket_path: str = Query("iceberg_json", description="Folder path in R2 (default: iceberg_json)"),
        start_range: int = Query(0, description="Start row (e.g. 0)"),
        end_rage: int = Query(100, description="End row (e.g. 100)")
):
        start_time = time.time()
        mysql_catalog = MysqlCatalog()
        rows = mysql_catalog.get_range(dbname,start= start_range, end=end_rage)
        r2_client = get_r2_client()

        uploaded_files = []

        for row in rows:
            row_dict = dict(row)
            if "pri_id" not in row_dict:
                continue

            pri_id_str = str(row_dict["pri_id"])
            row_dict["pri_id"] = pri_id_str

            # r2_key = f"iceberg_json/{namespace}_{table_name}/model_{pri_id_str}.json"
            r2_key = f"{bucket_path}/{pri_id_str}.json"

            r2_client.put_object(
                Bucket=bucket_name,
                Key=r2_key,
                Body=json.dumps(row_dict, indent=2, cls=CustomJSONEncoder).encode("utf-8")
            )
            uploaded_files.append(r2_key)
            elapsed = time.time() - start_time
            minutes = int(elapsed // 60)
            seconds = int(elapsed % 60)

            print("message", f"{len(uploaded_files)} JSON files uploaded to R2")
            print("files", "uploaded_files")
            print("Elapsed time", f"{minutes} minutes {seconds} seconds")


        elapsed = time.time() - start_time
        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)
        return {
            "message": f"{len(uploaded_files)} JSON files uploaded to R2",
            "files": uploaded_files,
            "Elapsed time": f"{minutes} minutes {seconds} seconds"
        }


@router.post("/Normal/IncrestOne")
def create_table_json_single_store(
        bucket_name: str = Query(..., title="Bucket Name"),
        bucket_path: str = Query("iceberg_json", description="Folder path in R2 (default: iceberg_json)"),
        model: dict = Body(..., description="JSON model to store in R2")
):
    start_time = time.time()
    r2_client = get_r2_client()

    uploaded_files = []

    # Ensure primary key exists
    if "pri_id" not in model:
        return {"error": "pri_id field is required in the model"}

    pri_id_str = str(model["pri_id"])
    model["pri_id"] = pri_id_str

    # Store object in R2
    r2_key = f"{bucket_path}/{pri_id_str}.json"

    r2_client.put_object(
        Bucket=bucket_name,
        Key=r2_key,
        Body=json.dumps(model, indent=2, cls=CustomJSONEncoder).encode("utf-8")
    )
    uploaded_files.append(r2_key)

    elapsed = time.time() - start_time
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)

    return {
        "message": f"{len(uploaded_files)} JSON file uploaded to R2",
        "files": uploaded_files,
        "Elapsed time": f"{minutes} minutes {seconds} seconds"
    }
#
BATCH_SIZE = 1   # rows per JSON file
MAX_WORKERS = 10    # parallel uploads


def upload_file(r2_client, bucket, key, body):
    r2_client.put_object(Bucket=bucket, Key=key, Body=body)
    return key
#
@router.post("/fast/Create")
def create_table_json_store(
    bucket_name: str = Query(..., title="Bucket Name"),
    dbname: str = Query(..., description="Database name"),
    bucket_path: str = Query("iceberg_json", description="Folder path in R2 (default: iceberg_json)"),
    start_range: int = Query(0, description="Start row (e.g. 0)"),
    end_range: int = Query(100, description="End row (e.g. 100)")

):
    start_time = time.time()

    mysql_catalog = MysqlCatalog()
    rows = mysql_catalog.get_range(dbname, start=start_range, end=end_range)

    # cloud_r2_creds = CloudflareR2Creds()
    r2_client = get_r2_client()



    uploaded_files = []
    futures = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for i in range(0, len(rows), BATCH_SIZE):
            # print("#"*100)
            print(rows[i]["pri_id"])
            # print(f"{i}")
            batch = rows[i:i + BATCH_SIZE]

            # file_key = f"iceberg_json/{namespace}_{table_name}/batch_{i//BATCH_SIZE}.json"
            file_key = f"{bucket_path}/{rows[i]["pri_id"]}.json"
            body = json.dumps(batch, indent=2, cls=CustomJSONEncoder).encode("utf-8")

            futures.append(executor.submit(upload_file, r2_client, bucket_name, file_key, body))

        for future in as_completed(futures):
            print(future.result())
            elapsed = time.time() - start_time
            minutes = int(elapsed // 60)
            seconds = int(elapsed % 60)
            print("elapsed_time", f"{minutes} minutes {seconds} seconds")

            try:
                uploaded_files.append(future.result())
            except Exception as e:
                print(f"Upload failed: {e}")

    elapsed = time.time() - start_time
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)

    return {
        "message": f"{len(uploaded_files)} JSON files uploaded to R2",
        "files": uploaded_files,
        "elapsed_time": f"{minutes} minutes {seconds} seconds"
    }

@router.get("/list")
def get_bucket_list(
    bucket_name: str = Query(..., title="Bucket Name"),
    bucket_path: str = Query("iceberg_json", description="Folder path in R2 (default: iceberg_json)"),

):
    r2_client = get_r2_client()
    prefix = f"{bucket_path}/"

    try:
        response = r2_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        print(response)

        files = []
        if "Contents" in response:
            files = [obj["Key"] for obj in response["Contents"]]

        return {

            "total_files": len(files),
            "files": files
        }

    except Exception as e:
        return {"error": str(e)}


@router.delete("/delete-files")
def delete_files(
        bucket_name: str = Query(..., title="Bucket Name"),
        bucket_path: str = Query("iceberg_json", description="Folder path in R2 (default: iceberg_json)"),
        prefix_only: bool = Query(True, description="Delete all files under prefix (True) or specific file (False)"),
        file_name: str = Query(None, description="Specific file name (e.g. 'batch_0.json') if prefix_only=False")
):


    r2_client = get_r2_client()
    prefix = f"{bucket_path}/"
    print("Deleting files")
    print(f"{prefix}")
    try:
        deleted_files = []

        if prefix_only:
            response = r2_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            if "Contents" in response:
                for obj in response["Contents"]:
                    print(obj["Key"])
                    r2_client.delete_object(Bucket=bucket_name, Key=obj["Key"])
                    deleted_files.append(obj["Key"])
        else:
            if not file_name:
                return {"error": "file_name is required if prefix_only=False"}

            file_key = prefix + file_name
            r2_client.delete_object(Bucket=bucket_name, Key=file_key)
            deleted_files.append(file_key)

        return {
            "message": f"{len(deleted_files)} file(s) deleted",
            "deleted_files": deleted_files
        }

    except Exception as e:
        return {"error": str(e)}



@router.post("/Normal/insertMany")
def create_bucket_multiple_store(
        bucket_name: str = Query(..., title="Bucket Name"),
        models: List[dict] = Body(..., description="List of JSON models to store in R2"),
        bucket_path: str = Query("iceberg_json", description="Folder path in R2 (default: iceberg_json)")
):
    start_time = time.time()

    try:
        # cloud_r2_creds = CloudflareR2Creds()
        r2_client = get_r2_client()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to initialize R2 client: {str(e)}")

    uploaded_files = []

    try:
        for model in models:
            print("serial :",model["serial_no"])
            if "serial_no" not in model:
                raise HTTPException(status_code=400, detail="Each model must have a serial_no field")

            serial_no = str(model["serial_no"])
            model["serial_no"] = serial_no  # ensure string

            # Key path for R2
            r2_key = f"{bucket_path.rstrip('/')}/{serial_no}.json"

            # metadata = {
            #     "author": "Mani",
            #     "project": "CentralInventory",
            #     "serial_no": serial_no
            # }

            # Upload JSON object
            r2_client.put_object(
                Bucket=bucket_name,
                Key=r2_key,
                Body=json.dumps(model, indent=2, cls=CustomJSONEncoder).encode("utf-8"),
                ContentType="application/json",
                # Metadata=metadata,
            )
            uploaded_files.append(r2_key)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload objects to R2: {str(e)}")

    elapsed = time.time() - start_time
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)

    return {
        "message": f"{len(uploaded_files)} JSON files uploaded to R2",
        "files": uploaded_files,
        "Elapsed time": f"{minutes} minutes {seconds} seconds"
    }

@router.post("/insertMany/fast")
def create_bucket_multiple_store_fast(
    bucket_name: str = Query(..., title="Bucket Name"),
    bucket_path: str = Query("iceberg_json", description="Folder path in R2 (default: iceberg_json)"),
    models: List[dict] = Body(..., description="List of JSON models to store in R2"),

):
    start_time = time.time()

    try:
        # cloud_r2_creds = CloudflareR2Creds()
        r2_client = get_r2_client()
        # bucket = os.getenv("BUCKET_NAME")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to initialize R2 client: {str(e)}")

    uploaded_files = []
    futures = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for model in models:
            print("serial :", model["serial_no"])
            if "serial_no" not in model:
                raise HTTPException(status_code=400, detail="Each model must have a serial_no field")

            serial_no = str(model["serial_no"])
            model["serial_no"] = serial_no  # ensure string

            # Key path
            r2_key = f"{bucket_path.rstrip('/')}/{serial_no}.json"

            # Metadata
            # metadata = {
            #     "author": "Mani",
            #     "project": "CentralInventory",
            #     "serial_no": serial_no
            # }

            # File body
            body = json.dumps(model, indent=2, cls=CustomJSONEncoder).encode("utf-8")

            # Submit task
            # futures.append(executor.submit(upload_file, r2_client, bucket_name, r2_key, body, metadata))
            futures.append(executor.submit(upload_file, r2_client, bucket_name, r2_key, body))

        # Collect results
        for future in as_completed(futures):
            try:
                uploaded_files.append(future.result())
            except Exception as e:
                print(f"Upload failed: {e}")

    elapsed = time.time() - start_time
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)

    return {
        "message": f"{len(uploaded_files)} JSON files uploaded to R2",
        "files": uploaded_files,
        "Elapsed time": f"{minutes} minutes {seconds} seconds"
    }