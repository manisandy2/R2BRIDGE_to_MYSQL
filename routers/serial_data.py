import uuid
from fastapi import APIRouter,HTTPException,Query,Body
from typing import List
from pyiceberg.exceptions import NoSuchTableError, RESTError, NamespaceAlreadyExistsError
import time
from ..core.catalog_client import get_catalog_client
from ..mapping import *
import json
from ..core.r2_client import get_r2_client
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError, BotoCoreError

router = APIRouter(prefix="", tags=["Serial"])


@router.post("/insertOne")
def single_json(
        bucket_name: str = Query(..., title="Bucket Name"),
        bucket_path: str = Query("iceberg_json", description="Folder path in R2 (default: iceberg_json)"),
        model: dict = Body(..., description="List of JSON models to store in R2"),
):
        start_time = time.time()

        try:
            r2_client = get_r2_client()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to initialize R2 client: {str(e)}")

        uploaded_files = []

        try:
            serial_no = model.get("serial_no", str(uuid.uuid4()))
            model["serial_no"] = serial_no

            r2_key = f"{bucket_path}/{serial_no}.json"

            r2_client.put_object(
                Bucket=bucket_name,
                Key=r2_key,
                Body=json.dumps(model, indent=2, cls=CustomJSONEncoder).encode("utf-8")
            )
            uploaded_files.append(r2_key)
            elapsed = time.time() - start_time
            minutes = int(elapsed // 60)
            seconds = int(elapsed % 60)

            # print("message", f"{len(uploaded_files)} JSON files uploaded to R2")
            # print("files", "uploaded_files")
            # print("Elapsed time", f"{minutes} minutes {seconds} seconds")

            return {
                "status": "success",
                "message": f"{len(uploaded_files)} JSON files uploaded to R2",
                "files": uploaded_files,
                "Elapsed time": f"{minutes} minutes {seconds} seconds"
            }
        except ClientError as e:
            raise HTTPException(status_code=400, detail=f"R2 Client error: {e.response['Error']['Message']}")

        except BotoCoreError as e:
            raise HTTPException(status_code=500, detail=f"R2 BotoCore error: {str(e)}")

        except json.JSONDecodeError as e:
            raise HTTPException(status_code=400, detail=f"JSON serialization error: {str(e)}")

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

@router.post("/insertMany")
def multiple_json(
    bucket_name: str = Query(..., title="Bucket Name"),
    bucket_path: str = Query("iceberg_json", description="Folder path in R2 (default: iceberg_json)"),
    models: List[dict] = Body(..., description="List of JSON models to store in R2"),

):
    start_time = time.time()
    # BATCH_SIZE = 1
    max_workers = 20

    try:
        r2_client = get_r2_client()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to initialize R2 client: {str(e)}")

    uploaded_files = []
    failed_files = []


    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for model in models:
            print("serial :", model["serial_no"])
            if "serial_no" not in model:
                raise HTTPException(status_code=400, detail="Each model must have a serial_no field")

            serial_no = str(model.get("serial_no", uuid.uuid4()))
            model["serial_no"] = serial_no  # ensure string

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
            futures[executor.submit(upload_file, r2_client, bucket_name, r2_key, body)] = r2_key

        # Collect results
        for future in as_completed(futures):
            r2_key = futures[future]
            try:
                result = future.result()
                uploaded_files.append(result)
            except Exception as e:
                print(f"Upload failed for {r2_key}: {e}")
                failed_files.append(r2_key)


    elapsed = time.time() - start_time
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)

    return {
        "status": "success" if not failed_files else "partial",
        "message": f"{len(uploaded_files)} JSON file(s) uploaded, {len(failed_files)} failed",
        "files_uploaded": uploaded_files,
        "files_failed": failed_files,
        "elapsed_time": f"{minutes} minutes {seconds} seconds"
    }


@router.post("/InsertOne")
def insert_one(
    namespace: str = Query(..., description="Namespace (e.g. 'transactions')"),
    table_name: str = Query(..., description="Table (e.g. 'pos')"),
    record: dict = Body(..., description="Single JSON record to insert into Iceberg")
):
    start_time = time.time()

    if not record:
        return {"status": "failed", "message": "No records provided"}

    try:
        iceberg_schema, arrow_schema = infer_schema_from_record(record)

        converted_record = convert_row(record,arrow_schema)
        arrow_table = pa.Table.from_pylist([converted_record], schema=arrow_schema)

        catalog = get_catalog_client()
        table_identifier = f"{namespace}.{table_name}"

        tbl = get_or_create_table(catalog, table_identifier, iceberg_schema)

        tbl.append(arrow_table)
        elapsed = time.time() - start_time

        # metadata = {
        #     "schema_version": getattr(tbl, "schema", lambda: None)().schema_id if hasattr(tbl, "schema") else None,
        #     "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        #     "record_size": len(str(record)),  # rough size in characters
        #     "namespace": namespace,
        #     "table": table_name
        # }
        #
        # preview = {k: (v if isinstance(v, (int, float, bool)) or len(str(v)) < 50 else str(v)[:47] + "...")
        #            for k, v in record.items()}

        ####################
        # r2 = get_r2_client()
        # bucket_name = "iceberg-json"  # change to your bucket
        # object_key = f"{namespace}/{table_name}/{int(time.time())}.json"
        #
        # r2.put_object(
        #     Bucket=bucket_name,
        #     Key=object_key,
        #     Body=json.dumps(record, indent=2).encode("utf-8"),
        #     Metadata={
        #         "namespace": namespace,
        #         "table": table_name,
        #         "rows_written": "1",
        #         "preview": json.dumps({k: str(v) for k, v in list(record.items())[:5]})  # small preview
        #     },
        #     ContentType="application/json"
        # )
        ######################

        return {
            "status": "success",
            "namespace": namespace,
            "table": table_name,
            "rows_written": 1,
            "elapsed_seconds": round(elapsed, 2)
        }

    except RESTError as e:
        raise HTTPException(status_code=500, detail=f"Iceberg REST API error: {str(e)}")

    except NamespaceAlreadyExistsError:
        raise HTTPException(status_code=409, detail=f"Namespace '{namespace}' already exists")

    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found in namespace '{namespace}'")

    except pa.ArrowInvalid as e:
        raise HTTPException(status_code=400, detail=f"Arrow schema error: {str(e)}")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

@router.post("/InsertMany")
def insert_many(
    namespace: str = Query(..., description="Namespace (e.g. 'transactions')"),
    table_name: str = Query(..., description="Table (e.g. 'pos')"),
    records: List[dict] = Body(..., description="List of JSON records to insert into Iceberg")
):
    start_time = time.time()

    if not records:
        raise HTTPException(status_code=400, detail="No records provided")

    try:
        # schema_record = records[0]

        iceberg_schema, arrow_schema = infer_schema_from_record(records[0])
        # print("iceberg_schema", iceberg_schema)
        # print("arrow_schema", arrow_schema)
        converted_records = [convert_row(r,arrow_schema) for r in records]
        arrow_table = pa.Table.from_pylist(converted_records, schema=arrow_schema)

        catalog = get_catalog_client()
        table_identifier = f"{namespace}.{table_name}"

        tbl = get_or_create_table(catalog, table_identifier, iceberg_schema)

        tbl.append(arrow_table)
        elapsed = time.time() - start_time

        return {
            "status": "success",
            "namespace": namespace,
            "table": table_name,
            "rows_written": len(records),
            "elapsed_seconds": round(elapsed, 2)
        }
    except RESTError as e:
        raise HTTPException(status_code=500, detail=f"Iceberg REST API error: {str(e)}")

    except NamespaceAlreadyExistsError:
        raise HTTPException(status_code=409, detail=f"Namespace '{namespace}' already exists")

    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found in namespace '{namespace}'")

    except pa.ArrowInvalid as e:
        raise HTTPException(status_code=400, detail=f"Arrow schema error: {str(e)}")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")