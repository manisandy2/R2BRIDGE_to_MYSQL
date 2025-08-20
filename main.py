from fastapi import FastAPI,Query,Body,HTTPException
# from mysql_catalog import MysqlCatalog
from .mysql_creds import  MysqlCatalog
from pyiceberg.exceptions import NoSuchNamespaceError,NamespaceAlreadyExistsError,TableAlreadyExistsError
# from creds import Creds
from .creds import Creds, CloudflareR2Creds
from pydantic import BaseModel
from pyiceberg.exceptions import NoSuchTableError
from .mapping import *
from pyiceberg.schema import Schema, NestedField
import json
import time
import os
from typing import List
from fastapi import FastAPI, Query, HTTPException
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import GreaterThanOrEqual,EqualTo
from decimal import Decimal
# from routers import namespace.router
import json
import decimal
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
from mysql.connector import Error
import pandas as pd

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        if isinstance(obj, bytes):
            return obj.decode("utf-8", errors="ignore")
        return super().default(obj)

app = FastAPI()

ALLOWED_TABLES = ["Transaction",]

@app.get("/")
def root():
    tables_name = ["Transaction", ]

    return {"message": "API is running",
            "version": "1.0",
            "Tables": tables_name
            }


@app.get("/table/count")
def get_count(table_name: str = Query(..., description="Table name")):
    catalog = MysqlCatalog()
    try:
        count = catalog.get_count(table_name)
        return {"count": count}
    except Error as e:
        # Database-specific error
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        # Generic error (e.g. wrong table name, runtime issue)
        raise HTTPException(status_code=400, detail=f"Error fetching count: {str(e)}")
    finally:
        catalog.close()


@app.get("/table/schema")
def table_schema(table_name: str = Query(..., description="Table name")):
    catalog = MysqlCatalog()
    try:
        description = catalog.get_describe(table_name)
        if not description:
            raise HTTPException(
                status_code=404,
                detail={
                    "error_code": "TABLE_NOT_FOUND",
                    "message": f"Table '{table_name}' not found"
                }
            )
        return {"schema": description}

    except Error as e:
        # Database-related error
        raise HTTPException(
            status_code=500,
            detail={
                "error_code": "DB_ERROR",
                "message": str(e)
            }
        )
    except Exception as e:
        # Unexpected error
        raise HTTPException(
            status_code=400,
            detail={
                "error_code": "BAD_REQUEST",
                "message": str(e)
            }
        )
    finally:
        catalog.close()


@app.get("/transactions/namespaces/list")
def list_namespaces():
    try:
        catalog = Creds().catalog_valid()
        namespaces = catalog.list_namespaces()
        return {"namespaces": namespaces}
    # catalog.list_namespaces
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list namespaces: {str(e)}")

class NamespaceRequest(BaseModel):
    name: str

@app.post("/transactions/namespaces/Create")
def create_namespace(namespace: str = Query(..., description="Namespace (e.g. 'transaction')"),):
    try:
        catalog = Creds().catalog_valid()
        catalog.create_namespace(namespace)
        return {"message": f"Namespace '{namespace}' created successfully."}
    except NamespaceAlreadyExistsError:
        raise HTTPException(status_code=409, detail=f"Namespace '{namespace}' already exists.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create namespace '{namespace}': {str(e)}")

@app.delete("/transactions/namespaces/Delete")
def delete_namespace(namespace: str = Query(..., description="Namespace to delete")):
    catalog = Creds().catalog_valid()
    try:
        catalog.drop_namespace(namespace)
        # print(f"Table '{f"{namespace}"}' dropped successfully.")
        return {"message": f" Namespace '{namespace}' dropped successfully."}
    except NoSuchNamespaceError:
        # print(f"Table '{f"{namespace}"}' does not exist.")
        raise HTTPException(status_code=404, detail=f"Namespace '{namespace}' does not exist.")
    except Exception as e:
        # print(f"Failed to drop table '{f"{namespace}"}': {e}")
        raise HTTPException(status_code=500, detail=f"Failed to delete namespace '{namespace}': {str(e)}")


def convert_row(row, column_types):
    """Convert MySQL row values to types PyArrow accepts."""
    converted = []
    for value, col_type in zip(row, column_types):
        if col_type.startswith("decimal") and value is not None:
            # Always convert to string to keep precision and satisfy PyArrow
            converted.append(str(value))
        else:
            converted.append(value)
    return converted



def normalize_mysql_type(t):
    return re.sub(r"\(.*\)", "", t).strip().lower()

@app.get("/transactions/tables/list")
def list_tables(namespace: str = Query(..., description="Namespace to list tables from")):
    try:
        catalog = Creds().catalog_valid()
        tables = catalog.list_tables(namespace)

        if tables:
            return {"namespace": namespace, "tables": tables}
        else:
            return {"namespace": namespace, "tables": [], "message": "No tables found."}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list tables in namespace '{namespace}': {str(e)}")

@app.post("/transactions/table/create")
def transactions(
    namespace: str = Query(..., description="transactions (e.g. 'transactions')"),
    table_name: str = Query(..., description="transactions pos (e.g. 'transactions pos')"),
    start_range: int = Query(0, description="Start row (e.g. 0)"),
    end_range: int = Query(100000, description="End row (e.g. 100000)"),
    dbname:str = Query(..., description="Database name")
):
    start_time = time.time()

    mysql_creds = MysqlCatalog()
    try:
        description = mysql_creds.get_describe(dbname)
        rows = mysql_creds.get_range(dbname,start_range,end_range)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")

    if not rows:
        raise HTTPException(status_code=400, detail="No data found in the given range.")

    iceberg_fields = []
    arrow_fields = []

    for idx, column in enumerate(description):

        name = column["Field"]
        col_type = column["Type"].split('(')[0].lower()
        is_nullable = column["Null"].upper() == "YES"

        is_primary = column["Key"] == "PRI"
        is_unique = column["Key"] == "UNI"

        ice_type = type_mapping.get(col_type, StringType())
        arrow_type = arrow_mapping.get(col_type, pa.string())

        # tu_rows.append([name, col_type,is_nullable,is_key,is_primary, str(ice_type),str(arrow_type)])

        iceberg_fields.append(NestedField(field_id=idx + 1, name=name, field_type=ice_type, required=not is_nullable))

        arrow_fields.append(pa.field(name, arrow_type, nullable=is_nullable))


    iceberg_schema = Schema(*iceberg_fields)
    arrow_schema = pa.schema(arrow_fields)

    pylist_rows = []
    for row in rows:
        converted = {}
        # print(row)
        for field in arrow_schema:
            val = row[field.name]

            if pa.types.is_integer(field.type):
                converted[field.name] = int(val) if val is not None else None
            elif pa.types.is_floating(field.type):
                converted[field.name] = float(val) if val is not None else None
            else:
                converted[field.name] = val
        pylist_rows.append(converted)

    arrow_table = pa.Table.from_pylist(pylist_rows, schema=arrow_schema)

    creds = Creds()
    catalog = creds.catalog_valid()

    table_identifier = "{}.{}".format(namespace, table_name)
    try:
        tbl = catalog.create_table(table_identifier, schema=iceberg_schema)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating table: {str(e)}")
    tbl.append(arrow_table)

    elapsed = time.time() - start_time
    return {
        "status": "success",
        # "action": action,
        "namespace": namespace,
        "table": table_name,
        "rows_written": len(pylist_rows),
        "elapsed_seconds": round(elapsed, 2)
    }

@app.put("/transactions/table/update")
def update_transactions(
    namespace: str = Query(..., description="transactions (e.g. 'transactions')"),
    table_name: str = Query(..., description="transactions pos (e.g. 'transactions pos')"),
    start_range: int = Query(0, description="Start row (e.g. 0)"),
    end_range: int = Query(100000, description="End row (e.g. 100000)"),
    dbname:str = Query(..., description="Database name")
):
    start_time = time.time()

    mysql_creds = MysqlCatalog()
    try:
        description = mysql_creds.get_describe(dbname)
        rows = mysql_creds.get_range(dbname,start_range,end_range)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")

    if not rows:
        raise HTTPException(status_code=400, detail="No data found in the given range.")

    iceberg_fields = []
    arrow_fields = []

    for idx, column in enumerate(description):
        name = column["Field"]
        col_type = column["Type"].split('(')[0].lower()
        is_nullable = column["Null"].upper() == "YES"

        ice_type = type_mapping.get(col_type, StringType())
        arrow_type = arrow_mapping.get(col_type, pa.string())

        # tu_rows.append([name, col_type,is_nullable,is_key,is_primary, str(ice_type),str(arrow_type)])

        iceberg_fields.append(NestedField(field_id=idx + 1, name=name, field_type=ice_type, required=not is_nullable))
        arrow_fields.append(pa.field(name, arrow_type, nullable=is_nullable))

    iceberg_schema = Schema(*iceberg_fields)
    arrow_schema = pa.schema(arrow_fields)

    pylist_rows = []
    for row in rows:
        converted = {}
        for field in arrow_schema:
            val = row[field.name]

            if pa.types.is_integer(field.type):
                converted[field.name] = int(val) if val is not None else None
            elif pa.types.is_floating(field.type):
                converted[field.name] = float(val) if val is not None else None
            else:
                converted[field.name] = val
        pylist_rows.append(converted)

    arrow_table = pa.Table.from_pylist(pylist_rows, schema=arrow_schema)

    creds = Creds()
    catalog = creds.catalog_valid()

    table_identifier = "{}.{}".format(namespace, table_name)

    try:
        tbl = catalog.load_table(table_identifier)
    except Exception:
        raise HTTPException(status_code=404, detail=f"Table '{table_identifier}' not found.")

    tbl.append(arrow_table)
    elapsed = time.time() - start_time
    return {
        "status": "success",
        # "action": action,
        "namespace": namespace,
        "table": table_name,
        "rows_written": len(pylist_rows),
        "elapsed_seconds": round(elapsed, 2)
    }



#####
@app.delete("/transactions/tables/delete")
def delete_table(
    namespace: str = Query(..., description="Namespace of the table"),
    table_name: str = Query(..., description="Name of the table to drop")
):
    catalog = Creds().catalog_valid()
    full_table_name = f"{namespace}.{table_name}"

    try:
        catalog.drop_table(full_table_name)
        return {"message": f"Table '{full_table_name}' dropped successfully."}

    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table '{full_table_name}' does not exist.")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to drop table '{full_table_name}': {str(e)}")


# @app.get("/iceberg/get-table-data")
# def read_table(
#     namespace: str = Query(..., description="Namespace (e.g. 'Namespace')"),
#     table_name: str = Query(..., description="Table name (e.g. 'Table name')")
# ):
#     try:
#         catalog = Creds().catalog_valid()
#         table = catalog.load_table((namespace, table_name))
#
#         reader = table.scan().to_arrow()
#         df = reader.to_pandas()
#
#         return {
#             "namespace": namespace,
#             "table_name": table_name,
#             "records_count": len(df),
#             "data": df.to_dict(orient="records")
#         }
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed to read Iceberg table: {str(e)}")

@app.get("/Transaction/table/data")
def read_table(
    namespace: str = Query(..., description="Namespace (e.g. 'Namespace')"),
    table_name: str = Query(..., description="Table name (e.g. 'Table name')")
):
    try:
        catalog = Creds().catalog_valid()
        table = catalog.load_table((namespace, table_name))

        reader = table.scan().to_arrow()
        df = reader.to_pandas()

        # Replace NaN/Inf with None so JSON can serialize
        df = df.replace({pd.NA: None, float("nan"): None, float("inf"): None, -float("inf"): None})

        return {
            "namespace": namespace,
            "table_name": table_name,
            "records_count": len(df),
            "data": df.to_dict(orient="records")
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read Iceberg table: {str(e)}")

from pyiceberg.expressions import And, GreaterThanOrEqual, LessThanOrEqual

@app.get("/Transaction/table/dataWithFilter")
def read_table(
    namespace: str = Query(..., description="Namespace (e.g. 'Namespace')"),
    table_name: str = Query(..., description="Table name (e.g. 'Table name')"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)")
):
    try:
        catalog = Creds().catalog_valid()
        table = catalog.load_table((namespace, table_name))

        # Convert to ISO timestamps (assuming CreatedDate is stored as timestamp or date)
        start = f"{start_date}T00:00:00"
        end = f"{end_date}T23:59:59"

        # Build filter expression
        filter_expr = And(
            GreaterThanOrEqual("CreatedDate", start),
            LessThanOrEqual("CreatedDate", end),
        )

        # Apply filter at scan level
        reader = table.scan(row_filter=filter_expr).to_arrow()
        df = reader.to_pandas()

        df = df.replace({pd.NA: None, float("nan"): None, float("inf"): None, -float("inf"): None})

        return {
            "namespace": namespace,
            "table_name": table_name,
            "records_count": len(df),
            "data": df.to_dict(orient="records")
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read Iceberg table: {str(e)}")


@app.get("/transactions/table/Inspect")
def table_inspect(
    namespace: str = Query(..., description="Namespace (e.g. 'Namespace')"),
    table_name: str = Query(..., description="Table name (e.g. 'Table name')")
):
    try:
        catalog = Creds().catalog_valid()
        table = catalog.load_table((namespace, table_name))

        snapshots = list(table.snapshots())

        snapshot_data = []
        for s in snapshots:
            snapshot_data.append({
                "snapshot_id": getattr(s, "snapshot_id", None),
                "parent_snapshot_id": getattr(s, "parent_snapshot_id", None),
                "timestamp_ms": getattr(s, "timestamp_ms", None),
                "manifest_list": getattr(s, "manifest_list", None),
                "summary": getattr(s, "summary", {})
            })

        return {
            "namespace": namespace,
            "table_name": table.name,
            "records_count": len(snapshots),
            "snapshots": snapshot_data
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to inspect table: {str(e)}")

# @app.get("/Transaction/scan-files")
# def scan_iceberg_files(
#     namespace: str = Query(..., description="Namespace (e.g. 'nyc')"),
#     table_name: str = Query(..., description="Table name (e.g. 'taxis')"),
#     column: str = Query(..., description="Column to filter on (e.g. 'trip_distance')"),
#     min_value: int = Query(..., description="Minimum value for filtering"),
#     limit: int = Query(100, description="Limit on number of rows to scan")
# ):
#     try:
#
#         creds = Creds()
#         catalog = creds.catalog_valid()
#
#         table_identifier = f"{namespace}.{table_name}"
#
#         table = catalog.load_table(table_identifier)
#         schema_obj = table.schema()
#
#         field_type = schema_obj.find_field(column).field_type
#
#         if isinstance(field_type, (IntegerType, LongType)):
#             cast_value = int(float(min_value))
#         elif isinstance(field_type, (FloatType, DoubleType)):
#             cast_value = float(min_value)
#         elif isinstance(field_type, StringType):
#             cast_value = str(min_value)
#         else:
#             raise HTTPException(status_code=400, detail=f"Unsupported column type: {field_type}")
#
#         scan = table.scan(
#             row_filter=EqualTo(column, cast_value),
#             limit=limit
#         )
#
#         file_paths = [task.file.file_path for task in scan.plan_files()]
#
#         return {
#             "table": table_identifier,
#             "filter": f"{column} >= {cast_value}",
#             "file_count": len(file_paths),
#             "files": file_paths,
#             "data": scan.to_array()
#         }
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error scanning table: {str(e)}")




# normal
@app.post("/Transaction/bucket/Normal/Create")
def create_table_json_store(
        namespace: str = Query(..., description="Namespace (e.g. 'Namespace')"),
        table_name: str = Query(..., description="Table name (e.g. 'Table name')"),
        start_range: int = Query(0, description="Start row (e.g. 0)"),
        end_rage: int = Query(100, description="End row (e.g. 100)"),
        dbname:str = Query(..., description="Database name")
):
        start_time = time.time()

        mysql_catalog = MysqlCatalog()
        # description = mysql_catalog.get_describe(dbname)
        # columns = [col["Field"] for col in description]

        rows = mysql_catalog.get_range(dbname,start= start_range, end=end_rage)

        cloud_r2_creds = CloudflareR2Creds()
        r2_client = cloud_r2_creds.get_client()

        uploaded_files = []

        for row in rows:
            # print(idx, column)
            row_dict = dict(row)
            if "pri_id" not in row_dict:
                continue

            pri_id_str = str(row_dict["pri_id"])
            row_dict["pri_id"] = pri_id_str

            # r2_key = f"iceberg_json/{namespace}_{table_name}/model_{pri_id_str}.json"
            r2_key = f"iceberg_json/{pri_id_str}.json"

            r2_client.put_object(
                Bucket=os.getenv("BUCKET_NAME"),
                Key=r2_key,
                Body=json.dumps(row_dict,indent=2,cls=CustomJSONEncoder).encode("utf-8")
            )
            uploaded_files.append(r2_key)
            elapsed = time.time() - start_time
            minutes = int(elapsed // 60)
            seconds = int(elapsed % 60)
            # print(row)
            print("message", f"{len(uploaded_files)} JSON files uploaded to R2")
            print("files", "uploaded_files")
            print("Elapsed time", f"{minutes} minutes {seconds} seconds")

        # elapsed = time.time() - start_time
        #
        # minutes = int(elapsed // 60)
        # seconds = int(elapsed % 60)
        elapsed = time.time() - start_time
        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)
        return {
            "message": f"{len(uploaded_files)} JSON files uploaded to R2",
            "files": uploaded_files,
            "Elapsed time": f"{minutes} minutes {seconds} seconds"
        }

BATCH_SIZE = 1   # rows per JSON file
MAX_WORKERS = 10    # parallel uploads

def upload_file(r2_client, bucket, key, body):
    """Helper to upload a file to R2."""
    r2_client.put_object(Bucket=bucket, Key=key, Body=body)
    return key

@app.post("/Transaction/bucket/fast/Create")
def create_table_json_store(
    namespace: str = Query(..., description="Namespace (e.g. 'Namespace')"),
    table_name: str = Query(..., description="Table name (e.g. 'Table name ')"),
    start_range: int = Query(0, description="Start row (e.g. 0)"),
    end_range: int = Query(100, description="End row (e.g. 100)"),
    dbname: str = Query(..., description="Database name")
):
    start_time = time.time()

    mysql_catalog = MysqlCatalog()
    rows = mysql_catalog.get_range(dbname, start=start_range, end=end_range)

    cloud_r2_creds = CloudflareR2Creds()
    r2_client = cloud_r2_creds.get_client()

    bucket = os.getenv("BUCKET_NAME")

    uploaded_files = []
    futures = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for i in range(0, len(rows), BATCH_SIZE):
            # print("#"*100)
            print(rows[i]["pri_id"])
            # print(f"{i}")
            batch = rows[i:i + BATCH_SIZE]

            # file_key = f"iceberg_json/{namespace}_{table_name}/batch_{i//BATCH_SIZE}.json"
            file_key = f"iceberg_json/{rows[i]['pri_id']}.json"
            body = json.dumps(batch, indent=2, cls=CustomJSONEncoder).encode("utf-8")

            futures.append(executor.submit(upload_file, r2_client, bucket, file_key, body))

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

@app.get("/Transaction/bucket/list")
def get_bucket_list(
    namespace: str = Query(..., description="Namespace (e.g. 'Namespace')"),
    table_name: str = Query(..., description="Table name (e.g. 'Table name')"),
    folder_path: str = Query(..., description="Folder Path (e.g. 'Folder Path')")
):

    cloud_r2_creds = CloudflareR2Creds()
    r2_client = cloud_r2_creds.get_client()
    bucket = os.getenv("BUCKET_NAME")

    # prefix = f"iceberg_json/{namespace}_{table_name}/"
    prefix = f"{folder_path}/"

    try:
        response = r2_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        print(response)

        files = []
        if "Contents" in response:
            files = [obj["Key"] for obj in response["Contents"]]

        return {
            "namespace": namespace,
            "table_name": table_name,
            "total_files": len(files),
            "files": files
        }

    except Exception as e:
        return {"error": str(e)}


@app.delete("/Transaction/bucket/delete-files")
def delete_files(
        namespace: str = Query(..., description="Namespace (e.g. 'employees')"),
        table_name: str = Query(..., description="Table name (e.g. 'people')"),
        prefix_only: bool = Query(True, description="Delete all files under prefix (True) or specific file (False)"),
        file_name: str = Query(None, description="Specific file name (e.g. 'batch_0.json') if prefix_only=False")
):

    cloud_r2_creds = CloudflareR2Creds()
    r2_client = cloud_r2_creds.get_client()
    bucket = os.getenv("BUCKET_NAME")

    # prefix = f"iceberg_json/{namespace}_{table_name}/"
    prefix = f"iceberg_json/"
    print("Deleting files")
    print(f"{prefix}")
    try:
        deleted_files = []

        if prefix_only:
            response = r2_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            if "Contents" in response:
                for obj in response["Contents"]:
                    print(obj["Key"])
                    r2_client.delete_object(Bucket=bucket, Key=obj["Key"])
                    deleted_files.append(obj["Key"])
        else:
            if not file_name:
                return {"error": "file_name is required if prefix_only=False"}

            file_key = prefix + file_name
            r2_client.delete_object(Bucket=bucket, Key=file_key)
            deleted_files.append(file_key)

        return {
            "message": f"{len(deleted_files)} file(s) deleted",
            "deleted_files": deleted_files
        }

    except Exception as e:
        return {"error": str(e)}
    

@app.post("/Transaction/bucket/insertone")
def create_bucket_single_store(
        model: dict = Body(..., description="JSON model to store in R2"),
        bucket_path: str = Query("iceberg_json", description="Folder path in R2 (default: iceberg_json)")
):
    start_time = time.time()

    try:
        cloud_r2_creds = CloudflareR2Creds()
        r2_client = cloud_r2_creds.get_client()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to initialize R2 client: {str(e)}")

    uploaded_files = []

    # Ensure primary key exists
    if "serial_no" not in model:
        raise HTTPException(status_code=400, detail="serial_no field is required in the model")

    try:
        serial_no = str(model["serial_no"])
        model["serial_no"] = serial_no

        # Store object in R2
        r2_key = f"{bucket_path.rstrip('/')}/{serial_no}.json"

        r2_client.put_object(
            Bucket=os.getenv("BUCKET_NAME"),
            Key=r2_key,
            Body=json.dumps(model, indent=2, cls=CustomJSONEncoder).encode("utf-8")
        )
        uploaded_files.append(r2_key)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload object to R2: {str(e)}")

    elapsed = time.time() - start_time
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)

    return {
        "message": f"{len(uploaded_files)} JSON file uploaded to R2",
        "files": uploaded_files,
        "Elapsed time": f"{minutes} minutes {seconds} seconds"
    }




@app.post("/Transaction/bucket/insertMany")
def create_bucket_multiple_store(
        models: List[dict] = Body(..., description="List of JSON models to store in R2"),
        bucket_path: str = Query("iceberg_json", description="Folder path in R2 (default: iceberg_json)")
):
    start_time = time.time()

    try:
        cloud_r2_creds = CloudflareR2Creds()
        r2_client = cloud_r2_creds.get_client()
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

            metadata = {
                "author": "Mani",
                "project": "CentralInventory",
                "serial_no": serial_no
            }

            # Upload JSON object
            r2_client.put_object(
                Bucket=os.getenv("BUCKET_NAME"),
                Key=r2_key,
                Body=json.dumps(model, indent=2, cls=CustomJSONEncoder).encode("utf-8"),
                ContentType="application/json",
                Metadata=metadata,
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


def upload_file(r2_client, bucket: str, key: str, body: bytes, metadata: dict):
    """Helper function to upload a single file to R2"""
    try:
        r2_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
            Metadata=metadata,
        )
        return key
    except Exception as e:
        raise RuntimeError(f"Upload failed for {key}: {str(e)}")


@app.post("/Transaction/bucket/insertMany/fast")
def create_bucket_multiple_store_fast(
    models: List[dict] = Body(..., description="List of JSON models to store in R2"),
    bucket_path: str = Query("iceberg_json", description="Folder path in R2 (default: iceberg_json)")
):
    start_time = time.time()

    try:
        cloud_r2_creds = CloudflareR2Creds()
        r2_client = cloud_r2_creds.get_client()
        bucket = os.getenv("BUCKET_NAME")
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
            metadata = {
                "author": "Mani",
                "project": "CentralInventory",
                "serial_no": serial_no
            }

            # File body
            body = json.dumps(model, indent=2, cls=CustomJSONEncoder).encode("utf-8")

            # Submit task
            futures.append(executor.submit(upload_file, r2_client, bucket, r2_key, body, metadata))

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

@app.get("/Transaction/bucket/listWithTotalCount")
def get_bucket_list(
    namespace: str = Query(..., description="Namespace (e.g. 'Namespace')"),
    folder_path: str = Query(..., description="Folder Path (e.g. 'Folder Path')")
):
    cloud_r2_creds = CloudflareR2Creds()
    r2_client = cloud_r2_creds.get_client()
    bucket = os.getenv("BUCKET_NAME")

    prefix = f"{folder_path}/"

    try:
        files = []
        continuation_token = None

        while True:
            if continuation_token:
                response = r2_client.list_objects_v2(
                    Bucket=bucket,
                    Prefix=prefix,
                    ContinuationToken=continuation_token
                )
            else:
                response = r2_client.list_objects_v2(
                    Bucket=bucket,
                    Prefix=prefix
                )

            if "Contents" in response:
                files.extend([obj["Key"] for obj in response["Contents"]])

            # Check if there are more objects to fetch
            if response.get("IsTruncated"):
                continuation_token = response.get("NextContinuationToken")
            else:
                break

        return {
            "namespace": namespace,
            "path name": folder_path,
            "total_files": len(files),   # ✅ total number of objects
            "files": files               # ✅ all object keys
        }

    except Exception as e:
        return {"error": str(e)}
    
@app.get("/Transaction/bucket/listWithPagination")
def get_bucket_list(
    namespace: str = Query(..., description="Namespace (e.g. 'Namespace')"),
    folder_path: str = Query(..., description="Folder Path (e.g. 'Folder Path')"),
    page_size: int = Query(100, description="Max number of files to return (default=100, max=1000)"),
    continuation_token: str = Query(None, description="Token for pagination (use value from previous response)")
):
    cloud_r2_creds = CloudflareR2Creds()
    r2_client = cloud_r2_creds.get_client()
    bucket = os.getenv("BUCKET_NAME")

    prefix = f"{folder_path}/"

    try:
        # Enforce max page size (S3/R2 supports up to 1000)
        page_size = min(page_size, 1000)

        if continuation_token:
            response = r2_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                MaxKeys=page_size,
                ContinuationToken=continuation_token
            )
        else:
            response = r2_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                MaxKeys=page_size
            )

        files = []
        if "Contents" in response:
            files = [obj["Key"] for obj in response["Contents"]]

        return {
            "namespace": namespace,
            "path name": folder_path,
            "returned_files": len(files),
            "files": files,
            "is_truncated": response.get("IsTruncated", False),
            "next_token": response.get("NextContinuationToken")  # 🔑 use this for next page
        }

    except Exception as e:
        return {"error": str(e)}

@app.get("/Transaction/bucket/object-total-count")
def get_bucket_total_count(
    namespace: str = Query(..., description="Namespace (e.g. 'Namespace')"),
    folder_path: str = Query(..., description="Folder Path (e.g. 'Folder Path')")
):
    cloud_r2_creds = CloudflareR2Creds()
    r2_client = cloud_r2_creds.get_client()
    bucket = os.getenv("BUCKET_NAME")

    prefix = f"{folder_path}/"

    try:
        total_count = 0

        paginator = r2_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if "Contents" in page:
                total_count += len(page["Contents"])

        return {
            "namespace": namespace,
            "total_files": total_count
        }

    except Exception as e:
        return {"error": str(e)}