
from fastapi import APIRouter,HTTPException,Query,Body
import time
from ..core.catalog_client import get_catalog_client
from ..mapping import *
from pyiceberg.exceptions import NoSuchTableError, RESTError, NamespaceAlreadyExistsError
from typing import List
import json
import pyarrow as pa
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform


router = APIRouter(prefix="/crm", tags=["CRM Application"])



# @router.post("/InsertOne")
# def insert_one(
#     namespace: str = Query(..., description="Namespace (e.g. 'transactions')"),
#     table_name: str = Query(..., description="Table (e.g. 'pos')"),
#     record: dict = Body(..., description="Single JSON record to insert into Iceberg")
# ):
#     start_time = time.time()
#
#     if not record:
#         return {"status": "failed", "message": "No records provided"}
#
#     try:
#         iceberg_schema, arrow_schema = infer_schema_from_record(record)
#
#         converted_record = convert_row(record,arrow_schema)
#         arrow_table = pa.Table.from_pylist([converted_record], schema=arrow_schema)
#
#         catalog = get_catalog_client()
#         table_identifier = f"{namespace}.{table_name}"
#
#         tbl = get_or_create_table(catalog, table_identifier, iceberg_schema)
#
#         tbl.append(arrow_table)
#         elapsed = time.time() - start_time
#
#
#
#         return {
#             "status": "success",
#             "namespace": namespace,
#             "table": table_name,
#             "rows_written": 1,
#             "elapsed_seconds": round(elapsed, 2)
#         }
#     except RESTError as e:
#         raise HTTPException(status_code=500, detail=f"Iceberg REST API error: {str(e)}")
#
#     except NamespaceAlreadyExistsError:
#         raise HTTPException(status_code=409, detail=f"Namespace '{namespace}' already exists")
#
#     except NoSuchTableError:
#         raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found in namespace '{namespace}'")
#
#     except pa.ArrowInvalid as e:
#         raise HTTPException(status_code=400, detail=f"Arrow schema error: {str(e)}")
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")



# @router.post("/InsertOne")
# def insert_one(
#     namespace: str = Query(..., description="Namespace (e.g. 'transactions')"),
#     table_name: str = Query(..., description="Table (e.g. 'pos')"),
#     record: dict = Body(..., description="Single JSON record to insert into Iceberg"),
# ):
#     start_time = time.time()
#
#     if not record:
#         return {"status": "failed", "message": "No records provided"}
#
#     try:
#         # ✅ Add raw JSON column
#         record_with_raw = {**record, "raw_json": json.dumps(record)}
#
#         # ✅ Make sure create_date exists
#         if "ticketId" not in record_with_raw:
#             raise HTTPException(status_code=400, detail="Missing required field 'create_date' for partitioning")
#
#         # ✅ Infer schema
#         iceberg_schema, arrow_schema = infer_schema_from_record(record_with_raw)
#
#         # ✅ Convert into Arrow table
#         converted_record = convert_row(record_with_raw, arrow_schema)
#         arrow_table = pa.Table.from_pylist([converted_record], schema=arrow_schema)
#
#         catalog = get_catalog_client()
#         table_identifier = f"{namespace}.{table_name}"
#
#         # ✅ PartitionSpec: always partition by create_date
#         partition_spec = PartitionSpec()
#         if "create_date" in iceberg_schema.names:
#             partition_spec = partition_spec.identity(f"{table_name}create_date")
#
#         # ✅ Create or get table
#         tbl = get_or_create_table(catalog, table_identifier, iceberg_schema, partition_spec)
#
#         # ✅ Append data
#         tbl.append(arrow_table)
#
#         elapsed = time.time() - start_time
#         return {
#             "status": "success",
#             "namespace": namespace,
#             "table": table_name,
#             "rows_written": 1,
#             "partition_by": "create_date",
#             "elapsed_seconds": round(elapsed, 2)
#         }
#
#     except RESTError as e:
#         raise HTTPException(status_code=500, detail=f"Iceberg REST API error: {str(e)}")
#
#     except NamespaceAlreadyExistsError:
#         raise HTTPException(status_code=409, detail=f"Namespace '{namespace}' already exists")
#
#     except NoSuchTableError:
#         raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found in namespace '{namespace}'")
#
#     except pa.ArrowInvalid as e:
#         raise HTTPException(status_code=400, detail=f"Arrow schema error: {str(e)}")
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchNamespaceError, NoSuchTableError, RESTError
from ..core.r2_client import get_r2_client
import uuid
from ..mapping import *

import json
import time
import uuid
from datetime import datetime
from fastapi import APIRouter, Query, Body, HTTPException

from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError, NamespaceAlreadyExistsError, RESTError



@router.post("/InsertOne")
def insert_one(
    namespace: str = Query(..., description="Namespace (e.g. 'transactions')"),
    table_name: str = Query(..., description="Table (e.g. 'pos')"),
    record: dict = Body(..., description="Single JSON record to insert into Iceberg"),
):
    start_time = time.time()

    if not record:
        raise HTTPException(status_code=400, detail="No records provided")

    # ------------------------
    # Step 1: Save JSON to R2
    # ------------------------
    try:
        r2_client = get_r2_client()
        ticket_id = record.get("ticketId", str(uuid.uuid4()))
        record["ticketId"] = ticket_id

        created_at = record.get("createdAt", str(uuid.uuid4()))
        record["createdAt"] = created_at

        r2_key = f"{namespace}/{table_name}/{created_at[0:10]}/{ticket_id}.json"
        r2_client.put_object(
            Bucket=namespace,
            Key=r2_key,
            Body=json.dumps(record, indent=2, cls=CustomJSONEncoder).encode("utf-8"),
            ContentType="application/json",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to store record in R2: {str(e)}")

    # ------------------------
    # Step 2: Prepare record
    # ------------------------
    try:
        # Add raw JSON string
        record_with_raw = {**record, "raw_json": json.dumps(record, ensure_ascii=False)}

        # Auto-fill required fields
        if "ticketId" not in record_with_raw:
            record_with_raw["ticket_id"] = str(uuid.uuid4())

        if "createdAt" not in record_with_raw:
            record_with_raw["createdAt"] = datetime.utcnow().isoformat()

        # Infer schema
        iceberg_schema, arrow_schema = infer_schema_from_record(record_with_raw)

        # Convert row → Arrow table
        converted_record = convert_row(record_with_raw, arrow_schema)
        arrow_table = pa.Table.from_pylist([converted_record], schema=arrow_schema)

        catalog = get_catalog_client()
        table_identifier = f"{namespace}.{table_name}"

        # Ensure namespace exists
        try:
            catalog.load_namespace_properties(namespace)
        except NoSuchNamespaceError:
            catalog.create_namespace(namespace)

        # Partition spec
        ticket_id_field = iceberg_schema.find_field("ticketId")
        created_at_field = iceberg_schema.find_field("createdAt")

        # partition_spec = PartitionSpec(
        #     fields=[
        #         PartitionField(
        #             source_id=ticket_id_field.field_id,
        #             field_id=1000,
        #             transform=IdentityTransform(),
        #             name="ticketId",
        #         ),
        #         PartitionField(
        #             source_id=created_at_field.field_id,
        #             field_id=1001,
        #             transform=IdentityTransform(),
        #             name="createdAt",
        #         ),
        #     ]
        # )

        # Load or create table
        try:
            tbl = catalog.load_table(table_identifier)
        except NoSuchTableError:
            tbl = catalog.create_table(
                identifier=table_identifier,
                schema=iceberg_schema,
                # partition_spec=partition_spec,
                # properties={
                #     "write.partition.path-style": "directory"
                # },

            )

        # Append record
        tbl.append(arrow_table)

        elapsed = round(time.time() - start_time, 2)

        return {
            "status": "success",
            "namespace": namespace,
            "table": table_name,
            "ticketId": ticket_id,
            "rows_written": 1,
            "partition_by": ["ticket_id", "createdAt"],
            "elapsed_seconds": elapsed,
        }

    except RESTError as e:
        raise HTTPException(status_code=500, detail=f"Iceberg REST API error: {str(e)}")

    except NamespaceAlreadyExistsError:
        raise HTTPException(status_code=409, detail=f"Namespace '{namespace}' already exists")

    except pa.ArrowInvalid as e:
        raise HTTPException(status_code=400, detail=f"Arrow schema error: {str(e)}")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


@router.get("/filter")
def get_data(namespace: str = Query(..., description="Namespace (e.g. 'Namespace')"),
    table_name: str = Query(..., description="Table name (e.g. 'Table name')")
):
    try:
        catalog = get_catalog_client()
        table = catalog.load_table((namespace, table_name))
    except Exception as e:
        # logger.error(f"Failed to load table: {str(e)}")
        raise HTTPException(status_code=500, detail="Error loading table from catalog")

    try:
        scan = table.scan()
        files = [task.file.file_path for task in scan.plan_files()]
    except Exception as e:
        # logger.error(f"Failed to scan table: {str(e)}")
        raise HTTPException(status_code=500, detail="Error scanning table files")

    try:
        snapshots = list(table.snapshots())
        snapshot_data = []
        for s in snapshots:
            ts = getattr(s, "timestamp_ms", None)
            snapshot_data.append({
                "snapshot_id": getattr(s, "snapshot_id", None),
                "parent_snapshot_id": getattr(s, "parent_snapshot_id", None),
                "timestamp_ms": ts,
                "manifest_list": getattr(s, "manifest_list", None),
                "date_time": datetime.fromtimestamp(ts / 1000.0).strftime("%Y-%m-%d %H:%M:%S") if ts else None,
                "summary": getattr(s, "summary", {})
            })
    except Exception as e:
        # logger.error(f"Failed to retrieve snapshots: {str(e)}")
        raise HTTPException(status_code=500, detail="Error retrieving snapshot data")

    return {
        "namespace": namespace,
        "table_name": table.name,
        "records_count": len(snapshots),
        "snapshots": snapshot_data,
        "parquet_files": files
    }


from pyiceberg.expressions import EqualTo
import time

@router.get("/filter-data")
def get_filtered_data(
    namespace: str = Query(..., description="Namespace (e.g. 'CRM_Application')"),
    table_name: str = Query(..., description="Table name (e.g. 'Serial')"),
    columns: list[str] = Query(..., description="Columns to filter on (comma-separated or multiple query params)"),
    values: list[str] = Query(..., description="Values to match (in same order as columns)")
):
    start_time = time.time()

    try:
        if len(columns) != len(values):
            raise HTTPException(status_code=400, detail="Number of columns and values must match")

        catalog = get_catalog_client()
        table = catalog.load_table((namespace, table_name))

        schema_fields = {f.name for f in table.schema().fields}
        for col in columns:
            if col not in schema_fields:
                raise HTTPException(status_code=400, detail=f"Column '{col}' not found in schema")

        filter_expr = None
        for col, val in zip(columns, values):
            expr = EqualTo(col, val)
            filter_expr = expr if filter_expr is None else And (filter_expr, expr)


        # Apply filter
        scan = table.scan(row_filter=filter_expr)
        print(scan.to_arrow())
        rows = []

        arrow_table = scan.to_arrow()

        for batch in arrow_table.to_batches():
            rows.extend(batch.to_pylist())

        elapsed = round(time.time() - start_time, 2)

        # --- Metadata without using _plan() ---
        metadata = {
            "namespace": namespace,
            "table": table_name,
            "filters": dict(zip(columns, values)),
            "snapshot_id": table.current_snapshot().snapshot_id if table.current_snapshot() else None,
            "execution_time_seconds": [round(elapsed, 2),"seconds"]
        }
        return {
            "status": "success",
            "metadata": metadata,
            "data": rows
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error filtering data: {str(e)}")