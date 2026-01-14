from fastapi import APIRouter,Query,HTTPException
from ..core.catalog_client import get_catalog_client
import pyarrow.parquet as pq
from typing import Optional

router = APIRouter(prefix="/partition", tags=["partition"])
import pandas as pd
@router.get("/filter")
def get_partition(
    namespace: str = Query(..., description="Namespace (e.g. 'Namespace')"),
    table_name: str = Query(..., description="Table name (e.g. 'Table name')")
):

    catalog = get_catalog_client()
    table = catalog.load_table(f"{namespace}.{table_name}")

    # Partition spec
    # print("Partition Spec:", table.spec())
    # print("Table Spec:", table.schema())
    # print()
    # Partition summary
    # for p in table.scan().plan_files():
    #     print(p.partition)
    # return table.scan().plan_files()
    # return table.schema()
    # return table.spec()
    for task in table.scan().plan_files():
        print(task.file)
        # df = pd.read_parquet(task.file.file_path)
        # print(df.info())
        # print("path:",task.file.file_path)
        # print("record:",task.file.record_count)
        # print("partition:",task.file.partition)

    # return table.scan().plan_files()
    # Partition spec
    partition_spec = table.spec()  # convert to JSON-serializable
    table_schema = table.schema()
    # print("par",partition_spec)
    # print("tab",table_schema)
    # Summary of partitions
    partitions_summary = []
    file_paths = []
    for task in table.scan().plan_files():
        partition = task.file.partition  # dict of partition values

        file_path = task.file.file_path
        file_paths.append(file_path)
        # print(f"Partition: {task.file.file_path}")
        record_count = task.file.record_count
        partitions_summary.append({
            "file_path": file_path,
            "partition": partition,
            "record_count": record_count
        })

    # file_path = partitions_summary['file_path']
    # print(file_path)
    # table_arrow = pq.read_table(file_path)
    # df = table_arrow.to_pandas()
    # print(df)


    return {
        # "file_path": file_paths,
        "table_schema": table_schema,
        "partition_spec": partition_spec,
        # "partitions": partitions_summary[0],

        "partitions_file_path": partitions_summary,
    }

@router.get("/filter_schema")
def get_schema(
    namespace: str = Query(..., description="Namespace (e.g. 'Namespace')"),
    table_name: str = Query(..., description="Table name (e.g. 'Table name')")
):

    catalog = get_catalog_client()
    table = catalog.load_table(f"{namespace}.{table_name}")

    # Partition spec
    # print("Partition Spec:", table.spec())
    # print("Table Spec:", table.schema())

    # Partition summary
    # for p in table.scan().plan_files():
    #     print(p.partition)
    # return table.scan().plan_files()
    # return table.schema()
    # return table.spec()
    # for task in table.scan().plan_files():
    #     df = pd.read_parquet(task.file.file_path)
    #     print(df.info())
        # print("path:",task.file.file_path)
        # print("record:",task.file.record_count)
        # print("partition:",task.file.partition)

    # return table.scan().plan_files()
    # Partition spec
    partition_spec = table.spec()  # convert to JSON-serializable
    table_schema = table.schema()



    return {
        "partition_spec": partition_spec,
        "table_schema": table_schema,

    }

from pydantic import BaseModel
from pyiceberg.types import (
    IntegerType,
    StringType,
    DoubleType,
    BooleanType,
    TimestampType,
)

class ColumnAddRequest(BaseModel):
    table_name: str
    column_name: str
    column_type: str
    doc: str = "Added via FastAPI"

def get_iceberg_type(type_str: str):
    type_str = type_str.lower()
    if type_str in ["int", "integer"]:
        return IntegerType()
    elif type_str in ["string", "str"]:
        return StringType()
    elif type_str in ["double", "float"]:
        return DoubleType()
    elif type_str in ["bool", "boolean"]:
        return BooleanType()
    elif type_str in ["timestamp", "datetime"]:
        return TimestampType()
    else:
        raise ValueError(f"Unsupported Iceberg type: {type_str}")

@router.post("/update_schema")
def update_schema(

        column_name:str = Query(None, description="Column name"),
        column_type: str = Query(None, description="Column type"),
        namespace: str = Query(..., description="Namespace (e.g. 'Namespace')"),
        table_name: str = Query(..., description="Table name (e.g. 'Table name')")
                  ):
    try:
        catalog = get_catalog_client()
        table = catalog.load_table(f"{namespace}.{table_name}")

        iceberg_type = get_iceberg_type(column_type)

        with table.update_schema() as update:
            update.add_column(column_name, iceberg_type, f"Added {column_name} via API")

        # reload updated table schema
        table = catalog.load_table(f"{namespace}.{table_name}")

        return {
            "status": "success",
            "message": f"Column '{column_name}' of type '{column_type}' added successfully.",
            "schema": [f"{f.name}: {f.field_type}" for f in table.schema().fields]
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Schema update failed: {e}")

    # catalog = get_catalog_client()
    # table = catalog.load_table(f"{namespace}.{table_name}")
    # print(table.update_schema(column_name,column_type))
    # return table.schema()
    # try:
        # table = catalog.load_table(request.table_name)
        # iceberg_type = get_iceberg_type(request.column_type)
        #
        # with table.update_schema() as update:
        #     update.add_column(request.column_name, iceberg_type, request.doc)
        #
        # return {"status": "success", "message": f"Column '{request.column_name}' added successfully"}
    # except Exception as e:
    #     # logging.error(f"Schema update failed: {e}")
    #     raise HTTPException(status_code=500, detail=str(e))

# @router.put("/update_column")
# def update_column(
#     column_name: str = Query(..., description="Column name to update"),
#     column_type: Optional[str] = Query(None, description="New column type (optional)"),
#     doc: Optional[str] = Query(None, description="New column description (optional)"),
#     namespace: str = Query(..., description="Namespace (e.g. 'Namespace')"),
#     table_name: str = Query(..., description="Table name (e.g. 'Table name')")
# ):
#     try:
#         catalog = get_catalog_client()
#         table = catalog.load_table(f"{namespace}.{table_name}")
#
#         with table.update_schema() as update:
#             updated = False
#
#             if column_type:
#                 iceberg_type = get_iceberg_type(column_type)
#                 update.update_column_type(column_name, iceberg_type)
#                 updated = True
#
#             if doc:
#                 update.update_column_doc(column_name, doc)
#                 updated = True
#
#             if not updated:
#                 raise HTTPException(status_code=400, detail="No updates provided. Specify column_type or doc.")
#
#         # Reload schema after update
#         table = catalog.load_table(f"{namespace}.{table_name}")
#         updated_field = next((f for f in table.schema().fields if f.name == column_name), None)
#
#         return {
#             "status": "success",
#             "message": f"Column '{column_name}' updated successfully.",
#             "updated_column": {
#                 "name": updated_field.name,
#                 "type": str(updated_field.field_type),
#                 "doc": updated_field.doc,
#             } if updated_field else None
#         }
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Update column failed: {e}")

@router.delete("/")
def delete_column(
        column_name: str = Query(..., description="Column name to delete"),
        namespace: str = Query(..., description="Namespace (e.g. 'Namespace')"),
        table_name: str = Query(..., description="Table name (e.g. 'Table name')")
):
    try:
        catalog = get_catalog_client()
        table = catalog.load_table(f"{namespace}.{table_name}")

        with table.update_schema() as update:
            update.delete_column(column_name)

        return {"status": "success", "message": f"Column '{column_name}' deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delete column failed: {e}")

import logging

logger = logging.getLogger(__name__)

# @router.get("/copy_tables")
# def copy_tables(
#         namespace: str = Query(..., description="Namespace (e.g. 'Namespace')"),
#         source_table_name: str = Query(..., description="Table name (e.g. 'Table name')"),
#         destination_table_name: str = Query(..., description="Table name (e.g. 'Table name')")
#     ):
#     try:
#         catalog = get_catalog_client()
#
#         # --- Load the source table ---
#         source_table = catalog.load_table(f"{namespace}.{source_table_name}")
#         logger.info(f"Loaded source table: {namespace}.{source_table_name}")
#
#         # destination_table_name = catalog.load_table(f"{namespace}.{destination_table_name}")
#
#         # --- Create the destination table with the same schema ---
#         catalog.create_table(
#             identifier=f"{namespace}.{destination_table_name}",
#             schema=source_table.schema(),
#             partition_spec=source_table.spec(),
#             properties=source_table.properties
#         )
#         logger.info(f"Created destination table: {namespace}.{destination_table_name}")
#
#         # Write data to new table
#         destination_table = catalog.load_table(f"{namespace}.{destination_table_name}")
#
#         # --- Copy data files from source to destination ---
#         planned_files = source_table.new_scan().plan_files()
#
#         for file in planned_files:
#             destination_table.append_files(file.data_file)
#         destination_table.commit()
#         logger.info(f"Copied data from {source_table_name} to {destination_table_name}")
#
#         return {
#             "status": "success",
#             "message": f"Table '{source_table_name}' copied successfully.",
#
#         }
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Copy table failed: {e}")



@router.get("/copy_tables")
def copy_tables(
    namespace: str = Query(..., description="Namespace (e.g. 'Namespace')"),
    source_table_name: str = Query(..., description="Source Table name"),
    destination_table_name: str = Query(..., description="Destination Table name")
):
    try:
        catalog = get_catalog_client()

        # --- Load source table ---
        source_table = catalog.load_table(f"{namespace}.{source_table_name}")
        logger.info(f"Loaded source table: {namespace}.{source_table_name}")

        # --- Get metadata location ---
        metadata_location = source_table.metadata_location

        # --- Register a new table using the same metadata ---
        catalog.register_table(
            identifier=f"{namespace}.{destination_table_name}",
            metadata_location=metadata_location
        )

        logger.info(f"Copied table '{source_table_name}' to '{destination_table_name}' using metadata location.")
        return {
            "status": "success",
            "message": f"Table '{source_table_name}' copied successfully as '{destination_table_name}'",
            "copy_type": "metadata-only"
        }

    except Exception as e:
        logger.error(f"Copy table failed: {e}")
        raise HTTPException(status_code=500, detail=f"Copy table failed: {str(e)}")

import pandas as pd
import time
@router.get("/filter-parquet")
def get_parquet_files(
    namespace: str = Query(..., description="Namespace (e.g. 'transactions')"),
    table_name: str = Query(..., description="Table name (e.g. 'sales')")
):
    """
    Returns only Parquet file paths from the Iceberg table.
    """
    try:
        catalog = get_catalog_client()
        table = catalog.load_table(f"{namespace}.{table_name}")

        parquet_files = []
        # parquet_files = []
        for task in table.scan().plan_files():
            parquet_files.append(task.file.file_path)

        # Iterate over planned scan tasks
        # for task in table.scan().plan_files():
        #     print(task.file.file_path)
        #     print(pd.read_parquet(task.file.file_path).head(5))
        #     time.sleep(1)
        #     print(task.file.file_path)
            # parquet_files.append(task.file.file_path)

        return {
            "count": len(parquet_files),
            "parquet_files": parquet_files,

        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching parquet files: {str(e)}")