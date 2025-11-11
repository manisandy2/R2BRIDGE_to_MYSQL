from fastapi import FastAPI,Query,HTTPException
# from packaging.metadata import Metadata
from botocore.exceptions import ClientError
import time, json, boto3, os

from .core import r2_client
# from packaging.version import Version
# from mysql_catalog import MysqlCatalog
from .mysql_creds import  MysqlCatalog
from pyiceberg.exceptions import NoSuchNamespaceError,NamespaceAlreadyExistsError,TableAlreadyExistsError
# from creds import Creds
from .creds import Creds
from pydantic import BaseModel
from pyiceberg.exceptions import NoSuchTableError
from .mapping import *
from pyiceberg.schema import Schema, NestedField
# from .creds import get_r2_client
from .core.r2_client import get_r2_client
from .core.catalog_client import get_catalog_client
import json
import time
import os
from fastapi import FastAPI, Query,Body, HTTPException,UploadFile, File
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import GreaterThanOrEqual,EqualTo
from decimal import Decimal
# from routers import namespace.router
from typing import List
import json
import decimal
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
from mysql.connector import Error
import pandas as pd
import logging
from .routers import (bucket, namespace, objects_folder,
                      json_data_store, get_data, serial_data,
                      database_to_transaction, crm_application,
                      partition, schemas, columns, filter,bucket_data_store
                      )
from .routers.transaction import bucket as transaction_bucket
from .routers.transaction_model02 import namespace as transaction_namespace
from .routers.transaction_model02 import bucket_data_store01 as transaction_bds
from .routers.transaction_model02 import database_to_transaction as transaction_database
from .routers.transaction_model02 import table as transaction_table
from .routers.transaction_model02 import meta_data as transaction_meta_data
from .routers.transaction_model02 import cum_ph_06 as data_insert
from .routers.transaction_model02 import filters as filters
from .routers.transaction_model02 import parquet as parquet_table
from .routers.transaction_model02 import avro as avro_files

from .routers.transaction_model02 import schema as schema_schema
from .routers.transaction_model02 import duckdb_cum_ph as duckdb_ph
from .routers.transaction_model02 import Inspecting_tables as Inspecting_tables
from .routers.transaction_model02 import partition as partition_schema
from .routers.duckdb import r2_catalog_create_table

logger = logging.getLogger(__name__)

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
# app.include_router(bucket.router)
# app.include_router(transaction_bucket.router)
app.include_router(transaction_namespace.router)
# app.include_router(transaction_database.router)
app.include_router(transaction_bds.router)
app.include_router(transaction_table.router)
app.include_router(transaction_meta_data.router)
app.include_router(data_insert.router)
app.include_router(filters.router)
app.include_router(parquet_table.router)
app.include_router(avro_files.router)
app.include_router(schema_schema.router)
# app.include_router(duckdb_ph.router)
app.include_router(Inspecting_tables.router)
app.include_router(partition_schema.router)
app.include_router(r2_catalog_create_table.router)


# app.include_router(namespace.router)
# app.include_router(database_to_transaction.router)
# app.include_router(objects_folder.router)
# app.include_router(json_data_store.router)
# app.include_router(get_data.router)
# app.include_router(serial_data.router)
# app.include_router(crm_application.router)
# app.include_router(partition.router)
# app.include_router(schemas.router)
# app.include_router(columns.router)
# app.include_router(filter.router)
# app.include_router(bucket_data_store.router)

ALLOWED_TABLES = ["Transaction",]

@app.get("/")
def root():
    tables_name = ["Transaction", ]

    return {"message": "API is running",
            "version": "1.0",
            "Tables": tables_name
            }



@app.get("/iceberg/table/count")
def iceberg_table_count(
    name_space: str = Query(...),
    table_name: str = Query(...)
):
    table_identifier = f"{name_space}.{table_name}"
    catalog = get_catalog_client()

    try:
        tbl = catalog.load_table(table_identifier)
        print("data",tbl)
        row_count = tbl.scan().count()
        return {"table": table_identifier, "count": row_count}

    except NoSuchTableError:
        raise HTTPException(404, f"Table not found: {table_identifier}")
    except Exception as e:
        raise HTTPException(500, f"Error counting rows: {str(e)}")


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

