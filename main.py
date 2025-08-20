from fastapi import FastAPI,Query,HTTPException
# from botocore.exceptions import ClientError
from .core import r2_client
from .mysql_creds import  MysqlCatalog
from .creds import Creds
from pydantic import BaseModel
from pyiceberg.exceptions import NoSuchTableError
from .mapping import *
from pyiceberg.schema import Schema, NestedField
from .core.r2_client import get_r2_client
import json

from fastapi import FastAPI, Query,Body, HTTPException,UploadFile, File
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import GreaterThanOrEqual,EqualTo
from decimal import Decimal

from typing import List
import json
import decimal
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
from mysql.connector import Error
import pandas as pd
import logging
from .routers import bucket,namespace,objects_folder,json_data_store

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
app.include_router(bucket.router)
app.include_router(namespace.router)
app.include_router(objects_folder.router)
app.include_router(json_data_store.router)

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





# def convert_row(row, column_types):
#     """Convert MySQL row values to types PyArrow accepts."""
#     converted = []
#     for value, col_type in zip(row, column_types):
#         if col_type.startswith("decimal") and value is not None:
#             # Always convert to string to keep precision and satisfy PyArrow
#             converted.append(str(value))
#         else:
#             converted.append(value)
#     return converted



# def normalize_mysql_type(t):
#     return re.sub(r"\(.*\)", "", t).strip().lower()

