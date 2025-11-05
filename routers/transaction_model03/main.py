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
from .routers.transaction_model02 import cum_ph_04 as phone_invoice
from .routers.transaction_model02 import schemas as schemas_schema
from .routers.transaction_model02 import duckdb_cum_ph as duckdb_ph
from .routers.transaction_model02 import Inspecting_tables as Inspecting_tables
from .routers.transaction_model02 import partition as partition_schema


logger = logging.getLogger(__name__)



app = FastAPI()

app.include_router(transaction_namespace.router)

app.include_router(transaction_bds.router)
app.include_router(transaction_table.router)
app.include_router(transaction_meta_data.router)
app.include_router(phone_invoice.router)
app.include_router(schemas_schema.router)
app.include_router(duckdb_ph.router)
app.include_router(Inspecting_tables.router)
app.include_router(partition_schema.router)


@app.get("/")
def root():
    tables_name = ["Transaction", ]

    return {"message": "API is running",
            "version": "1.0",
            "Tables": tables_name
            }




