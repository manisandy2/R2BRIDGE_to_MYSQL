from fastapi import APIRouter, HTTPException
import duckdb
from pyarrow.dataset import partitioning
from fastapi.encoders import jsonable_encoder
from ...mysql_creds import *
from pyiceberg.schema import Schema
from pyiceberg.types import *
from botocore.client import Config
import botocore
import boto3
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform,YearTransform,MonthTransform,DayTransform,BucketTransform,VoidTransform
from pyiceberg.catalog import NoSuchNamespaceError,NamespaceAlreadyExistsError,TableAlreadyExistsError,NoSuchTableError
from ...core.catalog_client import get_catalog_client
import traceback
import pyarrow as pa
from datetime import datetime, date
from fastapi import APIRouter,HTTPException,Query
from pyiceberg.expressions import And, EqualTo

import time

from concurrent.futures import ThreadPoolExecutor, as_completed

router = APIRouter(prefix="", tags=["filters"])

@router.get("/filters/get")
def filter_customer_phone(
    namespace: str = Query("pos_transactions01", description="Iceberg namespace name"),
    table_name: str = Query("transaction01", description="Iceberg table name"),
    customer_mobile: str | None = Query(None, description="Filter by customer_mobile__c")
):
    import datetime
    """
    Inspect an existing Iceberg table's metadata.
    Optionally filter by partition values (bill_date, store_code, customer_mobile).
    Adds a timeline field to measure total execution time.
    """
    start_time = time.perf_counter()  # Start timeline measurement

    table_identifier = f"{namespace}.{table_name}"
    catalog = get_catalog_client()

    # --- Load the table ---
    try:
        tbl = catalog.load_table(table_identifier)
    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading table: {str(e)}")

    # --- Build filter expressions dynamically ---
    expr = None
    if customer_mobile:
        try:
            cond = EqualTo("customer_mobile__c", int(customer_mobile))
        except:
            raise HTTPException(status_code=400, detail=f"Invalid filter value: {str(e)}")
        expr = cond
    # --- Perform scan ---
    try:
        scan = tbl.scan(row_filter=expr) if expr else tbl.scan()
        df = scan.to_arrow().to_pandas()
        # df = arrow_table.to_pandas().reset_index(drop=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading data: {str(e)}")

    timeline = round(time.perf_counter() - start_time, 3)  # seconds (rounded to 3 decimals)

    # --- Construct response ---
    return {
        "namespace": namespace,
        "table_name": table_name,
        "customer_mobile": customer_mobile,
        "count": len(df),
        "sample_rows": df.head(10).to_dict(orient="records"),
        "timeline_seconds": timeline
    }

@router.get("/filters/exact-date")
def filter_exact_date(
    namespace: str = Query("pos_transactions01", description="Iceberg namespace name"),
    table_name: str = Query("transaction01", description="Iceberg table name"),
    bill_date: str | None = Query(None, description="Filter by Bill_Date__c (YYYY-MM-DD)"),
    customer_mobile: int | None = Query(None, description="Filter by customer_mobile__c")
):
    import datetime
    """
    Inspect an existing Iceberg table's metadata.
    Optionally filter by partition values (bill_date, store_code, customer_mobile).
    Adds a timeline field to measure total execution time.
    """
    start_time = time.perf_counter()  # Start timeline measurement

    table_identifier = f"{namespace}.{table_name}"
    catalog = get_catalog_client()

    # --- Load the table ---
    try:
        tbl = catalog.load_table(table_identifier)
    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading table: {str(e)}")

    # --- Build filter expressions dynamically ---
    expr = None
    try:
        if bill_date:
            bill_date_parsed = datetime.datetime.strptime(bill_date, "%Y-%m-%d").date()
            expr = EqualTo("Bill_Date__c", bill_date_parsed)

        if customer_mobile:
            cond = EqualTo("customer_mobile__c", int(customer_mobile))
            expr = cond if expr is None else And(expr, cond)

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid filter value: {str(e)}")

    # --- Perform scan ---
    try:
        scan = tbl.scan(row_filter=expr) if expr else tbl.scan()
        arrow_table = scan.to_arrow()
        df = arrow_table.to_pandas().reset_index(drop=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading data: {str(e)}")

    end_time = time.perf_counter()  # End timeline measurement
    total_time = round(end_time - start_time, 3)  # seconds (rounded to 3 decimals)

    # --- Construct response ---
    return {
        "namespace": namespace,
        "table_name": table_name,
        "filter_applied": {
            "bill_date": bill_date,
            "customer_mobile": customer_mobile
        },
        "count": len(df),
        "sample_rows": df.head(10).to_dict(orient="records"),
        "timeline_seconds": total_time
    }

@router.get("/filters/date-range")
def filter_between_date_range(
    namespace: str = Query("pos_transactions"),
    table_name: str = Query("iceberg_with_partitioning"),
    start_date: str = Query(..., description="YYYY-MM-DD"),
    end_date: str = Query(..., description="YYYY-MM-DD"),
    phone: str | None = Query(None, description="Filter by customer_mobile__c")
):
    from pyiceberg.expressions import And, GreaterThanOrEqual, LessThanOrEqual, EqualTo
    import datetime
    start = time.perf_counter()

    # validate dates
    try:
        d1 = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
        d2 = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
    except:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

    table_identifier = f"{namespace}.{table_name}"
    catalog = get_catalog_client()

    try:
        tbl = catalog.load_table(table_identifier)
    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")

    # base expr = date range
    expr = And(
        GreaterThanOrEqual("Bill_Date__c", d1),
        LessThanOrEqual("Bill_Date__c", d2),
    )

    # add phone filter if present
    if phone:
        try:
            phone_int = int(phone)
        except:
            raise HTTPException(status_code=400, detail="phone must be integer digits")
        expr = And(expr, EqualTo("customer_mobile__c", phone_int))

    # scan / read data
    try:
        df = tbl.scan(row_filter=expr).to_arrow().to_pandas().reset_index(drop=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading data: {str(e)}")

    return {
        "namespace": namespace,
        "table_name": table_name,
        "start_date": start_date,
        "end_date": end_date,
        "phone": phone,
        "count": len(df),
        "sample_rows": df.head(10).to_dict(orient="records"),
        "timeline_seconds": round(time.perf_counter() - start, 3)
    }

@router.get("/filters/pri_id")
def filter_id(
    namespace: str = Query("pos_transactions"),
    table_name: str = Query("iceberg_with_partitioning"),
    pri_id: str = Query(default=None),
    # phone: str = Query(default=None),
):
    from pyiceberg.expressions import And, GreaterThanOrEqual, LessThanOrEqual

    start_time = time.perf_counter()
    table_identifier = f"{namespace}.{table_name}"
    catalog = get_catalog_client()

    try:
        tbl = catalog.load_table(table_identifier)
    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")

    if pri_id is None:
        raise HTTPException(status_code=400, detail="pri_id is required")

    try:
        pri_id_value = int(pri_id)
    except:
        raise HTTPException(status_code=400, detail="pri_id must be integer")

    expr = EqualTo("pri_id", pri_id_value)

    try:
        df = tbl.scan(row_filter=expr).to_arrow().to_pandas().reset_index(drop=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading data: {str(e)}")

    total_time = round(time.perf_counter() - start_time, 3)

    return {
        "namespace": namespace,
        "table_name": table_name,
        "count": len(df),
        "sample_rows": df.head(10).to_dict(orient="records"),
        "timeline_seconds": total_time
    }

# @router.get("/filters/get-multi")
# def filter_customer_phone_multi(
#     namespaces: list[str] = Query(["pos_transactions01", "pos_transactions02", "pos_transactions03", "pos_transactions04"], description="List of Iceberg namespaces"),
#     table_names: list[str] = Query(["transaction01", "transaction02", "transaction03", "transaction04"], description="List of Iceberg table names (same order as namespaces)"),
#     customer_mobile: str | None = Query(None, description="Filter by customer_mobile__c"),
# ):
#     """
#     Fetch records from multiple Iceberg tables (across namespaces)
#     using a single filter condition (e.g., customer_mobile__c).
#     Returns individual table metrics + total execution summary.
#     """
#
#     import time
#     import pandas as pd
#     from pyiceberg.expressions import EqualTo
#     from fastapi import HTTPException
#
#     catalog = get_catalog_client()
#     all_results = []
#     total_start = time.perf_counter()
#
#     # --- Iterate over all namespace-table pairs ---
#     for idx, (ns, tbl_name) in enumerate(zip(namespaces, table_names)):
#         table_identifier = f"{ns}.{tbl_name}"
#         start_time = time.perf_counter()
#
#         try:
#             tbl = catalog.load_table(table_identifier)
#         except Exception as e:
#             all_results.append({
#                 "namespace": ns,
#                 "table_name": tbl_name,
#                 "error": str(e),
#                 "count": 0,
#                 "timeline_seconds": 0
#             })
#             continue
#
#         # Build filter condition
#         expr = None
#         if customer_mobile:
#             try:
#                 expr = EqualTo("customer_mobile__c", int(customer_mobile))
#             except Exception as e:
#                 raise HTTPException(status_code=400, detail=f"Invalid filter value: {str(e)}")
#
#         # Perform scan
#         try:
#             scan = tbl.scan(row_filter=expr) if expr else tbl.scan()
#             df = scan.to_arrow().to_pandas()
#         except Exception as e:
#             all_results.append({
#                 "namespace": ns,
#                 "table_name": tbl_name,
#                 "error": f"Error reading table: {str(e)}",
#                 "count": 0,
#                 "timeline_seconds": 0
#             })
#             continue
#
#         # Measure timing
#         timeline = round(time.perf_counter() - start_time, 3)
#
#         all_results.append({
#             "namespace": ns,
#             "table_name": tbl_name,
#             "record_count": len(df),
#             "timeline_seconds": timeline,
#             "sample_rows": df.head(3).to_dict(orient="records"),
#         })
#
#     total_time = round(time.perf_counter() - total_start, 3)
#
#     # Combine results
#     summary = {
#         "total_namespaces": len(namespaces),
#         "total_tables": len(table_names),
#         "total_execution_time": total_time,
#         "details": all_results
#     }
#
#     return summary

def process_table(namespace: str, table_name: str, customer_mobile: str | None):
    """Worker function for each table query (runs in parallel threads)."""
    start_time = time.perf_counter()
    catalog = get_catalog_client()
    table_identifier = f"{namespace}.{table_name}"

    result = {
        "namespace": namespace,
        "table_name": table_name,
        "record_count": 0,
        "timeline_seconds": 0,
        "sample_rows": [],
        "error": None
    }

    try:
        tbl = catalog.load_table(table_identifier)
    except NoSuchTableError:
        result["error"] = f"Table not found: {table_identifier}"
        return result
    except Exception as e:
        result["error"] = f"Error loading table: {str(e)}"
        return result

    # 🔸 Build filter condition
    expr = None
    if customer_mobile:
        try:
            expr = EqualTo("customer_mobile__c", int(customer_mobile))
        except Exception as e:
            result["error"] = f"Invalid filter value: {str(e)}"
            return result

    # 🔸 Perform table scan
    try:
        scan = tbl.scan(row_filter=expr) if expr else tbl.scan()
        df = scan.to_arrow().to_pandas()
        result["record_count"] = len(df)
        result["sample_rows"] = df.head(3).to_dict(orient="records")
    except Exception as e:
        result["error"] = f"Error reading table: {str(e)}"
        return result

    result["timeline_seconds"] = round(time.perf_counter() - start_time, 3)
    return result

@router.get("/filters/get-multi")
def filter_customer_phone_multi(
    namespaces: list[str] = Query(["pos_transactions01", "pos_transactions02", "pos_transactions03", "pos_transactions04"], description="List of Iceberg namespaces"),
    table_names: list[str] = Query(["transaction01", "transaction02", "transaction03", "transaction04"], description="List of Iceberg table names (same order as namespaces)"),
    customer_mobile: str | None = Query(None, description="Filter by customer_mobile__c"),
    max_threads: int = Query(4, description="Maximum number of parallel threads"),
):
    """
    Multithreaded filter across multiple Iceberg namespaces and tables.
    Executes all queries concurrently and returns per-table metrics.
    """

    total_start = time.perf_counter()
    all_results = []

    # --- Use ThreadPoolExecutor for parallel querying ---
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = [
            executor.submit(process_table, ns, tbl_name, customer_mobile)
            for ns, tbl_name in zip(namespaces, table_names)
        ]

        # Collect completed results
        for future in as_completed(futures):
            all_results.append(future.result())

    total_time = round(time.perf_counter() - total_start, 3)

    return {
        "total_namespaces": len(namespaces),
        "total_tables": len(table_names),
        "thread_count": max_threads,
        "total_execution_time": total_time,
        "details": all_results
    }