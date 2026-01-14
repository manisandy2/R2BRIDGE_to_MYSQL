from fastapi import FastAPI, Query, HTTPException,APIRouter

from datetime import datetime
from typing import Optional
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThanOrEqual
from ..core.catalog_client import  get_catalog_client
app = FastAPI()
router = APIRouter(prefix="/filter", tags=["filter"])
# Load catalog (example: REST catalog or file-based)
# 1. Filter by string column
# GET /filter_data?namespace=sales&table_name=transactions&column_name=status&column_value=SUCCESS
# 2. Filter by datetime range
# GET /filter_data?namespace=sales&table_name=transactions&date_column=created_at&start_date=2025-10-01T00:00:00&end_date=2025-10-10T23:59:59
# 3. Combine both
# GET /filter_data?namespace=sales&table_name=transactions&column_name=status&column_value=SUCCESS&date_column=created_at&start_date=2025-10-01T00:00:00

@router.get("/filter_data")
def filter_data(
    namespace: str = Query(..., description="Namespace name"),
    table_name: str = Query(..., description="Table name"),
    column_name: Optional[str] = Query(None, description="Column name to filter"),
    column_value: Optional[str] = Query(None, description="Value to match (string)"),
    start_date: Optional[str] = Query(None, description="Start datetime in ISO format (e.g., 2024-10-01T00:00:00)"),
    end_date: Optional[str] = Query(None, description="End datetime in ISO format (e.g., 2024-10-10T23:59:59)"),
    date_column: Optional[str] = Query(None, description="Datetime column name for filtering")
):
    try:
        catalog = get_catalog_client()
        table = catalog.load_table(f"{namespace}.{table_name}")

        # Build dynamic filters
        filters = []

        if column_name and column_value:
            filters.append(EqualTo(column_name, column_value))

        if start_date or end_date:
            if not date_column:
                raise HTTPException(status_code=400, detail="date_column is required for datetime filtering")

            if start_date:
                try:
                    start_dt = datetime.fromisoformat(start_date)
                    filters.append(GreaterThanOrEqual(date_column, start_dt))
                except ValueError:
                    raise HTTPException(status_code=400, detail="Invalid start_date format. Use ISO format (YYYY-MM-DDTHH:MM:SS)")

            if end_date:
                try:
                    end_dt = datetime.fromisoformat(end_date)
                    filters.append(LessThanOrEqual(date_column, end_dt))
                except ValueError:
                    raise HTTPException(status_code=400, detail="Invalid end_date format. Use ISO format (YYYY-MM-DDTHH:MM:SS)")

        # Combine filters with AND
        expression = None
        for f in filters:
            expression = f if expression is None else And(expression, f)

        # Perform the scan
        scan = table.scan(row_filter=expression) if expression else table.scan()

        df = scan.to_pandas()
        return {"count": len(df), "data": df.to_dict(orient="records")}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Filtering failed: {str(e)}")

# @router.get("/filter_by_date")
# def filter_by_date_range(
#     namespace: str = Query(..., description="Namespace (e.g. 'sales')"),
#     table_name: str = Query(..., description="Table name (e.g. 'transactions')"),
#     date_column: str = Query(..., description="Column name that stores date or datetime"),
#     start_date: str = Query(None, description="Start datetime in ISO format (e.g., 2025-10-01T00:00:00)"),
#     end_date: str = Query(None, description="End datetime in ISO format (e.g., 2025-10-10T23:59:59)")
# ):
#     """
#     Filter Iceberg table data by date range using Iceberg expressions.
#     Returns rows between start_date and end_date.
#     """
#     try:
#         catalog = get_catalog_client()
#         table = catalog.load_table(f"{namespace}.{table_name}")
#
#         # Validate and parse dates
#         filters = []
#         if start_date:
#             try:
#                 start_dt = datetime.fromisoformat(start_date)
#                 filters.append(GreaterThanOrEqual(date_column, start_dt))
#             except ValueError:
#                 raise HTTPException(status_code=400, detail="Invalid start_date format. Use ISO (YYYY-MM-DDTHH:MM:SS)")
#
#         if end_date:
#             try:
#                 end_dt = datetime.fromisoformat(end_date)
#                 filters.append(LessThanOrEqual(date_column, end_dt))
#             except ValueError:
#                 raise HTTPException(status_code=400, detail="Invalid end_date format. Use ISO (YYYY-MM-DDTHH:MM:SS)")
#
#         # Combine filters (if both provided)
#         if not filters:
#             raise HTTPException(status_code=400, detail="At least one of start_date or end_date must be provided")
#
#         date_filter = filters[0]
#         if len(filters) > 1:
#             date_filter = And(filters[0], filters[1])
#
#         # Perform Iceberg scan with filter
#         scan = table.scan(row_filter=date_filter)
#         df = scan.to_pandas()
#
#         return {
#             "status": "success",
#             "namespace": namespace,
#             "table": table_name,
#             "filter": {
#                 "column": date_column,
#                 "start_date": start_date,
#                 "end_date": end_date
#             },
#             "count": len(df),
#             "data": df.to_dict(orient="records")
#         }
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Date range filter failed: {str(e)}")

@router.get("/filter_by_date")
def filter_by_date_range(
    namespace: str = Query(...),
    table_name: str = Query(...),
    date_column: str = Query(...),
    start_date: str = Query(None),
    end_date: str = Query(None)
):
    try:
        catalog = get_catalog_client()
        table = catalog.load_table(f"{namespace}.{table_name}")

        # --- 1️⃣ Check column type from Iceberg schema ---
        schema = table.schema()
        field = schema.find_field(date_column)
        if not field:
            raise HTTPException(status_code=400, detail=f"Column '{date_column}' not found in table schema")

        field_type = str(field.field_type).lower()

        def format_value(value: str):
            # convert string to matching column type
            if "timestamp" in field_type or "timestamptz" in field_type:
                return datetime.fromisoformat(value)
            elif "date" in field_type:
                return datetime.fromisoformat(value).date()
            else:
                # for string-based columns, return raw string
                return value

        # --- 2️⃣ Build filter expressions ---
        filters = []
        if start_date:
            filters.append(GreaterThanOrEqual(date_column, format_value(start_date)))
        if end_date:
            filters.append(LessThanOrEqual(date_column, format_value(end_date)))

        if not filters:
            raise HTTPException(status_code=400, detail="Provide at least start_date or end_date")

        row_filter = filters[0]
        if len(filters) == 2:
            row_filter = And(filters[0], filters[1])

        # --- 3️⃣ Run Iceberg scan with row filter ---
        scan = table.scan(row_filter=row_filter)
        df = scan.to_pandas()

        return {
            "status": "success",
            "filter": {
                "column": date_column,
                "start_date": start_date,
                "end_date": end_date,
                "column_type": field_type
            },
            "count": len(df),
            # "data": df.to_dict(orient="records")
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Date range filter failed: {e}")


@router.get("/column_stats")
def column_stats(
    namespace: str = Query(..., description="Namespace (e.g. 'sales')"),
    table_name: str = Query(..., description="Table name (e.g. 'transactions')"),
    column_name: str = Query(..., description="Column name to analyze (e.g. 'pri_id')")
):
    """
    Returns total row count and NaN/null count for a specific column in an Iceberg table.
    """
    try:
        catalog = get_catalog_client()
        table = catalog.load_table(f"{namespace}.{table_name}")

        # Load the table data into a pandas DataFrame
        scan = table.scan()
        df = scan.to_pandas()

        if column_name not in df.columns:
            raise HTTPException(status_code=404, detail=f"Column '{column_name}' not found in table '{table_name}'")

        total_rows = len(df)
        nan_count = df[column_name].isna().sum()

        return {
            "status": "success",
            "namespace": namespace,
            "table": table_name,
            "column": column_name,
            "total_rows": total_rows,
            "nan_count": int(nan_count),
            "non_null_count": int(total_rows - nan_count)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to calculate column stats: {e}")



@router.get("/find_duplicates")
def find_duplicates(
    namespace: str = Query(..., description="Namespace (e.g. 'sales')"),
    table_name: str = Query(..., description="Table name (e.g. 'transactions')"),
    column_name: str = Query(..., description="Column to check duplicates for (e.g. 'pri_id')")
):
    """
    Finds duplicate values in a column and returns their counts.
    """
    try:
        catalog = get_catalog_client()
        table = catalog.load_table(f"{namespace}.{table_name}")

        # Load table into pandas
        scan = table.scan()
        df = scan.to_pandas()

        if column_name not in df.columns:
            raise HTTPException(status_code=404, detail=f"Column '{column_name}' not found in table '{table_name}'")

        # Find duplicates
        duplicates = df[column_name].value_counts()
        duplicates = duplicates[duplicates > 1]

        # Format result
        result = duplicates.reset_index().rename(columns={"index": column_name, column_name: "count"})
        duplicates_list = result.to_dict(orient="records")
        print(len(duplicates_list))
        return {
            "status": "success",
            "namespace": namespace,
            "table": table_name,
            "column": column_name,
            "duplicate_count": len(duplicates_list),
            # "duplicates": duplicates_list
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to find duplicates: {str(e)}")

import time

@router.get("/filter-data-test")
def get_filtered_data(
    namespace: str = Query(..., description="Namespace (e.g. 'transactions')"),
    table_name: str = Query(..., description="Table name (e.g. 'pos')"),
    column: str = Query(..., description="Column name to filter on"),
    value: str = Query(..., description="Value to match")
):
    start_time = time.time()

    try:
        catalog = get_catalog_client()
        table = catalog.load_table((namespace, table_name))

        field = next((f for f in table.schema().fields if f.name == column), None)
        if not field:
            raise HTTPException(status_code=400, detail=f"Column '{column}' not found in schema")

        # Apply filter
        scan = table.scan(row_filter=EqualTo(column, value))
        arrow_table = scan.to_arrow()
        rows = [row for batch in arrow_table.to_batches() for row in batch.to_pylist()]
        # rows = []
        #
        # for batch in scan.to_arrow():
        #     rows.extend(batch.to_pylist())
        #     # batch = batch.to_pandas()
            # print(batch)
        elapsed = round(time.time() - start_time, 2)

        # --- Metadata without using _plan() ---
        metadata = {
            "namespace": namespace,
            "table": table_name,
            "filter_column": column,
            "filter_value": value,
            "data":rows,
            # "schema_fields": [f.name for f in table.schema().fields],
            "snapshot_id": table.current_snapshot().snapshot_id if table.current_snapshot() else None,
            # "manifest_count": len(table.current_snapshot().manifests) if table.current_snapshot() else None,
            "execution_time_seconds": elapsed,

        }

        # --- Preview (first 5 rows only) ---
        preview = rows[:5] if rows else []

        return {
            "status": "success",
            "metadata": metadata,
            "records_count": len(rows),
            # "preview": preview,
            # "data": rows,  # full result
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error filtering data: {str(e)}")


# @router.get("/snapshots")
# def list_snapshots(
#     namespace: str = Query(..., description="Namespace (e.g. 'transactions')"),
#     table_name: str = Query(..., description="Table name (e.g. 'pos')")
# ):
#     try:
#         catalog = get_catalog_client()
#         table = catalog.load_table((namespace, table_name))
#
#         # Get all snapshots
#         snapshots = table.snapshots()  # returns a list of Snapshot objects
#         print(snapshots)
#
#         snapshot_list = [
#             {
#                 "snapshot_id": snap.snapshot_id,
#                 "timestamp": snap.timestamp_ms,
#                 "operation": snap.operation,
#                 "manifest_count": len(snap.manifests) if snap.manifests else 0
#             }
#             for snap in snapshots
#         ]
#
#         return {
#             "status": "success",
#             "snapshot_count": len(snapshot_list),
#             "snapshots": snapshot_list
#         }
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error fetching snapshots: {str(e)}")

@router.get("/snapshots")
def list_snapshots(
    namespace: str = Query(..., description="Namespace (e.g. 'transactions')"),
    table_name: str = Query(..., description="Table name (e.g. 'pos')")
):
    try:
        catalog = get_catalog_client()
        table = catalog.load_table((namespace, table_name))

        # Get all snapshots
        snapshots = table.snapshots()  # returns a list of Snapshot objects
        snapshot_list = [
            {
                "snapshot_id": snap.snapshot_id,
                # "parent_id": snap.parent_id,
                "manifest_list": snap.manifest_list,
                "timestamp_ms": snap.timestamp_ms
            }
            for snap in snapshots
        ]

        # Optional: sort by timestamp descending
        snapshot_list.sort(key=lambda x: x["timestamp_ms"], reverse=True)

        return {
            "status": "success",
            "snapshot_count": len(snapshot_list),
            "snapshots": snapshot_list
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching snapshots: {str(e)}")

from pyiceberg.expressions import AlwaysTrue
@router.get("/snapshot-data")
def get_snapshot_data(
    namespace: str = Query(..., description="Namespace (e.g. 'transactions')"),
    table_name: str = Query(..., description="Table name (e.g. 'pos')"),
    snapshot_id: int = Query(..., description="Snapshot ID to query"),
    limit: int = Query(100, description="Number of rows to return (default: 100)")
):
    """
    Fetches data from a specific Iceberg snapshot ID (time travel query).
    """
    start_time = time.time()

    try:
        catalog = get_catalog_client()
        table = catalog.load_table((namespace, table_name))

        # Validate snapshot ID
        snapshot_ids = [snap.snapshot_id for snap in table.snapshots()]
        if snapshot_id not in snapshot_ids:
            raise HTTPException(status_code=400, detail=f"Snapshot ID {snapshot_id} not found in table")

        # Perform scan using the snapshot_id
        scan = table.scan(row_filter=AlwaysTrue(), snapshot_id=snapshot_id)

        # Read Arrow data
        arrow_table = scan.to_arrow()
        rows = []
        for batch in arrow_table.to_batches():
            rows.extend(batch.to_pylist())
            if len(rows) >= limit:
                break

        elapsed = round(time.time() - start_time, 2)

        return {
            "status": "success",
            "namespace": namespace,
            "table": table_name,
            "snapshot_id": snapshot_id,
            "record_count": len(rows),
            "execution_time_seconds": elapsed,
            "data_preview": rows[:min(len(rows), 10)]
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data from snapshot {snapshot_id}: {str(e)}")

@router.get("/filter-data-range")
def filter_data_range(
    namespace: str = Query(..., description="Namespace (e.g. 'transactions')"),
    table_name: str = Query(..., description="Table name (e.g. 'pos')"),
    column: str = Query(..., description="Column name to filter on"),
    start: int = Query(..., description="Start value (inclusive)"),
    end: int = Query(..., description="End value (inclusive)"),
    limit: int = Query(1000, description="Number of rows to return (default: 1000)")
):
    """
    Fetch rows where column BETWEEN start and end.
    Example:
    /filter-data-range?namespace=transactions&table_name=pos&column=pri_id&start=1&end=5000
    """
    start_time = time.time()
    try:
        catalog = get_catalog_client()
        table = catalog.load_table((namespace, table_name))

        # Ensure column exists
        field = next((f for f in table.schema().fields if f.name == column), None)
        if not field:
            raise HTTPException(status_code=400, detail=f"Column '{column}' not found in schema")

        # Build range filter
        range_filter = And(GreaterThanOrEqual(column, start), LessThanOrEqual(column, end))

        # Scan table with filter
        scan = table.scan(row_filter=range_filter)
        arrow_table = scan.to_arrow()

        # Convert to list of dicts
        rows = []
        for batch in arrow_table.to_batches():
            rows.extend(batch.to_pylist())
            if len(rows) >= limit:
                break

        elapsed = round(time.time() - start_time, 2)

        return {
            "status": "success",
            "namespace": namespace,
            "table": table_name,
            "filter_column": column,
            "range": {"start": start, "end": end},
            "records_count": len(rows),
            "execution_time_seconds": elapsed,
            "preview": rows[:min(len(rows), 10)]
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error filtering data: {str(e)}")


# @router.get("/filter-data-fast")
# def get_filtered_data(
#     namespace: str = Query(..., description="Namespace (e.g. 'transactions')"),
#     table_name: str = Query(..., description="Table name (e.g. 'pos')"),
#     column: str = Query(..., description="Column name to filter on"),
#     value: str = Query(..., description="Value to match")
# ):
#     start_time = time.time()
#
#     try:
#         catalog = get_catalog_client()
#         table = catalog.load_table((namespace, table_name))
#
#         field = next((f for f in table.schema().fields if f.name == column), None)
#         if not field:
#             raise HTTPException(status_code=400, detail=f"Column '{column}' not found in schema")
#
#         # Apply filter
#         scan = table.scan(row_filter=EqualTo(column, value))
#         arrow_table = scan.to_arrow()
#         rows = [row for batch in arrow_table.to_batches() for row in batch.to_pylist()]
#
#         elapsed = round(time.time() - start_time, 2)
#
#         # --- Metadata without using _plan() ---
#         metadata = {
#             "namespace": namespace,
#             "table": table_name,
#             "execution_time_seconds": elapsed,
#             "filter_column": column,
#             "filter_value": value,
#             "data":rows,
#             "snapshot_id": table.current_snapshot().snapshot_id if table.current_snapshot() else None,
#         }
#
#
#
#         return {
#             "status": "success",
#             "metadata": metadata,
#             "records_count": len(rows),
#
#         }
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error filtering data: {str(e)}")

# @router.get("/filter-customer-mobile-fast")
# def filter_customer_mobile_fast(
#         namespace: str = Query(..., description="Namespace (e.g. 'transactions')"),
#         table_name: str = Query(..., description="Table name (e.g. 'pos')"),
#         mobile_phone: str = Query(..., description="Customer mobile phone number to filter"),
#         limit: int = Query(1000, description="Number of records per page"),
#         offset: int = Query(0, description="Offset for pagination")
# ):
#     start_time = time.time()
#
#     try:
#         catalog = get_catalog_client()
#         table = catalog.load_table((namespace, table_name))
#
#         column = "customer_mobile__c"
#         field = next((f for f in table.schema().fields if f.name == column), None)
#         if not field:
#             raise HTTPException(status_code=400, detail=f"Column '{column}' not found in schema")
#
#         # --- Scan only relevant partition files ---
#         scan = table.scan(row_filter=EqualTo(column, mobile_phone)).select(column)
#         print("scan",scan.count())
#         rows = []
#
#         # --- Lazy iteration with offset and limit ---
#         total_seen = 0
#         batch_size = 5000  # adjust as needed
#         for batch in scan.to_arrow_batches(batch_size=batch_size):
#             batch_rows = batch.to_pylist()
#             if total_seen + len(batch_rows) < offset:
#                 total_seen += len(batch_rows)
#                 continue  # skip until reaching offset
#             start_idx = max(0, offset - total_seen)
#             rows.extend(batch_rows[start_idx:])
#             total_seen += len(batch_rows)
#             if len(rows) >= limit:
#                 rows = rows[:limit]
#                 break
#
#         elapsed = round(time.time() - start_time, 2)
#
#         metadata = {
#             "namespace": namespace,
#             "table": table_name,
#             "execution_time_seconds": elapsed,
#             "filter_column": column,
#             "filter_value": mobile_phone,
#             "snapshot_id": table.current_snapshot().snapshot_id if table.current_snapshot() else None,
#             "records_count": len(rows),
#         }
#
#         return {"status": "success", "metadata": metadata, "data": rows}
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))