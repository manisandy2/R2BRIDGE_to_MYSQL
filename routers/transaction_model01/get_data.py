from fastapi import APIRouter,HTTPException,Query,Body
from pyiceberg.catalog import load_catalog
from ..core.catalog_client import get_catalog_client
from pyiceberg.expressions import EqualTo
from ..core.r2_client import get_r2_client
import pandas as pd
import pyarrow.parquet as pq
# from fastparquet import ParquetFile
from pyiceberg.expressions import EqualTo
import time,logging
from datetime import datetime

router = APIRouter(prefix="/data", tags=["Data"])

logger = logging.getLogger(__name__)
@router.get("/filter")

def get_data(
    namespace: str = Query(..., description="Namespace (e.g. 'Namespace')"),
    table_name: str = Query(..., description="Table name (e.g. 'Table name')")
):
    try:
        catalog = get_catalog_client()
        table = catalog.load_table((namespace, table_name))
    except Exception as e:
        logger.error(f"Failed to load table: {str(e)}")
        raise HTTPException(status_code=500, detail="Error loading table from catalog")

    try:
        scan = table.scan()
        files = [task.file.file_path for task in scan.plan_files()]
    except Exception as e:
        logger.error(f"Failed to scan table: {str(e)}")
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
        logger.error(f"Failed to retrieve snapshots: {str(e)}")
        raise HTTPException(status_code=500, detail="Error retrieving snapshot data")

    return {

        "records_count": len(snapshots),
        "snapshots": snapshot_data,
        "parquet_files": files
    }

# @router.get("/filter-data")
# def get_filtered_data(
#     namespace: str = Query(...),
#     table_name: str = Query(...),
#     column: str = Query(...),
#     value: str = Query(...)
# ):
#     catalog = get_catalog_client()
#     table = catalog.load_table((namespace, table_name))
#
#     # Filtering automatically scans all 30 files
#     scan = table.scan(row_filter=EqualTo(column, value))
#
#     rows = []
#     for batch in scan.to_arrow():   # Arrow batches from multiple parquet files
#         rows.extend(batch.to_pylist())
#
#     return {
#         "namespace": namespace,
#         "table_name": table_name,
#         "records_count": len(rows),
#         "data": rows
#     }


@router.get("/filter-data")
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
            "preview": preview,
            "data": rows,  # full result
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error filtering data: {str(e)}")

@router.get("/tables_row_counts")
def get_tables(
    namespace: str = Query(..., description="Namespace (e.g. 'transactions')"),
    table_name: str = Query(..., description="Table name (e.g. 'pos')"),

):
    try:
        catalog = get_catalog_client()

        results = []

        if table_name:
            # Load specific table
            tables = [f"{namespace}.{table_name}"]
            # print("tables:", tables)
        else:
            # List all tables in namespace
            tables = catalog.list_tables(namespace)

        for tbl in tables:
            try:
                table = catalog.load_table(tbl)
                snapshot = table.current_snapshot()
                print("snapshot", snapshot.summary.get("total-records"))
                total_rows = (
                    snapshot.summary.get("total-records")
                    if snapshot and snapshot.summary
                    else None
                )

                results.append({
                    "table": tbl,
                    "row_count": total_rows
                })

            except Exception as e:
                logger.error(f"Failed to read table {tbl}: {e}")
                results.append({
                    "table": tbl,
                    "row_count": None,
                    "error": str(e)
                })

        return {"status": "success", "data": results}

    except Exception as e:
        logger.error(f"Failed to fetch table row counts: {e}")
        raise HTTPException(status_code=500, detail=str(e))
