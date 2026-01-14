from fastapi import FastAPI, Query, HTTPException,APIRouter

from datetime import datetime
from typing import Optional
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThanOrEqual
from ..core.catalog_client import  get_catalog_client
import logging

logger = logging.getLogger(__name__)

app = FastAPI()
router = APIRouter(prefix="/filter", tags=["filter"])
# Load catalog (example: REST catalog or file-based)
# 1. Filter by string column
# GET /filter_data?namespace=sales&table_name=transactions&column_name=status&column_value=SUCCESS
# 2. Filter by datetime range
# GET /filter_data?namespace=sales&table_name=transactions&date_column=created_at&start_date=2025-10-01T00:00:00&end_date=2025-10-10T23:59:59
# 3. Combine both
# GET /filter_data?namespace=sales&table_name=transactions&column_name=status&column_value=SUCCESS&date_column=created_at&start_date=2025-10-01T00:00:00

@router.get("/filter-data-pri-id")
def get_filtered_data(
    # namespace: str = Query(..., description="Namespace (e.g. 'transactions')"),
    # table_name: str = Query(..., description="Table name (e.g. 'pos')"),
    column: str = Query(..., description="Column name to filter on pri_id"),
    value: str = Query(..., description="Value to match")
):
    start_time = time.time()

    try:
        catalog = get_catalog_client()
        namespace = "pos_transactions"
        table_name = "transaction"
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
        # preview = rows[:5] if rows else []

        return {
            "status": "success",
            # "metadata": metadata,
            "records_count": len(rows),
            "elapsed":elapsed,
            # "preview": preview,
            "data": rows,  # full result
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error filtering data: {str(e)}")
    
@router.get("/filter-data-customer")
def filter_customer_mobile(column: str = Query(...), value: str = Query(...)):
    import time
    start_time = time.time()
    
    try:
        catalog = get_catalog_client()
        table = catalog.load_table(("pos_transactions", "transaction"))

        field = next((f for f in table.schema().fields if f.name == column), None)
        if not field:
            raise HTTPException(status_code=400, detail=f"Column '{column}' not found")
        
        # Scan with filter and column projection
        scan = table.scan().filter(EqualTo(column, value)).select(column)
        
        rows = []
        arrow_table = scan.to_arrow()
        # for batch in scan.to_arrow_batches(batch_size=5000):
        #     rows.extend(batch.to_pylist())
        rows = [row for batch in arrow_table.to_batches() for row in batch.to_pylist()]

        elapsed = round(time.time() - start_time, 2)
        return {
            "status": "success",
            "records_count": len(rows),
            "elapsed_seconds": elapsed,
            "data": rows[:500]  # limit for preview
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# @router.get("/filter-customer-phone")
# def filter_customer_phone(
#     phone: str = Query(..., description="Customer phone number to search"),
#     start_date: str = Query(..., description="Start date in YYYY-MM-DD"),
#     end_date: str = Query(..., description="End date in YYYY-MM-DD"),
#     max_preview: int = Query(500, description="Maximum number of rows to preview")
# ):
#     start_time = time.time()

#     try:
#         catalog = get_catalog_client()
#         table = catalog.load_table(("pos_transactions", "transaction"))

#         # Filter expression: phone match AND date between start_date and end_date
#         filter_expr = And(
#             EqualTo("customer_mobile__c", phone),
#             And(
#                 GreaterThanOrEqual("created_date", start_date),
#                 LessThanOrEqual("created_date", end_date)
#             )
#         )

#         # Scan with filter and only necessary columns
#         scan = table.scan().filter(filter_expr).select("customer_mobile__c", "created_date", "pri_id")

#         rows = []
#         for batch in scan.to_arrow_batches(batch_size=5000):
#             rows.extend(batch.to_pylist())

#         elapsed = round(time.time() - start_time, 2)

#         return {
#             "status": "success",
#             "records_count": len(rows),
#             "elapsed_seconds": elapsed,
#             "data": rows[:max_preview]  # preview limited
#         }

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error filtering data: {str(e)}")


@router.get("/filter-customer-phone")
def filter_customer_phone(
    phone: str = Query(..., description="Customer phone number to search"),
    start_date: str = Query(..., description="Start date in YYYY-MM-DD"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD"),
    max_preview: int = Query(500, description="Maximum number of rows to preview")
):
    start_time = time.time()

    try:
        catalog = get_catalog_client()
        table = catalog.load_table(("pos_transactions", "transaction"))

        # Build filter: phone AND date range
        filter_expr = And(
            EqualTo("customer_mobile__c", phone),
            And(
                GreaterThanOrEqual("Bill_Date__c", start_date),
                LessThanOrEqual("Bill_Date__c", end_date)
            )
        )

        # Scan with filter and column projection
        scan = table.scan().filter(filter_expr).select("customer_mobile__c", "Bill_Date__c", "pri_id")

        # Convert to Arrow table
        arrow_table = scan.to_arrow()
        rows = [row for batch in arrow_table.to_batches() for row in batch.to_pylist()]

        elapsed = round(time.time() - start_time, 2)

        return {
            "status": "success",
            "records_count": len(rows),
            "elapsed_seconds": elapsed,
            "data": rows[:max_preview]  # preview only
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error filtering data: {str(e)}")

# @router.get("/r2-batch-fetch")
# def get_data(
#     namespace: str = Query(..., description="Namespace (e.g. 'transactions')"),
#     table_name: str = Query(..., description="Table name (e.g. 'pos')")
# ):
#     try:
#         catalog = get_catalog_client()
#         table = catalog.load_table(f"{namespace}.{table_name}")
#     except Exception as e:
#         logger.error(f"Failed to load table: {str(e)}")
#         raise HTTPException(status_code=500, detail="Error loading table from catalog")

#     try:
#         # same logic as MySQL - fetch every 50000 rows, but take only 5 rows each time
#         batch_size = 5
#         step = 50000
#         total_records = 40000000  # you can also calculate len(table.scan()) if needed
#         num_batches = total_records // step + 1

#         all_data = []

#         for i in range(num_batches):
#             start = i * step
#             end = start + batch_size

#             # Equivalent of LIMIT + OFFSET in Iceberg
#             scan = table.scan().select("*").limit(batch_size).offset(start)
#             batch_records = []

#             for batch in scan.to_pandas_batches():
#                 df = batch.to_pandas()
#                 batch_records.extend(df.to_dict(orient="records"))

#             if batch_records:
#                 all_data.extend(batch_records)
#                 logger.info(f"Fetched rows {start + 1} to {start + batch_size}, records: {len(batch_records)}")

#         # Save all collected samples into Excel
#         df_final = pd.DataFrame(all_data)
#         output_file = f"{table_name}_sampled.xlsx"
#         df_final.to_excel(output_file, index=False)
#         logger.info(f"All data saved to {output_file}")

#         return {
#             "status": "success",
#             "records_fetched": len(all_data),
#             "output_file": output_file
#         }

#     except Exception as e:
#         logger.error(f"Failed to read data: {str(e)}")
#         raise HTTPException(status_code=500, detail=f"Error fetching data from Iceberg table: {str(e)}")
import pandas as pd
import pyarrow.parquet as pq

# @router.get("/r2-batch-fetch")
# def get_data(
#     namespace: str = Query(..., description="Namespace (e.g. 'transactions')"),
#     table_name: str = Query(..., description="Table name (e.g. 'pos')")
# ):
#     try:
#         catalog = get_catalog_client()
#         table = catalog.load_table(f"{namespace}.{table_name}")
#     except Exception as e:
#         logger.error(f"Failed to load table: {str(e)}")
#         raise HTTPException(status_code=500, detail=f"Error loading table from catalog: {str(e)}")

#     try:
#         # ✅ Convert Iceberg table directly into Arrow Table or Pandas DataFrame
#         try:
#             # Preferred if available
#             arrow_table = table.scan().to_arrow_table()
#             df = arrow_table.to_pandas()
#         except AttributeError:
#             # Fallback: some R2 clients expose .to_df()
#             try:
#                 df = table.scan().to_df()
#             except Exception as e:
#                 raise HTTPException(
#                     status_code=500,
#                     detail="Your Iceberg DataScan implementation does not support to_arrow_table() or to_df()."
#                 )

#         if df.empty:
#             raise HTTPException(status_code=404, detail="No data found in Iceberg table")

#         # ---- Replicate MySQL Logic ----
#         batch_size = 5
#         step = 50000
#         total_records = len(df)
#         sampled_rows = []

#         for start in range(0, total_records, step):
#             sampled_rows.extend(df.iloc[start:start + batch_size].to_dict(orient="records"))

#         if not sampled_rows:
#             raise HTTPException(status_code=404, detail="No records sampled from data")

#         # Save to Excel
#         df_sampled = pd.DataFrame(sampled_rows)
#         output_file = f"{table_name}_sampled.xlsx"
#         df_sampled.to_excel(output_file, index=False)

#         logger.info(f"All data saved to {output_file}")

#         return {
#             "status": "success",
#             "records_fetched": len(df_sampled),
#             "output_file": output_file
#         }

#     except Exception as e:
#         logger.error(f"Failed to read data: {str(e)}")
#         raise HTTPException(status_code=500, detail=f"Error fetching data from Iceberg table: {str(e)}")
import s3fs
import os

# @router.get("/r2-batch-fetch")
# def get_data(
#     namespace: str = Query(..., description="Namespace (e.g. 'transactions')"),
#     table_name: str = Query(..., description="Table name (e.g. 'pos')")
# ):
#     """
#     Replicates MySQL logic:
#     Fetch 5 rows every 50,000 rows from an Iceberg table (R2 Catalog)
#     and save to a single Excel file.
#     """
#     try:
#         catalog = get_catalog_client()
#         table = catalog.load_table(f"{namespace}.{table_name}")
#     except Exception as e:
#         logger.error(f"Failed to load table: {str(e)}")
#         raise HTTPException(status_code=500, detail=f"Error loading table from catalog: {str(e)}")

#     try:
#         # ✅ Step 1: Get list of all Parquet files from Iceberg table metadata
#         scan = table.scan()
#         parquet_files = [task.file.file_path for task in scan.plan_files()]

#         if not parquet_files:
#             raise HTTPException(status_code=404, detail="No Parquet files found in Iceberg table")

#         logger.info(f"Found {len(parquet_files)} Parquet files in table {namespace}.{table_name}")

#         # ✅ Step 2: Initialize S3FS for Cloudflare R2
#         fs = s3fs.S3FileSystem(
#             key=os.getenv("ACCESS_KEY_ID"),
#             secret=os.getenv("SECRET_ACCESS_KEY"),
#             client_kwargs={"endpoint_url": os.getenv("ENDPOINT")}
#         )


#         # ✅ Step 2: Sampling configuration (same logic as MySQL)
#         batch_size = 5
#         step = 50000
#         all_data = []
#         files_processed = 0

#         # ✅ Step 3: Read each Parquet file
#         for file_idx, file_path in enumerate(parquet_files, start=1):
#             try:
#                 logger.info(f"Reading file {file_idx}/{len(parquet_files)}: {file_path}")

#                 # Read Parquet file (directly from R2 / S3-like path)
#                 parquet_table = pq.read_table(file_path)
#                 df = parquet_table.to_pandas()

#                 if df.empty:
#                     continue

#                 total_records = len(df)
#                 for start in range(0, total_records, step):
#                     sample = df.iloc[start:start + batch_size]
#                     if not sample.empty:
#                         all_data.extend(sample.to_dict(orient="records"))

#                 files_processed += 1
#                 logger.info(f"Processed {file_idx}/{len(parquet_files)} successfully")

#             except Exception as e:
#                 logger.warning(f"Skipping file {file_path}: {e}")
#                 continue

#         if not all_data:
#             raise HTTPException(status_code=404, detail="No records found or sampled")

#         # ✅ Step 4: Save all sampled records to Excel
#         df_sampled = pd.DataFrame(all_data)
#         output_file = f"{table_name}_sampled.xlsx"
#         df_sampled.to_excel(output_file, index=False)

#         logger.info(f"Saved {len(df_sampled)} sampled rows to {output_file}")

#         return {
#             "status": "success",
#             "files_processed": files_processed,
#             "records_sampled": len(df_sampled),
#             "output_file": output_file
#         }

#     except Exception as e:
#         logger.error(f"Failed to fetch data: {str(e)}")
#         raise HTTPException(status_code=500, detail=f"Error fetching data from Iceberg table: {str(e)}")

# @router.get("/r2-batch-fetch")
# def get_data(
#     namespace: str = Query(..., description="Namespace (e.g. 'transactions')"),
#     table_name: str = Query(..., description="Table name (e.g. 'pos')")
# ):
#     """
#     Read Iceberg Parquet files from Cloudflare R2 using S3FS,
#     sample 5 rows every 50,000, and save as Excel.
#     """
#     try:
#         catalog = get_catalog_client()
#         table = catalog.load_table(f"{namespace}.{table_name}")
#     except Exception as e:
#         logger.error(f"Failed to load table: {str(e)}")
#         raise HTTPException(status_code=500, detail=f"Error loading table from catalog: {str(e)}")

#     try:
#         # ✅ Step 1: Collect Parquet file paths
#         scan = table.scan()
#         parquet_files = [task.file.file_path for task in scan.plan_files() if hasattr(task, "file")]

#         if not parquet_files:
#             raise HTTPException(status_code=404, detail="No Parquet files found in Iceberg table")

#         logger.info(f"Found {len(parquet_files)} parquet files")

#         # ✅ Step 2: Initialize S3FS for Cloudflare R2
#         fs = s3fs.S3FileSystem(
#             key=os.getenv("ACCESS_KEY_ID"),
#             secret=os.getenv("SECRET_ACCESS_KEY"),
#             client_kwargs={"endpoint_url": os.getenv("ENDPOINT")}
#         )

#         logger.info("Cloudflare R2",fs)

#         batch_size = 5
#         step = 50000
#         all_data = []
#         files_processed = 0

#         # ✅ Step 3: Read each Parquet file
#         for file_idx, file_path in enumerate(parquet_files, start=1):
#             try:
#                 logger.info(f"Reading file {file_idx}/{len(parquet_files)}: {file_path}")

#                 # Use filesystem-aware read
#                 parquet_table = pq.read_table(file_path, filesystem=fs)
#                 print("#"*100)
#                 print(parquet_files.to_pandas())
#                 df = parquet_table.to_pandas()

#                 if df.empty:
#                     continue

#                 total_records = len(df)
#                 for start in range(0, total_records, step):
#                     sample = df.iloc[start:start + batch_size]
#                     if not sample.empty:
#                         all_data.extend(sample.to_dict(orient="records"))

#                 files_processed += 1
#                 logger.info(f"Processed file {file_idx}/{len(parquet_files)} successfully")

#             except Exception as e:
#                 logger.warning(f"Skipping file {file_path}: {e}")
#                 continue

#         if not all_data:
#             raise HTTPException(status_code=404, detail="No records sampled from Parquet files")

#         # ✅ Step 4: Save to Excel
#         df_sampled = pd.DataFrame(all_data)
#         output_file = f"{table_name}_sampled.xlsx"
#         df_sampled.to_excel(output_file, index=False)

#         logger.info(f"Saved {len(df_sampled)} sampled rows to {output_file}")

#         return {
#             "status": "success",
#             "files_processed": files_processed,
#             "records_sampled": len(df_sampled),
#             "output_file": output_file
#         }

#     except Exception as e:
#         logger.error(f"Failed to fetch data: {str(e)}")
#         raise HTTPException(status_code=500, detail=f"Error fetching data from Iceberg table: {str(e)}")
import pyarrow as pa


@router.get("/r2-batch-fetch")
def get_data(
    namespace: str = Query(..., description="Namespace (e.g. 'transactions')"),
    table_name: str = Query(..., description="Table name (e.g. 'pos')")
):
    """
    Sample 5 rows every 50,000 rows from Iceberg Parquet files in R2
    and save as Excel. Handles dictionary and string type conflicts.
    """
    try:
        # 1️⃣ Load table
        catalog = get_catalog_client()
        table = catalog.load_table(f"{namespace}.{table_name}")
        print("success catalog")
    except Exception as e:
        logger.error(f"Failed to load table: {e}")
        raise HTTPException(status_code=500, detail=f"Error loading table: {e}")

    try:
        # 2️⃣ Collect Parquet files
        scan = table.scan()
        parquet_files = [task.file.file_path for task in scan.plan_files() if hasattr(task, "file")]
        # print("parquet_files",parquet_files)
        if not parquet_files:
            raise HTTPException(status_code=404, detail="No Parquet files found")

        print(f"Found {len(parquet_files)} parquet files")

        # 3️⃣ Configure R2 S3 access
        fs = s3fs.S3FileSystem(
            key=os.getenv("ACCESS_KEY_ID"),
            secret=os.getenv("SECRET_ACCESS_KEY"),
            client_kwargs={"endpoint_url": os.getenv("ENDPOINT")}
        )
        print("cred",fs)
        batch_size = 5
        step = 50000
        all_data = []
        files_processed = 0

        # 4️⃣ Read and sample each Parquet file
        for file_idx, file_path in enumerate(parquet_files, start=1):
            try:
                print(f"Reading file {file_idx}/{len(parquet_files)}: {file_path}")
                table_parquet = pq.read_table(file_path, filesystem=fs)
                print("parquet data",table_parquet)
                # Convert dictionary columns to base type
                for col in table_parquet.schema.names:
                    if pa.types.is_dictionary(table_parquet.schema.field(col).type):
                        table_parquet = table_parquet.set_column(
                            table_parquet.schema.get_field_index(col),
                            col,
                            table_parquet.column(col).cast(table_parquet.schema.field(col).type.value_type)
                        )

                # Convert all columns to string to avoid merge conflicts
                df = table_parquet.to_pandas()
                print(df)
                df = df.astype({col: "str" for col in df.columns})

                total_records = len(df)
                for start in range(0, total_records, step):
                    sample = df.iloc[start:start + batch_size]
                    if not sample.empty:
                        all_data.extend(sample.to_dict(orient="records"))

                files_processed += 1

            except Exception as e:
                logger.warning(f"Skipping file {file_path}: {e}")
                continue

        if not all_data:
            raise HTTPException(status_code=404, detail="No sampled data found")

        # 5️⃣ Save all sampled rows to Excel
        df_sampled = pd.DataFrame(all_data)
        output_file = f"{table_name}_sampled.xlsx"
        df_sampled.to_excel(output_file, index=False)

        logger.info(f"Saved {len(df_sampled)} sampled rows to {output_file}")

        return {
            "status": "success",
            "files_processed": files_processed,
            "records_sampled": len(df_sampled),
            "output_file": output_file
        }

    except Exception as e:
        logger.error(f"Failed to fetch data: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching data from Iceberg table: {e}")


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