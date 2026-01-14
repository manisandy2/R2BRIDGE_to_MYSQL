from .table_utility import transaction_clean_row,infer_schema_from_record
import math
from ...mysql_creds import *
from pyiceberg.schema import Schema
from pyiceberg.types import *
from botocore.client import Config
import boto3
from pyiceberg.catalog import NoSuchNamespaceError,NamespaceAlreadyExistsError,TableAlreadyExistsError,NoSuchTableError
from ...core.catalog_client import get_catalog_client
import traceback
import pyarrow as pa
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date
from fastapi import APIRouter,HTTPException,Query
from dateutil import parser
from datetime import datetime
import os
import time

s3 = boto3.client("s3")



LOGS_FOLDER = "logs/iceberg_upload"
os.makedirs(LOGS_FOLDER, exist_ok=True)

router = APIRouter(prefix="", tags=["insert_data"])

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("ENDPOINT"),
    aws_access_key_id=os.getenv("ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"),
    config=Config(signature_version="s3v4"),
    region_name="auto"
)



# def infer_schema_from_record(record: dict):
#     iceberg_fields = []
#     arrow_fields = []
#
#     # Custom field overrides (by name)
#     field_overrides = {
#         "pri_id": (LongType(), pa.int64(), True),
#         "Invoice_Amount__c": (DoubleType(), pa.float64(), False),
#         "Bill_Date__c": (DateType(), pa.date32(), False),
#         "CreatedDate": (DateType(), pa.date32(), False),
#     }
#
#     for idx, (name, value) in enumerate(record.items(), start=1):
#         if name in field_overrides:
#             ice_type, arrow_type, required = field_overrides[name]
#         else:
#             # Type inference
#             if isinstance(value, bool):
#                 ice_type = BooleanType()
#                 arrow_type = pa.bool_()
#             elif isinstance(value, int):
#                 ice_type = LongType()
#                 arrow_type = pa.int64()
#             elif isinstance(value, float):
#                 ice_type = DoubleType()
#                 arrow_type = pa.float64()
#             elif isinstance(value, (date, datetime)):
#                 ice_type = DateType()
#                 arrow_type = pa.date32()
#             else:
#                 ice_type = StringType()
#                 arrow_type = pa.string()
#             required = False
#
#         iceberg_fields.append(
#             NestedField(field_id=idx, name=name, field_type=ice_type, required=required)
#         )
#         arrow_fields.append(pa.field(name, arrow_type, nullable=not required))
#
#     iceberg_schema = Schema(*iceberg_fields)
#     arrow_schema = pa.schema(arrow_fields)
#     return iceberg_schema, arrow_schema


def process_chunk(chunk, arrow_schema):
    processed_rows = []
    date_formats = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y")

    for row_idx, row in enumerate(chunk):
        converted_row = {}
        # print(f" Processing row {row_idx} -> keys: {list(row.keys())}")

        for field in arrow_schema:
            val = row.get(field.name, None)

            # Debug mismatched field
            if field.name not in row:
                print(f"⚠️ Field '{field.name}' missing in row; available keys: {list(row.keys())}")

            try:
                # --- Handle empty or None values ---
                if val in ("", " ", None):
                    converted_row[field.name] = None
                    continue

                # --- Integer fields ---
                if pa.types.is_integer(field.type):
                    converted_row[field.name] = int(val)

                # --- Float fields ---
                elif pa.types.is_floating(field.type):
                    converted_row[field.name] = float(val)

                # --- Timestamp or date fields ---
                elif pa.types.is_timestamp(field.type) or pa.types.is_date(field.type):
                    parsed_date = None

                    if isinstance(val, (datetime, date)):
                        parsed_date = val
                    elif isinstance(val, str):
                        val = val.strip()
                        for fmt in date_formats:
                            try:
                                parsed_date = datetime.strptime(val, fmt)
                                break
                            except ValueError:
                                continue

                    if parsed_date:
                        converted_row[field.name] = (
                            parsed_date if isinstance(parsed_date, datetime)
                            else datetime.combine(parsed_date, datetime.min.time())
                        )
                    else:
                        print(f" Row {row_idx}: Unrecognized date in '{field.name}': {val}")
                        converted_row[field.name] = None

                # --- Default: keep as string or object ---
                else:
                    converted_row[field.name] = val

            except Exception as e:
                print(f" Row {row_idx}, Field '{field.name}', Value: {val}, Error: {e}")
                converted_row[field.name] = None

        processed_rows.append(converted_row)

    #  Debug before conversion
    # print(" Example converted_row:", processed_rows[0] if processed_rows else "EMPTY")

    return pa.Table.from_pylist(processed_rows, schema=arrow_schema)


# @router.post("/data/insert/data")
# def r2_catalog(
#     start_range: int = Query(0, description="Start row offset for MySQL data fetch"),
#     end_range: int = Query(0, description="End row offset for MySQL data fetch"),
#     chunk_size: int = Query(100000, description="Chunk size for multithreading"),
# ):
#     total_start = time.time()
#     # namespace, table_name = "pos_transactions_with_out", "iceberg_out_partitioning"
#     namespace, table_name = "data_transactions_test", "transactions_test"
#     # namespace, table_name = "pos_transactions_year", "transaction_year"
#     # namespace, table_name = "pos_transactions_year_month", "transaction_year_month"
#     dbname = "Transaction"
#     mysql_creds = MysqlCatalog()
#
#     # -------------------------------------------------
#     # Step 1: Fetch and Convert MySQL Data
#     # -------------------------------------------------
#     mysql_start = time.time()
#     try:
#         rows = mysql_creds.get_range_ph_bi(dbname, start_range, end_range)
#         if not rows:
#             raise HTTPException(status_code=400, detail="No data found in the given range.")
#
#         converted_rows = []
#
#         transaction_clean_row(rows)
#
#
#         mysql_end = time.time()
#         print(f"MySQL fetch completed in {mysql_end - mysql_start:.2f} sec ({len(rows)} rows).")
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")
#
#     # -------------------------------------------------
#     # Step 2: Infer Iceberg + Arrow Schema
#     # -------------------------------------------------
#     schema_start = time.time()
#     iceberg_schema, arrow_schema = infer_schema_from_record(rows[0])
#     # print("iceberg_schema",iceberg_schema)
#     # print("arrow_schema",arrow_schema)
#     arrow_table = pa.Table.from_pylist(
#         rows,
#         schema=arrow_schema
#     )
#
#     schema_end = time.time()
#     print(f"Schema inference completed in {schema_end - schema_start:.2f} sec")
#
#     # -------------------------------------------------
#     # Step 3: Convert Rows to Arrow Tables (Multithreaded)
#     # -------------------------------------------------
#     arrow_start = time.time()
#     chunks = [converted_rows[i:i + chunk_size] for i in range(0, len(converted_rows), chunk_size)]
#
#     # print("chunks",chunks)
#     arrow_tables = []
#
#     with ThreadPoolExecutor(max_workers=10) as executor:
#         futures = {executor.submit(process_chunk, chunk, arrow_schema): idx for idx, chunk in enumerate(chunks)}
#         for future in as_completed(futures):
#             idx = futures[future]
#             try:
#                 tbl = future.result()
#                 arrow_tables.append(tbl)
#                 print(f"Chunk {idx + 1}/{len(chunks)} processed with {tbl.num_rows} rows")
#             except Exception as e:
#                 print(f"Chunk {idx + 1} failed: {e}")
#                 raise HTTPException(status_code=500, detail=f"Arrow chunk conversion failed: {e}")
#
#
#     arrow_end = time.time()
#     print(f"Arrow conversion completed in {arrow_end - arrow_start:.2f} sec")
#
#     # -------------------------------------------------
#     # Step 4: Load Iceberg Table
#     # -------------------------------------------------
#     catalog_start = time.time()
#     catalog = get_catalog_client()
#     table_identifier = f"{namespace}.{table_name}"
#     # print(f"catalog table_identifier: {table_identifier}")
#     try:
#         tbl = catalog.load_table(table_identifier)
#         catalog_end = time.time()
#         print(f"Catalog load completed in {catalog_end - catalog_start:.2f} sec")
#     except NoSuchTableError:
#         raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")
#
#
#     append_start = time.time()
#     try:
#
#         for i, batch in enumerate(arrow_tables, start=1):
#             print(f"Appending batch {i}/{len(arrow_tables)} rows={batch.num_rows}")
#             tbl.append(batch)  # commit each
#
#
#         append_end = time.time()
#
#     except Exception as e:
#         error_message = str(e)
#         error_code = "ICEBERG_APPEND_FAILED"
#         print(f" {error_code}: {error_message}")
#
#         raise HTTPException(
#             status_code=500,
#             detail={
#                 "error_code": error_code,
#                 "message": f"Data append failed for table {table_identifier}",
#                 "exception": error_message,
#             },
#         )
#
#     print(f" Append completed in {append_end - append_start:.2f} sec")
#
#     # -------------------------------------------------
#     # Step 6: Return Response
#     # -------------------------------------------------
#     return {
#         "success": True,
#         "message": "Data appended successfully with multithreading",
#         "rows_fetched": len(rows),
#         "chunks": len(chunks),
#         "execution_times": {
#             "mysql_fetch": round(mysql_end - mysql_start, 2),
#             "schema_infer": round(schema_end - schema_start, 2),
#             "arrow_convert": round(arrow_end - arrow_start, 2),
#             "catalog_load": round(catalog_end - catalog_start, 2),
#             "append_refresh": round(append_end - append_start, 2),
#             "total_time": round(append_end - total_start, 2),
#         },
#     }

@router.post("/data/insert/data")
def r2_catalog(
    start_range: int = Query(0, description="Start row offset for MySQL data fetch"),
    end_range: int = Query(0, description="End row offset for MySQL data fetch"),
    chunk_size: int = Query(10000, description="Batch size for Iceberg append"),
):
    total_start = time.time()

    namespace, table_name = "data_transactions_test", "transactions_test"
    table_identifier = f"{namespace}.{table_name}"

    dbname = "Transaction"
    mysql_creds = MysqlCatalog()

    # -------------------------------------------------
    # Step 1: Fetch MySQL Data
    # -------------------------------------------------
    mysql_start = time.time()
    try:
        rows = mysql_creds.get_range_ph_bi(dbname, start_range, end_range)
        if not rows:
            raise HTTPException(status_code=400, detail="No data found in the given range.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MySQL fetch error: {e}")

    mysql_end = time.time()
    print(f"MySQL fetch: {len(rows)} rows in {mysql_end - mysql_start:.2f}s")

    # -------------------------------------------------
    # Step 2: Clean Rows (CRITICAL)
    # -------------------------------------------------
    clean_start = time.time()
    cleaned_rows = transaction_clean_row(rows)
    clean_end = time.time()

    # -------------------------------------------------
    # Step 3: Load Iceberg Table
    # -------------------------------------------------
    catalog_start = time.time()
    catalog = get_catalog_client()
    try:
        tbl = catalog.load_table(table_identifier)
    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")
    catalog_end = time.time()
    print(f"Catalog load: {catalog_end - catalog_start:.2f}s")
    # -------------------------------------------------
    # Step 4: Batch → Arrow → Sort → Append
    # -------------------------------------------------
    append_start = time.time()

    total_batches = math.ceil(len(cleaned_rows) / chunk_size)

    for i in range(0, len(cleaned_rows), chunk_size):
        batch_no = (i // chunk_size) + 1
        batch_rows = cleaned_rows[i:i + chunk_size]

        # Infer schema ONCE from first batch row
        iceberg_schema, arrow_schema = infer_schema_from_record(batch_rows[0])
        # print(f"Batch {batch_no} of {total_batches}: {batch_rows}")
        # arrow_table = pa.Table.from_pylist(
        #     batch_rows,
        #     schema=arrow_schema
        # )
        try:
            arrow_table = pa.Table.from_pylist(batch_rows, schema=arrow_schema)
            # print(f"Batch {batch_no} of {total_batches}: {batch_rows}")
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Arrow conversion failed: {e}"
            )
        # 🔥 IMPORTANT: sort for fast search
        arrow_table = arrow_table.sort_by([
            ("customer_mobile__c", "ascending"),
            ("Bill_Date__c", "ascending"),
        ])

        print(f"Appending batch {batch_no}/{total_batches} rows={arrow_table.num_rows}")
        tbl.append(arrow_table)
        print(time.time() - append_start)
    append_end = time.time()

    # -------------------------------------------------
    # Step 5: Response
    # -------------------------------------------------
    return {
        "success": True,
        "message": "Data appended successfully",
        "rows_fetched": len(rows),
        "batches": total_batches,
        "execution_times": {
            "mysql_fetch": round(mysql_end - mysql_start, 2),
            "row_clean": round(clean_end - clean_start, 2),
            "catalog_load": round(catalog_end - catalog_start, 2),
            "append": round(append_end - append_start, 2),
            "total": round(append_end - total_start, 2),
        },
    }




