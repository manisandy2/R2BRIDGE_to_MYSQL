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

import time

s3 = boto3.client("s3")



LOGS_FOLDER = "logs/iceberg_upload"
os.makedirs(LOGS_FOLDER, exist_ok=True)

router = APIRouter(prefix="", tags=["insert"])

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("ENDPOINT"),
    aws_access_key_id=os.getenv("ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"),
    config=Config(signature_version="s3v4"),
    region_name="auto"
)

type_mapping = {
    "int": LongType(),
    'bigint': LongType(),
    'varchar': StringType(),
    'char': StringType(),
    'text': StringType(),
    'longtext': StringType(),
    'date': DateType(),
    'datetime': TimestampType(),
    'timestamp': TimestampType(),
    'float': FloatType(),
    'double': DoubleType(),
    'boolean': BooleanType(),
    'tinyint': BooleanType()
}

arrow_mapping = {
    # 'int': pa.int32(),
    "int": pa.int64(),
    'bigint': pa.int64(),
    'varchar': pa.string(),
    'char': pa.string(),
    'text': pa.string(),
    'longtext': pa.string(),
    'date': pa.date32(),
    'datetime': pa.timestamp('ms'),
    'timestamp': pa.timestamp('ms'),
    'float': pa.float32(),
    'double': pa.float64(),
    'boolean': pa.bool_(),
    'tinyint': pa.bool_(),
    'bit': pa.bool_(),
    # 'decimal': lambda p=18, s=6: pa.decimal128(p, s)
    'decimal' : pa.decimal128(18, 6)
}

def infer_schema_from_record(record: dict):
    iceberg_fields = []
    arrow_fields = []

    # Custom field overrides (by name)
    field_overrides = {
        "pri_id": (LongType(), pa.int64(), True),
        "Invoice_Amount__c": (DoubleType(), pa.float64(), False),
        "Bill_Date__c": (DateType(), pa.date32(), False),
        "CreatedDate": (DateType(), pa.date32(), False),
    }

    for idx, (name, value) in enumerate(record.items(), start=1):
        if name in field_overrides:
            ice_type, arrow_type, required = field_overrides[name]
        else:
            # Type inference
            if isinstance(value, bool):
                ice_type = BooleanType()
                arrow_type = pa.bool_()
            elif isinstance(value, int):
                ice_type = LongType()
                arrow_type = pa.int64()
            elif isinstance(value, float):
                ice_type = DoubleType()
                arrow_type = pa.float64()
            elif isinstance(value, (date, datetime)):
                ice_type = DateType()
                arrow_type = pa.date32()
            else:
                ice_type = StringType()
                arrow_type = pa.string()
            required = False

        iceberg_fields.append(
            NestedField(field_id=idx, name=name, field_type=ice_type, required=required)
        )
        arrow_fields.append(pa.field(name, arrow_type, nullable=not required))

    iceberg_schema = Schema(*iceberg_fields)
    arrow_schema = pa.schema(arrow_fields)
    return iceberg_schema, arrow_schema


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


@router.post("/data/insert/data")
def r2_catalog(
    start_range: int = Query(0, description="Start row offset for MySQL data fetch"),
    end_range: int = Query(0, description="End row offset for MySQL data fetch"),
    chunk_size: int = Query(100000, description="Chunk size for multithreading"),
):
    total_start = time.time()
    # namespace, table_name = "pos_transactions_with_out", "iceberg_out_partitioning"
    namespace, table_name = "pos_transactions", "transaction"
    # namespace, table_name = "pos_transactions_year", "transaction_year"
    # namespace, table_name = "pos_transactions_year_month", "transaction_year_month"
    dbname = "Transaction"
    mysql_creds = MysqlCatalog()

    # -------------------------------------------------
    # Step 1: Fetch and Convert MySQL Data
    # -------------------------------------------------
    mysql_start = time.time()
    try:
        rows = mysql_creds.get_range_ph_bi(dbname, start_range, end_range)
        if not rows:
            raise HTTPException(status_code=400, detail="No data found in the given range.")

        converted_rows = []


        for row in rows:
            # Convert float fields safely
            float_fields = ["bill_tax__c", "bill_grand_total__c", "Invoice_Amount__c"]
            for f in float_fields:
                val = row.get(f)
                if isinstance(val, str):
                    try:
                        row[f] = float(val)
                    except ValueError:
                        row[f] = 0.0
                elif val is None:
                    row[f] = 0.0

            # Convert mobile numbers to int64
            mobile_val = row.get("customer_mobile__c")
            if isinstance(mobile_val, str):
                try:
                    row["customer_mobile__c"] = int(mobile_val)
                except ValueError:
                    row["customer_mobile__c"] = None

            # Convert Item_Code__c to int64
            item_val = row.get("Item_Code__c")
            if isinstance(item_val, str):
                try:
                    row["Item_Code__c"] = int(item_val)
                except ValueError:
                    row["Item_Code__c"] = 0

            # Convert date strings to Python `date` object (yyyy-mm-dd only)
            for date_field in ["Bill_Date__c",  "CreatedDate"]:
                # print(date_field)
                val = row.get(date_field)

                if not val or str(val).strip() == "":
                    row[date_field] = None
                    continue

                try:
                    # use auto parser
                    dt = parser.parse(str(val))  # can parse both '6/24/2021 0:00' and '2021-06-24 00:00:00'
                    row[date_field] = dt
                except Exception as e:
                    print(f" Error converting {date_field}: {val} ({e})")
                    row[date_field] = None

            converted_rows.append(row)


        mysql_end = time.time()
        print(f"MySQL fetch completed in {mysql_end - mysql_start:.2f} sec ({len(rows)} rows).")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")

    # -------------------------------------------------
    # Step 2: Infer Iceberg + Arrow Schema
    # -------------------------------------------------
    schema_start = time.time()
    iceberg_schema, arrow_schema = infer_schema_from_record(rows[0])
    # print("iceberg_schema",iceberg_schema)
    # print("arrow_schema",arrow_schema)

    schema_end = time.time()
    print(f"Schema inference completed in {schema_end - schema_start:.2f} sec")

    # -------------------------------------------------
    # Step 3: Convert Rows to Arrow Tables (Multithreaded)
    # -------------------------------------------------
    arrow_start = time.time()
    chunks = [converted_rows[i:i + chunk_size] for i in range(0, len(converted_rows), chunk_size)]

    # print("chunks",chunks)
    arrow_tables = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_chunk, chunk, arrow_schema): idx for idx, chunk in enumerate(chunks)}
        for future in as_completed(futures):
            idx = futures[future]
            try:
                tbl = future.result()
                arrow_tables.append(tbl)
                print(f"Chunk {idx + 1}/{len(chunks)} processed with {tbl.num_rows} rows")
            except Exception as e:
                print(f"Chunk {idx + 1} failed: {e}")
                raise HTTPException(status_code=500, detail=f"Arrow chunk conversion failed: {e}")


    arrow_end = time.time()
    print(f"Arrow conversion completed in {arrow_end - arrow_start:.2f} sec")

    # -------------------------------------------------
    # Step 4: Load Iceberg Table
    # -------------------------------------------------
    catalog_start = time.time()
    catalog = get_catalog_client()
    table_identifier = f"{namespace}.{table_name}"
    # print(f"catalog table_identifier: {table_identifier}")
    try:
        tbl = catalog.load_table(table_identifier)
        catalog_end = time.time()
        print(f"Catalog load completed in {catalog_end - catalog_start:.2f} sec")
    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")


    append_start = time.time()
    try:

        for i, batch in enumerate(arrow_tables, start=1):
            print(f"Appending batch {i}/{len(arrow_tables)} rows={batch.num_rows}")
            tbl.append(batch)  # commit each


        append_end = time.time()

    except Exception as e:
        error_message = str(e)
        error_code = "ICEBERG_APPEND_FAILED"
        print(f" {error_code}: {error_message}")

        raise HTTPException(
            status_code=500,
            detail={
                "error_code": error_code,
                "message": f"Data append failed for table {table_identifier}",
                "exception": error_message,
            },
        )

    print(f" Append completed in {append_end - append_start:.2f} sec")

    # -------------------------------------------------
    # Step 6: Return Response
    # -------------------------------------------------
    return {
        "success": True,
        "message": "Data appended successfully with multithreading",
        "rows_fetched": len(rows),
        "chunks": len(chunks),
        "execution_times": {
            "mysql_fetch": round(mysql_end - mysql_start, 2),
            "schema_infer": round(schema_end - schema_start, 2),
            "arrow_convert": round(arrow_end - arrow_start, 2),
            "catalog_load": round(catalog_end - catalog_start, 2),
            "append_refresh": round(append_end - append_start, 2),
            "total_time": round(append_end - total_start, 2),
        },
    }
################################################################################
# @router.post("/data/insert/month-sort")
# def r2_catalog(
#     start_range: int = Query(0, description="Start row offset for MySQL data fetch"),
#     end_range: int = Query(0, description="End row offset for MySQL data fetch"),
#     chunk_size: int = Query(100000, description="Chunk size for multithreading"),
# ):
#     total_start = time.time()
#     # namespace, table_name = "pos_transactions_with_out", "iceberg_out_partitioning"
#     namespace, table_name = "pos_transactions_year_sort_month", "transaction_year_sort_month"
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
#
#         for row in rows:
#             # 1️⃣ Convert float fields safely
#             float_fields = ["bill_tax__c", "bill_grand_total__c", "Invoice_Amount__c"]
#             for f in float_fields:
#                 val = row.get(f)
#                 if isinstance(val, str):
#                     try:
#                         row[f] = float(val)
#                     except ValueError:
#                         row[f] = 0.0
#                 elif val is None:
#                     row[f] = 0.0
#
#             # Convert mobile numbers to int64
#             mobile_val = row.get("customer_mobile__c")
#             if isinstance(mobile_val, str):
#                 try:
#                     row["customer_mobile__c"] = int(mobile_val)
#                 except ValueError:
#                     row["customer_mobile__c"] = None
#
#             # Convert Item_Code__c to int64
#             item_val = row.get("Item_Code__c")
#             if isinstance(item_val, str):
#                 try:
#                     row["Item_Code__c"] = int(item_val)
#                 except ValueError:
#                     row["Item_Code__c"] = 0
#
#             # Convert date strings to Python `date` object (yyyy-mm-dd only)
#             for date_field in ["Bill_Date__c",  "CreatedDate"]:
#                 # print(date_field)
#                 val = row.get(date_field)
#
#                 if not val or str(val).strip() == "":
#                     row[date_field] = None
#                     continue
#
#                 try:
#                     # use auto parser
#                     dt = parser.parse(str(val))  # can parse both '6/24/2021 0:00' and '2021-06-24 00:00:00'
#                     row[date_field] = dt
#                 except Exception as e:
#                     print(f"⚠️ Error converting {date_field}: {val} ({e})")
#                     row[date_field] = None
#
#             converted_rows.append(row)
#
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
#         print(f"❌ {error_code}: {error_message}")
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
#     print(f"✅ Append completed in {append_end - append_start:.2f} sec")
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
#             # "total_time": round(total_end - total_start, 2),
#         },
#     }

# @router.post("/data/insert/month-none-sort")
# def r2_catalog(
#     start_range: int = Query(0, description="Start row offset for MySQL data fetch"),
#     end_range: int = Query(0, description="End row offset for MySQL data fetch"),
#     chunk_size: int = Query(100000, description="Chunk size for multithreading"),
# ):
#     total_start = time.time()
#     # namespace, table_name = "pos_transactions_with_out", "iceberg_out_partitioning"
#     namespace, table_name = "pos_transactions_month", "transaction_month"
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
#
#         for row in rows:
#             # 1️⃣ Convert float fields safely
#             float_fields = ["bill_tax__c", "bill_grand_total__c", "Invoice_Amount__c"]
#             for f in float_fields:
#                 val = row.get(f)
#                 if isinstance(val, str):
#                     try:
#                         row[f] = float(val)
#                     except ValueError:
#                         row[f] = 0.0
#                 elif val is None:
#                     row[f] = 0.0
#
#             # Convert mobile numbers to int64
#             mobile_val = row.get("customer_mobile__c")
#             if isinstance(mobile_val, str):
#                 try:
#                     row["customer_mobile__c"] = int(mobile_val)
#                 except ValueError:
#                     row["customer_mobile__c"] = None
#
#             # Convert Item_Code__c to int64
#             item_val = row.get("Item_Code__c")
#             if isinstance(item_val, str):
#                 try:
#                     row["Item_Code__c"] = int(item_val)
#                 except ValueError:
#                     row["Item_Code__c"] = 0
#
#             # Convert date strings to Python `date` object (yyyy-mm-dd only)
#             for date_field in ["Bill_Date__c",  "CreatedDate"]:
#                 # print(date_field)
#                 val = row.get(date_field)
#
#                 if not val or str(val).strip() == "":
#                     row[date_field] = None
#                     continue
#
#                 try:
#                     # use auto parser
#                     dt = parser.parse(str(val))  # can parse both '6/24/2021 0:00' and '2021-06-24 00:00:00'
#                     row[date_field] = dt
#                 except Exception as e:
#                     print(f"⚠️ Error converting {date_field}: {val} ({e})")
#                     row[date_field] = None
#
#                 if date_field == "Bill_Date__c":
#                     row["Bill_Date__month"] = dt.month
#
#             converted_rows.append(row)
#             print(converted_rows[0])
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
#         print(f"❌ {error_code}: {error_message}")
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
#     print(f"✅ Append completed in {append_end - append_start:.2f} sec")
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
#             # "total_time": round(total_end - total_start, 2),
#         },
#     }

# @router.get("/table-count")
# def table_count(
#     namespace: str = Query("pos_transactions"),
#     table_name: str = Query("iceberg_with_partitioning"),
#     bill_date: str | None = Query(None),
#     store_code: str | None = Query(None),
#     customer_mobile: str | None = Query(None)
# ):
#     import datetime
#     from pyiceberg.expressions import And, EqualTo
#
#     start = time.perf_counter()
#     table_identifier = f"{namespace}.{table_name}"
#     catalog = get_catalog_client()
#
#     try:
#         tbl = catalog.load_table(table_identifier)
#     except NoSuchTableError:
#         raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error loading table: {str(e)}")
#
#     expr = None
#     try:
#         if bill_date:
#             bill_date_parsed = datetime.datetime.strptime(bill_date, "%Y-%m-%d").date()
#             expr = EqualTo("Bill_Date__c", bill_date_parsed)
#
#         if store_code:
#             cond = EqualTo("store_code__c", store_code)
#             expr = cond if expr is None else And(expr, cond)
#
#         if customer_mobile:
#             cond = EqualTo("customer_mobile__c", int(customer_mobile))
#             expr = cond if expr is None else And(expr, cond)
#
#     except Exception as e:
#         raise HTTPException(status_code=400, detail=f"Invalid filter value: {str(e)}")
#
#     # fast count
#     try:
#         scan = tbl.scan(row_filter=expr) if expr else tbl.scan()
#         count_rows = scan.count()
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error reading count: {str(e)}")
#
#     return {
#         "namespace": namespace,
#         "table_name": table_name,
#         "filters": {
#             "bill_date": bill_date,
#             "store_code": store_code,
#             "customer_mobile": customer_mobile
#         },
#         "count": count_rows,
#         "seconds": round(time.perf_counter() - start, 4)
#     }

# @router.get("/iceberg/metadata-list")
# def get_metadata_list(
#     namespace: str = Query(...),
#     table_name: str = Query(...)
# ):
#     try:
#         catalog = get_catalog_client()
#         table_identifier = f"{namespace}.{table_name}"
#         tbl = catalog.load_table(table_identifier)
#         print(tbl.metadata.metadata_log)
#         logs = tbl.metadata.metadata_log
#
#         return {
#             "success": True,
#             "table": table_identifier,
#             "metadata_files": [
#                 {
#                     "metadata_file": x.metadata_file,
#                     "timestamp_ms": x.timestamp_ms
#                 }
#                 for x in logs
#             ]
#         }
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

# @router.get("/iceberg/avro-files")
# def iceberg_avro_files(
#     namespace: str = Query(...),
#     table_name: str = Query(...)
# ):
#     try:
#         catalog = get_catalog_client()
#         table_identifier = f"{namespace}.{table_name}"
#         tbl = catalog.load_table(table_identifier)
#         # # print(tbl.snapshots())
#         # result = []
#         # # print(tbl.metadata.avro_files)
#         # for snap in tbl.snapshots():
#         #     # print("snap",snap.manifests)
#         #     for manifest in snap.manifests():
#         #         print("manifest:")
#         #         try:
#         #             for df in manifest.fetch_data_files():
#         #                 print(df)
#         #                 # only include AVRO files, skip parquet
#         #                 if df.file_path.endswith(".avro"):
#         #                     result.append({
#         #                         "snapshot_id": snap.snapshot_id,
#         #                         "manifest": manifest.path,
#         #                         "avro_file": df.file_path,
#         #                         "record_count": df.record_count
#         #                     })
#         #         except Exception:
#         #             pass
#         result = []
#
#         for snap in tbl.snapshots:  # no ()
#             for manifest in snap.manifests:  # no ()
#                 try:
#                     for df in manifest.fetch_data_files():  # correct for 0.10.0
#                         if df.file_path.endswith(".avro"):
#                             result.append({
#                                 "snapshot_id": snap.snapshot_id,
#                                 "manifest": manifest.path,
#                                 "avro_file": df.file_path,
#                                 "record_count": df.record_count
#                             })
#                 except Exception:
#                     pass
#
#         return {
#             "success": True,
#             "table": table_identifier,
#             "total_avro_files": len(result),
#             "files": result
#         }

        # return {
        #     "success": True,
        #     "table": table_identifier,
        #     "total_avro_files": len(result),
        #     "files": result
        # }

    # except Exception as e:
    #     raise HTTPException(status_code=500, detail=str(e))




import gzip
import json
import pandas as pd
#
# with gzip.open("json_backups/05.metadata.json", "rb") as f:
#     data = json.loads(f.read().decode("utf-8"))   # decompress + decode
#
# df = pd.json_normalize(data)   # flatten into DataFrame
# # print(df.columns)
#
# # for col,index in df:
# #     print(col,index)
# print("start ...")
# for col in df.columns:
#
#     for idx in df.index:
#         print(col, idx, df.loc[idx, col])
#         print("*"*100)

# @router.get("/metadata-json-read")
# def read_metadata_json(
#     file_path: str = Query(..., description="path to metadata gz file, example: json_backups/05.metadata.json")
# ):
#     try:
#         with gzip.open(file_path, "rb") as f:
#             data = json.loads(f.read().decode("utf-8"))
#
#         df = pd.json_normalize(data)
#
#         # convert dataframe to list of dicts for JSON response
#         result = df.to_dict(orient="records")
#
#         return {
#             "success": True,
#             "rows": len(result),
#             "columns": list(df.columns),
#             "data": result
#         }
#
#     except FileNotFoundError:
#         raise HTTPException(status_code=404, detail="file not found")
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

# @router.get("/metadata-json-read")
# def read_metadata_json(s3_path: str):
#     # s3_path example:
#     # s3://dev-transaction/....json
#
#     if not s3_path.startswith("s3://"):
#         raise HTTPException(400, "must start with s3://")
#
#     parts = s3_path[5:].split("/", 1)
#     bucket = parts[0]
#     key = parts[1]
#
#     try:
#         obj = s3.get_object(Bucket=bucket, Key=key)
#         raw = obj["Body"].read()
#         data = json.loads(gzip.decompress(raw))   # decompress + parse
#
#         df = pd.json_normalize(data)
#         return df.to_dict(orient="records")
#
#     except Exception as e:
#         raise HTTPException(500, str(e))

# @router.get("/metadata-json-read")
# def read_metadata_json(
#     s3_path: str = Query(..., description="full s3 metadata gz path")
# ):
#     """
#     example:
#     /metadata-json-read?s3_path=s3://dev-transaction/.../metadata/00000-abc.gz.metadata.json
#     """
#     try:
#         parts = s3_path[5:].split("/", 1)
#         bucket = parts[0]
#         key = parts[1]
#
#         obj = s3.get_object(Bucket=bucket, Key=key)
#         raw = obj["Body"].read()
#         data = json.loads(gzip.decompress(raw))
#
#         df = pd.json_normalize(data)
#         return df.to_dict(orient="records")
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))
# import boto3
# s3 = boto3.client("s3")





