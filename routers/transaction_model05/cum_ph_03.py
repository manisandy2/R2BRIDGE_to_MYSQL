from fastapi import APIRouter, HTTPException
import duckdb
from pyarrow.dataset import partitioning
from fastapi.encoders import jsonable_encoder
from ...mysql_creds import *
from pyiceberg.schema import Schema
from pyiceberg.types import *
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

LOGS_FOLDER = "logs/iceberg_upload"
os.makedirs(LOGS_FOLDER, exist_ok=True)

router = APIRouter(prefix="", tags=["Transaction version01"])

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
    print("Ice Berg Schema",iceberg_schema)
    print("Arrow Schema",arrow_schema)
    return iceberg_schema, arrow_schema

##########################################################################
@router.post("/manual-create-ph-table")
def create_transaction():
    namespace = "pos_transactions01"
    table_name = "transaction01"
    table_identifier = f"{namespace}.{table_name}"

    # Step 1: Define Iceberg schema
    transaction_schema = Schema(
        NestedField(1,"pri_id",LongType(),required=True),
        NestedField(2, "store_code__c", StringType()),
        NestedField(3, "Branch_Name__c", StringType()),
        NestedField(4, "customer_mobile__c", LongType()),
        NestedField(5, "Customer_Name__c", StringType()),
        NestedField(6, "Bill_No__c", StringType()),
        NestedField(7, "Bill_Date__c", DateType()),
        NestedField(8, "Invoice_Date__c", StringType()),
        NestedField(9, "Invoice_Amount__c", DoubleType()),
        NestedField(10, "bill_status__c", StringType()),
        NestedField(11, "bill_transaction_no__c", StringType()),
        NestedField(12, "Item_Code__c", LongType()),
        NestedField(13, "Item_Name__c", StringType()),
        NestedField(14, "bill_tax__c", DoubleType()),
        NestedField(15, "bill_grand_total__c", DoubleType()),
        NestedField(16, "CreatedDate", DateType()),
    )


    # Step 2: Define partition spec
    transaction_partition_spec = PartitionSpec(
        PartitionField(
            source_id=transaction_schema.find_field("Bill_Date__c").field_id,
            field_id=2001,
            transform=DayTransform(),
            name="day",
        ),

        PartitionField(
            source_id=transaction_schema.find_field("store_code__c").field_id,
            field_id=2002,
            transform=BucketTransform(32),
            name="store_bucket",
        ),
        # PartitionField(
        #     source_id=transaction_schema.find_field("customer_mobile__c").field_id,
        #     field_id=2004,
        #     transform=IdentityTransform(),
        #     name="customer_mobile",
        # ),
    )

    # Step 3: Connect to catalog
    catalog = get_catalog_client()

    # Step 4: Ensure namespace exists
    try:
        catalog.load_namespace_properties(namespace)
    except NoSuchNamespaceError:
        catalog.create_namespace(namespace)
    except NamespaceAlreadyExistsError:
        pass

    # Step 5: Create table
    try:
        tbl = catalog.create_table(
            identifier=table_identifier,
            schema=transaction_schema,
            partition_spec=transaction_partition_spec,
            properties={
                "write.format.default": "parquet",
                "write.parquet.compression-codec": "zstd",
                "write.partition.path-style": "directory",
                "write.target-file-size-bytes": "268435456"
            },
        )
        print(f"✅ Created Iceberg table: {table_identifier}")

        # Step 6: Return confirmation
        return {
            "status": "created",
            "table": table_identifier,
            "schema_fields": [f.name for f in transaction_schema.fields],
            "partitions": [f.name for f in transaction_partition_spec.fields],
        }

    except TableAlreadyExistsError:
        return {"status": "exists", "table": table_identifier}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Table creation failed: {str(e)}")



def process_chunk(chunk, arrow_schema):
    processed_rows = []
    date_formats = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y")

    for row_idx, row in enumerate(chunk):
        converted_row = {}
        print(f"🧩 Processing row {row_idx} -> keys: {list(row.keys())}")

        for field in arrow_schema:
            val = row.get(field.name, None)

            # ✅ Debug mismatched field
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
                        print(f"⚠️ Row {row_idx}: Unrecognized date in '{field.name}': {val}")
                        converted_row[field.name] = None

                # --- Default: keep as string or object ---
                else:
                    converted_row[field.name] = val

            except Exception as e:
                print(f"❌ Row {row_idx}, Field '{field.name}', Value: {val}, Error: {e}")
                converted_row[field.name] = None

        processed_rows.append(converted_row)

    # ✅ Debug before conversion
    print("✅ Example converted_row:", processed_rows[0] if processed_rows else "EMPTY")

    return pa.Table.from_pylist(processed_rows, schema=arrow_schema)




# with out multithreading

@router.post("/insert-ph-direct-data")
def insert_transaction_phone_data(
    start_range: int = Query(0, description="Start row offset for MySQL data fetch"),
    end_range: int = Query(100000, description="End row offset for MySQL data fetch"),
    chunk_size: int = Query(10000, description="Chunk size for multithreading"),
):
    total_start = time.time()
    namespace, table_name = "pos_transactions01", "transaction01"
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
        print("before ...")
        print("rows:",rows)

        for row in rows:
            # 1️⃣ Convert float fields safely
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
            for date_field in ["Bill_Date__c", "Invoice_Date__c", "CreatedDate"]:
                val = row.get(date_field)
                if val in ("", " ", None):
                    row[date_field] = None
                    continue

                try:
                    if isinstance(val, str):
                        val = val.strip()
                        # handle both with and without time part
                        if " " in val:
                            # date_obj = datetime.strptime(val, "%Y-%m-%d %H:%M:%S").date()
                            row[date_field] = datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
                        else:
                            # date_obj = datetime.strptime(val, "%Y-%m-%d").date()
                            row[date_field] = datetime.strptime(val, "%Y-%m-%d")
                        # row[date_field] = date_obj
                    elif isinstance(val, datetime):
                        row[date_field] = val
                except Exception as e:
                    print(f"⚠️ Error converting {date_field}: {val} ({e})")
                    row[date_field] = None

            converted_rows.append(row)

        print("After ...")
        print("rows:",converted_rows)

        mysql_end = time.time()
        print(f"MySQL fetch completed in {mysql_end - mysql_start:.2f} sec ({len(rows)} rows).")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")

    # -------------------------------------------------
    # Step 2: Infer Iceberg + Arrow Schema
    # -------------------------------------------------
    schema_start = time.time()
    iceberg_schema, arrow_schema = infer_schema_from_record(rows[0])
    print("iceberg_schema",iceberg_schema)
    print("arrow_schema",arrow_schema)

    schema_end = time.time()
    print(f"Schema inference completed in {schema_end - schema_start:.2f} sec")

    # -------------------------------------------------
    # Step 3: Convert Rows to Arrow Tables (Multithreaded)
    # -------------------------------------------------
    arrow_start = time.time()
    chunks = [converted_rows[i:i + chunk_size] for i in range(0, len(converted_rows), chunk_size)]

    print("chunks",chunks)
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

    # Correctly combine Arrow tables
    combined_table = pa.concat_tables(arrow_tables)
    print(f"Combined Arrow table rows: {combined_table.num_rows}")


    arrow_end = time.time()
    print(f"Arrow conversion completed in {arrow_end - arrow_start:.2f} sec")

    # -------------------------------------------------
    # Step 4: Load Iceberg Table
    # -------------------------------------------------
    catalog_start = time.time()
    catalog = get_catalog_client()
    table_identifier = f"{namespace}.{table_name}"
    print(f"catalog table_identifier: {table_identifier}")
    try:
        tbl = catalog.load_table(table_identifier)
        catalog_end = time.time()
        print(f"Catalog load completed in {catalog_end - catalog_start:.2f} sec")
    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")

    # -------------------------------------------------
    # Step 5: Append Data to Iceberg
    # -------------------------------------------------
    append_start = time.time()


    print(combined_table.schema)
    print(combined_table.to_pandas().head(1))

    try:
        print(f"🟢 Append table start: {table_identifier}")
        tbl.append(combined_table)
        tbl.refresh()
        append_end = time.time()
        print(f"✅ Append table end: {table_identifier}")
        print(f"Data append + refresh completed in {append_end - append_start:.2f} sec")

    except Exception as e:
        error_message = str(e)

        # Define custom error codes for clarity
        if "more columns" in error_message and "Update the schema" in error_message:
            error_code = "ICEBERG_SCHEMA_MISMATCH"
        elif "catalog" in error_message.lower():
            error_code = "ICEBERG_CATALOG_ERROR"
        elif "permission" in error_message.lower():
            error_code = "ICEBERG_PERMISSION_DENIED"
        elif "connection" in error_message.lower():
            error_code = "ICEBERG_CONNECTION_FAILED"
        elif "not found" in error_message.lower():
            error_code = "ICEBERG_TABLE_NOT_FOUND"
        else:
            error_code = "ICEBERG_APPEND_FAILED"

        print(f"❌ [ERROR CODE: {error_code}] Failed to append table {table_identifier}: {error_message}")
        raise HTTPException(
            status_code=500,
            detail={
                "error_code": error_code,
                "message": f"Data append failed for table {table_identifier}",
                "exception": error_message,
            },
        )

    total_end = time.time()
    print(f"✅ Total execution time: {total_end - total_start:.2f} sec")

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
            "total_time": round(total_end - total_start, 2),
        },
    }

# with in multithreading
# @router.post("/insert-ph-direct-data")
# def insert_transaction_phone_data(
#     start_range: int = Query(0, description="Start row offset for MySQL data fetch"),
#     end_range: int = Query(100000, description="End row offset for MySQL data fetch"),
#     chunk_size: int = Query(10000, description="Chunk size for parallel processing"),
# ):
#     total_start = time.time()
#     namespace, table_name = "pos_transactions01", "transaction01"
#     dbname = "Transaction"
#     mysql_creds = MysqlCatalog()
#     catalog = get_catalog_client()
#     table_identifier = f"{namespace}.{table_name}"
#
#     # -------------------------------------------------
#     # 1️⃣ Parallel MySQL Fetch
#     # -------------------------------------------------
#     def fetch_chunk(start, end):
#         return mysql_creds.get_range_ph_bi(dbname, start, end)
#
#     print("🟢 Fetching MySQL data in parallel...")
#     mysql_start = time.time()
#     ranges = [(i, min(i + chunk_size, end_range)) for i in range(start_range, end_range, chunk_size)]
#
#     rows = []
#     with ThreadPoolExecutor(max_workers=8) as executor:
#         futures = [executor.submit(fetch_chunk, s, e) for s, e in ranges]
#         for f in as_completed(futures):
#             chunk = f.result()
#             if chunk:
#                 rows.extend(chunk)
#
#     if not rows:
#         raise HTTPException(status_code=400, detail="No data found in the given range.")
#
#     mysql_end = time.time()
#     print(f"✅ MySQL fetch completed in {mysql_end - mysql_start:.2f} sec ({len(rows)} rows)")
#
#     # -------------------------------------------------
#     # 2️⃣ Data Conversion
#     # -------------------------------------------------
#     def convert_row(row):
#         float_fields = ["bill_tax__c", "bill_grand_total__c", "Invoice_Amount__c"]
#         for f in float_fields:
#             val = row.get(f)
#             if isinstance(val, str):
#                 try:
#                     row[f] = float(val)
#                 except ValueError:
#                     row[f] = 0.0
#             elif val is None:
#                 row[f] = 0.0
#
#         # Mobile number
#         mobile_val = row.get("customer_mobile__c")
#         if isinstance(mobile_val, str):
#             try:
#                 row["customer_mobile__c"] = int(mobile_val)
#             except ValueError:
#                 row["customer_mobile__c"] = None
#
#         # Item code
#         item_val = row.get("Item_Code__c")
#         if isinstance(item_val, str):
#             try:
#                 row["Item_Code__c"] = int(item_val)
#             except ValueError:
#                 row["Item_Code__c"] = 0
#
#         # Date fields
#         for field in ["Bill_Date__c", "Invoice_Date__c", "CreatedDate"]:
#             val = row.get(field)
#             if val in ("", " ", None):
#                 row[field] = None
#                 continue
#             try:
#                 if isinstance(val, str):
#                     val = val.strip()
#                     if " " in val:
#                         row[field] = datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
#                     else:
#                         row[field] = datetime.strptime(val, "%Y-%m-%d")
#             except Exception:
#                 row[field] = None
#         return row
#
#     print("🟢 Converting rows...")
#     with ThreadPoolExecutor(max_workers=2) as executor:
#         converted_rows = list(executor.map(convert_row, rows))
#     print(f"✅ Data conversion completed for {len(converted_rows)} rows")
#
#     # -------------------------------------------------
#     # 3️⃣ Infer Schema
#     # -------------------------------------------------
#     schema_start = time.time()
#     iceberg_schema, arrow_schema = infer_schema_from_record(converted_rows[0])
#     schema_end = time.time()
#     print(f"✅ Schema inference completed in {schema_end - schema_start:.2f} sec")
#
#     # -------------------------------------------------
#     # 4️⃣ Convert to Arrow Tables (Multithreaded)
#     # -------------------------------------------------
#     print("🟢 Creating Arrow tables...")
#     arrow_start = time.time()
#     chunks = [converted_rows[i:i + chunk_size] for i in range(0, len(converted_rows), chunk_size)]
#
#     def process_chunk_safe(chunk):
#         return process_chunk(chunk, arrow_schema)
#
#     arrow_tables = []
#     with ThreadPoolExecutor(max_workers=2) as executor:
#         futures = {executor.submit(process_chunk_safe, chunk): idx for idx, chunk in enumerate(chunks)}
#         for f in as_completed(futures):
#             idx = futures[f]
#             try:
#                 tbl = f.result()
#                 arrow_tables.append(tbl)
#                 print(f"✅ Chunk {idx + 1}/{len(chunks)} processed ({tbl.num_rows} rows)")
#             except Exception as e:
#                 print(f"❌ Chunk {idx + 1} failed: {e}")
#
#     combined_table = pa.concat_tables(arrow_tables)
#     arrow_end = time.time()
#     print(f"✅ Arrow conversion completed in {arrow_end - arrow_start:.2f} sec")
#
#     # -------------------------------------------------
#     # 5️⃣ Load Iceberg Table
#     # -------------------------------------------------
#     print(f"🟢 Loading Iceberg table: {table_identifier}")
#     try:
#         tbl = catalog.load_table(table_identifier)
#         print("✅ Iceberg table loaded successfully")
#     except NoSuchTableError:
#         raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")
#
#     # -------------------------------------------------
#     # 6️⃣ Parallel Append to Iceberg
#     # -------------------------------------------------
#     print("🟢 Appending data in parallel to Iceberg...")
#     append_start = time.time()
#
#     def append_chunk(chunk_table, idx):
#         try:
#             tbl.append(chunk_table)
#             print(f"✅ Chunk {idx} appended successfully")
#         except Exception as e:
#             print(f"❌ Append failed for chunk {idx}: {e}")
#
#     arrow_chunks = [
#         combined_table.slice(i, min(chunk_size, combined_table.num_rows - i))
#         for i in range(0, combined_table.num_rows, chunk_size)
#     ]
#
#     with ThreadPoolExecutor(max_workers=5) as executor:
#         futures = [executor.submit(append_chunk, c, i) for i, c in enumerate(arrow_chunks)]
#         for f in as_completed(futures):
#             f.result()
#
#     tbl.refresh()
#     append_end = time.time()
#     print(f"✅ Parallel append completed in {append_end - append_start:.2f} sec")
#
#     # -------------------------------------------------
#     # 7️⃣ Return Result
#     # -------------------------------------------------
#     total_end = time.time()
#     print(f"✅ Total execution time: {total_end - total_start:.2f} sec")
#
#     return {
#         "success": True,
#         "message": "Data appended successfully with multithreading",
#         "rows_fetched": len(rows),
#         "chunks": len(chunks),
#         "execution_times": {
#             "mysql_fetch": round(mysql_end - mysql_start, 2),
#             "schema_infer": round(schema_end - schema_start, 2),
#             "arrow_convert": round(arrow_end - arrow_start, 2),
#             "append_refresh": round(append_end - append_start, 2),
#             "total_time": round(total_end - total_start, 2),
#         },
#     }

# from pyiceberg.expressions import And, EqualTo
# @router.get("/inspect-ph-table")
# def inspect_transaction_table(
#     namespace: str = Query("pos_transactions01", description="Iceberg namespace name"),
#     table_name: str = Query("transaction01", description="Iceberg table name"),
#     bill_date: str | None = Query(None, description="Filter by Bill_Date__c (YYYY-MM-DD)"),
#     store_code: str | None = Query(None, description="Filter by store_code__c"),
#     customer_mobile: str | None = Query(None, description="Filter by customer_mobile__c")
# ):
#     """
#     Inspect an existing Iceberg table's metadata.
#     Optionally filter by partition values (bill_date, store_code, customer_mobile).
#     """
#     import datetime
#
#     table_identifier = f"{namespace}.{table_name}"
#     catalog = get_catalog_client()
#
#     try:
#         tbl = catalog.load_table(table_identifier)
#
#     except NoSuchTableError:
#         raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error loading table: {str(e)}")
#
#     # Build filter expressions dynamically
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
#     except Exception as e:
#         raise HTTPException(status_code=400, detail=f"Invalid filter value: {str(e)}")
#
#     try:
#         # scan = tbl.scan(row_filter=expr) if expr else tbl.scan()
#         scan = tbl.scan(row_filter=expr) if expr else tbl.scan()
#
#         # print("data",scan)
#         # arrow_table = scan.to_arrow()
#         # # print("arrow_table",arrow_table)
#         # df = arrow_table.to_pandas().reset_index()
#         # print(df)
#         arrow_table = scan.to_arrow()
#         df = arrow_table.to_pandas().reset_index(drop=True)
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error reading data: {str(e)}")
#
#     return {
#         # "table_identifier": table_identifier,
#         # "row_count": len(df),
#         "count": len(df),
#         "sample_rows": df.head(10).to_dict(orient="records"),  # show first 10 rows only
#
#     }

###################################################
#     Filter
@router.get("/inspect-ph-table")
def inspect_transaction_table(
    namespace: str = Query("pos_transactions01", description="Iceberg namespace name"),
    table_name: str = Query("transaction01", description="Iceberg table name"),
    bill_date: str | None = Query(None, description="Filter by Bill_Date__c (YYYY-MM-DD)"),
    store_code: str | None = Query(None, description="Filter by store_code__c"),
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
    try:
        if bill_date:
            bill_date_parsed = datetime.datetime.strptime(bill_date, "%Y-%m-%d").date()
            expr = EqualTo("Bill_Date__c", bill_date_parsed)

        if store_code:
            cond = EqualTo("store_code__c", store_code)
            expr = cond if expr is None else And(expr, cond)

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
            "store_code": store_code,
            "customer_mobile": customer_mobile
        },
        "count": len(df),
        "sample_rows": df.head(10).to_dict(orient="records"),
        "timeline_seconds": total_time
    }