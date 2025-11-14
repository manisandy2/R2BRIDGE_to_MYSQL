from fastapi import APIRouter, HTTPException
import duckdb
from pyarrow.dataset import partitioning

from ...mysql_creds import *
from pyiceberg.schema import Schema
from pyiceberg.types import *
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform,YearTransform,MonthTransform,DayTransform,BucketTransform
from pyiceberg.catalog import NoSuchNamespaceError,NamespaceAlreadyExistsError,TableAlreadyExistsError,NoSuchTableError
from ...core.catalog_client import get_catalog_client
import traceback
import pyarrow as pa
from datetime import datetime, date
from fastapi import APIRouter,HTTPException,Query

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
    return iceberg_schema, arrow_schema

# def convert_column(row: dict, arrow_schema: pa.Schema) -> dict:
#     converted = {}
#
#     for field in arrow_schema:
#         name = field.name
#         dtype = field.type
#         val = row.get(name)
#         print("name", name, "dtype", dtype, "val", val)
#         # Handle None / Empty
#         if val in (None, "", "NULL"):
#             converted[name] = None
#             continue
#
#         # ---- Type-based Conversion ----
#         try:
#             # Integer
#             if pa.types.is_integer(dtype):
#                 converted[name] = int(val)
#
#             # Floating point
#             elif pa.types.is_floating(dtype):
#                 converted[name] = float(val)
#
#             # Boolean
#             elif pa.types.is_boolean(dtype):
#                 converted[name] = str(val).lower() in ("true", "1", "yes")
#
#             # Timestamp / Date
#             # elif pa.types.is_timestamp(dtype) or "date" in name.lower():
#             #     if isinstance(val, str):
#             #         val = datetime.fromisoformat(val[:19]) if len(val) >= 10 else None
#             #     if isinstance(val, datetime):
#             #         converted[name] = val.strftime("%Y-%m-%d %H:%M:%S")
#             #
#             #     else:
#             #         converted[name] = None
#                 # --- Date / Timestamp handling ---
#             elif pa.types.is_date(dtype) or pa.types.is_timestamp(dtype) or "date" in name.lower():
#                 # Case 1: Already a datetime/date object
#                 if isinstance(val, datetime):
#                     # Keep as datetime for timestamp64, or convert to date for date32
#                     if pa.types.is_timestamp(dtype):
#                         converted[name] = val
#                     else:
#                         converted[name] = val.date()
#
#                 elif isinstance(val, date):
#                     converted[name] = val
#
#                 # Case 2: String values from MySQL or CSV
#                 elif isinstance(val, str):
#                     val = val.strip()
#                     try:
#                         if " " in val:
#                             dt = datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
#                         else:
#                             dt = datetime.strptime(val, "%Y-%m-%d")
#
#                         if pa.types.is_timestamp(dtype):
#                             converted[name] = dt
#                         else:
#                             converted[name] = dt.date()
#                     except ValueError:
#                         converted[name] = None
#                 else:
#                     converted[name] = None
#
#             # String / Bytes
#             elif pa.types.is_string(dtype):
#                 converted[name] = str(val)
#
#             else:
#                 converted[name] = str(val)
#
#         except Exception as e:
#             # print("Failed to convert column", row)
#             #
#             print("name", name, "dtype", dtype, "val", val ,{e})
#             converted[name] = None
#
#     return converted

# @router.post("/with_out_partition")
# ###########################################################################



##########################################################################
@router.post("/manual-create-ph-table")
def create_transaction():
    """
    Create a predefined Iceberg table for transaction phone data
    with a static schema and partition spec.
    """
    namespace = "pos_transactions01"
    table_name = "transaction01"
    table_identifier = f"{namespace}.{table_name}"

    # Step 1: Define Iceberg schema
    transaction_schema = Schema(
        NestedField(1,"pri_id",LongType()),
        NestedField(2, "store_code__c", StringType()),
        NestedField(3, "Branch_Name__c", StringType()),
        NestedField(4, "customer_mobile__c", StringType()),
        NestedField(5, "Customer_Name__c", StringType()),
        NestedField(6, "Bill_No__c", StringType()),
        NestedField(7, "Bill_Date__c", StringType()),
        NestedField(8, "Invoice_Date__c", StringType()),
        NestedField(9, "Invoice_Amount__c", IntegerType()),
        NestedField(10, "bill_status__c", StringType()),
        NestedField(11, "bill_transaction_no__c", StringType()),
        NestedField(12, "Item_Code__c", StringType()),
        NestedField(13, "Item_Name__c", StringType()),
        NestedField(14, "bill_tax__c", StringType()),
        NestedField(15, "bill_grand_total__c", StringType()),
        NestedField(16, "CreatedDate", StringType()),
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
        PartitionField(
            source_id=transaction_schema.find_field("customer_mobile__c").field_id,
            field_id=2004,
            transform=IdentityTransform(),
            name="customer_mobile",
        ),
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
            },
        )
        print(f"Created Iceberg table: {table_identifier}")

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


DUCKDB_PATH = "data/pos_transactions.duckdb"  # You can customize the DB path

@router.post("/duckdb-create-ph-table")
def create_transaction_duckdb():
    """
    Create a predefined DuckDB table for transaction phone data
    with a static schema and partition-like columns.
    """
    namespace = "pos_transactions01"
    table_name = "transaction_duckdb"
    full_table_name = f"{namespace}_{table_name}"

    # Ensure data folder exists
    os.makedirs("data", exist_ok=True)

    try:
        # Step 1: Connect to DuckDB
        conn = duckdb.connect(DUCKDB_PATH)

        # Step 2: Create schema (namespace) if not exists
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {namespace};")

        # Step 3: Define table schema
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {namespace}.{table_name} (
            Bill_No__c TEXT,
            Bill_Date__c DATE,
            store_code__c TEXT,
            customer_mobile__c TEXT,
            Item_Code__c TEXT,
            Invoice_Amount__c DOUBLE,
            bill_tax__c DOUBLE,
            bill_grand_total__c DOUBLE,
            CreatedDate TIMESTAMP,
            -- partition-like columns
            day INTEGER GENERATED ALWAYS AS (EXTRACT(DAY FROM Bill_Date__c)) STORED,
            store_bucket INTEGER GENERATED ALWAYS AS (MOD(abs(hash(store_code__c)), 32)) STORED,
            customer_mobile TEXT
        );
        """
        conn.execute(create_table_query)
        conn.close()

        print(f"✅ Created DuckDB table: {namespace}.{table_name}")

        # Step 4: Return confirmation
        return {
            "status": "created",
            "database": DUCKDB_PATH,
            "table": f"{namespace}.{table_name}",
            "schema_fields": [
                "Bill_No__c",
                "Bill_Date__c",
                "store_code__c",
                "customer_mobile__c",
                "Item_Code__c",
                "Invoice_Amount__c",
                "bill_tax__c",
                "bill_grand_total__c",
                "CreatedDate",
                "day",
                "store_bucket",
                "customer_mobile"
            ]
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DuckDB table creation failed: {str(e)}")



# @router.post("/insert-ph-data")
# def insert_transaction_phone_data(
#     start_range: int = Query(0),
#     end_range: int = Query(100000),
# ):
#     """
#     Insert phone transaction data into Iceberg (with timing + automatic numeric conversion)
#     """
#     total_start = time.time()
#     mysql_creds = MysqlCatalog()
#     namespace, table_name = "pos_transactions01", "transaction_phone_in_con_sum"
#     dbname = "Transaction"
#
#     # --- Log setup ---
#     log_file = os.path.join(LOGS_FOLDER, f"insert_ph_data_{datetime.now():%Y%m%d_%H%M%S}.log")
#     def log_info(msg: str):
#         with open(log_file, "a") as f:
#             f.write(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}\n")
#     def log_error(msg: str):
#         with open(log_file, "a") as f:
#             f.write(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] [ERROR] {msg}\n")
#
#     # --- Step 1: Fetch data from MySQL ---
#     fetch_start = time.time()
#     try:
#         rows = mysql_creds.get_range_ph_bi(dbname, start_range, end_range)
#         # print(rows)
#         if not rows:
#             raise HTTPException(status_code=400, detail="No data found in range.")
#         log_info(f"MySQL Fetch: Retrieved {len(rows)} rows.")
#     except Exception as e:
#         log_error(f"MySQL fetch error: {traceback.format_exc()}")
#         raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")
#     fetch_end = time.time()
#
#     # --- Step 2: Schema inference ---
#     schema_start = time.time()
#     try:
#         iceberg_schema, arrow_schema = infer_schema_from_record(rows[0])
#         log_info("Schema inference completed successfully.")
#     except Exception as e:
#         log_error(f"Schema inference failed: {traceback.format_exc()}")
#         raise HTTPException(status_code=400, detail=f"Schema inference failed: {str(e)}")
#     schema_end = time.time()
#
#     # --- Step 3: Load Iceberg table ---
#     catalog_start = time.time()
#     catalog = get_catalog_client()
#     table_identifier = f"{namespace}.{table_name}"
#     try:
#         tbl = catalog.load_table(table_identifier)
#         log_info(f"Table loaded: {table_identifier}")
#     except NoSuchTableError:
#         log_error(f"Table not found: {table_identifier}")
#         raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")
#     catalog_end = time.time()
#
#     # --- Step 4: Convert data to Arrow table ---
#     arrow_start = time.time()
#     try:
#         columns_data = {field.name: [] for field in arrow_schema}
#         for row in rows:
#             for field in arrow_schema:
#                 val = row.get(field.name)
#                 # 🔹 Auto-convert numeric fields safely
#                 if pa.types.is_integer(field.type):
#                     try:
#                         val = int(val) if val not in (None, "") else None
#                     except Exception as e:
#                         print(f"Failed to convert {field.name} to integer: {e}")
#                         val = None
#                 elif pa.types.is_floating(field.type):
#                     try:
#                         val = float(val) if val not in (None, "") else None
#                     except Exception as e:
#                         print(f"Failed to convert {field.name} to float: {e}")
#                         val = None
#                 columns_data[field.name].append(val)
#
#         arrays = [pa.array(columns_data[f.name], type=f.type) for f in arrow_schema]
#         arrow_table = pa.Table.from_arrays(arrays, schema=arrow_schema)
#         log_info(f"Arrow table built successfully ({len(rows)} rows).")
#     except Exception as e:
#         log_error(f"Arrow table build failed: {traceback.format_exc()}")
#         raise HTTPException(status_code=500, detail=f"Arrow table build failed: {str(e)}")
#     arrow_end = time.time()
#
#     # --- Step 5: Append data to Iceberg ---
#     append_start = time.time()
#     try:
#         tbl.append(arrow_table)
#         log_info("Data appended to Iceberg successfully.")
#     except Exception as e:
#         log_error(f"Data append failed: {traceback.format_exc()}")
#         raise HTTPException(status_code=500, detail=f"Data append failed: {str(e)}")
#     append_end = time.time()
#
#     # --- Step 6: Refresh Iceberg table ---
#     refresh_start = time.time()
#     try:
#         tbl.refresh()
#         log_info("Table refreshed successfully.")
#     except Exception as e:
#         log_error(f"Table refresh failed: {traceback.format_exc()}")
#         raise HTTPException(status_code=500, detail=f"Table refresh failed: {str(e)}")
#     refresh_end = time.time()
#
#     # --- Step 7: Summary ---
#     total_end = time.time()
#     summary = {
#         "namespace": namespace,
#         "table": table_name,
#         "rows_written": len(rows),
#         "timing_seconds": {
#             "fetch_mysql": round(fetch_end - fetch_start, 2),
#             "schema_inference": round(schema_end - schema_start, 2),
#             "catalog_load": round(catalog_end - catalog_start, 2),
#             "arrow_build": round(arrow_end - arrow_start, 2),
#             "append_data": round(append_end - append_start, 2),
#             "refresh_table": round(refresh_end - refresh_start, 2),
#             "total_runtime": round(total_end - total_start, 2),
#         },
#
#     }
#
#
#     return summary

# @router.post("/insert-ph-direct-data")
# def insert_transaction_phone_data(
#     start_range: int = Query(0, description="Start row offset for MySQL data fetch"),
#     end_range: int = Query(100000, description="End row offset for MySQL data fetch"),
# ):
#
#     total_start = time.time()
#     namespace, table_name = "pos_transactions01", "transaction01"
#     dbname = "Transaction"
#     mysql_creds = MysqlCatalog()
#
#     try:
#         rows = mysql_creds.get_range_ph_bi(dbname, start_range, end_range)
#         print("Mysql Data")
#         print(rows[0])
#         if not rows:
#             raise HTTPException(status_code=400, detail="No data found in the given range.")
#
#     except Exception as e:
#
#         raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")
#
#
#
#
#     # print(converted_rows[0])
#     iceberg_schema, arrow_schema = infer_schema_from_record(rows[0])
#     print(f"Iceberg schema: {iceberg_schema}")
#     print(f"Arrow schema: {arrow_schema}")
#     # converted_records = [convert_column(r, arrow_schema) for r in rows]
#     # print(f"Converted records: {converted_records[0]}")
#
#     try:
#         arrow_table = pa.Table.from_pylist(rows, schema=arrow_schema)
#         print(f"Arrow table: {arrow_table}")
#     except pa.lib.ArrowTypeError as e:
#         # Debug row/field causing error
#         for row_idx, row in enumerate(rows):
#             for field in arrow_schema:
#                 val = row.get(field.name)
#                 try:
#                     pa.array([val], type=field.type)
#                 except Exception as field_e:
#                     print(f"Row {row_idx}, Field '{field.name}', Value: {val}, Type: {field.type}")
#                     print(f"  Error: {field_e}")
#         raise HTTPException(status_code=400, detail=f"Arrow conversion error: {str(e)}")
#
#     catalog_start = time.time()
#     catalog = get_catalog_client()
#     table_identifier = f"{namespace}.{table_name}"
#     try:
#         tbl = catalog.load_table(table_identifier)
#
#     except NoSuchTableError:
#
#         raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")
#     catalog_end = time.time()
#     try:
#         tbl.append(arrow_table)
#         tbl.refresh()
#
#     except Exception as e:
#
#         raise HTTPException(status_code=500, detail=f"Data append failed: {str(e)}")
#     # --- Step 6: Create table ---
#
#     return {
#         "success": True,
#         "message": "Data appended",
#         "data":rows
#         # "data": converted_records
#     }

# without multi threading
# @router.post("/insert-ph-direct-data")
# def insert_transaction_phone_data(
#     start_range: int = Query(0, description="Start row offset for MySQL data fetch"),
#     end_range: int = Query(100000, description="End row offset for MySQL data fetch"),
# ):
#
#     total_start = time.time()
#     namespace, table_name = "pos_transactions01", "transaction01"
#     dbname = "Transaction"
#     mysql_creds = MysqlCatalog()
#
#     # Step 1: Fetch from MySQL
#     mysql_start = time.time()
#     try:
#         rows = mysql_creds.get_range_ph_bi(dbname, start_range, end_range)
#         mysql_end = time.time()
#         print(f"MySQL fetch completed in {mysql_end - mysql_start:.2f} sec")
#         if not rows:
#             raise HTTPException(status_code=400, detail="No data found in the given range.")
#         # print(f"Sample MySQL Row: {rows[0]}")
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")
#
#     # Step 2: Infer Schema
#     schema_start = time.time()
#     iceberg_schema, arrow_schema = infer_schema_from_record(rows[0])
#     schema_end = time.time()
#     print(f"Schema inference completed in {schema_end - schema_start:.2f} sec")
#     # print(f"Iceberg schema: {iceberg_schema}")
#     # print(f"Arrow schema: {arrow_schema}")
#
#     # Step 3: Convert to Arrow Table
#     arrow_start = time.time()
#     try:
#         arrow_table = pa.Table.from_pylist(rows, schema=arrow_schema)
#         arrow_end = time.time()
#         print(f"Arrow table creation completed in {arrow_end - arrow_start:.2f} sec")
#     except pa.lib.ArrowTypeError as e:
#         for row_idx, row in enumerate(rows):
#             for field in arrow_schema:
#                 val = row.get(field.name)
#                 try:
#                     pa.array([val], type=field.type)
#                 except Exception as field_e:
#                     print(f"Row {row_idx}, Field '{field.name}', Value: {val}, Type: {field.type}")
#                     print(f"  Error: {field_e}")
#         raise HTTPException(status_code=400, detail=f"Arrow conversion error: {str(e)}")
#
#     # Step 4: Load Iceberg Table from Catalog
#     catalog_start = time.time()
#     catalog = get_catalog_client()
#     table_identifier = f"{namespace}.{table_name}"
#     try:
#         tbl = catalog.load_table(table_identifier)
#         catalog_end = time.time()
#         print(f"Catalog load completed in {catalog_end - catalog_start:.2f} sec")
#     except NoSuchTableError:
#         raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")
#
#     # Step 5: Append Data to Table
#     append_start = time.time()
#     try:
#         tbl.append(arrow_table)
#         # tbl.refresh()
#         append_end = time.time()
#         print(f"Data append + refresh completed in {append_end - append_start:.2f} sec")
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Data append failed: {str(e)}")
#
#     total_end = time.time()
#     print(f"Total execution time: {total_end - total_start:.2f} sec")
#
#     return {
#         "success": True,
#         "message": "Data appended",
#         "rows_fetched": len(rows),
#         "execution_times": {
#             "mysql_fetch": round(mysql_end - mysql_start, 2),
#             "schema_infer": round(schema_end - schema_start, 2),
#             "arrow_convert": round(arrow_end - arrow_start, 2),
#             "catalog_load": round(catalog_end - catalog_start, 2),
#             "append_refresh": round(append_end - append_start, 2),
#             "total_time": round(total_end - total_start, 2)
#         }
#     }
# def convert_column(row: dict, arrow_schema: pa.Schema) -> dict:
#     converted = {}
#
#     for field in arrow_schema:
#         name = field.name
#         dtype = field.type
#         val = row.get(name)
#         # print("name", name, "dtype", dtype, "val", val)
#         # Handle None / Empty
#         if val in (None, "", "NULL"):
#             converted[name] = None
#             continue
#
#         # ---- Type-based Conversion ----
#         try:
#             # Integer
#             if pa.types.is_integer(dtype):
#                 converted[name] = int(val)
#
#             # Floating point
#             elif pa.types.is_floating(dtype):
#                 converted[name] = float(val)
#
#             # Boolean
#             elif pa.types.is_boolean(dtype):
#                 converted[name] = str(val).lower() in ("true", "1", "yes")
#
#             # Timestamp / Date
#             elif pa.types.is_timestamp(dtype) or "date" in name.lower():
#                 if isinstance(val, str):
#                     val = datetime.fromisoformat(val[:19]) if len(val) >= 10 else None
#                 if isinstance(val, datetime):
#                     converted[name] = val.strftime("%Y-%m-%d %H:%M:%S")
#                     converted[f"{name}_year"] = val.year
#                     converted[f"{name}_month"] = val.month
#                     converted[f"{name}_day"] = val.day
#                 else:
#                     converted[name] = None
#
#             # String / Bytes
#             elif pa.types.is_string(dtype):
#                 converted[name] = str(val)
#
#             else:
#                 converted[name] = str(val)
#
#         except Exception as e:
#             # print("Failed to convert column", row)
#             #
#             print("name", name, "dtype", dtype, "val", val ,{e})
#             converted[name] = None
#
#     return converted

def convert_column(row: dict[str, Any], arrow_schema: pa.Schema) -> dict[str, Any]:

    converted = {}

    for field in arrow_schema:
        name = field.name
        dtype = field.type
        val = row.get(name)

        # Skip empty or null-like values
        if val in (None, "", "NULL"):
            converted[name] = None
            continue

        try:
            # --- Integer ---
            if pa.types.is_integer(dtype):
                converted[name] = int(val)

            # --- Float ---
            elif pa.types.is_floating(dtype):
                converted[name] = float(val)

            # --- Boolean ---
            elif pa.types.is_boolean(dtype):
                converted[name] = str(val).lower() in ("true", "1", "yes")

            # --- Timestamp or Date ---
            elif pa.types.is_timestamp(dtype) or "date" in name.lower():
                parsed_dt = None
                if isinstance(val, datetime):
                    parsed_dt = val
                elif isinstance(val, str):
                    try:
                        parsed_dt = datetime.fromisoformat(val[:19])
                    except ValueError:
                        # Try fallback parsing for MySQL-style timestamps
                        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                            try:
                                parsed_dt = datetime.strptime(val, fmt)
                                break
                            except ValueError:
                                continue

                if parsed_dt:
                    converted[name] = parsed_dt.strftime("%Y-%m-%d %H:%M:%S")
                    converted[f"{name}_year"] = parsed_dt.year
                    converted[f"{name}_month"] = parsed_dt.month
                    converted[f"{name}_day"] = parsed_dt.day
                else:
                    converted[name] = None

            # --- String ---
            elif pa.types.is_string(dtype):
                converted[name] = str(val)

            # --- Fallback for any other type ---
            else:
                converted[name] = str(val)

        except Exception as e:
            print(f"[WARN] Conversion failed: column={name}, type={dtype}, value={val}, error={e}")
            converted[name] = None

    return converted



def process_chunk(chunk, arrow_schema):
    """Convert a chunk of rows to an Arrow table."""
    cleaned_chunk = convert_chunk_rows(chunk, arrow_schema)
    return pa.Table.from_pylist(cleaned_chunk, schema=arrow_schema)

def convert_chunk_rows(rows: list[dict], arrow_schema: pa.Schema) -> list[dict]:
    """Ensure all row values match expected Arrow schema types."""
    cleaned = []
    for row in rows:
        new_row = {}
        for field in arrow_schema:
            name = field.name
            dtype = field.type
            val = row.get(name)

            if val in (None, "", "NULL"):
                new_row[name] = None
                continue

            try:
                # Handle integer
                if pa.types.is_integer(dtype):
                    new_row[name] = int(val)

                # Handle float
                elif pa.types.is_floating(dtype):
                    new_row[name] = float(val)

                # Handle boolean
                elif pa.types.is_boolean(dtype):
                    new_row[name] = str(val).lower() in ("1", "true", "yes")

                # Handle timestamp or date
                elif pa.types.is_timestamp(dtype) or "date" in name.lower():
                    if isinstance(val, str):
                        try:
                            new_row[name] = datetime.strptime(val[:19], "%Y-%m-%d %H:%M:%S")
                        except Exception:
                            new_row[name] = datetime.strptime(val[:10], "%Y-%m-%d")
                    else:
                        new_row[name] = val

                # Handle string
                elif pa.types.is_string(dtype):
                    new_row[name] = str(val)

                else:
                    new_row[name] = str(val)

            except Exception:
                # Fallback if type conversion fails
                new_row[name] = None
        cleaned.append(new_row)
    return cleaned



@router.post("/insert-ph-direct-data")
def insert_transaction_phone_data(
    start_range: int = Query(0, description="Start row offset for MySQL data fetch"),
    end_range: int = Query(100000, description="End row offset for MySQL data fetch"),
    chunk_size: int = Query(10000, description="Chunk size for multithreading")
):
    total_start = time.time()
    namespace, table_name = "pos_transactions01", "transaction01"
    # namespace, table_name = "pos_transactions01", "transaction_with_in"
    # namespace, table_name = "pos_transactions01", "transaction_with_out"
    dbname = "Transaction"
    mysql_creds = MysqlCatalog()

    # Step 1: Fetch from MySQL
    mysql_start = time.time()
    try:
        rows = mysql_creds.get_range_ph_bi(dbname, start_range, end_range)
        print(rows)
        mysql_end = time.time()
        print(f"MySQL fetch completed in {mysql_end - mysql_start:.2f} sec with {len(rows)} rows.")
        if not rows:
            raise HTTPException(status_code=400, detail="No data found in the given range.")
        # print(f"Sample MySQL Row: {rows[0]}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")

    # Step 2: Infer Schema
    schema_start = time.time()
    iceberg_schema, arrow_schema = infer_schema_from_record(rows[0])
    converted_records = [convert_column(r, arrow_schema) for r in rows]
    schema_end = time.time()
    print(f"Schema inference completed in {schema_end - schema_start:.2f} sec")

    # Step 3: Convert rows → Arrow tables using multithreading
    arrow_start = time.time()
    chunks = [rows[i:i + chunk_size] for i in range(0, len(rows), chunk_size)]
    arrow_tables = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_chunk, chunk, arrow_schema): idx for idx, chunk in enumerate(chunks)}
        for future in as_completed(futures):
            idx = futures[future]
            try:
                tbl = future.result()
                arrow_tables.append(tbl)
                print(f"Chunk {idx+1}/{len(chunks)} processed with {tbl.num_rows} rows")
            except Exception as e:
                print(f"Chunk {idx+1} failed: {e}")
                raise HTTPException(status_code=500, detail=f"Arrow chunk conversion failed: {e}")
    arrow_end = time.time()
    print(f"All chunks converted to Arrow tables in {arrow_end - arrow_start:.2f} sec")

    # Combine all Arrow tables into one
    combined_table = pa.concat_tables(arrow_tables)
    print(f"Combined Arrow table rows: {combined_table.num_rows}")

    # Step 4: Load Iceberg Table from Catalog
    catalog_start = time.time()
    catalog = get_catalog_client()
    table_identifier = f"{namespace}.{table_name}"
    try:
        tbl = catalog.load_table(table_identifier)
        catalog_end = time.time()
        print(f"Catalog load completed in {catalog_end - catalog_start:.2f} sec")
    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")

    # Step 5: Append data
    append_start = time.time()
    try:
        tbl.append(combined_table)
        tbl.refresh()
        append_end = time.time()
        print(f"Data append + refresh completed in {append_end - append_start:.2f} sec")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Data append failed: {str(e)}")

    total_end = time.time()
    print(f"Total execution time: {total_end - total_start:.2f} sec")

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
            "total_time": round(total_end - total_start, 2)
        }
    }