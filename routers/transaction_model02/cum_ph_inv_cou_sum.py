from fastapi import APIRouter,HTTPException,Query,Body
import time

# from ..utility import namespace
from ...mysql_creds import *
from datetime import datetime
# from ...mapping import *
from ...core.catalog_client import get_catalog_client
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform,YearTransform,MonthTransform,DayTransform,BucketTransform
from pyiceberg.exceptions import BadRequestError
from pyiceberg.expressions import EqualTo,And,GreaterThanOrEqual,LessThanOrEqual,In
from pyiceberg.exceptions import  BadRequestError
import time,logging
import pyarrow as pa
from pyiceberg.types import *
from pyiceberg.schema import Schema

logger = logging.getLogger(__name__)

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

    for idx, (name, value) in enumerate(record.items(), start=1):


        if name in ("pri_id",):
            ice_type = LongType()
            arrow_type = pa.int64()
            required = True
        elif name in ("Invoice_Amount__c"):
            ice_type = LongType()
            arrow_type = pa.int64()
            required = False

        else:
            ice_type = StringType()
            arrow_type = pa.string()
            required = False

            # Add field once
        iceberg_fields.append(
            NestedField(field_id=idx, name=name, field_type=ice_type, required=required)
        )
        arrow_fields.append(pa.field(name, arrow_type, nullable=not required))

    iceberg_schema = Schema(*iceberg_fields)
    arrow_schema = pa.schema(arrow_fields)
    return iceberg_schema, arrow_schema


def convert_column(row: dict, arrow_schema: pa.Schema) -> dict:
    converted = {}

    for field in arrow_schema:
        name = field.name
        dtype = field.type
        val = row.get(name)
        # print("name", name, "dtype", dtype, "val", val)
        # Handle None / Empty
        if val in (None, "", "NULL"):
            converted[name] = None
            continue

        # ---- Type-based Conversion ----
        try:
            # Integer
            if pa.types.is_integer(dtype):
                converted[name] = int(val)

            # Floating point
            elif pa.types.is_floating(dtype):
                converted[name] = float(val)

            # Boolean
            elif pa.types.is_boolean(dtype):
                converted[name] = str(val).lower() in ("true", "1", "yes")

            # Timestamp / Date
            elif pa.types.is_timestamp(dtype) or "date" in name.lower():
                if isinstance(val, str):
                    val = datetime.fromisoformat(val[:19]) if len(val) >= 10 else None
                if isinstance(val, datetime):
                    converted[name] = val.strftime("%Y-%m-%d %H:%M:%S")
                    converted[f"{name}_year"] = val.year
                    converted[f"{name}_month"] = val.month
                    converted[f"{name}_day"] = val.day
                else:
                    converted[name] = None

            # String / Bytes
            elif pa.types.is_string(dtype):
                converted[name] = str(val)

            else:
                converted[name] = str(val)

        except Exception as e:
            # print("Failed to convert column", row)
            #
            print("name", name, "dtype", dtype, "val", val ,{e})
            converted[name] = None

    return converted

router = APIRouter(prefix="", tags=["Transaction phone invoice count and sum"])

# @router.post("/create-ph")
# def transaction(
#     start_range: int = Query(0, description="Start row (e.g. 0)"),
#     end_range: int = Query(100000, description="End row (e.g. 100000)"),
# ):
#     total_start = time.time()
#     mysql_creds = MysqlCatalog()  # Your MySQL wrapper
#
#     namespace, table_name = "pos_transactions01", "transaction_phone_in_con_sum"
#     # dbname = "Transaction_pos"
#     dbname = "Transaction"
#
#     # ---------- Fetch data from MySQL ----------
#     print("Transaction Table name 01 phone count")
#     db_fetch_start = time.time()
#     try:
#         rows = mysql_creds.get_range_ph_bi(dbname, start_range, end_range)
#         print(rows)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")
#     # print("MySQL fetch", time.time() - db_fetch_start)
#     if not rows:
#         raise HTTPException(status_code=400, detail="No data found in the given range.")
#
#     # def safe_parse_date(value):
#     #     """Try multiple formats or datetime types for Bill_Date__c"""
#     #     if isinstance(value, datetime):
#     #         return value  # already a datetime object
#     #     if not value:
#     #         return None
#     #
#     #     # Try common string formats
#     #     for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d"):
#     #         try:
#     #             return datetime.strptime(value[:10], fmt)
#     #         except Exception as e:
#     #             print(f"Failed to parse {fmt} from {value}{e}")
#     #
#     #             continue
#     #     return None  # fallback if nothing matches
#
#     # ---------- Convert Bill_Date__c to timestamp ----------
#     convert_start = time.time()
#     # converted_rows = []
#     # for row in rows:
#     #     bill_date = row.get("Bill_Date__c","")
#     #     dt = safe_parse_date(bill_date)
#     #     if dt:
#     #         row["year"] = dt.year
#     #         row["month"] = dt.month
#     #         row["day"] = dt.day
#     #     else:
#     #         row["year"] = row["month"] = row["day"] = None  # fallback for invalid date
#     #
#     #     converted_rows.append(row)
#
#     # ---------- Infer Iceberg / Arrow schema ----------
#     iceberg_schema, arrow_schema = infer_schema_from_record(rows[0])
#
#     converted_records = [convert_column(r, arrow_schema) for r in rows]
#
#     try:
#         arrow_table = pa.Table.from_pylist(converted_records, schema=arrow_schema)
#     except pa.lib.ArrowTypeError as e:
#         # Debug row/field causing error
#         for row_idx, row in enumerate(converted_records):
#             for field in arrow_schema:
#                 val = row.get(field.name)
#                 try:
#                     pa.array([val], type=field.type)
#                 except Exception as field_e:
#                     print(f"Row {row_idx}, Field '{field.name}', Value: {val}, Type: {field.type}")
#                     print(f"  Error: {field_e}")
#         raise HTTPException(status_code=400, detail=f"Arrow conversion error: {str(e)}")
#
#     catalog = get_catalog_client()
#     try:
#         catalog.load_namespace_properties(namespace)
#     except NoSuchNamespaceError:
#         catalog.create_namespace(namespace)
#
#     table_identifier = f"{namespace}.{table_name}"
#
#     try:
#         tbl = catalog.load_table(table_identifier)
#         # print("Table exists. Ready to append data.")
#     except NoSuchTableError:
#         partition_spec = PartitionSpec(
#             fields=[
#                 PartitionField(
#                     source_id=iceberg_schema.find_field("Bill_Date__c").field_id,
#                     field_id=2001,
#                     transform=IdentityTransform(),
#                     name="Bill_Date__c",
#                 ),
#                 PartitionField(
#                     source_id=iceberg_schema.find_field("store_code__c").field_id,
#                     field_id=2002,
#                     transform=BucketTransform(32),
#                     name="store_code",
#                 ),
#                 PartitionField(
#                     source_id=iceberg_schema.find_field("customer_mobile__c").field_id,
#                     field_id=2002,
#                     transform=BucketTransform(32),
#                     name="Customer",
#                 )
#             ]
#         )
#         # print(tbl.schema)
#         # tbl = catalog.create_table(
#         #     identifier=table_identifier,
#         #     schema=iceberg_schema,
#         #     partition_spec=partition_spec,
#         #     properties={"write.partition.path-style": "directory"},
#         # )
#         # print("✅ Iceberg table created successfully")
#         try:
#             tbl = catalog.create_table(
#                 identifier=table_identifier,
#                 schema=iceberg_schema,
#                 partition_spec=partition_spec,
#                 properties={"write.partition.path-style": "directory"},
#             )
#             print(f"✅ Created new Iceberg table: {table_identifier}")
#
#         # except AlreadyExistsError:
#         #     # Table already exists, just load it
#         #     print(f"⚠️ Table already exists — loading: {table_identifier}")
#         #     tbl = catalog.load_table(table_identifier)
#
#         except BadRequestError as e:
#             # Likely duplicate partition names or invalid field references
#             print(f"❌ BadRequestError while creating table {table_identifier}: {e}")
#             raise HTTPException(status_code=400, detail=f"Iceberg table creation failed: {e}")
#
#         except Exception as e:
#             # Catch any other unexpected issues
#             print(f"❌ Unexpected error creating table {table_identifier}: {e}")
#             raise HTTPException(status_code=500, detail=f"Unexpected error creating Iceberg table: {str(e)}")
#
#     try:
#         tbl.append(arrow_table)
#         tbl.refresh()
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed to append data to Iceberg table: {str(e)}")
#
#     elapsed = time.time() - total_start
#     return {
#         "status": "success",
#         "namespace": namespace,
#         "table": table_name,
#         "rows_written": len(converted_records),
#         "elapsed_seconds": round(elapsed, 2),
#         "schema": [f.name for f in iceberg_schema.columns],
#         "table_properties": getattr(tbl, "properties", {}),
#     }
# pri_id without
# @router.post("/create-ph-table")
# def create_transaction_phone_table(
#     # name_space:str = Query(default="pos_transactions01",description="Name space"),
#     # table_name:str = Query(default="transaction_phone_in_con_sum",description="Table name"),
# ):
#     namespace, table_name = "pos_transactions01", "transaction_phone_in_con_sum"
#     dbname = "Transaction"
#     mysql_creds = MysqlCatalog()
#
#     # --- Sample data to infer schema ---
#     try:
#         rows = mysql_creds.get_range_ph_bi(dbname, 0, 10)
#         print(rows[0])
#         if not rows:
#             raise HTTPException(status_code=400, detail="No sample data found to infer schema.")
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")
#
#     iceberg_schema, _ = infer_schema_from_record(rows[0])
#
#     # --- Partition specification ---
#     partition_spec = PartitionSpec(
#         fields=[
#             PartitionField(
#                 source_id=iceberg_schema.find_field("Bill_Date__c").field_id,
#                 field_id=2001,
#                 transform=IdentityTransform(),
#                 name="Bill_Date__c",
#             ),
#             PartitionField(
#                 source_id=iceberg_schema.find_field("store_code__c").field_id,
#                 field_id=2002,
#                 transform=BucketTransform(32),
#                 name="store_code",
#             ),
#             PartitionField(
#                 source_id=iceberg_schema.find_field("customer_mobile__c").field_id,
#                 field_id=2003,
#                 transform=BucketTransform(32),
#                 name="Customer",
#             ),
#         ]
#     )
#
#     catalog = get_catalog_client()
#
#     try:
#         catalog.load_namespace_properties(namespace)
#     except NoSuchNamespaceError:
#         catalog.create_namespace(namespace)
#
#     table_identifier = f"{namespace}.{table_name}"
#
#     try:
#         tbl = catalog.create_table(
#             identifier=table_identifier,
#             schema=iceberg_schema,
#             partition_spec=partition_spec,
#             properties={"write.partition.path-style": "directory"},
#         )
#         return {"status": "created", "table": table_identifier}
#     # except AlreadyExistsError:
#     #     return {"status": "exists", "table": table_identifier}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Table creation failed: {str(e)}")

@router.post("/create-ph-table")
def create_transaction_phone_table():
    """
    Create Iceberg table for transaction phone data based on MySQL schema sample.
    """
    namespace, table_name = "pos_transactions01", "transaction_phone_in_con_sum"
    dbname = "Transaction"
    mysql_creds = MysqlCatalog()

    # --- Step 1: Fetch sample data for schema inference ---
    try:
        rows = mysql_creds.get_range_ph_bi(dbname, 0, 10)
        if not rows:
            raise HTTPException(status_code=400, detail="No sample data found to infer schema.")
        print(f"Sample row for schema inference:\n{rows[0]}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")

    # --- Step 2: Infer Iceberg schema from MySQL data ---
    try:
        iceberg_schema, _ = infer_schema_from_record(rows[0])
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Schema inference failed: {str(e)}")

    # --- Step 3: Ensure primary key fields are NOT NULL (required) ---
    # You can explicitly force pri_id to be non-nullable
    try:
        pri_field = iceberg_schema.find_field("pri_id")
        if pri_field and pri_field.is_optional:
            iceberg_schema = iceberg_schema.update_field("pri_id", required=True)
            print("pri_id marked as required in schema.")
    except Exception:
        # skip if not found
        pass

    # --- Step 4: Partition specification ---
    try:
        partition_spec = PartitionSpec(
            fields=[
                PartitionField(
                    source_id=iceberg_schema.find_field("Bill_Date__c").field_id,
                    field_id=2001,
                    transform=DayTransform(),
                    name="Day",
                ),
                PartitionField(
                    source_id=iceberg_schema.find_field("store_code__c").field_id,
                    field_id=2002,
                    transform=BucketTransform(32),
                    name="store_code_bucket",
                ),
                PartitionField(
                    source_id=iceberg_schema.find_field("customer_mobile__c").field_id,
                    field_id=2003,
                    transform=BucketTransform(32),
                    name="customer_bucket",
                ),
            ]
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Partition spec error: {str(e)}")

    # --- Step 5: Get or create namespace ---
    catalog = get_catalog_client()
    try:
        catalog.load_namespace_properties(namespace)
    except NoSuchNamespaceError:
        catalog.create_namespace(namespace)

    table_identifier = f"{namespace}.{table_name}"

    # --- Step 6: Create table ---
    try:
        tbl = catalog.create_table(
            identifier=table_identifier,
            schema=iceberg_schema,
            partition_spec=partition_spec,
            properties={
                "write.format.default": "parquet",
                "write.parquet.compression-codec": "zstd",
                "write.partition.path-style": "directory",
            },
        )
        return {
            "status": "created",
            "table": table_identifier,
            "partitions": [f.name for f in partition_spec.fields],
        }

    # except AlreadyExistsError:
    #     return {"status": "exists", "table": table_identifier}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Table creation failed: {str(e)}")


# @router.post("/insert-ph-data")
# def insert_transaction_phone_data(
#     start_range: int = Query(0, description="Start row (e.g. 0)"),
#     end_range: int = Query(100000, description="End row (e.g. 100000)"),
# ):
#     total_start = time.time()
#     mysql_creds = MysqlCatalog()
#     namespace, table_name = "pos_transactions01", "transaction_phone_in_con_sum"
#     dbname = "Transaction"
#
#     # --- Fetch data ---
#     try:
#         rows = mysql_creds.get_range_ph_bi(dbname, start_range, end_range)
#         if not rows:
#             raise HTTPException(status_code=400, detail="No data found in range.")
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")
#
#     iceberg_schema, arrow_schema = infer_schema_from_record(rows[0])
#     converted_records = [convert_column(r, arrow_schema) for r in rows]
#
#     try:
#         arrow_table = pa.Table.from_pylist(converted_records, schema=arrow_schema)
#     except Exception as e:
#         raise HTTPException(status_code=400, detail=f"Arrow conversion error: {str(e)}")
#
#     # --- Load and append ---
#     catalog = get_catalog_client()
#     table_identifier = f"{namespace}.{table_name}"
#
#     try:
#         tbl = catalog.load_table(table_identifier)
#         tbl.append(arrow_table)
#         tbl.refresh()
#     except NoSuchTableError:
#         raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Append failed: {str(e)}")
#
#     elapsed = round(time.time() - total_start, 2)
#     return {
#         "status": "success",
#         "namespace": namespace,
#         "table": table_name,
#         "rows_written": len(converted_records),
#         "elapsed_seconds": elapsed,
#     }
from concurrent.futures import ThreadPoolExecutor
import traceback
LOGS_FOLDER = "logs/iceberg_upload"
os.makedirs(LOGS_FOLDER, exist_ok=True)

# @router.post("/insert-ph-data")
# def insert_transaction_phone_data(
#     start_range: int = Query(0, description="Start row (e.g. 0)"),
#     end_range: int = Query(100000, description="End row (e.g. 100000)"),
#     batch_size: int = Query(20000, description="Number of records per upload batch"),
# ):
#     total_start = time.time()
#     mysql_creds = MysqlCatalog()
#     namespace, table_name = "pos_transactions01", "transaction_phone_in_con_sum"
#     dbname = "Transaction"
#
#     log_file = os.path.join(LOGS_FOLDER, f"insert_ph_data_{datetime.now():%Y%m%d_%H%M%S}.log")
#
#     def log_error(message: str):
#         with open(log_file, "a") as f:
#             f.write(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {message}\n")
#
#     # --- Step 1: Fetch data ---
#     try:
#         rows = mysql_creds.get_range_ph_bi(dbname, start_range, end_range)
#         if not rows:
#             raise HTTPException(status_code=400, detail="No data found in range.")
#     except Exception as e:
#         log_error(f"MySQL fetch error: {traceback.format_exc()}")
#         raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")
#
#     # --- Step 2: Prepare Arrow schema ---
#     try:
#         iceberg_schema, arrow_schema = infer_schema_from_record(rows[0])
#         converted_records = [convert_column(r, arrow_schema) for r in rows]
#     except Exception as e:
#         log_error(f"Schema conversion failed: {traceback.format_exc()}")
#         raise HTTPException(status_code=400, detail=f"Schema conversion failed: {str(e)}")
#
#     # --- Step 3: Fast upload using batching ---
#     catalog = get_catalog_client()
#     table_identifier = f"{namespace}.{table_name}"
#
#     try:
#         tbl = catalog.load_table(table_identifier)
#     except NoSuchTableError:
#         log_error(f"Table not found: {table_identifier}")
#         raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")
#
#     total_rows = len(converted_records)
#     batches = [converted_records[i:i + batch_size] for i in range(0, total_rows, batch_size)]
#
#     print(f"Uploading {total_rows} rows in {len(batches)} batches...")
#
#     def append_batch(batch_index, batch_data):
#         try:
#             arrow_table = pa.Table.from_pylist(batch_data, schema=arrow_schema)
#             tbl.append(arrow_table)
#         except Exception as e:
#             log_error(f"[Batch {batch_index}] Append failed: {traceback.format_exc()}{e}")
#             return False
#         return True
#
#     success_batches = 0
#     with ThreadPoolExecutor(max_workers=30) as executor:
#         results = list(executor.map(
#             lambda b: append_batch(b[0], b[1]),
#             enumerate(batches)
#         ))
#         success_batches = sum(1 for r in results if r)
#
#     try:
#         tbl.refresh()
#     except Exception as e:
#         log_error(f"Table refresh failed: {traceback.format_exc()}")
#         raise HTTPException(status_code=500, detail=f"Table refresh failed: {str(e)}")
#
#     elapsed = round(time.time() - total_start, 2)
#     return {
#         "status": "success",
#         "namespace": namespace,
#         "table": table_name,
#         "rows_written": total_rows,
#         "batches_total": len(batches),
#         "batches_success": success_batches,
#         "log_file": log_file,
#         "elapsed_seconds": elapsed,
#     }
# @router.post("/insert-ph-data")
# def insert_transaction_phone_data(
#     start_range: int = Query(0),
#     end_range: int = Query(100000),
#     batch_size: int = Query(20000),
# ):
#     total_start = time.time()
#     mysql_creds = MysqlCatalog()
#     namespace, table_name = "pos_transactions01", "transaction_phone_in_con_sum"
#     dbname = "Transaction"
#
#     log_file = os.path.join(LOGS_FOLDER, f"insert_ph_data_{datetime.now():%Y%m%d_%H%M%S}.log")
#
#     def log_error(message: str):
#         with open(log_file, "a") as f:
#             f.write(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {message}\n")
#
#     # --- Step 1: Fetch ---
#     try:
#         rows = mysql_creds.get_range_ph_bi(dbname, start_range, end_range)
#         if not rows:
#             raise HTTPException(status_code=400, detail="No data found in range.")
#     except Exception as e:
#         log_error(f"MySQL fetch error: {traceback.format_exc()}")
#         raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")
#
#     # --- Step 2: Schema & conversion ---
#     # try:
#     #     iceberg_schema, arrow_schema = infer_schema_from_record(rows[0])
#     #     converted_records = [convert_column(r, arrow_schema) for r in rows]
#     # except Exception as e:
#     #     log_error(f"Schema conversion failed: {traceback.format_exc()}")
#     #     raise HTTPException(status_code=400, detail=f"Schema conversion failed: {str(e)}")
#
#     catalog = get_catalog_client()
#     table_identifier = f"{namespace}.{table_name}"
#
#     try:
#         tbl = catalog.load_table(table_identifier)
#     except NoSuchTableError:
#         log_error(f"Table not found: {table_identifier}")
#         raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")
#
#     # total_rows = len(converted_records)
#     # batches = [converted_records[i:i + batch_size] for i in range(0, total_rows, batch_size)]
#
#     # print(f"Uploading {total_rows} rows in {len(batches)} batches...")
#
#
#     # --- ✅ Optimized column-wise conversion ---
#     def build_arrow_table_columnwise(batch_data, schema):
#         """
#         Converts list of dicts → dict of lists → Arrow Table (column-wise)
#         """
#         columns_data = {field.name: [] for field in schema}
#         for row in batch_data:
#             for field in schema:
#                 columns_data[field.name].append(row.get(field.name))
#
#         arrays = [pa.array(columns_data[field.name], type=field.type) for field in schema]
#         return pa.Table.from_arrays(arrays, schema=schema)
#
#     def append_batch(batch_index, batch_data):
#         try:
#             arrow_table = build_arrow_table_columnwise(batch_data, arrow_schema)
#             tbl.append(arrow_table)
#         except Exception as e:
#             log_error(f"[Batch {batch_index}] Append failed: {traceback.format_exc(),e}")
#             return False
#         return True
#
#     # --- Step 3: Parallel batch upload ---
#     success_batches = 0
#     with ThreadPoolExecutor(max_workers=10) as executor:
#         results = list(executor.map(
#             lambda b: append_batch(b[0], b[1]),
#             # enumerate(batches)
#         ))
#         success_batches = sum(1 for r in results if r)
#
#     try:
#         tbl.refresh()
#     except Exception as e:
#         log_error(f"Table refresh failed: {traceback.format_exc()}")
#         raise HTTPException(status_code=500, detail=f"Table refresh failed: {str(e)}")
#
#     elapsed = round(time.time() - total_start, 2)
#     return {
#         "status": "success",
#         "namespace": namespace,
#         "table": table_name,
#         # "rows_written": total_rows,
#         # "batches_total": len(batches),
#         "batches_success": success_batches,
#         "log_file": log_file,
#         "elapsed_seconds": elapsed,
#     }
# @router.post("/insert-ph-data")
# def insert_transaction_phone_data(
#     start_range: int = Query(0),
#     end_range: int = Query(100000),
#
# ):
#     """
#     Insert phone transaction data into Iceberg (parallel upload + per-step timing)
#     """
#     total_start = time.time()
#     mysql_creds = MysqlCatalog()
#     namespace, table_name = "pos_transactions01", "transaction_phone_in_con_sum"
#     dbname = "Transaction"
#
#     log_file = os.path.join(LOGS_FOLDER, f"insert_ph_data_{datetime.now():%Y%m%d_%H%M%S}.log")
#
#     def log_info(message: str):
#         with open(log_file, "a") as f:
#             f.write(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {message}\n")
#
#     def log_error(message: str):
#         with open(log_file, "a") as f:
#             f.write(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] [ERROR] {message}\n")
#
#     # --- Step 1: Fetch data from MySQL ---
#     fetch_start = time.time()
#     try:
#         rows = mysql_creds.get_range_ph_bi(dbname, start_range, end_range)
#         if not rows:
#             raise HTTPException(status_code=400, detail="No data found in range.")
#         log_info(f"MySQL Fetch: Retrieved {len(rows)} rows.")
#     except Exception as e:
#         log_error(f"MySQL fetch error: {traceback.format_exc()}")
#         raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")
#     fetch_end = time.time()
#
#     # --- Step 2: Schema Inference ---
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
#
#     try:
#         tbl = catalog.load_table(table_identifier)
#         log_info(f"Table loaded: {table_identifier}")
#     except NoSuchTableError:
#         log_error(f"Table not found: {table_identifier}")
#         raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")
#     catalog_end = time.time()
#
#     # --- Step 4: Create Batches ---
#     batch_start = time.time()
#     total_rows = len(rows)
#     batches = [rows[i:i + batch_size] for i in range(0, total_rows, batch_size)]
#     log_info(f"Batching completed: {len(batches)} batches of size {batch_size}")
#     batch_end = time.time()
#
#     # --- Step 5: Column-wise Table Builder ---
#     def build_arrow_table_columnwise(batch_data):
#         build_start = time.time()
#         columns_data = {field.name: [] for field in arrow_schema}
#         for row in batch_data:
#             for field in arrow_schema:
#                 columns_data[field.name].append(row.get(field.name))
#         arrays = [pa.array(columns_data[field.name], type=field.type) for field in arrow_schema]
#         arrow_table = pa.Table.from_arrays(arrays, schema=arrow_schema)
#         build_end = time.time()
#         log_info(f"Build Arrow Table: Took {round(build_end - build_start, 2)} sec for {len(batch_data)} rows")
#         return arrow_table
#
#     # --- Step 6: Append Batch Function ---
#     def append_batch(batch_index, batch_data):
#         append_start = time.time()
#         try:
#             arrow_table = build_arrow_table_columnwise(batch_data)
#             tbl.append(arrow_table)
#         except Exception as e:
#             log_error(f"[Batch {batch_index}] Append failed: {traceback.format_exc()}")
#             return False
#         append_end = time.time()
#         log_info(f"[Batch {batch_index}] Append success. Rows: {len(batch_data)}, Time: {round(append_end - append_start, 2)}s")
#         return True
#
#     # --- Step 7: Parallel Upload ---
#     upload_start = time.time()
#     with ThreadPoolExecutor(max_workers=15) as executor:
#         results = list(executor.map(lambda b: append_batch(b[0], b[1]), enumerate(batches)))
#
#     success_batches = sum(1 for r in results if r)
#     failed_batches = len(batches) - success_batches
#     upload_end = time.time()
#
#     # --- Step 8: Refresh Table ---
#     refresh_start = time.time()
#     try:
#         tbl.refresh()
#         log_info("Table refreshed successfully.")
#     except Exception as e:
#         log_error(f"Table refresh failed: {traceback.format_exc()}")
#         raise HTTPException(status_code=500, detail=f"Table refresh failed: {str(e)}")
#     refresh_end = time.time()
#
#     # --- Step 9: Summary ---
#     total_end = time.time()
#     summary = {
#         # "status": "success" if failed_batches == 0 else "partial_success",
#         "namespace": namespace,
#         "table": table_name,
#         "rows_written": total_rows,
#         # "batches_total": len(batches),
#         "batches_success": success_batches,
#         # "batches_failed": failed_batches,
#         "timing_seconds": {
#             "fetch_mysql": round(fetch_end - fetch_start, 2),
#             "schema_inference": round(schema_end - schema_start, 2),
#             "catalog_load": round(catalog_end - catalog_start, 2),
#             "batch_creation": round(batch_end - batch_start, 2),
#             "upload_total": round(upload_end - upload_start, 2),
#             "refresh_table": round(refresh_end - refresh_start, 2),
#             "total_runtime": round(total_end - total_start, 2),
#         },
#         "log_file": log_file,
#     }
#
#     log_info("=== Summary ===")
#     for k, v in summary["timing_seconds"].items():
#         log_info(f"{k}: {v} seconds")
#     log_info("================")
#
#     return summary
# @router.post("/insert-ph-data")
# def insert_transaction_phone_data(
#     start_range: int = Query(0),
#     end_range: int = Query(100000),
# ):
#     """
#     Insert phone transaction data into Iceberg (single batch, full dataset, with per-step timing)
#     """
#     total_start = time.time()
#     mysql_creds = MysqlCatalog()
#     namespace, table_name = "pos_transactions01", "transaction_phone_in_con_sum"
#     dbname = "Transaction"
#
#     log_file = os.path.join(LOGS_FOLDER, f"insert_ph_data_{datetime.now():%Y%m%d_%H%M%S}.log")
#
#     def log_info(message: str):
#         with open(log_file, "a") as f:
#             f.write(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {message}\n")
#
#     def log_error(message: str):
#         with open(log_file, "a") as f:
#             f.write(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] [ERROR] {message}\n")
#
#     # --- Step 1: Fetch data from MySQL ---
#     fetch_start = time.time()
#     try:
#         rows = mysql_creds.get_range_ph_bi(dbname, start_range, end_range)
#         if not rows:
#             raise HTTPException(status_code=400, detail="No data found in range.")
#         log_info(f"MySQL Fetch: Retrieved {len(rows)} rows.")
#     except Exception as e:
#         log_error(f"MySQL fetch error: {traceback.format_exc()}")
#         raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")
#     fetch_end = time.time()
#
#     # --- Step 2: Schema Inference ---
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
#
#     try:
#         tbl = catalog.load_table(table_identifier)
#         log_info(f"Table loaded: {table_identifier}")
#     except NoSuchTableError:
#         log_error(f"Table not found: {table_identifier}")
#         raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")
#     catalog_end = time.time()
#
#     # --- Step 4: Build Column-Wise Arrow Table ---
#     build_start = time.time()
#     try:
#         columns_data = {field.name: [] for field in arrow_schema}
#         for row in rows:
#             for field in arrow_schema:
#                 columns_data[field.name].append(row.get(field.name))
#         arrays = [pa.array(columns_data[field.name], type=field.type) for field in arrow_schema]
#         arrow_table = pa.Table.from_arrays(arrays, schema=arrow_schema)
#         log_info(f"Arrow table built successfully for {len(rows)} rows.")
#     except Exception as e:
#         log_error(f"Arrow table build failed: {traceback.format_exc()}")
#         raise HTTPException(status_code=500, detail=f"Arrow table build failed: {str(e)}")
#     build_end = time.time()
#
#     # --- Step 5: Append Data to Iceberg ---
#     append_start = time.time()
#     try:
#         tbl.append(arrow_table)
#         log_info("Data appended successfully to Iceberg table.")
#     except Exception as e:
#         log_error(f"Append failed: {traceback.format_exc()}")
#         raise HTTPException(status_code=500, detail=f"Append failed: {str(e)}")
#     append_end = time.time()
#
#     # --- Step 6: Refresh Table ---
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
#         "status": "success",
#         "namespace": namespace,
#         "table": table_name,
#         "rows_written": len(rows),
#         "timing_seconds": {
#             "fetch_mysql": round(fetch_end - fetch_start, 2),
#             "schema_inference": round(schema_end - schema_start, 2),
#             "catalog_load": round(catalog_end - catalog_start, 2),
#             "build_arrow_table": round(build_end - build_start, 2),
#             "append_data": round(append_end - append_start, 2),
#             "refresh_table": round(refresh_end - refresh_start, 2),
#             "total_runtime": round(total_end - total_start, 2),
#         },
#         "log_file": log_file,
#     }
#
#     log_info("=== Summary ===")
#     for k, v in summary["timing_seconds"].items():
#         log_info(f"{k}: {v} seconds")
#     log_info("================")
#
#     return summary

@router.post("/insert-ph-data")
def insert_transaction_phone_data(
    start_range: int = Query(0),
    end_range: int = Query(100000),
):
    """
    Insert phone transaction data into Iceberg (with timing + automatic numeric conversion)
    """
    total_start = time.time()
    mysql_creds = MysqlCatalog()
    namespace, table_name = "pos_transactions01", "transaction_phone_in_con_sum"
    dbname = "Transaction"

    # --- Log setup ---
    log_file = os.path.join(LOGS_FOLDER, f"insert_ph_data_{datetime.now():%Y%m%d_%H%M%S}.log")
    def log_info(msg: str):
        with open(log_file, "a") as f:
            f.write(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}\n")
    def log_error(msg: str):
        with open(log_file, "a") as f:
            f.write(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] [ERROR] {msg}\n")

    # --- Step 1: Fetch data from MySQL ---
    fetch_start = time.time()
    try:
        rows = mysql_creds.get_range_ph_bi(dbname, start_range, end_range)
        # print(rows)
        if not rows:
            raise HTTPException(status_code=400, detail="No data found in range.")
        log_info(f"MySQL Fetch: Retrieved {len(rows)} rows.")
    except Exception as e:
        log_error(f"MySQL fetch error: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")
    fetch_end = time.time()

    # --- Step 2: Schema inference ---
    schema_start = time.time()
    try:
        iceberg_schema, arrow_schema = infer_schema_from_record(rows[0])
        log_info("Schema inference completed successfully.")
    except Exception as e:
        log_error(f"Schema inference failed: {traceback.format_exc()}")
        raise HTTPException(status_code=400, detail=f"Schema inference failed: {str(e)}")
    schema_end = time.time()

    # --- Step 3: Load Iceberg table ---
    catalog_start = time.time()
    catalog = get_catalog_client()
    table_identifier = f"{namespace}.{table_name}"
    try:
        tbl = catalog.load_table(table_identifier)
        log_info(f"Table loaded: {table_identifier}")
    except NoSuchTableError:
        log_error(f"Table not found: {table_identifier}")
        raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")
    catalog_end = time.time()

    # --- Step 4: Convert data to Arrow table ---
    arrow_start = time.time()
    try:
        columns_data = {field.name: [] for field in arrow_schema}
        for row in rows:
            for field in arrow_schema:
                val = row.get(field.name)
                # 🔹 Auto-convert numeric fields safely
                if pa.types.is_integer(field.type):
                    try:
                        val = int(val) if val not in (None, "") else None
                    except Exception:
                        val = None
                elif pa.types.is_floating(field.type):
                    try:
                        val = float(val) if val not in (None, "") else None
                    except Exception:
                        val = None
                columns_data[field.name].append(val)

        arrays = [pa.array(columns_data[f.name], type=f.type) for f in arrow_schema]
        arrow_table = pa.Table.from_arrays(arrays, schema=arrow_schema)
        log_info(f"Arrow table built successfully ({len(rows)} rows).")
    except Exception as e:
        log_error(f"Arrow table build failed: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Arrow table build failed: {str(e)}")
    arrow_end = time.time()

    # --- Step 5: Append data to Iceberg ---
    append_start = time.time()
    try:
        tbl.append(arrow_table)
        log_info("Data appended to Iceberg successfully.")
    except Exception as e:
        log_error(f"Data append failed: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Data append failed: {str(e)}")
    append_end = time.time()

    # --- Step 6: Refresh Iceberg table ---
    refresh_start = time.time()
    try:
        tbl.refresh()
        log_info("Table refreshed successfully.")
    except Exception as e:
        log_error(f"Table refresh failed: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Table refresh failed: {str(e)}")
    refresh_end = time.time()

    # --- Step 7: Summary ---
    total_end = time.time()
    summary = {
        "namespace": namespace,
        "table": table_name,
        "rows_written": len(rows),
        "timing_seconds": {
            "fetch_mysql": round(fetch_end - fetch_start, 2),
            "schema_inference": round(schema_end - schema_start, 2),
            "catalog_load": round(catalog_end - catalog_start, 2),
            "arrow_build": round(arrow_end - arrow_start, 2),
            "append_data": round(append_end - append_start, 2),
            "refresh_table": round(refresh_end - refresh_start, 2),
            "total_runtime": round(total_end - total_start, 2),
        },
        "log_file": log_file,
    }

    log_info("=== Summary ===")
    for k, v in summary["timing_seconds"].items():
        log_info(f"{k}: {v} seconds")
    log_info("================")

    return summary

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
                # print("snapshot", snapshot.summary.get("total-records"))
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

# @router.get("/filter-customer-mobile-fast")
# def filter_customer_mobile_fast(
#         # namespace: str = Query(..., description="Namespace (e.g. 'transactions')"),
#         # table_name: str = Query(..., description="Table name (e.g. 'pos')"),
#         mobile_phone: str = Query(..., description="Customer mobile phone number to filter"),
#
# ):
#     start_time = time.time()
#
#     try:
#         namespace, table_name = "pos_transactions01", "transaction_phone_in_con_sum"
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
#         print("scan",scan.to_array())
#         rows = []
#
#         # --- Lazy iteration with offset and limit ---
#
#
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

import pandas as pd
# @router.get("/filter-customer-mobile-fast")
# def filter_customer_mobile_fast(
#     mobile_phone: str = Query(..., description="Customer mobile phone number to filter"),
# ):
#     """
#     Fetch records from Iceberg where `customer_mobile__c` == mobile_phone
#     """
#     start_time = time.time()
#     namespace, table_name = "pos_transactions01", "transaction_phone_in_con_sum"
#
#     try:
#         # --- Load table ---
#         catalog = get_catalog_client()
#         table = catalog.load_table(f"{namespace}.{table_name}")
#
#         # --- Validate column ---
#         column = "customer_mobile__c"
#         field = next((f for f in table.schema().fields if f.name == column), None)
#         if not field:
#             raise HTTPException(status_code=400, detail=f"Column '{column}' not found in schema")
#
#         # --- Build filter & scan ---
#         scan = table.scan(row_filter=EqualTo(column, mobile_phone))
#
#         # --- Convert scanned data to Arrow / Python list ---
#         batches = scan.to_arrow()  # returns Arrow RecordBatches generator
#         rows = []
#         # for batch in scan.to_arrow():
#         #     # Convert to pandas DataFrame (handles 1 or many columns safely)
#         #     df = batch.to_pandas()
#         #     # Ensure it's always a DataFrame
#         #     if isinstance(df, pd.Series):
#         #         df = df.to_frame()
#         #     rows.extend(df.to_dict(orient="records"))
#         # --- Metadata ---
#         elapsed = round(time.time() - start_time, 2)
#         metadata = {
#             "namespace": namespace,
#             "table": table_name,
#             "execution_time_seconds": elapsed,
#             "filter_column": column,
#             "filter_value": mobile_phone,
#             "snapshot_id": table.current_snapshot().snapshot_id if table.current_snapshot() else None,
#             "records_count": len(batches),
#         }
#
#         return {"status": "success", "metadata": metadata, "data": batches.to_pylist()}  # limit response for safety
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Filter failed: {str(e)}")

# @router.get("/filter-customer-mobile-fast")
# def filter_customer_mobile_fast(
#     mobile_phone: str = Query(..., description="Customer mobile phone number to filter"),
# ):
#     """
#     Fetch records from Iceberg where `customer_mobile__c` == mobile_phone (duplicate-free)
#     """
#     import pyarrow as pa
#     import pandas as pd
#     import time
#
#     start_time = time.time()
#     namespace, table_name = "pos_transactions01", "transaction_phone_in_con_sum"
#
#     try:
#         # --- Load Iceberg table ---
#         catalog = get_catalog_client()
#         table = catalog.load_table(f"{namespace}.{table_name}")
#
#         # --- Validate column ---
#         column = "customer_mobile__c"
#         if not any(f.name == column for f in table.schema().fields):
#             raise HTTPException(status_code=400, detail=f"Column '{column}' not found in schema")
#
#         # --- Build filter and perform scan ---
#         scan = table.scan(row_filter=EqualTo(column, mobile_phone))
#
#         # --- Collect Arrow batches ---
#         arrow_batches = list(scan.to_arrow())
#
#         if not arrow_batches:
#             elapsed = round(time.time() - start_time, 2)
#             return {
#                 "status": "success",
#                 "metadata": {
#                     "namespace": namespace,
#                     "table": table_name,
#                     "execution_time_seconds": elapsed,
#                     "filter_column": column,
#                     "filter_value": mobile_phone,
#                     "snapshot_id": table.current_snapshot().snapshot_id if table.current_snapshot() else None,
#                     "records_count": 0,
#                 },
#                 "data": [],
#             }
#
#         # --- Combine Arrow batches into one Table ---
#         arrow_table = pa.concat_tables(arrow_batches)
#
#         # --- Drop duplicates safely using Pandas ---
#         df = arrow_table.to_pandas()
#         df = df.drop_duplicates(ignore_index=True)
#
#         # --- Prepare final clean data ---
#         data = df.to_dict(orient="records")
#
#         # --- Metadata ---
#         elapsed = round(time.time() - start_time, 2)
#         metadata = {
#             "namespace": namespace,
#             "table": table_name,
#             "execution_time_seconds": elapsed,
#             "filter_column": column,
#             "filter_value": mobile_phone,
#             "snapshot_id": table.current_snapshot().snapshot_id if table.current_snapshot() else None,
#             "records_count": len(data),
#         }

    #     return {"status": "success", "metadata": metadata, "data": data}
    #
    # except Exception as e:
    #     raise HTTPException(status_code=500, detail=f"Filter failed: {str(e)}")

# @router.get("/filter-customer-mobile-fast")
# def filter_customer_mobile_fast(
#     mobile_phone: str = Query(..., description="Customer mobile phone number to filter"),
# ):
#     """
#     Fetch records from Iceberg where `customer_mobile__c` == mobile_phone (duplicate-free, Arrow-safe)
#     """
#     import pyarrow as pa
#     import pandas as pd
#     import time
#
#     start_time = time.time()
#     namespace, table_name = "pos_transactions01", "transaction_phone_in_con_sum"
#
#     try:
#         # --- Load Iceberg table ---
#         catalog = get_catalog_client()
#         table = catalog.load_table(f"{namespace}.{table_name}")
#
#         # --- Validate column ---
#         column = "customer_mobile__c"
#         if not any(f.name == column for f in table.schema().fields):
#             raise HTTPException(status_code=400, detail=f"Column '{column}' not found in schema")
#
#         # --- Build filter and perform scan ---
#         scan = table.scan(row_filter=EqualTo(column, mobile_phone))
#
#         # --- Get Arrow data ---
#         arrow_result = scan.to_arrow()
#
#         # Handle single table or batch list
#         if isinstance(arrow_result, pa.Table):
#             arrow_table = arrow_result
#         elif isinstance(arrow_result, (list, tuple)):
#             # Convert list of RecordBatch to a Table
#             arrow_table = pa.Table.from_batches(arrow_result)
#         else:
#             raise ValueError(f"Unexpected Arrow result type: {type(arrow_result)}")
#
#         # --- Convert to Pandas and remove duplicates ---
#         df = arrow_table.to_pandas()
#         df = df.drop_duplicates(ignore_index=True)
#
#         # --- Prepare clean data ---
#         data = df.to_dict(orient="records")
#
#         # --- Metadata ---
#         elapsed = round(time.time() - start_time, 2)
#         metadata = {
#             # "namespace": namespace,
#             # "table": table_name,
#             "execution_time_seconds": elapsed,
#             # "filter_column": column,
#             "filter_value": mobile_phone,
#             # "snapshot_id": table.current_snapshot().snapshot_id if table.current_snapshot() else None,
#             "records_count": len(data),
#         }
#
#         return {"status": "success", "metadata": metadata, "data": data}
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Filter failed: {str(e)}")

from fastapi import APIRouter, Query, Header, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from pyiceberg.expressions import EqualTo, NotStartsWith

class Transaction(BaseModel):
    bill_no: Optional[str]
    bill_status: Optional[str]
    bill_date: Optional[str]
    customer_mobile: Optional[str]
    bill_transaction_no: Optional[str]
    item_name: Optional[str]
    invoice_amount: Optional[float]
    branch_name: Optional[str]

class CrmResponse(BaseModel):
    data: List[Transaction]
    total_records: int
    latest_bill_date: Optional[str]

def is_valid_mobile(number: str) -> bool:
    return re.match(r"^[0-9]{10}$", number) is not None

def filter_bill_status(bills: List[dict]) -> List[dict]:
    bill_groups = {}
    for bill in bills:
        bill_no = bill.get("bill_no")
        if not bill_no:
            continue
        bill_groups.setdefault(bill_no, []).append(bill)

    result = []
    for group in bill_groups.values():
        edit_bill = next((b for b in group if b.get("bill_status") == "Edit"), None)
        if edit_bill:
            result.append(edit_bill)
        else:
            new_bills = [b for b in group if b.get("bill_status") == "New"]
            result.extend(new_bills)
    return result

from fastapi.responses import JSONResponse

# =====================
# 🔹 R2 CRM Endpoint
# =====================
@router.get("/crm-r2")
def crm_r2(
    customer_mobile: str = Query(..., description="Customer mobile number"),
    invoice_no: Optional[str] = Query(None, description="Invoice number filter"),
    limit: int = Query(10, description="Limit per page"),
    page: int = Query(1, description="Page number"),

):
    start_time = time.time()


    # ✅ Mobile Number Validation
    if not is_valid_mobile(customer_mobile.strip()):
        raise HTTPException(status_code=400, detail=f"{customer_mobile} is NOT a valid mobile number")

    namespace, table_name = "pos_transactions01", "transaction_phone_in_con_sum"

    try:
        # --- Load Iceberg table ---
        catalog = get_catalog_client()
        table = catalog.load_table(f"{namespace}.{table_name}")
        # print(table.scan("customer_mobile__c"))
        # --- Build filters ---
        filters = EqualTo("customer_mobile__c", customer_mobile)
        # print(filters)
        # Bill_No__c NOT LIKE 'DN%'
        # Iceberg doesn't support LIKE directly, so we use NotStartsWith
        # filters = filters & NotStartsWith("bill_no__c", "DN")
        filters = filters & NotStartsWith("Bill_No__c", "DN")

        # Apply invoice filter
        if invoice_no:
            # Iceberg doesn’t support SQL-style LIKE filters natively;
            # we’ll filter later in Pandas
            invoice_filter = invoice_no
        else:
            invoice_filter = None

        # --- Scan table with filters ---
        scan = table.scan(row_filter=filters)

        print(scan.to_arrow().to_pandas().to_dict(orient="records"))
        arrow_result = scan.to_arrow()

        # --- Normalize Arrow data ---
        if isinstance(arrow_result, pa.Table):
            arrow_table = arrow_result
        elif isinstance(arrow_result, (list, tuple)):
            arrow_table = pa.Table.from_batches(arrow_result)
        else:
            raise ValueError(f"Unexpected Arrow result type: {type(arrow_result)}")

        # --- Convert to DataFrame ---
        df = arrow_table.to_pandas()
        print(df)
        # --- Optional Invoice Filter ---
        if invoice_filter:
            df = df[df["bill_transaction_no__c"].str.contains(invoice_filter, na=False)]

        # --- Clean & Rename columns ---
        df = df.rename(columns={
            "bill_no__c": "bill_no",
            "bill_status__c": "bill_status",
            "Bill_Date__c": "bill_date",
            "customer_mobile__c": "customer_mobile",
            "bill_transaction_no__c": "bill_transaction_no",
            "item_name__c": "item_name",
            "Invoice_Amount__c": "invoice_amount",
            "Branch_Name__c": "branch_name"
        })

        # --- Sort & paginate ---
        df["bill_date"] = df["bill_date"].astype(str).str.replace(" 00:00:00", "")
        df = df.sort_values(by="bill_date", ascending=False)
        total_records = len(df)

        offset = (page - 1) * limit
        paginated_df = df.iloc[offset:offset + limit]

        # --- Latest bill date ---
        latest_bill_date = df["bill_date"].max() if not df.empty else None

        # --- Apply business bill status logic ---
        filtered_data = filter_bill_status(paginated_df.to_dict(orient="records"))

        elapsed = round(time.time() - start_time, 2)

        return JSONResponse(
            content={
                "total_records": total_records,
                "data": scan.to_arrow().to_pandas().to_dict(orient="records"),
                "latest_bill_date": latest_bill_date,
                "execution_time_seconds": elapsed
            }
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"R2 CRM query failed: {str(e)}")

@router.put("/update-partition")
def update_partition_spec():
    namespace, table_name = "pos_transactions01", "transaction_phone_in_con_sum"

    try:
        catalog = get_catalog_client()
        table = catalog.load_table(f"{namespace}.{table_name}")
    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table {namespace}.{table_name} not found")

    iceberg_schema = table.schema()
    print(iceberg_schema)

    # --- ✅ Updated Partition Spec ---
    new_partition_spec = PartitionSpec(
        fields=[
            PartitionField(
                source_id=iceberg_schema.find_field("Bill_Date__c").field_id,
                field_id=2001,
                transform=DayTransform(),  # <-- ✅ Partition by Day
                name="Bill_Date__c",
            ),
            PartitionField(
                source_id=iceberg_schema.find_field("store_code__c").field_id,
                field_id=2002,
                transform=BucketTransform(32),
                name="store_code__c",
            ),
            PartitionField(
                source_id=iceberg_schema.find_field("customer_mobile__c").field_id,
                field_id=2003,
                transform=BucketTransform(32),
                name="customer_mobile__c",
            ),
        ]
    )

    try:
        # --- Update Table Partition Spec ---
        table.update_spec(new_partition_spec)
        table.commit()

        return {
            "status": "success",
            "message": f"Partition spec updated for {namespace}.{table_name}",
            "new_partition_spec": [
                {
                    "name": f.name,
                    "transform": type(f.transform).__name__
                }
                for f in new_partition_spec.fields
            ],
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Partition update failed: {str(e)}")