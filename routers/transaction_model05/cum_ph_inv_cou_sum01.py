from fastapi import APIRouter,HTTPException,Query,Body
import time
from concurrent.futures import ThreadPoolExecutor
import traceback

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

router = APIRouter(prefix="", tags=["Transaction phone version 01"])



# @router.post("/create-ph-table")
# def create_transaction_phone_table():
#     """
#     Create Iceberg table for transaction phone data based on MySQL schema sample.
#     """
#     namespace, table_name = "pos_transactions01", "transaction_phone_in_con_sum"
#     dbname = "Transaction"
#     mysql_creds = MysqlCatalog()
#
#     # --- Step 1: Fetch sample data for schema inference ---
#     try:
#         rows = mysql_creds.get_range_ph_bi(dbname, 0, 10)
#
#         if not rows:
#             raise HTTPException(status_code=400, detail="No sample data found to infer schema.")
#         sample_record = rows[0]
#         # sample_record = rows[0]
#         # for key, value in sample_record.items():
#         #     if isinstance(value, str):
#         #         try:
#         #             # try parsing common date formats
#         #             sample_record[key] = datetime.strptime(value, "%Y-%m-%d").date()
#         #         except ValueError:
#         #             pass
#         from datetime import datetime, date
#         for key, value in sample_record.items():
#             if isinstance(value, str):
#                 for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%Y-%m-%d %H:%M:%S"):
#                     try:
#                         sample_record[key] = datetime.strptime(value, fmt).date()
#                         print(f"Converted {key} to date from string format.")
#                         break
#                     except ValueError:
#                         continue
#         print(f"Sample row for schema inference:\n{sample_record}")
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")
#
#     # --- Step 2: Infer Iceberg schema from MySQL data ---
#     try:
#         iceberg_schema, _ = infer_schema_from_record(sample_record)
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Schema inference failed: {str(e)}")
#
#     # --- Step 3: Ensure primary key fields are NOT NULL (required) ---
#     # You can explicitly force pri_id to be non-nullable
#     try:
#         pri_field = iceberg_schema.find_field("pri_id")
#         if pri_field and pri_field.is_optional:
#             iceberg_schema = iceberg_schema.update_field("pri_id", required=True)
#             print("pri_id marked as required in schema.")
#     except Exception as e:
#         print(e)
#
#         pass
#     print(iceberg_schema)
#     # --- Step 4: Partition specification ---
#     try:
#         partition_spec = PartitionSpec(
#             fields=[
#                 PartitionField(
#                     source_id=iceberg_schema.find_field("Bill_Date__c").field_id,
#                     field_id=2001,
#                     transform=DayTransform(),
#                     name="Day",
#                 ),
#                 PartitionField(
#                     source_id=iceberg_schema.find_field("store_code__c").field_id,
#                     field_id=2002,
#                     transform=BucketTransform(32),
#                     name="store_code_bucket",
#                 ),
#                 PartitionField(
#                     source_id=iceberg_schema.find_field("customer_mobile__c").field_id,
#                     field_id=2003,
#                     transform=BucketTransform(32),
#                     name="customer_bucket",
#                 ),
#             ]
#         )
#     except Exception as e:
#         raise HTTPException(status_code=400, detail=f"Partition spec error: {str(e)}")
#
#     # --- Step 5: Get or create namespace ---
#     catalog = get_catalog_client()
#     try:
#         catalog.load_namespace_properties(namespace)
#     except NoSuchNamespaceError:
#         catalog.create_namespace(namespace)
#
#
#     table_identifier = f"{namespace}.{table_name}"
#
#     # --- Step 6: Create table ---
#     try:
#         tbl = catalog.create_table(
#             identifier=table_identifier,
#             schema=iceberg_schema,
#             partition_spec=partition_spec,
#             properties={
#                 "write.format.default": "parquet",
#                 "write.parquet.compression-codec": "zstd",
#                 "write.partition.path-style": "directory",
#             },
#         )
#         return {
#             "status": "created",
#             "namespace": namespace,
#             "table": table_name,
#             "schema_fields": [f.name for f in iceberg_schema.fields],
#             "partitions": [f.name for f in partition_spec.fields]
#         }
#
#     # except AlreadyExistsError:
#     #     return {"status": "exists", "table": table_identifier}
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Table creation failed: {str(e)}")

@router.post("/create-ph-table")
def create_transaction_phone_table():



    # --- Step 3: Ensure primary key (pri_id) is non-nullable ---
    try:
        pri_field = iceberg_schema.find_field("pri_id")
        if pri_field:
            if pri_field.is_optional:
                iceberg_schema = iceberg_schema.update_field("pri_id", required=True)
                print("[INFO] pri_id marked as required in schema.")
        else:
            print("[WARN] pri_id not found in schema.")
    except Exception as e:
        print("[WARN] pri_id modification failed:", e)

    # --- Step 4: Validate partition fields existence ---
    required_fields = ["Bill_Date__c", "store_code__c", "customer_mobile__c"]
    missing = [f for f in required_fields if not iceberg_schema.find_field(f)]
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing partition fields: {missing}")

    # --- Step 5: Define Iceberg partition spec ---
    try:
        partition_spec = PartitionSpec(
            fields=[
                PartitionField(
                    source_id=iceberg_schema.find_field("Bill_Date__c").field_id,
                    field_id=2001,
                    transform=DayTransform(),  # ✅ Works now since Bill_Date__c is 'date'
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
        print("[INFO] Partition specification created successfully.")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Partition spec error: {str(e)}")

    # --- Step 6: Get or create namespace in catalog ---
    catalog = get_catalog_client()
    try:
        catalog.load_namespace_properties(namespace)
        print(f"[INFO] Namespace '{namespace}' already exists.")
    except NoSuchNamespaceError:
        catalog.create_namespace(namespace)
        print(f"[INFO] Namespace '{namespace}' created.")

    table_identifier = f"{namespace}.{table_name}"

    # --- Step 7: Create Iceberg table ---
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
        print(f"[SUCCESS] Table created: {table_identifier}")
        return {
            "status": "created",
            "namespace": namespace,
            "table": table_name,
            "schema_fields": [f.name for f in iceberg_schema.fields],
            "partitions": [f.name for f in partition_spec.fields],
        }

    # except AlreadyExistsError:
    #     print(f"[INFO] Table already exists: {table_identifier}")
    #     return {"status": "exists", "table": table_identifier}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Table creation failed: {str(e)}")


LOGS_FOLDER = "logs/iceberg_upload"
os.makedirs(LOGS_FOLDER, exist_ok=True)



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