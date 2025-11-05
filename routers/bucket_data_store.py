from fastapi import FastAPI, APIRouter, Query, HTTPException
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import StringType, LongType, DateType,TimestampType
import time, json, boto3, os
from botocore.client import Config
import logging
import time
from ..mysql_creds import *
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
from datetime import datetime
from ..mapping import *
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError

from ..core.r2_client import get_r2_client

from ..core.catalog_client import get_catalog_client
from fastapi import APIRouter,Query,HTTPException

app = FastAPI()
router = APIRouter(prefix="/bucket_data_store", tags=["bucket_data_store"])
logger = logging.getLogger(__name__)


# ============================================================
# 🔧 CONFIGURATION
# ============================================================

R2_BUCKET_NAME = os.getenv("BUCKET_NAME")


s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("ENDPOINT"),
    aws_access_key_id=os.getenv("ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"),
    config=Config(signature_version="s3v4"),
    region_name="auto"
)

# DB_CONFIG = {
#     "host": os.getenv("MYSQL_HOST", "localhost"),
#     "user": os.getenv("MYSQL_USER", "root"),
#     "password": os.getenv("MYSQL_PASSWORD", ""),
#     "database": os.getenv("MYSQL_DATABASE", "r2bridge"),
# }

# ============================================================
# 🧩 HELPER FUNCTIONS
# ============================================================

# def get_db_connection():
#     return mysql.connector.connect(**DB_CONFIG)

def store_json_to_r2(data: dict, key: str):
    """Upload a JSON record to R2"""
    s3.put_object(
        Bucket=R2_BUCKET_NAME,
        Key=key,
        Body=json.dumps(data, indent=2).encode("utf-8"),
        ContentType="application/json"
    )

def fetch_json_from_r2(key: str):
    """
    Fetch JSON object from R2 using the S3-compatible API.
    Returns dict if found, None if not found.
    """
    try:
        # get_r2_client().get_object()
        resp = get_r2_client().get_object(Bucket=R2_BUCKET_NAME, Key=key)
        body = resp["Body"].read()
        data = json.loads(body)
        return data
    except get_r2_client().exceptions.NoSuchKey:
        print(f"⚠️ R2 key not found: {key}")
        return None
    except Exception as e:
        print(f"⚠️ Error fetching key {key} from R2: {e}")
        return None

# def insert_metadata(data: dict, r2_key: str):
#     """Insert metadata into MySQL"""
#     conn = get_db_connection()
#     cursor = conn.cursor()
#     query = """
#         INSERT INTO r2_data_index
#         (pri_id, bill_no, customer_mobile, customer_name, item_name, year, month, day, r2_key)
#         VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
#         ON DUPLICATE KEY UPDATE
#             bill_no = VALUES(bill_no),
#             customer_mobile = VALUES(customer_mobile),
#             customer_name = VALUES(customer_name),
#             item_name = VALUES(item_name),
#             updated_at = CURRENT_TIMESTAMP
#     """
#     cursor.execute(query, (
#         data["pri_id"],
#         data.get("Bill_No__c"),
#         data.get("customer_mobile__c"),
#         data.get("customer_fname__c"),
#         data.get("Item_Name__c"),
#         data.get("year"),
#         data.get("month"),
#         data.get("day"),
#         r2_key
#     ))
#     conn.commit()
#     cursor.close()
#     conn.close()

def make_json_serializable(record: dict) -> dict:
    """Convert all datetime objects in the record to ISO strings"""
    serializable = {}
    for k, v in record.items():
        if isinstance(v, datetime):
            serializable[k] = v.isoformat()  # e.g., "2020-06-25T17:37:36"
        else:
            serializable[k] = v
    return serializable

def safe_parse_date(value):
    """Flexible date parser for Bill_Date__c"""
    if isinstance(value, datetime):
        return value
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(value[:10], fmt)
        except Exception:
            continue
    return None

# ============================================================
# 🚀 ROUTE: Store Range to R2 + MySQL
# ============================================================

@router.post("/create")
def transaction(
    start_range: int = Query(0, description="Start row (e.g. 0)"),
    end_range: int = Query(100000, description="End row (e.g. 100000)")
):
    total_start = time.time()
    mysql_creds = MysqlCatalog()  # <-- Your existing MySQL data fetcher class

    dbname = "Transaction"
    namespace, table_name = "pos_transactions", "transaction"

    # --------------------------------------------------
    # 1️⃣ Fetch data from MySQL source
    # --------------------------------------------------
    try:
        rows = mysql_creds.get_range(dbname, start_range, end_range)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")

    if not rows:
        raise HTTPException(status_code=404, detail="No data found in the given range.")

    # --------------------------------------------------
    # 2️⃣ Process and normalize rows
    # --------------------------------------------------
    converted_rows = []
    for row in rows:
        bill_date = row.get("Bill_Date__c", "")
        dt = safe_parse_date(bill_date)
        if dt:
            row["year"], row["month"], row["day"] = dt.year, dt.month, dt.day
        else:
            row["year"], row["month"], row["day"] = None, None, None
        converted_rows.append(row)

    # --------------------------------------------------
    # 3️⃣ Store to R2 and index metadata
    # --------------------------------------------------
    stored_count = 0
    for record in converted_rows:
        try:
            customer_mobile = record["customer_mobile__c"]
            pri_id = record["pri_id"]
            year = record.get("year") or "unknown"
            month = record.get("month") or "unknown"
            r2_id_key = f"{namespace}/{table_name}/id/{pri_id}.json"
            r2_cm_key = f"{namespace}/{table_name}/phone/{year}/{month}/{customer_mobile}.json"
            r2_pym_key = f"{namespace}/{table_name}/phone_year_month/{customer_mobile}/{year}/{month}/{pri_id}.json"
            # or
            # r2_id_key = f"{namespace}/{table_name}/id/{year}/{month}/{pri_id}.json"
            # r2_cm_key = f"{namespace}/{table_name}/phone/{customer_mobile}.json"

            record_safe = make_json_serializable(record)

            # Upload JSON to R2
            # store_json_to_r2(record, r2_id_key)
            # store_json_to_r2(record, r2_cm_key)
            # Upload JSON to R2
            store_json_to_r2(record_safe, r2_id_key)
            store_json_to_r2(record_safe, r2_cm_key)
            store_json_to_r2(record_safe, r2_pym_key)
            # Insert metadata
            # insert_metadata(record, r2_id_key)
            # insert_metadata(record, r2_cm_key)

            stored_count += 1
        except Exception as e:
            print(f"⚠️ Error saving pri_id={record.get('pri_id')}: {e}")
            continue

    elapsed = round(time.time() - total_start, 2)

    return {
        "status": "success",
        "namespace": namespace,
        "table": table_name,
        "rows_processed": len(rows),
        "rows_stored": stored_count,
        "elapsed_seconds": elapsed
    }

@router.get("/fetch")
def fetch_r2_data(
    pri_id: int = Query(None, description="PRI ID of the record"),
    customer_mobile: str = Query(None, description="Customer mobile number"),
    year: str = Query(None, description="Year partition (optional)"),
    month: str = Query(None, description="Month partition (optional)")
):
    """
    Fetch JSON from R2 by pri_id or customer_mobile.
    - If pri_id is provided, fetch by pri_id.
    - If customer_mobile is provided, fetch by phone.
    """

    namespace, table_name = "pos_transactions", "transaction"

    if not pri_id and not customer_mobile:
        raise HTTPException(status_code=400, detail="Provide either pri_id or customer_mobile.")

    results = []

    try:
        if pri_id:
            # Key by pri_id
            r2_id_key = f"{namespace}/{table_name}/id/{pri_id}.json"
            data = fetch_json_from_r2(r2_id_key)
            if data:
                results.append(data)

        if customer_mobile:
            # Key by customer_mobile
            # If year/month are provided, use partitioned path
            if year and month:
                r2_cm_key = f"{namespace}/{table_name}/phone/{year}/{month}/{customer_mobile}.json"
                data = fetch_json_from_r2(r2_cm_key)
                if data:
                    results.append(data)
            else:
                # Fallback: fetch by phone without partitions
                r2_cm_key = f"{namespace}/{table_name}/phone/{customer_mobile}.json"
                data = fetch_json_from_r2(r2_cm_key)
                if data:
                    results.append(data)

        if not results:
            raise HTTPException(status_code=404, detail="No data found in R2 for given keys.")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"R2 fetch error: {str(e)}")

    return {
        "status": "success",
        "records_found": len(results),
        "data": results
    }