from duckdb.duckdb import limit

from core.r2_client import get_r2_client
import pyarrow.fs as fs
import os

def get_s3_fs():
    return fs.S3FileSystem(
        access_key=os.getenv("ACCESS_KEY_ID"),
        secret_key=os.getenv("SECRET_ACCESS_KEY"),
        endpoint_override=os.getenv("ENDPOINT"),
    )

from datetime import date, datetime
from decimal import Decimal
import math

def make_json_safe(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj


def read_parquet_and_save_to_mobile(
    # path: str = Query(..., description="Full s3:// R2 parquet path"),
    # mobile: str = Query(..., description="Mobile number for bucket folder"),
    # limit: int = Query(1000, description="Rows to convert (0 = full file)")
):
    path = ""
    limit = 0
    """
    1. Reads parquet file from R2
    2. Converts rows to JSON-safe values
    3. Saves JSON output into R2 under mobile/<mobile>/parquet-json/
    """
    import pyarrow.parquet as pq
    import json
    import math
    from datetime import date, datetime
    from decimal import Decimal
    from collections import defaultdict

    try:
        s3fs = get_s3_fs()

        # Remove s3:// prefix
        if path.startswith("s3://"):
            path = path.replace("s3://", "", 1)

        pq_file = pq.ParquetFile(path, filesystem=s3fs)

        rows = []
        rows_read = 0

        # ----------- READ BATCHES SAFELY -----------
        for batch in pq_file.iter_batches(batch_size=1000):
            df = batch.to_pandas()

            # Convert every cell to JSON-safe value
            df = df.applymap(make_json_safe)

            part = df.to_dict(orient="records")

            # Limit rows if requested
            if limit > 0:
                remaining = limit - rows_read
                rows.extend(part[:remaining])
                rows_read += len(part[:remaining])
                if rows_read >= limit:
                    break
            else:
                rows.extend(part)

        mobile_groups = defaultdict(list)

        for row in rows:
            mobile = row.get("customer_mobile__c", "unknown")
            mobile = str(mobile).replace("/", "_").strip()
            mobile_groups[mobile].append(row)



        # ----------- CONVERT TO JSON STRING -----------
        json_data = json.dumps(rows, indent=2)

        # ----------- SAVE TO R2 BUCKET -----------
        r2 = get_r2_client()
        bucket = "dev-transaction"
        # print("data",json_data.get('customer_mobile__c'))
        saved_files = []

        for mobile, items in mobile_groups.items():
            key = f"mobile/{mobile}/{mobile}.json"
            json_body = json.dumps(items, indent=2)

            r2.put_object(
                Bucket=bucket,
                Key=key,
                Body=json_body.encode("utf-8"),
                ContentType="application/json"
            )

            saved_files.append(f"r2://{bucket}/{key}")

        return {
            "status": "success",
            "source_parquet": path,
            "unique_mobiles": list(mobile_groups.keys()),
            "files_saved": saved_files,
            "total_rows_processed": len(rows),
            "total_rows_in_file": pq_file.metadata.num_rows
        }

    except Exception as e:
        print("error",{e})