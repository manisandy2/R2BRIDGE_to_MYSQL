# import os
# import json
# import math
# from datetime import date, datetime
# from decimal import Decimal
# from collections import defaultdict
# import time
#
# import pyarrow.fs as fs
# import pyarrow.parquet as pq
# from core.r2_client import get_r2_client
#
# LOG_FILE = "mobile_parquet.log"
#
# def write_log(message: str):
#     timestamp = datetime.now().isoformat()
#     with open(LOG_FILE, "a") as f:
#         f.write(f"[{timestamp}] {message}\n")
#
# # -----------------------------
# # S3 FS INIT
# # -----------------------------
# def get_s3_fs():
#     return fs.S3FileSystem(
#         access_key=os.getenv("ACCESS_KEY_ID"),
#         secret_key=os.getenv("SECRET_ACCESS_KEY"),
#         endpoint_override=os.getenv("ENDPOINT"),
#     )
#
#
# # -----------------------------
# # JSON SAFE CONVERTER
# # -----------------------------
# def make_json_safe(obj):
#     if isinstance(obj, (datetime, date)):
#         return obj.isoformat()
#     if isinstance(obj, Decimal):
#         return float(obj)
#     if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
#         return None
#     return obj
#
#
# # -----------------------------
# # MAIN FUNCTION
# # -----------------------------
# def read_parquet_and_save_to_mobile():
#
#     # INPUT PARQUET PATH
#     start_time = time.time()
#     path = "s3://dev-transaction/__r2_data_catalog/019a7771-98bc-79c0-b5f6-c23d4f722f45/019a7771-9ba6-7811-b657-63b5eee83b3b/data/year=2025/00000-0-5c235710-64fb-4de8-a004-61135e53aaff.parquet"
#     # print("Reading path:", path)
#     write_log(f"START processing parquet: {path}")
#
#     limit = 0  # 0 = read full file
#
#     try:
#         s3fs = get_s3_fs()
#
#         # Strip s3:// prefix → required for pyarrow
#         if path.startswith("s3://"):
#             path = path.replace("s3://", "", 1)
#
#         # Load parquet file metadata + reader
#         pq_file = pq.ParquetFile(path, filesystem=s3fs)
#         rows = []
#         total_rows_read = 0
#         # print("Reading parquet file:", pq_file.iter_batches(batch_size=2000))
#         # -----------------------------
#         # READ PARQUET IN BATCHES
#         # -----------------------------
#         for batch in pq_file.iter_batches(batch_size=2000):
#         # for batch in pq_file():
#             df = batch.to_pandas()
#             # print("data",df)
#             # Convert all values to JSON-safe
#             df = df.applymap(make_json_safe)
#
#             part = df.to_dict(orient="records")
#
#             # Apply limit
#             if limit > 0:
#                 remaining = limit - total_rows_read
#                 rows.extend(part[:remaining])
#                 total_rows_read += len(part[:remaining])
#                 if total_rows_read >= limit:
#                     break
#             else:
#                 rows.extend(part)
#         write_log(f"Rows loaded from parquet: {len(rows)}")
#         # -----------------------------
#         # GROUP BY MOBILE NUMBER
#         # -----------------------------
#         mobile_groups = defaultdict(list)
#
#         for row in rows:
#             mobile = row.get("customer_mobile__c", "unknown")
#             mobile = str(mobile).replace("/", "_").strip()
#             mobile_groups[mobile].append(row)
#             # print("row",row)
#
#         # -----------------------------
#         # SAVE JSON TO R2 PER MOBILE
#         # -----------------------------
#         r2 = get_r2_client()
#         bucket = "dev-transaction"
#
#         saved_files = []
#         i = 0
#         for mobile, items in mobile_groups.items():
#             i = i +1
#             print(i)
#             key = f"mobile/{mobile}/{mobile}.json"
#
#             json_body = json.dumps(items, indent=2)
#
#             r2.put_object(
#                 Bucket=bucket,
#                 Key=key,
#                 Body=json_body.encode("utf-8"),
#                 ContentType="application/json"
#             )
#
#             saved_files.append(f"r2://{bucket}/{key}")
#             write_log(f"{items}Saved mobile={mobile}, rows={len(items)}, file={key}")
#             print(f"Saved mobile={mobile}, rows={len(items)}, file={key}")
#
#         # -----------------------------
#         # RETURN SUMMARY
#         # -----------------------------
#         result = {
#             "status": "success",
#             "source_parquet": path,
#             "unique_mobiles": list(mobile_groups.keys()),
#             "files_saved": saved_files,
#             "total_rows_processed": len(rows),
#             "total_rows_in_file": pq_file.metadata.num_rows
#         }
#
#         print(json.dumps(result, indent=2))
#         print("Total rows processed:", pq_file.metadata.num_rows)
#         end_time = time.time()
#         print("Total time taken:", end_time - start_time)
#         return result
#
#     except Exception as e:
#         print("ERROR:", str(e))
#
#
# # -----------------------------
# # RUN
# # -----------------------------
# read_parquet_and_save_to_mobile()


import os
import json
import math
from datetime import date, datetime
from decimal import Decimal
from collections import defaultdict
import time
import multiprocessing as mp

import pyarrow.fs as fs
import pyarrow.parquet as pq
from core.r2_client import get_r2_client

LOG_FILE = "mobile_parquet.log"

def write_log(message: str):
    timestamp = datetime.now().isoformat()
    with open(LOG_FILE, "a") as f:
        f.write(f"[{timestamp}] {message}\n")


def get_s3_fs():
    return fs.S3FileSystem(
        access_key=os.getenv("ACCESS_KEY_ID"),
        secret_key=os.getenv("SECRET_ACCESS_KEY"),
        endpoint_override=os.getenv("ENDPOINT"),
    )


def make_json_safe(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj


# -----------------------------
# WORKER FUNCTION (multiprocessing)
# -----------------------------
def process_mobile_group(args):
    mobile, items = args

    start = time.time()

    # Each worker must initialize its own R2 client
    r2 = get_r2_client()
    bucket = "dev-transaction"

    key = f"mobile/{mobile}/{mobile}.json"
    json_body = json.dumps(items, indent=2)

    r2.put_object(
        Bucket=bucket,
        Key=key,
        Body=json_body.encode("utf-8"),
        ContentType="application/json"
    )

    elapsed = round((time.time() - start) * 1000, 2)

    write_log(
        f"Saved mobile={mobile}, rows={len(items)}, file={key}, time={elapsed} ms"
    )

    print(f"[Worker] Saved {mobile}, rows={len(items)}, time={elapsed} ms")

    return f"r2://{bucket}/{key}"


# -----------------------------
# MAIN FUNCTION
# -----------------------------
def read_parquet_and_save_to_mobile():
    start_time = time.time()

    path = "s3://dev-transaction/__r2_data_catalog/019a7771-98bc-79c0-b5f6-c23d4f722f45/019a7771-9ba6-7811-b657-63b5eee83b3b/data/year=2025/00000-0-5c235710-64fb-4de8-a004-61135e53aaff.parquet"
    write_log(f"START processing parquet: {path}")

    limit = 0

    try:
        s3fs = get_s3_fs()

        if path.startswith("s3://"):
            path = path.replace("s3://", "", 1)

        pq_file = pq.ParquetFile(path, filesystem=s3fs)
        rows = []

        for batch in pq_file.iter_batches(batch_size=2000):
            df = batch.to_pandas()
            df = df.applymap(make_json_safe)
            rows.extend(df.to_dict(orient="records"))

        print(rows)
        print("count:",len(rows))
        write_log(f"Rows loaded from parquet: {len(rows)}")

        # -----------------------------
        # GROUP BY MOBILE
        # -----------------------------
        mobile_groups = defaultdict(list)
        for row in rows:
            mobile = str(row.get("customer_mobile__c", "unknown")).replace("/", "_").strip()
            mobile_groups[mobile].append(row)

        write_log(f"Unique mobiles: {len(mobile_groups)}")

        # -----------------------------
        # MULTIPROCESSING STARTS
        # -----------------------------
        pool = mp.Pool(mp.cpu_count())   # use all CPU cores
        print(f"Using {mp.cpu_count()} processors...")

        results = pool.map(process_mobile_group, mobile_groups.items())

        pool.close()
        pool.join()

        # -----------------------------
        # SUMMARY
        # -----------------------------
        total_time = round(time.time() - start_time, 2)

        result = {
            "status": "success",
            "source_parquet": path,
            "unique_mobiles": list(mobile_groups.keys()),
            "files_saved": results,
            "total_rows_processed": len(rows),
            "total_rows_in_file": pq_file.metadata.num_rows,
            "time_taken_sec": total_time
        }

        write_log(f"FINISH processing: {total_time} sec")
        write_log("------")

        print(json.dumps(result, indent=2))
        return result

    except Exception as e:
        write_log(f"ERROR: {str(e)}")
        print("ERROR:", str(e))


# RUN
# if __name__ == "__main__":
read_parquet_and_save_to_mobile()