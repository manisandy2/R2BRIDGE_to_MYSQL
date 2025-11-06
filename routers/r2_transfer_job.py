import pyiceberg
print(pyiceberg.__version__)

# import requests
# import time
# import logging
# from datetime import datetime
#
# # ------------------ CONFIGURATION ------------------
#
# # API_URL = "http://localhost:8000/transaction/create"  # Your FastAPI endpoint
# # API_URL = "http://127.0.0.1:8000/create"  # Your FastAPI endpoint
# # API_URL = "http://127.0.0.1:8000/bucket_data_store/create"  # Your FastAPI endpoint
# # API_URL = "http://127.0.0.1:8000/transaction/create"  # Your FastAPI endpoint
# API_URL = "http://127.0.0.1:8000/insert-ph-data"  # Your FastAPI endpoint
# BATCH_SIZE = 100                                    # Rows per batch
# # BATCH_SIZE = 5                                    # Rows per batch
# TOTAL_ROWS = 40000000                                 # 4 crore rows
# # TOTAL_ROWS = 40942768                                 # 4 crore rows
# # TOTAL_ROWS = 10                                 # 4 crore rows
# MAX_RETRIES = 3                                       # Retry count per batch
# SLEEP_BETWEEN_BATCHES = 2                             # Seconds
# LOG_FILE = "r2_transfer_bucket.log"                          # Log file name
#
# # ------------------ LOGGING SETUP ------------------
#
# logging.basicConfig(
#     filename=LOG_FILE,
#     level=logging.INFO,
#     format="%(asctime)s [%(levelname)s] %(message)s",
# )
#
# # ------------------ MAIN FUNCTION ------------------
#
# def transfer_batches():
#     # start = 19700000
#     start =   0
#     batch_no = 1
#     total_batches = TOTAL_ROWS // BATCH_SIZE
#
#     logging.info(f"🚀 Starting R2 Catalog Data Transfer")
#     logging.info(f"Total rows: {TOTAL_ROWS:,} | Batch size: {BATCH_SIZE:,} | Total batches: {total_batches}")
#
#     while start < TOTAL_ROWS:
#         end = start + BATCH_SIZE
#         print(f"🟡 Processing Batch {batch_no}/{total_batches} → Rows {start:,} to {end:,}")
#
#         success = False
#         for attempt in range(1, MAX_RETRIES + 1):
#             try:
#                 response = requests.post(API_URL, params={"start_range": start, "end_range": end}, timeout=1800)
#                 if response.status_code == 200:
#                     result = response.json()
#                     success = True
#                     logging.info(f"✅ Batch {batch_no} Success | Rows {result.get('rows_written')} | Time {result.get('elapsed_seconds')}s")
#                     print(f"✅ Batch {batch_no} Completed ({result.get('elapsed_seconds')}s)")
#                     break
#                 else:
#                     logging.warning(f"⚠️ Batch {batch_no} Failed (Attempt {attempt}) | HTTP {response.status_code}")
#                     print(f"⚠️ Batch {batch_no} failed: HTTP {response.status_code}")
#             except Exception as e:
#                 logging.error(f"❌ Batch {batch_no} Error (Attempt {attempt}): {str(e)}")
#                 print(f"❌ Batch {batch_no} Error: {str(e)}")
#             time.sleep(5)
#
#         if not success:
#             logging.error(f"🚫 Batch {batch_no} permanently failed after {MAX_RETRIES} retries.")
#             print(f"🚫 Batch {batch_no} permanently failed. Skipping...")
#
#         start += BATCH_SIZE
#         batch_no += 1
#         time.sleep(SLEEP_BETWEEN_BATCHES)
#
#     logging.info("🏁 All batches processed.")
#     print("🏁 Transfer complete! Check r2_transfer.log for details.")
#
#
# # ------------------ EXECUTE ------------------
#
# if __name__ == "__main__":
#     transfer_batches()

###################################################
# Bucket
import requests
import time
import math
import logging
from datetime import datetime

# ------------------ CONFIGURATION ------------------

# API_URL = "http://127.0.0.1:8000/transaction/create-pri-id"  # FastAPI endpoint
# API_URL = "http://127.0.0.1:8000/insert-ph-data"  # FastAPI endpoint
API_URL = "http://127.0.0.1:8000/insert-ph-direct-data"  # FastAPI endpoint
# BATCH_SIZE = 1
BATCH_SIZE = 100000
# BATCH_SIZE = 10
# TOTAL_ROWS = 100000
TOTAL_ROWS = 100000
# TOTAL_ROWS = 400000
MAX_RETRIES = 3
SLEEP_BETWEEN_BATCHES = 2

SUCCESS_LOG_FILE = "phone/r2_transfer_bucket_phone.log"
FAILED_LOG_FILE = "phone/r2_transfer_failed_phone.log"

# ------------------ LOGGING SETUP ------------------

# Success logger
success_logger = logging.getLogger("success_logger")
success_handler = logging.FileHandler(SUCCESS_LOG_FILE)
success_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
success_handler.setFormatter(success_formatter)
success_logger.addHandler(success_handler)
success_logger.setLevel(logging.INFO)

# Failed logger
failed_logger = logging.getLogger("failed_logger")
failed_handler = logging.FileHandler(FAILED_LOG_FILE)
failed_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
failed_handler.setFormatter(failed_formatter)
failed_logger.addHandler(failed_handler)
failed_logger.setLevel(logging.ERROR)

# ------------------ MAIN FUNCTION ------------------

def transfer_batches():
    session = requests.Session()
    start = 0
    batch_no = 1
    total_batches = math.ceil(TOTAL_ROWS / BATCH_SIZE)
    success_batches = 0
    failed_batches = 0

    success_logger.info(f"🚀 Starting R2 Catalog Data Transfer")
    success_logger.info(f"Total Rows: {TOTAL_ROWS:,} | Batch Size: {BATCH_SIZE:,} | Total Batches: {total_batches}")

    while start < TOTAL_ROWS:
        end = min(start + BATCH_SIZE, TOTAL_ROWS)
        range_info = f"Rows {start:,}–{end:,}"
        print(f"🟡 Processing Batch {batch_no}/{total_batches} → {range_info}")

        success = False

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                batch_start_time = time.time()
                response = session.post(API_URL, params={"start_range": start, "end_range": end}, timeout=1800)

                if response.status_code == 200:
                    result = response.json()
                    elapsed = round(time.time() - batch_start_time, 2)
                    success_logger.info(f"✅ Batch {batch_no} Success | {range_info} | Rows Written: {result.get('rows_written')} | Time: {elapsed}s")
                    print(f"✅ Batch {batch_no} Completed in {elapsed}s")
                    success = True
                    success_batches += 1
                    break
                else:
                    print(f"⚠️ Batch {batch_no} Failed (Attempt {attempt}) | HTTP {response.status_code}")
                    success_logger.warning(f"⚠️ Batch {batch_no} Failed | {range_info} | Attempt {attempt} | HTTP {response.status_code}")
            except Exception as e:
                print(f"❌ Batch {batch_no} Error (Attempt {attempt}): {str(e)}")
                failed_logger.error(f"❌ Batch {batch_no} Error | {range_info} | Attempt {attempt} | Error: {str(e)}")
            time.sleep(5)

        if not success:
            failed_logger.error(f"🚫 Batch {batch_no} permanently failed after {MAX_RETRIES} retries | {range_info}")
            print(f"🚫 Batch {batch_no} permanently failed. Skipping...")
            failed_batches += 1

        start += BATCH_SIZE
        batch_no += 1
        time.sleep(SLEEP_BETWEEN_BATCHES)

    summary_msg = f"🏁 Transfer Completed | Success: {success_batches} | Failed: {failed_batches}"
    print(summary_msg)
    success_logger.info(summary_msg)
    failed_logger.info(summary_msg)


# ------------------ EXECUTE ------------------

if __name__ == "__main__":
    transfer_batches()