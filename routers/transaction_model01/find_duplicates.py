from pyiceberg.catalog import load_catalog
from core.catalog_client import  get_catalog_client
from collections import defaultdict
from collections import Counter

catalog = get_catalog_client()

table = catalog.load_table("pos_transactions.transaction")

# batch_size = 100_000  # process 100k rows at a time
# pri_id_counts = defaultdict(int)
#
# # Scan table
# scan = table.scan().select("pri_id")
#
# # iter_batches() yields batches as lists of dicts
# for batch in scan.iter_batches(batch_size=batch_size):
#     for row in batch:
#         pri_id = row["pri_id"]
#         pri_id_counts[pri_id] += 1
#
# # Extract duplicates
# duplicates_list = [
#     {"pri_id": pri_id, "count": count}
#     for pri_id, count in pri_id_counts.items() if count > 1
# ]
#
# print(f"Total duplicate pri_id rows: {len(duplicates_list)}")

# catalog = get_catalog_client()
# table = catalog.load_table("pos_transactions.transaction")

rows = table.scan().select("pri_id").to_pylist()  # pulls all rows into memory
pri_id_counts = Counter(row["pri_id"] for row in rows)

duplicates_list = [
    {"pri_id": k, "count": v} for k, v in pri_id_counts.items() if v > 1
]

print(f"Total duplicate pri_id rows: {len(duplicates_list)}")