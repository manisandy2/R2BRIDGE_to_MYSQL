# import pandas as pd
# # ##########
# data = pd.read_parquet(r"json_backups/ofs.parquet")
# print(len(data))
# print(data)
# print(data["pri_id"])
#########
# for i in range(1,1000000):
#     print(i)
#
# for index, row in data:
#     print(row)

# print(data.get("Bill_Date__c"))

# for date in data:
#     print(date)
import time
# for da in data.index:
#     print(da)
    # time.sleep(1)
##########
# import pyarrow as pa
# from fastavro import reader
#
# with open(r"json_backups/m5.avro", "rb") as f:
#     rows = list(reader(f))
#
# table = pa.Table.from_pylist(rows)
# print("#"*100)
# # print(table)
# print(table.schema.names)
# df = table.to_pandas()
# print(df.head(50))          # first 50 rows
# print(len(df))
# print("#"*100)
# for col in table.column_names:
#     print(f"\nCOLUMN: {col}")
#     # show first 10 values
#     print(table[col].to_pylist()[:10])
# print("#"*100)
# print(table.num_rows)

# import pyarrow as pa
# from fastavro import reader
#
# with open("json_backups/m5.avro", "rb") as f:
#     rows = list(reader(f))
#
# table = pa.Table.from_pylist(rows)
#
# print("TOTAL ROWS:", table.num_rows)
#
# print("\nCOLUMNS:")
# for col in table.column_names:
#     print("-", col)
#
# print("\nCOLUMN + first 10 values each")
# for col in table.column_names:
#     print(f"\n{col}: {table[col].to_pylist()[:10]}")


##########################################
import json

# import pandas as pd
# #
# df = pd.read_json(r"json_backups/001.metadata.json")
# print(df)
######################
# import gzip
# import json
# import pandas as pd
# #
#  # with in partition
# # with gzip.open("json_backups/Test01", "rb") as f:
# #     data = json.loads(f.read().decode("utf-8"))   # decompress + decode
#
# # # # # with out partition
# with gzip.open(r"json_backups/001.metadata.json", "rb") as f:
#     data = json.loads(f.read().decode("utf-8"))   # decompress + decode
# # #
# # #
# df = pd.json_normalize(data)   # flatten into DataFrame
# # print(df.head())
# # #
# # for col,index in df:
# #     print(col,index)
# # print("start ...")
# for col in df.columns:
#
#     for idx in df.index:
#         print(col, idx, df.loc[idx, col])
#         print("*"*100)
#################################################################################
import gzip
import json
import pandas as pd
from pathlib import Path
from typing import Dict, Any


def load_gzipped_json(filepath: str) -> Dict[str, Any]:
    """Load and parse a gzipped JSON file."""
    with gzip.open(filepath, "rb") as f:
        return json.loads(f.read().decode("utf-8"))


def process_metadata(filepath: str) -> None:
    """
    Process metadata from a gzipped JSON file and print its contents.

    Args:
        filepath: Path to the gzipped JSON file
    """
    # Input validation
    if not Path(filepath).exists():
        print(f"Error: File not found: {filepath}")
        return

    try:
        # Load and normalize data
        data = load_gzipped_json(filepath)
        df = pd.json_normalize(data)

        # Print basic info
        print(f"Processing file: {filepath}")
        print(f"Total rows: {len(df)}")
        print(f"Columns: {', '.join(df.columns)}\n")

        # Iterate through DataFrame more efficiently
        for idx, row in df.iterrows():
            print(f"--- Row {idx} ---")
            for col in df.columns:
                print(f"{col}: {row[col]}")
            print("-" * 80)

    except Exception as e:
        print(f"Error processing file: {e}")


if __name__ == "__main__":
    # Example usage
    file_path = "json_backups/001.metadata.json"
    process_metadata(file_path)