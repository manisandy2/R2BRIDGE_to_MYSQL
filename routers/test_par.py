# import pandas as pd
# ##########
# data = pd.read_parquet(r"json_backups/02.parquet")
# print(len(data))
# # print(data["Bill_Date__c"])
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


#####################
import json

# import pandas as pd
# #
# df = pd.read_json(r"json_backups/01.metadata.json")
# print(df)
######################
import gzip
import json
import pandas as pd
#
 # with in partition
with gzip.open("json_backups/01.metadata.json", "rb") as f:
    data = json.loads(f.read().decode("utf-8"))   # decompress + decode

# # # # with out partition
# # with gzip.open("json_backups/04.metadata.json", "rb") as f:
# #     data = json.loads(f.read().decode("utf-8"))   # decompress + decode
# #
# #
df = pd.json_normalize(data)   # flatten into DataFrame
# print(df.head())
# #
# for col,index in df:
#     print(col,index)
# print("start ...")
for col in df.columns:

    for idx in df.index:
        print(col, idx, df.loc[idx, col])
        print("*"*100)