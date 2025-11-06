import pandas as pd
##########
# data = pd.read_parquet(r"json_backups/03.parquet")
# print(len(data))
##########
# for i in range(1,1000000):
#     print(i)

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
# with open(r"json_backups/m0.avro", "rb") as f:
#     rows = list(reader(f))
#
# table = pa.Table.from_pylist(rows)
# print("#"*100)
# print(table)
# print("#"*100)
# print(table.num_rows)
#####################
import json

# import pandas as pd
#
# df = pd.read_json(r"json_backups/gz.metadata.json")
# print(df)
######################
import gzip
import json
# import pandas as pd

with gzip.open("json_backups/05.metadata.json", "rb") as f:
    data = json.loads(f.read().decode("utf-8"))   # decompress + decode

df = pd.json_normalize(data)   # flatten into DataFrame
# print(df.columns)

# for col,index in df:
#     print(col,index)
print("start ...")
for col in df.columns:

    for idx in df.index:
        print(col, idx, df.loc[idx, col])
        print("*"*100)