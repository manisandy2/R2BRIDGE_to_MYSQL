import mysql.connector
import os
import mysql
from dotenv import load_dotenv
import pandas as pd



load_dotenv()

# MySQL connection
print("Host",os.getenv("HOST"))
conn = mysql.connector.connect(
    host=os.getenv("HOST"),
    user=os.getenv("MYSQL_USER"),
    password=os.getenv("PASSWORD"),
    database=os.getenv("DATABASE"),
    port=3306
)
# cursor = conn.cursor()

# batch_size = 5
# total_records = 4000000
# num_batches = total_records // batch_size  # 80 batches

# for i in range(num_batches):
    
#     offset = i * batch_size
#     # query = f"SELECT * FROM your_table ORDER BY id LIMIT {batch_size} OFFSET {offset}"
#     # query = f"SELECT * FROM {'Transaction'} ORDER BY pri_id LIMIT %s, %s"
#     query = "SELECT * FROM `Transaction` ORDER BY pri_id LIMIT %s, %s"
#     cursor.execute(query, (offset, batch_size))  # pass parameters here
#     data = cursor.fetchall()
#     print(data)

#     # Fetches 50,000 rows per batch.
#     if data:
#         df = pd.DataFrame(data)
#         file_name = f"Transaction_batch_{i+1}.xlsx"
#         df.to_excel(file_name, index=False)

#     cursor.execute(query)
#     data = cursor.fetchall()
    
#     print(f"Batch {i+1} fetched, records: {len(data)}")
#     # Process your data here

# cursor.close()
# conn.close()


cursor = conn.cursor(dictionary=True)

batch_size = 5
step = 50000
total_records = 40000000
num_batches = total_records // step + 1  # number of intervals

all_data = []  # to collect all batches

for i in range(num_batches):
    start = i * step
    end = start + batch_size
    query = "SELECT * FROM `Transaction` ORDER BY pri_id LIMIT %s, %s"
    cursor.execute(query, (start, batch_size))
    data = cursor.fetchall()
    
    if data:
        all_data.extend(data)
        print(f"Fetched rows {start + 1} to {start + batch_size}, records: {len(data)}")

# Save all collected data into a single Excel file
df = pd.DataFrame(all_data)
df.to_excel("Transaction_sampled.xlsx", index=False)
print("All data saved to Transaction_sampled.xlsx")


cursor.close()
conn.close()