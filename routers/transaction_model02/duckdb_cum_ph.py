# import duckdb
# from fastapi import APIRouter, HTTPException
# from ...core.catalog_client import get_catalog_client
# router = APIRouter(prefix="/duckdb", tags=["DuckDB"])
# from fastapi import APIRouter, HTTPException, Query
#
# # @router.post("/manual-create-duckdb-table")
# # def create_transaction_duckdb():
# #     """
# #     Create a DuckDB table and a view that simulates Iceberg-style partitions.
# #     """
# #     namespace = "pos_transactions01"
# #     table_name = "transaction_duck01"
# #     view_name = f"{table_name}_view"
# #     table_identifier = f"{namespace}.{table_name}"
# #     view_identifier = f"{namespace}.{view_name}"
# #     db_path = "data/catalog.db"
# #
# #     try:
# #         # Connect to DuckDB
# #         con = duckdb.connect(database=db_path, read_only=False)
# #
# #         # Create schema
# #         con.execute(f"CREATE SCHEMA IF NOT EXISTS {namespace}")
# #
# #         # Create base table
# #         create_table_sql = f"""
# #         CREATE TABLE IF NOT EXISTS {table_identifier} (
# #             pri_id BIGINT PRIMARY KEY,
# #             store_code__c VARCHAR,
# #             Branch_Name__c VARCHAR,
# #             customer_mobile__c BIGINT,
# #             Customer_Name__c VARCHAR,
# #             Bill_No__c VARCHAR,
# #             Bill_Date__c TIMESTAMP,
# #             Invoice_Date__c TIMESTAMP,
# #             Invoice_Amount__c DOUBLE,
# #             bill_status__c VARCHAR,
# #             bill_transaction_no__c VARCHAR,
# #             Item_Code__c BIGINT,
# #             Item_Name__c VARCHAR,
# #             bill_tax__c DOUBLE,
# #             bill_grand_total__c DOUBLE,
# #             CreatedDate TIMESTAMP
# #         );
# #         """
# #         con.execute(create_table_sql)
# #
# #         # Create a view that adds partition-equivalent computed columns
# #         create_view_sql = f"""
# #         CREATE OR REPLACE VIEW {view_identifier} AS
# #         SELECT
# #             *,
# #             DATE_TRUNC('day', Bill_Date__c) AS day,
# #             HASH(store_code__c) % 32 AS store_bucket,
# #             HASH(customer_mobile__c) % 32 AS customer_bucket
# #         FROM {table_identifier};
# #         """
# #         con.execute(create_view_sql)
# #         con.close()
# #
# #         return {
# #             "status": "success",
# #             "table": table_identifier,
# #             "view": view_identifier,
# #             "message": "DuckDB table and partition view created successfully",
# #             "partition_equivalents": ["day", "store_bucket", "customer_bucket"]
# #         }
# #
# #     except Exception as e:
# #         raise HTTPException(status_code=500, detail=f"DuckDB table creation failed: {str(e)}")
#
#
# @router.post("/manual-create-duckdb-table")
# def create_transaction_duckdb():
#     """
#     Generate an Iceberg-compatible CREATE TABLE DDL
#     based on the DuckDB table schema.
#     """
#     namespace = "pos_transactions01"
#     table_name = "transaction_duck01"
#     table_identifier = f"{namespace}.{table_name}"
#     db_path = "data/catalog.db"
#
#     try:
#         # === Iceberg DDL equivalent ===
#         iceberg_table_identifier = "prod.db.transaction_duck01"
#         iceberg_create_sql = f"""
#             CREATE TABLE {iceberg_table_identifier} (
#                 pri_id BIGINT,
#                 store_code__c STRING,
#                 Branch_Name__c STRING,
#                 customer_mobile__c BIGINT,
#                 Customer_Name__c STRING,
#                 Bill_No__c STRING,
#                 Bill_Date__c TIMESTAMP,
#                 Invoice_Date__c TIMESTAMP,
#                 Invoice_Amount__c DOUBLE,
#                 bill_status__c STRING,
#                 bill_transaction_no__c STRING,
#                 Item_Code__c BIGINT,
#                 Item_Name__c STRING,
#                 bill_tax__c DOUBLE,
#                 bill_grand_total__c DOUBLE,
#                 CreatedDate TIMESTAMP
#             )
#             USING iceberg
#             PARTITIONED BY (
#                 days(Bill_Date__c),
#                 bucket(32, store_code__c),
#                 bucket(32, customer_mobile__c)
#             );
#             """
#
#         return {
#             "status": "success",
#             "duckdb_table": table_identifier,
#             "message": "Iceberg DDL generated successfully",
#             "partition_equivalents": ["days(Bill_Date__c)", "bucket(32, store_code__c)", "bucket(32, customer_mobile__c)"],
#             "iceberg_ddl": iceberg_create_sql.strip()
#         }
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"DuckDB table creation failed: {str(e)}")
#
#
# @router.get("/read-duckdb")
# def read_duckdb_table(
#     schema: str = Query("pos_transactions01", description="DuckDB schema name"),
#     table: str = Query("transaction_duck01", description="DuckDB table name"),
#     limit: int = Query(10, description="Number of rows to fetch")
# ):
#     """
#     Read data from a DuckDB table or view with optional row limit.
#     """
#     db_path = "data/catalog.db"
#     table_identifier = f"{schema}.{table}"
#
#     try:
#         # Connect to DuckDB
#         con = duckdb.connect(database=db_path, read_only=True)
#
#         # Verify table existence
#         result = con.execute(f"""
#             SELECT table_name
#             FROM information_schema.tables
#             WHERE table_schema = '{schema}' AND table_name = '{table}'
#         """).fetchall()
#
#         if not result:
#             raise HTTPException(status_code=404, detail=f"Table {table_identifier} not found")
#
#         # Fetch data
#         query = f"SELECT * FROM {table_identifier} LIMIT {limit};"
#         rows = con.execute(query).fetchdf()
#
#         con.close()
#
#         # Convert to list of dicts for JSON response
#         return {
#             "status": "success",
#             "table": table_identifier,
#             "rows_fetched": len(rows),
#             "data": rows.to_dict(orient="records")
#         }
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed to read DuckDB table: {str(e)}")