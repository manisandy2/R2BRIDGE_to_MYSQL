import duckdb
from fastapi import APIRouter, HTTPException
from ...core.catalog_client import get_catalog_client
router = APIRouter(prefix="/duckdb", tags=["DuckDB"])
from fastapi import APIRouter, HTTPException, Query
from pyiceberg.catalog import NoSuchNamespaceError,NamespaceAlreadyExistsError,TableAlreadyExistsError,NoSuchTableError
from pyiceberg.types import (
    LongType, StringType, DoubleType, TimestampType
)
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.transforms import (
    YearTransform,
    BucketTransform,
)
# from pyiceberg.partitioning import PartitionSpecBuilder
# from pyiceberg import partitioning.PartitionSpec

# @router.post("/manual-create-duckdb-table")
# def create_transaction_duckdb():
#     """
#     Create a DuckDB table and a view that simulates Iceberg-style partitions.
#     """
#     namespace = "pos_transactions01"
#     table_name = "transaction_duck01"
#     view_name = f"{table_name}_view"
#     table_identifier = f"{namespace}.{table_name}"
#     view_identifier = f"{namespace}.{view_name}"
#     db_path = "data/catalog.db"
#
#     try:
#         # Connect to DuckDB
#         con = duckdb.connect(database=db_path, read_only=False)
#
#         # Create schema
#         con.execute(f"CREATE SCHEMA IF NOT EXISTS {namespace}")
#
#         # Create base table
#         create_table_sql = f"""
#         CREATE TABLE IF NOT EXISTS {table_identifier} (
#             pri_id BIGINT PRIMARY KEY,
#             store_code__c VARCHAR,
#             Branch_Name__c VARCHAR,
#             customer_mobile__c BIGINT,
#             Customer_Name__c VARCHAR,
#             Bill_No__c VARCHAR,
#             Bill_Date__c TIMESTAMP,
#             Invoice_Date__c TIMESTAMP,
#             Invoice_Amount__c DOUBLE,
#             bill_status__c VARCHAR,
#             bill_transaction_no__c VARCHAR,
#             Item_Code__c BIGINT,
#             Item_Name__c VARCHAR,
#             bill_tax__c DOUBLE,
#             bill_grand_total__c DOUBLE,
#             CreatedDate TIMESTAMP
#         );
#         """
#         con.execute(create_table_sql)
#
#         # Create a view that adds partition-equivalent computed columns
#         create_view_sql = f"""
#         CREATE OR REPLACE VIEW {view_identifier} AS
#         SELECT
#             *,
#             DATE_TRUNC('day', Bill_Date__c) AS day,
#             HASH(store_code__c) % 32 AS store_bucket,
#             HASH(customer_mobile__c) % 32 AS customer_bucket
#         FROM {table_identifier};
#         """
#         con.execute(create_view_sql)
#         con.close()
#
#         return {
#             "status": "success",
#             "table": table_identifier,
#             "view": view_identifier,
#             "message": "DuckDB table and partition view created successfully",
#             "partition_equivalents": ["day", "store_bucket", "customer_bucket"]
#         }
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"DuckDB table creation failed: {str(e)}")


# @router.post("/table")
# def create_duckdb():
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
#                 year(Bill_Date__c),
#                 # bucket(32, store_code__c),
#                 # bucket(32, customer_mobile__c)
#             );
#             """
#         catalog = get_catalog_client()
#
#         try:
#             catalog.load_namespace_properties(namespace)
#         except NoSuchNamespaceError:
#             catalog.create_namespace(namespace)
#         except NamespaceAlreadyExistsError:
#             pass
#
#         try:
#             tbl = catalog.create_table(
#                 identifier=table_identifier,
#
#                 # partition_spec=transaction_partition_spec,
#                 properties={
#                     # "write.format.default": "parquet",
#                     # "write.parquet.compression-codec": "zstd",
#                     "write.partition.path-style": "directory",
#                 },
#             )
#             print(f"✅ Created Iceberg table: {table_identifier}")
#         except TableAlreadyExistsError:
#             print("Table already exists")
#
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

# @router.post("/table")
# def create_duckdb():
#     """
#     Create table inside DuckDB only.
#     """
#     namespace = "pos_transactions"     # duckdb has no namespace, optional logical name
#     table_name = "transaction_duck"
#     db_path = "data/catalog.db"
#
#     create_sql = f"""
#         CREATE TABLE IF NOT EXISTS {table_name} (
#             pri_id BIGINT,
#             store_code__c VARCHAR,
#             Branch_Name__c VARCHAR,
#             customer_mobile__c BIGINT,
#             Customer_Name__c VARCHAR,
#             Bill_No__c VARCHAR,
#             Bill_Date__c TIMESTAMP,
#             Invoice_Date__c TIMESTAMP,
#             Invoice_Amount__c DOUBLE,
#             bill_status__c VARCHAR,
#             bill_transaction_no__c VARCHAR,
#             Item_Code__c BIGINT,
#             Item_Name__c VARCHAR,
#             bill_tax__c DOUBLE,
#             bill_grand_total__c DOUBLE,
#             CreatedDate TIMESTAMP
#         );
#     """
#
#     try:
#         import duckdb
#         con = duckdb.connect(db_path)
#         con.execute(create_sql)
#         con.close()
#         return {
#             "status": "success",
#             "table": table_name,
#             "message": "DuckDB table created successfully",
#             "duckdb_ddl": create_sql.strip()
#         }
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"DuckDB table creation failed: {str(e)}")

@router.post("/table")
def create_duckdb():
    """
    Create an Iceberg table (via PyIceberg) and also return a
    portable Iceberg SQL DDL you can run in Spark/Trino/FlinK.
    """
    namespace = "pos_transactions"
    table_name = "transaction_duck"
    table_identifier = f"{namespace}.{table_name}"          # PyIceberg identifier
    sql_identifier = f"prod.db.{table_name}"                # For the SQL DDL string (keep consistent if you prefer)

    # --- Clean, engine-friendly DDL (no '#' comments, correct transform names) ---
    iceberg_create_sql = f"""
CREATE TABLE {sql_identifier} (
    pri_id BIGINT,
    store_code__c STRING,
    Branch_Name__c STRING,
    customer_mobile__c BIGINT,
    Customer_Name__c STRING,
    Bill_No__c STRING,
    Bill_Date__c TIMESTAMP,
    Invoice_Date__c TIMESTAMP,
    Invoice_Amount__c DOUBLE,
    bill_status__c STRING,
    bill_transaction_no__c STRING,
    Item_Code__c BIGINT,
    Item_Name__c STRING,
    bill_tax__c DOUBLE,
    bill_grand_total__c DOUBLE,
    CreatedDate TIMESTAMP
)
USING iceberg
PARTITIONED BY (
    years(Bill_Date__c),
    bucket(32, store_code__c),
    bucket(32, customer_mobile__c)
);
""".strip()

    try:
        catalog = get_catalog_client()

        # Ensure namespace
        try:
            catalog.load_namespace_properties(namespace)
        except NoSuchNamespaceError:
            catalog.create_namespace(namespace)
        except NamespaceAlreadyExistsError:
            pass

        # --- Build Schema with stable field IDs ---
        # (IDs start at 1 and must remain stable once published)
        fields = [
            ("pri_id", LongType()),
            ("store_code__c", StringType()),
            ("Branch_Name__c", StringType()),
            ("customer_mobile__c", LongType()),
            ("Customer_Name__c", StringType()),
            ("Bill_No__c", StringType()),
            ("Bill_Date__c", TimestampType()),       # timestamp (no tz)
            ("Invoice_Date__c", TimestampType()),
            ("Invoice_Amount__c", DoubleType()),
            ("bill_status__c", StringType()),
            ("bill_transaction_no__c", StringType()),
            ("Item_Code__c", LongType()),
            ("Item_Name__c", StringType()),
            ("bill_tax__c", DoubleType()),
            ("bill_grand_total__c", DoubleType()),
            ("CreatedDate", TimestampType()),
        ]

        from pyiceberg.types import NestedField
        schema = Schema(
            *[
                NestedField(id=i + 1, name=name, field_type=typ, required=False)
                for i, (name, typ) in enumerate(fields)
            ]
        )
        spec = PartitionSpec(
            partitions=[
                {
                    "source-id": schema.find_field("Bill_Date__c").field_id,
                    # "transform": YearTransform(column="Bill_Date__c"),
                    # YearTransform()
                    "transform": YearTransform(),
                },
                {
                    "source-id": schema.find_field("store_code__c").field_id,
                    # "transform": BucketTransform(transform="bucket", column="store_code__c", num_buckets=32),
                    "transform": BucketTransform(num_buckets=32),
                },
                {
                    "source-id": schema.find_field("customer_mobile__c").field_id,
                    # "transform": BucketTransform(transform="bucket", column="customer_mobile__c", num_buckets=32),
                    "transform": BucketTransform(num_buckets=32),
                },
            ]
        )


        # --- Create the table (no-op if it already exists) ---
        try:
            catalog.create_table(
                identifier=table_identifier,
                schema=schema,
                partition_spec=spec,
                properties={
                    # Example write props; adjust to your infra
                    # "write.format.default": "parquet",
                    # "write.parquet.compression-codec": "zstd",
                    "write.partition.path-style": "directory",
                },
            )
            created_msg = f"✅ Created Iceberg table: {table_identifier}"
        except TableAlreadyExistsError:
            created_msg = f"ℹ️ Table already exists: {table_identifier}"

        return {
            "status": "success",
            "duckdb_table": table_identifier,
            "message": created_msg,
            "partition_equivalents": [
                "years(Bill_Date__c)",
                # "bucket(32, store_code__c)",
                # "bucket(32, customer_mobile__c)",
            ],
            "iceberg_ddl": iceberg_create_sql,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DuckDB/Iceberg table creation failed: {str(e)}")

@router.get("/read-duckdb")
def read_duckdb_table(
    schema: str = Query("pos_transactions01", description="DuckDB schema name"),
    table: str = Query("transaction_duck01", description="DuckDB table name"),
    limit: int = Query(10, description="Number of rows to fetch")
):
    """
    Read data from a DuckDB table or view with optional row limit.
    """
    db_path = "data/catalog.db"
    table_identifier = f"{schema}.{table}"

    try:
        # Connect to DuckDB
        con = duckdb.connect(database=db_path, read_only=True)

        # Verify table existence
        result = con.execute(f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{schema}' AND table_name = '{table}'
        """).fetchall()

        if not result:
            raise HTTPException(status_code=404, detail=f"Table {table_identifier} not found")

        # Fetch data
        query = f"SELECT * FROM {table_identifier} LIMIT {limit};"
        rows = con.execute(query).fetchdf()

        con.close()

        # Convert to list of dicts for JSON response
        return {
            "status": "success",
            "table": table_identifier,
            "rows_fetched": len(rows),
            "data": rows.to_dict(orient="records")
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read DuckDB table: {str(e)}")