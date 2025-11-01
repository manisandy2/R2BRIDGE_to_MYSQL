import duckdb
from fastapi import APIRouter, HTTPException

router = APIRouter()

@router.post("/-duckdb-table")
def create_transaction_duckdb():
    """
    Create a predefined DuckDB table for transaction phone data
    with a static schema (similar to Iceberg schema).
    """
    namespace = "pos_transactions01"
    table_name = "transaction_duck01"
    db_path = "data/catalog.db"  # <-- change this path as needed
    con = duckdb.connect(database=db_path)

    # Step 1: Create schema (namespace) if not exists
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {namespace}")

    # Step 2: Define the CREATE TABLE query
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {namespace}.{table_name} (
        pri_id BIGINT PRIMARY KEY,
        store_code__c VARCHAR,
        Branch_Name__c VARCHAR,
        customer_mobile__c BIGINT,
        Customer_Name__c VARCHAR,
        Bill_No__c VARCHAR,
        Bill_Date__c TIMESTAMP,
        Invoice_Date__c TIMESTAMP,
        Invoice_Amount__c DOUBLE,
        bill_status__c VARCHAR,
        bill_transaction_no__c VARCHAR,
        Item_Code__c BIGINT,
        Item_Name__c VARCHAR,
        bill_tax__c DOUBLE,
        bill_grand_total__c DOUBLE,
        CreatedDate TIMESTAMP
    );
    """

    # Step 3: Execute the table creation
    try:
        con.execute(create_table_sql)
        con.close()

        return {
            "status": "created",
            "table": f"{namespace}.{table_name}",
            "schema_fields": [
                "pri_id", "store_code__c", "Branch_Name__c", "customer_mobile__c",
                "Customer_Name__c", "Bill_No__c", "Bill_Date__c", "Invoice_Date__c",
                "Invoice_Amount__c", "bill_status__c", "bill_transaction_no__c",
                "Item_Code__c", "Item_Name__c", "bill_tax__c",
                "bill_grand_total__c", "CreatedDate"
            ]
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DuckDB table creation failed: {str(e)}")