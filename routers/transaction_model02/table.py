from fastapi import APIRouter,Query,HTTPException
from pyiceberg.exceptions import NoSuchTableError
from ...core.catalog_client import get_catalog_client
from pyiceberg.schema import Schema
# from core.catalog_client import security,verify_jwt
# from fastapi.security import HTTPAuthorizationCredentials
from pyiceberg.types import *
from pyiceberg.catalog import NoSuchNamespaceError,NamespaceAlreadyExistsError,TableAlreadyExistsError,NoSuchTableError

router = APIRouter(prefix="", tags=["Tables"])


@router.get("/table/list")
def get_tables(
        namespace: str = Query(..., description="Namespace to list tables from"),

):
    try:
        catalog = get_catalog_client()
        tables = catalog.list_tables(namespace)

        if tables:
            return {"namespace": namespace, "tables": tables}
        else:
            return {"namespace": namespace, "tables": [], "message": "No tables found."}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list tables in namespace '{namespace}': {str(e)}")

@router.post("/table/create")
def create_transaction(
        namespace: str = Query("pos_transactions"),
        table_name: str = Query(..., description="Table name"),
):
    # namespace = "pos_transactions_add_range"
    # table_name = "transaction_with_in_partition"
    # table_name = "iceberg_add_range_test"
    table_identifier = f"{namespace}.{table_name}"

    # Step 1: Define Iceberg schema
    transaction_schema = Schema(
        NestedField(1,"pri_id",LongType(),required=True),
        NestedField(2, "store_code__c", StringType()),
        NestedField(3, "Branch_Name__c", StringType()),
        NestedField(4, "customerId", StringType()),
        NestedField(5, "customer_mobile__c", LongType()),
        NestedField(6, "Customer_Name__c", StringType()),
        NestedField(7, "Bill_No__c", StringType()),
        NestedField(8, "Bill_Date__c", DateType()),
        NestedField(9, "Invoice_Amount__c", DoubleType()),
        NestedField(10, "bill_status__c", StringType()),
        NestedField(11, "bill_transaction_no__c", StringType()),
        NestedField(12, "Item_Code__c", LongType()),
        NestedField(13, "Item_Name__c", StringType()),
        NestedField(14, "bill_tax__c", DoubleType()),
        NestedField(15, "bill_grand_total__c", DoubleType()),
        NestedField(16, "CreatedDate", DateType()),
    )


    # Step 2: Define partition spec
    # transaction_partition_spec = PartitionSpec(
    #     PartitionField(
    #         source_id=transaction_schema.find_field("Bill_Date__c").field_id,
    #         field_id=2001,
    #         transform=YearTransform(),
    #         name="year",
    #     ),
    #
    # )

    # Step 3: Connect to catalog
    catalog = get_catalog_client()

    # Step 4: Ensure namespace exists
    try:
        catalog.load_namespace_properties(namespace)
    except NoSuchNamespaceError:
        catalog.create_namespace(namespace)
    except NamespaceAlreadyExistsError:
        pass

    # Step 5: Create table
    try:
        tbl = catalog.create_table(
            identifier=table_identifier,
            schema=transaction_schema,
            # partition_spec=transaction_partition_spec,
            properties={
                "format-version": "2",  # <-- mandatory
                "table-type": "MERGE_ON_READ",  # <-- enable merge-on-read
                "primary-key": "pri_id",        # <-- enforce PK
                "identifier-field-ids": "1",
                "write.format.default": "parquet",
                "write.parquet.compression-codec": "zstd",
                "write.partition.path-style": "directory",
                "write.sort.order": "customer_mobile__c ASC, Bill_Date__c ASC",
                # write.sort.order": "month(Bill_Date__c) ASC, customer_mobile__c ASC, Bill_Date__c ASC"
                "write.target-file-size-bytes": "268435456"
            },
        )
        print(f"✅ Created Iceberg table: {table_identifier}")

        # Step 6: Return confirmation
        return {
            "status": "created",
            "table": table_identifier,
            "schema_fields": [f.name for f in transaction_schema.fields],
            # "partitions": [f.name for f in transaction_partition_spec.fields],
        }

    except TableAlreadyExistsError:
        return {"status": "exists", "table": table_identifier}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Table creation failed: {str(e)}")


@router.post("/table/rename")
def rename_table(
    namespace: str = Query(..., description="Namespace containing the table"),
    old_table_name: str = Query(..., description="Current table name (e.g. 'transactions')"),
    new_table_name: str = Query(..., description="New table name (e.g. 'transactions_v2')"),

):

    catalog = get_catalog_client()
    try:

        old_identifier = f"{namespace}.{old_table_name}"
        new_identifier = f"{namespace}.{new_table_name}"

        catalog.rename_table(old_identifier, new_identifier)

        return {
            "status": "success",
            "message": f"Table renamed from '{old_table_name}' to '{new_table_name}' in namespace '{namespace}' successfully."
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to rename table '{old_table_name}' in namespace '{namespace}': {str(e)}")

    finally:
        try:
            catalog.close()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to close catalog: {str(e)}")



@router.delete("/table/delete")
def delete_table(
    namespace: str = Query(..., description="Namespace of the table"),
    table_name: str = Query(..., description="Name of the table to drop"),

):

    catalog = get_catalog_client()
    full_table_name = f"{namespace}.{table_name}"

    try:
        catalog.drop_table(full_table_name)
        return {"message": f"Table '{full_table_name}' dropped successfully."}

    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table '{full_table_name}' does not exist.")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to drop table '{full_table_name}': {str(e)}")
